// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets (Correct Tabs from Uploaded Files)

require("dotenv").config();

const express = require("express");
const http = require("http");
const https = require("https"); // Native HTTPS for Webhooks (No Axios required)
const WebSocket = require("ws");
const { google } = require("googleapis");

// --------------------------------------------------
// Helpers
// --------------------------------------------------
const envNum = (k, d) => {
  const v = Number(process.env[k]);
  return Number.isFinite(v) ? v : d;
};
const envBool = (k, d = false) =>
  ["1", "true", "yes", "on"].includes(String(process.env[k] || "").toLowerCase()) || d;

const PORT = envNum("PORT", 10000);

// --------------------------------------------------
// ENV (NO EARLY FAILS ❗)
// --------------------------------------------------
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

const ALLOWED_VOICES = new Set([
  "alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse", "marin", "cedar"
]);
function normalizeVoice(v) {
  const raw = String(v || "").trim().toLowerCase();
  if (ALLOWED_VOICES.has(raw)) return raw;
  return "alloy";
}
const OPENAI_VOICE = normalizeVoice(process.env.OPENAI_VOICE || "alloy");

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const MB_WEBHOOK_URL = process.env.MB_WEBHOOK_URL || "";
const MB_DEBUG = envBool("MB_DEBUG", false);

const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);

const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";
const MB_LOG_RAW_OPENAI = envBool("MB_LOG_RAW_OPENAI", false);

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);
const always = (...a) => console.log("[ALWAYS]", ...a);

const preview = (s, n = 300) => {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  return t.length > n ? t.slice(0, n) + "..." : t;
};

// --------------------------------------------------
// Runtime diagnostics
// --------------------------------------------------
const RUNTIME = {
  booted_at: new Date().toISOString(),
  ws_connections: 0,
  ws_closed: 0,
  ws_errors: 0,
  openai_errors: 0,
  openai_closed: 0,
  last_ws_conn_at: null,
  last_ws_close_at: null
};

// --------------------------------------------------
// Webhook (Native HTTPS - NO AXIOS)
// --------------------------------------------------
function callWebhook(payload) {
  if (!MB_WEBHOOK_URL) return;
  const data = JSON.stringify({ ...payload, timestamp: new Date().toISOString() });
  try {
    const url = new URL(MB_WEBHOOK_URL);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Content-Length": Buffer.byteLength(data)
      }
    };
    const req = https.request(options);
    req.on("error", (e) => error("Webhook failed", e.message));
    req.write(data);
    req.end();
  } catch (e) {
    error("Webhook URL error", e.message);
  }
}

// --------------------------------------------------
// Sheets (Matching EXACT uploaded files)
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {}, 
  settings: {},
  kb_facts: [],
  do_not_say: [],
  suppliers: [],
  delivery_contacts: []
};

function parseTable(rows, keyColName, valColName) {
  const out = {};
  if (!rows || rows.length === 0) return out;
  const headers = (rows[0] || []).map((h) => String(h || "").trim());
  const keyIdx = headers.indexOf(keyColName);
  const valIdx = headers.indexOf(valColName);
  if (keyIdx === -1 || valIdx === -1) return out;

  for (let i = 1; i < rows.length; i++) {
    const r = rows[i];
    const k = String(r[keyIdx] || "").trim();
    const v = String(r[valIdx] || "");
    if (!k) continue;
    out[k] = v;
  }
  return out;
}

function parseToObjects(rows) {
  if (!rows || rows.length < 2) return [];
  const headers = rows[0].map(h => String(h || "").trim());
  return rows.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => { if(h) obj[h] = r[i] || ""; });
    return obj;
  });
}

async function loadSheets() {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) return;
  try {
    const json = JSON.parse(Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8"));
    const auth = new google.auth.JWT({
      email: json.client_email,
      key: json.private_key,
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    });
    const sheets = google.sheets({ version: "v4", auth });

    // EXACT tab names from your files
    const tabs = ["PROMPTS", "SETTINGS", "KB_FACTS", "DO_NOT_SAY", "SUPPLIERS_IMPORTERS", "DELIVERY_CONTACTS"];
    
    const results = await Promise.all(tabs.map(async (tab) => {
      try {
        const res = await sheets.spreadsheets.values.get({ spreadsheetId: GSHEET_ID, range: `${tab}!A:Z` });
        return { tab, values: res.data.values || [] };
      } catch (e) {
        error(`Tab ${tab} load error`, e.message);
        return { tab, values: [] };
      }
    }));

    const get = (name) => results.find(r => r.tab === name)?.values || [];

    SHEETS.prompts = parseTable(get("PROMPTS"), "prompt_id", "content_he");
    SHEETS.settings = parseTable(get("SETTINGS"), "key", "value");
    SHEETS.kb_facts = parseToObjects(get("KB_FACTS"));
    SHEETS.do_not_say = parseToObjects(get("DO_NOT_SAY"));
    SHEETS.suppliers = parseToObjects(get("SUPPLIERS_IMPORTERS"));
    SHEETS.delivery_contacts = parseToObjects(get("DELIVERY_CONTACTS"));

    SHEETS.loaded_at = new Date().toISOString();
    log(`Sheets loaded (6 tabs) at ${SHEETS.loaded_at}`);
  } catch (e) {
    error("Critical Sheets load failure", e.message);
  }
}

const getPrompt = (id, fallback = "") => String(SHEETS.prompts[id] || fallback).trim();
const getSetting = (key, fallback = "") => String(SHEETS.settings[key] || fallback).trim();

// --------------------------------------------------
// Decision Proxy (Knowledge Injection)
// --------------------------------------------------
function getKnowledgeInjection(callerText) {
  const text = (callerText || "").toLowerCase();
  let injection = "";

  // 1. Delivery Decision
  if (text.includes("משלוח") || text.includes("איפה") || text.includes("הזמנה")) {
    const contacts = SHEETS.delivery_contacts.map(c => `${c.name}: ${c.phone}`).join(", ");
    injection += `\nמידע משלוחים חשוב: אל תבטיחי שבדקת סטטוס חי. אם זה אחרי שעות הפעילות, מסרי את מספרי המובילים הבאים: ${contacts}.`;
  }

  // 2. Do Not Say Enforcement
  if (SHEETS.do_not_say.length > 0) {
    const rules = SHEETS.do_not_say.map(r => r.rule || r.content).join(" | ");
    injection += `\nכללים שאסור להפר: ${rules}`;
  }

  // 3. KB Facts Keywords
  const relevantFact = SHEETS.kb_facts.find(f => text.includes(String(f.keyword || "").toLowerCase()));
  if (relevantFact) {
    injection += `\nעובדה רלוונטית מהידע שלך: ${relevantFact.content || relevantFact.fact}`;
  }

  return injection;
}

// --------------------------------------------------
// Express
// --------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.get("/health", (_, res) => res.json({ ok: true, sheets: SHEETS.loaded_at }));

app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  const wsUrl = `wss://${host}/twilio-media-stream`;
  res.type("text/xml").send(`
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
    </Stream>
  </Connect>
</Response>`.trim());
});

const server = http.createServer(app);

// --------------------------------------------------
// WebSocket (Twilio <-> OpenAI) - YOUR ORIGINAL LOGIC
// --------------------------------------------------
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs, req) => {
  RUNTIME.ws_connections += 1;
  RUNTIME.last_ws_conn_at = new Date().toISOString();

  let twilioStreamSid = null;
  let callerNum = "";
  let calledNum = "";
  let openaiReady = false;
  const pendingAudio = [];
  const connTag = `conn_${Date.now().toString(36)}`;

  let lastCallerFinal = "";
  let lastBotFinal = "";
  let transcript = [];
  let hasTranscripts = false;

  const printCallerFinal = (text) => {
    const t = String(text || "").trim();
    if (!t || t === lastCallerFinal) return;
    lastCallerFinal = t;
    transcript.push(`User: ${t}`);
    hasTranscripts = true;
    always(`[CALLER][${connTag}]`, t);
    
    // Knowledge Injection Trigger
    const injection = getKnowledgeInjection(t);
    if (injection) {
      safeOpenAISend({
        type: "conversation.item.create",
        item: {
          type: "message",
          role: "system",
          content: [{ type: "input_text", text: injection }]
        }
      });
    }
  };

  const printBotFinal = (text) => {
    const t = String(text || "").trim();
    if (!t || t === lastBotFinal) return;
    lastBotFinal = t;
    transcript.push(`Bot: ${t}`);
    always(`[BOT][${connTag}]`, t);
  };

  let openaiWs = null;

  const safeOpenAISend = (obj) => {
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) {
        openaiWs.send(JSON.stringify(obj));
        return true;
      }
      return false;
    } catch (e) {
      error("OpenAI send failed", e.message);
      return false;
    }
  };

  const safeTwilioSend = (obj) => {
    try {
      if (twilioWs && twilioWs.readyState === WebSocket.OPEN) {
        twilioWs.send(JSON.stringify(obj));
        return true;
      }
      return false;
    } catch (e) {
      error("Twilio send failed", e.message);
      return false;
    }
  };

  // -----------------------------
  // Anti-overlap logic (YOUR ORIGINAL)
  // -----------------------------
  let awaitingResponse = false;
  let pendingResponseRequest = false;

  const requestAssistantResponse = (reason = "") => {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    if (awaitingResponse) {
      pendingResponseRequest = true;
      debug(`[${connTag}] response.request queued (reason=${reason})`);
      return;
    }
    awaitingResponse = true;
    pendingResponseRequest = false;
    debug(`[${connTag}] response.create (reason=${reason})`);
    safeOpenAISend({ type: "response.create", response: { modalities: ["audio", "text"] } });
  };

  openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "OpenAI-Beta": "realtime=v1"
    }
  });

  openaiWs.on("open", async () => {
    openaiReady = true;
    if (!SHEETS.loaded_at) await loadSheets();

    const masterPrompt = getPrompt("MASTER_PROMPT", "את נטע מגיל ספורט.");
    const openingScript = getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");

    const session = {
      modalities: ["audio", "text"],
      voice: OPENAI_VOICE,
      input_audio_format: "g711_ulaw",
      output_audio_format: "g711_ulaw",
      turn_detection: {
        type: "server_vad",
        threshold: MB_VAD_THRESHOLD,
        silence_duration_ms: MB_VAD_SILENCE_MS,
        prefix_padding_ms: MB_VAD_PREFIX_MS
      },
      instructions: masterPrompt,
      input_audio_transcription: { model: MB_TRANSCRIPTION_MODEL }
    };

    safeOpenAISend({ type: "session.update", session });

    // Forced Greeting
    awaitingResponse = true;
    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: `תגידי בדיוק: ${openingScript}`
      }
    });

    callWebhook({ event: "call_started", caller: callerNum, callSid: twilioStreamSid });

    while (pendingAudio.length > 0) {
      const audio = pendingAudio.shift();
      safeOpenAISend({ type: "input_audio_buffer.append", audio });
    }
  });

  openaiWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());

    if (msg.type === "response.audio_transcript.done") {
      printBotFinal(msg.transcript);
    }

    if (msg.type === "conversation.item.input_audio_transcription.completed") {
      const t = msg.transcript?.trim();
      if (t) {
        printCallerFinal(t);
        requestAssistantResponse("caller_transcript_done");
      }
    }

    if (msg.type === "input_audio_buffer.speech_stopped") {
      requestAssistantResponse("speech_stopped");
    }

    if (msg.type === "response.done") {
      awaitingResponse = false;
      if (pendingResponseRequest) {
        pendingResponseRequest = false;
        requestAssistantResponse("pending_after_done");
      }
    }

    if (msg.type === "response.audio.delta" && twilioStreamSid) {
      safeTwilioSend({ event: "media", streamSid: twilioStreamSid, media: { payload: msg.delta } });
    }
  });

  twilioWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.event === "start") {
      twilioStreamSid = msg.start.streamSid;
      callerNum = msg.start.customParameters?.caller || "";
      calledNum = msg.start.customParameters?.called || "";
    }
    if (msg.event === "media") {
      if (!openaiReady) {
        pendingAudio.push(msg.media.payload);
      } else {
        safeOpenAISend({ type: "input_audio_buffer.append", audio: msg.media.payload });
      }
    }
    if (msg.event === "stop") {
      if (openaiWs) openaiWs.close();
    }
  });

  twilioWs.on("close", () => {
    RUNTIME.ws_closed += 1;
    const recUrl = `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/${twilioStreamSid}`;
    
    callWebhook({
      event: hasTranscripts ? "call_ended" : "call_abandoned",
      callSid: twilioStreamSid,
      caller: callerNum,
      called: calledNum,
      caller_last_utterance: lastCallerFinal,
      transcript: transcript.join("\n"),
      recording_url_public: recUrl
    });

    if (openaiWs) openaiWs.close();
  });
});

server.listen(PORT, () => {
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
});
