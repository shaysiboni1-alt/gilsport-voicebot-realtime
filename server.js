// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets (6 Tabs)

require("dotenv").config();

const express = require("express");
const http = require("http");
const https = require("https"); // שימוש במודול מובנה במקום axios
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
// ENV
// --------------------------------------------------
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

const ALLOWED_VOICES = new Set(["alloy", "ash", "ballad", "coral", "echo", "sage", "shimmer", "verse", "marin", "cedar"]);
function normalizeVoice(v) {
  const raw = String(v || "").trim().toLowerCase();
  return ALLOWED_VOICES.has(raw) ? raw : "alloy";
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
// Webhook Caller (Native HTTPS)
// --------------------------------------------------
function callWebhook(payload) {
  if (!MB_WEBHOOK_URL) return;
  const data = JSON.stringify({ ...payload, timestamp: new Date().toISOString() });
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
}

// --------------------------------------------------
// Sheets (Single Source of Truth - 6 Tabs)
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {}, 
  settings: {},
  routing_rules: [],
  business_info: [],
  sales_script: [],
  support_script: []
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

    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: GSHEET_ID,
      ranges: ["PROMPTS!A:Z", "SETTINGS!A:Z", "ROUTING_RULES!A:Z", "BUSINESS_INFO!A:Z", "SALES_SCRIPT!A:Z", "SUPPORT_SCRIPT!A:Z"]
    });

    const vrs = res.data.valueRanges || [];
    const getRange = (name) => vrs.find(v => v.range.includes(name))?.values || [];

    SHEETS.prompts = parseTable(getRange("PROMPTS"), "prompt_id", "content_he");
    SHEETS.settings = parseTable(getRange("SETTINGS"), "key", "value");
    SHEETS.routing_rules = parseToObjects(getRange("ROUTING_RULES"));
    SHEETS.business_info = parseToObjects(getRange("BUSINESS_INFO"));
    SHEETS.sales_script = parseToObjects(getRange("SALES_SCRIPT"));
    SHEETS.support_script = parseToObjects(getRange("SUPPORT_SCRIPT"));

    SHEETS.loaded_at = new Date().toISOString();
    log(`Sheets loaded (6 tabs) at ${SHEETS.loaded_at}`);
  } catch (e) {
    error("Sheets load failed", e.message);
  }
}

const getPrompt = (id, fallback = "") => String(SHEETS.prompts[id] || fallback).trim();
const getSetting = (key, fallback = "") => String(SHEETS.settings[key] || fallback).trim();

// --------------------------------------------------
// Decision Proxy Logic (No FSM)
// --------------------------------------------------
function getKnowledgeInjection(callerText) {
  const text = (callerText || "").toLowerCase();
  let injection = "";

  // Delivery / Routing Rules
  if (text.includes("משלוח") || text.includes("איפה") || text.includes("הזמנה")) {
    const biz = SHEETS.business_info.find(i => String(i.topic || "").includes("delivery"))?.content || "";
    injection += `\nמידע משלוחים מהשיטס: ${biz}. חשוב: אין לנו גישה לסטטוס חי. אם מדובר באספקה להיום ואנחנו אחרי שעות הפעילות, מסרי מספרי מובילים (אם יש במידע) ואל תבטיחי בדיקה.`;
  }
  
  // Support
  if (text.includes("תקלה") || text.includes("בעיה") || text.includes("עזרה")) {
    const support = SHEETS.support_script.map(s => `${s.issue}: ${s.solution}`).join(" | ");
    if (support) injection += `\nפתרונות תמיכה מהשיטס: ${support}`;
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
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// --------------------------------------------------
// WebSocket (Twilio <-> OpenAI)
// --------------------------------------------------
wss.on("connection", (twilioWs, req) => {
  RUNTIME.ws_connections += 1;
  RUNTIME.last_ws_conn_at = new Date().toISOString();

  let twilioStreamSid = null;
  let callerNum = "";
  let calledNum = "";
  let openaiReady = false;
  let awaitingResponse = false;
  let pendingResponseRequest = false;
  let hasSpokenToBot = false; // To distinguish between active calls and instant hangups
  let lastCallerFinal = "";
  let transcript = [];

  const connTag = `conn_${Date.now().toString(36)}`;

  let openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" }
  });

  const safeOpenAISend = (obj) => {
    if (openaiWs && openaiWs.readyState === WebSocket.OPEN) {
      openaiWs.send(JSON.stringify(obj));
      return true;
    }
    return false;
  };

  const requestAssistantResponse = (reason = "") => {
    if (awaitingResponse) {
      pendingResponseRequest = true;
      return;
    }
    awaitingResponse = true;
    pendingResponseRequest = false;
    safeOpenAISend({ type: "response.create", response: { modalities: ["audio", "text"] } });
  };

  openaiWs.on("open", async () => {
    openaiReady = true;
    if (!SHEETS.loaded_at) await loadSheets();

    const masterPrompt = getPrompt("MASTER_PROMPT", "את נטע מגיל ספורט. קצרה, קלילה, עוזרת.");
    const openingScript = getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");

    safeOpenAISend({
      type: "session.update",
      session: {
        modalities: ["audio", "text"],
        voice: OPENAI_VOICE,
        input_audio_transcription: { model: MB_TRANSCRIPTION_MODEL },
        turn_detection: {
          type: "server_vad",
          threshold: MB_VAD_THRESHOLD,
          silence_duration_ms: MB_VAD_SILENCE_MS,
          prefix_padding_ms: MB_VAD_PREFIX_MS
        },
        instructions: masterPrompt
      }
    });

    // Initial Greeting
    awaitingResponse = true;
    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: `תגידי בדיוק: ${openingScript}`
      }
    });

    callWebhook({ event: "call_started", callSid: twilioStreamSid, caller: callerNum });
  });

  openaiWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());

    if (msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) {
        transcript.push(`Bot: ${t}`);
        always(`[BOT][${connTag}]`, t);
      }
    }

    if (msg.type === "conversation.item.input_audio_transcription.completed") {
      const t = String(msg.transcript || "").trim();
      if (t) {
        lastCallerFinal = t;
        transcript.push(`User: ${t}`);
        always(`[CALLER][${connTag}]`, t);
        hasSpokenToBot = true;

        // Inject Knowledge based on Sheets
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
      twilioWs.send(JSON.stringify({
        event: "media",
        streamSid: twilioStreamSid,
        media: { payload: msg.delta }
      }));
    }
  });

  twilioWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.event === "start") {
      twilioStreamSid = msg.start.streamSid;
      callerNum = msg.start.customParameters?.caller || "";
      calledNum = msg.start.customParameters?.called || "";
    }
    if (msg.event === "media" && openaiReady) {
      safeOpenAISend({ type: "input_audio_buffer.append", audio: msg.media.payload });
    }
    if (msg.event === "stop") {
      if (openaiWs) openaiWs.close();
    }
  });

  const onCallEnd = () => {
    const recUrl = `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/${twilioStreamSid}`;
    const commonPayload = {
      callSid: twilioStreamSid,
      caller: callerNum,
      called: calledNum,
      caller_last_utterance: lastCallerFinal,
      transcript: transcript.join("\n"),
      recording_url_public: recUrl
    };

    if (!hasSpokenToBot && transcript.length < 2) {
      callWebhook({ ...commonPayload, event: "call_abandoned" });
    } else {
      callWebhook({ ...commonPayload, event: "call_ended" });
    }
  };

  twilioWs.on("close", onCallEnd);
  openaiWs.on("close", () => twilioWs.close());
  openaiWs.on("error", (e) => error("OpenAI Error", e.message));
});

server.listen(PORT, () => {
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
});
