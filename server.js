// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets (6 Tabs)

require("dotenv").config();

const express = require("express");
const http = require("http");
const https = require("https");
const WebSocket = require("ws");
const { google } = require("googleapis");

// --------------------------------------------------
// Helpers & Env
// --------------------------------------------------
const envNum = (k, d) => {
  const v = Number(process.env[k]);
  return Number.isFinite(v) ? v : d;
};
const envBool = (k, d = false) =>
  ["1", "true", "yes", "on"].includes(String(process.env[k] || "").toLowerCase()) || d;

const PORT = envNum("PORT", 10000);
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

const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);
const always = (...a) => console.log("[ALWAYS]", ...a);

// --------------------------------------------------
// Webhook Caller (Native HTTPS)
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
    error("Webhook URL parse failed", e.message);
  }
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
    if (k) out[k] = v;
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

    // רשימת טאבים לטעינה - הוספת חסינות לשמות טאבים
    const tabs = ["PROMPTS", "SETTINGS", "ROUTING_RULES", "BUSINESS_INFO", "SALES_SCRIPT", "SUPPORT_SCRIPT"];
    
    // טעינה אחד אחד כדי למנוע קריסה של כל ה-Batch אם טאב אחד חסר
    const results = await Promise.all(tabs.map(async (tab) => {
      try {
        const res = await sheets.spreadsheets.values.get({ spreadsheetId: GSHEET_ID, range: `${tab}!A:Z` });
        return { tab, values: res.data.values || [] };
      } catch (e) {
        error(`Tab ${tab} load failed (check if it exists in Sheet)`, e.message);
        return { tab, values: [] };
      }
    }));

    const getValues = (name) => results.find(r => r.tab === name)?.values || [];

    SHEETS.prompts = parseTable(getValues("PROMPTS"), "prompt_id", "content_he");
    SHEETS.settings = parseTable(getValues("SETTINGS"), "key", "value");
    SHEETS.routing_rules = parseToObjects(getValues("ROUTING_RULES"));
    SHEETS.business_info = parseToObjects(getValues("BUSINESS_INFO"));
    SHEETS.sales_script = parseToObjects(getValues("SALES_SCRIPT"));
    SHEETS.support_script = parseToObjects(getValues("SUPPORT_SCRIPT"));

    SHEETS.loaded_at = new Date().toISOString();
    log(`Sheets refresh completed at ${SHEETS.loaded_at}`);
  } catch (e) {
    error("Critical Sheets load error", e.message);
  }
}

const getPrompt = (id, fallback = "") => SHEETS.prompts[id] || fallback;
const getSetting = (key, fallback = "") => SHEETS.settings[key] || fallback;

// --------------------------------------------------
// Decision Proxy (Knowledge Injection)
// --------------------------------------------------
function getKnowledgeInjection(callerText) {
  const text = (callerText || "").toLowerCase();
  let injection = "";

  if (text.includes("משלוח") || text.includes("איפה") || text.includes("הזמנה")) {
    const biz = SHEETS.business_info.find(i => String(i.topic || "").toLowerCase().includes("delivery"))?.content || "";
    injection += `\nמידע משלוחים: ${biz}. חשוב: אין גישה לסטטוס חי. אם זה אחרי שעות הפעילות, תני מספרי מובילים ואל תבטיחי לבדוק.`;
  }
  
  if (text.includes("תקלה") || text.includes("בעיה") || text.includes("עזרה")) {
    const support = SHEETS.support_script.map(s => `${s.issue}: ${s.solution}`).join(" | ");
    if (support) injection += `\nפתרונות תמיכה: ${support}`;
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
app.post("/sheets/reload", async (_, res) => { await loadSheets(); res.json({ ok: true }); });

app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  res.type("text/xml").send(`
<Response>
  <Connect>
    <Stream url="wss://${host}/twilio-media-stream">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
    </Stream>
  </Connect>
</Response>`.trim());
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// --------------------------------------------------
// WS Bridge (Twilio <-> OpenAI)
// --------------------------------------------------
wss.on("connection", (twilioWs) => {
  let twilioStreamSid = null;
  let callerNum = "";
  let calledNum = "";
  let openaiReady = false;
  let awaitingResponse = false;
  let pendingResponseRequest = false;
  let hasTranscripts = false;
  let lastCallerText = "";
  let fullTranscript = [];

  const connTag = `conn_${Date.now().toString(36)}`;

  const openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" }
  });

  const requestAssistantResponse = () => {
    if (awaitingResponse) { pendingResponseRequest = true; return; }
    awaitingResponse = true;
    pendingResponseRequest = false;
    if (openaiWs.readyState === WebSocket.OPEN) {
      openaiWs.send(JSON.stringify({ type: "response.create", response: { modalities: ["audio", "text"] } }));
    }
  };

  openaiWs.on("open", async () => {
    openaiReady = true;
    if (!SHEETS.loaded_at) await loadSheets();

    const master = getPrompt("MASTER_PROMPT", "את נטע מגיל ספורט. קצרה וקלילה.");
    const opening = getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");

    openaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        voice: OPENAI_VOICE,
        instructions: master,
        input_audio_transcription: { model: MB_TRANSCRIPTION_MODEL },
        turn_detection: { type: "server_vad", threshold: MB_VAD_THRESHOLD, silence_duration_ms: MB_VAD_SILENCE_MS }
      }
    }));

    // Opening
    awaitingResponse = true;
    openaiWs.send(JSON.stringify({
      type: "response.create",
      response: { modalities: ["audio", "text"], instructions: `תגידי בדיוק: ${opening}` }
    }));

    callWebhook({ event: "call_started", callSid: twilioStreamSid, caller: callerNum });
  });

  openaiWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());

    if (msg.type === "response.audio_transcript.done") {
      const t = msg.transcript?.trim();
      if (t) { fullTranscript.push(`Bot: ${t}`); always(`[BOT][${connTag}]`, t); }
    }

    if (msg.type === "conversation.item.input_audio_transcription.completed") {
      const t = msg.transcript?.trim();
      if (t) {
        lastCallerText = t;
        fullTranscript.push(`User: ${t}`);
        always(`[CALLER][${connTag}]`, t);
        hasTranscripts = true;

        const injection = getKnowledgeInjection(t);
        if (injection) {
          openaiWs.send(JSON.stringify({
            type: "conversation.item.create",
            item: { type: "message", role: "system", content: [{ type: "input_text", text: injection }] }
          }));
        }
        requestAssistantResponse();
      }
    }

    if (msg.type === "input_audio_buffer.speech_stopped") { requestAssistantResponse(); }

    if (msg.type === "response.done") {
      awaitingResponse = false;
      if (pendingResponseRequest) requestAssistantResponse();
    }

    if (msg.type === "response.audio.delta" && twilioStreamSid) {
      twilioWs.send(JSON.stringify({ event: "media", streamSid: twilioStreamSid, media: { payload: msg.delta } }));
    }
  });

  twilioWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.event === "start") {
      twilioStreamSid = msg.start.streamSid;
      callerNum = msg.start.customParameters?.caller || "";
      calledNum = msg.start.customParameters?.called || "";
    }
    if (msg.event === "media" && openaiReady && openaiWs.readyState === WebSocket.OPEN) {
      openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
    }
    if (msg.event === "stop") openaiWs.close();
  });

  const finalize = () => {
    const rec = `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/${twilioStreamSid}`;
    const payload = { 
      callSid: twilioStreamSid, caller: callerNum, caller_last_utterance: lastCallerText, 
      transcript: fullTranscript.join("\n"), recording_url_public: rec 
    };
    callWebhook({ ...payload, event: hasTranscripts ? "call_ended" : "call_abandoned" });
  };

  twilioWs.on("close", finalize);
  openaiWs.on("close", () => twilioWs.close());
});

server.listen(PORT, () => {
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
});
