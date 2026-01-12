// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets (6 Tabs)

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const axios = require("axios");

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

const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";
const MB_LOG_RAW_OPENAI = envBool("MB_LOG_RAW_OPENAI", false);

// --------------------------------------------------
// Logging & Diagnostics
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);
const always = (...a) => console.log("[ALWAYS]", ...a);

const preview = (s, n = 300) => {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  return t.length > n ? t.slice(0, n) + "..." : t;
};

const RUNTIME = {
  booted_at: new Date().toISOString(),
  ws_connections: 0,
  ws_closed: 0,
  openai_errors: 0,
  last_ws_conn_at: null
};

// --------------------------------------------------
// Sheets (Single Source of Truth - 6 Tabs)
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {},
  settings: {},
  routingRules: [],
  businessInfo: [],
  salesScript: [],
  supportScript: []
};

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

    const ranges = [
      "SETTINGS!A:Z", "PROMPTS!A:Z", "ROUTING_RULES!A:Z", 
      "BUSINESS_INFO!A:Z", "SALES_SCRIPT!A:Z", "SUPPORT_SCRIPT!A:Z"
    ];

    const res = await sheets.spreadsheets.values.batchGet({ spreadsheetId: GSHEET_ID, ranges });
    const valueRanges = res.data.valueRanges || [];

    const getRows = (name) => (valueRanges.find(vr => (vr.range || "").includes(name))?.values || []);
    
    const parseMap = (rows) => {
      const out = {};
      if (rows.length < 2) return out;
      const headers = rows[0];
      for (let i = 1; i < rows.length; i++) {
        const k = String(rows[i][0] || "").trim();
        const v = String(rows[i][1] || "").trim();
        if (k) out[k] = v;
      }
      return out;
    };

    const parseList = (rows) => {
      if (rows.length < 2) return [];
      const headers = rows[0];
      return rows.slice(1).map(r => {
        const obj = {};
        headers.forEach((h, i) => { if (h) obj[h] = r[i] || ""; });
        return obj;
      });
    };

    SHEETS.settings = parseMap(getRows("SETTINGS"));
    SHEETS.prompts = parseMap(getRows("PROMPTS"));
    SHEETS.routingRules = parseList(getRows("ROUTING_RULES"));
    SHEETS.businessInfo = parseList(getRows("BUSINESS_INFO"));
    SHEETS.salesScript = parseList(getRows("SALES_SCRIPT"));
    SHEETS.supportScript = parseList(getRows("SUPPORT_SCRIPT"));
    
    SHEETS.loaded_at = new Date().toISOString();
    log(`Sheets loaded successfully at ${SHEETS.loaded_at}`);
  } catch (e) {
    error("Sheets load failed", e.message);
  }
}

const getPrompt = (id, fallback = "") => SHEETS.prompts[id] || fallback;
const getSetting = (key, fallback = "") => SHEETS.settings[key] || fallback;

// --------------------------------------------------
// Webhook & Recording Logic
// --------------------------------------------------
async function sendWebhook(payload) {
  if (!MB_WEBHOOK_URL) return;
  try {
    const data = {
      timestamp: new Date().toISOString(),
      ...payload
    };
    debug("[WEBHOOK_SEND]", data.event);
    await axios.post(MB_WEBHOOK_URL, data, { timeout: 5000 });
  } catch (e) {
    error("Webhook failed", e.message);
  }
}

// --------------------------------------------------
// Decision Proxy (Option B) - No FSM
// --------------------------------------------------
function getContextKnowledge(callerText) {
  const text = (callerText || "").toLowerCase();
  let contextInjection = "";

  // 1. Check Routing/Delivery
  if (text.includes("משלוח") || text.includes("איפה ההזמנה") || text.includes("אספקה")) {
    const deliveryPolicy = SHEETS.businessInfo.find(i => i.topic === "delivery")?.content || "";
    contextInjection += `\nמידע על משלוחים: ${deliveryPolicy}. חשוב: אין לנו גישה לסטטוס חי בזמן אמת. אם מדובר באספקה להיום ואנחנו אחרי שעות הפעילות, תציעי להשאיר פרטים או תני מספרי מובילים אם יש במידע.`;
  }

  // 2. Check Support
  if (text.includes("תמיכה") || text.includes("בעיה") || text.includes("תקלה")) {
    const supportInfo = SHEETS.supportScript.map(s => `${s.issue}: ${s.solution}`).join("\n");
    contextInjection += `\nמידע תמיכה: ${supportInfo}`;
  }

  // 3. Sales/Products
  if (text.includes("מחיר") || text.includes("קנייה") || text.includes("מעוניין")) {
    const salesInfo = SHEETS.salesScript.map(s => `${s.product}: ${s.pitch}`).join("\n");
    contextInjection += `\nמידע מכירות: ${salesInfo}`;
  }

  return contextInjection;
}

// --------------------------------------------------
// Express & Server Start
// --------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

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
// WebSocket (The Bridge)
// --------------------------------------------------
wss.on("connection", (twilioWs, req) => {
  RUNTIME.ws_connections += 1;
  RUNTIME.last_ws_conn_at = new Date().toISOString();
  
  const connTag = `conn_${Date.now().toString(36)}`;
  let twilioStreamSid = null;
  let openaiReady = false;
  let awaitingResponse = false;
  let pendingResponseRequest = false;
  let callAbandoned = true; // Default to true until response.done or clean close
  let callerId = "";
  let calledId = "";
  
  let lastCallerText = "";
  let fullTranscript = [];

  const openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" }
  });

  const safeSendOpenAI = (obj) => {
    if (openaiWs.readyState === WebSocket.OPEN) openaiWs.send(JSON.stringify(obj));
  };

  const requestAssistantResponse = (reason = "") => {
    if (awaitingResponse) {
      pendingResponseRequest = true;
      return;
    }
    awaitingResponse = true;
    pendingResponseRequest = false;
    safeSendOpenAI({ type: "response.create", response: { modalities: ["audio", "text"] } });
  };

  openaiWs.on("open", async () => {
    debug(`[${connTag}] OpenAI Connected`);
    openaiReady = true;
    if (!SHEETS.loaded_at) await loadSheets();

    const masterPrompt = getPrompt("MASTER_PROMPT", "את נטע מגיל ספורט. דברי קצר ומהיר.");
    const openingScript = getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");

    safeSendOpenAI({
      type: "session.update",
      session: {
        voice: OPENAI_VOICE,
        instructions: masterPrompt,
        input_audio_transcription: { model: MB_TRANSCRIPTION_MODEL },
        turn_detection: { type: "server_vad", threshold: MB_VAD_THRESHOLD, silence_duration_ms: MB_VAD_SILENCE_MS }
      }
    });

    // Opening Script (Verbatim)
    awaitingResponse = true;
    safeSendOpenAI({
      type: "response.create",
      response: {
        instructions: `תגידי עכשיו בדיוק: ${openingScript}. אל תוסיפי כלום.`,
        modalities: ["audio", "text"]
      }
    });

    sendWebhook({ event: "call_started", callSid: twilioStreamSid, caller: callerId });
  });

  openaiWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());

    if (msg.type === "response.audio_transcript.done") {
      const text = msg.transcript;
      fullTranscript.push(`Bot: ${text}`);
      always(`[BOT][${connTag}]`, text);
    }

    if (msg.type === "conversation.item.input_audio_transcription.completed") {
      const text = msg.transcript?.trim();
      if (text) {
        lastCallerText = text;
        fullTranscript.push(`User: ${text}`);
        always(`[CALLER][${connTag}]`, text);
        
        // Proxy Decision Layer: Inject sheet knowledge based on caller text
        const extraContext = getContextKnowledge(text);
        if (extraContext) {
          safeSendOpenAI({
            type: "conversation.item.create",
            item: { type: "message", role: "system", content: [{ type: "input_text", text: extraContext }] }
          });
        }
      }
    }

    if (msg.type === "input_audio_buffer.speech_stopped") {
      requestAssistantResponse("speech_stopped");
    }

    if (msg.type === "response.done") {
      awaitingResponse = false;
      callAbandoned = false; // Successfully completed at least one turn
      if (pendingResponseRequest) {
        pendingResponseRequest = false;
        requestAssistantResponse("pending_after_done");
      }
    }

    if (msg.type === "response.audio.delta" && twilioStreamSid) {
      twilioWs.send(JSON.stringify({ event: "media", streamSid: twilioStreamSid, media: { payload: msg.delta } }));
    }
  });

  twilioWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.event === "start") {
      twilioStreamSid = msg.start.streamSid;
      callerId = msg.start.customParameters?.caller || "";
      calledId = msg.start.customParameters?.called || "";
      always(`[TWILIO_START][${connTag}]`, twilioStreamSid);
    }
    if (msg.event === "media" && openaiReady) {
      safeSendOpenAI({ type: "input_audio_buffer.append", audio: msg.media.payload });
    }
    if (msg.event === "stop") {
      openaiWs.close();
    }
  });

  const cleanup = () => {
    if (callAbandoned) {
      sendWebhook({
        event: "call_abandoned",
        callSid: twilioStreamSid,
        caller: callerId,
        caller_last_utterance: lastCallerText,
        transcript: fullTranscript.join("\n"),
        recording_url_public: `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/${twilioStreamSid}` 
      });
    } else {
      sendWebhook({
        event: "call_ended",
        callSid: twilioStreamSid,
        caller: callerId,
        caller_last_utterance: lastCallerText,
        transcript: fullTranscript.join("\n"),
        recording_url_public: `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/${twilioStreamSid}`
      });
    }
  };

  twilioWs.on("close", cleanup);
  openaiWs.on("close", () => twilioWs.close());
  openaiWs.on("error", (e) => error("OpenAI Error", e.message));
});

// --------------------------------------------------
// Health & Diagnostic Endpoints
// --------------------------------------------------
app.get("/health", (req, res) => res.json({ ok: true, sheets_loaded: SHEETS.loaded_at }));
app.post("/sheets/reload", async (req, res) => {
  await loadSheets();
  res.json({ ok: true, at: SHEETS.loaded_at });
});

server.listen(PORT, () => {
  log(`Neta VoiceBot running on port ${PORT}`);
  loadSheets();
});
