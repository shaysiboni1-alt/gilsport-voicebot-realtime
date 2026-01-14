// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const crypto = require("crypto");

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

// Voice normalize to lowercase + validate
const ALLOWED_VOICES = new Set([
  "alloy",
  "ash",
  "ballad",
  "coral",
  "echo",
  "sage",
  "shimmer",
  "verse",
  "marin",
  "cedar"
]);
function normalizeVoice(v) {
  const raw = String(v || "").trim();
  const lower = raw.toLowerCase();
  if (ALLOWED_VOICES.has(lower)) return lower;
  return "alloy";
}
const OPENAI_VOICE = normalizeVoice(process.env.OPENAI_VOICE || "alloy");

// Optional voice style and speaking rate controls. These environment variables
// allow operators to adjust the tone and speed of the synthetic voice without
// changing code. If OPENAI_VOICE_STYLE is set, it will be passed to the
// realtime API as the voice style. OPENAI_SPEAKING_RATE should be a number
// (e.g. 1.0 for normal speed, 0.9 for slower, 1.1 for faster). If not set,
// defaults are used. Note: the realtime API may ignore unknown styles or
// unsupported rates.
const OPENAI_VOICE_STYLE = process.env.OPENAI_VOICE_STYLE || "";
const OPENAI_SPEAKING_RATE = (() => {
  const rate = parseFloat(process.env.OPENAI_SPEAKING_RATE);
  return Number.isFinite(rate) && rate > 0 ? rate : 1.0;
})();

// Base style override. Operators can define MB_BASE_STYLE in the
// environment to change the overall tone/phrasing of the assistant (e.g.
// "טון מקצועי ורשמי" או "טון קליל ומזמין"). This will be appended to the
// dynamic instructions for every turn.
const MB_BASE_STYLE = process.env.MB_BASE_STYLE || "";

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const MB_WEBHOOK_URL = process.env.MB_WEBHOOK_URL || "";
const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);
const MB_RECORDING_WAIT_MS = envNum("MB_RECORDING_WAIT_MS", 8000);
const MB_DEBUG = envBool("MB_DEBUG", false);

const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);

const MB_IDLE_WARNING_MS = envNum("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNum("MB_IDLE_HANGUP_MS", 90000);

const MB_MAX_CALL_MS = envNum("MB_MAX_CALL_MS", 5 * 60 * 1000);

// Transcript/logging flags
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";
const MB_LOG_RAW_OPENAI = envBool("MB_LOG_RAW_OPENAI", false);
// Public recording proxy (optional)
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || "";
const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";

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
// Webhook (single endpoint) helpers
// --------------------------------------------------

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function twilioHasRecording(callSid) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return false;
  if (!callSid) return false;
  try {
    const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings.json?CallSid=${encodeURIComponent(
      callSid
    )}&PageSize=1`;
    const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString(
      "base64"
    );
    const resp = await fetch(listUrl, { headers: { Authorization: `Basic ${auth}` } });
    if (!resp.ok) return false;
    const data = await resp.json();
    return Array.isArray(data.recordings) && data.recordings.length > 0;
  } catch (e) {
    return false;
  }
}

async function waitForRecording(callSid, waitMs) {
  const deadline = Date.now() + Math.max(0, Number(waitMs) || 0);
  // Quick path
  if (await twilioHasRecording(callSid)) return true;
  // Poll (1s)
  while (Date.now() < deadline) {
    await sleep(1000);
    if (await twilioHasRecording(callSid)) return true;
  }
  return false;
}

const nowIso = () => new Date().toISOString();

async function sendWebhookEvent(event, payload, opts = {}) {
  if (!MB_WEBHOOK_URL) return false;
  try {
    const callSid = payload && payload.callSid ? String(payload.callSid) : "";
    // If caller requires recording link, wait a bit for Twilio to generate it.
    if (callSid && (opts.wait_for_recording || opts.waitForRecording)) {
      await waitForRecording(callSid, MB_RECORDING_WAIT_MS);
    }

    const recording_url_public =
      payload && Object.prototype.hasOwnProperty.call(payload, "recording_url_public")
        ? payload.recording_url_public
        : makeRecordingPublicUrl(callSid);

    const body = JSON.stringify({ event, ...payload, recording_url_public });
    const resp = await fetch(MB_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body
    });
    if (!resp.ok) {
      debug("Webhook non-200", event, resp.status);
    }
    return true;
  } catch (e) {
    debug("Webhook failed", event, e && e.message ? e.message : e);
    return false;
  }
}

function makeRecordingPublicUrl(callSid) {
  if (!PUBLIC_BASE_URL) return "";
  const base = String(PUBLIC_BASE_URL).replace(/\/$/, "");
  return callSid ? `${base}/recording/${callSid}` : "";
}

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
// Sheets (Single Source of Truth)
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {}, // PROMPTS: prompt_id -> content_he
  settings: {}, // SETTINGS: key -> value
  kbFacts: [], // KB_FACTS rows
  doNotSay: [], // DO_NOT_SAY rows
  suppliersImporters: [], // SUPPLIERS_IMPORTERS rows
  deliveryContacts: [], // DELIVERY_CONTACTS rows
  routingRules: [], // (legacy/compat)
  businessInfo: [] // (legacy/compat)
};

function parseTable(rows, keyColName, valColName) {
  const out = {};
  const headers = (rows.shift() || []).map((h) => String(h || "").trim());
  const keyIdx = headers.indexOf(keyColName);
  const valIdx = headers.indexOf(valColName);

  if (keyIdx === -1 || valIdx === -1) return out;

  for (const r of rows) {
    const k = String(r[keyIdx] || "").trim();
    const v = String(r[valIdx] || "");
    if (!k) continue;
    out[k] = v;
  }
  return out;
}

function rowsToObjects(rows) {
  const out = [];
  const headers = (rows.shift() || []).map((h) => String(h || "").trim());
  if (!headers.length) return out;
  for (const r of rows) {
    const o = {};
    headers.forEach((h, i) => {
      o[h] = r[i];
    });
    out.push(o);
  }
  return out;
}

function parseArray(rows, keyColName, valColName) {
  const out = [];
  const headers = (rows.shift() || []).map((h) => String(h || "").trim());
  const keyIdx = headers.indexOf(keyColName);
  const valIdx = headers.indexOf(valColName);
  if (keyIdx === -1 || valIdx === -1) return out;

  for (const r of rows) {
    const k = String(r[keyIdx] || "").trim();
    const v = String(r[valIdx] || "");
    if (!k && !v) continue;
    out.push({ key: k, value: v });
  }
  return out;
}

async function loadSheets() {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) {
    error("Sheets env missing", { GSHEET_ID: !!GSHEET_ID, GOOGLE_SERVICE_ACCOUNT_JSON_B64: !!GOOGLE_SERVICE_ACCOUNT_JSON_B64 });
    return;
  }

  const credsJson = Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8");
  const creds = JSON.parse(credsJson);

  const auth = new google.auth.GoogleAuth({
    credentials: creds,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  });

  const sheets = google.sheets({ version: "v4", auth });

  const range = [
    "PROMPTS!A:B",
    "SETTINGS!A:B",
    "KB_FACTS!A:B",
    "DO_NOT_SAY!A:B",
    "SUPPLIERS_IMPORTERS!A:B",
    "DELIVERY_CONTACTS!A:D",
    "ROUTING_RULES!A:D",
    "BUSINESS_INFO!A:B"
  ];

  const resp = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: GSHEET_ID,
    ranges: range,
    majorDimension: "ROWS"
  });

  const [prompts, settings, kbFacts, doNotSay, suppliersImporters, deliveryContacts, routingRules, businessInfo] =
    resp.data.valueRanges.map((r) => r.values || []);

  SHEETS = {
    loaded_at: new Date().toISOString(),
    prompts: parseTable(prompts, "prompt_id", "content_he"),
    settings: parseTable(settings, "key", "value"),
    kbFacts: rowsToObjects(kbFacts),
    doNotSay: parseArray(doNotSay, "key", "value"),
    suppliersImporters: rowsToObjects(suppliersImporters),
    deliveryContacts: rowsToObjects(deliveryContacts),
    routingRules: rowsToObjects(routingRules),
    businessInfo: rowsToObjects(businessInfo)
  };

  log(
    `Sheets loaded (prompts=${Object.keys(SHEETS.prompts).length}, settings=${Object.keys(
      SHEETS.settings
    ).length}, kbFacts=${SHEETS.kbFacts.length}, doNotSay=${SHEETS.doNotSay.length}, suppliersImporters=${SHEETS.suppliersImporters.length}, deliveryContacts=${SHEETS.deliveryContacts.length})`
  );
}

function getPrompt(id, fallback = "") {
  return (SHEETS.prompts && SHEETS.prompts[id]) || fallback;
}

function getSetting(key, fallback = "") {
  return (SHEETS.settings && SHEETS.settings[key]) || fallback;
}

// --------------------------------------------------
// Text utils (Hebrew normalization)
// --------------------------------------------------
const normalizeText = (s) => {
  return String(s || "")
    .replace(/\s+/g, " ")
    .replace(/[“”]/g, '"')
    .replace(/[’‘]/g, "'")
    .trim();
};

// --------------------------------------------------
// Call-level memory (simple, in-memory, TTL)
// --------------------------------------------------
const memory = new Map();

function getMemory(callSid) {
  const entry = memory.get(callSid);
  if (!entry) return null;
  if (Date.now() > entry.expiresAt) {
    memory.delete(callSid);
    return null;
  }
  return entry.data;
}

function setMemory(callSid, data, ttlMinutes = 30) {
  memory.set(callSid, {
    data,
    expiresAt: Date.now() + ttlMinutes * 60 * 1000
  });
}

// --------------------------------------------------
// Assistants logic (prompt building)
// --------------------------------------------------
function buildSystemPrompt(masterPrompt) {
  return normalizeText(masterPrompt);
}

function buildBaseStyleInstructions() {
  if (!MB_BASE_STYLE) return "";
  return `\nסגנון בסיס:\n${MB_BASE_STYLE}`;
}

function buildKnowledgeBase() {
  const lines = [];

  const kbFacts = (SHEETS.kbFacts || []).map((r) => `${r.question || ""}: ${r.answer || ""}`);
  if (kbFacts.length) {
    lines.push("ידע כללי:");
    lines.push(...kbFacts);
  }

  const doNotSay = (SHEETS.doNotSay || []).map((r) => `❌ ${r.key} — ${r.value}`);
  if (doNotSay.length) {
    lines.push("לא לומר:");
    lines.push(...doNotSay);
  }

  const suppliers = (SHEETS.suppliersImporters || []).map((r) => `• ${r.supplier || ""} — ${r.contact || ""}`);
  if (suppliers.length) {
    lines.push("ספקים/יבואנים:");
    lines.push(...suppliers);
  }

  const deliveryContacts = (SHEETS.deliveryContacts || []).map((r) => {
    const name = r.name || "";
    const phone = r.phone || "";
    const area = r.area || "";
    return `• ${name} — ${phone} — ${area}`;
  });
  if (deliveryContacts.length) {
    lines.push("שליחים/הובלה:");
    lines.push(...deliveryContacts);
  }

  const businessInfo = (SHEETS.businessInfo || []).map((r) => `${r.key || ""}: ${r.value || ""}`);
  if (businessInfo.length) {
    lines.push("מידע עסקי:");
    lines.push(...businessInfo);
  }

  if (!lines.length) return "";
  return `\nמידע לבוט:\n${lines.join("\n")}`;
}

function buildNextInstructions() {
  const masterPrompt = getPrompt(
    "MASTER_PROMPT",
    "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
  );

  const promptParts = [
    buildSystemPrompt(masterPrompt),
    buildBaseStyleInstructions(),
    buildKnowledgeBase()
  ];

  return normalizeText(promptParts.filter(Boolean).join("\n\n"));
}

// --------------------------------------------------
// Simple lead extraction
// --------------------------------------------------
const phoneRegex = /(\+972|0)\s?(\d)([\s-]?\d){7,9}/g;

function extractLeadFromText(text) {
  const leads = [];

  const matches = String(text || "").match(phoneRegex) || [];
  for (const m of matches) {
    const digits = m.replace(/\D/g, "");
    if (digits.length >= 9) {
      leads.push({ type: "phone", value: digits });
    }
  }

  return leads;
}

// --------------------------------------------------
// Express setup
// --------------------------------------------------
const app = express();
app.use(express.urlencoded({ extended: true }));
app.use(express.json({ limit: "5mb" }));

app.get("/", (_req, res) => {
  res.json({
    status: "ok",
    time: nowIso(),
    sheets_loaded_at: SHEETS.loaded_at,
    ws_connections: RUNTIME.ws_connections,
    ws_closed: RUNTIME.ws_closed,
    ws_errors: RUNTIME.ws_errors,
    openai_errors: RUNTIME.openai_errors,
    openai_closed: RUNTIME.openai_closed,
    last_ws_conn_at: RUNTIME.last_ws_conn_at,
    last_ws_close_at: RUNTIME.last_ws_close_at
  });
});

app.post("/healthz", (_req, res) => {
  res.json({ ok: true, ts: nowIso() });
});

app.post("/twilio-voice", (req, res) => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Start>
    <Stream url="wss://${req.headers.host}/twilio-media-stream" />
  </Start>
  <Say voice="Polly.Natalie">Connecting you to Neta.</Say>
  <Pause length="60" />
</Response>`;
  res.type("text/xml").send(xml);
});

app.get("/recording/:callSid", async (req, res) => {
  const callSid = req.params.callSid || "";
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) {
    return res.status(500).json({ error: "Twilio auth not configured" });
  }
  if (!callSid) {
    return res.status(400).json({ error: "Missing callSid" });
  }

  const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings.json?CallSid=${encodeURIComponent(
    callSid
  )}&PageSize=1`;
  const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");

  try {
    const resp = await fetch(listUrl, {
      headers: {
        Authorization: `Basic ${auth}`
      }
    });

    if (!resp.ok) {
      return res.status(resp.status).json({ error: "Twilio error" });
    }

    const data = await resp.json();
    const rec = data.recordings && data.recordings[0];

    if (!rec || !rec.media_url) {
      return res.status(404).json({ error: "Recording not found" });
    }

    res.redirect(`${rec.media_url}.mp3`);
  } catch (e) {
    return res.status(500).json({ error: "Failed to fetch recording" });
  }
});

// --------------------------------------------------
// HTTP server
// --------------------------------------------------
const server = http.createServer(app);

// --------------------------------------------------
// WebSocket: Twilio Media Streams
// --------------------------------------------------
const wss = new WebSocket.Server({ server });

wss.on("connection", (twilioWs, req) => {
  RUNTIME.ws_connections += 1;
  RUNTIME.last_ws_conn_at = nowIso();

  const connId = crypto.randomUUID().slice(0, 8);
  const connTag = `conn_${connId}`;

  always("WS connection", {
    at: nowIso(),
    ip: req.socket.remoteAddress,
    ua: req.headers["user-agent"],
    url: req.url,
    total_ws_connections: RUNTIME.ws_connections
  });

  let openaiWs = null;
  let openaiReady = false;
  let twilioStreamSid = null;
  let callSid = null;

  let pendingAudio = [];
  let awaitingResponse = false;
  let pendingResponseRequest = false;
  let lastUserAudioAt = Date.now();
  let lastBotAudioAt = Date.now();
  let totalMs = 0;
  let idleWarningSent = false;
  let maxCallWarningSent = false;
  let callEnded = false;

  // One-time per connection
  const reportOnce = new Set();

  // --------------------------------------------------
  // Twilio -> OpenAI
  // --------------------------------------------------

  const safeOpenAISend = (payload) => {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
    openaiWs.send(JSON.stringify(payload));
  };

  const closeBoth = () => {
    try {
      if (twilioWs && twilioWs.readyState === WebSocket.OPEN) twilioWs.close();
    } catch (_) {}
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) openaiWs.close();
    } catch (_) {}
  };

  const markCallEnded = () => {
    if (callEnded) return;
    callEnded = true;
    sendWebhookEvent("call_end", {
      callSid,
      ended_at: nowIso(),
      ws_connection_id: connTag
    });
  };

  const clearIfShouldHangup = () => {
    if (!callSid) return;
    if (totalMs > MB_MAX_CALL_MS && !maxCallWarningSent) {
      maxCallWarningSent = true;
      const msg = "הגעתם לזמן המקסימלי של השיחה. ניתוק בקרוב.";
      safeOpenAISend({
        type: "response.create",
        response: { modalities: ["audio", "text"], instructions: msg }
      });
    }

    if (totalMs > MB_MAX_CALL_MS + MB_IDLE_HANGUP_MS) {
      markCallEnded();
      closeBoth();
    }
  };

  const handleTwilioMessage = (message) => {
    let data;
    try {
      data = JSON.parse(message);
    } catch {
      return;
    }

    if (data.event === "start") {
      twilioStreamSid = data.start && data.start.streamSid;
      callSid = data.start && data.start.callSid;

      always(`[TWILIO_START][${connTag}]`, JSON.stringify(data.start || {}));

      // Call start webhook
      sendWebhookEvent("call_start", {
        callSid,
        started_at: nowIso(),
        ws_connection_id: connTag
      });
    }

    if (data.event === "media") {
      lastUserAudioAt = Date.now();
      if (!openaiReady) {
        pendingAudio.push(data.media.payload);
      } else {
        safeOpenAISend({
          type: "input_audio_buffer.append",
          audio: data.media.payload
        });
      }
    }

    if (data.event === "stop") {
      always(`[TWILIO_STOP][${connTag}] stream stopped`);
      markCallEnded();
      closeBoth();
    }
  };

  twilioWs.on("message", handleTwilioMessage);

  twilioWs.on("error", (e) => {
    RUNTIME.ws_errors += 1;
    error("Twilio WS error", e?.message || e);
    closeBoth();
  });

  twilioWs.on("close", () => {
    RUNTIME.ws_closed += 1;
    always(`[TWILIO_CLOSE][${connTag}] socket closed`);
    closeBoth();
  });

  // --------------------------------------------------
  // OpenAI WebSocket
  // --------------------------------------------------

  const sendAssistantResponse = (reason = "turn") => {
    if (awaitingResponse) return;
    awaitingResponse = true;

    let instructions = buildNextInstructions();

    // Optional use of OPENAI_VOICE_STYLE on first response
    if (OPENAI_VOICE_STYLE) {
      instructions = `${instructions}\n\nסגנון קול: ${OPENAI_VOICE_STYLE}`;
    }

    if (MB_BASE_STYLE) {
      instructions = `${instructions}\n\n${MB_BASE_STYLE}`;
    }

    const tone = getSetting("TONE", "");
    if (tone) {
      instructions = `${instructions}\n\n${tone}`;
    }

    if (getSetting("ASK_ONE_QUESTION_ONLY", "false") === "true") {
      instructions = `${instructions}\n\nתשאלי שאלה אחת בלבד בסוף התשובה.`;
    }

    if (getSetting("SHORT_ANSWERS", "false") === "true") {
      instructions = `${instructions}\n\nהתשובות צריכות להיות קצרות (עד 1-2 משפטים).`;
    }

    try {
      instructions = buildNextInstructions();
    } catch (_) {
      instructions = getPrompt(
        "MASTER_PROMPT",
        "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
      );
    }

    debug(`[${connTag}] response.create (reason=${reason})`);
    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions
      }
    });
  };

  debug(`[${connTag}] Creating OpenAI WS... model=${OPENAI_REALTIME_MODEL} voice=${OPENAI_VOICE}`);

  openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1"
      }
    }
  );

  openaiWs.on("open", async () => {
    debug(`[${connTag}] OpenAI connected`);
    openaiReady = true;

    // Ensure sheets loaded before config
    if (!SHEETS.loaded_at) {
      debug(`[${connTag}] Sheets not loaded yet. Loading now...`);
      await loadSheets();
    }

    // ✅ Master prompt stays in PROMPTS
    const masterPrompt = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
    );

    // ✅ Opening script comes from SETTINGS
    const openingScript = getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");

    always(`[${connTag}] SOURCES`, {
      sheets_loaded_at: SHEETS.loaded_at,
      opening_from: "SETTINGS.OPENING_SCRIPT",
      master_from: "PROMPTS.MASTER_PROMPT",
      opening_preview: preview(openingScript, 220),
      master_preview: preview(masterPrompt, 220)
    });

    // Prepare the realtime session settings. The API now expects voice.id,
    // so always send an object with id and optional style/rate overrides.
    const session = {
      modalities: ["audio", "text"],
      voice: {
        id: OPENAI_VOICE,
        ...(OPENAI_VOICE_STYLE ? { style: OPENAI_VOICE_STYLE } : {}),
        ...(OPENAI_SPEAKING_RATE !== 1.0 ? { rate: OPENAI_SPEAKING_RATE } : {})
      },
      input_audio_format: "g711_ulaw",
      output_audio_format: "g711_ulaw",
      turn_detection: {
        type: "server_vad",
        threshold: MB_VAD_THRESHOLD,
        silence_duration_ms: MB_VAD_SILENCE_MS,
        prefix_padding_ms: MB_VAD_PREFIX_MS
      },
      instructions: masterPrompt
    };

    // ✅ enable caller transcription
    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    safeOpenAISend({ type: "session.update", session });

    // ✅ Make the bot SAY the opening verbatim (one-time override)
    awaitingResponse = true;
    pendingResponseRequest = false;

    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions:
          `תגידי עכשיו בדיוק את המשפט הבא מילה במילה, ללא תוספות וללא שאלות:\n` +
          `${openingScript}`
      }
    });

    // Flush buffered audio
    while (pendingAudio.length > 0 && openaiWs && openaiWs.readyState === WebSocket.OPEN) {
      const audio = pendingAudio.shift();
      safeOpenAISend({ type: "input_audio_buffer.append", audio });
    }
  });

  openaiWs.on("error", (e) => {
    RUNTIME.openai_errors += 1;
    error(`[${connTag}] OpenAI websocket error`, e?.message || e);
    try {
      twilioWs.close();
    } catch (_) {}
  });

  openaiWs.on("close", () => {
    RUNTIME.openai_closed += 1;
    debug(`[${connTag}] OpenAI closed`);
    try {
      twilioWs.close();
    } catch (_) {}
  });

  openaiWs.on("message", (msg) => {
    let data;
    try {
      data = JSON.parse(msg);
    } catch {
      return;
    }

    if (MB_LOG_RAW_OPENAI) {
      debug(`[${connTag}] OpenAI raw`, data);
    }

    if (data.type === "error") {
      RUNTIME.openai_errors += 1;
      error(`[${connTag}] OpenAI error event`, data);
      return;
    }

    if (data.type === "session.updated") {
      debug(`[${connTag}] session.updated`);
    }

    if (data.type === "response.created") {
      debug(`[${connTag}] response.created`);
    }

    if (data.type === "response.output_item.added") {
      debug(`[${connTag}] response.output_item.added`, data.item?.type);
    }

    if (data.type === "response.output_item.done") {
      debug(`[${connTag}] response.output_item.done`, data.item?.type);
    }

    if (data.type === "response.content_part.added") {
      debug(`[${connTag}] response.content_part.added`, data.part?.type);
    }

    if (data.type === "response.content_part.done") {
      if (data.part?.type === "audio") {
        debug(`[${connTag}] response.content_part.done audio`);
      }
      if (data.part?.type === "text") {
        const text = data.part?.text || "";
        if (text) {
          always(`[BOT][${connTag}]`, text);
        }
      }
    }

    if (data.type === "response.audio.delta") {
      const audio = data.delta;
      if (audio) {
        twilioWs.send(
          JSON.stringify({
            event: "media",
            streamSid: twilioStreamSid,
            media: { payload: audio }
          })
        );
        lastBotAudioAt = Date.now();
      }
    }

    if (data.type === "response.audio.done") {
      awaitingResponse = false;
      pendingResponseRequest = false;
      lastBotAudioAt = Date.now();
    }

    if (data.type === "response.done") {
      awaitingResponse = false;
      pendingResponseRequest = false;

      const output = data.response?.output || [];
      const content = output
        .flatMap((o) => o.content || [])
        .map((c) => c.text || "")
        .filter(Boolean)
        .join(" ");

      if (content) {
        if (MB_LOG_TRANSCRIPTS) {
          always(`[BOT][${connTag}]`, content);
        }

        const leads = extractLeadFromText(content);
        if (leads.length) {
          sendWebhookEvent("lead_detected", {
            callSid,
            at: nowIso(),
            leads,
            source: "assistant",
            text: content
          });
        }
      }

      // Allow next
      pendingResponseRequest = false;
    }

    if (data.type === "input_audio_buffer.speech_started") {
      lastUserAudioAt = Date.now();
      if (awaitingResponse) {
        debug(`[${connTag}] user speech while bot speaking`);
      }
    }

    if (data.type === "input_audio_buffer.speech_stopped") {
      lastUserAudioAt = Date.now();
    }

    if (data.type === "input_audio_buffer.committed") {
      lastUserAudioAt = Date.now();
      if (!awaitingResponse && !pendingResponseRequest) {
        pendingResponseRequest = true;
        sendAssistantResponse("speech_end");
      }
    }

    if (data.type === "conversation.item.input_audio_transcription.completed") {
      const text = data.transcript || "";
      if (text) {
        if (MB_LOG_TRANSCRIPTS) {
          always(`[USER][${connTag}]`, text);
        }

        const leads = extractLeadFromText(text);
        if (leads.length) {
          sendWebhookEvent("lead_detected", {
            callSid,
            at: nowIso(),
            leads,
            source: "caller",
            text
          });
        }
      }
    }
  });

  // --------------------------------------------------
  // Timers / guardrails
  // --------------------------------------------------
  const interval = setInterval(() => {
    totalMs += 1000;
    clearIfShouldHangup();

    // Idle handling
    const now = Date.now();
    const idleMs = now - Math.max(lastUserAudioAt, lastBotAudioAt);

    if (!idleWarningSent && idleMs > MB_IDLE_WARNING_MS) {
      idleWarningSent = true;
      const msg = "האם תרצו עזרה נוספת?";
      safeOpenAISend({
        type: "response.create",
        response: { modalities: ["audio", "text"], instructions: msg }
      });
    }

    if (idleMs > MB_IDLE_HANGUP_MS) {
      markCallEnded();
      closeBoth();
    }
  }, 1000);

  twilioWs.on("close", () => {
    clearInterval(interval);
    markCallEnded();
  });

  openaiWs.on("close", () => {
    clearInterval(interval);
    markCallEnded();
  });
});

server.listen(PORT, async () => {
  await loadSheets();

  log(`GilSport VoiceBot running on port ${PORT}`);
  always("BOOT", {
    at: nowIso(),
    port: PORT,
    MB_DEBUG,
    has_OPENAI_API_KEY: !!OPENAI_API_KEY,
    OPENAI_REALTIME_MODEL,
    OPENAI_VOICE,
    has_GSHEET_ID: !!GSHEET_ID,
    has_GOOGLE_SERVICE_ACCOUNT_JSON_B64: !!GOOGLE_SERVICE_ACCOUNT_JSON_B64,
    has_TWILIO_ACCOUNT_SID: !!TWILIO_ACCOUNT_SID,
    has_TWILIO_AUTH_TOKEN: !!TWILIO_AUTH_TOKEN,
    PUBLIC_BASE_URL,
    TIME_ZONE,
    MB_LOG_TRANSCRIPTS,
    MB_ENABLE_TRANSCRIPTION,
    MB_TRANSCRIPTION_MODEL,
    MB_LOG_RAW_OPENAI
  });
});
