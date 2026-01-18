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

// Optional voice style and speaking rate controls.
const OPENAI_VOICE_STYLE = process.env.OPENAI_VOICE_STYLE || "";
const OPENAI_SPEAKING_RATE = (() => {
  const rate = parseFloat(process.env.OPENAI_SPEAKING_RATE);
  return Number.isFinite(rate) && rate > 0 ? rate : 1.0;
})();

// Base style override.
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

// IMPORTANT: keep minimal fallbacks for safety only if Sheets is empty/unavailable.
// The operational expectation is that all speech text exists in Sheets.
const FALLBACK_EMPTY_INSTRUCTIONS = "סליחה, לא הבנתי. תוכלו לחזור בבקשה?";
const FALLBACK_ROUTING_CLARIFY =
  "כדי לעזור במדויק—זה לגבי התעניינות במוצר, שירות/תקלה/אחריות, משלוח/אספקה, או להשאיר הודעה למישהו מהצוות?";
const FALLBACK_PHONE_MISSING_DIGIT = "נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?";
const FALLBACK_NAME_INVALID = "אפשר בבקשה שם מלא?";
let warnedSheetsEmpty = false;

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
    const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
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
  if (await twilioHasRecording(callSid)) return true;
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

async function completeTwilioCall(callSid) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return false;
  if (!callSid) return false;
  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${encodeURIComponent(
      callSid
    )}.json`;
    const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
    const body = new URLSearchParams({ Status: "completed" });
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Basic ${auth}`,
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body
    });
    return resp.ok;
  } catch (_) {
    return false;
  }
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
  kbFacts: [], // KB_FACTS rows (objects)
  doNotSay: [], // DO_NOT_SAY rows (objects)
  suppliersImporters: [], // SUPPLIERS_IMPORTERS rows (objects)
  deliveryContacts: [], // DELIVERY_CONTACTS rows (objects)
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
    headers.forEach((h, i) => (o[h] = r[i] || ""));
    const hasAny = Object.values(o).some((v) => String(v || "").trim() !== "");
    if (hasAny) out.push(o);
  }
  return out;
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
      ranges: [
        "PROMPTS!A:Z",
        "SETTINGS!A:Z",
        "KB_FACTS!A:Z",
        "DO_NOT_SAY!A:Z",
        "SUPPLIERS_IMPORTERS!A:Z",
        "DELIVERY_CONTACTS!A:Z"
      ]
    });

    const valueRanges = res.data.valueRanges || [];
    const promptsRange = valueRanges.find((vr) => (vr.range || "").startsWith("PROMPTS!"));
    const settingsRange = valueRanges.find((vr) => (vr.range || "").startsWith("SETTINGS!"));

    const promptsRows = (promptsRange?.values || []).slice();
    const settingsRows = (settingsRange?.values || []).slice();

    const kbFactsRange = valueRanges.find((vr) => (vr.range || "").startsWith("KB_FACTS!"));
    const doNotSayRange = valueRanges.find((vr) => (vr.range || "").startsWith("DO_NOT_SAY!"));
    const suppliersImportersRange = valueRanges.find((vr) =>
      (vr.range || "").startsWith("SUPPLIERS_IMPORTERS!")
    );
    const deliveryContactsRange = valueRanges.find((vr) => (vr.range || "").startsWith("DELIVERY_CONTACTS!"));

    const kbFactsRows = rowsToObjects((kbFactsRange?.values || []).slice());
    const doNotSayRows = rowsToObjects((doNotSayRange?.values || []).slice());
    const suppliersImportersRows = rowsToObjects((suppliersImportersRange?.values || []).slice());
    const deliveryContactsRows = rowsToObjects((deliveryContactsRange?.values || []).slice());

    // PROMPTS: expects columns prompt_id + content_he
    const prompts = {};
    if (promptsRows.length) {
      const headers = promptsRows.shift() || [];
      for (const r of promptsRows) {
        const row = {};
        headers.forEach((h, i) => (row[h] = r[i] || ""));
        if (row.prompt_id && row.content_he) {
          prompts[String(row.prompt_id).trim()] = String(row.content_he);
        }
      }
    }

    // SETTINGS: expects columns key + value
    const settings = settingsRows.length ? parseTable(settingsRows, "key", "value") : {};

    SHEETS = {
      loaded_at: new Date().toISOString(),
      prompts,
      settings,
      kbFacts: kbFactsRows,
      doNotSay: doNotSayRows,
      suppliersImporters: suppliersImportersRows,
      deliveryContacts: deliveryContactsRows,
      routingRules: [],
      businessInfo: []
    };

    log(
      `Sheets loaded (prompts=${Object.keys(prompts).length}, settings=${Object.keys(settings).length}, kbFacts=${
        kbFactsRows.length
      }, doNotSay=${doNotSayRows.length}, suppliersImporters=${suppliersImportersRows.length}, deliveryContacts=${
        deliveryContactsRows.length
      })`
    );
  } catch (e) {
    error("Sheets load failed", e.message);
  }
}

const getPrompt = (id, fallback = "") => String(SHEETS.prompts[id] || fallback).trim();
const getSetting = (key, fallback = "") => String(SHEETS.settings[key] || fallback).trim();

// --------------------------------------------------
// Express
// --------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.get("/health", (_, res) => {
  res.json({
    ok: true,
    sheets_loaded_at: SHEETS.loaded_at,
    prompts: Object.keys(SHEETS.prompts).length,
    settings: Object.keys(SHEETS.settings).length,
    kbFacts: (SHEETS.kbFacts || []).length,
    doNotSay: (SHEETS.doNotSay || []).length,
    suppliersImporters: (SHEETS.suppliersImporters || []).length,
    deliveryContacts: (SHEETS.deliveryContacts || []).length
  });
});

app.get("/diag/env", (_, res) => {
  res.json({
    ok: true,
    booted_at: RUNTIME.booted_at,
    has_OPENAI_API_KEY: Boolean(OPENAI_API_KEY),
    OPENAI_REALTIME_MODEL,
    OPENAI_VOICE,
    has_GSHEET_ID: Boolean(GSHEET_ID),
    has_GOOGLE_SERVICE_ACCOUNT_JSON_B64: Boolean(GOOGLE_SERVICE_ACCOUNT_JSON_B64),
    has_TWILIO_ACCOUNT_SID: Boolean(TWILIO_ACCOUNT_SID),
    has_TWILIO_AUTH_TOKEN: Boolean(TWILIO_AUTH_TOKEN),
    PUBLIC_BASE_URL,
    TIME_ZONE,
    MB_DEBUG,
    MB_LOG_TRANSCRIPTS,
    MB_ENABLE_TRANSCRIPTION,
    MB_TRANSCRIPTION_MODEL,
    MB_LOG_RAW_OPENAI,
    sheets_loaded_at: SHEETS.loaded_at,
    prompts_count: Object.keys(SHEETS.prompts).length,
    settings_count: Object.keys(SHEETS.settings).length
  });
});

app.get("/diag/prompts", (_, res) => {
  const keys = Object.keys(SHEETS.prompts).sort();
  const sKeys = Object.keys(SHEETS.settings).sort();
  res.json({
    ok: true,
    sheets_loaded_at: SHEETS.loaded_at,
    prompts_count: keys.length,
    settings_count: sKeys.length,
    prompt_ids: keys,
    setting_keys: sKeys,
    opening_from_settings_preview: preview(getSetting("OPENING_SCRIPT", "")),
    master_from_prompts_preview: preview(getPrompt("MASTER_PROMPT", "")),
    do_not_say_rows: (SHEETS.doNotSay || []).length
  });
});

app.get("/diag/runtime", (_, res) => {
  res.json({ ok: true, ...RUNTIME });
});

app.get("/diag/sheets", (_, res) => {
  res.json({
    ok: true,
    sheets_loaded_at: SHEETS.loaded_at,
    counts: {
      prompts: Object.keys(SHEETS.prompts || {}).length,
      settings: Object.keys(SHEETS.settings || {}).length,
      kbFacts: (SHEETS.kbFacts || []).length,
      doNotSay: (SHEETS.doNotSay || []).length,
      suppliersImporters: (SHEETS.suppliersImporters || []).length,
      deliveryContacts: (SHEETS.deliveryContacts || []).length
    }
  });
});

// Public recording proxy (optional). If Twilio creds missing -> 404.
app.get("/recording/:callSid", async (req, res) => {
  try {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return res.status(404).send("recording proxy disabled");
    const callSid = String(req.params.callSid || "").trim();
    if (!callSid) return res.status(400).send("missing callSid");

    const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings.json?CallSid=${encodeURIComponent(
      callSid
    )}&PageSize=1`;

    const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
    const listResp = await fetch(listUrl, { headers: { Authorization: `Basic ${auth}` } });
    if (!listResp.ok) return res.status(404).send("no recording found for callSid");
    const listJson = await listResp.json();
    const rec = (listJson.recordings || [])[0];
    if (!rec || !rec.sid) return res.status(404).send("no recording found for callSid");

    const mediaUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${rec.sid}.mp3`;
    const mediaResp = await fetch(mediaUrl, { headers: { Authorization: `Basic ${auth}` } });
    if (!mediaResp.ok) return res.status(404).send("no recording found for callSid");

    res.setHeader("Content-Type", "audio/mpeg");
    res.setHeader("Cache-Control", "no-store");
    const buf = Buffer.from(await mediaResp.arrayBuffer());
    res.status(200).send(buf);
  } catch (e) {
    error("recording proxy failed", e.message);
    res.status(500).send("recording proxy error");
  }
});

app.post("/sheets/reload", async (_, res) => {
  await loadSheets();
  res.json({ ok: true, reloaded: true, at: SHEETS.loaded_at });
});

// Twilio Voice → Media Stream
app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  const wsUrl = `wss://${host}/twilio-media-stream`;

  res.type("text/xml").send(
    `
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
    </Stream>
  </Connect>
</Response>
`.trim()
  );
});

const server = http.createServer(app);

// --------------------------------------------------
// WebSocket (Twilio <-> OpenAI)
// --------------------------------------------------
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs, req) => {
  RUNTIME.ws_connections += 1;
  RUNTIME.last_ws_conn_at = new Date().toISOString();

  always("WS connection", {
    at: RUNTIME.last_ws_conn_at,
    ip: req?.socket?.remoteAddress || "?",
    ua: req?.headers?.["user-agent"] || "?",
    url: req?.url || "/twilio-media-stream",
    total_ws_connections: RUNTIME.ws_connections
  });

  let twilioStreamSid = null;
  let openaiReady = false;
  const pendingAudio = [];

  const connTag = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;

  // Stream parameters
  let caller = "";
  let called = "";

  try {
    const u = new URL(req.url || "", "http://localhost");
    caller = u.searchParams.get("caller") || "";
    called = u.searchParams.get("called") || "";
  } catch (_) {}

  let lastCallerFinal = "";
  let lastBotFinal = "";
  let lastRequestedCallerFinal = "";

  // Call/session state
  let callSid = null;
  let startedAt = nowIso();
  let endedAt = null;
  let route = "other";
  let language = getSetting("DEFAULT_LANGUAGE", "he") || "he";

  let transcriptTurns = []; // {from, text, at}

  // Abandoned / ended dedupe guards
  let sentCallEnded = false;
  let sentCallAbandoned = false;
  let hangupRequested = false;

  // Dynamic response instructions (no FSM, but deterministic stage tracking)
  let proxyInstructions = "";

  // Phones seen in call (dedupe)
  let recognizedPhones = [];

  // Buffer audio while assistant is speaking
  let pausedAudioBuffer = [];

  const pushTurn = (from, text) => {
    const t = String(text || "").trim();
    if (!t) return;
    transcriptTurns.push({ from, text: t, at: nowIso() });
    if (transcriptTurns.length > 400) transcriptTurns = transcriptTurns.slice(-400);
  };

  const formatSpacedDigits = (digits) => String(digits || "").split("").join(" ");

  const normalizePhoneDigits = (raw) => {
    let text = String(raw || "");
    const wordToDigit = {
      אפס: "0",
      אחת: "1",
      אחד: "1",
      שתיים: "2",
      שתים: "2",
      שניים: "2",
      שנים: "2",
      שלוש: "3",
      שלושה: "3",
      ארבע: "4",
      ארבעה: "4",
      חמש: "5",
      חמישה: "5",
      שש: "6",
      שישה: "6",
      שבע: "7",
      שבעה: "7",
      שמונה: "8",
      תשע: "9",
      תשעה: "9"
    };
    for (const [word, digit] of Object.entries(wordToDigit)) {
      text = text.replace(new RegExp(`\\b${word}\\b`, "g"), digit);
    }
    let digits = text.replace(/\D+/g, "");
    if (digits.startsWith("972") && digits.length > 3) digits = "0" + digits.slice(3);
    if (digits.startsWith("0") && digits.length > 10) digits = digits.slice(0, 10);
    return digits;
  };

  const isValidPhoneDigits = (digits) => {
    const d = String(digits || "").replace(/\D+/g, "");
    return d.length === 10 && d.startsWith("0");
  };

  const extractPhoneCandidates = (text) => {
    const normalized = normalizePhoneDigits(String(text || ""));
    return isValidPhoneDigits(normalized) ? normalized : "";
  };

  const isYes = (text) =>
    /(כן|כן כן|נכון|מאשר|אישור|yes|yep|yeah|ok|בסדר|סבבה|מוסכם)/i.test(String(text || "").trim());

  const isNo = (text) =>
    /(לא|לא תודה|לא זה|לא נכון|no|nope|לא מעוניין|לא מסכים)/i.test(String(text || "").trim());

  const ensureCallerDigits = () => {
    const callerRaw = String(caller || "").trim();
    if (!callerRaw) return "";
    const digits = normalizePhoneDigits(callerRaw);
    return isValidPhoneDigits(digits) ? digits : "";
  };

  // ---- Hours / after-hours detection (for Delivery special offer) ----
  const parseHours = (s) => {
    const m = String(s || "").match(/(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})/);
    if (!m) return null;
    const aH = Number(m[1]),
      aM = Number(m[2]),
      bH = Number(m[3]),
      bM = Number(m[4]);
    if (![aH, aM, bH, bM].every((x) => Number.isFinite(x))) return null;
    return { start: aH * 60 + aM, end: bH * 60 + bM };
  };

  const isAfterHours = () => {
    const hoursStr =
      getSetting("WORKING_HOURS", "") ||
      getSetting("BUSINESS_HOURS", "") ||
      getSetting("HOURS", "") ||
      "";
    const parsed = parseHours(hoursStr);
    if (!parsed) return false;

    const now = new Date();
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: TIME_ZONE,
      hour12: false,
      hour: "2-digit",
      minute: "2-digit"
    }).formatToParts(now);
    const hh = Number(parts.find((p) => p.type === "hour")?.value || 0);
    const mm = Number(parts.find((p) => p.type === "minute")?.value || 0);
    const cur = hh * 60 + mm;
    return cur < parsed.start || cur > parsed.end;
  };

  // ---- Sheets text helpers (PROMPTS + SETTINGS only) ----
  const getSheetText = (key, fallback = "") => {
    const k = String(key || "").trim();
    if (!k) return String(fallback || "").trim();
    const p = String((SHEETS.prompts || {})[k] || "").trim();
    if (p) return p;
    const s = String((SHEETS.settings || {})[k] || "").trim();
    if (s) return s;
    return String(fallback || "").trim();
  };

  const getFlowText = (key) => getSheetText(key, "");
  const getFlowTextOrFallback = (key, fallback) => getSheetText(key, fallback);

  // Supports both {{var}} and {var}
  const renderFlowText = (template, vars = {}) => {
    if (!template) return "";
    let t = String(template);
    t = t.replace(/\{\{(\w+)\}\}/g, (match, k) =>
      Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : match
    );
    t = t.replace(/\{(\w+)\}/g, (match, k) =>
      Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : match
    );
    return t;
  };

  // ---- DO_NOT_SAY pack (guardrails; not spoken, only instruction context) ----
  const buildDoNotSayText = () => {
    const dnsRows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
    return dnsRows
      .map((r) => {
        const a = String(r.forbidden_topic || "").trim();
        const b = String(r.trigger_examples || "").trim();
        const c = String(r.safe_response_he || "").trim();
        const parts = [a && `נושא: ${a}`, b && `טריגרים: ${b}`, c && `תגובה בטוחה: ${c}`].filter(Boolean);
        return parts.join(" | ");
      })
      .filter(Boolean)
      .slice(0, 40)
      .join("\n");
  };

  // ---- KB_FACTS lookup (only when explicitly asked for facts; otherwise we keep collecting) ----
  const findKBAnswer = (utterance) => {
    const q = String(utterance || "").toLowerCase().trim();
    if (!q) return "";
    const rows = Array.isArray(SHEETS.kbFacts) ? SHEETS.kbFacts : [];
    // Heuristic: match if any keyword appears; prefer longer keyword lists.
    let best = null;
    let bestScore = 0;
    for (const r of rows) {
      const kwRaw = String(r.keywords || r.keyword || "").trim();
      const ans = String(r.answer_he || r.answer || "").trim();
      if (!kwRaw || !ans) continue;
      const kws = kwRaw
        .split(",")
        .map((x) => x.trim().toLowerCase())
        .filter(Boolean);
      if (!kws.length) continue;
      let score = 0;
      for (const kw of kws) {
        if (kw && q.includes(kw)) score += 1;
      }
      if (score > 0) {
        // Prefer higher coverage
        const coverage = score / Math.max(1, kws.length);
        const weighted = score * 10 + Math.round(coverage * 10);
        if (weighted > bestScore) {
          bestScore = weighted;
          best = ans;
        }
      }
    }
    return best || "";
  };

  // ---- Importers & delivery contacts ----
  const findExactImporter = (brandName) => {
    const brand = String(brandName || "").trim();
    if (!brand) return null;
    const rows = Array.isArray(SHEETS.suppliersImporters) ? SHEETS.suppliersImporters : [];
    const match = rows.find((r) => String(r.brand_name || "").trim() === brand);
    if (!match) return null;
    return {
      brand,
      importer: String(match.importer_name || "").trim(),
      phone: String(match.phone_e164 || match.phone || "").trim()
    };
  };

  const buildCarrierList = () => {
    const rows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
    const carrierDescriptions = rows
      .map((r) => {
        let p = String(r.phone_e164 || r.phone || "").replace(/\D+/g, "");
        if (!p) return "";
        if (p.startsWith("972") && p.length > 3) p = "0" + p.slice(3);
        const spaced = formatSpacedDigits(p);
        const name = String(r.name || "").trim();
        return name ? `${name} – ${spaced}` : spaced;
      })
      .filter(Boolean);
    return carrierDescriptions;
  };

  const shouldOfferCarriersAfterHoursSameDay = (text) => {
    const t = String(text || "").trim();
    if (!t) return false;
    if (!isAfterHours()) return false;

    const low = t.toLowerCase();
    const rows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
    for (const r of rows) {
      const rule = String(r.rule || "").trim();
      if (rule !== "after_hours_same_day_only") continue;
      const kws = String(r.condition_keywords || "")
        .split(",")
        .map((x) => x.trim())
        .filter(Boolean);
      if (!kws.length) continue;
      if (kws.some((kw) => kw && low.includes(kw.toLowerCase()))) return true;
    }
    return false;
  };

  // ---- Minimal route hint (still deterministic); the primary behavior is "collect only" once set ----
  const extractRouteHint = (text) => {
    const low = String(text || "").toLowerCase();
    if (/(אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול)/.test(low)) return "support";
    if (/(משלוח|אספקה|הספקה|שליח|הזמנה|הגיע|לא הגיע|מוביל)/.test(low)) return "delivery";
    if (/(מחיר|לקנות|רכישה|מוצר|דגם|מידה|צבע|מלאי|כמה עולה|מבצע)/.test(low)) return "sales";
    if (/(הודעה|מנהל|עובד|לחזור אלי|השארת הודעה)/.test(low)) return "message";
    return "";
  };

  // --------------------------------------------------
  // Flow state (no state-machine engine; deterministic gating by missing fields)
  // --------------------------------------------------
  const flowState = {
    stage: "routing",
    route: "other",
    askedRoutingClarify: false,

    // Collected fields (mandatory depends on route)
    data: {
      // Shared
      full_name: "",
      callback_phone: "",
      // Sales
      product_type: "",
      product_model: "",
      product_brand: "",
      // Support
      issue_desc: "",
      support_product: "",
      // Delivery
      delivery_desc: "",
      carriers_info_given: false,
      carriers_info_offered: false,
      // Message
      message_target: "",
      message_body: "",
      message_target_confirmed: false,

      // internal
      _importerMatch: null
    },

    phoneConfirmed: false,

    // Finalization
    finalEvent: "",
    finalSummary: "",
    shouldHangup: false,
    finalPayloadSent: false,
    doneLocked: false,
    allowFinalResponse: false
  };

  // ---- Name validation: at least two tokens ----
  const extractNameCandidate = (text) => {
    let t = String(text || "").trim();
    if (!t) return "";

    const digits = extractPhoneCandidates(t);
    if (digits) t = t.replace(digits, "").trim();

    const lowered = t.toLowerCase();
    const markers = ["השם שלי", "קוראים לי", "שמי", "אני"];
    for (const m of markers) {
      if (lowered.includes(m)) {
        const idx = lowered.lastIndexOf(m);
        t = t.slice(idx + m.length).trim();
        break;
      }
    }

    t = t.replace(/[0-9]/g, "").replace(/\s+/g, " ").trim();
    if (!t || t.length < 2 || t.length > 60) return "";

    // Must include at least two words (first+last)
    const parts = t.split(" ").filter(Boolean);
    if (parts.length < 2) return "";

    // Basic sanity
    if (!/[A-Za-z\u0590-\u05FF]/.test(t)) return "";
    const filler = new Set(["תודה", "אוקיי", "אוקי", "כן", "לא", "ביי", "שלום", "בסדר", "סבבה", "בבקשה"]);
    if (filler.has(t)) return "";
    return t;
  };

  const isExplicitNamePhrase = (text) => /(השם שלי|קוראים לי|שמי)/.test(String(text || ""));

  // ---- Model/brand extraction: only if explicitly marked in utterance (avoid hallucination) ----
  const extractBrandModelExplicit = (text) => {
    const t = String(text || "");
    const brandMatch = t.match(/מותג\s+([^,.\n\r]+)/);
    const modelMatch = t.match(/דגם\s+([^,.\n\r]+)/);
    return {
      brand: brandMatch ? String(brandMatch[1] || "").trim() : "",
      model: modelMatch ? String(modelMatch[1] || "").trim() : ""
    };
  };

  // --------------------------------------------------
  // Instructions builder (enforces: text to speak comes from Sheets)
  // --------------------------------------------------
  const buildFlowInstructions = (sayText, ctx = {}) => {
    const baseStyle =
      MB_BASE_STYLE && MB_BASE_STYLE.trim()
        ? MB_BASE_STYLE.trim()
        : "סגנון: נטע. תשובות קצרות, ענייניות, אנושיות. בלי חזרות מיותרות.";

    const guardrailsPrompt = getPrompt("GUARDRAILS_PROMPT", "");
    const routingPrompt = getPrompt("ROUTING_PROMPT", "");
    const salesPrompt = getPrompt("SALES_PROMPT", "");
    const supportPrompt = getPrompt("SUPPORT_PROMPT", "");
    const deliveryPrompt = getPrompt("DELIVERY_PROMPT", "");
    const messagePrompt = getPrompt("MESSAGE_TO_MANAGER_PROMPT", "");

    const dns = buildDoNotSayText();
    const routeContext =
      flowState.route === "sales"
        ? salesPrompt
        : flowState.route === "support"
        ? supportPrompt
        : flowState.route === "delivery"
        ? deliveryPrompt
        : flowState.route === "message"
        ? messagePrompt
        : "";

    // Runtime vars for rendering
    const callerDigits = ensureCallerDigits();
    const vars = {
      caller_id: callerDigits ? formatSpacedDigits(callerDigits) : "",
      phone: flowState.data.callback_phone ? formatSpacedDigits(flowState.data.callback_phone) : "",
      target: flowState.data.message_target || "",
      model: flowState.data.product_model || "",
      brand: flowState.data.product_brand || "",
      carriers: (ctx.carriers || []).join(", "),
      importer_phone:
        ctx.importer_phone ? formatSpacedDigits(normalizePhoneDigits(ctx.importer_phone)) : ""
    };

    const renderedSay = renderFlowText(String(sayText || ""), vars).trim();

    const rules = [
      baseStyle,
      "עברית בלבד. תמיד בלשון רבים וללא פנייה מגדרית.",
      "אין המצאת מידע. תשובות עובדתיות/אינפורמטיביות רק מה-Sheets (KB_FACTS/SETTINGS/PROMPTS/DELIVERY_CONTACTS/SUPPLIERS_IMPORTERS).",
      "לא מבטיחים מחיר. לא מתחייבים לזמני אספקה. לא מציגים עצמנו כבעל העסק.",
      "אחרי זיהוי כוונה (Intent) — הבוט רק אוסף מידע לפי הזרימה. לא מסבירים מעבר לכך.",
      "בכל שלב: שאלה אחת בלבד (אם יש שאלה). להמתין למענה לפני מעבר.",
      "מספרי טלפון: לקרוא ספרה-ספרה בלבד.",
      guardrailsPrompt ? `GUARDRAILS_PROMPT:\n${guardrailsPrompt}` : "",
      flowState.stage === "routing" || flowState.stage === "routing_clarify"
        ? routingPrompt
          ? `ROUTING_PROMPT:\n${routingPrompt}`
          : ""
        : "",
      routeContext ? `ROUTE_PROMPT:\n${routeContext}` : "",
      dns ? `DO_NOT_SAY (מחייב):\n${dns}` : ""
    ].filter(Boolean);

    if (!renderedSay) {
      // Safety only
      return [...rules, `SAY:\n${getFlowTextOrFallback("FLOW_ROUTING_CLARIFY", FALLBACK_EMPTY_INSTRUCTIONS)}`]
        .filter(Boolean)
        .join("\n\n")
        .trim();
    }

    return [...rules, `SAY:\n${renderedSay}`].join("\n\n").trim();
  };

  // --------------------------------------------------
  // Stage → what the assistant should say next (always from Sheets)
  // --------------------------------------------------
  const buildNextInstructions = () => {
    const callerDigits = ensureCallerDigits();
    const spacedCaller = callerDigits ? formatSpacedDigits(callerDigits) : "";
    const priceClaim = getSetting("PRICE_CLAIM_SENTENCE", "");
    const coupon = getSetting("SALES_COUPON_CODE", "");
    const couponDigits = coupon ? String(coupon).replace(/\D+/g, "") : "";
    const couponSpaced = couponDigits ? formatSpacedDigits(couponDigits) : "";

    // ROUTING
    if (flowState.stage === "routing") {
      const t = getFlowText("FLOW_ROUTING_CLARIFY") || getPrompt("ROUTING_PROMPT", "") || "";
      return buildFlowInstructions(t || FALLBACK_ROUTING_CLARIFY);
    }
    if (flowState.stage === "routing_clarify") {
      const t = getFlowText("FLOW_ROUTING_CLARIFY");
      return buildFlowInstructions(t || FALLBACK_ROUTING_CLARIFY);
    }

    // SALES
    if (flowState.stage === "sales_product") {
      return buildFlowInstructions(getFlowText("FLOW_SALES_PRODUCT"));
    }
    if (flowState.stage === "sales_name") {
      return buildFlowInstructions(getFlowText("FLOW_SALES_NAME"));
    }
    if (flowState.stage === "sales_model_ask") {
      // Single assistant turn: price-claim sentence (settings) + model ask (prompts)
      const modelAsk = getFlowText("FLOW_SALES_MODEL_ASK");
      const p = String(priceClaim || "").trim();
      const c = couponSpaced ? ` ${couponSpaced}` : "";
      const combined = [p ? `${p}${c ? " " + c : ""}` : "", modelAsk].filter(Boolean).join("\n");
      // If model already known, do not ask again; acknowledge via FLOW_SALES_MODEL_COLLECT (optional) and continue to brand ask
      if (flowState.data.product_model) {
        const ack = getFlowText("FLOW_SALES_MODEL_COLLECT");
        const brandAsk = getFlowText("FLOW_SALES_BRAND_ASK");
        const combined2 = [ack, brandAsk].filter(Boolean).join("\n");
        return buildFlowInstructions(combined2 || brandAsk || combined);
      }
      return buildFlowInstructions(combined || modelAsk || p || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "sales_model_collect") {
      return buildFlowInstructions(getFlowText("FLOW_SALES_MODEL_COLLECT"));
    }
    if (flowState.stage === "sales_brand_ask") {
      // If brand already known, do not ask again; acknowledge via FLOW_SALES_BRAND_COLLECT and continue to phone confirm
      if (flowState.data.product_brand) {
        const ack = getFlowText("FLOW_SALES_BRAND_COLLECT");
        const next = getFlowText("FLOW_SALES_PHONE_CONFIRM");
        const combined = [ack, next].filter(Boolean).join("\n");
        return buildFlowInstructions(
          renderFlowText(combined || next || "", { caller_id: spacedCaller }) || next || FALLBACK_EMPTY_INSTRUCTIONS
        );
      }
      return buildFlowInstructions(getFlowText("FLOW_SALES_BRAND_ASK"));
    }
    if (flowState.stage === "sales_brand_collect") {
      return buildFlowInstructions(getFlowText("FLOW_SALES_BRAND_COLLECT"));
    }
    if (flowState.stage === "sales_phone_confirm") {
      const t = getFlowText("FLOW_SALES_PHONE_CONFIRM");
      if (spacedCaller && t) return buildFlowInstructions(renderFlowText(t, { caller_id: spacedCaller }));
      return buildFlowInstructions(getFlowText("FLOW_SALES_PHONE_COLLECT") || t || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "sales_phone_collect") {
      return buildFlowInstructions(getFlowText("FLOW_SALES_PHONE_COLLECT"));
    }
    if (flowState.stage === "sales_phone_confirm_new") {
      const t = getFlowText("FLOW_SALES_PHONE_CONFIRM_NEW");
      const spaced = flowState.data.callback_phone ? formatSpacedDigits(flowState.data.callback_phone) : "";
      if (t) return buildFlowInstructions(renderFlowText(t, { phone: spaced }));
      return buildFlowInstructions(getFlowText("FLOW_SALES_PHONE_COLLECT") || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "sales_done") {
      flowState.finalEvent = "מתעניין במכירות";
      flowState.finalSummary = "מתעניין במכירות";
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowText("FLOW_SALES_DONE"));
    }

    // SUPPORT
    if (flowState.stage === "support_issue_desc") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_ISSUE_DESC"));
    }
    if (flowState.stage === "support_product") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_PRODUCT"));
    }
    if (flowState.stage === "support_model_ask") {
      // If model already known, acknowledge and continue to brand ask
      if (flowState.data.product_model) {
        const ack = getFlowText("FLOW_SUPPORT_MODEL_COLLECT");
        const ask = getFlowText("FLOW_SUPPORT_BRAND_ASK");
        return buildFlowInstructions([ack, ask].filter(Boolean).join("\n") || ask || FALLBACK_EMPTY_INSTRUCTIONS);
      }
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_MODEL_ASK"));
    }
    if (flowState.stage === "support_model_collect") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_MODEL_COLLECT"));
    }
    if (flowState.stage === "support_brand_ask") {
      // If brand already known, acknowledge and continue to importer check/name
      if (flowState.data.product_brand) {
        const ack = getFlowText("FLOW_SUPPORT_BRAND_COLLECT");
        return buildFlowInstructions(ack || getFlowText("FLOW_SUPPORT_BRAND_ASK") || FALLBACK_EMPTY_INSTRUCTIONS);
      }
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_BRAND_ASK"));
    }
    if (flowState.stage === "support_brand_collect") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_BRAND_COLLECT"));
    }
    if (flowState.stage === "support_importer_offer") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_IMPORTER_FOUND_NOTICE"));
    }
    if (flowState.stage === "support_importer_give_and_continue") {
      const importer = flowState.data._importerMatch;
      const give = getFlowText("FLOW_SUPPORT_IMPORTER_FOUND_GIVE_NUMBER");
      const nameAsk = getFlowText("FLOW_SUPPORT_NAME");
      const combined = [give, nameAsk].filter(Boolean).join("\n");
      return buildFlowInstructions(
        renderFlowText(combined, {
          importer_phone: importer?.phone || ""
        }),
        { importer_phone: importer?.phone || "" }
      );
    }
    if (flowState.stage === "support_importer_decline_and_continue") {
      const decline = getFlowText("FLOW_SUPPORT_IMPORTER_FOUND_DECLINE");
      const nameAsk = getFlowText("FLOW_SUPPORT_NAME");
      return buildFlowInstructions([decline, nameAsk].filter(Boolean).join("\n") || nameAsk);
    }
    if (flowState.stage === "support_name") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_NAME"));
    }
    if (flowState.stage === "support_phone_confirm") {
      const t = getFlowText("FLOW_SUPPORT_PHONE_CONFIRM");
      if (spacedCaller && t) return buildFlowInstructions(renderFlowText(t, { caller_id: spacedCaller }));
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_PHONE_COLLECT") || t || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "support_phone_collect") {
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_PHONE_COLLECT"));
    }
    if (flowState.stage === "support_phone_confirm_new") {
      const t = getFlowText("FLOW_SUPPORT_PHONE_CONFIRM_NEW");
      const spaced = flowState.data.callback_phone ? formatSpacedDigits(flowState.data.callback_phone) : "";
      if (t) return buildFlowInstructions(renderFlowText(t, { phone: spaced }));
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_PHONE_COLLECT") || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "support_done") {
      flowState.finalEvent = "שירות לקוחות – תקלה";
      flowState.finalSummary = "שירות לקוחות – תקלה";
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowText("FLOW_SUPPORT_DONE"));
    }

    // DELIVERY
    if (flowState.stage === "delivery_desc") {
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_DESC"));
    }
    if (flowState.stage === "delivery_after_hours_offer") {
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_CARRIERS_OFFER"));
    }
    if (flowState.stage === "delivery_carriers_give_and_continue") {
      const carriers = buildCarrierList();
      const give = getFlowText("FLOW_DELIVERY_CARRIERS_GIVE");
      const desc = getFlowText("FLOW_DELIVERY_DESC");
      const combined = [give, desc].filter(Boolean).join("\n");
      flowState.data.carriers_info_given = true;
      return buildFlowInstructions(renderFlowText(combined, { carriers: carriers.join(", ") }), { carriers });
    }
    if (flowState.stage === "delivery_carriers_decline_and_continue") {
      const decline = getFlowText("FLOW_DELIVERY_CARRIERS_DECLINE");
      const desc = getFlowText("FLOW_DELIVERY_DESC");
      flowState.data.carriers_info_given = false;
      return buildFlowInstructions([decline, desc].filter(Boolean).join("\n") || desc);
    }
    if (flowState.stage === "delivery_name") {
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_NAME"));
    }
    if (flowState.stage === "delivery_phone_confirm") {
      const t = getFlowText("FLOW_DELIVERY_PHONE_CONFIRM");
      if (spacedCaller && t) return buildFlowInstructions(renderFlowText(t, { caller_id: spacedCaller }));
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_PHONE_COLLECT") || t || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "delivery_phone_collect") {
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_PHONE_COLLECT"));
    }
    if (flowState.stage === "delivery_phone_confirm_new") {
      const t = getFlowText("FLOW_DELIVERY_PHONE_CONFIRM_NEW");
      const spaced = flowState.data.callback_phone ? formatSpacedDigits(flowState.data.callback_phone) : "";
      if (t) return buildFlowInstructions(renderFlowText(t, { phone: spaced }));
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_PHONE_COLLECT") || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "delivery_done") {
      flowState.finalEvent = "אספקה / משלוח";
      flowState.finalSummary = "אספקה / משלוח";
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowText("FLOW_DELIVERY_DONE"));
    }

    // MESSAGE
    if (flowState.stage === "message_target") {
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_TARGET"));
    }
    if (flowState.stage === "message_target_confirm") {
      const t = getFlowText("FLOW_MESSAGE_TARGET_CONFIRM");
      return buildFlowInstructions(renderFlowText(t, { target: flowState.data.message_target || "" }) || t);
    }
    if (flowState.stage === "message_body") {
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_BODY"));
    }
    if (flowState.stage === "message_name") {
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_NAME"));
    }
    if (flowState.stage === "message_phone_confirm") {
      const t = getFlowText("FLOW_MESSAGE_PHONE_CONFIRM");
      if (spacedCaller && t) return buildFlowInstructions(renderFlowText(t, { caller_id: spacedCaller }));
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_PHONE_COLLECT") || t || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "message_phone_collect") {
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_PHONE_COLLECT"));
    }
    if (flowState.stage === "message_phone_confirm_new") {
      const t = getFlowText("FLOW_MESSAGE_PHONE_CONFIRM_NEW");
      const spaced = flowState.data.callback_phone ? formatSpacedDigits(flowState.data.callback_phone) : "";
      if (t) return buildFlowInstructions(renderFlowText(t, { phone: spaced }));
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_PHONE_COLLECT") || FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (flowState.stage === "message_done") {
      flowState.finalEvent = `הודעה`;
      flowState.finalSummary = `הודעה עבור: ${flowState.data.message_target || ""}`.trim();
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowText("FLOW_MESSAGE_DONE"));
    }

    // Generic invalids (from sheets)
    const invalidName = getFlowText("FLOW_NAME_INVALID");
    return buildFlowInstructions(invalidName || FALLBACK_EMPTY_INSTRUCTIONS);
  };

  // --------------------------------------------------
  // Build webhook payloads
  // --------------------------------------------------
  const buildSummary = () => {
    const brandModel = [flowState.data.product_brand, flowState.data.product_model]
      .filter((v) => String(v || "").trim())
      .join(" ");
    if (flowState.route === "sales") {
      const parts = [flowState.data.product_type ? `התעניינות: ${flowState.data.product_type}` : "התעניינות", brandModel]
        .filter(Boolean)
        .join(" | ");
      return parts || "מתעניין במכירות";
    }
    if (flowState.route === "support") {
      const parts = [
        flowState.data.issue_desc ? `שירות: ${flowState.data.issue_desc}` : "שירות",
        flowState.data.support_product ? `מוצר: ${flowState.data.support_product}` : "",
        brandModel
      ]
        .filter(Boolean)
        .join(" | ");
      return parts || "שירות לקוחות – תקלה";
    }
    if (flowState.route === "delivery") {
      const parts = [
        flowState.data.delivery_desc ? `משלוח: ${flowState.data.delivery_desc}` : "משלוח",
        `מובילים נמסרו: ${flowState.data.carriers_info_given ? "כן" : "לא"}`
      ].join(" | ");
      return parts || "אספקה / משלוח";
    }
    if (flowState.route === "message") {
      const target = flowState.data.message_target || "הצוות";
      const body = String(flowState.data.message_body || "").trim();
      let bodyShort = body;
      if (bodyShort.length > 80) {
        const cut = bodyShort.slice(0, 80);
        const lastSpace = cut.lastIndexOf(" ");
        bodyShort = lastSpace > 30 ? cut.slice(0, lastSpace) : cut;
      }
      return `הודעה ל-${target}: ${bodyShort || "ללא תוכן"}`;
    }
    return "פנייה כללית";
  };

  const buildFinalPayload = () => {
    const ended = endedAt || nowIso();
    const callerDigits = ensureCallerDigits();
    const recording_url_public = makeRecordingPublicUrl(callSid);
    const caller_id = isValidPhoneDigits(callerDigits) ? callerDigits : "";
    const callbackFromData = isValidPhoneDigits(flowState.data.callback_phone) ? flowState.data.callback_phone : "";
    const callback_phone = callbackFromData || (flowState.phoneConfirmed && caller_id ? caller_id : "");
    const full_name = flowState.data.full_name || "";
    const summary = buildSummary();

    const payload = {
      callSid,
      streamSid: twilioStreamSid,
      caller,
      caller_id,
      full_name,
      callback_phone,
      called,
      started_at: startedAt,
      ended_at: ended,
      language,
      route: flowState.route,
      stage: flowState.stage,
      summary,

      // Sales
      product_type: flowState.data.product_type || "",
      product_brand: flowState.data.product_brand || "",
      product_model: flowState.data.product_model || "",

      // Support
      issue_desc: flowState.data.issue_desc || "",
      support_product: flowState.data.support_product || "",

      // Delivery
      delivery_desc: flowState.data.delivery_desc || "",
      carriers_info_given: Boolean(flowState.data.carriers_info_given),

      // Message
      message_target: flowState.data.message_target || "",
      message_body: flowState.data.message_body || "",

      caller_last_utterance: lastCallerFinal,
      bot_last_utterance: lastBotFinal,
      transcript: transcriptTurns,
      recognized_phones: recognizedPhones,

      event_name: flowState.finalEvent || "",
      timestamp: ended,
      identified_phone: caller_id,
      additional_phone: callbackFromData && callbackFromData !== caller_id ? callbackFromData : "",
      disconnected_stage: flowState.stage,
      recording_url_public,
      collected: flowState.data,
      call_reason: flowState.route,
      call_subject: flowState.finalSummary || lastBotFinal || lastCallerFinal
    };

    return payload;
  };

  const applyWebhookDefaults = (payload = {}) => {
    const callerDigits = ensureCallerDigits();
    const caller_id = isValidPhoneDigits(callerDigits) ? callerDigits : "";
    const callbackCandidate = isValidPhoneDigits(flowState.data.callback_phone) ? flowState.data.callback_phone : "";
    const fallbackCallback = callbackCandidate || (flowState.phoneConfirmed && caller_id ? caller_id : "");
    const fallbackStartedAt = startedAt || nowIso();
    const fallbackEndedAt = endedAt || nowIso();

    const merged = {
      callSid: callSid || "",
      streamSid: twilioStreamSid || "",
      caller: caller || "",
      caller_id: caller_id || "",
      called: called || "",
      started_at: fallbackStartedAt,
      ended_at: fallbackEndedAt,
      route: flowState.route || route || "",
      stage: flowState.stage || "",
      full_name: flowState.data.full_name || "",
      callback_phone: fallbackCallback || "",
      recording_url_public: makeRecordingPublicUrl(callSid),
      collected: flowState.data,
      ...payload
    };

    if (!merged.recording_url_public) merged.recording_url_public = makeRecordingPublicUrl(callSid);
    if (!merged.collected) merged.collected = flowState.data;
    return merged;
  };

  // --------------------------------------------------
  // Caller utterance processing (strict: once routed -> collect only)
  // --------------------------------------------------
  const processCallerUtterance = (utterance) => {
    const text = String(utterance || "").trim();
    if (!text) return "";

    // Best-effort explicit name capture (won't advance stage by itself)
    if (isExplicitNamePhrase(text)) {
      const nameCandidate = extractNameCandidate(text);
      if (nameCandidate) flowState.data.full_name = nameCandidate;
    }

    // Extract additional phone any time
    const maybePhone = extractPhoneCandidates(text);
    if (maybePhone && !recognizedPhones.includes(maybePhone)) recognizedPhones.push(maybePhone);

    // Allow explicit brand/model markers early (won't invent)
    const bm = extractBrandModelExplicit(text);
    if (bm.model && !flowState.data.product_model) flowState.data.product_model = bm.model;
    if (bm.brand && !flowState.data.product_brand) flowState.data.product_brand = bm.brand;

    // If user explicitly asks an informational question BEFORE routing, we do not answer; we route/clarify.
    // If AFTER routing, we ignore and continue collecting (per requirements).

    // ROUTING
    if (flowState.stage === "routing" || flowState.stage === "routing_clarify") {
      const hint = extractRouteHint(text);
      if (hint) {
        flowState.route = hint;
        route = hint;

        if (hint === "sales") flowState.stage = "sales_product";
        else if (hint === "support") flowState.stage = "support_issue_desc";
        else if (hint === "delivery") {
          // Special carrier offer can be triggered after we understand it's a delivery same-day after-hours.
          // We’ll decide after the user describes the delivery; but if it already matches now, offer first.
          if (shouldOfferCarriersAfterHoursSameDay(text)) {
            flowState.data.carriers_info_offered = true;
            flowState.stage = "delivery_after_hours_offer";
          } else {
            flowState.stage = "delivery_desc";
          }
        } else {
          // message
          flowState.route = "message";
          route = "message";
          flowState.stage = "message_target";
        }
        return buildNextInstructions();
      }

      if (!flowState.askedRoutingClarify) {
        flowState.askedRoutingClarify = true;
        flowState.stage = "routing_clarify";
        return buildNextInstructions();
      }

      // Default to message collection if still unclear (collect-only flow)
      flowState.route = "message";
      route = "message";
      flowState.stage = "message_target";
      return buildNextInstructions();
    }

    // SALES FLOW
    if (flowState.route === "sales") {
      if (flowState.stage === "sales_product") {
        flowState.data.product_type = text;
        // Only store explicit markers (already captured above); do not guess.
        flowState.stage = "sales_name";
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          const t = getFlowText("FLOW_NAME_INVALID") || FALLBACK_NAME_INVALID;
          return buildFlowInstructions(t);
        }
        flowState.data.full_name = nameCandidate;
        flowState.stage = "sales_model_ask";
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_model_ask") {
        // Expect yes/no OR model provided.
        if (isYes(text)) {
          flowState.stage = "sales_model_collect";
          return buildNextInstructions();
        }
        if (isNo(text)) {
          flowState.stage = "sales_brand_ask";
          return buildNextInstructions();
        }
        // Model said directly
        if (text.length >= 2) {
          flowState.data.product_model = text;
          flowState.stage = "sales_brand_ask";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_model_collect") {
        if (text.length >= 2) flowState.data.product_model = text;
        flowState.stage = "sales_brand_ask";
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_brand_ask") {
        if (isYes(text)) {
          flowState.stage = "sales_brand_collect";
          return buildNextInstructions();
        }
        if (isNo(text)) {
          flowState.stage = "sales_phone_confirm";
          return buildNextInstructions();
        }
        // Brand said directly
        if (text.length >= 2) {
          flowState.data.product_brand = text;
          flowState.stage = "sales_phone_confirm";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_brand_collect") {
        if (text.length >= 2) flowState.data.product_brand = text;
        flowState.stage = "sales_phone_confirm";
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_phone_confirm") {
        const callerDigits = ensureCallerDigits();
        if (isYes(text)) {
          if (!callerDigits) {
            flowState.phoneConfirmed = false;
            flowState.stage = "sales_phone_collect";
            return buildNextInstructions();
          }
          flowState.data.callback_phone = callerDigits;
          flowState.phoneConfirmed = true;
          flowState.stage = "sales_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "sales_phone_collect";
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_phone_collect") {
        const digits = extractPhoneCandidates(text);
        if (!isValidPhoneDigits(digits)) {
          const t = getFlowText("FLOW_PHONE_MISSING_DIGIT") || FALLBACK_PHONE_MISSING_DIGIT;
          return buildFlowInstructions(t);
        }
        flowState.data.callback_phone = digits;
        flowState.phoneConfirmed = false;
        if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
        flowState.stage = "sales_phone_confirm_new";
        return buildNextInstructions();
      }

      if (flowState.stage === "sales_phone_confirm_new") {
        if (isYes(text)) {
          if (!isValidPhoneDigits(flowState.data.callback_phone)) {
            flowState.phoneConfirmed = false;
            flowState.stage = "sales_phone_collect";
            return buildNextInstructions();
          }
          flowState.phoneConfirmed = true;
          flowState.stage = "sales_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "sales_phone_collect";
        return buildNextInstructions();
      }

      return "";
    }

    // SUPPORT FLOW
    if (flowState.route === "support") {
      if (flowState.stage === "support_issue_desc") {
        flowState.data.issue_desc = text;
        flowState.stage = "support_product";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_product") {
        flowState.data.support_product = text;
        flowState.stage = "support_model_ask";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_model_ask") {
        if (isYes(text)) {
          flowState.stage = "support_model_collect";
          return buildNextInstructions();
        }
        if (isNo(text)) {
          // If they refuse to provide model, we still proceed to brand ask (best effort)
          flowState.stage = "support_brand_ask";
          return buildNextInstructions();
        }
        if (text.length >= 2) {
          flowState.data.product_model = text;
          flowState.stage = "support_brand_ask";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }

      if (flowState.stage === "support_model_collect") {
        if (text.length >= 2) flowState.data.product_model = text;
        flowState.stage = "support_brand_ask";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_brand_ask") {
        if (isNo(text)) {
          flowState.stage = "support_name";
          return buildNextInstructions();
        }
        if (isYes(text)) {
          flowState.stage = "support_brand_collect";
          return buildNextInstructions();
        }
        if (text.length >= 2) {
          flowState.data.product_brand = text;
          // importer check
          const importer = findExactImporter(flowState.data.product_brand);
          if (importer && importer.phone) {
            flowState.data._importerMatch = importer;
            flowState.stage = "support_importer_offer";
            return buildNextInstructions();
          }
          flowState.stage = "support_name";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }

      if (flowState.stage === "support_brand_collect") {
        if (!isNo(text) && text.length >= 2) flowState.data.product_brand = text;
        const importer = findExactImporter(flowState.data.product_brand);
        if (importer && importer.phone) {
          flowState.data._importerMatch = importer;
          flowState.stage = "support_importer_offer";
          return buildNextInstructions();
        }
        flowState.stage = "support_name";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_importer_offer") {
        if (isYes(text)) {
          flowState.stage = "support_importer_give_and_continue";
          return buildNextInstructions();
        }
        flowState.stage = "support_importer_decline_and_continue";
        return buildNextInstructions();
      }

      if (
        flowState.stage === "support_importer_give_and_continue" ||
        flowState.stage === "support_importer_decline_and_continue"
      ) {
        // We just spoke; next caller utterance should be name
        flowState.stage = "support_name";
        // Continue normally; treat this utterance as name
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          const t = getFlowText("FLOW_NAME_INVALID") || FALLBACK_NAME_INVALID;
          return buildFlowInstructions(t);
        }
        flowState.data.full_name = nameCandidate;
        flowState.stage = "support_phone_confirm";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          const t = getFlowText("FLOW_NAME_INVALID") || FALLBACK_NAME_INVALID;
          return buildFlowInstructions(t);
        }
        flowState.data.full_name = nameCandidate;
        flowState.stage = "support_phone_confirm";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_phone_confirm") {
        const callerDigits = ensureCallerDigits();
        if (isYes(text)) {
          if (!callerDigits) {
            flowState.phoneConfirmed = false;
            flowState.stage = "support_phone_collect";
            return buildNextInstructions();
          }
          flowState.data.callback_phone = callerDigits;
          flowState.phoneConfirmed = true;
          flowState.stage = "support_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "support_phone_collect";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_phone_collect") {
        const digits = extractPhoneCandidates(text);
        if (!isValidPhoneDigits(digits)) {
          const t = getFlowText("FLOW_PHONE_MISSING_DIGIT") || FALLBACK_PHONE_MISSING_DIGIT;
          return buildFlowInstructions(t);
        }
        flowState.data.callback_phone = digits;
        flowState.phoneConfirmed = false;
        if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
        flowState.stage = "support_phone_confirm_new";
        return buildNextInstructions();
      }

      if (flowState.stage === "support_phone_confirm_new") {
        if (isYes(text)) {
          if (!isValidPhoneDigits(flowState.data.callback_phone)) {
            flowState.phoneConfirmed = false;
            flowState.stage = "support_phone_collect";
            return buildNextInstructions();
          }
          flowState.phoneConfirmed = true;
          flowState.stage = "support_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "support_phone_collect";
        return buildNextInstructions();
      }

      return "";
    }

    // DELIVERY FLOW
    if (flowState.route === "delivery") {
      if (flowState.stage === "delivery_after_hours_offer") {
        // user answers if they want carriers
        flowState.data.carriers_info_offered = true;
        if (isYes(text)) {
          flowState.stage = "delivery_carriers_give_and_continue";
          return buildNextInstructions();
        }
        flowState.stage = "delivery_carriers_decline_and_continue";
        return buildNextInstructions();
      }

      if (
        flowState.stage === "delivery_carriers_give_and_continue" ||
        flowState.stage === "delivery_carriers_decline_and_continue"
      ) {
        // We just spoke; next is delivery description
        flowState.stage = "delivery_desc";
        flowState.data.delivery_desc = text;
        flowState.stage = "delivery_name";
        return buildNextInstructions();
      }

      if (flowState.stage === "delivery_desc") {
        // Special: if same-day after-hours, offer carriers before taking full desc
        if (!flowState.data.carriers_info_offered && shouldOfferCarriersAfterHoursSameDay(text)) {
          flowState.data.carriers_info_offered = true;
          flowState.stage = "delivery_after_hours_offer";
          return buildNextInstructions();
        }
        flowState.data.delivery_desc = text;
        flowState.stage = "delivery_name";
        return buildNextInstructions();
      }

      if (flowState.stage === "delivery_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          const t = getFlowText("FLOW_NAME_INVALID") || FALLBACK_NAME_INVALID;
          return buildFlowInstructions(t);
        }
        flowState.data.full_name = nameCandidate;
        flowState.stage = "delivery_phone_confirm";
        return buildNextInstructions();
      }

      if (flowState.stage === "delivery_phone_confirm") {
        const callerDigits = ensureCallerDigits();
        if (isYes(text)) {
          if (!callerDigits) {
            flowState.phoneConfirmed = false;
            flowState.stage = "delivery_phone_collect";
            return buildNextInstructions();
          }
          flowState.data.callback_phone = callerDigits;
          flowState.phoneConfirmed = true;
          flowState.stage = "delivery_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "delivery_phone_collect";
        return buildNextInstructions();
      }

      if (flowState.stage === "delivery_phone_collect") {
        const digits = extractPhoneCandidates(text);
        if (!isValidPhoneDigits(digits)) {
          const t = getFlowText("FLOW_PHONE_MISSING_DIGIT") || FALLBACK_PHONE_MISSING_DIGIT;
          return buildFlowInstructions(t);
        }
        flowState.data.callback_phone = digits;
        flowState.phoneConfirmed = false;
        if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
        flowState.stage = "delivery_phone_confirm_new";
        return buildNextInstructions();
      }

      if (flowState.stage === "delivery_phone_confirm_new") {
        if (isYes(text)) {
          if (!isValidPhoneDigits(flowState.data.callback_phone)) {
            flowState.phoneConfirmed = false;
            flowState.stage = "delivery_phone_collect";
            return buildNextInstructions();
          }
          flowState.phoneConfirmed = true;
          flowState.stage = "delivery_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "delivery_phone_collect";
        return buildNextInstructions();
      }

      return "";
    }

    // MESSAGE FLOW
    if (flowState.route === "message") {
      if (flowState.stage === "message_target") {
        // If a target was already captured earlier, confirm it; else collect it now
        if (!flowState.data.message_target) {
          flowState.data.message_target = text;
        }
        flowState.stage = "message_target_confirm";
        return buildNextInstructions();
      }

      if (flowState.stage === "message_target_confirm") {
        if (isYes(text)) {
          flowState.data.message_target_confirmed = true;
          flowState.stage = "message_body";
          return buildNextInstructions();
        }
        if (isNo(text)) {
          flowState.data.message_target = "";
          flowState.data.message_target_confirmed = false;
          flowState.stage = "message_target";
          return buildNextInstructions();
        }
        // If they respond with a name instead of yes/no, treat it as corrected target
        if (text.length >= 2) {
          flowState.data.message_target = text;
          flowState.data.message_target_confirmed = true;
          flowState.stage = "message_body";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }

      if (flowState.stage === "message_body") {
        flowState.data.message_body = text;
        flowState.stage = "message_name";
        return buildNextInstructions();
      }

      if (flowState.stage === "message_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          const t = getFlowText("FLOW_NAME_INVALID") || FALLBACK_NAME_INVALID;
          return buildFlowInstructions(t);
        }
        flowState.data.full_name = nameCandidate;
        flowState.stage = "message_phone_confirm";
        return buildNextInstructions();
      }

      if (flowState.stage === "message_phone_confirm") {
        const callerDigits = ensureCallerDigits();
        if (isYes(text)) {
          if (!callerDigits) {
            flowState.phoneConfirmed = false;
            flowState.stage = "message_phone_collect";
            return buildNextInstructions();
          }
          flowState.data.callback_phone = callerDigits;
          flowState.phoneConfirmed = true;
          flowState.stage = "message_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "message_phone_collect";
        return buildNextInstructions();
      }

      if (flowState.stage === "message_phone_collect") {
        const digits = extractPhoneCandidates(text);
        if (!isValidPhoneDigits(digits)) {
          const t = getFlowText("FLOW_PHONE_MISSING_DIGIT") || FALLBACK_PHONE_MISSING_DIGIT;
          return buildFlowInstructions(t);
        }
        flowState.data.callback_phone = digits;
        flowState.phoneConfirmed = false;
        if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
        flowState.stage = "message_phone_confirm_new";
        return buildNextInstructions();
      }

      if (flowState.stage === "message_phone_confirm_new") {
        if (isYes(text)) {
          if (!isValidPhoneDigits(flowState.data.callback_phone)) {
            flowState.phoneConfirmed = false;
            flowState.stage = "message_phone_collect";
            return buildNextInstructions();
          }
          flowState.phoneConfirmed = true;
          flowState.stage = "message_done";
          return buildNextInstructions();
        }
        flowState.phoneConfirmed = false;
        flowState.stage = "message_phone_collect";
        return buildNextInstructions();
      }

      return "";
    }

    return "";
  };

  // --------------------------------------------------
  // Normalize transcript for duplicate detection
  // --------------------------------------------------
  const normalizeTranscript = (s) => {
    try {
      let t = String(s || "").toLowerCase();
      t = t.replace(/[\.,!?\-–—;:'"\u05be]/g, " ");
      t = t.replace(/\s+/g, " ").trim();
      const greetings = [
        "היי",
        "הי",
        "שלום",
        "ביי",
        "היי שלום",
        "שלום לך",
        "ביי שלום",
        "אה",
        "בבקשה",
        "hi",
        "hello",
        "bye",
        "bye bye"
      ];
      for (const g of greetings) {
        if (t.startsWith(g + " ")) t = t.slice(g.length).trim();
        if (t === g) return "";
      }
      return t;
    } catch (_) {
      return String(s || "").trim().toLowerCase();
    }
  };

  const isFillerOnly = (normalized) => {
    const fillerPhrases = ["תודה", "תודה רבה", "כן", "סבבה", "בבקשה", "בסדר"];
    return fillerPhrases.some((fp) => normalized === fp || normalized.startsWith(fp + " ") || normalized.endsWith(" " + fp));
  };

  let lastCallerNormalized = "";
  let lastRequestedCallerNormalized = "";

  const printCallerFinal = (text) => {
    const t = String(text || "").trim();
    if (!t) return;
    if (t === lastCallerFinal) return;
    lastCallerFinal = t;
    pushTurn("caller", t);
    proxyInstructions = processCallerUtterance(t);
    always(`[CALLER][${connTag}]`, t);
  };

  const printBotFinal = (text) => {
    const t = String(text || "").trim();
    if (!t) return;
    if (t === lastBotFinal) return;
    lastBotFinal = t;
    pushTurn("bot", t);
    always(`[BOT][${connTag}]`, t);
  };

  // NOTE: declare openaiWs variable early
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

  if (!OPENAI_API_KEY) {
    error("OPENAI_API_KEY missing — closing call");
    try {
      twilioWs.close();
    } catch (_) {}
    return;
  }

  // -----------------------------
  // Anti-overlap: only ONE active response at a time
  // -----------------------------
  let awaitingResponse = false;
  let pendingResponseRequest = false;
  let isFlushingBufferedAudio = false;

  const requestAssistantResponse = (reason = "") => {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;

    if (flowState.doneLocked && !flowState.allowFinalResponse) {
      pendingResponseRequest = false;
      return;
    }

    if (awaitingResponse) {
      safeOpenAISend({ type: "response.cancel" });
      awaitingResponse = false;
      pendingResponseRequest = false;
    }

    let instructions = proxyInstructions;
    if (!instructions) {
      try {
        instructions = buildNextInstructions();
      } catch (_) {
        instructions = buildFlowInstructions(getFlowTextOrFallback("FLOW_ROUTING_CLARIFY", FALLBACK_EMPTY_INSTRUCTIONS));
      }
    }

    if (!String(instructions || "").trim()) {
      instructions = buildFlowInstructions(getFlowTextOrFallback("FLOW_ROUTING_CLARIFY", FALLBACK_EMPTY_INSTRUCTIONS));
    }

    awaitingResponse = true;
    pendingResponseRequest = false;

    if (flowState.doneLocked) {
      flowState.allowFinalResponse = false;
    }

    lastRequestedCallerFinal = lastCallerFinal;
    lastRequestedCallerNormalized = lastCallerNormalized;

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

  openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "OpenAI-Beta": "realtime=v1"
    }
  });

  openaiWs.on("open", async () => {
    debug(`[${connTag}] OpenAI connected`);
    openaiReady = true;

    if (!SHEETS.loaded_at) {
      debug(`[${connTag}] Sheets not loaded yet. Loading now...`);
      await loadSheets();
    }

    if (
      (!SHEETS.loaded_at || (!Object.keys(SHEETS.prompts || {}).length && !Object.keys(SHEETS.settings || {}).length)) &&
      !warnedSheetsEmpty
    ) {
      warnedSheetsEmpty = true;
      console.warn("[WARNING] Sheets not loaded or empty; using fallbacks.");
    }

    const masterPrompt = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
    );

    const openingScript = getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");
    const openingFromSheet = Boolean(String((SHEETS.settings || {}).OPENING_SCRIPT || "").trim());
    const masterFromSheet = Boolean(String((SHEETS.prompts || {}).MASTER_PROMPT || "").trim());

    always(`[${connTag}] SOURCES`, {
      sheets_loaded_at: SHEETS.loaded_at,
      opening_from: openingFromSheet ? "SETTINGS.OPENING_SCRIPT" : "FALLBACK.DEFAULT",
      master_from: masterFromSheet ? "PROMPTS.MASTER_PROMPT" : "FALLBACK.DEFAULT",
      opening_preview: preview(openingScript, 220),
      master_preview: preview(masterPrompt, 220)
    });

    const session = {
      modalities: ["audio", "text"],
      voice: OPENAI_VOICE,
      input_audio_format: "g711_ulaw",
      output_audio_format: "g711_ulaw",
      turn_detection: {
        type: "server_vad",
        threshold: MB_VAD_THRESHOLD,
        silence_duration_ms: MB_VAD_SILENCE_MS,
        prefix_padding_ms: MB_VAD_PREFIX_MS,
        create_response: false
      },
      instructions: masterPrompt
    };

    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    // Optional voice style / speaking rate (best-effort; API may ignore)
    if (OPENAI_VOICE_STYLE) {
      session.voice_style = OPENAI_VOICE_STYLE;
    }
    if (OPENAI_SPEAKING_RATE && Number.isFinite(OPENAI_SPEAKING_RATE)) {
      session.speaking_rate = OPENAI_SPEAKING_RATE;
    }

    safeOpenAISend({ type: "session.update", session });

    // Opening (one-time; verbatim)
    awaitingResponse = true;
    pendingResponseRequest = false;

    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions: `תגידי עכשיו בדיוק את המשפט הבא מילה במילה, ללא תוספות וללא שאלות:\n${openingScript}`
      }
    });

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

  openaiWs.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      error(`[${connTag}] OpenAI message JSON parse failed`, e.message);
      return;
    }

    if (MB_LOG_RAW_OPENAI) {
      const small = { type: msg.type, event_id: msg.event_id };
      if (msg.delta) small.delta_len = String(msg.delta).length;
      if (msg.transcript) small.transcript = preview(msg.transcript, 200);
      if (msg.text) small.text = preview(msg.text, 200);
      always(`[RAW_OPENAI][${connTag}]`, JSON.stringify(small));
    }

    try {
      if (
        MB_LOG_TRANSCRIPTS &&
        msg &&
        typeof msg.transcript === "string" &&
        msg.type &&
        String(msg.type).startsWith("response.audio_transcript.delta")
      ) {
        always(`[BOT_PART][${connTag}]`, msg.transcript.trim());
      }
    } catch (_) {}

    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error event`, msg);
      const errCode = msg && msg.error && msg.error.code ? String(msg.error.code) : "";
      if (errCode === "conversation_already_has_active_response") {
        awaitingResponse = true;
      }
      return;
    }

    if (msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) printBotFinal(t);
      return;
    }

    // CALLER FINAL
    {
      const type = String(msg.type || "");
      const doneLike = type.includes("done") || type.includes("completed");
      const possible =
        msg.transcript ||
        msg.text ||
        msg?.item?.content?.[0]?.transcript ||
        msg?.item?.content?.[0]?.text ||
        "";
      const isInputTranscript =
        type.includes("input_audio_transcription") ||
        type.includes("input_audio_transcript") ||
        type.includes("conversation.item.input_audio_transcription");
      if (doneLike && isInputTranscript && possible) {
        const utterance = String(possible).trim();
        const normalized = normalizeTranscript(utterance);
        const wordCount = normalized.split(/\s+/).filter(Boolean).length;

        if (isFlushingBufferedAudio) {
          lastCallerNormalized = normalized;
          lastCallerFinal = utterance;
          return;
        }

        printCallerFinal(utterance);
        lastCallerNormalized = normalized;

        // Duplicate / short / filler filtering
        let isDup = false;
        if (lastRequestedCallerNormalized) {
          if (normalized === lastRequestedCallerNormalized) isDup = true;
          else if (normalized.startsWith(lastRequestedCallerNormalized)) isDup = true;
          else if (lastRequestedCallerNormalized.startsWith(normalized)) isDup = true;
        }

        const allowShortReplyStages = new Set([
          "routing",
          "routing_clarify",
          "sales_product",
          "sales_name",
          "sales_model_ask",
          "sales_model_collect",
          "sales_brand_ask",
          "sales_brand_collect",
          "sales_phone_confirm",
          "sales_phone_confirm_new",
          "support_issue_desc",
          "support_product",
          "support_model_ask",
          "support_model_collect",
          "support_brand_ask",
          "support_brand_collect",
          "support_importer_offer",
          "support_name",
          "support_phone_confirm",
          "support_phone_confirm_new",
          "delivery_after_hours_offer",
          "delivery_desc",
          "delivery_name",
          "delivery_phone_confirm",
          "delivery_phone_confirm_new",
          "message_target",
          "message_target_confirm",
          "message_body",
          "message_name",
          "message_phone_confirm",
          "message_phone_confirm_new"
        ]);

        const allowShortReply = flowState && flowState.stage && allowShortReplyStages.has(String(flowState.stage));
        const hasPhone = Boolean(extractPhoneCandidates(normalized));
        const meaningfulShort = isYes(normalized) || isNo(normalized) || hasPhone || (normalized && !isFillerOnly(normalized));

        if (!isDup && normalized) {
          const shouldRespond = (allowShortReply && meaningfulShort) || (!allowShortReply && wordCount >= 5);
          if (shouldRespond) {
            if (awaitingResponse) {
              safeOpenAISend({ type: "response.cancel" });
              awaitingResponse = false;
            }
            pendingResponseRequest = false;
            requestAssistantResponse("caller_final");
          }
        }
        return;
      }
    }

    if (msg.type === "input_audio_buffer.speech_stopped") {
      return;
    }

    if (msg.type === "response.done") {
      awaitingResponse = false;

      if (Array.isArray(pausedAudioBuffer) && pausedAudioBuffer.length > 0) {
        isFlushingBufferedAudio = true;
        while (pausedAudioBuffer.length > 0) {
          const audioFrame = pausedAudioBuffer.shift();
          safeOpenAISend({ type: "input_audio_buffer.append", audio: audioFrame });
        }
        setTimeout(() => {
          isFlushingBufferedAudio = false;
        }, 50);
      }

      // Final webhook only after _done stages (mandatory fields collected)
      if (flowState.shouldHangup && flowState.finalEvent && !sentCallEnded && !flowState.finalPayloadSent) {
        sentCallEnded = true;
        flowState.finalPayloadSent = true;
        endedAt = endedAt || nowIso();
        const payload = applyWebhookDefaults(buildFinalPayload());
        await sendWebhookEvent(flowState.finalEvent, payload, { wait_for_recording: true });
        if (!hangupRequested) {
          hangupRequested = true;
          completeTwilioCall(callSid);
        }
        try {
          if (openaiWs) openaiWs.close();
        } catch (_) {}
        try {
          if (twilioWs) twilioWs.close();
        } catch (_) {}
      }

      return;
    }

    // AUDIO back to Twilio
    if (msg.type === "response.audio.delta") {
      if (!twilioStreamSid) return;

      safeTwilioSend({
        event: "media",
        streamSid: twilioStreamSid,
        media: { payload: msg.delta || "" }
      });
      return;
    }
  });

  twilioWs.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      error(`[${connTag}] Twilio message JSON parse failed`, e.message);
      return;
    }

    if (msg.event === "start" && msg.start?.streamSid) {
      twilioStreamSid = msg.start.streamSid;
      callSid = msg.start?.callSid || callSid;
      startedAt = startedAt || nowIso();
      const params = msg.start?.customParameters || {};
      const startCaller = params.caller || params.Caller || "";
      const startCalled = params.called || params.Called || "";
      if (startCaller) caller = startCaller;
      if (startCalled) called = startCalled;

      if (!caller) {
        try {
          const u = new URL(req.url || "", "http://localhost");
          caller = u.searchParams.get("caller") || caller;
          called = u.searchParams.get("called") || called;
        } catch (_) {}
      }

      if (!MB_FINAL_WEBHOOK_ONLY) {
        sendWebhookEvent(
          "call_started",
          applyWebhookDefaults({
            callSid,
            streamSid: twilioStreamSid,
            caller,
            called,
            started_at: startedAt,
            language,
            route,
            recording_url_public: makeRecordingPublicUrl(callSid)
          })
        );
      }

      always(
        `[TWILIO_START][${connTag}]`,
        JSON.stringify({
          streamSid: twilioStreamSid,
          callSid: msg.start?.callSid,
          tracks: msg.start?.tracks,
          mediaFormat: msg.start?.mediaFormat
        })
      );
      return;
    }

    if (msg.event === "media" && msg.media?.payload) {
      const payload = msg.media.payload;
      if (!openaiReady || !openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
        pendingAudio.push(payload);
        if (pendingAudio.length > 400) pendingAudio.splice(0, pendingAudio.length - 400);
        return;
      }

      if (awaitingResponse) {
        pausedAudioBuffer.push(payload);
        if (pausedAudioBuffer.length > 400) pausedAudioBuffer.splice(0, pausedAudioBuffer.length - 400);
        return;
      }

      safeOpenAISend({
        type: "input_audio_buffer.append",
        audio: payload
      });
      return;
    }

    if (msg.event === "stop") {
      always(`[TWILIO_STOP][${connTag}]`, "stream stopped");
      endedAt = nowIso();

      // If ended before collecting mandatory fields -> Abandoned webhook (per spec)
      if (!sentCallEnded && !flowState.finalPayloadSent) {
        if (!String(flowState.stage || "").endsWith("_done")) {
          sentCallEnded = true;
          await sendWebhookEvent(
            "Abandoned",
            applyWebhookDefaults({
              callSid,
              streamSid: twilioStreamSid,
              caller,
              called,
              started_at: startedAt,
              ended_at: endedAt,
              language,
              route: flowState.route || route,
              stage: flowState.stage,
              disconnected_stage: flowState.stage,
              caller_last_utterance: lastCallerFinal,
              bot_last_utterance: lastBotFinal,
              transcript: transcriptTurns,
              collected: flowState.data,
              recording_url_public: makeRecordingPublicUrl(callSid)
            }),
            { wait_for_recording: true }
          );
          if (!hangupRequested) {
            hangupRequested = true;
            completeTwilioCall(callSid);
          }
          try {
            if (openaiWs) openaiWs.close();
          } catch (_) {}
          return;
        }

        // If for any reason stop happens after done but before response.done finalized webhook, send final now.
        sentCallEnded = true;
        const finalEvent = flowState.finalEvent || "call_ended";
        const payload = applyWebhookDefaults(buildFinalPayload());
        flowState.finalPayloadSent = true;
        await sendWebhookEvent(finalEvent, payload, { wait_for_recording: true });
        if (!hangupRequested) {
          hangupRequested = true;
          completeTwilioCall(callSid);
        }
      }

      try {
        if (openaiWs) openaiWs.close();
      } catch (_) {}
      return;
    }
  });

  twilioWs.on("error", (e) => {
    RUNTIME.ws_errors += 1;
    error(`[${connTag}] Twilio websocket error`, e?.message || e);
    try {
      if (openaiWs) openaiWs.close();
    } catch (_) {}
  });

  twilioWs.on("close", () => {
    RUNTIME.ws_closed += 1;
    RUNTIME.last_ws_close_at = new Date().toISOString();
    always(`[TWILIO_CLOSE][${connTag}]`, "socket closed");

    // Abandoned if closed unexpectedly before final
    if (!sentCallEnded && !sentCallAbandoned) {
      sentCallAbandoned = true;
      endedAt = endedAt || nowIso();
      sendWebhookEvent(
        "Abandoned",
        applyWebhookDefaults({
          callSid,
          streamSid: twilioStreamSid,
          caller,
          called,
          started_at: startedAt,
          ended_at: endedAt,
          language,
          route: flowState.route || route,
          stage: flowState.stage,
          disconnected_stage: flowState.stage,
          caller_last_utterance: lastCallerFinal,
          bot_last_utterance: lastBotFinal,
          transcript: transcriptTurns,
          collected: flowState.data,
          recording_url_public: makeRecordingPublicUrl(callSid)
        }),
        { wait_for_recording: true }
      );
    }

    if (!hangupRequested && (flowState.finalPayloadSent || sentCallEnded)) {
      hangupRequested = true;
      completeTwilioCall(callSid);
    }

    try {
      if (openaiWs) openaiWs.close();
    } catch (_) {}
  });
});

// --------------------------------------------------
// Start
// --------------------------------------------------
server.listen(PORT, () => {
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
  always("BOOT", {
    at: RUNTIME.booted_at,
    port: PORT,
    MB_DEBUG,
    has_OPENAI_API_KEY: Boolean(OPENAI_API_KEY),
    OPENAI_REALTIME_MODEL,
    OPENAI_VOICE,
    has_GSHEET_ID: Boolean(GSHEET_ID),
    has_GOOGLE_SERVICE_ACCOUNT_JSON_B64: Boolean(GOOGLE_SERVICE_ACCOUNT_JSON_B64),
    has_TWILIO_ACCOUNT_SID: Boolean(TWILIO_ACCOUNT_SID),
    has_TWILIO_AUTH_TOKEN: Boolean(TWILIO_AUTH_TOKEN),
    PUBLIC_BASE_URL,
    TIME_ZONE,
    MB_LOG_TRANSCRIPTS,
    MB_ENABLE_TRANSCRIPTION,
    MB_TRANSCRIPTION_MODEL,
    MB_LOG_RAW_OPENAI
  });
});
