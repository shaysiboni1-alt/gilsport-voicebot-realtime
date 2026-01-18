// server.js
//
// GilSport Realtime Voice Bot – "נטע" (MisterBot-style, Sheets-only prompts)
// Twilio Media Streams <-> OpenAI Realtime API
//
// Key goals:
// - EXACT MisterBot architecture: clean prompts, no flow FSM.
// - Opening/Closing/Prompts ONLY from Google Sheets (SETTINGS + PROMPTS).
// - Whisper transcription enabled.
// - Smart lead parsing via LLM (MB_LEAD_PARSING_MODEL).
// - Call log webhook for EVERY call (MB_CALL_LOG_WEBHOOK_URL).
// - Abandoned webhook on early hangup (MB_ABANDONED_WEBHOOK_URL).
// - Optional call recording + recording_url using Twilio REST API (MB_ENABLE_RECORDING).
// - Optional Memory with TTL across calls (MB_ENABLE_MEMORY, MB_MEMORY_TTL_MINUTES, MB_MEMORY_KEY_MODE).
//
// Required ENV:
// - PORT (Render sets), OPENAI_API_KEY, OPENAI_REALTIME_MODEL, OPENAI_VOICE
// - GOOGLE_SERVICE_ACCOUNT_JSON_B64, GSHEET_ID
// - TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN (for hangup + recording lookup)
// - PUBLIC_BASE_URL, TIME_ZONE
//
// Webhooks:
// - MB_WEBHOOK_URL (lead webhook; sent only when "full lead" is detected)
// - MB_CALL_LOG_WEBHOOK_URL (always, if MB_CALL_LOG_ENABLED=true)
// - MB_ABANDONED_WEBHOOK_URL (if set; on early hangup)
//
// Lead parsing model:
// - MB_LEAD_PARSING_MODEL = gpt-4.1-mini (recommended)
// - OPENAI_SUMMARY_MODEL = gpt-4.1-mini (for memory summaries)
//
// Notes:
// - No external NLP layer. “NLP” = Whisper + LLM parsing (like MisterBot).
// - Temperature must be >= 0.6 for some realtime models; we use 0.7.

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const fetch = global.fetch || require("node-fetch");

// -----------------------------
// ENV helpers
// -----------------------------
function envNumber(name, def) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}
function envBool(name, def = false) {
  const raw = (process.env[name] || "").toLowerCase().trim();
  if (!raw) return def;
  return ["1", "true", "yes", "on"].includes(raw);
}
function sanitizeWebhookUrl(url) {
  const u = (url || "").trim();
  if (!u) return "";
  if (/^MB_[A-Z0-9_]+$/.test(u)) return "";
  if (!/^https?:\/\//i.test(u)) return "";
  return u;
}

// -----------------------------
// Core ENV
// -----------------------------
const PORT = envNumber("PORT", 10000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
if (!OPENAI_API_KEY) console.error("❌ Missing OPENAI_API_KEY in ENV.");

const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";
const OPENAI_SUMMARY_MODEL = process.env.OPENAI_SUMMARY_MODEL || "gpt-4.1-mini";

const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";
const GSHEET_ID = process.env.GSHEET_ID || "";

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || "";

const MB_DEBUG = envBool("MB_DEBUG", false);

const MB_LANGUAGES = (process.env.MB_LANGUAGES || "he,en,ru,ar")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// VAD / barge-in
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 1600);

const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.75);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 150);

// Idle / max call
const MB_IDLE_WARNING_MS = envNumber("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNumber("MB_IDLE_HANGUP_MS", 90000);
const MB_MAX_CALL_MS = envNumber("MB_MAX_CALL_MS", 5 * 60 * 1000);
const MB_MAX_WARN_BEFORE_MS = envNumber("MB_MAX_WARN_BEFORE_MS", 45000);

const MB_HANGUP_AFTER_GOODBYE = envBool("MB_HANGUP_AFTER_GOODBYE", true);
const MB_HANGUP_GRACE_MS = envNumber("MB_HANGUP_GRACE_MS", 4000);

// Transcription
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);

// Lead capture + parsing
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", true);
const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || "");
const MB_ENABLE_SMART_LEAD_PARSING = envBool("MB_ENABLE_SMART_LEAD_PARSING", true);
const MB_LEAD_PARSING_MODEL = process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini";

// Call log
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", false);
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");

// Abandoned
const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");

// Recording
const MB_ENABLE_RECORDING = envBool("MB_ENABLE_RECORDING", false);

// Memory
const MB_ENABLE_MEMORY = envBool("MB_ENABLE_MEMORY", false);
const MB_MEMORY_TTL_MINUTES = envNumber("MB_MEMORY_TTL_MINUTES", 30);
const MB_MEMORY_KEY_MODE = (process.env.MB_MEMORY_KEY_MODE || "caller").toLowerCase().trim(); // caller | callSid

// Twilio auth
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// -----------------------------
// Logging helpers
// -----------------------------
function logDebug(tag, msg, extra) {
  if (!MB_DEBUG) return;
  if (extra !== undefined) console.log(`[DEBUG][${tag}] ${msg}`, extra);
  else console.log(`[DEBUG][${tag}] ${msg}`);
}
function logInfo(tag, msg, extra) {
  if (extra !== undefined) console.log(`[INFO][${tag}] ${msg}`, extra);
  else console.log(`[INFO][${tag}] ${msg}`);
}
function logError(tag, msg, extra) {
  if (extra !== undefined) console.error(`[ERROR][${tag}] ${msg}`, extra);
  else console.error(`[ERROR][${tag}] ${msg}`);
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 4500) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(t);
  }
}

// -----------------------------
// Google Sheets loader (SETTINGS + PROMPTS only)
// -----------------------------
let sheetsCache = {
  loadedAt: 0,
  settings: {}, // key->value
  prompts: {},  // prompt_id->content_he
};
const SHEETS_MIN_REFRESH_MS = envNumber("MB_SHEETS_MIN_REFRESH_MS", 60 * 1000);

function decodeServiceAccountJsonFromB64(b64) {
  if (!b64) return null;
  try {
    const jsonStr = Buffer.from(b64, "base64").toString("utf8");
    return JSON.parse(jsonStr);
  } catch (e) {
    return null;
  }
}

async function loadSheetsIfNeeded(tag = "Sheets") {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) {
    logError(tag, "Missing GSHEET_ID or GOOGLE_SERVICE_ACCOUNT_JSON_B64");
    return;
  }

  const now = Date.now();
  if (sheetsCache.loadedAt && now - sheetsCache.loadedAt < SHEETS_MIN_REFRESH_MS) return;

  const sa = decodeServiceAccountJsonFromB64(GOOGLE_SERVICE_ACCOUNT_JSON_B64);
  if (!sa || !sa.client_email || !sa.private_key) {
    logError(tag, "Invalid GOOGLE_SERVICE_ACCOUNT_JSON_B64");
    return;
  }

  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  const sheets = google.sheets({ version: "v4", auth });

  // SETTINGS: columns A=key, B=value (with headers on row 1)
  // PROMPTS: columns A=prompt_id, B=content_he
  const ranges = ["SETTINGS!A:B", "PROMPTS!A:B"];
  const res = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: GSHEET_ID,
    ranges,
    majorDimension: "ROWS",
  });

  const valueRanges = res.data.valueRanges || [];
  const next = { settings: {}, prompts: {} };

  for (const vr of valueRanges) {
    const range = (vr.range || "").toUpperCase();
    const rows = vr.values || [];
    if (range.includes("SETTINGS!")) {
      // skip header row
      for (let i = 1; i < rows.length; i++) {
        const [k, v] = rows[i];
        const key = (k || "").trim();
        if (!key) continue;
        next.settings[key] = (v || "").toString();
      }
    } else if (range.includes("PROMPTS!")) {
      for (let i = 1; i < rows.length; i++) {
        const [pid, content] = rows[i];
        const id = (pid || "").trim();
        if (!id) continue;
        next.prompts[id] = (content || "").toString();
      }
    }
  }

  sheetsCache = {
    loadedAt: Date.now(),
    settings: next.settings,
    prompts: next.prompts,
  };

  logInfo(tag, `Sheets loaded. settings=${Object.keys(next.settings).length} prompts=${Object.keys(next.prompts).length}`);
}

function sSetting(key, fallback = "") {
  const v = sheetsCache.settings[key];
  const out = (v === undefined || v === null) ? "" : String(v);
  return out.trim() || fallback;
}
function sPrompt(id, fallback = "") {
  const v = sheetsCache.prompts[id];
  const out = (v === undefined || v === null) ? "" : String(v);
  return out.trim() || fallback;
}

// -----------------------------
// Memory store (in-process)
// -----------------------------
const memoryStore = new Map(); // key -> { text, expiresAt, updatedAt }
function memKey({ callerNumber, callSid }) {
  if (MB_MEMORY_KEY_MODE === "callsid") return callSid || "";
  return callerNumber || "";
}
function getMemory(key) {
  if (!key) return "";
  const m = memoryStore.get(key);
  if (!m) return "";
  if (Date.now() > m.expiresAt) {
    memoryStore.delete(key);
    return "";
  }
  return (m.text || "").trim();
}
function setMemory(key, text) {
  if (!key) return;
  const ttlMs = Math.max(1, MB_MEMORY_TTL_MINUTES) * 60 * 1000;
  memoryStore.set(key, {
    text: (text || "").trim(),
    updatedAt: Date.now(),
    expiresAt: Date.now() + ttlMs,
  });
}

// -----------------------------
// Twilio helpers (hangup + recording)
// -----------------------------
function twilioAuthHeader() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return null;
  return "Basic " + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
}

async function hangupTwilioCall(callSid, tag = "Twilio") {
  if (!callSid) return;
  const auth = twilioAuthHeader();
  if (!auth) return;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`;
    const body = new URLSearchParams({ Status: "completed" });
    const res = await fetch(url, {
      method: "POST",
      headers: { Authorization: auth, "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `hangupTwilioCall HTTP ${res.status}`, txt);
    }
  } catch (e) {
    logError(tag, "hangupTwilioCall error", e);
  }
}

async function startTwilioRecording(callSid, tag = "Twilio") {
  if (!MB_ENABLE_RECORDING) return null;
  if (!callSid) return null;
  const auth = twilioAuthHeader();
  if (!auth) return null;

  try {
    // Start a recording for the call
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}/Recordings.json`;
    const body = new URLSearchParams({
      RecordingChannels: "dual",
    });
    const res = await fetch(url, {
      method: "POST",
      headers: { Authorization: auth, "Content-Type": "application/x-www-form-urlencoded" },
      body,
    });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `startTwilioRecording HTTP ${res.status}`, txt);
      return null;
    }
    const data = await res.json().catch(() => null);
    const sid = data && data.sid ? String(data.sid) : null;
    return sid;
  } catch (e) {
    logError(tag, "startTwilioRecording error", e);
    return null;
  }
}

async function fetchRecordingUrlForCall(callSid, tag = "Twilio") {
  if (!MB_ENABLE_RECORDING) return { recording_sid: null, recording_url: null };
  if (!callSid) return { recording_sid: null, recording_url: null };
  const auth = twilioAuthHeader();
  if (!auth) return { recording_sid: null, recording_url: null };

  try {
    const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}/Recordings.json`;
    const res = await fetch(listUrl, { method: "GET", headers: { Authorization: auth } });
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `fetchRecordingUrlForCall HTTP ${res.status}`, txt);
      return { recording_sid: null, recording_url: null };
    }
    const data = await res.json().catch(() => null);
    const rec = data && Array.isArray(data.recordings) && data.recordings.length ? data.recordings[0] : null;
    if (!rec) return { recording_sid: null, recording_url: null };

    const recording_sid = rec.sid ? String(rec.sid) : null;
    const uri = rec.uri ? String(rec.uri) : null;
    // Twilio recording media URL (requires auth). We provide mp3 form for convenience.
    const recording_url = uri ? `https://api.twilio.com${uri.replace(".json", ".mp3")}` : null;

    return { recording_sid, recording_url };
  } catch (e) {
    logError(tag, "fetchRecordingUrlForCall error", e);
    return { recording_sid: null, recording_url: null };
  }
}

// -----------------------------
// Lead parsing (LLM) – MisterBot-style
// -----------------------------
async function extractLeadFromConversation({ botName, businessName, conversationLog }) {
  const tag = "LeadParse";

  if (!MB_ENABLE_SMART_LEAD_PARSING) return null;
  if (!OPENAI_API_KEY) return null;
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;

  try {
    const conversationText = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const leadPrompt = sPrompt("LEAD_CAPTURE_PROMPT", "").trim();
    // If LEAD_CAPTURE_PROMPT exists, we trust it; otherwise fallback to a safe schema.
    const systemPrompt =
      leadPrompt ||
      `
החזר אך ורק JSON תקין לפי הסכמה הבאה (בלי טקסט נוסף):
{
  "is_lead": boolean,
  "intent": "sales"|"support"|"delivery"|"message"|"unknown",
  "full_name": string|null,
  "phone_number": string|null,
  "reason": string|null,
  "notes": string|null
}
`.trim();

    const userPrompt = `
תמלול שיחה בין לקוח לבוט בשם "${botName}" עבור "${businessName}".

תמלול:
${conversationText}
`.trim();

    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        model: MB_LEAD_PARSING_MODEL,
        response_format: { type: "json_object" },
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt },
        ],
        temperature: 0.2,
      }),
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `HTTP ${res.status}`, txt);
      return null;
    }

    const data = await res.json().catch(() => null);
    const raw = data?.choices?.[0]?.message?.content;
    if (!raw) return null;

    let parsed = null;
    try {
      parsed = JSON.parse(raw);
    } catch {
      parsed = raw;
    }

    if (!parsed || typeof parsed !== "object") return null;
    return parsed;
  } catch (e) {
    logError(tag, "extractLeadFromConversation error", e);
    return null;
  }
}

// -----------------------------
// Memory summarization (LLM)
// -----------------------------
async function summarizeForMemory({ botName, businessName, oldMemory, conversationLog }) {
  const tag = "Memory";
  if (!OPENAI_API_KEY) return null;
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;

  try {
    const transcript = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const sys = `
אתה מסכם זיכרון שירות לטלפון עבור העסק "${businessName}".
מטרות:
- לשמור זיכרון קצר ושימושי לשיחות עתידיות (עד 6 שורות).
- לכלול: שם אם ידוע, מה הלקוח רצה, מוצרים/מותגים/דגמים, סטטוס, העדפות, פרטים חשובים.
- לא להמציא. אם לא ידוע — לא לכתוב.
החזר טקסט קצר בלבד (לא JSON), בעברית.
`.trim();

    const usr = `
זיכרון קודם (אם קיים):
${oldMemory ? oldMemory : "(אין)"}

תמלול שיחה חדשה:
${transcript}

עדכן זיכרון מאוחד ותמציתי:
`.trim();

    const res = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: OPENAI_SUMMARY_MODEL,
        messages: [
          { role: "system", content: sys },
          { role: "user", content: usr },
        ],
        temperature: 0.2,
      }),
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `HTTP ${res.status}`, txt);
      return null;
    }

    const data = await res.json().catch(() => null);
    const text = data?.choices?.[0]?.message?.content?.trim();
    return text || null;
  } catch (e) {
    logError(tag, "summarizeForMemory error", e);
    return null;
  }
}

// -----------------------------
// Phone helpers (IL) – digits only, like MisterBot
// -----------------------------
function digitsOnly(v) {
  if (!v) return "";
  return String(v).replace(/\D/g, "");
}
function toIsraeliLocalFromAny(raw) {
  const d = digitsOnly(raw);
  if (!d) return null;
  if (d.startsWith("0") && (d.length === 9 || d.length === 10)) return d;
  if (d.startsWith("972") && (d.length === 11 || d.length === 12)) return "0" + d.slice(3);
  return null;
}
function formatIsraeliPhoneForTts(ilLocalDigits) {
  const d = digitsOnly(ilLocalDigits);
  if (!d || !d.startsWith("0")) return ilLocalDigits;
  if (d.length === 10 && d.startsWith("05")) return `${d.slice(0, 3)}-${d.slice(3, 6)}-${d.slice(6)}`;
  if (d.length === 9) return `${d.slice(0, 2)}-${d.slice(2, 5)}-${d.slice(5)}`;
  if (d.length === 10 && !d.startsWith("05")) return `${d.slice(0, 3)}-${d.slice(3, 6)}-${d.slice(6)}`;
  return d;
}

// -----------------------------
// Build system instructions (Sheets-only prompts)
// -----------------------------
function buildSystemInstructions({ botName, businessName, memoryText }) {
  const master = sPrompt("MASTER_PROMPT", "").trim();
  const kb = sPrompt("BUSINESS_KB_PROMPT", "").trim();
  const guard = sPrompt("GUARDRAILS_PROMPT", "").trim();

  // We keep the “MisterBot” behavioral rules minimal but effective.
  const extraRules = `
כללי מערכת קבועים:
1) דברו בעברית כברירת מחדל (אם הלקוח מדבר שפה אחרת – התאימו).
2) תשובות קצרות וממוקדות (בדרך כלל 1–2 משפטים ואז שאלה אחת).
3) לא להמציא מידע. אם אין מידע – לומר שאין מידע ולהציע להשאיר פרטים.
4) אין לנחש מספרי טלפון. אם צריך טלפון – לבקש ספרות בלבד ולאשר.
5) אל תסיימי שיחה לבד. אם הלקוח מסיים – שאלי "יש עוד משהו?" ורק אז סגרי עם משפט הסגירה מהמערכת.
`.trim();

  const memBlock = (memoryText || "").trim()
    ? `\n\nזיכרון רלוונטי משיחות קודמות עם הלקוח (אם קיים):\n${memoryText}\n`
    : "";

  let instructions = "";
  if (master) instructions += master;
  if (guard) instructions += (instructions ? "\n\n" : "") + guard;
  if (kb) instructions += (instructions ? "\n\n" : "") + kb;
  instructions += (instructions ? "\n\n" : "") + extraRules;
  instructions += memBlock;

  if (!instructions.trim()) {
    instructions = `
אתם עוזר קולי בשם "${botName}" עבור "${businessName}".
ענו קצר וברור, ואל תמציאו מידע.
`.trim();
  }

  return instructions.trim();
}

// -----------------------------
// Express
// -----------------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/health", async (req, res) => {
  try {
    await loadSheetsIfNeeded("Health");
    res.json({
      ok: true,
      sheets_loaded_at: sheetsCache.loadedAt ? new Date(sheetsCache.loadedAt).toISOString() : null,
      prompts: Object.keys(sheetsCache.prompts).length,
      settings: Object.keys(sheetsCache.settings).length,
      model: OPENAI_REALTIME_MODEL,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: "health_failed" });
  }
});

// Twilio voice webhook (TwiML)
function buildTwimlStream(wsUrl, caller, called, direction) {
  return `
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${caller || ""}"/>
      <Parameter name="called" value="${called || ""}"/>
      <Parameter name="direction" value="${direction || "inbound"}"/>
    </Stream>
  </Connect>
</Response>`.trim();
}

app.post(["/twilio-voice", "/voice"], async (req, res) => {
  await loadSheetsIfNeeded("Twilio-Voice").catch(() => {});

  const host = process.env.DOMAIN || req.headers.host;
  const wsUrl =
    process.env.MB_TWILIO_STREAM_URL ||
    `wss://${String(host || "").replace(/^https?:\/\//, "")}/twilio-media-stream`;

  const caller = req.body.From || "";
  const called = req.body.To || "";
  const direction = req.body.Direction || "inbound";

  res.type("text/xml").send(buildTwimlStream(wsUrl, caller, called, direction));
});

const server = http.createServer(app);

// -----------------------------
// WebSocket server for Twilio Media Streams
// -----------------------------
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// -----------------------------
// Per-call handler
// -----------------------------
wss.on("connection", async (twilioWs, req) => {
  const connId = `conn_${Math.random().toString(36).slice(2, 8)}_${Math.random().toString(36).slice(2, 6)}`;
  const tag = connId;

  logInfo("WS", "connection", {
    at: new Date().toISOString(),
    ip: req.socket?.remoteAddress,
    ua: req.headers["user-agent"],
    url: req.url,
    connId,
  });

  await loadSheetsIfNeeded("OnConnect").catch(() => {});

  const botName = sSetting("BOT_NAME", "נטע");
  const businessName = sSetting("BUSINESS_NAME", "גיל ספורט");
  const openingScript = sSetting("OPENING_SCRIPT", `שלום! מדברת ${botName} מ${businessName} במה אפשר לעזור?`);
  const closingScript = sSetting("CLOSING_SCRIPT", `תודה שדיברתם עם ${businessName}. יום נעים ולהתראות.`);

  let streamSid = null;
  let callSid = null;
  let callerNumber = null;
  let calledNumber = null;
  let direction = "inbound";

  let callStartTs = Date.now();
  let lastMediaTs = Date.now();

  let conversationLog = []; // [{from:'user'|'bot', text}]
  let currentBotText = "";

  let openAiReady = false;
  let twilioClosed = false;
  let openAiClosed = false;
  let callEnded = false;

  let hasActiveResponse = false;
  let botSpeaking = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  let idleWarningSent = false;
  let idleHangupScheduled = false;

  let idleInterval = null;
  let maxCallTimeout = null;
  let maxWarnTimeout = null;

  let leadWebhookSent = false;
  let callLogSent = false;
  let abandonedSent = false;

  let recordingStartSid = null;

  // Memory (inject at start)
  const memoryKeyCandidate = memKey({ callerNumber: null, callSid: null }); // placeholder; will rebuild after start
  let memoryKeyResolved = "";
  let memoryTextAtStart = "";

  function getGraceMs() {
    const v = MB_HANGUP_GRACE_MS > 0 ? MB_HANGUP_GRACE_MS : 4000;
    return Math.max(2000, Math.min(v, 8000));
  }

  function safeSendToTwilioAudio(b64) {
    if (!b64 || !streamSid) return;
    if (twilioWs.readyState !== WebSocket.OPEN) return;
    twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: b64 } }));
  }

  function safeCancelResponse() {
    if (!openAiReady) return;
    if (openAiWs.readyState !== WebSocket.OPEN) return;
    if (!hasActiveResponse) return;
    try {
      openAiWs.send(JSON.stringify({ type: "response.cancel" }));
    } catch (_) {}
    hasActiveResponse = false;
    botSpeaking = false;
    botTurnActive = false;
  }

  function sendUserTextToModel(text, purpose = "user_text") {
    if (!openAiReady) return;
    if (openAiWs.readyState !== WebSocket.OPEN) return;
    if (hasActiveResponse) return;

    openAiWs.send(JSON.stringify({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text }],
      },
    }));
    openAiWs.send(JSON.stringify({ type: "response.create" }));
    hasActiveResponse = true;
    botTurnActive = true;
    logDebug(tag, `response.create (${purpose})`);
  }

  // Determine if this looks like a graceful close
  function lastBotWasClosing() {
    const lastBot = [...conversationLog].reverse().find((m) => m.from === "bot")?.text || "";
    if (!lastBot) return false;
    const a = lastBot.replace(/\s+/g, " ").trim();
    const b = closingScript.replace(/\s+/g, " ").trim();
    return a.includes(b) || b.includes(a);
  }

  // Abandoned payload rule: caller ID + last user utterance
  function lastUserUtterance() {
    return [...conversationLog].reverse().find((m) => m.from === "user")?.text || "";
  }

  async function sendAbandonedWebhook(reason) {
    if (!MB_ABANDONED_WEBHOOK_URL) return;
    if (abandonedSent) return;
    abandonedSent = true;

    try {
      const payload = {
        event: "call_abandoned",
        reason,
        callSid,
        streamSid,
        caller_id: callerNumber || null,
        called: calledNumber || null,
        direction,
        started_at: new Date(callStartTs).toISOString(),
        ended_at: new Date().toISOString(),
        duration_sec: Math.max(0, Math.round((Date.now() - callStartTs) / 1000)),
        last_user_utterance: lastUserUtterance() || null,
        transcript: conversationLog
          .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
          .join("\n"),
      };

      await fetchWithTimeout(
        MB_ABANDONED_WEBHOOK_URL,
        { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
        4500
      ).catch(() => {});
      logInfo(tag, "Abandoned webhook sent.");
    } catch (e) {
      logError(tag, "Abandoned webhook error", e);
    }
  }

  async function sendCallLogWebhook({ reason, parsedLead, recording }) {
    if (!MB_CALL_LOG_ENABLED || !MB_CALL_LOG_WEBHOOK_URL) return;
    if (callLogSent) return;
    callLogSent = true;

    try {
      const transcript = conversationLog
        .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
        .join("\n");

      const payload = {
        event: "call_log",
        reason,
        callSid,
        streamSid,
        caller_id: callerNumber || null,
        called: calledNumber || null,
        direction,
        started_at: new Date(callStartTs).toISOString(),
        ended_at: new Date().toISOString(),
        duration_sec: Math.max(0, Math.round((Date.now() - callStartTs) / 1000)),
        transcript,
        last_user_utterance: lastUserUtterance() || null,
        parsedLead: parsedLead || null,
        recording_sid: recording?.recording_sid || null,
        recording_url: recording?.recording_url || null,
        memory_key: memoryKeyResolved || null,
      };

      await fetchWithTimeout(
        MB_CALL_LOG_WEBHOOK_URL,
        { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
        4500
      ).catch(() => {});
      logInfo(tag, "Call log webhook sent.");
    } catch (e) {
      logError(tag, "Call log webhook error", e);
    }
  }

  async function sendLeadWebhook({ reason, parsedLead, recording }) {
    if (!MB_ENABLE_LEAD_CAPTURE || !MB_WEBHOOK_URL) return;
    if (leadWebhookSent) return;

    // MisterBot rule: only send if full lead (is_lead true + phone_number exists)
    const phoneLocal = parsedLead?.phone_number ? toIsraeliLocalFromAny(parsedLead.phone_number) : null;
    const callerLocal = callerNumber ? toIsraeliLocalFromAny(callerNumber) : null;
    const finalPhone = phoneLocal || callerLocal || null;

    const isFullLead = parsedLead?.is_lead === true && !!finalPhone;
    if (!isFullLead) return;

    leadWebhookSent = true;

    const transcript = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const payload = {
      event: "lead",
      reason,
      callSid,
      streamSid,
      botName,
      businessName,
      caller_id: callerNumber || null,
      called: calledNumber || null,
      direction,
      started_at: new Date(callStartTs).toISOString(),
      ended_at: new Date().toISOString(),
      duration_sec: Math.max(0, Math.round((Date.now() - callStartTs) / 1000)),
      phone_number: finalPhone ? (finalPhone.startsWith("0") ? `+972${finalPhone.slice(1)}` : finalPhone) : null,
      phone_local: finalPhone || null,
      transcript,
      last_user_utterance: lastUserUtterance() || null,
      parsedLead: {
        ...parsedLead,
        phone_number: finalPhone || parsedLead?.phone_number || null,
      },
      recording_sid: recording?.recording_sid || null,
      recording_url: recording?.recording_url || null,
      memory_key: memoryKeyResolved || null,
    };

    try {
      await fetch(MB_WEBHOOK_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      logInfo(tag, "Lead webhook sent.");
    } catch (e) {
      logError(tag, "Lead webhook error", e);
    }
  }

  async function endCall(reason) {
    if (callEnded) return;
    callEnded = true;

    if (idleInterval) clearInterval(idleInterval);
    if (maxCallTimeout) clearTimeout(maxCallTimeout);
    if (maxWarnTimeout) clearTimeout(maxWarnTimeout);

    // Fetch recording URL (best effort) + lead parsing + memory update (all non-blocking-ish)
    const snapshot = {
      reason,
      callSid,
      streamSid,
      callerNumber,
      calledNumber,
      direction,
      callStartTs,
      conversationLog: [...conversationLog],
      memoryKeyResolved,
      memoryTextAtStart,
    };

    // Close sockets fast
    if (!openAiClosed && openAiWs.readyState === WebSocket.OPEN) {
      openAiClosed = true;
      try { openAiWs.close(); } catch (_) {}
    }
    if (!twilioClosed && twilioWs.readyState === WebSocket.OPEN) {
      twilioClosed = true;
      try { twilioWs.close(); } catch (_) {}
    }

    // Hard hangup best effort
    if (snapshot.callSid) hangupTwilioCall(snapshot.callSid, tag).catch(() => {});

    // Background post-call
    (async () => {
      // Recording
      const recording = await fetchRecordingUrlForCall(snapshot.callSid, tag).catch(() => ({ recording_sid: null, recording_url: null }));

      // Lead parsing
      let parsedLead = null;
      try {
        parsedLead = await extractLeadFromConversation({
          botName,
          businessName,
          conversationLog: snapshot.conversationLog,
        });
      } catch (_) {}

      // Call log ALWAYS (if enabled)
      await sendCallLogWebhook({ reason: snapshot.reason, parsedLead, recording }).catch(() => {});

      // Lead webhook only if full lead
      await sendLeadWebhook({ reason: snapshot.reason, parsedLead, recording }).catch(() => {});

      // Abandoned webhook: if not graceful close and ended by stop/close
      const looksGraceful = lastBotWasClosing();
      const looksAbandonedReason = String(snapshot.reason || "").includes("twilio") || String(snapshot.reason || "").includes("ws_closed");
      if (!looksGraceful && looksAbandonedReason) {
        await sendAbandonedWebhook(snapshot.reason).catch(() => {});
      }

      // Memory update (across calls)
      if (MB_ENABLE_MEMORY && snapshot.memoryKeyResolved) {
        try {
          const oldMem = snapshot.memoryTextAtStart || "";
          const newMem = await summarizeForMemory({
            botName,
            businessName,
            oldMemory: oldMem,
            conversationLog: snapshot.conversationLog,
          });
          if (newMem) {
            setMemory(snapshot.memoryKeyResolved, newMem);
            logInfo(tag, "Memory updated.", { key: snapshot.memoryKeyResolved });
          }
        } catch (e) {
          logError(tag, "Memory update failed", e);
        }
      }
    })().catch(() => {});
  }

  // -----------------------------
  // OpenAI Realtime WS
  // -----------------------------
  const openAiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "OpenAI-Beta": "realtime=v1",
    },
  });

  openAiWs.on("open", async () => {
    openAiReady = true;
    logInfo(tag, `OpenAI connected model=${OPENAI_REALTIME_MODEL} voice=${OPENAI_VOICE}`);

    // Ensure sheets fresh (non-blocking)
    await loadSheetsIfNeeded("OpenAI-open").catch(() => {});

    // Memory key can be resolved only after Twilio start; for now we will set instructions without memory.
    const instructions = buildSystemInstructions({
      botName,
      businessName,
      memoryText: "",
    });

    const effectiveSilence = MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS;

    // IMPORTANT: modalities MUST be ['audio','text'] for realtime audio
    openAiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        model: OPENAI_REALTIME_MODEL,
        modalities: ["audio", "text"],
        voice: OPENAI_VOICE,
        input_audio_format: "g711_ulaw",
        output_audio_format: "g711_ulaw",
        input_audio_transcription: MB_ENABLE_TRANSCRIPTION ? { model: MB_TRANSCRIPTION_MODEL } : undefined,
        turn_detection: {
          type: "server_vad",
          threshold: MB_VAD_THRESHOLD,
          silence_duration_ms: effectiveSilence,
          prefix_padding_ms: MB_VAD_PREFIX_MS,
        },
        instructions,
        temperature: 0.7,
      },
    }));

    // Opening: force short greeting from sheet, then wait.
    sendUserTextToModel(
      `פתחי את השיחה במשפט הבא (לא להאריך): "${openingScript}". לאחר מכן עצרי והמתיני לתשובת הלקוח.`,
      "opening"
    );
  });

  openAiWs.on("message", (buf) => {
    let msg = null;
    try {
      msg = JSON.parse(buf.toString());
    } catch {
      return;
    }

    switch (msg.type) {
      case "response.created":
        currentBotText = "";
        hasActiveResponse = true;
        botTurnActive = true;
        botSpeaking = false;
        noListenUntilTs = Date.now() + MB_NO_BARGE_TAIL_MS;
        break;

      case "response.output_text.delta":
      case "response.audio_transcript.delta": {
        const delta = msg.delta || "";
        if (delta) currentBotText += delta;
        break;
      }

      case "response.output_text.done":
      case "response.audio_transcript.done": {
        const text = (currentBotText || "").trim();
        if (text) {
          conversationLog.push({ from: "bot", text });
          logInfo("BOT", `[${tag}] ${text}`);
        }
        currentBotText = "";
        break;
      }

      case "response.audio.delta": {
        const b64 = msg.delta;
        if (!b64) break;
        botSpeaking = true;
        noListenUntilTs = Date.now() + MB_NO_BARGE_TAIL_MS;
        safeSendToTwilioAudio(b64);
        break;
      }

      case "response.audio.done":
      case "response.completed":
        botSpeaking = false;
        hasActiveResponse = false;
        botTurnActive = false;
        break;

      case "conversation.item.input_audio_transcription.completed": {
        const t = (msg.transcript || "").trim();
        if (!t) break;
        conversationLog.push({ from: "user", text: t });
        if (MB_LOG_TRANSCRIPTS) logInfo("CALLER", `[${tag}] ${t}`);
        break;
      }

      case "error":
        logError(tag, "OpenAI error event", msg);
        hasActiveResponse = false;
        botSpeaking = false;
        botTurnActive = false;
        noListenUntilTs = 0;
        break;

      default:
        break;
    }
  });

  openAiWs.on("close", () => {
    openAiClosed = true;
    logInfo(tag, "OpenAI closed");
    if (!callEnded) endCall("openai_ws_closed");
  });

  openAiWs.on("error", (e) => {
    logError(tag, "OpenAI ws error", e);
    if (!openAiClosed) {
      openAiClosed = true;
      try { openAiWs.close(); } catch (_) {}
    }
    if (!callEnded) endCall("openai_ws_error");
  });

  // -----------------------------
  // Twilio WS events
  // -----------------------------
  twilioWs.on("message", async (buf) => {
    let msg = null;
    try {
      msg = JSON.parse(buf.toString());
    } catch {
      return;
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;

      const cp = msg.start?.customParameters || {};
      callerNumber = cp.caller || cp.From || cp.from || msg.start?.caller || msg.start?.from || null;
      calledNumber = cp.called || cp.To || cp.to || msg.start?.to || null;
      direction = cp.direction || msg.start?.direction || "inbound";

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      // Resolve memory key now that caller/callSid exist
      memoryKeyResolved = memKey({ callerNumber, callSid });
      if (MB_ENABLE_MEMORY && memoryKeyResolved) {
        memoryTextAtStart = getMemory(memoryKeyResolved);
        if (memoryTextAtStart) {
          // Inject memory by updating session instructions once (safe, like MisterBot)
          const refreshedInstructions = buildSystemInstructions({
            botName,
            businessName,
            memoryText: memoryTextAtStart,
          });

          if (openAiReady && openAiWs.readyState === WebSocket.OPEN) {
            openAiWs.send(JSON.stringify({
              type: "session.update",
              session: { instructions: refreshedInstructions },
            }));
            logInfo(tag, "Injected memory into session.", { key: memoryKeyResolved });
          }
        }
      }

      logInfo(tag, "TWILIO_START", { streamSid, callSid, callerNumber, calledNumber, direction });

      // Start recording best-effort
      if (MB_ENABLE_RECORDING && callSid) {
        recordingStartSid = await startTwilioRecording(callSid, tag).catch(() => null);
        if (recordingStartSid) logInfo(tag, "Recording started", { recordingStartSid });
      }

      // Idle timer
      idleInterval = setInterval(() => {
        const now = Date.now();
        const since = now - lastMediaTs;

        if (!idleWarningSent && since >= MB_IDLE_WARNING_MS && !callEnded) {
          idleWarningSent = true;
          sendUserTextToModel(
            `אמרי משפט קצר ללקוח: "אני עדיין כאן על הקו, אתם איתי?" ואז המתיני.`,
            "idle_warning"
          );
        }

        if (!idleHangupScheduled && since >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          // Say closing then end after grace
          safeCancelResponse();
          sendUserTextToModel(
            `סיימי את השיחה עם הלקוח במשפט הבא בלבד: "${closingScript}"`,
            "idle_closing"
          );
          setTimeout(() => endCall("idle_timeout"), getGraceMs());
        }
      }, 1000);

      // Max call timers
      if (MB_MAX_CALL_MS > 0 && MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
        maxWarnTimeout = setTimeout(() => {
          if (callEnded) return;
          sendUserTextToModel(
            `אמרי ללקוח משפט קצר: "אנחנו מתקרבים לסיום זמן השיחה. אם תרצו, אפשר לסכם ולהשאיר פרטים." ואז שאלי שאלה אחת.`,
            "max_warn"
          );
        }, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
      }
      if (MB_MAX_CALL_MS > 0) {
        maxCallTimeout = setTimeout(() => {
          if (callEnded) return;
          safeCancelResponse();
          sendUserTextToModel(
            `סיימי את השיחה עם הלקוח במשפט הבא בלבד: "${closingScript}"`,
            "max_closing"
          );
          setTimeout(() => endCall("max_call_duration"), getGraceMs());
        }, MB_MAX_CALL_MS);
      }

      return;
    }

    if (msg.event === "media") {
      lastMediaTs = Date.now();
      const payload = msg.media?.payload;
      if (!payload) return;
      if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;

      const now = Date.now();

      if (!MB_ALLOW_BARGE_IN) {
        if (botTurnActive || botSpeaking || now < noListenUntilTs) {
          logDebug("BargeIn", "Ignored media (bot speaking / tail)", { botTurnActive, botSpeaking });
          return;
        }
      }

      openAiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));
      return;
    }

    if (msg.event === "stop") {
      twilioClosed = true;
      logInfo(tag, "TWILIO_STOP");
      if (!callEnded) endCall("twilio_stop");
      return;
    }
  });

  twilioWs.on("close", () => {
    twilioClosed = true;
    logInfo(tag, "TWILIO_CLOSE");
    if (!callEnded) endCall("twilio_ws_closed");
  });

  twilioWs.on("error", (e) => {
    twilioClosed = true;
    logError(tag, "Twilio ws error", e);
    if (!callEnded) endCall("twilio_ws_error");
  });
});

// -----------------------------
// Start server
// -----------------------------
server.listen(PORT, async () => {
  console.log(`✅ GilSport MisterBot-style Voice Bot running on port ${PORT}`);
  console.log(`[CONFIG] model=${OPENAI_REALTIME_MODEL} voice=${OPENAI_VOICE} tz=${TIME_ZONE}`);
  console.log(`[CONFIG] MB_ENABLE_RECORDING=${MB_ENABLE_RECORDING} MB_ENABLE_MEMORY=${MB_ENABLE_MEMORY} MB_MEMORY_TTL_MINUTES=${MB_MEMORY_TTL_MINUTES} MB_MEMORY_KEY_MODE=${MB_MEMORY_KEY_MODE}`);
  console.log(`[CONFIG] MB_CALL_LOG_ENABLED=${MB_CALL_LOG_ENABLED} MB_ABANDONED_WEBHOOK_URL=${!!MB_ABANDONED_WEBHOOK_URL}`);

  await loadSheetsIfNeeded("Startup").catch(() => {});
});
