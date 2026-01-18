// server.js
//
// GilSport Realtime Voice Bot – "נטע" (MisterBot-style 1:1)
// Twilio Media Streams <-> OpenAI Realtime API
//
// Fixes in this version:
// 1) No-audio bug: do NOT send opening prompt until Twilio START (streamSid present).
//    Also buffers early audio deltas until streamSid exists.
// 2) Abandoned webhook missing caller/callSid: backfill streamSid from any event,
//    and fetch caller number from Twilio REST if missing.
//
// Dependencies:
//   npm i express ws dotenv googleapis
// Node 18+ recommended (fetch global)

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
  if (!raw) return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}
function envBool(name, def = false) {
  const raw = String(process.env[name] || "").trim().toLowerCase();
  if (!raw) return def;
  return ["1", "true", "yes", "on"].includes(raw);
}
function sanitizeWebhookUrl(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  if (/^MB_[A-Z0-9_]+$/.test(u)) return "";
  if (!/^https?:\/\//i.test(u)) return "";
  return u;
}
function nowIso() {
  return new Date().toISOString();
}

// -----------------------------
// Core ENV
// -----------------------------
const PORT = envNumber("PORT", 10000);

// OpenAI
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

// Parsing / Summary model (HTTP)
const MB_LEAD_PARSING_MODEL = process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini";
const OPENAI_SUMMARY_MODEL = process.env.OPENAI_SUMMARY_MODEL || MB_LEAD_PARSING_MODEL;

// Twilio
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Base URL
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "").trim();

// Debug
const MB_DEBUG = envBool("MB_DEBUG", false);

// Webhooks
const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || ""); // FINAL lead webhook
const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", !!MB_CALL_LOG_WEBHOOK_URL);

// Leads
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", true);
const MB_ENABLE_SMART_LEAD_PARSING = envBool("MB_ENABLE_SMART_LEAD_PARSING", true);

// Recording
const MB_ENABLE_RECORDING = envBool("MB_ENABLE_RECORDING", false);

// Transcription
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);

// Languages (informational / prompt-layer)
const MB_LANGUAGES = (process.env.MB_LANGUAGES || "he,en,ru,ar")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

// VAD defaults (noise hardened)
const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.75);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 150);

// Barge-in
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 1600);

// Idle / Duration
const MB_IDLE_WARNING_MS = envNumber("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNumber("MB_IDLE_HANGUP_MS", 90000);

// Max call
const MB_MAX_CALL_MS = envNumber("MB_MAX_CALL_MS", 5 * 60 * 1000);
const MB_MAX_WARN_BEFORE_MS = envNumber("MB_MAX_WARN_BEFORE_MS", 45000);
const MB_HANGUP_GRACE_MS = envNumber("MB_HANGUP_GRACE_MS", 4000);

// Goodbye handling
const MB_HANGUP_AFTER_GOODBYE = envBool("MB_HANGUP_AFTER_GOODBYE", true);

// Phone strictness
const MB_FORCE_DIGITS_PHONE = envBool("MB_FORCE_DIGITS_PHONE", true);

// Sheets
const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();
const GSHEETS_SETTINGS_TAB = (process.env.GSHEETS_SETTINGS_TAB || "SETTINGS").trim();
const GSHEETS_PROMPTS_TAB = (process.env.GSHEETS_PROMPTS_TAB || "PROMPTS").trim();
const GSHEETS_REFRESH_MS = envNumber("GSHEETS_REFRESH_MS", 30000);

// Safety: must have key
if (!OPENAI_API_KEY) console.error("[FATAL] Missing OPENAI_API_KEY.");
if (!GSHEET_ID) console.error("[FATAL] Missing GSHEET_ID.");
if (!GOOGLE_SERVICE_ACCOUNT_JSON_B64) console.error("[FATAL] Missing GOOGLE_SERVICE_ACCOUNT_JSON_B64.");

// -----------------------------
// Logging helpers
// -----------------------------
function logDebug(tag, msg, extra) {
  if (!MB_DEBUG) return;
  if (extra !== undefined) console.log(`[DEBUG] [${tag}] ${msg}`, extra);
  else console.log(`[DEBUG] [${tag}] ${msg}`);
}
function logInfo(tag, msg, extra) {
  if (extra !== undefined) console.log(`[INFO] [${tag}] ${msg}`, extra);
  else console.log(`[INFO] [${tag}] ${msg}`);
}
function logAlways(msg, extra) {
  if (extra !== undefined) console.log(`[ALWAYS] ${msg}`, extra);
  else console.log(`[ALWAYS] ${msg}`);
}
function logError(tag, msg, extra) {
  if (extra !== undefined) console.error(`[ERROR] [${tag}] ${msg}`, extra);
  else console.error(`[ERROR] [${tag}] ${msg}`);
}

// -----------------------------
// Fetch with timeout
// -----------------------------
async function fetchWithTimeout(url, options = {}, timeoutMs = 4500) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

// -----------------------------
// Simple transcript garbage filter (reduces Whisper hallucinations)
// -----------------------------
function isTranscriptGarbage(t, hasRealUserYet) {
  const s = String(t || "").trim();
  if (!s) return true;

  const low = s.toLowerCase();
  const common = ["thank you", "thanks", "hello", "ok", "okay", "yes", "no", "bye", "goodbye"];
  if (!hasRealUserYet && s.length <= 18 && common.includes(low)) return true;

  const letters = (s.match(/[A-Za-z\u0590-\u05FF]/g) || []).length;
  const total = s.length;
  if (total >= 6 && letters / total < 0.25) return true;

  if (s.split(/\s+/).length === 1 && s.length <= 3 && !/^\d+$/.test(s)) return true;

  return false;
}

// -----------------------------
// Phone normalization (Israel)
// -----------------------------
function digitsOnly(v) {
  if (!v) return null;
  const d = String(v).replace(/\D/g, "");
  return d ? d : null;
}
function toIsraeliLocalFromAny(raw) {
  const d = digitsOnly(raw);
  if (!d) return null;
  if (d.startsWith("0") && (d.length === 9 || d.length === 10)) return d;
  if (d.startsWith("972") && (d.length === 11 || d.length === 12)) return "0" + d.slice(3);
  return null;
}
function toE164FromIsraeliLocal(local) {
  const d = digitsOnly(local);
  if (!d) return null;
  if (d.startsWith("0")) return `+972${d.slice(1)}`;
  if (d.startsWith("972")) return `+${d}`;
  if (String(local).startsWith("+972")) return String(local);
  return null;
}
function isValidIsraeliPhone(digits) {
  if (!/^0\d{8,9}$/.test(digits)) return false;
  const prefix2 = digits.slice(0, 2);
  if (digits.length === 9) return ["02", "03", "04", "07", "08", "09"].includes(prefix2);
  if (prefix2 === "05" || prefix2 === "07") return true;
  if (["02", "03", "04", "07", "08", "09"].includes(prefix2)) return true;
  return false;
}
function normalizePhoneNumber(rawPhone, callerNumber) {
  function clean(num) {
    const d0 = digitsOnly(num);
    if (!d0) return null;
    let d = d0;
    if (d.startsWith("972") && (d.length === 11 || d.length === 12)) d = "0" + d.slice(3);
    if (!d.startsWith("0")) return null;
    if (!isValidIsraeliPhone(d)) return null;
    return d;
  }
  return clean(rawPhone) || clean(callerNumber) || null;
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
// Normalize for closing phrase detection
// -----------------------------
function normalizeForClosing(text) {
  return (text || "")
    .toLowerCase()
    .replace(/["'״׳]/g, "")
    .replace(/[.,!?;:]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

// -----------------------------
// Google Sheets loader (SETTINGS + PROMPTS only)
// -----------------------------
let sheetsCache = { loadedAt: 0, settings: {}, prompts: {} };

function decodeServiceAccountJsonB64(b64) {
  const raw = Buffer.from(b64, "base64").toString("utf8");
  return JSON.parse(raw);
}

async function getSheetsClient() {
  const sa = decodeServiceAccountJsonB64(GOOGLE_SERVICE_ACCOUNT_JSON_B64);
  const jwt = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
  await jwt.authorize();
  return google.sheets({ version: "v4", auth: jwt });
}

function parseSettingsRows(rows) {
  const out = {};
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i] || [];
    const k = String(r[0] || "").trim();
    const v = String(r[1] || "").trim();
    if (!k) continue;
    out[k] = v;
  }
  return out;
}

function parsePromptsRows(rows) {
  const out = {};
  for (let i = 1; i < rows.length; i++) {
    const r = rows[i] || [];
    const id = String(r[0] || "").trim();
    const txt = String(r[1] || "").trim();
    if (!id) continue;
    out[id] = txt;
  }
  return out;
}

async function refreshSheetsCache(tag = "Sheets") {
  const now = Date.now();
  if (sheetsCache.loadedAt && now - sheetsCache.loadedAt < GSHEETS_REFRESH_MS) return;

  try {
    const sheets = await getSheetsClient();
    const ranges = [`${GSHEETS_SETTINGS_TAB}!A:C`, `${GSHEETS_PROMPTS_TAB}!A:B`];
    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: GSHEET_ID,
      ranges,
      valueRenderOption: "FORMATTED_VALUE",
    });

    const valueRanges = res.data.valueRanges || [];
    const settingsRows = valueRanges[0]?.values || [];
    const promptsRows = valueRanges[1]?.values || [];

    const settings = parseSettingsRows(settingsRows);
    const prompts = parsePromptsRows(promptsRows);

    sheetsCache = { loadedAt: now, settings, prompts };

    logInfo(tag, "Sheets cache refreshed.", {
      loadedAt: nowIso(),
      settingsKeys: Object.keys(settings).length,
      promptIds: Object.keys(prompts).length,
    });
  } catch (err) {
    logError(tag, "Failed to refresh sheets cache", err);
  }
}

function getSetting(key, def = "") {
  const v = sheetsCache.settings[key];
  return v !== undefined && v !== null && String(v).trim() !== "" ? String(v).trim() : def;
}
function getPrompt(id, def = "") {
  const v = sheetsCache.prompts[id];
  return v !== undefined && v !== null && String(v).trim() !== "" ? String(v).trim() : def;
}

// -----------------------------
// Build system instructions from SHEETS only
// -----------------------------
const EXTRA_BEHAVIOR_RULES = `
חוקי מערכת קבועים (גבוהים מהפרומפט העסקי):
1. אל תתייחסי למוזיקה, רעשים או איכות הקו. אם לא הבנת – אמרי קצר: "לא שמעתי טוב, אפשר לחזור על זה?"
2. אל תסיימי שיחה רק בגלל "תודה/זהו" וכו'. סיימי רק אחרי אישור ברור.
3. כשמסיימים – אמרי את משפט הסגירה המדויק מהמערכת בלבד.
4. תשובות קצרות (2–3 משפטים) ואז שאלה אחת.
5. סיום טבעי: "לפני שאני מסיימת, יש עוד משהו שתרצו או שהכול ברור?" אם "לא" – אז משפט הסגירה.
6. מספר טלפון: חזרי בדיוק ובקשי "זה נכון?" בלי לנחש.
`.trim();

function buildSystemInstructionsFromSheets() {
  const botName = getSetting("BOT_NAME", "נטע");
  const businessName = getSetting("BUSINESS_NAME", "גיל ספורט");

  const master = getPrompt("MASTER_PROMPT", "");
  const kb = getPrompt("BUSINESS_KB_PROMPT", "");
  const guard = getPrompt("GUARDRAILS_PROMPT", "");

  let instructions = "";
  if (master) instructions += master;
  if (kb) instructions += (instructions ? "\n\n" : "") + kb;
  if (guard) instructions += (instructions ? "\n\n" : "") + guard;

  if (!instructions) {
    instructions = `אתם עוזר קולי בזמן אמת בשם "${botName}" עבור העסק "${businessName}". דברו באופן מקצועי וקצר, ברירת המחדל עברית.`;
  }

  instructions += "\n\n" + EXTRA_BEHAVIOR_RULES;
  return instructions;
}

function getOpeningScriptFromSheets() {
  return getSetting("OPENING_SCRIPT", "שלום! מדברת נטע מגיל ספורט במה אפשר לעזור?");
}
function getClosingScriptFromSheets() {
  return getSetting("CLOSING_SCRIPT", "תודה שפניתם לגיל ספורט. יום נעים ולהתראות.");
}

// -----------------------------
// Twilio REST helpers: auth + fetch caller + hangup + recordings
// -----------------------------
function twilioAuthHeader() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return null;
  return "Basic " + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
}

async function fetchCallerNumberFromTwilio(callSid, tag = "Twilio") {
  const auth = twilioAuthHeader();
  if (!auth || !callSid) return null;
  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`;
    const res = await fetchWithTimeout(
      url,
      { method: "GET", headers: { Authorization: auth } },
      6500
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `fetchCallerNumberFromTwilio HTTP ${res.status}`, txt);
      return null;
    }
    const data = await res.json().catch(() => null);
    return data?.from || null;
  } catch (err) {
    logError(tag, "fetchCallerNumberFromTwilio error", err);
    return null;
  }
}

async function hangupTwilioCall(callSid, tag = "Twilio") {
  if (!callSid) return;
  const auth = twilioAuthHeader();
  if (!auth) return;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`;
    const body = new URLSearchParams({ Status: "completed" });

    const res = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: { Authorization: auth, "Content-Type": "application/x-www-form-urlencoded" },
        body,
      },
      6500
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `Twilio hangup HTTP ${res.status}`, txt);
    } else {
      logInfo(tag, "Twilio hangup requested.");
    }
  } catch (err) {
    logError(tag, "Twilio hangup error", err);
  }
}

async function startTwilioRecording(callSid, tag = "TwilioRec") {
  if (!MB_ENABLE_RECORDING) return null;
  if (!callSid) return null;
  const auth = twilioAuthHeader();
  if (!auth) return null;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}/Recordings.json`;
    const body = new URLSearchParams({
      RecordingChannels: "dual",
      RecordingStatusCallbackEvent: "completed",
    });

    const res = await fetchWithTimeout(
      url,
      { method: "POST", headers: { Authorization: auth, "Content-Type": "application/x-www-form-urlencoded" }, body },
      6500
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `Start recording HTTP ${res.status}`, txt);
      return null;
    }

    const data = await res.json().catch(() => null);
    const sid = data?.sid || null;
    logInfo(tag, "Recording started.", { recording_sid: sid });
    return sid;
  } catch (err) {
    logError(tag, "Start recording error", err);
    return null;
  }
}

async function fetchLatestRecordingForCall(callSid, tag = "TwilioRec") {
  if (!MB_ENABLE_RECORDING) return null;
  if (!callSid) return null;
  const auth = twilioAuthHeader();
  if (!auth) return null;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}/Recordings.json?PageSize=20`;
    const res = await fetchWithTimeout(url, { method: "GET", headers: { Authorization: auth } }, 6500);
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `Fetch recordings HTTP ${res.status}`, txt);
      return null;
    }

    const data = await res.json().catch(() => null);
    const recs = Array.isArray(data?.recordings) ? data.recordings : [];
    if (!recs.length) return null;

    recs.sort((a, b) => String(b.date_created || "").localeCompare(String(a.date_created || "")));
    const r = recs[0];
    const recSid = r.sid || null;
    if (!recSid) return null;

    const apiMp3 = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${recSid}.mp3`;
    const apiWav = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${recSid}.wav`;
    const consoleRec = `https://console.twilio.com/us1/develop/voice/recordings/${recSid}`;
    const consoleCall = callSid ? `https://console.twilio.com/us1/develop/voice/calls/${callSid}` : null;

    return {
      recording_sid: recSid,
      recording_status: r.status || null,
      recording_duration: r.duration || null,
      recording_date_created: r.date_created || null,
      recording_url_api_mp3: apiMp3,
      recording_url_api_wav: apiWav,
      recording_url_console: consoleRec,
      call_url_console: consoleCall,
    };
  } catch (err) {
    logError(tag, "Fetch recordings error", err);
    return null;
  }
}

// -----------------------------
// Lead parsing helper (uses LEAD_CAPTURE_PROMPT from Sheets)
// -----------------------------
async function extractLeadFromConversation(conversationLog, botName, businessName) {
  const tag = "LeadParse";

  if (!MB_ENABLE_SMART_LEAD_PARSING) return null;
  if (!OPENAI_API_KEY) return null;
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;

  const leadCapturePrompt = getPrompt("LEAD_CAPTURE_PROMPT", "").trim();
  const defaultSchemaPrompt = `
החזר אך ורק JSON תקין, בלי טקסט נוסף.
סכמה:
{"is_lead":boolean,"intent":"sales"|"support"|"delivery"|"message"|"unknown","full_name":string|null,"phone_number":string|null,"reason":string|null,"notes":string|null}
`.trim();

  const systemPrompt = leadCapturePrompt || defaultSchemaPrompt;

  const transcript = conversationLog
    .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
    .join("\n");

  const userPrompt = `תמלול שיחה בין לקוח לבין הבוט "${botName}" עבור העסק "${businessName}":\n${transcript}`;

  try {
    const res = await fetchWithTimeout(
      "https://api.openai.com/v1/chat/completions",
      {
        method: "POST",
        headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          model: MB_LEAD_PARSING_MODEL,
          response_format: { type: "json_object" },
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: userPrompt },
          ],
        }),
      },
      8500
    );

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `Lead parsing HTTP ${res.status}`, txt);
      return null;
    }

    const data = await res.json();
    const raw = data.choices?.[0]?.message?.content;
    if (!raw) return null;

    let parsed = null;
    try {
      parsed = JSON.parse(raw);
    } catch (_) {
      parsed = null;
    }
    if (!parsed || typeof parsed !== "object") return null;

    if (MB_FORCE_DIGITS_PHONE) {
      const normalized = normalizePhoneNumber(parsed.phone_number, null);
      parsed.phone_number = normalized || null;
    }

    return parsed;
  } catch (err) {
    logError(tag, "Lead parsing error", err);
    return null;
  }
}

// -----------------------------
// Webhook helpers
// -----------------------------
function mapCallStatus(reason) {
  const r = String(reason || "").toLowerCase();
  if (r.includes("error")) return "error";
  if (r.includes("ws_closed") || r.includes("twilio_stop") || r.includes("stop")) return "abandoned";
  return "completed";
}

async function postJson(url, payload, timeoutMs = 6500) {
  if (!url) return { ok: false, status: 0 };
  try {
    const res = await fetchWithTimeout(
      url,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
      timeoutMs
    );
    return { ok: res.ok, status: res.status, text: await res.text().catch(() => "") };
  } catch (err) {
    return { ok: false, status: 0, text: String(err?.message || err) };
  }
}

// -----------------------------
// Express & HTTP
// -----------------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/health", async (req, res) => {
  await refreshSheetsCache("Health");
  res.status(200).json({
    ok: true,
    ts: nowIso(),
    sheets_loaded_at: sheetsCache.loadedAt ? new Date(sheetsCache.loadedAt).toISOString() : null,
    settings_keys: Object.keys(sheetsCache.settings || {}).length,
    prompt_ids: Object.keys(sheetsCache.prompts || {}).length,
  });
});

// Twilio Voice Webhook (TwiML)
app.post("/twilio-voice", async (req, res) => {
  const host = process.env.DOMAIN || req.headers.host;
  const wsUrl =
    process.env.MB_TWILIO_STREAM_URL ||
    `wss://${String(host || "").replace(/^https?:\/\//, "")}/twilio-media-stream`;

  const caller = req.body.From || "";
  const called = req.body.To || "";
  const direction = req.body.Direction || "inbound";

  const twiml = `
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${caller}"/>
      <Parameter name="called" value="${called}"/>
      <Parameter name="direction" value="${direction}"/>
    </Stream>
  </Connect>
</Response>`.trim();

  res.type("text/xml").send(twiml);
});

const server = http.createServer(app);

// -----------------------------
// WebSocket server for Twilio Media Streams
// -----------------------------
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs, req) => {
  const connId = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  logAlways(`WS connection`, { at: nowIso(), ip: req.socket?.remoteAddress, ua: req.headers["user-agent"], url: req.url });

  let streamSid = null;
  let callSid = null;
  let callerNumber = null;
  let calledNumber = null;
  let callDirection = null;

  let openAiReady = false;
  let twilioStarted = false;

  let twilioClosed = false;
  let openAiClosed = false;
  let callEnded = false;

  let conversationLog = [];
  let currentBotText = "";
  let callStartTs = Date.now();
  let lastMediaTs = Date.now();
  let idleCheckInterval = null;
  let idleWarningSent = false;
  let idleHangupScheduled = false;
  let maxCallTimeout = null;
  let maxCallWarningTimeout = null;

  let botSpeaking = false;
  let hasActiveResponse = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  let pendingHangup = null;
  let graceHangupTimer = null;

  let leadWebhookSent = false;
  let abandonedWebhookSent = false;

  let recordingInfo = null;

  // Buffer early OpenAI audio deltas until streamSid exists (prevents silence)
  const earlyAudioBuffer = [];
  const EARLY_AUDIO_MAX = 120; // about a short burst; protects memory

  refreshSheetsCache("OnConnect").catch(() => {});

  function getGraceMs() {
    const v = Number(MB_HANGUP_GRACE_MS);
    const raw = Number.isFinite(v) && v > 0 ? v : 4000;
    return Math.max(2000, Math.min(raw, 8000));
  }

  function sendTwilioAudioDelta(b64) {
    if (!b64) return;

    if (!streamSid) {
      // buffer until start event arrives
      if (earlyAudioBuffer.length < EARLY_AUDIO_MAX) earlyAudioBuffer.push(b64);
      return;
    }

    if (twilioWs.readyState === WebSocket.OPEN) {
      twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: b64 } }));
    }
  }

  function flushEarlyAudioIfAny() {
    if (!streamSid) return;
    if (!earlyAudioBuffer.length) return;
    while (earlyAudioBuffer.length) {
      const b64 = earlyAudioBuffer.shift();
      sendTwilioAudioDelta(b64);
    }
  }

  function safeCancelResponseIfNeeded(openAiWs) {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;
    if (!hasActiveResponse) return;
    try { openAiWs.send(JSON.stringify({ type: "response.cancel" })); } catch (_) {}
    hasActiveResponse = false;
    botSpeaking = false;
    botTurnActive = false;
  }

  function sendModelPrompt(openAiWs, text, purpose) {
    if (openAiWs.readyState !== WebSocket.OPEN) return;
    if (hasActiveResponse) return;

    openAiWs.send(JSON.stringify({
      type: "conversation.item.create",
      item: { type: "message", role: "user", content: [{ type: "input_text", text }] },
    }));
    openAiWs.send(JSON.stringify({ type: "response.create" }));

    hasActiveResponse = true;
    botTurnActive = true;
    logDebug(connId, `response.create SPEAK purpose=${purpose || "n/a"} text=${text}`);
  }

  function scheduleForceEndAfterGrace(reason, closingMessage) {
    if (callEnded) return;
    if (graceHangupTimer) return;
    const graceMs = getGraceMs();
    graceHangupTimer = setTimeout(() => {
      graceHangupTimer = null;
      endCall(reason, closingMessage).catch(() => {});
    }, graceMs);
  }

  function scheduleEndCall(openAiWs, reason, closingMessage) {
    if (callEnded) return;
    if (pendingHangup) return;

    const msg = closingMessage || getClosingScriptFromSheets();
    pendingHangup = { reason, closingMessage: msg };

    if (openAiWs.readyState === WebSocket.OPEN) {
      sendModelPrompt(openAiWs, `סיימי את השיחה עם הלקוח במשפט הבא בלבד, בלי להוסיף שום משפט נוסף: "${msg}"`, "closing");
    } else {
      scheduleForceEndAfterGrace(reason, msg);
      return;
    }

    setTimeout(() => {
      if (callEnded) return;
      if (!pendingHangup) return;
      const ph = pendingHangup;
      pendingHangup = null;
      scheduleForceEndAfterGrace(ph.reason, ph.closingMessage);
    }, getGraceMs() + 7000);
  }

  function checkBotClosing(text) {
    if (!MB_HANGUP_AFTER_GOODBYE) return;
    const closing = normalizeForClosing(getClosingScriptFromSheets());
    const norm = normalizeForClosing(text);
    if (!closing || !norm) return;
    if (norm.includes(closing) || closing.includes(norm)) {
      scheduleForceEndAfterGrace("bot_closing_config", getClosingScriptFromSheets());
    }
  }

  async function sendCallLogWebhook(payload) {
    if (!MB_CALL_LOG_ENABLED || !MB_CALL_LOG_WEBHOOK_URL) return;
    const r = await postJson(MB_CALL_LOG_WEBHOOK_URL, payload, 8500);
    if (!r.ok) logError(connId, `CallLog webhook failed status=${r.status}`, r.text);
    else logInfo(connId, `CallLog webhook delivered status=${r.status}`);
  }

  async function sendFinalLeadWebhook(payload) {
    if (!MB_ENABLE_LEAD_CAPTURE || !MB_WEBHOOK_URL) return;
    if (leadWebhookSent) return;
    leadWebhookSent = true;

    const r = await postJson(MB_WEBHOOK_URL, payload, 8500);
    if (!r.ok) logError(connId, `FINAL Lead webhook failed status=${r.status}`, r.text);
    else logInfo(connId, `FINAL Lead webhook delivered status=${r.status}`);
  }

  async function sendAbandonedWebhook(payload) {
    if (!MB_ABANDONED_WEBHOOK_URL) return;
    if (abandonedWebhookSent) return;
    abandonedWebhookSent = true;

    const r = await postJson(MB_ABANDONED_WEBHOOK_URL, payload, 8500);
    if (!r.ok) logError(connId, `ABANDONED webhook failed status=${r.status}`, r.text);
    else logInfo(connId, `ABANDONED webhook delivered status=${r.status}`);
  }

  async function endCall(reason, closingMessage) {
    if (callEnded) return;
    callEnded = true;

    if (graceHangupTimer) { clearTimeout(graceHangupTimer); graceHangupTimer = null; }
    if (idleCheckInterval) clearInterval(idleCheckInterval);
    if (maxCallTimeout) clearTimeout(maxCallTimeout);
    if (maxCallWarningTimeout) clearTimeout(maxCallWarningTimeout);

    const botName = getSetting("BOT_NAME", "נטע");
    const businessName = getSetting("BUSINESS_NAME", "גיל ספורט");

    // Ensure latest sheets available for close/log (best effort)
    await refreshSheetsCache("EndCall").catch(() => {});

    // Backfill caller from Twilio REST if missing
    if (!callerNumber && callSid) {
      const from = await fetchCallerNumberFromTwilio(callSid, connId).catch(() => null);
      if (from) callerNumber = from;
    }

    const endedAt = nowIso();
    const startedAt = new Date(callStartTs).toISOString();
    const durationSec = Math.max(0, Math.round((Date.now() - callStartTs) / 1000));

    const lastUser = [...conversationLog].reverse().find((m) => m.from === "user")?.text || null;
    const transcript = conversationLog.map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`).join("\n");

    if (MB_ENABLE_RECORDING && callSid) {
      try { recordingInfo = await fetchLatestRecordingForCall(callSid, connId); } catch (_) {}
    }

    let parsedLead = null;
    try { parsedLead = await extractLeadFromConversation(conversationLog, botName, businessName); } catch (_) {}

    const callerRaw = callerNumber ? String(callerNumber) : null;
    const callerIL = toIsraeliLocalFromAny(callerRaw) || null;
    const callerE164 =
      toE164FromIsraeliLocal(callerIL) || (callerRaw && callerRaw.startsWith("+") ? callerRaw : null);

    let normalizedPhone = null;
    if (parsedLead && typeof parsedLead === "object") {
      normalizedPhone = normalizePhoneNumber(parsedLead.phone_number, callerRaw);
      parsedLead.phone_number = normalizedPhone || parsedLead.phone_number || null;
    }
    const isFullLead = !!(parsedLead && parsedLead.is_lead === true && (parsedLead.phone_number || normalizedPhone));

    const basePayload = {
      event: "call_log",
      call_id: callSid || streamSid || `${connId}`,
      callSid: callSid || null,
      streamSid: streamSid || null,
      call_direction: callDirection || "inbound",
      started_at: startedAt,
      ended_at: endedAt,
      duration_sec: durationSec,
      reason: reason || null,
      call_status: mapCallStatus(reason),

      caller_id_raw: callerRaw,
      caller_id_il: callerIL,
      caller_id_e164: callerE164,

      last_user_utterance: lastUser,
      transcript: transcript || null,
      conversationLog: conversationLog || [],

      business_name: businessName,
      bot_name: botName,

      recording: recordingInfo || null,
      public_base_url: PUBLIC_BASE_URL || null,
      parsedLead: parsedLead || null,
      isFullLead,
    };

    // 1) ALWAYS call log (full documentation)
    await sendCallLogWebhook(basePayload);

    // 2) Final lead only if full lead
    if (isFullLead) {
      await sendFinalLeadWebhook({ ...basePayload, event: "lead_final", phone_number: parsedLead.phone_number, isFullLead: true });
    } else {
      // 3) Abandoned if not full lead (includes caller id as best effort)
      await sendAbandonedWebhook({ ...basePayload, event: "call_abandoned", isFullLead: false });
    }

    if (callSid) hangupTwilioCall(callSid, connId).catch(() => {});

    if (!openAiClosed) {
      openAiClosed = true;
      try { openAiWs.close(); } catch (_) {}
    }
    if (!twilioClosed) {
      twilioClosed = true;
      try { twilioWs.close(); } catch (_) {}
    }
  }

  // -----------------------------
  // OpenAI Realtime WS
  // -----------------------------
  const openAiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    { headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" } }
  );

  logDebug(connId, `Creating OpenAI WS... model=${OPENAI_REALTIME_MODEL} voice=${OPENAI_VOICE}`);

  let sessionConfigured = false;
  let openingSent = false;

  function maybeSendOpening() {
    if (!openAiReady) return;
    if (!twilioStarted) return;
    if (!streamSid) return;
    if (!sessionConfigured) return;
    if (openingSent) return;

    const opening = getOpeningScriptFromSheets();
    openingSent = true;

    sendModelPrompt(
      openAiWs,
      `פתחי את השיחה עם הלקוח במשפט הבא (אפשר לשנות מעט את הניסוח אבל לא להאריך): "${opening}" ואז עצרי והמתיני לתשובה שלו.`,
      "opening_greeting"
    );
  }

  openAiWs.on("open", async () => {
    openAiReady = true;
    logDebug(connId, "OpenAI connected");

    await refreshSheetsCache("Startup").catch(() => {});
    const instructions = buildSystemInstructionsFromSheets();

    logAlways(`[${connId}] SOURCES`, {
      sheets_loaded_at: sheetsCache.loadedAt ? new Date(sheetsCache.loadedAt).toISOString() : null,
      opening_from: "SETTINGS.OPENING_SCRIPT",
      master_from: "PROMPTS.MASTER_PROMPT (+ KB + GUARDRAILS)",
      opening_preview: (getOpeningScriptFromSheets() || "").slice(0, 120),
      master_preview: (getPrompt("MASTER_PROMPT", "") || "").slice(0, 160),
    });

    const effectiveSilenceMs = MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS;

    const sessionUpdate = {
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
          silence_duration_ms: effectiveSilenceMs,
          prefix_padding_ms: MB_VAD_PREFIX_MS,
        },
        // must be >= 0.6 (API constraint you already saw)
        temperature: 0.7,
        instructions,
      },
    };

    openAiWs.send(JSON.stringify(sessionUpdate));
    sessionConfigured = true;

    // IMPORTANT: opening is sent only after Twilio start/streamSid exists
    maybeSendOpening();
  });

  openAiWs.on("message", (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (err) { logError(connId, "Failed to parse OpenAI message", err); return; }

    switch (msg.type) {
      case "response.created":
        currentBotText = "";
        hasActiveResponse = true;
        botTurnActive = true;
        botSpeaking = false;
        noListenUntilTs = Date.now() + MB_NO_BARGE_TAIL_MS;
        break;

      case "response.output_text.delta":
        if (msg.delta) currentBotText += msg.delta;
        break;

      case "response.audio_transcript.delta":
        if (msg.delta) currentBotText += msg.delta;
        break;

      case "response.output_text.done":
      case "response.audio_transcript.done": {
        const text = (currentBotText || "").trim();
        if (text) {
          conversationLog.push({ from: "bot", text });
          logAlways(`[BOT][${connId}] ${text}`);
          checkBotClosing(text);
        }
        currentBotText = "";
        break;
      }

      case "response.audio.delta": {
        const b64 = msg.delta;
        if (!b64) break;
        botSpeaking = true;
        noListenUntilTs = Date.now() + MB_NO_BARGE_TAIL_MS;

        // This now buffers if streamSid not ready
        sendTwilioAudioDelta(b64);
        break;
      }

      case "response.audio.done": {
        botSpeaking = false;
        botTurnActive = false;

        if (pendingHangup && !callEnded) {
          const ph = pendingHangup;
          pendingHangup = null;
          scheduleForceEndAfterGrace(ph.reason, ph.closingMessage);
        }
        break;
      }

      case "response.completed": {
        botSpeaking = false;
        hasActiveResponse = false;
        botTurnActive = false;

        if (pendingHangup && !callEnded) {
          const ph = pendingHangup;
          pendingHangup = null;
          scheduleForceEndAfterGrace(ph.reason, ph.closingMessage);
        }
        break;
      }

      case "conversation.item.input_audio_transcription.completed": {
        if (!MB_ENABLE_TRANSCRIPTION || !MB_LOG_TRANSCRIPTS) break;

        const raw = String(msg.transcript || "").trim();
        const hasRealUserYet = conversationLog.some((m) => m.from === "user" && (m.text || "").trim().length >= 4);
        if (!raw) break;
        if (isTranscriptGarbage(raw, hasRealUserYet)) { logDebug(connId, `Filtered garbage transcript: "${raw}"`); break; }

        const t = raw.replace(/\s+/g, " ").replace(/\s+([,.:;!?])/g, "$1").trim();
        if (!t) break;

        conversationLog.push({ from: "user", text: t });
        logAlways(`[CALLER][${connId}] ${t}`);
        break;
      }

      case "error":
        logError(connId, "OpenAI error event", msg);
        hasActiveResponse = false;
        botSpeaking = false;
        botTurnActive = false;
        noListenUntilTs = 0;
        if (!callEnded) endCall("openai_error", getClosingScriptFromSheets()).catch(() => {});
        break;

      default:
        break;
    }
  });

  openAiWs.on("close", () => {
    openAiClosed = true;
    logDebug(connId, "OpenAI closed");
    if (!callEnded) endCall("openai_ws_closed", getClosingScriptFromSheets()).catch(() => {});
  });

  openAiWs.on("error", (err) => {
    logError(connId, "OpenAI WS error", err);
    if (!openAiClosed) { openAiClosed = true; try { openAiWs.close(); } catch (_) {} }
    if (!callEnded) endCall("openai_ws_error", getClosingScriptFromSheets()).catch(() => {});
  });

  // -----------------------------
  // Twilio Media Stream handlers
  // -----------------------------
  function backfillStreamSidFromAnyMsg(m) {
    if (streamSid) return;
    const sid = m?.streamSid || m?.start?.streamSid || null;
    if (sid) streamSid = sid;
  }

  twilioWs.on("message", async (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (err) { logError(connId, "Failed to parse Twilio WS message", err); return; }

    const event = msg.event;

    // Backfill streamSid from any message shape
    backfillStreamSidFromAnyMsg(msg);

    if (event === "start") {
      streamSid = msg.start?.streamSid || streamSid || null;
      callSid = msg.start?.callSid || null;

      const cp = msg.start?.customParameters || {};
      callerNumber = cp.caller || cp.From || cp.from || msg.start?.caller || msg.start?.from || callerNumber || null;
      calledNumber = cp.called || cp.To || cp.to || msg.start?.to || calledNumber || null;
      callDirection = cp.direction || msg.start?.direction || "inbound";

      twilioStarted = true;
      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logAlways(`[TWILIO_START][${connId}] ${JSON.stringify(msg.start || {})}`);

      // Start recording if enabled
      if (MB_ENABLE_RECORDING && callSid) {
        startTwilioRecording(callSid, connId).catch(() => {});
      }

      // Now streamSid exists -> flush any early audio
      flushEarlyAudioIfAny();

      // If OpenAI session already configured, send opening now
      maybeSendOpening();

      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          idleWarningSent = true;
          if (openAiReady && openAiWs.readyState === WebSocket.OPEN) {
            sendModelPrompt(openAiWs, `אם הלקוח שקט, אמרי משפט קצר: "אני עדיין כאן על הקו, אתם איתי?" ואז המתיני.`, "idle_warning");
          }
        }

        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          if (openAiReady && openAiWs.readyState === WebSocket.OPEN) {
            scheduleEndCall(openAiWs, "idle_timeout", getClosingScriptFromSheets());
          } else {
            endCall("idle_timeout", getClosingScriptFromSheets()).catch(() => {});
          }
        }
      }, 1000);

      if (MB_MAX_CALL_MS > 0) {
        if (MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
          maxCallWarningTimeout = setTimeout(() => {
            if (callEnded) return;
            if (openAiReady && openAiWs.readyState === WebSocket.OPEN) {
              sendModelPrompt(openAiWs, `תני משפט קצר: "אנחנו מתקרבים לסיום הזמן לשיחה הזאת. אם תרצו, אפשר לסכם ולהשאיר פרטים."`, "max_call_warning");
            }
          }, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
        }

        maxCallTimeout = setTimeout(() => {
          if (callEnded) return;
          if (openAiReady && openAiWs.readyState === WebSocket.OPEN) {
            scheduleEndCall(openAiWs, "max_call_duration", getClosingScriptFromSheets());
          } else {
            endCall("max_call_duration", getClosingScriptFromSheets()).catch(() => {});
          }
        }, MB_MAX_CALL_MS);
      }

    } else if (event === "media") {
      lastMediaTs = Date.now();
      const payload = msg.media?.payload;
      if (!payload) return;
      if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;

      const now = Date.now();
      if (!MB_ALLOW_BARGE_IN) {
        if (botTurnActive || botSpeaking || now < noListenUntilTs) return;
      }

      openAiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));

    } else if (event === "stop") {
      // stop sometimes includes streamSid at top-level
      if (msg.streamSid && !streamSid) streamSid = msg.streamSid;

      logAlways(`[TWILIO_STOP][${connId}] stream stopped`);
      twilioClosed = true;
      if (!callEnded) endCall("twilio_stop", getClosingScriptFromSheets()).catch(() => {});
    }
  });

  twilioWs.on("close", () => {
    twilioClosed = true;
    logAlways(`[TWILIO_CLOSE][${connId}] socket closed`);
    if (!callEnded) endCall("twilio_ws_closed", getClosingScriptFromSheets()).catch(() => {});
  });

  twilioWs.on("error", (err) => {
    twilioClosed = true;
    logError(connId, "Twilio WS error", err);
    if (!callEnded) endCall("twilio_ws_error", getClosingScriptFromSheets()).catch(() => {});
  });
});

// -----------------------------
// Start server
// -----------------------------
server.listen(PORT, () => {
  console.log(`✅ GilSport Realtime Voice Bot (MisterBot-style) running on port ${PORT}`);
  console.log(`[CONFIG] OPENAI_REALTIME_MODEL=${OPENAI_REALTIME_MODEL}, OPENAI_VOICE=${OPENAI_VOICE}`);
  console.log(`[CONFIG] Sheets: GSHEET_ID=${GSHEET_ID}, Tabs: ${GSHEETS_SETTINGS_TAB}, ${GSHEETS_PROMPTS_TAB}, refresh=${GSHEETS_REFRESH_MS}ms`);
  console.log(`[CONFIG] Webhooks: CALL_LOG=${MB_CALL_LOG_ENABLED && !!MB_CALL_LOG_WEBHOOK_URL}, FINAL=${!!MB_WEBHOOK_URL}, ABANDONED=${!!MB_ABANDONED_WEBHOOK_URL}`);
  console.log(`[CONFIG] Recording: MB_ENABLE_RECORDING=${MB_ENABLE_RECORDING}`);
  refreshSheetsCache("Startup").catch(() => {});
});
