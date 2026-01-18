// server.js
//
// GilSport Realtime Voice Bot – "נטע" (MisterBot-style 1:1)
// Twilio Media Streams <-> OpenAI Realtime API
//
// Design principles:
// - All business content (opening/closing/prompts) is loaded ONLY from Google Sheets (two tabs: SETTINGS + PROMPTS).
// - Realtime conversation is handled like MisterBot (single master instructions + no FSM).
// - Deterministic layers for: phone capture, caller-id preference, brand detection, delivery after-hours carrier numbers.
// - Post-call: call log webhook + lead webhook + abandoned webhook + call recording URL included.
//
// Requirements:
//   npm install express ws dotenv googleapis
//   Node 18+ recommended (global fetch available)
//
// Twilio Voice Webhook -> POST /twilio-voice (TwiML)
// Twilio Media Streams -> wss://<domain>/twilio-media-stream
//
// ------------------------------------------------------------

require("dotenv").config();
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

const fetch = global.fetch || require("node-fetch");

// ------------------------------------------------------------
// ENV helpers
// ------------------------------------------------------------
function envNumber(name, def) {
  const raw = process.env[name];
  if (!raw) return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}
function envBool(name, def = false) {
  const raw = (process.env[name] || "").toLowerCase();
  if (!raw) return def;
  return ["1", "true", "yes", "on"].includes(raw);
}
function sanitizeWebhookUrl(url) {
  const u = (url || "").trim();
  if (!u) return "";
  if (/^MB_[A-Z0-9_]+$/.test(u)) return "";
  if (u === "MB_WEBHOOK_URL") return "";
  if (!/^https?:\/\//i.test(u)) return "";
  return u;
}
function nowIso() {
  return new Date().toISOString();
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

// ------------------------------------------------------------
// Core ENV
// ------------------------------------------------------------
const PORT = envNumber("PORT", 10000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
if (!OPENAI_API_KEY) {
  console.error("❌ Missing OPENAI_API_KEY in ENV.");
}

const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

// Twilio (for hangup + recording URL + caller-id backfill)
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Sheets
const GSHEET_ID = process.env.GSHEET_ID || process.env.GOOGLE_SHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_EMAIL = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL || "";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");
const GOOGLE_SERVICE_ACCOUNT_JSON = process.env.GOOGLE_SERVICE_ACCOUNT_JSON || ""; // optional full JSON
const SHEETS_CACHE_TTL_MS = envNumber("SHEETS_CACHE_TTL_MS", 60 * 1000);

// Behavior / audio
const MB_DEBUG = envBool("MB_DEBUG", true);
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 1600);

// VAD
const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 200);

// Idle / duration
const MB_IDLE_WARNING_MS = envNumber("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNumber("MB_IDLE_HANGUP_MS", 90000);
const MB_MAX_CALL_MS = envNumber("MB_MAX_CALL_MS", 5 * 60 * 1000);
const MB_MAX_WARN_BEFORE_MS = envNumber("MB_MAX_WARN_BEFORE_MS", 45000);
const MB_HANGUP_GRACE_MS = envNumber("MB_HANGUP_GRACE_MS", 5000);

// Webhooks
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", !!MB_CALL_LOG_WEBHOOK_URL);

const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || ""); // lead webhook
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", !!MB_WEBHOOK_URL);

const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");
const MB_ENABLE_ABANDONED_WEBHOOK = envBool("MB_ENABLE_ABANDONED_WEBHOOK", !!MB_ABANDONED_WEBHOOK_URL);

// Lead parsing model (separate from realtime)
const MB_LEAD_PARSING_MODEL = process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini";
const MB_ENABLE_SMART_LEAD_PARSING = envBool("MB_ENABLE_SMART_LEAD_PARSING", true);

// ------------------------------------------------------------
// Logging helpers
// ------------------------------------------------------------
function logAlways(...args) {
  console.log(...args);
}
function logInfo(tag, msg, extra) {
  if (extra !== undefined) console.log(`[INFO] [${tag}] ${msg}`, extra);
  else console.log(`[INFO] [${tag}] ${msg}`);
}
function logDebug(tag, msg, extra) {
  if (!MB_DEBUG) return;
  if (extra !== undefined) console.log(`[DEBUG] [${tag}] ${msg}`, extra);
  else console.log(`[DEBUG] [${tag}] ${msg}`);
}
function logError(tag, msg, extra) {
  if (extra !== undefined) console.error(`[ERROR] [${tag}] ${msg}`, extra);
  else console.error(`[ERROR] [${tag}] ${msg}`);
}

// ------------------------------------------------------------
// Phone utilities (Israel)
// ------------------------------------------------------------
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
  if (!local) return null;
  const d = digitsOnly(local);
  if (!d) return null;
  if (d.startsWith("0")) return `+972${d.slice(1)}`;
  if (d.startsWith("972")) return `+${d}`;
  if (d.startsWith("+972")) return d;
  return null;
}
function isValidIsraeliPhone(digits) {
  if (!digits) return false;
  if (!/^0\d{8,9}$/.test(digits)) return false;
  const prefix2 = digits.slice(0, 2);
  if (digits.length === 9) {
    return ["02", "03", "04", "07", "08", "09"].includes(prefix2);
  } else {
    if (prefix2 === "05" || prefix2 === "07") return true;
    if (["02", "03", "04", "07", "08", "09"].includes(prefix2)) return true;
    return false;
  }
}
function normalizePhoneNumber(rawPhone, callerNumber) {
  function clean(num) {
    let digits = digitsOnly(num);
    if (!digits) return null;
    if (digits.startsWith("972")) digits = "0" + digits.slice(3);
    if (!isValidIsraeliPhone(digits)) return null;
    return digits;
  }
  const fromLead = clean(rawPhone);
  if (fromLead) return fromLead;

  const fromCaller = clean(callerNumber);
  if (fromCaller) return fromCaller;

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
function detectPhoneCandidateFromText(text) {
  const digits = String(text || "").replace(/\D/g, "");
  if (!digits) return null;
  const normalized = normalizePhoneNumber(digits, null);
  return normalized;
}
function isYes(text) {
  const t = (text || "").trim().toLowerCase();
  return /^(כן|נכון|בדיוק|אכן|כן נכון|נכון מאוד|כן זה נכון)\b/.test(t);
}
function isNo(text) {
  const t = (text || "").trim().toLowerCase();
  return /^(לא|ממש לא|לא נכון|טעות|זה לא|לא זה)\b/.test(t);
}

// ------------------------------------------------------------
// Twilio helpers
// ------------------------------------------------------------
async function twilioRequest(path, method = "GET", bodyParams = null) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return { ok: false, status: 0, data: null, text: "" };

  const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/${path}`;
  const headers = {
    Authorization: "Basic " + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64"),
  };
  let body = undefined;

  if (bodyParams) {
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    body = new URLSearchParams(bodyParams);
  }

  const res = await fetch(url, { method, headers, body });
  const text = await res.text().catch(() => "");
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch (_) {
    data = null;
  }
  return { ok: res.ok, status: res.status, data, text };
}

async function hangupTwilioCall(callSid, tag = "Call") {
  if (!callSid) return;
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return;
  const r = await twilioRequest(`Calls/${callSid}.json`, "POST", { Status: "completed" });
  if (!r.ok) logError(tag, `Twilio hangup HTTP ${r.status}`, r.text);
  else logInfo(tag, "Twilio hangup requested.");
}

async function fetchCallerNumberFromTwilio(callSid, tag = "Call") {
  if (!callSid) return null;
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return null;
  const r = await twilioRequest(`Calls/${callSid}.json`, "GET");
  if (!r.ok) {
    logError(tag, `fetchCallerNumberFromTwilio HTTP ${r.status}`, r.text);
    return null;
  }
  const fromRaw = r.data?.from || r.data?.From || null;
  return fromRaw || null;
}

async function startTwilioRecording(callSid, tag = "Call") {
  if (!callSid) return null;
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return null;
  // Try to start a recording (if Twilio allows for your plan/config)
  const r = await twilioRequest(`Calls/${callSid}/Recordings.json`, "POST", {
    RecordingStatusCallbackEvent: "completed",
  });
  if (!r.ok) {
    logError(tag, `Recording start HTTP ${r.status}`, r.text);
    return null;
  }
  const sid = r.data?.sid || r.data?.Sid || null;
  return sid || null;
}

function buildRecordingUrl(recordingSid) {
  if (!recordingSid) return null;
  // Twilio recording resource (public access depends on auth; still useful as reference)
  return `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${recordingSid}.json`;
}

// ------------------------------------------------------------
// Sheets loader (two tabs: SETTINGS + PROMPTS)
// ------------------------------------------------------------
let sheetsCache = {
  loadedAt: null,
  settings: {},
  prompts: {},
};

function haveSheetsCreds() {
  if (GOOGLE_SERVICE_ACCOUNT_JSON) return true;
  return !!(GOOGLE_SERVICE_ACCOUNT_EMAIL && GOOGLE_PRIVATE_KEY);
}

function getSheetsAuth() {
  if (!haveSheetsCreds()) return null;

  if (GOOGLE_SERVICE_ACCOUNT_JSON) {
    let parsed = null;
    try {
      parsed = JSON.parse(GOOGLE_SERVICE_ACCOUNT_JSON);
    } catch (e) {
      throw new Error("Invalid GOOGLE_SERVICE_ACCOUNT_JSON");
    }
    const jwt = new google.auth.JWT({
      email: parsed.client_email,
      key: (parsed.private_key || "").replace(/\\n/g, "\n"),
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
    });
    return jwt;
  }

  const jwt = new google.auth.JWT({
    email: GOOGLE_SERVICE_ACCOUNT_EMAIL,
    key: GOOGLE_PRIVATE_KEY,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
  return jwt;
}

async function loadSheetValues(rangeA1) {
  if (!GSHEET_ID) throw new Error("Missing GSHEET_ID / GOOGLE_SHEET_ID");
  const auth = getSheetsAuth();
  if (!auth) throw new Error("Missing Google Sheets credentials in ENV");

  const sheets = google.sheets({ version: "v4", auth });
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GSHEET_ID,
    range: rangeA1,
  });
  return res.data.values || [];
}

function parseSettings(values) {
  // Expected columns: key | value | (optional notes)
  const out = {};
  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const key = (row[0] || "").trim();
    if (!key) continue;
    const val = (row[1] ?? "").toString();
    out[key] = val;
  }
  return out;
}

function parsePrompts(values) {
  // Expected columns: prompt_id | prompt_text
  const out = {};
  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const id = (row[0] || "").trim();
    if (!id) continue;
    const text = (row[1] ?? "").toString();
    out[id] = text;
  }
  return out;
}

async function refreshSheetsCache(tag = "Startup") {
  const now = Date.now();
  if (sheetsCache.loadedAt && now - sheetsCache.loadedAt < SHEETS_CACHE_TTL_MS && tag !== "Force") {
    return;
  }
  const settingsValues = await loadSheetValues("SETTINGS!A:C");
  const promptsValues = await loadSheetValues("PROMPTS!A:B");

  sheetsCache.settings = parseSettings(settingsValues);
  sheetsCache.prompts = parsePrompts(promptsValues);
  sheetsCache.loadedAt = Date.now();

  logInfo(tag, "Sheets cache refreshed.", {
    loadedAt: new Date(sheetsCache.loadedAt).toISOString(),
    settingsKeys: Object.keys(sheetsCache.settings).length,
    promptIds: Object.keys(sheetsCache.prompts).length,
  });
}

function getSetting(key, def = "") {
  const v = sheetsCache.settings[key];
  if (v === undefined || v === null) return def;
  return String(v);
}

function getPrompt(id, def = "") {
  const v = sheetsCache.prompts[id];
  if (v === undefined || v === null) return def;
  return String(v);
}

// ------------------------------------------------------------
// Template interpolation from SETTINGS for prompt construction
// ------------------------------------------------------------
function applyVars(template, vars) {
  let out = String(template || "");
  for (const [k, v] of Object.entries(vars || {})) {
    out = out.replace(new RegExp(`\\{${k}\\}`, "g"), v == null ? "" : String(v));
  }
  return out;
}

// ------------------------------------------------------------
// Business-specific dynamic KB string (from SETTINGS only)
// This replaces the old KB_FACTS / other tabs.
// ------------------------------------------------------------
function buildBusinessInfoStringFromSettings() {
  const BUSINESS_NAME = getSetting("BUSINESS_NAME", "גיל ספורט");
  const BOT_NAME = getSetting("BOT_NAME", "נטע");
  const WEBSITE_URL = getSetting("WEBSITE_URL", "");
  const MAIN_PHONE = getSetting("MAIN_PHONE", "");
  const WORKING_HOURS = getSetting("WORKING_HOURS", "");
  const AFTER_HOURS_DELIVERY_RULE = getSetting("AFTER_HOURS_DELIVERY_RULE", "");
  const NO_DATA_MESSAGE = getSetting("NO_DATA_MESSAGE", "אין לי מידע זמין כרגע");
  const PHONE_CONFIRM_TEMPLATE = getSetting("PHONE_CONFIRM_TEMPLATE", "רק לוודא—המספר לחזרה הוא: {number}. נכון?");
  const PHONE_COLLECT_REPROMPT = getSetting(
    "PHONE_COLLECT_REPROMPT",
    "לא קלטתי מספר תקין. תגידו בבקשה מספר טלפון בן 10 ספרות שמתחיל ב-0"
  );

  // Optional: you can store carrier/importer info as SETTINGS keys (flat).
  // Example keys:
  // DELIVERY_CONTACTS_TEXT  (a ready-to-say list string)
  // IMPORTERS_TEXT          (brand->phone mapping string)
  const DELIVERY_CONTACTS_TEXT = getSetting("DELIVERY_CONTACTS_TEXT", "");
  const IMPORTERS_TEXT = getSetting("IMPORTERS_TEXT", "");

  const info = `
מידע עסקי עבור "${BUSINESS_NAME}":
- שם הבוט: "${BOT_NAME}"
- אתר: ${WEBSITE_URL}
- טלפון ראשי: ${MAIN_PHONE}
- שעות פעילות: ${WORKING_HOURS}
- כלל אספקה אחרי שעות: ${AFTER_HOURS_DELIVERY_RULE}

הודעות מערכת:
- NO_DATA_MESSAGE: ${NO_DATA_MESSAGE}
- PHONE_COLLECT_REPROMPT: ${PHONE_COLLECT_REPROMPT}
- PHONE_CONFIRM_TEMPLATE: ${PHONE_CONFIRM_TEMPLATE}

מובילים (למסירה רק במקרה של אספקה להיום אחרי שעות, בנוסף ללקיחת פרטים): ${DELIVERY_CONTACTS_TEXT || "לא הוגדר"}
יבואנים לפי מותג (למסירה רק אם הלקוח מציין מותג תואם או מבקש במפורש, בנוסף ללקיחת פרטים): ${IMPORTERS_TEXT || "לא הוגדר"}
`.trim();

  return info;
}

// ------------------------------------------------------------
// System Instructions builder (MisterBot-style)
// Uses: PROMPTS.MASTER_PROMPT only (+ optional additional rules)
// ------------------------------------------------------------
const EXTRA_BEHAVIOR_RULES = `
חוקי מערכת קבועים (גבוהים מהפרומפט העסקי):
1. אל תתייחסי למוזיקה, רעשים או איכות הקו. התייחסי רק לתוכן מילולי שנשמע כמו דיבור מכוון אלייך. אם לא הבנת – אמרי קצר: "לא שמעתי טוב, אפשר לחזור על זה?".
2. תשובות קצרות: בדרך כלל עד 1–2 משפטים, ובסוף שאלה אחת בלבד אם צריך.
3. מספרי טלפון: אם הלקוח מסר מספר, חזרי עליו בדיוק כפי שנמסר ובקשי אישור קצר ("זה נכון?"). אל תשני ספרות ואל תנחשי.
4. עדיפות: קודם לאסוף פרטים לליד (שם מלא, טלפון), ואז לטפל בהסברים.
5. אם נושא לא ברור – שאלת הבהרה אחת בלבד.
6. אספקה להיום אחרי שעות: אם הלקוח אומר במפורש שהאספקה תואמה להיום אחרי שעות הפעילות – מותר למסור את מספרי המובילים (כפי שמוגדרים ב-SETTINGS) בנוסף ללקיחת פרטים.
7. תקלה/שירות: שאלי תמיד מה המותג (ובמידת האפשר גם דגם). אם המותג תואם לרשימת יבואנים ב-SETTINGS – אפשר להציע מסירת מספר היבואן, אבל עדיין לקחת פרטים.
`.trim();

function buildSystemInstructions() {
  const BUSINESS_NAME = getSetting("BUSINESS_NAME", "גיל ספורט");
  const BOT_NAME = getSetting("BOT_NAME", "נטע");

  const master = getPrompt("MASTER_PROMPT", "").trim();
  const businessInfo = buildBusinessInfoStringFromSettings();

  let instructions = "";
  if (master) instructions += master;
  if (businessInfo) instructions += (instructions ? "\n\n" : "") + businessInfo;

  if (!instructions) {
    instructions = `
את/ה נציג/ת שירות ומכירה קולית בשם "${BOT_NAME}" עבור "${BUSINESS_NAME}".
דבר/י בעברית כברירת מחדל, בלשון רבים, בטון שירותי וקצר.
`.trim();
  }

  instructions += "\n\n" + EXTRA_BEHAVIOR_RULES;
  return instructions;
}

// ------------------------------------------------------------
// Lead parsing helper (separate model) – strict JSON
// IMPORTANT: OpenAI requires the word "json" in messages when using response_format json_object.
// ------------------------------------------------------------
async function extractLeadFromConversation(conversationLog) {
  const tag = "LeadParse";

  if (!MB_ENABLE_SMART_LEAD_PARSING) {
    logDebug(tag, "Smart lead parsing disabled via ENV.");
    return null;
  }
  if (!OPENAI_API_KEY) {
    logError(tag, "Missing OPENAI_API_KEY for lead parsing.");
    return null;
  }
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) {
    logDebug(tag, "Empty conversationLog – skipping lead parsing.");
    return null;
  }

  const BUSINESS_NAME = getSetting("BUSINESS_NAME", "גיל ספורט");
  const BOT_NAME = getSetting("BOT_NAME", "נטע");

  try {
    const conversationText = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : BOT_NAME}: ${m.text}`)
      .join("\n");

    const systemPrompt = `
You are a call transcript parser. Return ONLY valid JSON (no extra text). The output must be json.
Schema:
{
  "is_lead": boolean,
  "intent": "sales"|"support"|"delivery"|"message"|"unknown",
  "full_name": string|null,
  "phone_number": string|null,
  "prefers_caller_id": boolean|null,
  "brand": string|null,
  "model": string|null,
  "reason": string|null,
  "notes": string|null
}
Rules:
- Extract values from the conversation.
- If missing: null.
- phone_number should be Israeli local format 0XXXXXXXXX or 0XXXXXXXXXX when possible.
- Do not invent phone numbers.
Return json only.
`.trim();

    const userPrompt = `
json transcript for parsing:
Business="${BUSINESS_NAME}" Bot="${BOT_NAME}"
Transcript:
${conversationText}
`.trim();

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
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
      }),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      logError(tag, `Lead parsing HTTP ${response.status}`, text);
      return null;
    }

    const data = await response.json();
    const raw = data.choices?.[0]?.message?.content;
    if (!raw) {
      logError(tag, "No content in lead parsing response.");
      return null;
    }

    let parsed = null;
    try {
      parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch (_) {
      parsed = null;
    }

    if (!parsed || typeof parsed !== "object") {
      logError(tag, "Parsed lead is not an object.", parsed);
      return null;
    }

    logInfo(tag, "Lead parsed.", parsed);
    return parsed;
  } catch (err) {
    logError(tag, "Error in extractLeadFromConversation", err);
    return null;
  }
}

// ------------------------------------------------------------
// Webhooks
// ------------------------------------------------------------
function mapCallStatus(reason) {
  const r = String(reason || "").toLowerCase();
  if (r.includes("error")) return "error";
  if (r.includes("abandoned") || r.includes("ws_closed") || r.includes("twilio_stop") || r.includes("stop")) return "abandoned";
  return "completed";
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

async function sendCallLogWebhook({ callSid, streamSid, caller_id, called, reason, transcript, parsedLead, recording_url }) {
  if (!MB_CALL_LOG_ENABLED || !MB_CALL_LOG_WEBHOOK_URL) return;
  try {
    const payload = {
      callSid: callSid || null,
      streamSid: streamSid || null,
      caller_id: caller_id || null,
      called: called || null,
      reason: reason || null,
      call_status: mapCallStatus(reason),
      transcript: transcript || "",
      parsedLead: parsedLead || null,
      recording_url: recording_url || null,
      ts: nowIso(),
    };

    const res = await fetchWithTimeout(
      MB_CALL_LOG_WEBHOOK_URL,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      },
      4500
    ).catch(() => null);

    if (res && !res.ok) {
      const txt = await res.text().catch(() => "");
      logError("CallLog", `CallLog webhook HTTP ${res.status}`, txt);
    } else {
      logInfo("CallLog", `CallLog webhook delivered status=${res ? res.status : "timeout-safe"}`);
    }
  } catch (err) {
    logError("CallLog", "sendCallLogWebhook error", err);
  }
}

async function sendLeadWebhook({ callSid, streamSid, caller_id, called, reason, transcript, parsedLead, recording_url }) {
  if (!MB_ENABLE_LEAD_CAPTURE || !MB_WEBHOOK_URL) return;

  try {
    const payload = {
      callSid: callSid || null,
      streamSid: streamSid || null,
      caller_id: caller_id || null,
      called: called || null,
      reason: reason || null,
      call_status: mapCallStatus(reason),
      transcript: transcript || "",
      parsedLead: parsedLead || null,
      recording_url: recording_url || null,
      ts: nowIso(),
    };

    logInfo("Lead", `Sending lead webhook to ${MB_WEBHOOK_URL}`);
    const res = await fetch(MB_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError("Lead", `Lead webhook HTTP ${res.status}`, txt);
    } else {
      logInfo("Lead", `Lead webhook delivered status=${res.status}`);
    }
  } catch (err) {
    logError("Lead", "sendLeadWebhook error", err);
  }
}

async function sendAbandonedWebhook({ callSid, streamSid, caller_id, called, reason, last_user_utterance, transcript, recording_url }) {
  if (!MB_ENABLE_ABANDONED_WEBHOOK || !MB_ABANDONED_WEBHOOK_URL) return;

  try {
    const payload = {
      callSid: callSid || null,
      streamSid: streamSid || null,
      caller_id: caller_id || null,
      called: called || null,
      call_status: "abandoned",
      reason: reason || "abandoned",
      last_user_utterance: last_user_utterance || null,
      transcript: transcript || "",
      recording_url: recording_url || null,
      ts: nowIso(),
    };

    const res = await fetchWithTimeout(
      MB_ABANDONED_WEBHOOK_URL,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      },
      4500
    ).catch(() => null);

    if (res && !res.ok) {
      const txt = await res.text().catch(() => "");
      logError("Abandoned", `ABANDONED webhook HTTP ${res.status}`, txt);
    } else {
      logInfo("Abandoned", `ABANDONED webhook delivered status=${res ? res.status : "timeout-safe"}`);
    }
  } catch (err) {
    logError("Abandoned", "sendAbandonedWebhook error", err);
  }
}

// ------------------------------------------------------------
// Express + TwiML
// ------------------------------------------------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/health", (req, res) => res.status(200).json({ ok: true, ts: nowIso() }));

app.post("/dashboard/reload", async (req, res) => {
  try {
    await refreshSheetsCache("Force");
    res.status(200).json({ ok: true, reloaded: true, ts: Date.now() });
  } catch (err) {
    logError("Dashboard", "/dashboard/reload failed", err);
    res.status(500).json({ ok: false, error: "reload_failed" });
  }
});

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
      <Parameter name="source" value="GilSport Voice AI"/>
    </Stream>
  </Connect>
</Response>`.trim();

  res.type("text/xml").send(twiml);
});

// ------------------------------------------------------------
// HTTP server + WS server
// ------------------------------------------------------------
const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// ------------------------------------------------------------
// Per-call handler (MisterBot-style runtime)
// ------------------------------------------------------------
wss.on("connection", (connection, req) => {
  const connId = `conn_${Math.random().toString(36).slice(2, 10)}_${Math.random().toString(16).slice(2, 6)}`;
  const tag = connId;

  logAlways("[ALWAYS] WS connection", {
    at: nowIso(),
    ip: req.socket?.remoteAddress,
    ua: req.headers["user-agent"],
    url: req.url,
  });

  if (!OPENAI_API_KEY) {
    logError(tag, "OPENAI_API_KEY missing – closing connection.");
    connection.close();
    return;
  }

  // State
  let streamSid = null;
  let callSid = null;
  let callerNumberRaw = null;
  let calledNumberRaw = null;
  let callDirection = null;

  let recordingSid = null;
  let recordingUrl = null;

  // Dialogue/slots
  let awaitingPhone = false;
  let awaitingPhoneConfirm = false;
  let collectedPhoneIL = null;
  let awaitingName = false;
  let collectedName = null;

  let prefersCallerId = null; // null/true/false

  // Runtime
  let conversationLog = [];
  let currentBotText = "";
  let callStartTs = Date.now();
  let lastMediaTs = Date.now();
  let idleCheckInterval = null;
  let idleWarningSent = false;
  let idleHangupScheduled = false;
  let maxCallTimeout = null;
  let maxCallWarningTimeout = null;

  let openAiReady = false;
  let callEnded = false;
  let twilioClosed = false;
  let openAiClosed = false;

  let botSpeaking = false;
  let hasActiveResponse = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  let pendingHangup = null;
  let leadWebhookSent = false;

  // Pre-load sheets
  refreshSheetsCache("OnConnect").catch((e) => logError(tag, "Sheets refresh failed", e));

  function getGraceMs() {
    const rawGrace = MB_HANGUP_GRACE_MS && MB_HANGUP_GRACE_MS > 0 ? MB_HANGUP_GRACE_MS : 3000;
    return Math.max(2000, Math.min(rawGrace, 8000));
  }

  let graceHangupTimer = null;
  function scheduleForceEndAfterGrace(ph, why = "closing_done") {
    if (!ph || callEnded) return;
    if (graceHangupTimer) return;

    const graceMs = getGraceMs();
    logInfo(tag, `Scheduling endCall AFTER GRACE (${graceMs} ms). why=${why}`);

    graceHangupTimer = setTimeout(() => {
      graceHangupTimer = null;
      if (callEnded) return;
      endCall(ph.reason, ph.closingMessage);
    }, graceMs);
  }

  function safeCancelResponseIfNeeded() {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;
    if (!hasActiveResponse) return;
    try {
      openAiWs.send(JSON.stringify({ type: "response.cancel" }));
    } catch (_) {}
    hasActiveResponse = false;
    botSpeaking = false;
    botTurnActive = false;
  }

  function sendModelPrompt(text, purpose) {
    if (openAiWs.readyState !== WebSocket.OPEN) {
      logDebug(tag, `Cannot send model prompt (${purpose || "no-tag"}) – WS not open.`);
      return;
    }
    if (hasActiveResponse) {
      logDebug(tag, `Skipping model prompt (${purpose || "no-tag"}) – active response exists.`);
      return;
    }

    const item = {
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text }],
      },
    };
    openAiWs.send(JSON.stringify(item));
    openAiWs.send(JSON.stringify({ type: "response.create" }));
    hasActiveResponse = true;
    botTurnActive = true;
    logDebug(tag, `response.create SPEAK purpose=${purpose || "n/a"} text=${text}`);
  }

  function normalizeForGuard(text) {
    return (text || "")
      .toLowerCase()
      .replace(/["'״׳]/g, "")
      .replace(/[.,!?;:]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }

  function updateDialogueStateFromBotText(botText) {
    const t = normalizeForGuard(botText);

    if (/מה מספר הטלפון|מספר טלפון לחזרה|מספר טלפון|טלפון לחזור|טלפון לחזרה/.test(t)) {
      awaitingPhone = true;
      awaitingPhoneConfirm = false;
      logDebug(tag, "State: awaitingPhone=true");
      return;
    }
    if (/האם נוח לחזור.*למספר/.test(t) || /למספר שממנו התקשר/.test(t)) {
      // model asked caller-id preference
      prefersCallerId = null;
      return;
    }
    if (/מה השם שלכם|שם מלא|איך קוראים לכם|איך אפשר לפנות אליכם/.test(t)) {
      awaitingName = true;
      logDebug(tag, "State: awaitingName=true");
      return;
    }
  }

  function handleDeterministicPhoneFlowOnUserTranscript(userText) {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;

    const phoneIL = detectPhoneCandidateFromText(userText);

    // handle caller-id preference
    if (prefersCallerId === null && /למספר שממנו התקשר|מספר מזוהה|למספר המזוהה/.test(normalizeForGuard(userText))) {
      // user referenced caller id explicitly; keep null and let parser decide
    }

    // If we are awaiting confirmation: user says yes/no
    if (awaitingPhoneConfirm) {
      if (isYes(userText)) {
        if (collectedPhoneIL) {
          awaitingPhoneConfirm = false;
          awaitingPhone = false;
          const sayPhone = formatIsraeliPhoneForTts(collectedPhoneIL);
          safeCancelResponseIfNeeded();
          sendModelPrompt(
            `הלקוח אישר שמספר הטלפון שלו הוא "${sayPhone}". תודה קצרה ואז המשיכי באופן טבעי. אל תשני את המספר ואל תחזרי לבקש אותו שוב.`,
            "phone_confirmed"
          );
        }
        return;
      }
      if (isNo(userText)) {
        collectedPhoneIL = null;
        awaitingPhoneConfirm = false;
        awaitingPhone = true;
        safeCancelResponseIfNeeded();
        sendModelPrompt(`הלקוח אמר שהמספר לא נכון. בקשי שוב מספר טלפון לחזרה, ובקשי שיאמר אותו לאט ספרה-ספרה. תשובה קצרה.`, "phone_retake");
        return;
      }
      return;
    }

    if (awaitingPhone && phoneIL) {
      collectedPhoneIL = phoneIL;
      awaitingPhone = false;
      awaitingPhoneConfirm = true;

      const sayPhone = formatIsraeliPhoneForTts(collectedPhoneIL);
      logDebug(tag, `Captured phone from speech: ${collectedPhoneIL}`);

      safeCancelResponseIfNeeded();
      sendModelPrompt(
        `הלקוח מסר מספר טלפון. המספר שנקלט (חובה לדייק ללא שינוי) הוא: "${sayPhone}". חזרי עליו בדיוק ושאלי: "זה נכון?" בלי להוסיף שום מספר אחר ובלי לשנות ספרות.`,
        "phone_echo_confirm"
      );
      return;
    }

    // Opportunistic capture
    if (phoneIL && !collectedPhoneIL) {
      collectedPhoneIL = phoneIL;
      logDebug(tag, "Captured phone opportunistically", { collectedPhoneIL });
    }
  }

  function sendIdleWarningIfNeeded() {
    if (idleWarningSent || callEnded) return;
    idleWarningSent = true;
    const text = "אני עדיין כאן על הקו, אתם איתי? אם תרצו להמשיך, אפשר פשוט לשאול או לבקש.";
    sendModelPrompt(`תגיבי ללקוח במשפט קצר בסגנון הבא (אפשר לשנות קצת): "${text}"`, "idle_warning");
  }

  async function endCall(reason, closingMessage) {
    if (callEnded) {
      logDebug(tag, `endCall called again (${reason}) – already ended.`);
      return;
    }
    callEnded = true;

    if (graceHangupTimer) {
      clearTimeout(graceHangupTimer);
      graceHangupTimer = null;
    }

    if (idleCheckInterval) clearInterval(idleCheckInterval);
    if (maxCallTimeout) clearTimeout(maxCallTimeout);
    if (maxCallWarningTimeout) clearTimeout(maxCallWarningTimeout);

    const BUSINESS_NAME = getSetting("BUSINESS_NAME", "גיל ספורט");
    const BOT_NAME = getSetting("BOT_NAME", "נטע");
    const effectiveClosing = (closingMessage || getSetting("CLOSING_SCRIPT", "") || "").trim();

    // snapshot
    const callSidSnapshot = callSid;
    const streamSidSnapshot = streamSid;
    const callerSnapshot = callerNumberRaw;
    const calledSnapshot = calledNumberRaw;
    const convoSnapshot = Array.isArray(conversationLog) ? [...conversationLog] : [];
    const reasonSnapshot = reason || "completed";
    const closingSnapshot = effectiveClosing;
    const recordingUrlSnapshot = recordingUrl;

    // Decide if abandoned
    const transcriptText = convoSnapshot.map((m) => `${m.from === "user" ? "לקוח" : BOT_NAME}: ${m.text}`).join("\n");
    const lastUser = [...convoSnapshot].reverse().find((m) => m.from === "user")?.text || "";

    // Close quickly
    if (callSidSnapshot) hangupTwilioCall(callSidSnapshot, "Twilio").catch(() => {});
    if (!openAiClosed && openAiWs.readyState === WebSocket.OPEN) {
      openAiClosed = true;
      openAiWs.close();
    }
    if (!twilioClosed && connection.readyState === WebSocket.OPEN) {
      twilioClosed = true;
      connection.close();
    }

    // Post-call tasks (sync-ish but safe)
    (async () => {
      let parsedLead = null;

      try {
        if (MB_ENABLE_SMART_LEAD_PARSING) {
          parsedLead = await extractLeadFromConversation(convoSnapshot);
        }
      } catch (_) {}

      // Merge deterministic captures
      try {
        const normalizedCaller = normalizePhoneNumber(null, callerSnapshot);
        if (parsedLead && typeof parsedLead === "object") {
          // ensure consistent key names
          if (!parsedLead.full_name && collectedName) parsedLead.full_name = collectedName;
          if (!parsedLead.phone_number) {
            const best = collectedPhoneIL || normalizedCaller || null;
            parsedLead.phone_number = best;
          } else {
            parsedLead.phone_number = normalizePhoneNumber(parsedLead.phone_number, callerSnapshot) || parsedLead.phone_number;
          }
          if (prefersCallerId !== null && parsedLead.prefers_caller_id == null) {
            parsedLead.prefers_caller_id = prefersCallerId;
          }
          // derive is_lead if missing
          if (parsedLead.is_lead == null) parsedLead.is_lead = !!parsedLead.phone_number;
        }
      } catch (_) {}

      // Always send CallLog if enabled
      await sendCallLogWebhook({
        callSid: callSidSnapshot,
        streamSid: streamSidSnapshot,
        caller_id: callerSnapshot,
        called: calledSnapshot,
        reason: reasonSnapshot,
        transcript: transcriptText,
        parsedLead,
        recording_url: recordingUrlSnapshot,
      }).catch(() => {});

      // Lead webhook: only if full lead (phone exists)
      if (MB_ENABLE_LEAD_CAPTURE && MB_WEBHOOK_URL) {
        const phone = parsedLead?.phone_number || collectedPhoneIL || normalizePhoneNumber(null, callerSnapshot) || null;
        const isFullLead = !!phone;
        if (isFullLead) {
          await sendLeadWebhook({
            callSid: callSidSnapshot,
            streamSid: streamSidSnapshot,
            caller_id: callerSnapshot,
            called: calledSnapshot,
            reason: reasonSnapshot,
            transcript: transcriptText,
            parsedLead: { ...(parsedLead || {}), phone_number: phone },
            recording_url: recordingUrlSnapshot,
          }).catch(() => {});
          leadWebhookSent = true;
        } else {
          logInfo(tag, "Skipping lead webhook (no phone).");
        }
      }

      // Abandoned webhook only if truly abandoned (no user/bot exchange or very short)
      const isAbandoned =
        reasonSnapshot.includes("twilio_stop") ||
        reasonSnapshot.includes("ws_closed") ||
        reasonSnapshot.includes("idle_timeout") ||
        reasonSnapshot.includes("openai_ws") ||
        // heuristic: less than 2 user turns or less than 1 meaningful bot turn
        convoSnapshot.filter((m) => m.from === "user").length < 1;

      if (isAbandoned && MB_ENABLE_ABANDONED_WEBHOOK && MB_ABANDONED_WEBHOOK_URL) {
        // Ensure caller id is present
        let callerUse = callerSnapshot;
        if (!callerUse && callSidSnapshot) {
          const backfill = await fetchCallerNumberFromTwilio(callSidSnapshot, "Abandoned").catch(() => null);
          if (backfill) callerUse = backfill;
        }

        await sendAbandonedWebhook({
          callSid: callSidSnapshot,
          streamSid: streamSidSnapshot,
          caller_id: callerUse,
          called: calledSnapshot,
          reason: "abandoned",
          last_user_utterance: lastUser,
          transcript: transcriptText,
          recording_url: recordingUrlSnapshot,
        }).catch(() => {});
      } else {
        logInfo(tag, "Not abandoned → abandoned webhook skipped.");
      }
    })().catch((e) => logError(tag, "Post-call tasks error", e));
  }

  function scheduleEndCall(reason, closingMessage) {
    if (callEnded) return;

    const msg = (closingMessage || getSetting("CLOSING_SCRIPT", "") || "").trim();
    if (pendingHangup) return;

    logInfo(tag, `scheduleEndCall invoked. reason="${reason}", closingMessage="${msg}"`);
    pendingHangup = { reason, closingMessage: msg };

    if (openAiWs.readyState === WebSocket.OPEN && msg) {
      sendModelPrompt(`סיימי את השיחה עם הלקוח במשפט הבא בלבד, בלי להוסיף שום משפט נוסף: "${msg}"`, "closing");
    } else {
      const ph = pendingHangup;
      pendingHangup = null;
      scheduleForceEndAfterGrace(ph, "no_openai_or_no_closing");
      return;
    }

    const graceMs = getGraceMs();
    setTimeout(() => {
      if (callEnded) return;
      if (!pendingHangup) return;
      const ph = pendingHangup;
      pendingHangup = null;
      logInfo(tag, `Closing fallback reached (${graceMs} ms), forcing end AFTER GRACE.`);
      scheduleForceEndAfterGrace(ph, "closing_fallback");
    }, graceMs + 6000);
  }

  function checkBotClosing(botText) {
    const closingScript = (getSetting("CLOSING_SCRIPT", "") || "").trim();
    if (!closingScript || !botText) return;

    const normClosing = normalizeForGuard(closingScript);
    const normText = normalizeForGuard(botText);

    if (!normClosing || !normText) return;

    if (normText.includes(normClosing) || normClosing.includes(normText)) {
      logInfo(tag, `Detected configured bot closing phrase in output: "${botText}"`);
      if (pendingHangup) {
        const ph = pendingHangup;
        pendingHangup = null;
        scheduleForceEndAfterGrace(ph, "bot_closing_detected");
      } else {
        scheduleEndCall("bot_closing_config", closingScript);
      }
    }
  }

  function filteredGarbageTranscript(t) {
    const x = normalizeForGuard(t);
    if (!x) return true;
    // Very short english fillers / noise
    if (/^(ok|okay|thanks|thank you|bye|hello)$/.test(x)) return true;
    return false;
  }

  // ------------------------------------------------------------
  // OpenAI Realtime WS
  // ------------------------------------------------------------
  logDebug(tag, `Creating OpenAI WS... model=${OPENAI_REALTIME_MODEL} voice=${OPENAI_VOICE}`);
  const openAiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "OpenAI-Beta": "realtime=v1",
    },
  });

  openAiWs.on("open", async () => {
    openAiReady = true;
    logDebug(tag, "OpenAI connected");

    // Ensure sheets ready before building instructions
    try {
      await refreshSheetsCache("Startup");
    } catch (_) {}

    const instructions = buildSystemInstructions();

    const effectiveSilenceMs = MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS;

    const sessionUpdate = {
      type: "session.update",
      session: {
        model: OPENAI_REALTIME_MODEL,
        modalities: ["audio", "text"],
        voice: OPENAI_VOICE,
        input_audio_format: "g711_ulaw",
        output_audio_format: "g711_ulaw",
        input_audio_transcription: { model: "whisper-1" },
        turn_detection: {
          type: "server_vad",
          threshold: MB_VAD_THRESHOLD,
          silence_duration_ms: effectiveSilenceMs,
          prefix_padding_ms: MB_VAD_PREFIX_MS,
        },
        // Do not set temperature below supported min; let server default.
        instructions,
      },
    };

    openAiWs.send(JSON.stringify(sessionUpdate));

    // opening greeting is proactive
    const opening = (getSetting("OPENING_SCRIPT", "") || "").trim();
    logAlways(`[ALWAYS] [${tag}] SOURCES`, {
      sheets_loaded_at: sheetsCache.loadedAt ? new Date(sheetsCache.loadedAt).toISOString() : null,
      opening_from: "SETTINGS.OPENING_SCRIPT",
      master_from: "PROMPTS.MASTER_PROMPT (+ KB + GUARDRAILS)",
      opening_preview: opening,
      master_preview: (getPrompt("MASTER_PROMPT", "") || "").slice(0, 160),
    });

    if (opening) {
      sendModelPrompt(`פתחי את השיחה עם הלקוח במשפט הבא (אפשר לשנות מעט את הניסוח אבל לא להאריך): "${opening}" ואז עצרי והמתיני לתשובה שלו.`, "opening_greeting");
    } else {
      sendModelPrompt(`פתחי שיחה בברכה קצרה בעברית ואז שאלי איך אפשר לעזור.`, "opening_greeting_fallback");
    }
  });

  openAiWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (err) {
      logError(tag, "Failed to parse OpenAI WS message", err);
      return;
    }

    const type = msg.type;

    switch (type) {
      case "response.created":
        currentBotText = "";
        hasActiveResponse = true;
        botTurnActive = true;
        botSpeaking = false;
        noListenUntilTs = Date.now() + MB_NO_BARGE_TAIL_MS;
        break;

      case "response.output_text.delta": {
        const delta = msg.delta || "";
        if (delta) currentBotText += delta;
        break;
      }

      case "response.audio_transcript.delta": {
        const delta = msg.delta || "";
        if (delta) currentBotText += delta;
        break;
      }

      case "response.output_text.done":
      case "response.audio_transcript.done": {
        if (!currentBotText) break;
        const text = currentBotText.trim();
        if (text) {
          conversationLog.push({ from: "bot", text });
          logAlways(`[ALWAYS] [BOT][${tag}] ${text}`);

          updateDialogueStateFromBotText(text);
          checkBotClosing(text);
        }
        currentBotText = "";
        break;
      }

      case "response.audio.delta": {
        const b64 = msg.delta;
        if (!b64 || !streamSid) break;
        botSpeaking = true;
        const now = Date.now();
        noListenUntilTs = now + MB_NO_BARGE_TAIL_MS;

        if (connection.readyState === WebSocket.OPEN) {
          connection.send(
            JSON.stringify({
              event: "media",
              streamSid,
              media: { payload: b64 },
            })
          );
        }
        break;
      }

      case "response.audio.done": {
        botSpeaking = false;
        botTurnActive = false;

        if (pendingHangup && !callEnded) {
          const ph = pendingHangup;
          pendingHangup = null;
          logInfo(tag, "Closing audio finished, scheduling hangup AFTER GRACE.");
          scheduleForceEndAfterGrace(ph, "audio_done");
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
          logInfo(tag, "Response completed for closing, scheduling hangup AFTER GRACE.");
          scheduleForceEndAfterGrace(ph, "response_completed");
        }
        break;
      }

      case "conversation.item.input_audio_transcription.completed": {
        const transcriptRaw = msg.transcript || "";
        let t = transcriptRaw.trim();
        if (t && !filteredGarbageTranscript(t)) {
          t = t.replace(/\s+/g, " ").replace(/\s+([,.:;!?])/g, "$1");
          conversationLog.push({ from: "user", text: t });
          logAlways(`[ALWAYS] [CALLER][${tag}] ${t}`);

          handleDeterministicPhoneFlowOnUserTranscript(t);

          // basic name capture (best effort)
          if (awaitingName && !collectedName) {
            const possiblePhone = detectPhoneCandidateFromText(t);
            if (!possiblePhone) {
              const words = t.split(" ").filter(Boolean).slice(0, 4);
              const name = words.join(" ").trim();
              if (name && name.length <= 50) collectedName = name;
            }
            awaitingName = false;
          }
        } else if (t && filteredGarbageTranscript(t)) {
          logDebug(tag, `Filtered garbage transcript: "${t}"`);
        }
        break;
      }

      case "error":
        logError(tag, "OpenAI Realtime error event", msg);
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
    logDebug(tag, "OpenAI closed");
    if (!callEnded) endCall("openai_ws_closed", getSetting("CLOSING_SCRIPT", ""));
  });

  openAiWs.on("error", (err) => {
    logError(tag, "OpenAI WS error", err);
    if (!openAiClosed) {
      openAiClosed = true;
      try { openAiWs.close(); } catch (_) {}
    }
    if (!callEnded) endCall("openai_ws_error", getSetting("CLOSING_SCRIPT", ""));
  });

  // ------------------------------------------------------------
  // Twilio Media Stream handlers
  // ------------------------------------------------------------
  connection.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (err) {
      logError(tag, "Failed to parse Twilio WS message", err);
      return;
    }

    const event = msg.event;

    if (event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;

      const cp = msg.start?.customParameters || {};
      callerNumberRaw = cp.caller || cp.From || cp.from || msg.start?.caller || msg.start?.from || null;
      calledNumberRaw = cp.called || cp.To || cp.to || msg.start?.to || null;
      callDirection = cp.direction || msg.start?.direction || "inbound";

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logAlways(`[ALWAYS] [TWILIO_START][${tag}] ${JSON.stringify(msg.start || {})}`);

      // Start recording (best effort)
      (async () => {
        try {
          if (callSid) {
            const sid = await startTwilioRecording(callSid, tag);
            if (sid) {
              recordingSid = sid;
              recordingUrl = buildRecordingUrl(sid);
              logInfo(tag, "Recording started.", { recording_sid: recordingSid });
            }
          }
        } catch (e) {
          logError(tag, "Recording start failed", e);
        }
      })().catch(() => {});

      // Backfill caller if missing
      if (!callerNumberRaw && callSid) {
        fetchCallerNumberFromTwilio(callSid, tag)
          .then((resolved) => {
            if (resolved && !callerNumberRaw) {
              callerNumberRaw = resolved;
              logInfo(tag, `Caller backfilled from Twilio API: ${callerNumberRaw}`);
            }
          })
          .catch(() => {});
      }

      // Idle interval
      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          sendIdleWarningIfNeeded();
        }
        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          logInfo(tag, "Idle timeout reached, scheduling endCall.");
          scheduleEndCall("idle_timeout", getSetting("CLOSING_SCRIPT", ""));
        }
      }, 1000);

      // Max call
      if (MB_MAX_CALL_MS > 0) {
        if (MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
          maxCallWarningTimeout = setTimeout(() => {
            const t = "אנחנו מתקרבים לסיום הזמן לשיחה הזאת. אם תרצו להתקדם, אפשר עכשיו לסכם ולהשאיר פרטים.";
            sendModelPrompt(`תני ללקוח משפט קצר בסגנון הבא (אפשר לשנות קצת): "${t}"`, "max_call_warning");
          }, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
        }

        maxCallTimeout = setTimeout(() => {
          logInfo(tag, "Max call duration reached, scheduling endCall.");
          scheduleEndCall("max_call_duration", getSetting("CLOSING_SCRIPT", ""));
        }, MB_MAX_CALL_MS);
      }
    } else if (event === "media") {
      lastMediaTs = Date.now();
      const payload = msg.media?.payload;
      if (!payload) return;
      if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;

      const now = Date.now();

      if (!MB_ALLOW_BARGE_IN) {
        if (botTurnActive || botSpeaking || now < noListenUntilTs) {
          logDebug("BargeIn", "Ignoring media because bot is speaking / tail (MB_ALLOW_BARGE_IN=false)", {
            botTurnActive,
            botSpeaking,
            now,
            noListenUntilTs,
          });
          return;
        }
      }

      openAiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));
    } else if (event === "stop") {
      logAlways(`[ALWAYS] [TWILIO_STOP][${tag}] stream stopped`);
      twilioClosed = true;
      if (!callEnded) endCall("twilio_stop", getSetting("CLOSING_SCRIPT", ""));
    }
  });

  connection.on("close", () => {
    logAlways(`[ALWAYS] [TWILIO_CLOSE][${tag}] socket closed`);
    twilioClosed = true;
    if (!callEnded) endCall("twilio_ws_closed", getSetting("CLOSING_SCRIPT", ""));
  });

  connection.on("error", (err) => {
    logError(tag, "Twilio WS error", err);
    twilioClosed = true;
    if (!callEnded) endCall("twilio_ws_error", getSetting("CLOSING_SCRIPT", ""));
  });
});

// ------------------------------------------------------------
// Start server
// ------------------------------------------------------------
server.listen(PORT, async () => {
  console.log(`✅ GilSport MisterBot-style VoiceBot running on port ${PORT}`);
  try {
    await refreshSheetsCache("Startup");
  } catch (err) {
    logError("Startup", "Initial sheets load failed", err);
  }
});
