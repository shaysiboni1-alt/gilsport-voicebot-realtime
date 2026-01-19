// server.js
// GilSport VoiceBot – MisterBot-style (Sheet prompts only) + Recording + Lead/CallLog + Abandoned
// Version v6 – ENV compatibility hotfix:
// Fix: Support GOOGLE_SERVICE_ACCOUNT_JSON_B64 (Render) in addition to GOOGLE_CLIENT_EMAIL/GOOGLE_PRIVATE_KEY

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

// Node 18+: fetch is global; otherwise fall back
const fetch = global.fetch || require("node-fetch");

const { google } = require("googleapis");

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
  const raw = (process.env[name] || "").toLowerCase();
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

function nowIso() {
  return new Date().toISOString();
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

function digitsOnly(v) {
  if (!v) return "";
  return String(v).replace(/\D/g, "");
}

// Extract a single phone-like digit sequence from arbitrary text.
// Handles spaced/dashed formats like "0 5 0-3 2" by stripping non-digits.
// Returns null if the digit count is outside a sane phone range.
function extractPhoneDigitsLoose(text, minDigits = 7, maxDigits = 12) {
  const d = digitsOnly(text);
  if (!d) return null;
  if (d.length < minDigits || d.length > maxDigits) return null;
  return d;
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

// format digits as "0 5 0 3 ..." to reduce TTS swallowing digits
function formatDigitsForTts(d) {
  const s = digitsOnly(d);
  if (!s) return "";
  return s.split("").join(" ");
}

// Prefer Hebrew words for digits (with commas) so the TTS does not swallow repetitions.
// Example: 0503222237 -> "אפס, חמש, אפס, שלוש, שתיים, שתיים, שתיים, שתיים, שלוש, שבע".
function formatDigitsForHebrewSpeech(d) {
  const s = digitsOnly(d);
  if (!s) return "";
  const map = {
    "0": "אפס",
    "1": "אחת",
    "2": "שתיים",
    "3": "שלוש",
    "4": "ארבע",
    "5": "חמש",
    "6": "שש",
    "7": "שבע",
    "8": "שמונה",
    "9": "תשע",
  };
  return s
    .split("")
    .map((ch) => map[ch] || ch)
    .join(", ");
}

function normalizePhoneNumber(rawPhone, callerNumber) {
  function clean(num) {
    const d = digitsOnly(num);
    if (!d) return null;

    // normalize 972 -> 0
    let local = d;
    if (local.startsWith("972") && (local.length === 11 || local.length === 12)) {
      local = "0" + local.slice(3);
    }

    // IL validation basic
    if (!/^0\d{8,9}$/.test(local)) return null;
    return local;
  }

  return clean(rawPhone) || clean(callerNumber) || null;
}

function extractBestPhoneFromText(text) {
  const d = digitsOnly(text);
  if (!d) return null;
  return normalizePhoneNumber(d, null);
}

function isTranscriptGarbage(t, hasRealUserYet) {
  const s = String(t || "").trim();
  if (!s) return true;

  const low = s.toLowerCase();
  if (!hasRealUserYet && (low === "ok" || low === "okay" || low === "yes" || low === "no")) return true;

  const hasHeb = /[\u0590-\u05FF]/.test(s);
  const hasDigits = /\d/.test(s);
  const hasLetters = /[a-zA-Z]/.test(s);

  if (s.length <= 2 && !hasDigits) return true;

  const letters = s.replace(/[^a-zA-Z\u0590-\u05FF]/g, "");
  if (letters.length === 0 && !hasDigits) return true;

  if (hasRealUserYet) return false;

  if (hasHeb || hasDigits) return false;
  if (hasLetters && s.length > 5) return false;

  return true;
}

// -----------------------------
// Core ENV config
// -----------------------------
const PORT = envNumber("PORT", 10000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_DEBUG = envBool("MB_DEBUG", true);
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 1600);
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);

const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 200);

// Idle / Duration
const MB_IDLE_WARNING_MS = envNumber("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNumber("MB_IDLE_HANGUP_MS", 90000);

// Max call
const MB_MAX_CALL_MS = envNumber("MB_MAX_CALL_MS", 5 * 60 * 1000);
const MB_MAX_WARN_BEFORE_MS = envNumber("MB_MAX_WARN_BEFORE_MS", 45000);
const MB_HANGUP_GRACE_MS = envNumber("MB_HANGUP_GRACE_MS", 5000);
const MB_HANGUP_AFTER_GOODBYE = envBool("MB_HANGUP_AFTER_GOODBYE", true);

// Webhooks
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", !!MB_CALL_LOG_WEBHOOK_URL);

const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || "");
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", !!MB_WEBHOOK_URL);

const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");
const MB_ENABLE_ABANDONED_WEBHOOK = envBool("MB_ENABLE_ABANDONED_WEBHOOK", !!MB_ABANDONED_WEBHOOK_URL);

const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

// Lead parse
const MB_LEAD_PARSING_MODEL = process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini";

// Twilio credentials (for hangup, recording URL, caller resolution)
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

function logDebug(connId, msg, extra) {
  if (!MB_DEBUG) return;
  if (extra !== undefined) console.log(`[DEBUG] [${connId}] ${msg}`, extra);
  else console.log(`[DEBUG] [${connId}] ${msg}`);
}
function logInfo(connId, msg, extra) {
  if (extra !== undefined) console.log(`[INFO] [${connId}] ${msg}`, extra);
  else console.log(`[INFO] [${connId}] ${msg}`);
}
function logError(connId, msg, extra) {
  if (extra !== undefined) console.error(`[ERROR] [${connId}] ${msg}`, extra);
  else console.error(`[ERROR] [${connId}] ${msg}`);
}
function logAlways(msg, extra) {
  if (extra !== undefined) console.log(`[ALWAYS] ${msg}`, extra);
  else console.log(`[ALWAYS] ${msg}`);
}

// -----------------------------
// Google Sheets (SETTINGS + PROMPTS)
// -----------------------------
// IMPORTANT: Keep existing Render ENV names.
// Primary: GOOGLE_SERVICE_ACCOUNT_JSON_B64 + GSHEET_ID
// Fallback: GOOGLE_CLIENT_EMAIL + GOOGLE_PRIVATE_KEY + GSHEET_ID
const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL || "";
const GOOGLE_PRIVATE_KEY = (process.env.GOOGLE_PRIVATE_KEY || "").replace(/\\n/g, "\n");

const SETTINGS_TAB = process.env.SETTINGS_TAB || "SETTINGS";
const PROMPTS_TAB = process.env.PROMPTS_TAB || "PROMPTS";

let sheetsCache = {
  loadedAt: null,
  settings: {},
  prompts: {},
};

function decodeServiceAccountFromB64(b64) {
  const raw = String(b64 || "").trim();
  if (!raw) return null;
  try {
    const jsonStr = Buffer.from(raw, "base64").toString("utf8");
    const obj = JSON.parse(jsonStr);
    if (!obj || typeof obj !== "object") return null;

    const email = String(obj.client_email || "").trim();
    let key = String(obj.private_key || "").trim();

    // normalize newlines if stored escaped
    key = key.replace(/\\n/g, "\n");

    if (!email || !key) return null;
    return { email, key };
  } catch (_) {
    return null;
  }
}

function getSheetsCreds() {
  // Prefer JSON_B64 (your current Render)
  const fromB64 = decodeServiceAccountFromB64(GOOGLE_SERVICE_ACCOUNT_JSON_B64);
  if (fromB64) return fromB64;

  // Fallback (legacy)
  const email = String(GOOGLE_CLIENT_EMAIL || "").trim();
  const key = String(GOOGLE_PRIVATE_KEY || "").trim();
  if (email && key) return { email, key };

  return null;
}

function requireSheetsConfig() {
  if (!GSHEET_ID) throw new Error("Missing GSHEET_ID");

  const creds = getSheetsCreds();
  if (!creds) {
    // match the old error wording you saw (so it’s obvious what’s missing)
    throw new Error("Missing GOOGLE_CLIENT_EMAIL");
  }
}

function getAuth() {
  requireSheetsConfig();
  const creds = getSheetsCreds();

  const jwt = new google.auth.JWT({
    email: creds.email,
    key: creds.key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
  return jwt;
}

function normalizeKey(k) {
  return String(k || "")
    .trim()
    .replace(/\s+/g, "_")
    .toUpperCase();
}

async function loadSheetsCache(tag = "Startup") {
  const auth = getAuth();
  const sheets = google.sheets({ version: "v4", auth });

  const [settingsRes, promptsRes] = await Promise.all([
    sheets.spreadsheets.values.get({
      spreadsheetId: GSHEET_ID,
      range: `${SETTINGS_TAB}!A:B`,
    }),
    sheets.spreadsheets.values.get({
      spreadsheetId: GSHEET_ID,
      range: `${PROMPTS_TAB}!A:B`,
    }),
  ]);

  const settingsRows = settingsRes.data.values || [];
  const promptsRows = promptsRes.data.values || [];

  const settings = {};
  for (const row of settingsRows) {
    if (!row || row.length < 2) continue;
    const key = normalizeKey(row[0]);
    const val = String(row[1] ?? "").trim();
    if (!key) continue;
    settings[key] = val;
  }

  const prompts = {};
  for (const row of promptsRows) {
    if (!row || row.length < 2) continue;
    const key = normalizeKey(row[0]);
    const val = String(row[1] ?? "").trim();
    if (!key) continue;
    prompts[key] = val;
  }

  sheetsCache = {
    loadedAt: nowIso(),
    settings,
    prompts,
  };

  logInfo(tag, "Sheets cache refreshed.", {
    loadedAt: sheetsCache.loadedAt,
    settingsKeys: Object.keys(settings).length,
    promptIds: Object.keys(prompts).length,
  });
}

function getSetting(key, def = "") {
  const k = normalizeKey(key);
  const v = sheetsCache.settings[k];
  return v !== undefined && v !== null && String(v).trim() !== "" ? String(v) : def;
}
function getPrompt(id, def = "") {
  const k = normalizeKey(id);
  const v = sheetsCache.prompts[k];
  return v !== undefined && v !== null && String(v).trim() !== "" ? String(v) : def;
}

function interpolateVars(str, vars) {
  let out = String(str || "");
  for (const [k, v] of Object.entries(vars || {})) {
    const safeV = v === undefined || v === null ? "" : String(v);
    out = out.replaceAll(`{${k}}`, safeV);
  }
  return out;
}


// -----------------------------
// Dynamic SETTINGS-derived lists (importers / delivery phones)
// Convention:
// - Delivery phones: SETTINGS keys starting with DELIVERY_PHONE_*, value should already be a human-readable string (e.g., "אלכס 050...")
// - Importers: SETTINGS key pairs IMPORTER_<TOKEN>_NAME and IMPORTER_<TOKEN>_PHONE.
//   NAME can contain multiple brand keywords (comma-separated), and PHONE is the direct number.
// These lists are exposed to prompts via {DELIVERY_PHONES_LIST} and {IMPORTERS_LIST}.
function buildDeliveryPhonesList(settings) {
  const entries = [];
  for (const [k, v] of Object.entries(settings || {})) {
    if (!String(k).startsWith("DELIVERY_PHONE_")) continue;
    const val = String(v || "").trim();
    if (!val) continue;
    const suffix = String(k).slice("DELIVERY_PHONE_".length);
    entries.push({ k, suffix, val });
  }
  entries.sort((a, b) => {
    const na = parseInt(a.suffix, 10);
    const nb = parseInt(b.suffix, 10);
    const aNum = Number.isFinite(na);
    const bNum = Number.isFinite(nb);
    if (aNum && bNum) return na - nb;
    if (aNum && !bNum) return -1;
    if (!aNum && bNum) return 1;
    return String(a.suffix).localeCompare(String(b.suffix));
  });
  return entries.map((e) => e.val).join("; ");
}

function buildImportersList(settings) {
  const names = {};
  const phones = {};

  for (const [k, v] of Object.entries(settings || {})) {
    const key = String(k);
    if (!key.startsWith("IMPORTER_")) continue;
    const val = String(v || "").trim();
    if (!val) continue;

    if (key.endsWith("_NAME")) {
      const token = key.slice("IMPORTER_".length, -"_NAME".length);
      names[token] = val;
    } else if (key.endsWith("_PHONE")) {
      const token = key.slice("IMPORTER_".length, -"_PHONE".length);
      phones[token] = val;
    }
  }

  const tokens = Array.from(new Set([...Object.keys(names), ...Object.keys(phones)])).sort();
  const items = [];

  for (const t of tokens) {
    const phone = String(phones[t] || "").trim();
    if (!phone) continue;

    const name = String(names[t] || "").trim();
    const label = name || t.replace(/_/g, " ");

    items.push(`${label} ${phone}`.trim());
  }

  return items.join("; ");
}

// Collect all known phone numbers from SETTINGS so we can harden digit reading.
function buildAllowedPhoneDigitsFromSettings(settings, extra = []) {
  const set = new Set();

  for (const [k, v] of Object.entries(settings || {})) {
    const key = String(k || "").toUpperCase();
    if (
      key === "MAIN_PHONE" ||
      /^DELIVERY_PHONE_\d+$/.test(key) ||
      /^IMPORTER_[A-Z0-9_]+_PHONE$/.test(key)
    ) {
      const d = digitsOnly(v);
      if (d) set.add(d);
    }
  }

  for (const x of extra || []) {
    const d = digitsOnly(x);
    if (d) set.add(d);
  }

  return set;
}

function buildImporterIndex(settings) {
  const idx = [];
  const s = settings || {};

  for (const [keyRaw, nameVal] of Object.entries(s)) {
    const key = String(keyRaw || "").toUpperCase();
    if (!key.endsWith("_NAME")) continue;
    if (!key.startsWith("IMPORTER_")) continue;

    const base = key.replace(/_NAME$/, "");
    const phoneKey = `${base}_PHONE`;
    const phoneVal = s[phoneKey];

    const phone = digitsOnly(phoneVal);
    const label = String(nameVal || "").trim();
    if (!label || !phone) continue;

    const keywords = label
      .split(/[,，]/)
      .map((x) => String(x || "").trim())
      .filter(Boolean);

    idx.push({ base, label, phone, keywords: keywords.length ? keywords : [label] });
  }

  return idx;
}

function buildSystemInstructionsFromSheets() {
  const businessName = getSetting("BUSINESS_NAME", "GilSport");
  const botName = getSetting("BOT_NAME", "נטע");

  const opening = getSetting("OPENING_SCRIPT", "שלום! מדברת נטע מגיל ספורט במה אפשר לעזור?");
  const closing = getSetting("CLOSING_SCRIPT", "תודה שפנית אלינו. יום נעים!");

  const master = getPrompt("MASTER_PROMPT", "");
  const guard = getPrompt("GUARDRAILS_PROMPT", "");
  const kb = getPrompt("KB_PROMPT", "");

  const vars = {
    BUSINESS_NAME: businessName,
    BOT_NAME: botName,
    OPENING_SCRIPT: opening,
    CLOSING_SCRIPT: closing,

    WEBSITE_URL: getSetting("WEBSITE_URL", ""),
    MAIN_PHONE: getSetting("MAIN_PHONE", ""),
    WORKING_HOURS: getSetting("WORKING_HOURS", ""),
    AFTER_HOURS_DELIVERY_RULE: getSetting("AFTER_HOURS_DELIVERY_RULE", ""),

    // Dynamic lists derived from SETTINGS (no code changes needed when adding more entries)
    DELIVERY_PHONES_LIST: buildDeliveryPhonesList(sheetsCache.settings),
    IMPORTERS_LIST: buildImportersList(sheetsCache.settings),

    DELIVERY_PHONE_1: getSetting("DELIVERY_PHONE_1", ""),
    DELIVERY_PHONE_2: getSetting("DELIVERY_PHONE_2", ""),
    DELIVERY_PHONE_3: getSetting("DELIVERY_PHONE_3", ""),

    IMPORTER_VO2_NAME: getSetting("IMPORTER_VO2_NAME", ""),
    IMPORTER_VO2_PHONE: getSetting("IMPORTER_VO2_PHONE", ""),

    IMPORTER_A_NAME: getSetting("IMPORTER_A_NAME", ""),
    IMPORTER_A_PHONE: getSetting("IMPORTER_A_PHONE", ""),
    IMPORTER_B_NAME: getSetting("IMPORTER_B_NAME", ""),
    IMPORTER_B_PHONE: getSetting("IMPORTER_B_PHONE", ""),
    IMPORTER_C_NAME: getSetting("IMPORTER_C_NAME", ""),
    IMPORTER_C_PHONE: getSetting("IMPORTER_C_PHONE", ""),
  };

  const combined = [master, guard, kb].filter(Boolean).join("\n\n");
  const final = interpolateVars(combined, vars);

  return {
    businessName,
    botName,
    opening,
    closing,
    instructions:
      final ||
      `את/ה נציג/ת שירות ומכירה קולית בשם "${botName}" עבור "${businessName}". דבר/י בעברית כברירת מחדל, בלשון רבים, בטון שירותי וקצר.`,
  };
}

// -----------------------------
// Twilio helpers (hangup, recording URL)
// -----------------------------
async function hangupTwilioCall(callSid, connId) {
  if (!callSid) return;
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`;
    const body = new URLSearchParams({ Status: "completed" });

    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization:
          "Basic " + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64"),
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(connId, `Twilio hangup HTTP ${res.status}`, txt);
    } else {
      logInfo(connId, "Twilio hangup requested.");
    }
  } catch (err) {
    logError(connId, "Twilio hangup error", err);
  }
}

async function buildRecordingUrl(recordingSid) {
  if (!recordingSid || !TWILIO_ACCOUNT_SID) return null;
  return `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${recordingSid}.mp3`;
}

// -----------------------------
// Lead parsing via Chat Completions
// -----------------------------
function normalizeKeyLoose(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/[\s_'"״׳]/g, "");
}

function coerceLeadFields(obj) {
  const out = {};
  const entries = Object.entries(obj || {}).map(([k, v]) => [normalizeKeyLoose(k), v]);
  const loose = Object.fromEntries(entries);

  const getLoose = (names) => {
    for (const n of names) {
      const key = normalizeKeyLoose(n);
      if (loose[key] !== undefined && loose[key] !== null && String(loose[key]).trim() !== "")
        return loose[key];
    }
    return null;
  };

  out.is_lead = obj && typeof obj.is_lead === "boolean" ? obj.is_lead : !!getLoose(["is_lead", "islead"]);
  out.intent = getLoose(["intent", "סיבתפנייה", "סיבת_פנייה"]) || "unknown";
  out.full_name = getLoose(["full_name", "fullname", "שםמלא", "שם_מלא"]) || null;

  const phoneCandidate =
    getLoose(["phone_number", "phonenumber", "טלפון", "טלפוןלחזרה", "טלפון_לחזרה", "טלפוחזרה"]) || null;
  out.phone_number = phoneCandidate ? String(phoneCandidate) : null;

  const prefersCaller = getLoose(["prefers_caller_id", "preferscallerid", "מזוהה", "מספרמזוהה"]);
  if (typeof prefersCaller === "boolean") out.prefers_caller_id = prefersCaller;
  else if (typeof prefersCaller === "string") out.prefers_caller_id = /כן|נכון|true|1/.test(prefersCaller);
  else out.prefers_caller_id = null;

  out.brand = getLoose(["brand", "מותג"]) || null;
  out.model = getLoose(["model", "דגם"]) || null;
  out.reason = getLoose(["reason", "סיבת_פנייה", "סיבתפנייה"]) || null;
  out.notes = getLoose(["notes", "הערות"]) || null;

  return out;
}

function hasHebrewLetters(s) {
  return /[\u0590-\u05FF]/.test(String(s || ""));
}

async function translateToHebrewIfNeeded(connId, text, purpose = "") {
  const src = String(text || "").trim();
  if (!src) return src;
  if (hasHebrewLetters(src)) return src;
  if (!OPENAI_API_KEY) return src;
  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: MB_LEAD_PARSING_MODEL || "gpt-4.1-mini",
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              `Translate to Hebrew. Return ONLY valid JSON: {"he": "..."}. Preserve product/model names and phone numbers as-is. Purpose: ${purpose}`,
          },
          { role: "user", content: src },
        ],
        temperature: 0.2,
      }),
    });
    if (!response.ok) return src;
    const data = await response.json();
    const raw = data.choices?.[0]?.message?.content;
    if (!raw) return src;
    const parsed = JSON.parse(raw);
    const he = String(parsed?.he || "").trim();
    return he || src;
  } catch (e) {
    logDebug(connId, `translateToHebrewIfNeeded failed: ${e && (e.message || e)}`);
    return src;
  }
}

async function extractLeadFromConversation(conversationLog, connId, botName, businessName) {
  const tag = "LeadParse";
  if (!OPENAI_API_KEY) return null;
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;

  try {
    const conversationText = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const basePrompt = getPrompt(
      "LEAD_CAPTURE_PROMPT",
      `החזירו json תקין בלבד (בלי טקסט נוסף) לפי הסכמה: {"is_lead":boolean,"intent":"sales"|"support"|"delivery"|"message"|"unknown","full_name":string|null,"phone_number":string|null,"prefers_caller_id":boolean|null,"brand":string|null,"model":string|null,"reason":string|null,"notes":string|null}; מלאו מהשיחה בלבד; אם חסר ערך—null.`
    );
    const systemPrompt = `${basePrompt}\n\nכל שדות הטקסט reason ו-notes חייבים להיות בעברית (גם אם הלקוח דיבר באנגלית). אם נמסר מספר יבואן/מוביל—ציינו זאת במפורש ב-notes. אין להמציא.`.trim();

    const userPrompt = `
Transcript between a caller and a voice bot named "${botName}" for "${businessName}".
Return a JSON object ONLY.
Transcript:
${conversationText}
`.trim();

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
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
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      logError(connId, `${tag} HTTP ${response.status}`, text);
      return null;
    }

    const data = await response.json();
    const raw = data.choices?.[0]?.message?.content;
    if (!raw) return null;

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (_) {
      parsed = null;
    }
    if (!parsed || typeof parsed !== "object") return null;

    const coerced = coerceLeadFields(parsed);

    // Ensure webhook-facing fields are in Hebrew, even if caller spoke English.
    coerced.reason = await translateToHebrewIfNeeded(connId, coerced.reason, "reason");
    coerced.notes = await translateToHebrewIfNeeded(connId, coerced.notes, "notes");

    logInfo(connId, "Lead parsed.", coerced);
    return coerced;
  } catch (err) {
    logError(connId, "Lead parse error", err);
    return null;
  }
}

function isAbandonedReason(reason) {
  const r = String(reason || "").toLowerCase();
  return (
    r.includes("ws_closed") ||
    r.includes("ws_error") ||
    r.includes("openai_ws_closed") ||
    r.includes("openai_ws_error") ||
    r.includes("abandoned") ||
    r.includes("disconnect") ||
    r.includes("network") ||
    r.includes("timeout") ||
    r.includes("twilio_ws_closed") ||
    r.includes("twilio_ws_error") ||
    r.includes("twilio_stop")
  );
}

function mapCallStatus(reason, plannedEnd) {
  const r = String(reason || "").toLowerCase();
  if (r.includes("error")) return "error";
  if (plannedEnd) return "completed";
  if (isAbandonedReason(reason)) return "abandoned";
  return "completed";
}

function buildDetailedNotesHe({ parsedLead, importerPhoneProvided, matchedImporter, callerIL }) {
  const p = parsedLead || {};
  const parts = [];

  switch (p.intent) {
    case "support":
      parts.push("נפתחה קריאת שירות בגיל ספורט.");
      break;
    case "sales":
      parts.push("נרשמה פנייה למכירות.");
      break;
    case "delivery":
      parts.push("נפתחה פנייה למחלקת אספקה ומשלוחים.");
      break;
    case "message":
      parts.push("נרשמה הודעה כללית.");
      break;
    default:
      break;
  }

  if (p.reason) parts.push(`סיבת פנייה: ${p.reason}.`);
  if (p.brand) parts.push(`מותג: ${p.brand}.`);
  if (p.model) parts.push(`דגם: ${p.model}.`);

  if (importerPhoneProvided && matchedImporter?.phone) {
    parts.push(`נמסר מספר טלפון ישיר ליבואן (${matchedImporter.label}): ${digitsOnly(matchedImporter.phone)}.`);
  } else if (matchedImporter?.phone && matchedImporter?.label) {
    parts.push(`זוהה יבואן תואם: ${matchedImporter.label} (${digitsOnly(matchedImporter.phone)}).`);
  }

  // Phone summary
  const phone = digitsOnly(p.phone_number) || "";
  if (phone) parts.push(`טלפון לחזרה: ${phone}.`);
  else if (callerIL) parts.push(`טלפון מזוהה: ${digitsOnly(callerIL)}.`);

  return parts
    .map((s) => String(s || "").trim())
    .filter(Boolean)
    .join(" ");
}

// -----------------------------
// Express & HTTP
// -----------------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({ ok: true, ts: Date.now(), sheets_loaded_at: sheetsCache.loadedAt });
});

// Twilio Voice webhook -> returns TwiML with Stream
app.post("/voice", (req, res) => {
  const host = process.env.DOMAIN || req.headers.host;
  const wsUrl =
    process.env.MB_TWILIO_STREAM_URL ||
    `wss://${String(host || "").replace(/^https?:\/\//, "")}/twilio-media-stream`;

  const caller = req.body.From || "";
  const called = req.body.To || "";

  const twiml = `
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${caller}"/>
      <Parameter name="called" value="${called}"/>
      <Parameter name="source" value="GilSport Voice AI"/>
    </Stream>
  </Connect>
</Response>`.trim();

  res.type("text/xml").send(twiml);
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// -----------------------------
// Webhook payload builders
// -----------------------------
async function sendWebhook(url, payload, connId, label) {
  if (!url) return { ok: false, skipped: true };
  try {
    const res = await fetchWithTimeout(
      url,
      { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(payload) },
      4500
    );
    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(connId, `${label} webhook HTTP ${res.status}`, txt);
      return { ok: false, status: res.status };
    }
    logInfo(connId, `${label} webhook delivered status=${res.status}`);
    return { ok: true, status: res.status };
  } catch (err) {
    logError(connId, `${label} webhook error`, err);
    return { ok: false, error: String(err) };
  }
}

// -----------------------------
// Per-call handler
// -----------------------------
wss.on("connection", async (twilioWs, req) => {
  const connId = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  logAlways(`WS connection`, { at: nowIso(), ua: req.headers["user-agent"], url: req.url });

  if (!OPENAI_API_KEY) {
    logError(connId, "Missing OPENAI_API_KEY – closing.");
    twilioWs.close();
    return;
  }

  loadSheetsCache("OnConnect")
    .then(() => {
      importerIndex = buildImporterIndex(sheetsCache.settings);
      allowedPhoneDigits = buildAllowedPhoneDigitsFromSettings(sheetsCache.settings, callerIL ? [callerIL] : []);
    })
    .catch(() => {});

  let streamSid = null;
  let callSid = null;
  let callerRaw = null;
  let callerIL = null;

  let recordingSid = null;
  let recordingUrl = null;

  // OpenAI Realtime WS (kept as a variable so we can close it on endCall)
  let openAiWs = null;

  let openAiReady = false;
  let hasActiveResponse = false;
  let botSpeaking = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  let plannedEnd = false;
  let goodbyeEndScheduled = false;
  let callStartTs = Date.now();
  let lastMediaTs = Date.now();
  let idleCheckInterval = null;
  let idleWarningSent = false;
  let idleHangupScheduled = false;
  let maxCallTimeout = null;
  let maxCallWarningTimeout = null;

  let callEnded = false;

  let capturedPhoneIL = null;
  let phoneCorrectionSent = false;

  let preferredGender = null;
  let genderInstructionSent = false;
  let baseInstructions = null;

  // Session add-ons that depend on Twilio start data (caller id, captured phone, etc.)
  // These may arrive before OpenAI WS is ready.
  const pendingSessionAddons = [];

  let conversationLog = [];

  // Harden phone number reading / confirmations across all flows
  let lastAskedPhoneDigits = null;
  let lastAskedPhonePurpose = null;
  let phoneConfirmed = false;

  // Dynamic importer matching (from SETTINGS)
  let importerIndex = [];
  let matchedImporter = null; // { base,label,phone,keywords }
  let importerPhoneProvided = false;

  // Allowed digits we will accept the model to speak when a phone number is involved.
  let allowedPhoneDigits = buildAllowedPhoneDigitsFromSettings(sheetsCache.settings);

  function getGraceMs() {
    const raw = MB_HANGUP_GRACE_MS && MB_HANGUP_GRACE_MS > 0 ? MB_HANGUP_GRACE_MS : 3000;
    return Math.max(2000, Math.min(raw, 8000));
  }

  function sendModelPrompt(openAiWs, text, purpose) {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;
    if (hasActiveResponse) return;

    openAiWs.send(
      JSON.stringify({
        type: "conversation.item.create",
        item: { type: "message", role: "user", content: [{ type: "input_text", text }] },
      })
    );
    openAiWs.send(JSON.stringify({ type: "response.create" }));
    hasActiveResponse = true;
    botTurnActive = true;
    logDebug(connId, `response.create SPEAK purpose=${purpose || "no-tag"} text=${text}`);
  }

  function detectGenderPreference(text) {
    const t = String(text || "").toLowerCase();
    if (/(אני\s*(?:גבר|בן)|פנה\s*אלי\s*בלשון\s*זכר|בלשון\s*זכר|תדבר\s*אלי\s*בלשון\s*זכר)/.test(t)) return "male";
    if (/(אני\s*(?:אישה|בת)|פני\s*אלי\s*בלשון\s*נקבה|בלשון\s*נקבה|תדברי\s*אלי\s*בלשון\s*נקבה)/.test(t)) return "female";
    return null;
  }

  function updateSessionInstructions(openAiWs, addon, label) {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;
    const base = String(baseInstructions || "").trim();
    if (!base) return;
    const next = `${base}\n\n${String(addon || "").trim()}`.trim();
    openAiWs.send(JSON.stringify({ type: "session.update", session: { instructions: next } }));
    logInfo(connId, `session.update (${label || "addon"}) applied.`);
  }

  function queueSessionAddon(addon, label) {
    const text = String(addon || "").trim();
    if (!text) return;
    if (openAiReady && openAiWs && openAiWs.readyState === WebSocket.OPEN && String(baseInstructions || "").trim()) {
      updateSessionInstructions(openAiWs, text, label);
      return;
    }
    pendingSessionAddons.push({ addon: text, label: label || "addon" });
  }

  function scheduleForceEndAfterGrace(reason, closingMessage) {
    const graceMs = getGraceMs();
    setTimeout(() => {
      endCall(reason, closingMessage).catch(() => {});
    }, graceMs);
  }

  async function endCall(reason, closingMessage) {
    if (callEnded) return;
    callEnded = true;

    if (idleCheckInterval) clearInterval(idleCheckInterval);
    if (maxCallTimeout) clearTimeout(maxCallTimeout);
    if (maxCallWarningTimeout) clearTimeout(maxCallWarningTimeout);

    const { businessName, botName, closing } = buildSystemInstructionsFromSheets();
    const effectiveClosing = String(closingMessage || closing || "").trim();

    const endedAt = nowIso();
    const startedAt = new Date(callStartTs).toISOString();
    const durationSec = Math.max(0, Math.round((Date.now() - callStartTs) / 1000));

    let parsedLead = null;
    try {
      parsedLead = await extractLeadFromConversation(conversationLog, connId, botName, businessName);
    } catch (_) {}

    const callerILLocal = callerIL || toIsraeliLocalFromAny(callerRaw) || null;

    const coercedPhone =
      normalizePhoneNumber(parsedLead?.phone_number, callerRaw) ||
      normalizePhoneNumber(capturedPhoneIL, callerRaw) ||
      normalizePhoneNumber(callerILLocal, callerRaw) ||
      null;

    if (parsedLead && typeof parsedLead === "object") {
      parsedLead.phone_number = coercedPhone;
    }

    if (parsedLead && typeof parsedLead === "object") {
      // Ensure notes are descriptive and Hebrew.
      parsedLead.notes = buildDetailedNotesHe({
        parsedLead,
        importerPhoneProvided,
        matchedImporter,
        callerIL: callerILLocal,
      });
    }

    const isFullLead = !!(parsedLead && parsedLead.is_lead === true && coercedPhone);
    const call_status = mapCallStatus(reason, plannedEnd);

    const EVENT =
      parsedLead?.intent === "support"
        ? "שירות לקוחות"
        : parsedLead?.intent === "sales"
          ? "מכירות"
          : parsedLead?.intent === "delivery"
            ? "אספקה ומשלוחים"
            : parsedLead?.intent === "message"
              ? "הודעה כללית"
              : "לא ידוע";

    if (recordingSid && !recordingUrl) {
      recordingUrl = await buildRecordingUrl(recordingSid);
    }

    const transcript = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const payloadBase = {
      call_id: callSid || streamSid || `call_${Date.now()}`,
      callSid: callSid || null,
      streamSid: streamSid || null,

      started_at: startedAt,
      ended_at: endedAt,
      duration_sec: durationSec,

      caller_id_raw: callerRaw || null,
      caller_id_il: callerILLocal || null,
      caller_id_e164:
        toE164FromIsraeliLocal(callerILLocal) || (callerRaw && String(callerRaw).startsWith("+") ? callerRaw : null),

      collected_phone_il: coercedPhone || null,
      collected_phone_e164: coercedPhone ? toE164FromIsraeliLocal(coercedPhone) : null,

      business_name: businessName,
      bot_name: botName,
      EVENT,      call_status,
      reason: reason || null,
      closingMessage: effectiveClosing || null,

      recording_sid: recordingSid || null,
      recording_url: recordingUrl || null,

      transcript,
      conversationLog,
      parsedLead: parsedLead || null,
      isFullLead,
    };
    if (MB_CALL_LOG_ENABLED && MB_CALL_LOG_WEBHOOK_URL) {
      await sendWebhook(MB_CALL_LOG_WEBHOOK_URL, payloadBase, connId, "CallLog");
    }

    if (MB_ENABLE_LEAD_CAPTURE && MB_WEBHOOK_URL && isFullLead) {
      await sendWebhook(MB_WEBHOOK_URL, payloadBase, connId, "FINAL Lead");
    }

    if (
      MB_ENABLE_ABANDONED_WEBHOOK &&
      MB_ABANDONED_WEBHOOK_URL &&
      !isFullLead &&
      isAbandonedReason(reason) &&
      !plannedEnd
    ) {
      await sendWebhook(MB_ABANDONED_WEBHOOK_URL, payloadBase, connId, "ABANDONED");
    }

    if (callSid) hangupTwilioCall(callSid, connId).catch(() => {});

    // IMPORTANT: Close OpenAI WS when the call ends.
    // This prevents orphaned OpenAI sessions that later emit "session_expired" after ~60 minutes.
    try {
      if (openAiWs) {
        if (openAiWs.readyState === WebSocket.OPEN) {
          openAiWs.close(1000, "call_end");
        } else if (openAiWs.readyState === WebSocket.CONNECTING) {
          // If still connecting, force-terminate to avoid leaks
          if (typeof openAiWs.terminate === "function") openAiWs.terminate();
        }
      }
    } catch (_) {}

    try {
      if (twilioWs.readyState === WebSocket.OPEN) twilioWs.close();
    } catch (_) {}
  }

  // -----------------------------
  // OpenAI Realtime WS
  // -----------------------------
  openAiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  openAiWs.on("open", () => {
    openAiReady = true;
    const { opening, instructions } = buildSystemInstructionsFromSheets();
    baseInstructions = instructions;

    const effectiveSilenceMs = MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS;

    openAiWs.send(
      JSON.stringify({
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
          max_response_output_tokens: "inf",
          instructions,
        },
      })
    );

    // Apply any queued instruction add-ons (caller id, captured phone, etc.)
    // that arrived before the OpenAI session was ready.
    while (pendingSessionAddons.length) {
      const item = pendingSessionAddons.shift();
      try {
        updateSessionInstructions(openAiWs, item.addon, item.label);
      } catch (_) {}
    }

    sendModelPrompt(
      openAiWs,
      `פתחי את השיחה עם הלקוח במשפט הבא (אפשר לשנות מעט את הניסוח אבל לא להאריך): "${opening}" ואז עצרי והמתיני לתשובה שלו.`,
      "opening_greeting"
    );
  });

  let currentBotText = "";

  openAiWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (err) {
      logError(connId, "Failed to parse OpenAI WS message", err);
      return;
    }

    switch (msg.type) {
      case "response.created":
        hasActiveResponse = true;
        botTurnActive = true;
        botSpeaking = false;
        noListenUntilTs = Date.now() + MB_NO_BARGE_TAIL_MS;
        currentBotText = "";
        break;

      case "response.output_text.delta":
      case "response.audio_transcript.delta": {
        const delta = msg.delta || "";
        if (delta) currentBotText += delta;
        break;
      }

      case "response.output_text.done":
      case "response.audio_transcript.done": {
        const text = String(currentBotText || "").trim();
        if (text) {
          conversationLog.push({ from: "bot", text });
          logAlways(`[BOT][${connId}] ${text}`);

          // Harden all digit read-backs (caller ID, provided phone, importer phones, delivery phones).
          // If the bot's spoken digits don't exactly match the expected digits, force a correction turn.
          if (!phoneCorrectionSent) {
            const saidLoose = extractPhoneDigitsLoose(text);
            if (saidLoose) {
              // Determine which number the bot is supposed to be reading right now.
              let expected = null;
              if (matchedImporter && /יבואן|מספר\s+ישיר|תרצו\s+לרשום/i.test(text)) {
                expected = digitsOnly(matchedImporter.phone);
              } else {
                expected = digitsOnly(lastAskedPhoneDigits || "") || digitsOnly(capturedPhoneIL || "");
              }

              // Track if we actually provided the importer number
              if (matchedImporter) {
                const impDig = digitsOnly(matchedImporter.phone);
                if (impDig && saidLoose === impDig) importerPhoneProvided = true;
              }

              const looksLikePhoneReadback = /זה\s*נכון|נכון\?|לחזור\s*למספר|המספר\s*לחזרה|תרצו\s+לרשום|המספר\s+הוא/i.test(text);
              if (
                expected &&
                looksLikePhoneReadback &&
                expected.length >= 7 &&
                saidLoose.length >= 7 &&
                saidLoose !== expected
              ) {
              phoneCorrectionSent = true;
              const expectedSpoken = formatDigitsForHebrewSpeech(expected);
              logError(connId, "Model repeated wrong phone digits; forcing correction.", {
                expected,
                model_said: saidLoose,
                purpose: lastAskedPhonePurpose || "unknown",
              });
              try {
                openAiWs.send(JSON.stringify({ type: "response.cancel" }));
              } catch (_) {}
              hasActiveResponse = false;
              botSpeaking = false;
              botTurnActive = false;
              sendModelPrompt(
                openAiWs,
                `תיקון חובה: המספר הוא "${expectedSpoken}". חזרי עליו בדיוק ספרה-ספרה, עם פסיקים בין הספרות, ושאלי: "זה נכון?" בלי להוסיף או להשמיט ספרות ובלי לשנות סדר.`,
                "phone_correction"
              );
              }
            }
          }

          // If this looks like a closing utterance, mark planned end so Twilio stop won't be classified as abandoned.
          if (/להתראות|יום\s+נעים|תודה\s+שפנית|נחזור\s+אליכם\s+בהקדם/.test(text)) {
            plannedEnd = true;
            if (!goodbyeEndScheduled && MB_HANGUP_AFTER_GOODBYE) {
              goodbyeEndScheduled = true;
              setTimeout(() => {
                endCall("goodbye", text).catch(() => {});
              }, MB_HANGUP_GRACE_MS);
            }
          }
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

        if (twilioWs.readyState === WebSocket.OPEN) {
          twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: b64 } }));
        }
        break;
      }

      case "response.audio.done":
        botSpeaking = false;
        botTurnActive = false;
        break;

      case "response.completed":
        hasActiveResponse = false;
        botSpeaking = false;
        botTurnActive = false;
        break;

      case "conversation.item.input_audio_transcription.completed": {
        if (!MB_LOG_TRANSCRIPTS) break;

        const raw = String(msg.transcript || "").trim();
        const hasRealUserYet = conversationLog.some((m) => m.from === "user" && (m.text || "").trim().length >= 4);

        if (!raw) break;
        if (isTranscriptGarbage(raw, hasRealUserYet)) {
          logDebug(connId, `Filtered garbage transcript: "${raw}"`);
          break;
        }

        const t = raw.replace(/\s+/g, " ").replace(/\s+([,.:;!?])/g, "$1").trim();
        if (!t) break;

        conversationLog.push({ from: "user", text: t });
        logAlways(`[CALLER][${connId}] ${t}`);

        // If the bot just asked to confirm a phone number and the caller confirms, prevent re-asking.
        const lastBot = [...conversationLog].reverse().find((m) => m.from === "bot" && (m.text || "").trim());
        const lastBotText = String(lastBot?.text || "").trim();
        const userLow = t.toLowerCase();
        const userConfirmed =
          /^(כן|נכון|מאשר|מאשרת|בדיוק|יופי|סבבה|אוקיי|ok|okay|yes|yeah|yep|good|yay)\b/i.test(userLow);
        if (lastAskedPhoneDigits && userConfirmed && /זה\s*נכון|נכון\s*\?|לחזור\s*למספר|המספר\s*לחזרה/i.test(lastBotText)) {
          phoneConfirmed = true;
          lastAskedPhoneDigits = null;
          lastAskedPhonePurpose = null;
          queueSessionAddon(
            "המספר אושר על-ידי הלקוח. אל תשאלי שוב על אימות מספר הטלפון אלא אם הלקוח מבקש לשנות או לתקן.",
            "phone_confirmed"
          );
        }

        // Dynamic importer match: if the caller mentions a known brand keyword, lock the importer phone.
        if (importerIndex && importerIndex.length) {
          const norm = normalizeLoose(t);
          let best = null;
          for (const it of importerIndex) {
            for (const kw of it.keywords) {
              if (kw && norm.includes(kw)) {
                if (!best || kw.length > best.kw.length) best = { it, kw };
              }
            }
          }
          if (best && (!matchedImporter || matchedImporter.base !== best.it.base)) {
            matchedImporter = best.it;
            importerPhoneProvided = false;
            const phoneSpoken = formatDigitsForHebrewSpeech(matchedImporter.phone);
            allowedPhoneDigits.add(digitsOnly(matchedImporter.phone));
            queueSessionAddon(
              `זוהה מותג/יבואן תואם: "${matchedImporter.label}". אם הלקוח מבקש מספר יבואן, המספר הישיר היחיד שמותר למסור הוא: "${phoneSpoken}" (בדיוק). אין למסור מספר אחר.`,
              "importer_lock"
            );
          }
        }

        const gPref = detectGenderPreference(t);
        if (gPref && gPref !== preferredGender) {
          preferredGender = gPref;
          const addon =
            gPref === "male"
              ? 'הלקוח ביקש לפנות אליו בלשון זכר (אבל עדיין ברבים: "אתם"). אל תתנצלי ואל תדגישי את זה, פשוט התאימי ניסוח.'
              : 'הלקוחה ביקשה לפנות אליה בלשון נקבה (אבל עדיין ברבים: "אתן"). אל תתנצלי ואל תדגישי את זה, פשוט התאימי ניסוח.';
          if (!genderInstructionSent) {
            genderInstructionSent = true;
            updateSessionInstructions(openAiWs, addon, "gender_pref");
          }
        }

        const phoneFromSpeech = extractBestPhoneFromText(t);
        if (phoneFromSpeech) {
          capturedPhoneIL = phoneFromSpeech;
          logDebug(connId, `Captured phone from speech: ${capturedPhoneIL}`);

          // Inject the captured phone number so the model will repeat it digit-by-digit reliably.
          const spoken = formatDigitsForHebrewSpeech(capturedPhoneIL);
          lastAskedPhoneDigits = digitsOnly(capturedPhoneIL);
          lastAskedPhonePurpose = "captured_phone";
          queueSessionAddon(
            `המספר שנקלט מהלקוח לחזרה הוא: "${spoken}". כשאת מאשרת מספר טלפון—חובה לקרוא ספרה-ספרה, עם פסיקים/הפסקות, בדיוק כפי שמופיע כאן, ואז לשאול: "זה נכון?". אין להשמיט או לשנות ספרות.`,
            "captured_phone"
          );
        }

        break;
      }

      case "error": {
        const code = msg?.error?.code || null;

        // If the call already ended, ignore late OpenAI error events (prevents log noise).
        if (callEnded) {
          logDebug(connId, `OpenAI error after call ended (ignored) code=${code || "unknown"}`, msg?.error || msg);
          break;
        }

        // OpenAI Realtime hard-limits sessions (commonly 60 minutes).
        // Handle it as a controlled call end instead of emitting an error log.
        if (code === "session_expired") {
          plannedEnd = true;
          logInfo(connId, "OpenAI session expired (max duration). Ending call.", msg?.error || msg);
          hasActiveResponse = false;
          botSpeaking = false;
          botTurnActive = false;
          noListenUntilTs = 0;

          endCall("openai_session_expired", null).catch(() => {});
          break;
        }

        logError(connId, "OpenAI error event", msg);
        hasActiveResponse = false;
        botSpeaking = false;
        botTurnActive = false;
        noListenUntilTs = 0;
        break;
      }

      default:
        break;
    }
  });

  openAiWs.on("close", () => {
    if (!callEnded) endCall("openai_ws_closed", null).catch(() => {});
  });
  openAiWs.on("error", (err) => {
    logError(connId, "OpenAI WS error", err);
    if (!callEnded) endCall("openai_ws_error", null).catch(() => {});
  });

  // -----------------------------
  // Twilio stream handlers
  // -----------------------------
  twilioWs.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (err) {
      logError(connId, "Failed to parse Twilio WS message", err);
      return;
    }

    const event = msg.event;

    if (event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;

      const cp = msg.start?.customParameters || {};
      callerRaw = cp.caller || cp.From || cp.from || msg.start?.caller || msg.start?.from || null;

      // Derive caller id (IL local) and inject into the model instructions.
      // This prevents the bot from claiming it cannot see the caller ID and enables proper caller-id validation.
      callerIL = toIsraeliLocalFromAny(callerRaw) || null;
      if (callerIL) {
        const callerSpoken = formatDigitsForHebrewSpeech(callerIL);
        lastAskedPhoneDigits = digitsOnly(callerIL);
        lastAskedPhonePurpose = "caller_id";
        allowedPhoneDigits = buildAllowedPhoneDigitsFromSettings(sheetsCache.settings, [callerIL]);
        queueSessionAddon(
          `מספר הטלפון המזוהה של המתקשר הוא: "${callerSpoken}". כשאת שואלת האם לחזור למספר המזוהה—חובה קודם להקריא אותו ספרה-ספרה עם פסיקים בדיוק כמו כאן, ואז לשאול "לחזור למספר הזה?". אסור להשמיט/להוסיף ספרות. לעולם אל תגידי שאינך רואה/יודעת את המספר.`,
          "caller_id"
        );
      }

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logAlways(`[TWILIO_START][${connId}] ${JSON.stringify(msg.start || {})}`);

      if (msg.start?.recordingSid) {
        recordingSid = msg.start.recordingSid;
        recordingUrl = await buildRecordingUrl(recordingSid);
        logInfo(connId, "Recording started.", { recording_sid: recordingSid });
      }

      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          idleWarningSent = true;
          sendModelPrompt(openAiWs, `אני עדיין כאן על הקו, אתם איתי?`, "idle_warning");
        }

        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          plannedEnd = true;
          sendModelPrompt(openAiWs, `נראה שהשיחה התנתקה. אם תרצו, אפשר להתקשר שוב ולהשאיר פרטים.`, "idle_timeout");
          scheduleForceEndAfterGrace("idle_timeout", null);
        }
      }, 1000);

      if (MB_MAX_CALL_MS > 0) {
        if (MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
          maxCallWarningTimeout = setTimeout(() => {
            sendModelPrompt(openAiWs, `אנחנו מתקרבים לסיום הזמן לשיחה הזאת. תרצו להשאיר פרטים כדי שנחזור אליכם?`, "max_call_warning");
          }, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
        }
        maxCallTimeout = setTimeout(() => {
          plannedEnd = true;
          sendModelPrompt(openAiWs, `נאלץ לסיים כדי לפנות את הקו. תרצו להשאיר פרטים כדי שנחזור אליכם?`, "max_call_duration");
          scheduleForceEndAfterGrace("max_call_duration", null);
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
      logAlways(`[TWILIO_STOP][${connId}] stream stopped`);
      if (!plannedEnd && !callEnded) {
        endCall("twilio_stop", null).catch(() => {});
      } else if (!callEnded) {
        endCall("twilio_stop_planned", null).catch(() => {});
      }
    }
  });

  twilioWs.on("close", () => {
    logAlways(`[TWILIO_CLOSE][${connId}] socket closed`);
    if (!callEnded) endCall("twilio_ws_closed", null).catch(() => {});
  });

  twilioWs.on("error", (err) => {
    logError(connId, "Twilio WS error", err);
    if (!callEnded) endCall("twilio_ws_error", null).catch(() => {});
  });
});

// -----------------------------
// Start server
// -----------------------------
server.listen(PORT, () => {
  console.log(`==> Your service is live 🎉`);
  console.log(`==> Available at your primary URL ${process.env.RENDER_EXTERNAL_URL || ""}`);
  loadSheetsCache("Startup").catch((err) => console.error("[ERROR] Startup sheets load failed", err));
});
