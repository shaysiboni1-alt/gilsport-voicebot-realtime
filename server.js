// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets
// MODE A: Server-driven dialog (deterministic prompts from Sheets)
//         OpenAI is used ONLY for NLU (JSON extraction), not for free-form replies.

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

// Base style override (kept as-is; does not change model/voice settings)
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

// Fallbacks (used only if Sheets is empty)
const FALLBACK_EMPTY_INSTRUCTIONS = "סליחה, לא הבנתי. תוכלו לחזור בבקשה?";
const FALLBACK_ROUTING_CLARIFY =
  "כדי לעזור במדויק—זה לגבי התעניינות במוצר, שירות/תקלה/אחריות, משלוח/אספקה, או להשאיר הודעה למישהו מהצוות?";
const FALLBACK_PHONE_COLLECT =
  "לא קלטתי מספר תקין. תגידו בבקשה מספר טלפון בן 10 ספרות שמתחיל ב-0.";
const FALLBACK_PHONE_CONFIRM = "רק לוודא—המספר לחזרה הוא: {number}. נכון?";
const FALLBACK_NAME_INVALID = "אפשר בבקשה שם מלא?";
let warnedSheetsEmpty = false;

// --------------------------------------------------
// Webhook helpers (single endpoint)
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
    if (!resp.ok) debug("Webhook non-200", event, resp.status);
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
  prompts: {},
  settings: {},
  kbFacts: [],
  doNotSay: [],
  suppliersImporters: [],
  deliveryContacts: [],
  routingRules: [],
  businessInfo: []
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

    const kbFactsRange = valueRanges.find((vr) => (vr.range || "").startsWith("KB_FACTS!"));
    const doNotSayRange = valueRanges.find((vr) => (vr.range || "").startsWith("DO_NOT_SAY!"));
    const suppliersImportersRange = valueRanges.find((vr) =>
      (vr.range || "").startsWith("SUPPLIERS_IMPORTERS!")
    );
    const deliveryContactsRange = valueRanges.find((vr) =>
      (vr.range || "").startsWith("DELIVERY_CONTACTS!")
    );

    const promptsRows = (promptsRange?.values || []).slice();
    const settingsRows = (settingsRange?.values || []).slice();

    const kbFactsRows = rowsToObjects((kbFactsRange?.values || []).slice());
    const doNotSayRows = rowsToObjects((doNotSayRange?.values || []).slice());
    const suppliersImportersRows = rowsToObjects((suppliersImportersRange?.values || []).slice());
    const deliveryContactsRows = rowsToObjects((deliveryContactsRange?.values || []).slice());

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
      `Sheets loaded (prompts=${Object.keys(prompts).length}, settings=${Object.keys(settings).length}, kbFacts=${kbFactsRows.length}, doNotSay=${doNotSayRows.length}, suppliersImporters=${suppliersImportersRows.length}, deliveryContacts=${deliveryContactsRows.length})`
    );
  } catch (e) {
    error("Sheets load failed", e.message);
  }
}

const getPrompt = (id, fallback = "") => String(SHEETS.prompts[id] || fallback).trim();
const getSetting = (key, fallback = "") => String(SHEETS.settings[key] || fallback).trim();

// --------------------------------------------------
// Text normalization / matching
// --------------------------------------------------
const stripNiqqud = (s) => String(s || "").replace(/[\u0591-\u05C7]/g, "");

const normalizeForMatch = (s) =>
  stripNiqqud(String(s || ""))
    .toLowerCase()
    .replace(/[\u200f\u200e]/g, "")
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();

const hasWord = (haystackNorm, wordNorm) => {
  // word boundary-ish match for Hebrew/latin/numbers
  if (!haystackNorm || !wordNorm) return false;
  if (wordNorm.includes(" ")) return haystackNorm.includes(wordNorm);
  const re = new RegExp(`(^|\\s)${escapeRegExp(wordNorm)}(\\s|$)`, "i");
  return re.test(haystackNorm);
};

function escapeRegExp(s) {
  return String(s || "").replace(/[.*+?^${}()|[\\]\\]/g, "\\$&");
}

// --------------------------------------------------
// Phone helpers
// --------------------------------------------------
const formatSpacedDigits = (digits) => String(digits || "").split("").join(" ");

const normalizePhoneDigits = (raw) => {
  let text = String(raw || "");
  const wordToDigit = {
    "אפס": "0",
    "אחת": "1",
    "אחד": "1",
    "שתיים": "2",
    "שתים": "2",
    "שניים": "2",
    "שנים": "2",
    "שלוש": "3",
    "שלושה": "3",
    "ארבע": "4",
    "ארבעה": "4",
    "חמש": "5",
    "חמישה": "5",
    "שש": "6",
    "שישה": "6",
    "שבע": "7",
    "שבעה": "7",
    "שמונה": "8",
    "תשע": "9",
    "תשעה": "9"
  };
  for (const [word, digit] of Object.entries(wordToDigit)) {
    text = text.replace(new RegExp(`(^|\\s)${escapeRegExp(word)}(?=\\s|$)`, "g"), `$1${digit}`);
  }
  let digits = text.replace(/\D+/g, "");
  if (digits.startsWith("972") && digits.length > 3) digits = "0" + digits.slice(3);
  // Keep last 10 if someone said 11+ digits with leading 0
  if (digits.length > 10 && digits.startsWith("0")) digits = digits.slice(0, 10);
  return digits;
};

const isValidPhoneDigits = (digits) => {
  const d = String(digits || "").replace(/\D+/g, "");
  return d.length === 10 && d.startsWith("0");
};

// Hebrew-ish yes/no
// IMPORTANT: keep this STRICT. Ambiguous acknowledgements ("", "בסדר", "סבבה")
// must NOT advance decision points like phone-confirm or importer/carriers offers.
const isYes = (text) =>
  /(\bכן\b|נכון|מאשר|אישור|yes|yep|yeah)/i.test(String(text || "").trim());
const isNo = (text) => /(\bלא\b|לא תודה|לא נכון|no|nope|לא מעוניין|לא מסכים)/i.test(String(text || "").trim());

const isNameValid = (name) => {
  const t = String(name || "").trim().replace(/\s+/g, " ");
  if (t.length < 3 || t.length > 60) return false;
  const parts = t.split(" ").filter(Boolean);
  if (parts.length < 2) return false;
  // must include at least some letters
  return /[A-Za-z\u0590-\u05FF]/.test(t);
};

// --------------------------------------------------
// Business hours
// --------------------------------------------------
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

const isAfterHoursNow = () => {
  const hoursStr =
    getSetting("BUSINESS_HOURS", "") ||
    getSetting("HOURS", "") ||
    getSetting("WORKING_HOURS", "") ||
    "";
  const parsed = parseHours(hoursStr);
  if (!parsed) return false; // If unknown, do not force after-hours logic.

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

// --------------------------------------------------
// Sheets utilities (PROMPTS + SETTINGS only)
// --------------------------------------------------
const getFlowText = (key, fallback = "") => {
  const k = String(key || "").trim();
  if (!k) return String(fallback || "").trim();
  const p = String((SHEETS.prompts || {})[k] || "").trim();
  if (p) return p;
  const s = String((SHEETS.settings || {})[k] || "").trim();
  if (s) return s;
  return String(fallback || "").trim();
};

const renderFlowText = (template, vars = {}) => {
  if (!template) return "";
  return String(template).replace(/\{(\w+)\}/g, (match, k) =>
    Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : match
  );
};

// --------------------------------------------------
// KB_FACTS and DO_NOT_SAY matching
// --------------------------------------------------
const getKBFactsMatches = (utteranceNorm) => {
  const rows = Array.isArray(SHEETS.kbFacts) ? SHEETS.kbFacts : [];
  const matches = [];

  for (const r of rows) {
    const keywords = String(r.keywords || r.keyword || r.triggers || "").trim();
    const answer = String(r.answer_he || r.answer || r.content_he || "").trim();
    const intent = String(r.intent || r.route || r.category || "").trim().toLowerCase();

    if (!keywords) continue;

    const kws = keywords
      .split(",")
      .map((x) => normalizeForMatch(x))
      .filter(Boolean);
    if (!kws.length) continue;

    let score = 0;
    for (const kw of kws) {
      if (hasWord(utteranceNorm, kw)) score += 1;
    }
    if (score > 0) {
      matches.push({ score, intent, answer, row: r });
    }
  }

  matches.sort((a, b) => b.score - a.score);
  return matches;
};

const matchDoNotSay = (utteranceNorm) => {
  const rows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
  for (const r of rows) {
    const triggersRaw = String(r.trigger_examples || r.triggers || "").trim();
    const safe = String(r.safe_response_he || r.safe_response || "").trim();
    if (!triggersRaw || !safe) continue;
    const triggers = triggersRaw
      .split(",")
      .map((x) => normalizeForMatch(x))
      .filter(Boolean);
    for (const tr of triggers) {
      if (hasWord(utteranceNorm, tr)) {
        return {
          topic: String(r.forbidden_topic || "").trim(),
          safe_response_he: safe,
          row: r
        };
      }
    }
  }
  return null;
};

// --------------------------------------------------
// SUPPLIERS_IMPORTERS and DELIVERY_CONTACTS
// --------------------------------------------------
const findImporterByBrand = (brand) => {
  const b = String(brand || "").trim();
  if (!b) return null;
  const rows = Array.isArray(SHEETS.suppliersImporters) ? SHEETS.suppliersImporters : [];
  const bNorm = normalizeForMatch(b);

  for (const r of rows) {
    const whenToGive = String(r.when_to_give || "").trim();
    if (whenToGive && whenToGive !== "fault_or_specific_request") {
      // Only support the defined rule.
      continue;
    }

    // brand_keyword may include aliases separated by commas
    const keywords = String(r.brand_keyword || r.brand_keywords || "").trim();
    const brandName = String(r.brand_name || "").trim();

    const candidates = [];
    if (brandName) candidates.push(brandName);
    if (keywords)
      candidates.push(
        ...keywords
          .split(",")
          .map((x) => String(x || "").trim())
          .filter(Boolean)
      );

    for (const c of candidates) {
      const cNorm = normalizeForMatch(c);
      if (cNorm && (cNorm === bNorm || hasWord(bNorm, cNorm) || hasWord(cNorm, bNorm))) {
        const phone = String(r.phone_e164 || r.phone || "").trim();
        const digits = normalizePhoneDigits(phone);
        return {
          brand: brandName || b,
          importer_name: String(r.importer_name || "").trim(),
          phone_digits: isValidPhoneDigits(digits) ? digits : ""
        };
      }
    }
  }

  return null;
};

const buildCarriersList = () => {
  const rows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
  const list = [];
  for (const r of rows) {
    const name = String(r.name || "").trim();
    const phone = String(r.phone_e164 || r.phone || "").trim();
    const digits = normalizePhoneDigits(phone);
    if (!isValidPhoneDigits(digits)) continue;
    const s = name ? `${name} – ${formatSpacedDigits(digits)}` : formatSpacedDigits(digits);
    list.push(s);
  }
  return list;
};

const mentionsSameDayByDeliveryContacts = (utteranceNorm) => {
  const rows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
  for (const r of rows) {
    const rule = String(r.rule || r.condition_rule || "").trim();
    if (rule && rule !== "after_hours_same_day_only") continue;
    const kws = String(r.condition_keywords || "")
      .split(",")
      .map((x) => normalizeForMatch(x))
      .filter(Boolean);
    if (!kws.length) continue;
    for (const kw of kws) {
      if (hasWord(utteranceNorm, kw)) return true;
    }
  }
  return false;
};

const detectMessageTargetInText = (t) => {
  const s = String(t || "").trim();
  if (!s) return "";
  const m = s.match(/(?:הודעה\s+(?:ל|עבור)\s+)([^,.\n\r]{2,40})/);
  if (!m) return "";
  let name = String(m[1] || "").trim();
  name = name.replace(/\b(בבקשה|תודה)\b/g, "").replace(/\s+/g, " ").trim();
  if (name.length < 2) return "";
  return name;
};

// --------------------------------------------------
// MODE A: Slot engine (server decides what to say)
// --------------------------------------------------
const emptySlots = () => ({
  // Shared
  full_name: "",
  caller_id: "", // always stored
  callback_phone: "",
  additional_phone: "",

  // Sales
  product_type: "",
  product_model: "",
  product_brand: "",

  // Support
  issue_desc: "",
  importer_found: false,
  importer_phone: "",

  // Delivery
  delivery_desc: "",
  after_hours: false,
  carriers_info_given: false,

  // Message
  message_target: "",
  message_body: ""
});

const ROUTES = new Set(["sales", "support", "delivery", "message"]);

function routeToHumanEvent(route, slots) {
  if (route === "sales") return "מתעניין במכירות";
  if (route === "support") return "שירות לקוחות – תקלה";
  if (route === "delivery") return "אספקה / משלוח";
  if (route === "message") return "הודעה";
  return "call_ended";
}

function getFirstMissingForRoute(route, slots) {
  if (!ROUTES.has(route)) return "routing";

  if (route === "sales") {
    if (!slots.product_type) return "FLOW_SALES_PRODUCT";
    if (!slots.full_name) return "FLOW_SALES_NAME";
    // model/brand are optional; we ask but skip if already present
    if (!slots.product_model) return "FLOW_SALES_MODEL_ASK";
    if (!slots.product_brand) return "FLOW_SALES_BRAND_ASK";
    // phone confirmation handled separately
    if (!slots.callback_phone) return "FLOW_SALES_PHONE_CONFIRM";
    return "FLOW_SALES_DONE";
  }

  if (route === "support") {
    if (!slots.issue_desc) return "FLOW_SUPPORT_ISSUE_DESC";
    if (!slots.product_model) return "FLOW_SUPPORT_MODEL_ASK";
    // brand can be unknown, but we ask once
    if (!slots.product_brand) return "FLOW_SUPPORT_BRAND_ASK";
    if (!slots.full_name) return "FLOW_SUPPORT_NAME";
    if (!slots.callback_phone) return "FLOW_SUPPORT_PHONE_CONFIRM";
    return "FLOW_SUPPORT_DONE";
  }

  if (route === "delivery") {
    if (!slots.delivery_desc) return "FLOW_DELIVERY_DESC";
    if (!slots.full_name) return "FLOW_DELIVERY_NAME";
    if (!slots.callback_phone) return "FLOW_DELIVERY_PHONE_CONFIRM";
    return "FLOW_DELIVERY_DONE";
  }

  if (route === "message") {
    if (!slots.message_target) return "FLOW_MESSAGE_TARGET";
    if (!slots.message_body) return "FLOW_MESSAGE_BODY";
    if (!slots.full_name) return "FLOW_MESSAGE_NAME";
    if (!slots.callback_phone) return "FLOW_MESSAGE_PHONE_CONFIRM";
    return "FLOW_MESSAGE_DONE";
  }

  return "routing";
}

function buildPromptText(promptId, ctx) {
  const { slots, callerDigits, carriersList } = ctx;

  const spacedCaller = callerDigits ? formatSpacedDigits(callerDigits) : "";
  const closing = getSetting("CLOSING_SCRIPT", "");

  // Route prompts
  if (promptId === "FLOW_ROUTING_CLARIFY") {
    return getFlowText("FLOW_ROUTING_CLARIFY", FALLBACK_ROUTING_CLARIFY);
  }

  // SALES
  if (promptId === "FLOW_SALES_PRODUCT") return getFlowText("FLOW_SALES_PRODUCT", "");
  if (promptId === "FLOW_SALES_NAME") return getFlowText("FLOW_SALES_NAME", "");

  if (promptId === "FLOW_SALES_MODEL_ASK") {
    // Insert PRICE_CLAIM_SENTENCE + optional coupon code derived from SETTINGS
    const priceClaim = getSetting("PRICE_CLAIM_SENTENCE", "").trim();
    const coupon = getSetting("SALES_COUPON_CODE", "").trim();
    const couponLine = coupon ? `קוד הקופון לרכישה באתר הוא ${String(coupon).split("").join(" ")}.` : "";

    const ask = getFlowText("FLOW_SALES_MODEL_ASK", "האם יש דגם ספציפי?");
    const parts = [];
    if (priceClaim) parts.push(priceClaim);
    if (couponLine) parts.push(couponLine);
    parts.push(ask);
    return parts.filter(Boolean).join(" ").trim();
  }

  if (promptId === "FLOW_SALES_MODEL_COLLECT") return getFlowText("FLOW_SALES_MODEL_COLLECT", "אוקיי, מה הדגם?");

  if (promptId === "FLOW_SALES_BRAND_ASK") return getFlowText("FLOW_SALES_BRAND_ASK", "האם יש מותג ספציפי?");
  if (promptId === "FLOW_SALES_BRAND_COLLECT") return getFlowText("FLOW_SALES_BRAND_COLLECT", "אוקיי, מה המותג?");

  if (promptId === "FLOW_SALES_PHONE_CONFIRM") {
    const tmpl = getFlowText("FLOW_SALES_PHONE_CONFIRM", "האם נוח לחזור אליכם למספר הזה: {caller_id}?");
    if (!spacedCaller) return getFlowText("FLOW_SALES_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
    return renderFlowText(tmpl, { caller_id: spacedCaller });
  }
  if (promptId === "FLOW_SALES_PHONE_COLLECT") return getFlowText("FLOW_SALES_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
  if (promptId === "FLOW_SALES_PHONE_CONFIRM_NEW") {
    const tmpl = getFlowText("FLOW_SALES_PHONE_CONFIRM_NEW", FALLBACK_PHONE_CONFIRM);
    const spaced = slots.additional_phone ? formatSpacedDigits(slots.additional_phone) : "";
    return renderFlowText(tmpl, { number: spaced || "" });
  }

  if (promptId === "FLOW_SALES_DONE") {
    const done = getFlowText("FLOW_SALES_DONE", "");
    return [done, closing].filter(Boolean).join(" ").trim();
  }

  // SUPPORT
  if (promptId === "FLOW_SUPPORT_ISSUE_DESC") return getFlowText("FLOW_SUPPORT_ISSUE_DESC", "");
  if (promptId === "FLOW_SUPPORT_MODEL_ASK") return getFlowText("FLOW_SUPPORT_MODEL_ASK", "האם יש דגם ספציפי?");
  if (promptId === "FLOW_SUPPORT_MODEL_COLLECT") return getFlowText("FLOW_SUPPORT_MODEL_COLLECT", "מה הדגם?");

  if (promptId === "FLOW_SUPPORT_BRAND_ASK") return getFlowText("FLOW_SUPPORT_BRAND_ASK", "האם יש מותג ספציפי?");
  if (promptId === "FLOW_SUPPORT_BRAND_COLLECT") return getFlowText("FLOW_SUPPORT_BRAND_COLLECT", "מה המותג?");

  if (promptId === "FLOW_SUPPORT_IMPORTER_FOUND_NOTICE") {
    const tmpl = getFlowText(
      "FLOW_SUPPORT_IMPORTER_FOUND_NOTICE",
      "מצאתי שיש מספר ישיר ליבואן של {brand}. רוצה שאמסור?"
    );
    return renderFlowText(tmpl, { brand: slots.product_brand || "" });
  }
  if (promptId === "FLOW_SUPPORT_IMPORTER_FOUND_GIVE_NUMBER") {
    const tmpl = getFlowText("FLOW_SUPPORT_IMPORTER_FOUND_GIVE_NUMBER", "המספר הוא: {number}");
    const spaced = slots.importer_phone ? formatSpacedDigits(slots.importer_phone) : "";
    return renderFlowText(tmpl, { number: spaced });
  }
  if (promptId === "FLOW_SUPPORT_IMPORTER_FOUND_DECLINE") return getFlowText("FLOW_SUPPORT_IMPORTER_FOUND_DECLINE", "אין בעיה, ממשיכים.");

  if (promptId === "FLOW_SUPPORT_NAME") return getFlowText("FLOW_SUPPORT_NAME", "");

  if (promptId === "FLOW_SUPPORT_PHONE_CONFIRM") {
    const tmpl = getFlowText("FLOW_SUPPORT_PHONE_CONFIRM", "האם לחזור אליכם למספר הזה: {caller_id}?");
    if (!spacedCaller) return getFlowText("FLOW_SUPPORT_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
    return renderFlowText(tmpl, { caller_id: spacedCaller });
  }
  if (promptId === "FLOW_SUPPORT_PHONE_COLLECT") return getFlowText("FLOW_SUPPORT_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
  if (promptId === "FLOW_SUPPORT_PHONE_CONFIRM_NEW") {
    const tmpl = getFlowText("FLOW_SUPPORT_PHONE_CONFIRM_NEW", FALLBACK_PHONE_CONFIRM);
    const spaced = slots.additional_phone ? formatSpacedDigits(slots.additional_phone) : "";
    return renderFlowText(tmpl, { number: spaced || "" });
  }

  if (promptId === "FLOW_SUPPORT_DONE") {
    const done = getFlowText("FLOW_SUPPORT_DONE", "");
    return [done, closing].filter(Boolean).join(" ").trim();
  }

  // DELIVERY
  if (promptId === "FLOW_DELIVERY_CARRIERS_OFFER") return getFlowText("FLOW_DELIVERY_CARRIERS_OFFER", "");
  if (promptId === "FLOW_DELIVERY_CARRIERS_GIVE") {
    const tmpl = getFlowText("FLOW_DELIVERY_CARRIERS_GIVE", "אלה מספרי המובילים: {carriers}");
    return renderFlowText(tmpl, { carriers: (carriersList || []).join(", ") });
  }
  if (promptId === "FLOW_DELIVERY_CARRIERS_DECLINE") return getFlowText("FLOW_DELIVERY_CARRIERS_DECLINE", "אין בעיה, ממשיכים");

  if (promptId === "FLOW_DELIVERY_DESC") return getFlowText("FLOW_DELIVERY_DESC", "");
  if (promptId === "FLOW_DELIVERY_NAME") return getFlowText("FLOW_DELIVERY_NAME", "");

  if (promptId === "FLOW_DELIVERY_PHONE_CONFIRM") {
    const tmpl = getFlowText("FLOW_DELIVERY_PHONE_CONFIRM", "האם נוח לחזור אליכם למספר הזה: {caller_id}?");
    if (!spacedCaller) return getFlowText("FLOW_DELIVERY_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
    return renderFlowText(tmpl, { caller_id: spacedCaller });
  }
  if (promptId === "FLOW_DELIVERY_PHONE_COLLECT") return getFlowText("FLOW_DELIVERY_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
  if (promptId === "FLOW_DELIVERY_PHONE_CONFIRM_NEW") {
    const tmpl = getFlowText("FLOW_DELIVERY_PHONE_CONFIRM_NEW", FALLBACK_PHONE_CONFIRM);
    const spaced = slots.additional_phone ? formatSpacedDigits(slots.additional_phone) : "";
    return renderFlowText(tmpl, { number: spaced || "" });
  }

  if (promptId === "FLOW_DELIVERY_DONE") {
    const done = getFlowText("FLOW_DELIVERY_DONE", "");
    return [done, closing].filter(Boolean).join(" ").trim();
  }

  // MESSAGE
  if (promptId === "FLOW_MESSAGE_TARGET") return getFlowText("FLOW_MESSAGE_TARGET", "");
  if (promptId === "FLOW_MESSAGE_TARGET_CONFIRM") {
    const tmpl = getFlowText("FLOW_MESSAGE_TARGET_CONFIRM", "רק לוודא—ההודעה מיועדת ל־{target}, נכון?");
    return renderFlowText(tmpl, { target: slots.message_target || "" });
  }
  if (promptId === "FLOW_MESSAGE_BODY") return getFlowText("FLOW_MESSAGE_BODY", "");
  if (promptId === "FLOW_MESSAGE_NAME") return getFlowText("FLOW_MESSAGE_NAME", "");

  if (promptId === "FLOW_MESSAGE_PHONE_CONFIRM") {
    const tmpl = getFlowText("FLOW_MESSAGE_PHONE_CONFIRM", "האם לחזור אליכם למספר הזה: {caller_id}?");
    if (!spacedCaller) return getFlowText("FLOW_MESSAGE_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
    return renderFlowText(tmpl, { caller_id: spacedCaller });
  }
  if (promptId === "FLOW_MESSAGE_PHONE_COLLECT") return getFlowText("FLOW_MESSAGE_PHONE_COLLECT", FALLBACK_PHONE_COLLECT);
  if (promptId === "FLOW_MESSAGE_PHONE_CONFIRM_NEW") {
    const tmpl = getFlowText("FLOW_MESSAGE_PHONE_CONFIRM_NEW", FALLBACK_PHONE_CONFIRM);
    const spaced = slots.additional_phone ? formatSpacedDigits(slots.additional_phone) : "";
    return renderFlowText(tmpl, { number: spaced || "" });
  }

  if (promptId === "FLOW_MESSAGE_DONE") {
    const tmpl = getFlowText("FLOW_MESSAGE_DONE", "");
    const done = renderFlowText(tmpl, { target: slots.message_target || "הצוות" });
    return [done, closing].filter(Boolean).join(" ").trim();
  }

  // Generic fallbacks
  if (promptId === "FLOW_NAME_INVALID") return getFlowText("FLOW_NAME_INVALID", FALLBACK_NAME_INVALID);
  if (promptId === "FLOW_PHONE_MISSING_DIGIT") return getFlowText("FLOW_PHONE_MISSING_DIGIT", FALLBACK_PHONE_COLLECT);

  return getFlowText(promptId, FALLBACK_EMPTY_INSTRUCTIONS);
}

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

// Public recording proxy (optional)
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

  // Realtime API schema compatibility: prefer `output_modalities`, fall back to legacy `modalities`.
  // Some model versions / deployments validate one or the other.
  let USE_OUTPUT_MODALITIES = true;
  let DID_FALLBACK_MODALITIES = false;

  const connTag = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;

  // Stream parameters
  let caller = "";
  let called = "";
  try {
    const u = new URL(req.url || "", "http://localhost");
    caller = u.searchParams.get("caller") || "";
    called = u.searchParams.get("called") || "";
  } catch (_) {}

  // Call/session state
  let callSid = null;
  let startedAt = nowIso();
  let endedAt = null;

  let transcriptTurns = []; // {from, text, at}
  const pushTurn = (from, text) => {
    const t = String(text || "").trim();
    if (!t) return;
    transcriptTurns.push({ from, text: t, at: nowIso() });
    if (transcriptTurns.length > 400) transcriptTurns = transcriptTurns.slice(-400);
  };

  // Abandoned / ended dedupe guards
  let sentCallEnded = false;
  let sentCallAbandoned = false;
  let hangupRequested = false;

  // Server-driven dialog state
  const state = {
    route: "",
    stage: "routing", // logical stage (prompt_id or routing)
    last_prompt_id: "",
    last_prompt_text: "",
    awaiting: "opening", // opening | nlu | speak
    nlu_text_buf: "",
    nlu_request_id: "",
    speak_purpose: "",
    done: false,
    final_event: "",
    final_payload_sent: false,
    last_caller_final: "",
    last_bot_final: "",
    last_user_utterance_for_abandoned: "",
    slots: emptySlots(),
    caller_digits: "",
    carriers_list: [],
    importer_offer_pending: false,
    importer_offer_asked: false,
    importer_number_given: false,
    message_target_confirm_pending: false,
    // phone confirmation flow
    phone_mode: "", // confirm_caller | collect_new | confirm_new
    // sales optional asks (to ensure we ask model/brand questions only once)
    sales_model_asked: false,
    sales_brand_asked: false,
    support_model_asked: false,
    support_brand_asked: false,
    // delivery carrier offer asked
    carriers_offer_asked: false
  };

  const ensureCallerDigits = () => {
    const raw = String(caller || "").trim();
    const digits = normalizePhoneDigits(raw);
    return isValidPhoneDigits(digits) ? digits : "";
  };

  const applyCallerId = () => {
    state.caller_digits = ensureCallerDigits();
    state.slots.caller_id = state.caller_digits || "";
  };

  const buildFinalPayload = () => {
    const ended = endedAt || nowIso();
    applyCallerId();

    const caller_id = state.slots.caller_id || "";

    const callback_phone = state.slots.callback_phone || "";
    const additional_phone = state.slots.additional_phone || "";

    return {
      callSid,
      streamSid: twilioStreamSid,
      caller,
      called,
      started_at: startedAt,
      ended_at: ended,
      timestamp: ended,
      route: state.route || "",
      stage: state.stage || "",

      event_name: state.final_event || "",

      full_name: state.slots.full_name || "",
      caller_id,
      callback_phone,
      additional_phone: additional_phone && additional_phone !== caller_id ? additional_phone : "",

      product_type: state.slots.product_type || "",
      product_model: state.slots.product_model || "",
      product_brand: state.slots.product_brand || "",

      issue_desc: state.slots.issue_desc || "",
      importer_found: Boolean(state.slots.importer_found),
      importer_phone: state.slots.importer_phone || "",

      delivery_desc: state.slots.delivery_desc || "",
      after_hours: Boolean(state.slots.after_hours),
      carriers_info_given: Boolean(state.slots.carriers_info_given),

      message_target: state.slots.message_target || "",
      message_body: state.slots.message_body || "",

      caller_last_utterance: state.last_caller_final || "",
      bot_last_utterance: state.last_bot_final || "",
      transcript: transcriptTurns,

      recording_url_public: makeRecordingPublicUrl(callSid)
    };
  };

  const applyWebhookDefaults = (payload = {}) => {
    applyCallerId();
    const started = startedAt || nowIso();
    const ended = endedAt || nowIso();

    return {
      callSid: callSid || "",
      streamSid: twilioStreamSid || "",
      caller: caller || "",
      caller_id: state.slots.caller_id || "",
      called: called || "",
      started_at: started,
      ended_at: ended,
      route: state.route || "",
      stage: state.stage || "",
      full_name: state.slots.full_name || "",
      callback_phone: state.slots.callback_phone || "",
      recording_url_public: makeRecordingPublicUrl(callSid),
      ...payload
    };
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

  // --------------------------------------------------
  // OpenAI WS
  // --------------------------------------------------
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

  if (!OPENAI_API_KEY) {
    error("OPENAI_API_KEY missing — closing call");
    try {
      twilioWs.close();
    } catch (_) {}
    return;
  }

  // Audio buffering while assistant is speaking or NLU is running
  let awaitingResponse = false;
  let pausedAudioBuffer = [];
  let isFlushingBufferedAudio = false;

  const speakVerbatim = (text, purpose = "") => {
    const t = String(text || "").trim();
    if (!t) {
      state.last_prompt_text = FALLBACK_EMPTY_INSTRUCTIONS;
      return;
    }

    state.awaiting = "speak";
    state.speak_purpose = purpose;
    awaitingResponse = true;

    // Strong verbatim instruction (no extra question; no additions)
    const baseStyle = MB_BASE_STYLE && MB_BASE_STYLE.trim() ? `סגנון כללי: ${MB_BASE_STYLE.trim()}` : "";
    const instructions = [
      baseStyle,
      "אתם מקריא טקסט (TTS) בעברית בשם נטע.",
      "המשימה: להפיק פלט שזהה *בדיוק* לטקסט שמופיע בין התגים <SAY> ו-</SAY>.",
      "חובה: ללא תוספות, ללא שינויי ניסוח, ללא שאלה נוספת, ללא משפטי סיום כלליים.",
      "אם יש מספרים — להקריא ספרה-ספרה.",
      "פלטו רק את הטקסט עצמו (בלי התגים, בלי הקדמה).",
      "<SAY>",
      t,
      "</SAY>"
    ]
      .filter(Boolean)
      .join("\n");

    debug(`[${connTag}] response.create SPEAK purpose=${purpose} text=${preview(t, 160)}`);

    // Critical: do NOT let the model see prior conversation; provide the text as input.
    safeOpenAISend({
      type: "response.create",
      response: {
        // Do not write assistant output into the default conversation
        conversation: "none",

        // Clear context for this out-of-band response
        input: [],

        // Request audio (transcript is included with audio outputs)
        ...(USE_OUTPUT_MODALITIES
          ? { output_modalities: ["audio"] }
          : { modalities: ["audio", "text"] }),
        instructions,
        temperature: 0,
        max_output_tokens: 400
      }
    });
  };

  const nluExtract = (utterance) => {
    const u = String(utterance || "").trim();
    if (!u) return;

    state.awaiting = "nlu";
    awaitingResponse = true;
    state.nlu_text_buf = "";
    state.nlu_request_id = crypto.randomBytes(8).toString("hex");

    // Provide minimal context; explicitly forbid hallucination.
    const schema = {
      route: "sales|support|delivery|message|unknown",
      yes_no: "yes|no|unknown",
      full_name: "string or empty",
      product_type: "string or empty",
      product_model: "string or empty",
      product_brand: "string or empty",
      issue_desc: "string or empty",
      delivery_desc: "string or empty",
      message_target: "string or empty",
      message_body: "string or empty",
      phone_digits: "10 digits starting 0 or empty",
      mentions_same_day: "true|false",
      mentions_after_hours: "true|false"
    };

    const context = {
      current_route: state.route || "",
      current_stage: state.stage || "",
      existing: {
        full_name: state.slots.full_name || "",
        product_type: state.slots.product_type || "",
        product_model: state.slots.product_model || "",
        product_brand: state.slots.product_brand || "",
        issue_desc: state.slots.issue_desc || "",
        delivery_desc: state.slots.delivery_desc || "",
        message_target: state.slots.message_target || "",
        message_body: state.slots.message_body || "",
        callback_phone: state.slots.callback_phone || ""
      },
      caller_id: state.slots.caller_id || ""
    };

    const instructions = [
      "אתם מנוע חילוץ נתונים (NLU) לשיחה בעברית.",
      "המשימה: לחלץ רק מידע שמופיע במפורש במשפט האחרון של הלקוח. לא להמציא. לא לנחש.",
      "אם פרט לא מופיע במפורש—החזירו מחרוזת ריקה או unknown בהתאם.",
      "החזירו JSON בלבד. בלי טקסט נוסף. בלי Markdown.",
      `request_id: ${state.nlu_request_id}`,
      "--- SCHEMA ---",
      JSON.stringify(schema),
      "--- CONTEXT ---",
      JSON.stringify(context),
      "--- UTTERANCE (HE) ---",
      u
    ].join("\n");

    debug(`[${connTag}] response.create NLU rid=${state.nlu_request_id} u=${preview(u, 160)}`);

    safeOpenAISend({
      type: "response.create",
      response: {
        conversation: "none",
        input: [
          {
            type: "message",
            role: "user",
            content: [{ type: "input_text", text: u }]
          }
        ],
        ...(USE_OUTPUT_MODALITIES ? { output_modalities: ["text"] } : { modalities: ["text"] }),
        instructions,
        temperature: 0,
        max_output_tokens: 500
      }
    });
  };

  const decideRouteFromKB = (utteranceNorm) => {
    const u = String(utteranceNorm || "").trim();
    const matches = getKBFactsMatches(u);
    if (matches.length) {
      // Prefer explicit route intents
      for (const m of matches) {
        const r = String(m.route || "").toLowerCase();
        if (r === "sales" || r === "support" || r === "delivery" || r === "message") return r;
      }
      // Otherwise take first match
      const r0 = String(matches[0].route || "").toLowerCase();
      if (r0 === "sales" || r0 === "support" || r0 === "delivery" || r0 === "message") return r0;
    }

    // Built-in fallback heuristics (do not rely on the model)
    // Keep conservative: only strong keywords.
    const has = (arr) => arr.some((k) => u.includes(k));
    if (has(["תקלה", "אחריות", "שירות", "תיקון", "לא עובד", "לא נדלק", "בעיה"])) return "support";
    if (has(["משלוח", "אספקה", "שליח", "מוביל", "הזמנה לא הגיעה", "מחכה למשלוח", "להיום"])) return "delivery";
    if (has(["הודעה", "להשאיר", "השארת", "למסור", "למנהל", "לעובד"])) return "message";
    if (has(["לקנות", "קניה", "קנייה", "רכישה", "רוצה", "מעוניין", "מתעניין", "מחפש", "מחפשת", "לקנות באתר"])) return "sales";
    if (has(["הודעה ל", "להשאיר הודעה", "מסר ל", "להעביר הודעה"])) return "message";
    if (has(["לקנות", "רכישה", "קנייה", "מתעניין", "מעוניין", "לקנות", "מחפש", "מוצר"])) return "sales";

    return "";
  };

  const decideKBAnswer = (utteranceNorm) => {
    // Return best factual answer (no route) if present
    const matches = getKBFactsMatches(utteranceNorm);
    if (!matches.length) return "";
    const best = matches[0];
    const intent = String(best.intent || "").trim().toLowerCase();
    if (intent && ROUTES.has(intent)) return "";
    const ans = String(best.answer || "").trim();
    return ans;
  };

  const advanceDialogAfterNLU = (utterance) => {
    const u = String(utterance || "").trim();
    const uNorm = normalizeForMatch(u);

    state.last_caller_final = u;
    state.last_user_utterance_for_abandoned = u;
    pushTurn("caller", u);
    always(`[CALLER][${connTag}]`, u);

    applyCallerId();

    // 1) DO_NOT_SAY gate
    const dns = matchDoNotSay(uNorm);
    if (dns) {
      // Say the safe response; do NOT advance stage.
      const safe = String(dns.safe_response_he || "").trim();
      state.stage = state.stage || "routing";
      speakVerbatim(safe, "do_not_say");
      return;
    }

    // 2) KB_FACTS factual answer (only if not currently collecting a required field)
    // If we are routing (no route yet), we can answer and then ask routing clarify.
    if (!state.route) {
      const kbAnswer = decideKBAnswer(uNorm);
      if (kbAnswer) {
        const follow = getFlowText("FLOW_ROUTING_CLARIFY", FALLBACK_ROUTING_CLARIFY);
        const combined = `${kbAnswer} ${follow}`.trim();
        state.stage = "routing";
        state.last_prompt_id = "FLOW_ROUTING_CLARIFY";
        state.last_prompt_text = follow;
        speakVerbatim(combined, "kb_fact_then_routing");
        return;
      }
    }

    // 3) Route selection (server-first: KB intent; fallback: NLU)
    if (!state.route) {
      const kbRoute = decideRouteFromKB(uNorm);
      if (kbRoute) state.route = kbRoute;
    }

    // 4) Special: message target mentioned at beginning
    if (!state.route) {
      // if user said "הודעה ל..." prefer message
      const target = detectMessageTargetInText(u);
      if (target) {
        state.route = "message";
        if (!state.slots.message_target) state.slots.message_target = target;
        state.message_target_confirm_pending = true;
      }
    }

    // 5) If still no route, ask routing clarify
    if (!state.route) {
      state.stage = "routing";
      const t = getFlowText("FLOW_ROUTING_CLARIFY", FALLBACK_ROUTING_CLARIFY);
      state.last_prompt_id = "FLOW_ROUTING_CLARIFY";
      state.last_prompt_text = t;
      speakVerbatim(t, "routing_clarify");
      return;
    }

    // 6) Delivery after-hours + same-day offer
    if (state.route === "delivery") {
      state.slots.after_hours = Boolean(isAfterHoursNow());
      const sameDayMention = mentionsSameDayByDeliveryContacts(uNorm) || /\bהיום\b/.test(uNorm);
      if (state.slots.after_hours && sameDayMention && !state.carriers_offer_asked && !state.slots.carriers_info_given) {
        state.carriers_offer_asked = true;
        state.stage = "delivery_carriers_offer";
        const offer = getFlowText("FLOW_DELIVERY_CARRIERS_OFFER", "יש ברשותי מספרי מובילים זמינים להיום. רוצה שאמסור?");
        state.last_prompt_id = "FLOW_DELIVERY_CARRIERS_OFFER";
        state.last_prompt_text = offer;
        speakVerbatim(offer, "delivery_carriers_offer");
        return;
      }
    }

    // 7) Route-specific dialog continuation
    applyCallerId();
    const callerDigits = state.caller_digits;
    const carriersList = state.carriers_list;

    // Handle any pending sub-flows BEFORE generic missing fields
    // 7a) Delivery carriers offer decision
    if (state.route === "delivery" && state.stage === "delivery_carriers_offer") {
      if (isYes(u)) {
        state.slots.carriers_info_given = true;
        state.carriers_list = buildCarriersList();
        const give = buildPromptText("FLOW_DELIVERY_CARRIERS_GIVE", {
          slots: state.slots,
          callerDigits,
          carriersList: state.carriers_list
        });
        // After giving carriers, immediately ask delivery_desc (single speak)
        const nextQ = buildPromptText("FLOW_DELIVERY_DESC", {
          slots: state.slots,
          callerDigits,
          carriersList: state.carriers_list
        });
        const combined = `${give} ${nextQ}`.trim();
        state.stage = "FLOW_DELIVERY_DESC";
        state.last_prompt_id = "FLOW_DELIVERY_DESC";
        state.last_prompt_text = nextQ;
        speakVerbatim(combined, "delivery_carriers_give_then_desc");
        return;
      }
      if (isNo(u)) {
        const decline = buildPromptText("FLOW_DELIVERY_CARRIERS_DECLINE", {
          slots: state.slots,
          callerDigits,
          carriersList
        });
        const nextQ = buildPromptText("FLOW_DELIVERY_DESC", {
          slots: state.slots,
          callerDigits,
          carriersList
        });
        const combined = `${decline} ${nextQ}`.trim();
        state.stage = "FLOW_DELIVERY_DESC";
        state.last_prompt_id = "FLOW_DELIVERY_DESC";
        state.last_prompt_text = nextQ;
        speakVerbatim(combined, "delivery_carriers_decline_then_desc");
        return;
      }
      // unclear → repeat offer
      const offer = buildPromptText("FLOW_DELIVERY_CARRIERS_OFFER", {
        slots: state.slots,
        callerDigits,
        carriersList
      });
      speakVerbatim(offer, "repeat_delivery_carriers_offer");
      return;
    }

    // 7b) Message target confirm
    if (state.route === "message" && state.message_target_confirm_pending) {
      if (isYes(u)) {
        state.message_target_confirm_pending = false;
        const nextQ = buildPromptText("FLOW_MESSAGE_BODY", { slots: state.slots, callerDigits, carriersList });
        state.stage = "FLOW_MESSAGE_BODY";
        state.last_prompt_id = "FLOW_MESSAGE_BODY";
        state.last_prompt_text = nextQ;
        speakVerbatim(nextQ, "message_body");
        return;
      }
      if (isNo(u)) {
        // Need target again
        state.slots.message_target = "";
        state.message_target_confirm_pending = false;
        const nextQ = buildPromptText("FLOW_MESSAGE_TARGET", { slots: state.slots, callerDigits, carriersList });
        state.stage = "FLOW_MESSAGE_TARGET";
        state.last_prompt_id = "FLOW_MESSAGE_TARGET";
        state.last_prompt_text = nextQ;
        speakVerbatim(nextQ, "message_target");
        return;
      }
      // unclear → repeat confirm
      const conf = buildPromptText("FLOW_MESSAGE_TARGET_CONFIRM", { slots: state.slots, callerDigits, carriersList });
      speakVerbatim(conf, "repeat_message_target_confirm");
      return;
    }

    // 7c) Support importer offer
    if (state.route === "support" && state.importer_offer_pending) {
      if (isYes(u)) {
        state.importer_offer_pending = false;
        state.importer_number_given = true;
        // Give number, then continue to name
        const give = buildPromptText("FLOW_SUPPORT_IMPORTER_FOUND_GIVE_NUMBER", {
          slots: state.slots,
          callerDigits,
          carriersList
        });
        const nextQ = buildPromptText("FLOW_SUPPORT_NAME", { slots: state.slots, callerDigits, carriersList });
        const combined = `${give} ${nextQ}`.trim();
        state.stage = "FLOW_SUPPORT_NAME";
        state.last_prompt_id = "FLOW_SUPPORT_NAME";
        state.last_prompt_text = nextQ;
        speakVerbatim(combined, "support_importer_give_then_name");
        return;
      }
      if (isNo(u)) {
        state.importer_offer_pending = false;
        const decline = buildPromptText("FLOW_SUPPORT_IMPORTER_FOUND_DECLINE", {
          slots: state.slots,
          callerDigits,
          carriersList
        });
        const nextQ = buildPromptText("FLOW_SUPPORT_NAME", { slots: state.slots, callerDigits, carriersList });
        const combined = `${decline} ${nextQ}`.trim();
        state.stage = "FLOW_SUPPORT_NAME";
        state.last_prompt_id = "FLOW_SUPPORT_NAME";
        state.last_prompt_text = nextQ;
        speakVerbatim(combined, "support_importer_decline_then_name");
        return;
      }
      // unclear → repeat offer
      const offer = buildPromptText("FLOW_SUPPORT_IMPORTER_FOUND_NOTICE", {
        slots: state.slots,
        callerDigits,
        carriersList
      });
      speakVerbatim(offer, "repeat_support_importer_offer");
      return;
    }

    // 7d) Phone flow
    if (state.phone_mode === "confirm_caller") {
      if (isYes(u)) {
        if (!callerDigits) {
          state.phone_mode = "collect_new";
          const q = buildPromptText(
            state.route === "sales"
              ? "FLOW_SALES_PHONE_COLLECT"
              : state.route === "support"
              ? "FLOW_SUPPORT_PHONE_COLLECT"
              : state.route === "delivery"
              ? "FLOW_DELIVERY_PHONE_COLLECT"
              : "FLOW_MESSAGE_PHONE_COLLECT",
            { slots: state.slots, callerDigits, carriersList }
          );
          state.last_prompt_text = q;
          speakVerbatim(q, "phone_collect_no_caller_id");
          return;
        }
        state.slots.callback_phone = callerDigits;
        state.phone_mode = "";
      } else if (isNo(u)) {
        state.phone_mode = "collect_new";
        const q = buildPromptText(
          state.route === "sales"
            ? "FLOW_SALES_PHONE_COLLECT"
            : state.route === "support"
            ? "FLOW_SUPPORT_PHONE_COLLECT"
            : state.route === "delivery"
            ? "FLOW_DELIVERY_PHONE_COLLECT"
            : "FLOW_MESSAGE_PHONE_COLLECT",
          { slots: state.slots, callerDigits, carriersList }
        );
        state.last_prompt_text = q;
        speakVerbatim(q, "phone_collect_new");
        return;
      } else {
        // unclear → repeat confirm
        const q = buildPromptText(
          state.route === "sales"
            ? "FLOW_SALES_PHONE_CONFIRM"
            : state.route === "support"
            ? "FLOW_SUPPORT_PHONE_CONFIRM"
            : state.route === "delivery"
            ? "FLOW_DELIVERY_PHONE_CONFIRM"
            : "FLOW_MESSAGE_PHONE_CONFIRM",
          { slots: state.slots, callerDigits, carriersList }
        );
        speakVerbatim(q, "repeat_phone_confirm");
        return;
      }
    }

    if (state.phone_mode === "collect_new") {
      const digits = normalizePhoneDigits(u);
      if (!isValidPhoneDigits(digits)) {
        const reprompt = getFlowText("PHONE_COLLECT_REPROMPT", FALLBACK_PHONE_COLLECT);
        speakVerbatim(reprompt, "phone_collect_reprompt");
        return;
      }
      state.slots.additional_phone = digits;
      state.phone_mode = "confirm_new";
      const q = buildPromptText(
        state.route === "sales"
          ? "FLOW_SALES_PHONE_CONFIRM_NEW"
          : state.route === "support"
          ? "FLOW_SUPPORT_PHONE_CONFIRM_NEW"
          : state.route === "delivery"
          ? "FLOW_DELIVERY_PHONE_CONFIRM_NEW"
          : "FLOW_MESSAGE_PHONE_CONFIRM_NEW",
        { slots: state.slots, callerDigits, carriersList }
      );
      state.last_prompt_text = q;
      speakVerbatim(q, "phone_confirm_new");
      return;
    }

    if (state.phone_mode === "confirm_new") {
      if (isYes(u)) {
        if (isValidPhoneDigits(state.slots.additional_phone)) {
          state.slots.callback_phone = state.slots.additional_phone;
          state.phone_mode = "";
        } else {
          state.phone_mode = "collect_new";
          const reprompt = getFlowText("PHONE_COLLECT_REPROMPT", FALLBACK_PHONE_COLLECT);
          speakVerbatim(reprompt, "phone_collect_reprompt");
          return;
        }
      } else if (isNo(u)) {
        state.phone_mode = "collect_new";
        const reprompt = getFlowText("PHONE_COLLECT_REPROMPT", FALLBACK_PHONE_COLLECT);
        speakVerbatim(reprompt, "phone_collect_again");
        return;
      } else {
        const q = buildPromptText(
          state.route === "sales"
            ? "FLOW_SALES_PHONE_CONFIRM_NEW"
            : state.route === "support"
            ? "FLOW_SUPPORT_PHONE_CONFIRM_NEW"
            : state.route === "delivery"
            ? "FLOW_DELIVERY_PHONE_CONFIRM_NEW"
            : "FLOW_MESSAGE_PHONE_CONFIRM_NEW",
          { slots: state.slots, callerDigits, carriersList }
        );
        speakVerbatim(q, "repeat_phone_confirm_new");
        return;
      }
    }

    // 8) Ask the next missing field
    const missing = getFirstMissingForRoute(state.route, state.slots);

    // Sales: optional asks should be ASK → if yes ask COLLECT, if no skip
    if (state.route === "sales") {
      if (missing === "FLOW_SALES_MODEL_ASK" && state.sales_model_asked) {
        // We already asked; do not ask again
        state.slots.product_model = state.slots.product_model || "";
      }
      if (missing === "FLOW_SALES_BRAND_ASK" && state.sales_brand_asked) {
        state.slots.product_brand = state.slots.product_brand || "";
      }
    }

    // Support: model required; brand optional but ask once
    if (state.route === "support") {
      if (missing === "FLOW_SUPPORT_MODEL_ASK" && state.support_model_asked && !state.slots.product_model) {
        // We asked but still missing → collect directly
        const q = buildPromptText("FLOW_SUPPORT_MODEL_COLLECT", { slots: state.slots, callerDigits, carriersList });
        state.stage = "FLOW_SUPPORT_MODEL_COLLECT";
        state.last_prompt_id = "FLOW_SUPPORT_MODEL_COLLECT";
        state.last_prompt_text = q;
        speakVerbatim(q, "support_model_collect");
        return;
      }
      if (missing === "FLOW_SUPPORT_BRAND_ASK" && state.support_brand_asked && !state.slots.product_brand) {
        // asked once; allow unknown → skip to name
        state.slots.product_brand = "";
      }
    }

    const nextId = getFirstMissingForRoute(state.route, state.slots);

    // Done?
    if (nextId.endsWith("_DONE")) {
      state.final_event = routeToHumanEvent(state.route, state.slots);
      const doneText = buildPromptText(nextId, { slots: state.slots, callerDigits, carriersList });
      state.stage = nextId;
      state.done = true;
      state.last_prompt_id = nextId;
      state.last_prompt_text = doneText;
      speakVerbatim(doneText, "done");
      return;
    }

    // Phone confirm stage
    if (nextId.endsWith("PHONE_CONFIRM")) {
      state.phone_mode = "confirm_caller";
      const q = buildPromptText(nextId, { slots: state.slots, callerDigits, carriersList });
      state.stage = nextId;
      state.last_prompt_id = nextId;
      state.last_prompt_text = q;
      speakVerbatim(q, "phone_confirm");
      return;
    }

    // Message: if target already known from first utterance, confirm
    if (state.route === "message" && nextId === "FLOW_MESSAGE_BODY" && state.slots.message_target && !state.message_target_confirm_pending && !state.last_prompt_id) {
      // no-op
    }

    // Ask for the missing field
    const q = buildPromptText(nextId, { slots: state.slots, callerDigits, carriersList });
    state.stage = nextId;
    state.last_prompt_id = nextId;
    state.last_prompt_text = q;

    // Track one-time asks
    if (nextId === "FLOW_SALES_MODEL_ASK") state.sales_model_asked = true;
    if (nextId === "FLOW_SALES_BRAND_ASK") state.sales_brand_asked = true;
    if (nextId === "FLOW_SUPPORT_MODEL_ASK") state.support_model_asked = true;
    if (nextId === "FLOW_SUPPORT_BRAND_ASK") state.support_brand_asked = true;

    // If message target was captured early, confirm it first
    if (state.route === "message" && state.slots.message_target && !state.message_target_confirm_pending) {
      if (state.stage === "FLOW_MESSAGE_TARGET") {
        // We don't need to ask target; confirm instead.
        state.message_target_confirm_pending = true;
        const conf = buildPromptText("FLOW_MESSAGE_TARGET_CONFIRM", {
          slots: state.slots,
          callerDigits,
          carriersList
        });
        state.stage = "FLOW_MESSAGE_TARGET_CONFIRM";
        state.last_prompt_id = "FLOW_MESSAGE_TARGET_CONFIRM";
        state.last_prompt_text = conf;
        speakVerbatim(conf, "message_target_confirm");
        return;
      }
    }

    // Support: after we get brand, check importer
    if (state.route === "support") {
      if (!state.importer_offer_asked && state.slots.product_brand) {
        const imp = findImporterByBrand(state.slots.product_brand);
        if (imp && imp.phone_digits) {
          state.slots.importer_found = true;
          state.slots.importer_phone = imp.phone_digits;
          state.importer_offer_asked = true;
          state.importer_offer_pending = true;
          const offer = buildPromptText("FLOW_SUPPORT_IMPORTER_FOUND_NOTICE", {
            slots: state.slots,
            callerDigits,
            carriersList
          });
          state.stage = "FLOW_SUPPORT_IMPORTER_FOUND_NOTICE";
          state.last_prompt_id = "FLOW_SUPPORT_IMPORTER_FOUND_NOTICE";
          state.last_prompt_text = offer;
          speakVerbatim(offer, "support_importer_offer");
          return;
        }
      }
    }

    speakVerbatim(q, "next_question");
  };

  const applyNLUToSlots = (nluObj, utterance) => {
    if (!nluObj || typeof nluObj !== "object") return;

    // Route (only if not set)
    const r = String(nluObj.route || "").trim().toLowerCase();
    if (!state.route && ROUTES.has(r)) state.route = r;

    // yes/no can be used by subflows; but we already parse with isYes/isNo.

    // Names
    const full = String(nluObj.full_name || "").trim();
    if (full && !state.slots.full_name && isNameValid(full)) {
      state.slots.full_name = full;
    }

    // Phone
    const phoneDigits = normalizePhoneDigits(String(nluObj.phone_digits || ""));
    if (isValidPhoneDigits(phoneDigits)) {
      // Only store as candidate if we are in phone collect mode
      if (state.phone_mode === "collect_new" || state.phone_mode === "confirm_new") {
        state.slots.additional_phone = phoneDigits;
      }
    }

    // Sales
    const pType = String(nluObj.product_type || "").trim();
    if (pType && !state.slots.product_type) state.slots.product_type = pType;

    const pModel = String(nluObj.product_model || "").trim();
    if (pModel && !state.slots.product_model) state.slots.product_model = pModel;

    const pBrand = String(nluObj.product_brand || "").trim();
    if (pBrand && !state.slots.product_brand) state.slots.product_brand = pBrand;

    // Support
    const issue = String(nluObj.issue_desc || "").trim();
    if (issue && !state.slots.issue_desc) state.slots.issue_desc = issue;

    // Delivery
    const del = String(nluObj.delivery_desc || "").trim();
    if (del && !state.slots.delivery_desc) state.slots.delivery_desc = del;

    // Message
    const target = String(nluObj.message_target || "").trim();
    if (target && !state.slots.message_target) state.slots.message_target = target;

    const body = String(nluObj.message_body || "").trim();
    if (body && !state.slots.message_body) state.slots.message_body = body;

    // Special: message target from raw utterance
    if (!state.slots.message_target) {
      const t = detectMessageTargetInText(utterance);
      if (t) {
        state.slots.message_target = t;
        state.message_target_confirm_pending = true;
      }
    }
  };

  // --------------------------------------------------
  // Idle timers / max call guard
  // --------------------------------------------------
  let idleWarnTimer = null;
  let idleHangTimer = null;
  let maxCallTimer = null;

  const resetIdleTimers = () => {
    if (idleWarnTimer) clearTimeout(idleWarnTimer);
    if (idleHangTimer) clearTimeout(idleHangTimer);

    idleWarnTimer = setTimeout(() => {
      if (sentCallEnded || sentCallAbandoned) return;
      if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
      // gentle reminder: repeat last prompt
      const t = state.last_prompt_text || getFlowText("FLOW_ROUTING_CLARIFY", FALLBACK_ROUTING_CLARIFY);
      speakVerbatim(t, "idle_warn_repeat_last");
    }, MB_IDLE_WARNING_MS);

    idleHangTimer = setTimeout(async () => {
      if (sentCallEnded || sentCallAbandoned) return;
      endedAt = nowIso();
      sentCallAbandoned = true;
      await sendWebhookEvent(
        "Abandoned",
        applyWebhookDefaults({
          ended_at: endedAt,
          stage: state.stage,
          caller_last_utterance: state.last_user_utterance_for_abandoned || "",
          bot_last_utterance: state.last_bot_final || "",
          transcript: transcriptTurns
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
      try {
        if (twilioWs) twilioWs.close();
      } catch (_) {}
    }, MB_IDLE_HANGUP_MS);
  };

  const startMaxCallTimer = () => {
    if (maxCallTimer) clearTimeout(maxCallTimer);
    maxCallTimer = setTimeout(async () => {
      if (sentCallEnded || sentCallAbandoned) return;
      endedAt = nowIso();
      sentCallAbandoned = true;
      await sendWebhookEvent(
        "Abandoned",
        applyWebhookDefaults({
          ended_at: endedAt,
          stage: state.stage,
          caller_last_utterance: state.last_user_utterance_for_abandoned || "",
          bot_last_utterance: state.last_bot_final || "",
          transcript: transcriptTurns
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
      try {
        if (twilioWs) twilioWs.close();
      } catch (_) {}
    }, MB_MAX_CALL_MS);
  };

  // --------------------------------------------------
  // OpenAI connect
  // --------------------------------------------------
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
      (!SHEETS.loaded_at ||
        (!Object.keys(SHEETS.prompts || {}).length && !Object.keys(SHEETS.settings || {}).length)) &&
      !warnedSheetsEmpty
    ) {
      warnedSheetsEmpty = true;
      console.warn("[WARNING] Sheets not loaded or empty; using fallbacks.");
    }

    const masterPrompt = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
    );

    const openingScript = getSetting("OPENING_SCRIPT", "שלום! מדברת נטע מגיל ספורט. במה אפשר לעזור?");

    always(`[${connTag}] SOURCES`, {
      sheets_loaded_at: SHEETS.loaded_at,
      opening_from: getSetting("OPENING_SCRIPT", "") ? "SETTINGS.OPENING_SCRIPT" : "FALLBACK.DEFAULT",
      master_from: getPrompt("MASTER_PROMPT", "") ? "PROMPTS.MASTER_PROMPT" : "FALLBACK.DEFAULT",
      opening_preview: preview(openingScript, 220),
      master_preview: preview(masterPrompt, 220)
    });

    // Keep model/voice settings as-is
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

    if (Object.prototype.hasOwnProperty.call(session, "voice_style")) delete session.voice_style;
    if (Object.prototype.hasOwnProperty.call(session, "speaking_rate")) delete session.speaking_rate;

    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    safeOpenAISend({ type: "session.update", session });

    // Speak opening verbatim (out-of-band, no conversation memory)
    awaitingResponse = true;
    state.awaiting = "opening";
    state.last_prompt_text = openingScript;

    safeOpenAISend({
      type: "response.create",
      response: {
        conversation: "none",
        // Clear context for opening so it is always verbatim
        input: [],
        ...(USE_OUTPUT_MODALITIES
          ? { output_modalities: ["audio"] }
          : { modalities: ["audio", "text"] }),
        instructions: [
          "אתם מקריא טקסט (TTS) בעברית בשם נטע.",
          "המשימה: להפיק פלט שזהה *בדיוק* לטקסט שמופיע בין התגים <SAY> ו-</SAY>.",
          "חובה: ללא תוספות, ללא שינויי ניסוח, ללא שאלה נוספת.",
          "פלטו רק את הטקסט עצמו (בלי התגים, בלי הקדמה).",
          "<SAY>",
          openingScript,
          "</SAY>"
        ].join("\n"),
        temperature: 0,
        max_output_tokens: 200
      }
    });

    // Flush buffered audio
    while (pendingAudio.length > 0 && openaiWs && openaiWs.readyState === WebSocket.OPEN) {
      const audio = pendingAudio.shift();
      safeOpenAISend({ type: "input_audio_buffer.append", audio });
    }

    resetIdleTimers();
    startMaxCallTimer();
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

  const parseNLUJson = (text) => {
    const raw = String(text || "").trim();
    if (!raw) return null;

    // Strip code fences if any
    const cleaned = raw
      .replace(/^```(?:json)?/i, "")
      .replace(/```$/i, "")
      .trim();

    // Attempt to locate first { ... }
    const first = cleaned.indexOf("{");
    const last = cleaned.lastIndexOf("}");
    if (first === -1 || last === -1 || last <= first) return null;
    const slice = cleaned.slice(first, last + 1);

    try {
      return JSON.parse(slice);
    } catch (_) {
      return null;
    }
  };

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

    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error event`, msg);

      // Compatibility fallback: some deployments reject `output_modalities` and only accept legacy `modalities`.
      try {
        const e = msg.error || {};
        const param = String(e.param || "");
        const m = String(e.message || "");
        const isOutputModalitiesErr =
          e.code === "unknown_parameter" &&
          (param.includes("output_modalities") || m.toLowerCase().includes("output_modalities"));

        if (USE_OUTPUT_MODALITIES && !DID_FALLBACK_MODALITIES && isOutputModalitiesErr) {
          DID_FALLBACK_MODALITIES = true;
          USE_OUTPUT_MODALITIES = false;
          always(`[${connTag}] Falling back to legacy response.modalities schema due to output_modalities rejection`);

          // Best-effort retry once based on the current awaiting mode.
          if (state.awaiting === "nlu" && state.last_user_final) {
            state.nlu_text_buf = "";
            nluExtract(state.last_user_final);
          } else if ((state.awaiting === "speak" || state.awaiting === "opening") && state.last_prompt_text) {
            // Re-send the last prompt verbatim in legacy mode.
            const t = String(state.last_prompt_text || "").trim();
            if (t) {
              safeOpenAISend({
                type: "response.create",
                response: {
                  conversation: "none",
                  input: [],
                  modalities: ["audio", "text"],
                  instructions: [
                    "אתם מקריא טקסט (TTS) בעברית בשם נטע.",
                    "המשימה: להפיק פלט שזהה *בדיוק* לטקסט שמופיע בין התגים <SAY> ו-</SAY>.",
                    "חובה: ללא תוספות, ללא שינויי ניסוח, ללא שאלה נוספת.",
                    "אם יש מספרים — להקריא ספרה-ספרה.",
                    "פלטו רק את הטקסט עצמו (בלי התגים, בלי הקדמה).",
                    "<SAY>",
                    t,
                    "</SAY>"
                  ].join("\n"),
                  temperature: 0,
                  max_output_tokens: 400
                }
              });
            }
          }
        }
      } catch (_) {}

      return;
    }

    // Capture spoken transcript for logs
    if (msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) {
        state.last_bot_final = t;
        pushTurn("bot", t);
        always(`[BOT][${connTag}]`, t);
      }
      return;
    }

    // Capture NLU response text
    if (state.awaiting === "nlu") {
      const t = String(msg.type || "");
      if (t.includes("response.text.delta") || t.includes("response.output_text.delta") || t.includes("response.output_text") || t.includes("response.text")) {
        if (typeof msg.delta === "string") state.nlu_text_buf += msg.delta;
        if (typeof msg.text === "string") state.nlu_text_buf += msg.text;
      }
      if (t === "response.text.done" || t === "response.output_text.done") {
        if (typeof msg.text === "string") state.nlu_text_buf += msg.text;
      }
    }

    // Caller transcription done → start NLU
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
        if (!utterance) return;

        // Ignore transcripts generated while flushing buffered audio
        if (isFlushingBufferedAudio) {
          state.last_caller_final = utterance;
          return;
        }

        resetIdleTimers();

        // Prevent duplicate processing
        if (utterance === state.last_caller_final) return;

        // Do not accept caller audio while we are responding
        if (awaitingResponse) {
          safeOpenAISend({ type: "response.cancel" });
          awaitingResponse = false;
        }

        // Apply caller_id
        applyCallerId();

        // Run NLU
        state.last_caller_final = utterance;
        nluExtract(utterance);
        return;
      }
    }

    // Response done (both NLU and speak)
    if (msg.type === "response.done") {
      awaitingResponse = false;

      // Flush buffered audio after we finish speaking/NLU
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

      // If we finished NLU, parse it and continue server-driven dialog
      if (state.awaiting === "nlu") {
        const obj = parseNLUJson(state.nlu_text_buf);
        if (obj) {
          applyNLUToSlots(obj, state.last_caller_final);
        }
        // Additional deterministic extraction from last prompt stage
        // (helps when NLU returns empty)
        const u = state.last_caller_final;
        const uNorm = normalizeForMatch(u);

        // If stage expects name, validate and store
        if (state.stage === "FLOW_SALES_NAME" || state.stage === "FLOW_SUPPORT_NAME" || state.stage === "FLOW_DELIVERY_NAME" || state.stage === "FLOW_MESSAGE_NAME") {
          if (!state.slots.full_name) {
            const candidate = String(u || "").trim().replace(/\s+/g, " ");
            if (isNameValid(candidate)) state.slots.full_name = candidate;
          }
        }

        // If stage expects product type
        if (state.stage === "FLOW_SALES_PRODUCT") {
          if (!state.slots.product_type) state.slots.product_type = u;
        }

        // If stage is sales model ask
        if (state.stage === "FLOW_SALES_MODEL_ASK") {
          if (isYes(u)) {
            const q = buildPromptText("FLOW_SALES_MODEL_COLLECT", { slots: state.slots, callerDigits: state.caller_digits, carriersList: state.carriers_list });
            state.stage = "FLOW_SALES_MODEL_COLLECT";
            state.last_prompt_id = "FLOW_SALES_MODEL_COLLECT";
            state.last_prompt_text = q;
            speakVerbatim(q, "sales_model_collect");
            return;
          }
          if (isNo(u)) {
            state.sales_model_asked = true;
            // proceed
          }
        }

        // Sales model collect
        if (state.stage === "FLOW_SALES_MODEL_COLLECT") {
          if (!state.slots.product_model) state.slots.product_model = u;
        }

        // Sales brand ask
        if (state.stage === "FLOW_SALES_BRAND_ASK") {
          if (isYes(u)) {
            const q = buildPromptText("FLOW_SALES_BRAND_COLLECT", { slots: state.slots, callerDigits: state.caller_digits, carriersList: state.carriers_list });
            state.stage = "FLOW_SALES_BRAND_COLLECT";
            state.last_prompt_id = "FLOW_SALES_BRAND_COLLECT";
            state.last_prompt_text = q;
            speakVerbatim(q, "sales_brand_collect");
            return;
          }
          if (isNo(u)) {
            state.sales_brand_asked = true;
          }
        }

        if (state.stage === "FLOW_SALES_BRAND_COLLECT") {
          if (!state.slots.product_brand) state.slots.product_brand = u;
        }

        // Support issue
        if (state.stage === "FLOW_SUPPORT_ISSUE_DESC") {
          if (!state.slots.issue_desc) state.slots.issue_desc = u;
        }

        // Support model ask
        if (state.stage === "FLOW_SUPPORT_MODEL_ASK") {
          if (isYes(u)) {
            const q = buildPromptText("FLOW_SUPPORT_MODEL_COLLECT", { slots: state.slots, callerDigits: state.caller_digits, carriersList: state.carriers_list });
            state.stage = "FLOW_SUPPORT_MODEL_COLLECT";
            state.last_prompt_id = "FLOW_SUPPORT_MODEL_COLLECT";
            state.last_prompt_text = q;
            speakVerbatim(q, "support_model_collect");
            return;
          }
          if (isNo(u)) {
            // model required → collect anyway
            const q = buildPromptText("FLOW_SUPPORT_MODEL_COLLECT", { slots: state.slots, callerDigits: state.caller_digits, carriersList: state.carriers_list });
            state.stage = "FLOW_SUPPORT_MODEL_COLLECT";
            state.last_prompt_id = "FLOW_SUPPORT_MODEL_COLLECT";
            state.last_prompt_text = q;
            speakVerbatim(q, "support_model_collect_required");
            return;
          }
        }

        if (state.stage === "FLOW_SUPPORT_MODEL_COLLECT") {
          if (!state.slots.product_model) state.slots.product_model = u;
        }

        // Support brand ask
        if (state.stage === "FLOW_SUPPORT_BRAND_ASK") {
          if (isYes(u)) {
            const q = buildPromptText("FLOW_SUPPORT_BRAND_COLLECT", { slots: state.slots, callerDigits: state.caller_digits, carriersList: state.carriers_list });
            state.stage = "FLOW_SUPPORT_BRAND_COLLECT";
            state.last_prompt_id = "FLOW_SUPPORT_BRAND_COLLECT";
            state.last_prompt_text = q;
            speakVerbatim(q, "support_brand_collect");
            return;
          }
          if (isNo(u)) {
            state.support_brand_asked = true;
          }
        }

        if (state.stage === "FLOW_SUPPORT_BRAND_COLLECT") {
          if (!state.slots.product_brand) state.slots.product_brand = u;
        }

        // Delivery desc
        if (state.stage === "FLOW_DELIVERY_DESC") {
          if (!state.slots.delivery_desc) state.slots.delivery_desc = u;
        }

        // Message target
        if (state.stage === "FLOW_MESSAGE_TARGET") {
          if (!state.slots.message_target) state.slots.message_target = u;
          if (state.slots.message_target) {
            state.message_target_confirm_pending = true;
            const conf = buildPromptText("FLOW_MESSAGE_TARGET_CONFIRM", { slots: state.slots, callerDigits: state.caller_digits, carriersList: state.carriers_list });
            state.stage = "FLOW_MESSAGE_TARGET_CONFIRM";
            state.last_prompt_id = "FLOW_MESSAGE_TARGET_CONFIRM";
            state.last_prompt_text = conf;
            speakVerbatim(conf, "message_target_confirm");
            return;
          }
        }

        if (state.stage === "FLOW_MESSAGE_BODY") {
          if (!state.slots.message_body) state.slots.message_body = u;
        }

        // Move forward
        state.awaiting = "idle";
        advanceDialogAfterNLU(state.last_caller_final);
        return;
      }

      // If we finished a DONE speak, send final webhook + hangup
      if (state.done && state.final_event && !sentCallEnded && !state.final_payload_sent) {
        sentCallEnded = true;
        state.final_payload_sent = true;
        endedAt = endedAt || nowIso();

        const payload = applyWebhookDefaults(buildFinalPayload());
        await sendWebhookEvent(state.final_event, payload, { wait_for_recording: true });

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

    // Audio delta → Twilio
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

  // --------------------------------------------------
  // Twilio WS
  // --------------------------------------------------
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

      applyCallerId();

      if (!MB_FINAL_WEBHOOK_ONLY) {
        sendWebhookEvent(
          "call_started",
          applyWebhookDefaults({
            callSid,
            streamSid: twilioStreamSid,
            caller,
            called,
            started_at: startedAt,
            route: state.route,
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

      resetIdleTimers();
      startMaxCallTimer();
      return;
    }

    if (msg.event === "media" && msg.media?.payload) {
      const payload = msg.media.payload;
      if (!openaiReady || !openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
        pendingAudio.push(payload);
        if (pendingAudio.length > 400) pendingAudio.splice(0, pendingAudio.length - 400);
        return;
      }

      // During speak or NLU, buffer caller audio
      if (awaitingResponse) {
        pausedAudioBuffer.push(payload);
        if (pausedAudioBuffer.length > 400) pausedAudioBuffer.splice(0, pausedAudioBuffer.length - 400);
        return;
      }

      safeOpenAISend({ type: "input_audio_buffer.append", audio: payload });
      return;
    }

    if (msg.event === "stop") {
      always(`[TWILIO_STOP][${connTag}]`, "stream stopped");
      endedAt = nowIso();

      // If we already completed, ignore
      if (sentCallEnded || state.final_payload_sent) {
        try {
          if (openaiWs) openaiWs.close();
        } catch (_) {}
        return;
      }

      // If call stopped before DONE → Abandoned
      if (!sentCallAbandoned) {
        sentCallAbandoned = true;
        const payload = applyWebhookDefaults({
          ended_at: endedAt,
          stage: state.stage,
          caller_last_utterance: state.last_user_utterance_for_abandoned || state.last_caller_final || "",
          bot_last_utterance: state.last_bot_final || "",
          transcript: transcriptTurns,
          collected: state.slots,
          recording_url_public: makeRecordingPublicUrl(callSid)
        });
        await sendWebhookEvent("Abandoned", payload, { wait_for_recording: true });
      }

      if (!hangupRequested) {
        hangupRequested = true;
        completeTwilioCall(callSid);
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

    if (!sentCallEnded && !sentCallAbandoned && !state.final_payload_sent) {
      sentCallAbandoned = true;
      endedAt = endedAt || nowIso();
      sendWebhookEvent(
        "Abandoned",
        applyWebhookDefaults({
          ended_at: endedAt,
          stage: state.stage,
          caller_last_utterance: state.last_user_utterance_for_abandoned || state.last_caller_final || "",
          bot_last_utterance: state.last_bot_final || "",
          transcript: transcriptTurns,
          collected: state.slots,
          recording_url_public: makeRecordingPublicUrl(callSid)
        }),
        { wait_for_recording: true }
      );
    }

    if (!hangupRequested && (state.final_payload_sent || sentCallEnded)) {
      hangupRequested = true;
      completeTwilioCall(callSid);
    }

    try {
      if (openaiWs) openaiWs.close();
    } catch (_) {}

    if (idleWarnTimer) clearTimeout(idleWarnTimer);
    if (idleHangTimer) clearTimeout(idleHangTimer);
    if (maxCallTimer) clearTimeout(maxCallTimer);
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
