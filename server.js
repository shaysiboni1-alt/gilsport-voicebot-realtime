// server.js
//
// GilSport Realtime Voice Bot – "נטע" (Neta-based)
// Twilio Media Streams <-> OpenAI Realtime API (gpt-4o-realtime-preview-2024-12-17)
//
// ✅ Single Source of Truth: Google Sheets only
// ✅ One Make webhook for all events: MB_WEBHOOK_URL (payload includes "event")
//
// Requirements:
//   npm install
//   Node 18+
//
// Twilio Voice Webhook ->  POST /twilio-voice  (TwiML)
// Twilio Media Streams -> wss://<domain>/twilio-media-stream
//
require("dotenv").config();
const express = require("express");
const http = require("http");
const WebSocket = require("ws");
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
  if (!/^https?:\/\//i.test(u)) return "";
  return u;
}
function nowIsraelIso() {
  return new Date().toISOString();
}

// -----------------------------
// Core ENV config
// -----------------------------
const PORT = envNumber("PORT", 3000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
if (!OPENAI_API_KEY) console.error("❌ Missing OPENAI_API_KEY in ENV.");

const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

const BOT_NAME_DEFAULT = process.env.MB_BOT_NAME || "נטע";
const BUSINESS_NAME_DEFAULT = process.env.MB_BUSINESS_NAME || "גיל ספורט";

const OPENAI_VOICE_DEFAULT = process.env.OPENAI_VOICE || "alloy";

// VAD defaults (noise hardened)
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

// Barge-in
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 1600);

// Make webhook (single)
const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || "");

// Lead parsing
const MB_ENABLE_SMART_LEAD_PARSING = envBool("MB_ENABLE_SMART_LEAD_PARSING", true);
const MB_LEAD_PARSING_MODEL = process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini";

// Debug
const MB_DEBUG = envBool("MB_DEBUG", false);

// Twilio credentials (optional, only for hangup + resolve caller)
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Sheets (Single Source of Truth)
const GSHEET_ID = (process.env.GSHEET_ID || "").trim();
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = (process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "").trim();

if (!GSHEET_ID) console.error("❌ Missing GSHEET_ID in ENV.");
if (!GOOGLE_SERVICE_ACCOUNT_JSON_B64)
  console.error("❌ Missing GOOGLE_SERVICE_ACCOUNT_JSON_B64 in ENV.");

const SHEETS_REFRESH_MS = envNumber("SHEETS_REFRESH_MS", 60 * 1000);

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

// -----------------------------
// Phone helpers (IL)
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
  if (!local) return null;
  const d = digitsOnly(local);
  if (!d) return null;
  if (d.startsWith("0")) return `+972${d.slice(1)}`;
  if (d.startsWith("972")) return `+${d}`;
  if (d.startsWith("+972")) return d;
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
  const t = String(text || "");
  const digits = t.replace(/\D/g, "");
  if (!digits) return null;

  // Normalize Israel numbers
  const local = toIsraeliLocalFromAny(digits);
  if (!local) return null;

  // Validate basic Israel patterns
  if (!/^0\d{8,9}$/.test(local)) return null;
  return local;
}
function isYes(text) {
  const t = (text || "").trim().toLowerCase();
  return /^(כן|נכון|בדיוק|אכן|כן נכון|נכון מאוד|כן זה נכון)\b/.test(t);
}
function isNo(text) {
  const t = (text || "").trim().toLowerCase();
  return /^(לא|ממש לא|לא נכון|טעות|זה לא|לא זה)\b/.test(t);
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
// Google Sheets loader (service account)
// -----------------------------
function decodeServiceAccountJson() {
  try {
    const raw = Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8");
    const obj = JSON.parse(raw);
    if (!obj.client_email || !obj.private_key) throw new Error("Invalid service account JSON");
    return obj;
  } catch (e) {
    throw new Error("Failed to decode GOOGLE_SERVICE_ACCOUNT_JSON_B64 (invalid base64/json)");
  }
}

async function getSheetsClient() {
  const sa = decodeServiceAccountJson();
  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  });
  await auth.authorize();
  return google.sheets({ version: "v4", auth });
}

async function readSheetRange(sheets, range) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: GSHEET_ID,
    range
  });
  return res.data.values || [];
}

function rowsToObjects(values) {
  if (!Array.isArray(values) || values.length < 2) return [];
  const header = values[0].map((h) => String(h || "").trim());
  const out = [];
  for (let i = 1; i < values.length; i++) {
    const row = values[i] || [];
    const obj = {};
    header.forEach((h, idx) => {
      obj[h] = row[idx] !== undefined ? String(row[idx]) : "";
    });
    // skip fully empty
    const any = Object.values(obj).some((v) => String(v || "").trim() !== "");
    if (any) out.push(obj);
  }
  return out;
}

// -----------------------------
// In-memory Sheet state (Single Source of Truth)
// -----------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {}, // prompt_id -> content_he
  do_not_say: [], // strings
  delivery_contacts: [], // {name, phone_e164, condition_keywords, rule, notes_he}
  importers: [] // {brand_keyword, importer_name, phone_e164, when_to_give, notes_he}
};

let lastSheetsLoadAt = 0;

function getPrompt(id, fallback = "") {
  const v = (SHEETS.prompts[id] || "").trim();
  return v || fallback;
}

function parseCsvKeywords(s) {
  return String(s || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

async function refreshSheets(tag = "SHEETS") {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) return;

  const now = Date.now();
  if (tag !== "startup" && now - lastSheetsLoadAt < SHEETS_REFRESH_MS) return;

  try {
    const sheets = await getSheetsClient();

    // Expected tabs:
    // PROMPTS: prompt_id, content_he
    // DO_NOT_SAY: phrase_he (or any first column)
    // DELIVERY_CONTACTS: name, phone_e164, condition_keywords, rule, notes_he
    // SUPPLIERS_IMPORTERS: brand_keyword, importer_name, phone_e164, when_to_give, notes_he
    const [pRaw, dnsRaw, delRaw, impRaw] = await Promise.all([
      readSheetRange(sheets, "PROMPTS!A:Z"),
      readSheetRange(sheets, "DO_NOT_SAY!A:Z"),
      readSheetRange(sheets, "DELIVERY_CONTACTS!A:Z"),
      readSheetRange(sheets, "SUPPLIERS_IMPORTERS!A:Z")
    ]);

    const promptsRows = rowsToObjects(pRaw);
    const prompts = {};
    for (const r of promptsRows) {
      const id = String(r.prompt_id || "").trim();
      const he = String(r.content_he || "").trim();
      if (id && he) prompts[id] = he;
    }

    const dnsRows = rowsToObjects(dnsRaw);
    const doNotSay = [];
    // accept either phrase_he OR first column
    for (const r of dnsRows) {
      const phrase = String(r.phrase_he || r.phrase || r.text || r[""] || "").trim();
      if (phrase) doNotSay.push(phrase);
      else {
        // fallback: take first non-empty value
        const vals = Object.values(r).map((v) => String(v || "").trim()).filter(Boolean);
        if (vals[0]) doNotSay.push(vals[0]);
      }
    }

    const delRows = rowsToObjects(delRaw).map((r) => ({
      name: String(r.name || "").trim(),
      phone_e164: String(r.phone_e164 || "").trim(),
      condition_keywords: String(r.condition_keywords || "").trim(),
      rule: String(r.rule || "").trim(),
      notes_he: String(r.notes_he || "").trim()
    }));

    const impRows = rowsToObjects(impRaw).map((r) => ({
      brand_keyword: String(r.brand_keyword || "").trim(),
      importer_name: String(r.importer_name || "").trim(),
      phone_e164: String(r.phone_e164 || "").trim(),
      when_to_give: String(r.when_to_give || "").trim(), // fault_or_specific_request
      notes_he: String(r.notes_he || "").trim()
    }));

    SHEETS = {
      loaded_at: new Date().toISOString(),
      prompts,
      do_not_say: doNotSay,
      delivery_contacts: delRows,
      importers: impRows
    };
    lastSheetsLoadAt = Date.now();
    logInfo(tag, `Sheets loaded. prompts=${Object.keys(prompts).length}, do_not_say=${doNotSay.length}, delivery=${delRows.length}, importers=${impRows.length}`);
  } catch (e) {
    logError(tag, "Failed to refresh sheets", e?.message || e);
  }
}

// -----------------------------
// Guardrails (hard rules)
// -----------------------------
function buildHardRules() {
  const coupon = "5555";
  const doNotSay = (SHEETS.do_not_say || []).slice(0, 200);

  // We keep this ALWAYS present even if PROMPTS missing
  return `
כללי חובה (לא עוברים עליהם בשום מצב):
- אתם עוזרת קולית לעסק "גיל ספורט". תענו בעברית כברירת מחדל, לשון רבים, קצר, קליל ומהיר.
- אסור לדבר על: מחירים, מבצעים, זמינות מלאי, זמן אספקה, אחריות ספציפית, עלויות התקנה, השוואות למתחרים.
- אם שואלים על מחיר/מבצע/מלאי/זמינות/זמן אספקה: להגיד בנימוס שהמידע נמצא באתר, ולהציע שנציג מכירות יחזור אליהם. מותר להגיד: "המחירים באתר תמיד מעודכנים, וגיל ספורט מתחייבים למחיר הזול בארץ".
- קוד קופון מותר למסור למתעניינים ברכישה בלבד: ${coupon}.
- לא למסור טלפונים של סניפים. להגיד: "כל הפרטים באתר".
- תמיכה: לא פותרים תקלות בפועל בטלפון — רק אוספים פרטים ומעבירים, ובמקביל אפשר לתת טלפון יבואן כשזה מתאים.
- כל שאלה שלא קשורה לעסק: לענות קצר "אני כאן רק בענייני גיל ספורט" ולהחזיר לשאלה עסקית.
- "Do Not Say" (לא להגיד/לא להיכנס לזה): ${doNotSay.length ? doNotSay.join(" | ") : "(ריק כרגע)"}.
`.trim();
}

// -----------------------------
// Prompt builder from Sheets (PROMPTS tab)
// -----------------------------
function buildSystemInstructions() {
  const master = getPrompt("MASTER_PROMPT", "");
  const guard = getPrompt("GUARDRAILS_PROMPT", "");
  const routing = getPrompt("ROUTING_PROMPT", "");
  const sales = getPrompt("SALES_PROMPT", "");
  const support = getPrompt("SUPPORT_PROMPT", "");
  const delivery = getPrompt("DELIVERY_PROMPT", "");
  const msgmgr = getPrompt("MESSAGE_TO_MANAGER_PROMPT", "");

  // Always add hard rules at the end (highest priority in practice)
  const hard = buildHardRules();

  // If sheet prompts exist, we stitch them; otherwise fallback minimal
  const stitched = [
    master,
    guard,
    routing,
    sales,
    support,
    delivery,
    msgmgr
  ].map((x) => String(x || "").trim()).filter(Boolean).join("\n\n");

  const fallback = `
אתם עוזרת קולית בזמן אמת בשם "${BOT_NAME_DEFAULT}" עבור העסק "${BUSINESS_NAME_DEFAULT}".
דברו קצר, חם, קליל ומהיר. עברית כברירת מחדל, לשון רבים.
תפקידכם: לזהות כוונה (מכירה/שירות-תקלה/אספקה-משלוח/הודעה למנהל) ולקחת פרטים.
`.trim();

  return (stitched || fallback) + "\n\n" + hard;
}

function getOpeningScript() {
  // Opening is controlled from Sheets: we expect a prompt_id like OPENING_SCRIPT
  // If not present, fallback:
  return getPrompt(
    "OPENING_SCRIPT",
    'גיל ספורט שלום, מדברת נטע. במה אפשר לעזור לכם?'
  );
}

function getClosingScript() {
  return getPrompt(
    "CLOSING_SCRIPT",
    "תודה שפניתם לגיל ספורט. יום נעים ולהתראות."
  );
}

function getEffectiveOpenAiVoice() {
  return getPrompt("OPENAI_VOICE", OPENAI_VOICE_DEFAULT).trim() || OPENAI_VOICE_DEFAULT;
}

// -----------------------------
// Webhook (single Make endpoint) with event routing
// -----------------------------
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

async function sendMakeEvent(event, payload) {
  if (!MB_WEBHOOK_URL) return;
  try {
    await fetchWithTimeout(
      MB_WEBHOOK_URL,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ event, ...payload })
      },
      4500
    ).catch(() => {});
  } catch (_) {}
}

// -----------------------------
// Smart lead parsing (like Neta)
// -----------------------------
async function extractLeadFromConversation(conversationLog, botName, businessName) {
  const tag = "LeadParse";
  if (!MB_ENABLE_SMART_LEAD_PARSING) return null;
  if (!OPENAI_API_KEY) return null;
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;

  try {
    const conversationText = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const systemPrompt = `
אתה מנתח שיחות טלפון בעברית (ולעתים גם בשפות אחרות) בין לקוח לבין בוט שירות.
תפקידך להוציא JSON אחד בלבד שתואם בדיוק לסכמה הבאה:

{
  "is_lead": boolean,
  "lead_type": "new" | "existing" | "unknown",
  "full_name": string | null,
  "business_name": string | null,
  "phone_number": string | null,
  "reason": string | null,
  "notes": string | null
}

החזר אך ורק JSON תקין לפי הסכמה, בלי טקסט נוסף, בלי הסברים ובלי הערות.
`.trim();

    const userPrompt = `
להלן תמלול שיחה בין לקוח ובוט שירות בשם "${botName}" עבור העסק "${businessName}".

תמלול:
${conversationText}
`.trim();

    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify({
        model: MB_LEAD_PARSING_MODEL,
        response_format: { type: "json_object" },
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userPrompt }
        ]
      })
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      logError(tag, `OpenAI lead parsing HTTP ${response.status}`, text);
      return null;
    }

    const data = await response.json();
    const raw = data.choices?.[0]?.message?.content;
    if (!raw) return null;

    let parsed = null;
    try {
      parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
    } catch {
      parsed = raw;
    }

    if (typeof parsed !== "object" || parsed === null) return null;
    return parsed;
  } catch (err) {
    logError(tag, "Error in extractLeadFromConversation", err);
    return null;
  }
}

// -----------------------------
// Twilio helpers (optional)
// -----------------------------
async function hangupTwilioCall(callSid, tag = "Call") {
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
        "Content-Type": "application/x-www-form-urlencoded"
      },
      body
    });

    if (!res.ok) {
      const txt = await res.text().catch(() => "");
      logError(tag, `Twilio hangup HTTP ${res.status}`, txt);
    }
  } catch (err) {
    logError(tag, "Error calling Twilio hangup API", err);
  }
}

async function fetchCallerNumberFromTwilio(callSid, tag = "Call") {
  if (!callSid) return null;
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return null;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`;
    const res = await fetch(url, {
      method: "GET",
      headers: {
        Authorization:
          "Basic " + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64")
      }
    });
    if (!res.ok) return null;
    const data = await res.json();
    return data.from || null;
  } catch {
    return null;
  }
}

// -----------------------------
// Express & HTTP
// -----------------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/health", (_req, res) => {
  res.status(200).json({
    ok: true,
    ts: Date.now(),
    sheets_loaded_at: SHEETS.loaded_at,
    prompts: Object.keys(SHEETS.prompts || {}).length
  });
});

// Manual reload
app.post("/sheets/reload", async (_req, res) => {
  try {
    lastSheetsLoadAt = 0;
    await refreshSheets("manual");
    res.status(200).json({ ok: true, reloaded: true, loaded_at: SHEETS.loaded_at });
  } catch (e) {
    res.status(500).json({ ok: false, error: "reload_failed" });
  }
});

// Twilio Voice webhook (TwiML)
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

  logInfo("Twilio-Voice", `Returning TwiML with Stream URL: ${wsUrl}, From=${caller}, To=${called}`);
  res.type("text/xml").send(twiml);
});

const server = http.createServer(app);

// -----------------------------
// WebSocket server for Twilio Media Streams
// -----------------------------
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// -----------------------------
// Per-call handler
// -----------------------------
wss.on("connection", (connection, _req) => {
  const tag = "Call";
  logInfo(tag, "New Twilio Media Stream connection established.");

  if (!OPENAI_API_KEY) {
    logError(tag, "OPENAI_API_KEY missing – closing connection.");
    connection.close();
    return;
  }

  // best-effort refresh sheets on connect (non-blocking)
  refreshSheets("on_connect").catch(() => {});

  let streamSid = null;
  let callSid = null;
  let callerNumber = null;
  let calledNumber = null;
  let callDirection = null;

  // deterministic slot capture (phone + confirm)
  let awaitingPhone = false;
  let awaitingPhoneConfirm = false;
  let collectedPhoneIL = null;

  let awaitingName = false;
  let collectedName = null;

  // internal flags for delivery/importer logic
  let userRequestedImporterSpecifically = false;

  const botName = getPrompt("BOT_NAME", BOT_NAME_DEFAULT) || BOT_NAME_DEFAULT;
  const businessName = getPrompt("BUSINESS_NAME", BUSINESS_NAME_DEFAULT) || BUSINESS_NAME_DEFAULT;

  const instructions = buildSystemInstructions();

  const openAiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1"
      }
    }
  );

  let conversationLog = [];
  let currentBotText = "";
  let callStartTs = Date.now();
  let lastMediaTs = Date.now();
  let idleCheckInterval = null;
  let idleWarningSent = false;
  let idleHangupScheduled = false;
  let maxCallTimeout = null;
  let maxCallWarningTimeout = null;
  let pendingHangup = null;
  let openAiReady = false;
  let twilioClosed = false;
  let openAiClosed = false;
  let callEnded = false;

  let botSpeaking = false;
  let hasActiveResponse = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  let makeCallEndSent = false;
  let makeLeadSent = false;

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
    if (openAiWs.readyState !== WebSocket.OPEN) return;
    if (hasActiveResponse) return;

    openAiWs.send(
      JSON.stringify({
        type: "conversation.item.create",
        item: {
          type: "message",
          role: "user",
          content: [{ type: "input_text", text }]
        }
      })
    );
    openAiWs.send(JSON.stringify({ type: "response.create" }));
    hasActiveResponse = true;
    botTurnActive = true;
    logInfo(tag, `Sending model prompt (${purpose || "no-tag"})`);
  }

  function mapCallStatus(reason) {
    const r = String(reason || "").toLowerCase();
    if (r.includes("error")) return "error";
    if (r.includes("twilio") || r.includes("ws_closed") || r.includes("stop")) return "abandoned";
    return "completed";
  }

  function checkBotClosing(botText) {
    const closingScript = getClosingScript();
    const normalizedClosing = normalizeForClosing(closingScript);
    if (!botText || !normalizedClosing) return;

    const norm = normalizeForClosing(botText);
    if (!norm) return;

    if (norm.includes(normalizedClosing) || normalizedClosing.includes(norm)) {
      logInfo(tag, `Detected configured bot closing phrase in output: "${botText}"`);
      scheduleHangupAfterBotClosing("bot_closing_config");
    }
  }

  function scheduleHangupAfterBotClosing(reason) {
    if (callEnded) return;
    if (pendingHangup) return;

    const msg = getClosingScript();
    const ph = { reason, closingMessage: msg };
    scheduleForceEndAfterGrace(ph, "bot_closing_detected");
    logInfo(tag, `Bot closing detected – hangup scheduled AFTER GRACE reason="${reason}".`);
  }

  function scheduleEndCall(reason, closingMessage) {
    if (callEnded) return;

    const msg = closingMessage || getClosingScript();
    if (pendingHangup) return;

    pendingHangup = { reason, closingMessage: msg };

    if (openAiWs.readyState === WebSocket.OPEN) {
      sendModelPrompt(`סיימי את השיחה עם הלקוח במשפט הבא בלבד, בלי להוסיף שום משפט נוסף: "${msg}"`, "closing");
    } else {
      const ph = pendingHangup;
      pendingHangup = null;
      scheduleForceEndAfterGrace(ph, "no_openai");
      return;
    }

    const graceMs = getGraceMs();
    setTimeout(() => {
      if (callEnded) return;
      if (!pendingHangup) return;
      const ph = pendingHangup;
      pendingHangup = null;
      scheduleForceEndAfterGrace(ph, "closing_fallback");
    }, graceMs + 6000);
  }

  function updateDialogueStateFromBotText(botText) {
    const t = normalizeForClosing(botText);

    if (/מספר טלפון|טלפון לחזרה/.test(t)) {
      awaitingPhone = true;
      awaitingPhoneConfirm = false;
      logDebug(tag, "State: awaitingPhone=true");
      return;
    }

    if (/איך אפשר לרשום את השם|מה השם|שם מלא|שם פרטי/.test(t)) {
      awaitingName = true;
      logDebug(tag, "State: awaitingName=true");
      return;
    }
  }

  function handleDeterministicPhoneFlowOnUserTranscript(userText) {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;

    const phoneIL = detectPhoneCandidateFromText(userText);
    if (!phoneIL) return;

    if (awaitingPhoneConfirm) {
      if (isYes(userText)) {
        collectedPhoneIL = collectedPhoneIL || phoneIL;
        awaitingPhoneConfirm = false;
        awaitingPhone = false;

        const sayPhone = formatIsraeliPhoneForTts(collectedPhoneIL);
        safeCancelResponseIfNeeded();
        sendModelPrompt(
          `הלקוח אישר שמספר הטלפון שלו הוא "${sayPhone}". תודה קצרה ואז המשיכי לשלב הבא בצורה טבעית (למשל: בקשת שם אם חסר, או סיכום שיחזרו אליו). אל תשני את המספר ואל תחזרי לבקש אותו שוב.`,
          "phone_confirmed"
        );
        return;
      }
      if (isNo(userText)) {
        collectedPhoneIL = null;
        awaitingPhoneConfirm = false;
        awaitingPhone = true;

        safeCancelResponseIfNeeded();
        sendModelPrompt(
          `הלקוח אמר שהמספר לא נכון. בקשי שוב מספר טלפון לחזרה, ובקשי שיאמר אותו לאט ספרה-ספרה. תשובה קצרה.`,
          "phone_retake"
        );
        return;
      }
      return;
    }

    if (awaitingPhone) {
      collectedPhoneIL = phoneIL;
      awaitingPhone = false;
      awaitingPhoneConfirm = true;

      const sayPhone = formatIsraeliPhoneForTts(collectedPhoneIL);
      safeCancelResponseIfNeeded();
      sendModelPrompt(
        `הלקוח מסר מספר טלפון. המספר שנקלט (חובה לדייק ללא שינוי) הוא: "${sayPhone}". חזרי עליו בדיוק ושאלי: "זה נכון?" בלי להוסיף שום מספר אחר ובלי לשנות ספרות.`,
        "phone_echo_confirm"
      );
      return;
    }

    if (!collectedPhoneIL) {
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

  // -----------------------------
  // Importer + Delivery numbers helper (feed hints into model)
  // -----------------------------
  function detectBrandImporter(text) {
    const t = normalizeForClosing(text);
    const importers = Array.isArray(SHEETS.importers) ? SHEETS.importers : [];
    if (!t || importers.length === 0) return null;

    // detect if user explicitly asks for importer
    if (/יבואן|שירות יבואן|טלפון של היבואן/.test(t)) userRequestedImporterSpecifically = true;

    for (const imp of importers) {
      const key = String(imp.brand_keyword || "").trim().toLowerCase();
      if (!key) continue;
      if (t.includes(key.toLowerCase())) return imp;
    }
    return null;
  }

  function shouldOfferImporter(imp, routeGuess, userText) {
    if (!imp) return false;
    const when = String(imp.when_to_give || "").trim();
    if (when !== "fault_or_specific_request") return false;

    // Only if route is support/fault OR user specifically asked for importer
    const t = normalizeForClosing(userText);
    const mentionsFault = /תקלה|בעיה|לא עובד|שבור|אחריות|שירות/.test(t);
    const supportLike = routeGuess === "support" || mentionsFault;
    return supportLike || userRequestedImporterSpecifically;
  }

  function detectDeliveryAfterHoursSameDay(userText) {
    // We follow your rule: only if AFTER HOURS + SAME DAY intent.
    // Since business hours are not yet in sheet, we do keyword-based only.
    // (You can later add hours in PROMPTS/SETTINGS and we can harden it.)
    const t = normalizeForClosing(userText);
    const mentionsDelivery = /משלוח|אספקה|מוביל|הזמנה|הגיע|לא הגיע/.test(t);
    const mentionsSameDay = /היום|להיום|עוד היום|מהיום|עד היום|הערב|הלילה|עוד הערב/.test(t);
    const mentionsAfterHours = /אחרי שעות|סגור|אין מענה|לא עונים/.test(t);

    return mentionsDelivery && mentionsSameDay && (mentionsAfterHours || true); // keyword-based for now
  }

  function getDeliveryContacts() {
    return Array.isArray(SHEETS.delivery_contacts) ? SHEETS.delivery_contacts : [];
  }

  // -----------------------------
  // End call (fire-and-forget Make events)
  // -----------------------------
  async function endCall(reason, closingMessage) {
    if (callEnded) return;
    callEnded = true;

    if (graceHangupTimer) clearTimeout(graceHangupTimer);

    if (idleCheckInterval) clearInterval(idleCheckInterval);
    if (maxCallTimeout) clearTimeout(maxCallTimeout);
    if (maxCallWarningTimeout) clearTimeout(maxCallWarningTimeout);

    const effectiveClosing = closingMessage || getClosingScript();

    // Hangup ASAP
    if (callSid) hangupTwilioCall(callSid, tag).catch(() => {});

    // Close sockets ASAP
    if (!openAiClosed && openAiWs.readyState === WebSocket.OPEN) {
      openAiClosed = true;
      openAiWs.close();
    }
    if (!twilioClosed && connection.readyState === WebSocket.OPEN) {
      twilioClosed = true;
      connection.close();
    }

    // Post-call tasks (non-blocking)
    (async () => {
      try {
        // Ensure caller number if missing
        if (!callerNumber && callSid) {
          const resolved = await fetchCallerNumberFromTwilio(callSid, tag);
          if (resolved) callerNumber = resolved;
        }

        const callerRaw = callerNumber ? String(callerNumber) : null;
        const callerIL = toIsraeliLocalFromAny(callerRaw) || null;
        const callerE164 =
          toE164FromIsraeliLocal(callerIL) || (callerRaw && callerRaw.startsWith("+") ? callerRaw : null);

        // Parse lead once
        let parsedLead = await extractLeadFromConversation(conversationLog, botName, businessName);
        if (parsedLead && typeof parsedLead === "object") {
          // merge deterministic captures
          if (!parsedLead.phone_number && collectedPhoneIL) parsedLead.phone_number = collectedPhoneIL;
          if (!parsedLead.full_name && collectedName) parsedLead.full_name = collectedName;

          // normalize phone
          const normalized = toIsraeliLocalFromAny(parsedLead.phone_number) || toIsraeliLocalFromAny(callerRaw);
          parsedLead.phone_number = normalized || null;
        }

        const endedAt = nowIsraelIso();
        const startedAt = new Date(callStartTs).toISOString();
        const durationSec = Math.max(0, Math.round((Date.now() - callStartTs) / 1000));

        const lastUser = [...conversationLog].reverse().find((m) => m.from === "user")?.text || "";
        const transcript = conversationLog
          .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
          .join("\n");

        const callPayload = {
          call_id: callSid || streamSid || `call_${Date.now()}`,
          streamSid,
          callSid,
          call_direction: callDirection || "inbound",
          started_at: startedAt,
          ended_at: endedAt,
          duration_sec: durationSec,
          caller_id: callerE164 || callerIL || callerRaw || null,
          caller_raw: callerRaw,
          caller_il: callerIL,
          caller_e164: callerE164,
          collected_phone: collectedPhoneIL ? toE164FromIsraeliLocal(collectedPhoneIL) : null,
          contact_name: (parsedLead && parsedLead.full_name) || collectedName || null,
          call_status: mapCallStatus(reason),
          last_user_utterance: lastUser || null,
          reason: reason || null,
          closingMessage: effectiveClosing,
          summary: parsedLead?.reason || null,
          lead_notes: parsedLead?.notes || null,
          has_lead: parsedLead?.is_lead === true
        };

        // Send single webhook event: call_end
        if (!makeCallEndSent) {
          makeCallEndSent = true;
          await sendMakeEvent("call_end", {
            ...callPayload,
            transcript,
            parsedLead: parsedLead || null
          });
        }

        // Send lead event only if full lead (lead+phone)
        const finalPhoneLocal = parsedLead?.phone_number || collectedPhoneIL || callerIL || null;
        const finalPhoneE164 = finalPhoneLocal ? toE164FromIsraeliLocal(finalPhoneLocal) : (callerE164 || null);

        const isFullLead = parsedLead?.is_lead === true && !!finalPhoneE164;
        if (isFullLead && !makeLeadSent) {
          makeLeadSent = true;
          await sendMakeEvent("lead", {
            ...callPayload,
            phone_number: finalPhoneE164,
            parsedLead: { ...(parsedLead || {}), phone_number: finalPhoneLocal },
            transcript
          });
        }
      } catch (e) {
        logError(tag, "Post-call Make events error", e?.message || e);
      }
    })().catch(() => {});
  }

  // -----------------------------
  // OpenAI WS handlers
  // -----------------------------
  openAiWs.on("open", () => {
    openAiReady = true;
    logInfo(tag, "Connected to OpenAI Realtime API.");

    const effectiveSilenceMs = MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS;

    const sessionUpdate = {
      type: "session.update",
      session: {
        model: OPENAI_REALTIME_MODEL,
        modalities: ["audio", "text"],
        voice: getEffectiveOpenAiVoice(),
        input_audio_format: "g711_ulaw",
        output_audio_format: "g711_ulaw",
        input_audio_transcription: { model: "whisper-1" },
        turn_detection: {
          type: "server_vad",
          threshold: MB_VAD_THRESHOLD,
          silence_duration_ms: effectiveSilenceMs,
          prefix_padding_ms: MB_VAD_PREFIX_MS
        },
        max_response_output_tokens: "inf",
        instructions
      }
    };

    logDebug(tag, "Sending session.update to OpenAI.", sessionUpdate);
    openAiWs.send(JSON.stringify(sessionUpdate));

    const greetingText = getOpeningScript();
    sendModelPrompt(
      `פתחי את השיחה עם הלקוח במשפט הבא (אפשר לשנות מעט את הניסוח אבל לא להאריך): "${greetingText}" ואז עצרי והמתיני לתשובה שלו.`,
      "opening_greeting"
    );
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
          logInfo("Bot", text);

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
          connection.send(JSON.stringify({ event: "media", streamSid, media: { payload: b64 } }));
        }
        break;
      }

      case "response.audio.done": {
        botSpeaking = false;
        botTurnActive = false;
        break;
      }

      case "response.completed": {
        botSpeaking = false;
        hasActiveResponse = false;
        botTurnActive = false;
        break;
      }

      case "conversation.item.input_audio_transcription.completed": {
        const transcriptRaw = msg.transcript || "";
        let t = transcriptRaw.trim();
        if (t) {
          t = t.replace(/\s+/g, " ").replace(/\s+([,.:;!?])/g, "$1");
          conversationLog.push({ from: "user", text: t });
          logInfo("User", t);

          // deterministic phone capture + exact confirm
          handleDeterministicPhoneFlowOnUserTranscript(t);

          // best-effort name capture
          if (awaitingName && !collectedName) {
            const possiblePhone = detectPhoneCandidateFromText(t);
            if (!possiblePhone) {
              const words = t.split(" ").filter(Boolean).slice(0, 3);
              const name = words.join(" ").trim();
              if (name && name.length <= 40) collectedName = name;
            }
            awaitingName = false;
          }

          // Importer optional hint (support/fault or explicit request)
          const imp = detectBrandImporter(t);
          const routeGuess = /תקלה|בעיה|אחריות|שירות/.test(normalizeForClosing(t)) ? "support" : "unknown";
          if (shouldOfferImporter(imp, routeGuess, t) && imp.phone_e164) {
            safeCancelResponseIfNeeded();
            sendModelPrompt(
              `במקביל לאיסוף פרטים לשירות, אפשר גם למסור ללקוח שזה אופציונלי לפנות ליבואן. אם הלקוח רוצה: טלפון היבואן עבור "${imp.importer_name || imp.brand_keyword}" הוא "${imp.phone_e164}". חשוב: תגידי שזה "אופציונלי במקביל" ושאתם גם תחזרו אליו.`,
              "importer_optional"
            );
            break;
          }

          // Delivery after-hours same-day numbers (optional)
          if (detectDeliveryAfterHoursSameDay(t)) {
            const contacts = getDeliveryContacts().filter((c) => String(c.rule || "").trim() === "after_hours_same_day_only");
            if (contacts.length) {
              const lines = contacts
                .filter((c) => c.phone_e164)
                .map((c) => `${c.name}: ${c.phone_e164}`)
                .slice(0, 5)
                .join(" | ");
              safeCancelResponseIfNeeded();
              sendModelPrompt(
                `אם הלקוח ממתין לאספקה להיום והשיחה אחרי שעות הפעילות: בנוסף ללקיחת הודעה, אפשר למסור אופציונלית את מספרי המובילים: ${lines}. שמרי על תשובה קצרה.`,
                "delivery_after_hours_contacts"
              );
            }
          }
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
    logInfo(tag, "OpenAI WS closed.");
    if (!callEnded) endCall("openai_ws_closed", getClosingScript());
  });

  openAiWs.on("error", (err) => {
    logError(tag, "OpenAI WS error", err);
    if (!openAiClosed) {
      openAiClosed = true;
      try { openAiWs.close(); } catch {}
    }
    if (!callEnded) endCall("openai_ws_error", getClosingScript());
  });

  // -----------------------------
  // Twilio Media Stream handlers
  // -----------------------------
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
      callerNumber =
        cp.caller ||
        cp.From ||
        cp.from ||
        msg.start?.caller ||
        msg.start?.from ||
        null;

      calledNumber = cp.called || cp.To || cp.to || msg.start?.to || null;
      callDirection = cp.direction || msg.start?.direction || "inbound";

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logInfo(tag, `Twilio stream started. streamSid=${streamSid}, callSid=${callSid}, caller=${callerNumber}`);

      // Backfill caller from Twilio API (non-blocking)
      if (!callerNumber && callSid) {
        fetchCallerNumberFromTwilio(callSid, tag)
          .then((resolved) => {
            if (resolved && !callerNumber) {
              callerNumber = resolved;
              logInfo(tag, `Caller backfilled from Twilio API: ${callerNumber}`);
            }
          })
          .catch(() => {});
      }

      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          sendIdleWarningIfNeeded();
        }
        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          logInfo(tag, "Idle timeout reached, scheduling endCall.");
          scheduleEndCall("idle_timeout", getClosingScript());
        }
      }, 1000);

      if (MB_MAX_CALL_MS > 0) {
        if (MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
          maxCallWarningTimeout = setTimeout(() => {
            const t =
              "אנחנו מתקרבים לסיום הזמן לשיחה הזאת. אם תרצו להתקדם, אפשר עכשיו לסכם ולהשאיר פרטים.";
            sendModelPrompt(`תני ללקוח משפט קצר בסגנון הבא (אפשר לשנות קצת): "${t}"`, "max_call_warning");
          }, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
        }

        maxCallTimeout = setTimeout(() => {
          logInfo(tag, "Max call duration reached, scheduling endCall.");
          scheduleEndCall("max_call_duration", getClosingScript());
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
          logDebug("BargeIn", "Ignoring media (MB_ALLOW_BARGE_IN=false)", {
            botTurnActive,
            botSpeaking,
            now,
            noListenUntilTs
          });
          return;
        }
      }

      openAiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));
    } else if (event === "stop") {
      logInfo(tag, "Twilio stream stopped.");
      twilioClosed = true;
      if (!callEnded) endCall("twilio_stop", getClosingScript());
    }
  });

  connection.on("close", () => {
    twilioClosed = true;
    logInfo(tag, "Twilio WS closed.");
    if (!callEnded) endCall("twilio_ws_closed", getClosingScript());
  });

  connection.on("error", (err) => {
    twilioClosed = true;
    logError(tag, "Twilio WS error", err);
    if (!callEnded) endCall("twilio_ws_error", getClosingScript());
  });
});

// -----------------------------
// Start server
// -----------------------------
server.listen(PORT, () => {
  console.log(`✅ GilSport Realtime VoiceBot running on port ${PORT}`);
  refreshSheets("startup").catch(() => {});
  setInterval(() => refreshSheets("interval").catch(() => {}), Math.max(15000, SHEETS_REFRESH_MS));
});
