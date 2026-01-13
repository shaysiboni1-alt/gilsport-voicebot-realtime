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
  deliveryContacts: [] // DELIVERY_CONTACTS rows
  ,routingRules: [] // (legacy/compat)
  ,businessInfo: [] // (legacy/compat)
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
    const json = JSON.parse(
      Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8")
    );

    const auth = new google.auth.JWT({
      email: json.client_email,
      key: json.private_key,
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    });

    const sheets = google.sheets({ version: "v4", auth });

    // ✅ load PROMPTS + SETTINGS in one call
    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: GSHEET_ID,
      ranges: ["PROMPTS!A:Z", "SETTINGS!A:Z", "KB_FACTS!A:Z", "DO_NOT_SAY!A:Z", "SUPPLIERS_IMPORTERS!A:Z", "DELIVERY_CONTACTS!A:Z"]
    });

    const valueRanges = res.data.valueRanges || [];
    const promptsRange = valueRanges.find((vr) => (vr.range || "").startsWith("PROMPTS!"));
    const settingsRange = valueRanges.find((vr) => (vr.range || "").startsWith("SETTINGS!"));

    const promptsRows = (promptsRange?.values || []).slice();
    const settingsRows = (settingsRange?.values || []).slice();

const kbFactsRange = valueRanges.find((vr) => (vr.range || "").startsWith("KB_FACTS!"));
const doNotSayRange = valueRanges.find((vr) => (vr.range || "").startsWith("DO_NOT_SAY!"));
const suppliersImportersRange = valueRanges.find((vr) => (vr.range || "").startsWith("SUPPLIERS_IMPORTERS!"));
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
  deliveryContacts: deliveryContactsRows
  ,routingRules: []
  ,businessInfo: []
};


    log(`Sheets loaded (prompts=${Object.keys(prompts).length}, settings=${Object.keys(settings).length}, kbFacts=${kbFactsRows.length}, doNotSay=${doNotSayRows.length}, suppliersImporters=${suppliersImportersRows.length}, deliveryContacts=${deliveryContactsRows.length})`);
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
// Access: ${PUBLIC_BASE_URL}/recording/:callSid   (PUBLIC_BASE_URL should be this server public base)
app.get("/recording/:callSid", async (req, res) => {
  try {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return res.status(404).send("recording proxy disabled");
    const callSid = String(req.params.callSid || "").trim();
    if (!callSid) return res.status(400).send("missing callSid");

    // Fetch latest recording for this call
    const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings.json?CallSid=${encodeURIComponent(
      callSid
    )}&PageSize=1`;

    const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
    const listResp = await fetch(listUrl, { headers: { Authorization: `Basic ${auth}` } });
    if (!listResp.ok) return res.status(404).send("no recording");
    const listJson = await listResp.json();
    const rec = (listJson.recordings || [])[0];
    if (!rec || !rec.sid) return res.status(404).send("no recording");

    // Twilio media (mp3)
    const mediaUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${rec.sid}.mp3`;
    const mediaResp = await fetch(mediaUrl, { headers: { Authorization: `Basic ${auth}` } });
    if (!mediaResp.ok) return res.status(404).send("recording not ready");

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

  // Stream parameters (set early to avoid TDZ issues)
  let caller = "";
  let called = "";

  // Try to read Stream <Parameter> values from querystring if present
  try {
    const u = new URL(req.url || "", "http://localhost");
    caller = u.searchParams.get("caller") || "";
    called = u.searchParams.get("called") || "";
  } catch (_) {}


  let lastCallerFinal = "";
  let lastBotFinal = "";


// Call/session state for webhook + routing + abandoned
let callSid = null;
let startedAt = nowIso();
let endedAt = null;
let route = "other";
let language = getSetting("DEFAULT_LANGUAGE", "he") || "he";

let transcriptTurns = []; // {from, text, at}

// Abandoned / ended dedupe guards
let sentCallEnded = false;
let sentCallAbandoned = false;

// Proxy decision: dynamic response instructions (no FSM)
let proxyInstructions = "";

const pushTurn = (from, text) => {
  const t = String(text || "").trim();
  if (!t) return;
  transcriptTurns.push({ from, text: t, at: nowIso() });
  // cap
  if (transcriptTurns.length > 400) transcriptTurns = transcriptTurns.slice(-400);
};

const extractPhoneCandidates = (text) => {
  const t = String(text || "");
  const digits = t.replace(/\D+/g, "");
  // Israeli 9-10 digits typical
  if (digits.length === 9 || digits.length === 10) return digits;
  if (digits.length > 10) return digits.slice(-10);
  return "";
};

const getBizPhonesByHint = (hint) => {
  const rows = Array.isArray(SHEETS.businessInfo) ? SHEETS.businessInfo : [];
  const out = [];
  const h = String(hint || "").toLowerCase();
  for (const r of rows) {
    const values = Object.values(r || {}).map((v) => String(v || ""));
    const joined = values.join(" ").toLowerCase();
    if (h && !joined.includes(h)) continue;
    // try find any phone-like field
    for (const [k, v] of Object.entries(r || {})) {
      const key = String(k || "").toLowerCase();
      const val = String(v || "").trim();
      if (!val) continue;
      if (key.includes("phone") || key.includes("טלפון") || key.includes("מספר")) {
        out.push(val);
      }
    }
  }
  return Array.from(new Set(out)).slice(0, 10);
};

const normalizeKeywords = (s) =>
  String(s || "")
    .split(/[,;\n\r\t]+/)
    .map((x) => x.trim())
    .filter(Boolean);

const pickRouteFromRules = (text) => {
  const t = String(text || "").toLowerCase();
  const rules = Array.isArray(SHEETS.routingRules) ? SHEETS.routingRules : [];
  const scored = [];
  for (const r of rules) {
    const routeVal = (r.route || r.intent || r.Route || r.ROUTE || "").toString().trim();
    const pr = Number(r.priority || r.Priority || r.PRIORITY || 999);
    // find any keyword field
    let kws = [];
    for (const [k, v] of Object.entries(r || {})) {
      const key = String(k || "").toLowerCase();
      if (key.includes("keyword") || key.includes("keywords") || key.includes("מילות")) {
        kws = kws.concat(normalizeKeywords(v));
      }
    }
    kws = kws.map((x) => x.toLowerCase());
    if (!routeVal || !kws.length) continue;
    const hit = kws.some((kw) => kw && t.includes(kw));
    if (hit) scored.push({ route: routeVal, priority: Number.isFinite(pr) ? pr : 999 });
  }
  scored.sort((a, b) => a.priority - b.priority);
  return scored[0]?.route || "";
};

const parseHours = (s) => {
  // expects like "09:00-18:00" or "09:00–18:00"
  const m = String(s || "").match(/(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})/);
  if (!m) return null;
  const aH = Number(m[1]), aM = Number(m[2]), bH = Number(m[3]), bM = Number(m[4]);
  if (![aH, aM, bH, bM].every((x) => Number.isFinite(x))) return null;
  return { start: aH * 60 + aM, end: bH * 60 + bM };
};

const isAfterHours = () => {
  const hoursStr =
    getSetting("BUSINESS_HOURS", "") ||
    getSetting("HOURS", "") ||
    getSetting("WORKING_HOURS", "") ||
    "";
  const parsed = parseHours(hoursStr);
  if (!parsed) return false; // if unknown, do not force after-hours
  // Use local time in TIME_ZONE
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


const buildProxyInstructions = (callerText) => {
  const t = String(callerText || "").trim();
  if (!t) return "";

  const low = t.toLowerCase();

  // Route heuristics (no FSM)
  if (/(אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול)/.test(low)) route = "support";
  else if (/(משלוח|אספקה|שליח|הזמנה|הגיע|לא הגיע|מוביל)/.test(low)) route = "delivery";
  else if (/(מחיר|לקנות|רכישה|מוצר|דגם|מידה|צבע|מלאי|כמה עולה|מבצע)/.test(low)) route = "sales";
  else route = route || "other";

  // DO_NOT_SAY: rows in DO_NOT_SAY tab (enforce via instruction)
  const dnsRows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
  const doNotSayText = dnsRows
    .map((r) => {
      const a = String(r.forbidden_topic || "").trim();
      const b = String(r.trigger_examples || "").trim();
      const c = String(r.safe_response_he || "").trim();
      const parts = [a && `נושא: ${a}`, b && `טריגרים: ${b}`, c && `תגובה בטוחה: ${c}`].filter(Boolean);
      return parts.join(" | ");
    })
    .filter(Boolean)
    .slice(0, 20)
	    .join("\n");

  const mustNotLieDelivery =
    "אין לך גישה לסטטוס משלוח אמיתי. אסור להגיד 'בדקתי סטטוס' או להבטיח שראית מערכת משלוחים.";

  // DELIVERY_CONTACTS: provide carrier contacts when needed
  const deliveryRows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
  const carrierPhones = deliveryRows
    .filter((r) => {
      const ck = String(r.condition_keywords || "").toLowerCase();
	      return !ck || ck.split(/[,;\n\r\t]+/).some((kw) => kw.trim() && low.includes(kw.trim()));
    })
    .map((r) => String(r.phone_e164 || r.phone || "").trim())
    .filter(Boolean)
    .slice(0, 10);

  // KB_FACTS: soft facts injection (only a few)
  const factsRows = Array.isArray(SHEETS.kbFacts) ? SHEETS.kbFacts : [];
  const matchFacts = [];
  for (const r of factsRows) {
    const kw = String(r.keywords || "").toLowerCase();
    if (!kw) continue;
	    const kws = kw.split(/[,;\n\r\t]+/).map((x) => x.trim()).filter(Boolean);
    if (!kws.length) continue;
    if (kws.some((k) => k && low.includes(k))) {
      const ans = String(r.answer_he || "").trim();
      if (ans) matchFacts.push(`• ${ans}`);
    }
    if (matchFacts.length >= 5) break;
  }

  // After-hours check (uses SETTINGS hours if present; if unknown -> false)
  const afterHours = route === "delivery" && isAfterHours();

  const baseStyle =
    "סגנון: נטע. תשובות קצרות, ענייניות, אנושיות. משפט-שניים ואז שאלה מקדמת. לא לחפור, לא לחזור על עצמך.";

	  const parts = [];
	  parts.push(baseStyle);
	  parts.push("תעדיפי מידע מהשיטס (KB_FACTS/DELIVERY_CONTACTS/DO_NOT_SAY/SUPPLIERS_IMPORTERS) על פני המצאות.");

	  if (doNotSayText) {
	    parts.push("DO_NOT_SAY (כללים מחייבים):\n" + doNotSayText);
	  }

	  if (matchFacts.length) {
	    parts.push("עובדות רלוונטיות מהשיטס (להשתמש רק אם מתאים לשאלה):\n" + matchFacts.join("\n"));
	  }

	  if (route === "delivery") {
	    parts.push(mustNotLieDelivery);
	    if (afterHours) {
	      parts.push("זה אחרי שעות פעילות. תני מספרי מובילים אם יש, קחי הודעה קצרה והבטיחי שיחזרו אליהם בשעות פעילות.");
	      if (carrierPhones.length) parts.push("מספרי מובילים: " + carrierPhones.join(", "));
	    } else {
	      parts.push("אם מבקשים סטטוס משלוח: להסביר שאין סטטוס בזמן אמת ולהציע להשאיר הודעה/פרטים לחזרה.");
	    }
	  } else if (route === "support") {
	    parts.push("מטרה: להבין תקלה בקצרה, פרטי מוצר/מותג/הזמנה, ולסגור עם הבטחה לחזרה.");
	  } else if (route === "sales") {
	    parts.push("מטרה: להבין במה מתעניינים (סוג מוצר/דגם/מותג) ואז לקחת פרטי חזרה (אפשר להציע להשתמש במספר המזוהה).");
	  } else {
	    parts.push("אם לא ברור, תשאלי שאלה אחת להבהרה: מכירה / שירות / משלוח.");
	  }

	  let inst = parts.join("\n\n");

  const phone = extractPhoneCandidates(t);
	  if (phone) inst += `זוהה מספר בטקסט: ${phone}. אל תחזרי עליו אם לא צריך.\n`;

  return inst.trim();
};


  const printCallerFinal = (text) => {
    const t = String(text || "").trim();
    if (!t) return;
    if (t === lastCallerFinal) return;
    lastCallerFinal = t;
    pushTurn("caller", t);
    // Update proxy instructions for next response (Option B)
    proxyInstructions = buildProxyInstructions(t);
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

  // NOTE: declare openaiWs variable early so closures can reference safely
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

  const requestAssistantResponse = (reason = "") => {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;

    // If a response is already running - remember we owe ONE more response after it's done
    if (awaitingResponse) {
      pendingResponseRequest = true;
      debug(`[${connTag}] response.request queued (awaitingResponse=true) reason=${reason}`);
      return;
    }

    awaitingResponse = true;
    pendingResponseRequest = false;

    debug(`[${connTag}] response.create (reason=${reason})`);
    safeOpenAISend({
      type: "response.create",
      response: { modalities: ["audio", "text"] }
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

    const session = {
      modalities: ["audio", "text"],
      voice: OPENAI_VOICE,
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

  openaiWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      error(`[${connTag}] OpenAI message JSON parse failed`, e.message);
      return;
    }

    // RAW dump when needed
    if (MB_LOG_RAW_OPENAI) {
      const small = { type: msg.type, event_id: msg.event_id };
      if (msg.delta) small.delta_len = String(msg.delta).length;
      if (msg.transcript) small.transcript = preview(msg.transcript, 200);
      if (msg.text) small.text = preview(msg.text, 200);
      always(`[RAW_OPENAI][${connTag}]`, JSON.stringify(small));
    }

    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error event`, msg);
      const errCode = msg && msg.error && msg.error.code ? String(msg.error.code) : "";
      if (errCode === "conversation_already_has_active_response") {
        // Treat as still-speaking; queue exactly one response after current finishes
        awaitingResponse = true;
        pendingResponseRequest = true;
      }
      return;
    }

    // -----------------------------
    // BOT FINAL (clean)
    // -----------------------------
    if (MB_LOG_TRANSCRIPTS && msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) printBotFinal(t);
      return;
    }

    // -----------------------------
    // CALLER FINAL (robust)
    // -----------------------------
    if (MB_LOG_TRANSCRIPTS) {
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
        printCallerFinal(String(possible).trim());
        // after we got user final, request assistant response (if not already)
        requestAssistantResponse("caller_transcript_done");
        return;
      }
    }

    // -----------------------------
    // Turn boundary events (safe trigger)
    // -----------------------------
    if (msg.type === "input_audio_buffer.speech_stopped") {
      // Do not trigger responses on speech_stopped; we rely on final transcription only.
      return;
    }
// -----------------------------
    // response lifecycle
    // -----------------------------
    if (msg.type === "response.done") {
      awaitingResponse = false;

      // If something arrived while we were speaking - do exactly ONE next response
      if (pendingResponseRequest) {
        debug(`[${connTag}] response.done -> draining pendingResponseRequest`);
        // drain once
        pendingResponseRequest = false;
        requestAssistantResponse("pending_after_done");
      }
      return;
    }

    // -----------------------------
    // AUDIO back to Twilio
    // -----------------------------
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
      // call_started webhook suppressed (final-only mode)
      if (!MB_FINAL_WEBHOOK_ONLY) {
        sendWebhookEvent("call_started", {
          callSid,
          streamSid: twilioStreamSid,
          caller,
          called,
          started_at: startedAt,
          language,
          route,
          recording_url_public: makeRecordingPublicUrl(callSid)
        });
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

      safeOpenAISend({
        type: "input_audio_buffer.append",
        audio: payload
      });
      return;
    }

    if (msg.event === "stop") {
      always(`[TWILIO_STOP][${connTag}]`, "stream stopped");

      endedAt = nowIso();
      const recording_url_public = makeRecordingPublicUrl(callSid);

      if (!sentCallEnded) {
        sentCallEnded = true;

        // ONE final webhook (by route when possible) - always wait for recording (best effort)
        const finalEvent =
          route === "sales"
            ? "sales_lead"
            : route === "support"
            ? "support_ticket"
            : route === "delivery"
            ? "delivery_after_hours"
            : route === "message"
            ? "message_taken"
            : "call_ended";

        await sendWebhookEvent(
          finalEvent,
          {
            callSid,
            streamSid: twilioStreamSid,
            caller,
            called,
            started_at: startedAt,
            ended_at: endedAt,
            language,
            route,
            caller_last_utterance: lastCallerFinal,
            bot_last_utterance: lastBotText,
            transcript: transcriptTurns
          },
          { wait_for_recording: true }
        );
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

// If socket closed unexpectedly and we never sent call_ended -> abandoned
if (!sentCallEnded && !sentCallAbandoned) {
  sentCallAbandoned = true;
  endedAt = endedAt || nowIso();
  const recording_url_public = makeRecordingPublicUrl(callSid);
  sendWebhookEvent("call_abandoned", {
    callSid,
    streamSid: twilioStreamSid,
    caller,
    called,
    started_at: startedAt,
    ended_at: endedAt,
    language,
    route,
    caller_last_utterance: lastCallerFinal,
    bot_last_utterance: lastBotFinal,
    transcript: transcriptTurns,
    recording_url_public
  }, { wait_for_recording: true });
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
