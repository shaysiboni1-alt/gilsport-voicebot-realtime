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
    const suppliersImportersRange = valueRanges.find(
      (vr) => (vr.range || "").startsWith("SUPPLIERS_IMPORTERS!")
    );
    const deliveryContactsRange = valueRanges.find(
      (vr) => (vr.range || "").startsWith("DELIVERY_CONTACTS!")
    );

    const kbFactsRows = rowsToObjects((kbFactsRange?.values || []).slice());
    const doNotSayRows = rowsToObjects((doNotSayRange?.values || []).slice());
    const suppliersImportersRows = rowsToObjects(
      (suppliersImportersRange?.values || []).slice()
    );
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
      `Sheets loaded (prompts=${Object.keys(prompts).length}, settings=${
        Object.keys(settings).length
      }, kbFacts=${kbFactsRows.length}, doNotSay=${doNotSayRows.length}, suppliersImporters=${
        suppliersImportersRows.length
      }, deliveryContacts=${deliveryContactsRows.length})`
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
// Access: ${PUBLIC_BASE_URL}/recording/:callSid   (PUBLIC_BASE_URL should be this server public base)
app.get("/recording/:callSid", async (req, res) => {
  try {
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN)
      return res.status(404).send("recording proxy disabled");
    const callSid = String(req.params.callSid || "").trim();
    if (!callSid) return res.status(400).send("missing callSid");

    // Fetch latest recording for this call
    const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings.json?CallSid=${encodeURIComponent(
      callSid
    )}&PageSize=1`;

    const auth = Buffer.from(
      `${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`
    ).toString("base64");
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
  // Tracks the last caller utterance for which a response was requested.
  // This prevents sending multiple assistant responses for the same caller final.
  let lastRequestedCallerFinal = "";

  // Internal flags for response management.  We no longer track speech
  // segments; instead, we queue responses based on new caller final
  // transcriptions (see message handlers below).

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

  // Keep track of all phone numbers provided by the caller during this call. When
  // the caller mentions a phone number in their utterance, we extract the
  // digits and store them here. These numbers will be sent in the final
  // webhook payload under recognized_phones. The array is deduplicated.
  let recognizedPhones = [];

  // Buffer audio frames when the assistant is speaking. When awaitingResponse is
  // true, we temporarily store incoming caller audio and send it only after
  // the assistant finishes speaking. This prevents the model from listening
  // and reacting to noise or speech while it's talking.
  let pausedAudioBuffer = [];

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

  const formatSpacedDigits = (digits) => String(digits || "").split("").join(" ");

  const normalizePhoneDigits = (raw) => {
    let digits = String(raw || "").replace(/\D+/g, "");
    if (digits.startsWith("972") && digits.length > 3) {
      digits = "0" + digits.slice(3);
    }
    return digits;
  };

  const isValidPhoneDigits = (digits) => {
    const d = String(digits || "").replace(/\D+/g, "");
    return d.length === 9 || d.length === 10;
  };

  const isYes = (text) =>
    /(כן|כן כן|נכון|מאשר|אישור|yes|yep|yeah|ok|בסדר|סבבה|מוסכם)/i.test(
      String(text || "").trim()
    );

  const isNo = (text) =>
    /(לא|לא תודה|לא זה|לא נכון|no|nope|לא מעוניין|לא מסכים)/i.test(
      String(text || "").trim()
    );

  const extractBrandModel = (text) => {
    const t = String(text || "");
    const brandMatch = t.match(/מותג\s+([^,.\n\r]+)/);
    const modelMatch = t.match(/דגם\s+([^,.\n\r]+)/);
    return {
      brand: brandMatch ? brandMatch[1].trim() : "",
      model: modelMatch ? modelMatch[1].trim() : ""
    };
  };

  const extractRoute = (text) => {
    const low = String(text || "").toLowerCase();
    if (/(אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול)/.test(low)) return "support";
    if (/(משלוח|אספקה|עסקה|הספקה|אספקת|שליח|הזמנה|הגיע|לא הגיע|מוביל)/.test(low))
      return "delivery";
    if (/(מחיר|לקנות|רכישה|מוצר|דגם|מידה|צבע|מלאי|כמה עולה|מבצע)/.test(low))
      return "sales";
    if (/(הודעה|מנהל|עובד|לחזור אלי|השארת הודעה)/.test(low)) return "message";
    return "";
  };

  const parseHours = (s) => {
    // expects like "09:00-18:00" or "09:00–18:00"
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

  const buildFlowInstructions = (sayText, extra = []) => {
    const baseStyle =
      MB_BASE_STYLE && MB_BASE_STYLE.trim()
        ? MB_BASE_STYLE.trim()
        : "סגנון: נטע. תשובות קצרות, ענייניות, אנושיות. בלי חזרות מיותרות.";
    const dnsRows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
    const doNotSayText = dnsRows
      .map((r) => {
        const a = String(r.forbidden_topic || "").trim();
        const b = String(r.trigger_examples || "").trim();
        const c = String(r.safe_response_he || "").trim();
        const parts = [a && `נושא: ${a}`, b && `טריגרים: ${b}`, c && `תגובה בטוחה: ${c}`].filter(
          Boolean
        );
        return parts.join(" | ");
      })
      .filter(Boolean)
      .slice(0, 20)
      .join("\n");
    const rules = [
      baseStyle,
      "אין להמציא שמות, מספרים או פרטים שלא נאמרו או שלא קיימים בשיטס.",
      "כאשר מציינים מספר טלפון, הקריאי ספרה־ספרה בלבד.",
      doNotSayText ? `DO_NOT_SAY (כללים מחייבים):\n${doNotSayText}` : ""
    ].filter(Boolean);
    const say = sayText ? `תגידי בדיוק את המשפט הבא מילה במילה, ללא תוספות:\n${sayText}` : "";
    return [...rules, ...extra.filter(Boolean), say].filter(Boolean).join("\n\n").trim();
  };

  const buildCarrierList = () => {
    const deliveryRows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
    const carrierDescriptions = deliveryRows
      .map((r) => {
        let p = String(r.phone_e164 || r.phone || "").replace(/\D+/g, "");
        if (!p) return "";
        if (p.startsWith("972") && p.length > 3) {
          p = "0" + p.slice(3);
        }
        const spaced = formatSpacedDigits(p);
        const name = String(r.name || "").trim();
        return name ? `${name} – ${spaced}` : spaced;
      })
      .filter(Boolean);
    return carrierDescriptions;
  };

  const findExactImporter = (brandName) => {
    const brand = String(brandName || "").trim();
    if (!brand) return null;
    const importerRows = Array.isArray(SHEETS.suppliersImporters)
      ? SHEETS.suppliersImporters
      : [];
    const match = importerRows.find(
      (r) => String(r.brand_name || "").trim() === brand
    );
    if (!match) return null;
    return {
      brand: brand,
      importer: String(match.importer_name || "").trim(),
      phone: String(match.phone_e164 || match.phone || "").trim()
    };
  };

  const flowState = {
    stage: "routing",
    askedRouting: false,
    route: "other",
    afterHours: false,
    data: {
      product_interest: "",
      product_model: "",
      product_brand: "",
      issue_type: "",
      issue_desc: "",
      message_target: "",
      message_body: "",
      delivery_desc: "",
      full_name: "",
      callback_phone: ""
    },
    finalEvent: "",
    finalSummary: "",
    finalPayload: null,
    shouldHangup: false
  };

  const ensureCallerDigits = () => {
    const callerRaw = String(caller || "").trim();
    if (!callerRaw) return "";
    return normalizePhoneDigits(callerRaw);
  };

  const buildFinalPayload = () => {
    const ended = endedAt || nowIso();
    const fallbackCallerPhone = ensureCallerDigits();
    const payload = {
      callSid,
      streamSid: twilioStreamSid,
      caller,
      called,
      started_at: startedAt,
      ended_at: ended,
      language,
      route: flowState.route,
      caller_last_utterance: lastCallerFinal,
      bot_last_utterance: lastBotFinal,
      transcript: transcriptTurns,
      recognized_phones: recognizedPhones,
      call_reason: flowState.route,
      call_subject: flowState.finalSummary || lastBotFinal || lastCallerFinal
    };
    if (flowState.route === "sales") {
      payload.product_interest = flowState.data.product_interest;
      payload.product_model = flowState.data.product_model;
      payload.product_brand = flowState.data.product_brand;
      payload.full_name = flowState.data.full_name;
      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
    } else if (flowState.route === "support") {
      payload.issue_type = flowState.data.issue_type;
      payload.issue_desc = flowState.data.issue_desc;
      payload.product_model = flowState.data.product_model;
      payload.product_brand = flowState.data.product_brand;
      payload.full_name = flowState.data.full_name;
      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
    } else if (flowState.route === "delivery") {
      payload.delivery_desc = flowState.data.delivery_desc;
      payload.full_name = flowState.data.full_name;
      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
      payload.after_hours = flowState.afterHours ? "כן" : "לא";
    } else if (flowState.route === "message") {
      payload.message_target = flowState.data.message_target;
      payload.message_body = flowState.data.message_body;
      payload.full_name = flowState.data.full_name;
      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
    }
    return payload;
  };

  const handleRouting = (utterance) => {
    const routeCandidate = extractRoute(utterance);
    if (routeCandidate) {
      flowState.route = routeCandidate;
      flowState.stage =
        routeCandidate === "sales"
          ? "sales_product"
          : routeCandidate === "support"
          ? "support_issue"
          : routeCandidate === "delivery"
          ? "delivery_name"
          : "message_target";
      if (routeCandidate === "delivery") {
        flowState.afterHours = isAfterHours();
        flowState.data.delivery_desc = String(utterance || "").trim();
      }
      if (routeCandidate === "sales") {
        flowState.data.product_interest = String(utterance || "").trim();
      }
      if (routeCandidate === "support") {
        flowState.data.issue_desc = String(utterance || "").trim();
        flowState.data.issue_type = String(utterance || "").trim();
      }
    } else if (!flowState.askedRouting) {
      flowState.askedRouting = true;
      flowState.stage = "routing_clarify";
    } else {
      flowState.route = "message";
      flowState.stage = "message_target";
    }
    route = flowState.route;
  };

  const buildNextInstructions = () => {
    const callerDigits = ensureCallerDigits();
    const spacedCaller = callerDigits ? formatSpacedDigits(callerDigits) : "";
    if (flowState.stage === "routing_clarify") {
      return buildFlowInstructions(
        "כדי לעזור במדויק—זה לגבי התעניינות במוצר, שירות/תקלה/אחריות, משלוח/אספקה, או להשאיר הודעה למישהו מהצוות?"
      );
    }
    if (flowState.stage === "sales_product") {
      return buildFlowInstructions(
        "בשמחה. על איזה מוצר אתם מתעניינים? תגידו לי בבקשה: סוג מוצר, ואם יש—דגם ו־שם מותג."
      );
    }
    if (flowState.stage === "sales_name") {
      return buildFlowInstructions("מעולה, תודה. כדי שנחזור אליכם—מה השם המלא שלכם?");
    }
    if (flowState.stage === "sales_phone_confirm") {
      const text = spacedCaller
        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
        : "על איזה מספר טלפון נוח לחזור אליכם?";
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "sales_phone_collect") {
      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
    }
    if (flowState.stage === "sales_phone_confirm_new") {
      const spaced = formatSpacedDigits(flowState.data.callback_phone);
      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
    }
    if (flowState.stage === "sales_done") {
      flowState.finalEvent = "מתעניין";
      flowState.finalSummary = "לקוח מתעניין ברכישת מוצר";
      flowState.shouldHangup = true;
      return buildFlowInstructions(
        "מעולה. העברתי את הפרטים למחלקת המכירות, ויחזרו אליכם בהקדם. תודה רבה ויום טוב."
      );
    }
    if (flowState.stage === "support_issue") {
      return buildFlowInstructions(
        "הבנתי. כדי שאעביר לשירות בצורה מדויקת—מה סוג התקלה ומה מהות התקלה בכמה מילים?"
      );
    }
    if (flowState.stage === "support_product") {
      return buildFlowInstructions("ועל איזה מוצר זה? תגידו לי בבקשה דגם ו־שם מותג.");
    }
    if (flowState.stage === "support_name") {
      return buildFlowInstructions("תודה. מה השם המלא שלכם?");
    }
    if (flowState.stage === "support_phone_confirm") {
      const text = spacedCaller
        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
        : "על איזה מספר טלפון נוח לחזור אליכם?";
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "support_phone_collect") {
      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
    }
    if (flowState.stage === "support_phone_confirm_new") {
      const spaced = formatSpacedDigits(flowState.data.callback_phone);
      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
    }
    if (flowState.stage === "support_done") {
      flowState.finalEvent = "שירות לקוחות \\ תמיכה";
      flowState.finalSummary = "לקוח מבקש שירות/תמיכה";
      flowState.shouldHangup = true;
      const importer = findExactImporter(flowState.data.product_brand);
      const extra = [];
      if (importer && importer.phone) {
        const spaced = formatSpacedDigits(normalizePhoneDigits(importer.phone));
        extra.push(
          `תגידי לפני הסיום: רק לידיעה—המספר של היבואן עבור המותג ${importer.brand} הוא: ${spaced}.`
        );
      }
      return buildFlowInstructions(
        "מעולה. שלחתי את הפרטים למחלקת השירות, ויחזרו אליכם בהקדם. תודה רבה ויום טוב.",
        extra
      );
    }
    if (flowState.stage === "delivery_name") {
      flowState.afterHours = isAfterHours();
      const carriers = flowState.afterHours ? buildCarrierList() : [];
      const afterHoursText =
        flowState.afterHours && carriers.length
          ? `כרגע אנחנו מחוץ לשעות הפעילות. כדי לעזור כבר עכשיו—אלו מספרי הטלפון של המובילים: ${carriers.join(
              ", "
            )}.`
          : flowState.afterHours
          ? "כרגע אנחנו מחוץ לשעות הפעילות. אין לי כרגע מספרי מובילים זמינים מהטבלה."
          : "";
      const intro = "הבנתי. זה לגבי משלוח/אספקה. אני אקח כמה פרטים ואעביר למחלקת אספקה.";
      const askName = "מה השם המלא שלכם?";
      return buildFlowInstructions(
        [intro, afterHoursText, askName].filter(Boolean).join(" ")
      );
    }
    if (flowState.stage === "delivery_phone_confirm") {
      const text = spacedCaller
        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
        : "על איזה מספר טלפון נוח לחזור אליכם?";
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "delivery_phone_collect") {
      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
    }
    if (flowState.stage === "delivery_phone_confirm_new") {
      const spaced = formatSpacedDigits(flowState.data.callback_phone);
      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
    }
    if (flowState.stage === "delivery_done") {
      flowState.finalEvent = "שירות לקוחות \\ אספקה";
      flowState.finalSummary = "לקוח רוצה לברר לגבי משלוח/אספקה";
      flowState.shouldHangup = true;
      return buildFlowInstructions(
        "תודה. העברתי את הפרטים למחלקת אספקה, ויחזרו אליכם בהקדם. יום טוב."
      );
    }
    if (flowState.stage === "message_target") {
      return buildFlowInstructions("בשמחה. למי מיועדת ההודעה? (שם עובד/מנהל)");
    }
    if (flowState.stage === "message_name") {
      return buildFlowInstructions("מה השם המלא שלכם?");
    }
    if (flowState.stage === "message_body") {
      return buildFlowInstructions("מה מהות ההודעה? תאמרו/תאמרי את זה בקצרה.");
    }
    if (flowState.stage === "message_phone_confirm") {
      const text = spacedCaller
        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
        : "על איזה מספר טלפון נוח לחזור אליכם?";
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "message_phone_collect") {
      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
    }
    if (flowState.stage === "message_phone_confirm_new") {
      const spaced = formatSpacedDigits(flowState.data.callback_phone);
      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
    }
    if (flowState.stage === "message_done") {
      flowState.finalEvent = "הודעה כללית";
      flowState.finalSummary = `הודעה עבור ${flowState.data.message_target || "הצוות"}`;
      flowState.shouldHangup = true;
      return buildFlowInstructions(
        `תודה. העברתי את ההודעה ל־${flowState.data.message_target} ויחזרו אליכם בהקדם. יום טוב.`
      );
    }
    return "";
  };

  const processCallerUtterance = (utterance) => {
    const text = String(utterance || "").trim();
    if (!text) return "";
    if (flowState.stage === "routing" || flowState.stage === "routing_clarify") {
      handleRouting(text);
      return buildNextInstructions();
    }
    if (flowState.stage === "sales_product") {
      flowState.data.product_interest = text;
      const { brand, model } = extractBrandModel(text);
      if (brand) flowState.data.product_brand = brand;
      if (model) flowState.data.product_model = model;
      flowState.stage = "sales_name";
      return buildNextInstructions();
    }
    if (flowState.stage === "sales_name") {
      flowState.data.full_name = text;
      flowState.stage = "sales_phone_confirm";
      return buildNextInstructions();
    }
    if (flowState.stage === "sales_phone_confirm") {
      if (isYes(text)) {
        flowState.data.callback_phone = ensureCallerDigits();
        flowState.stage = "sales_done";
        return buildNextInstructions();
      }
      if (isNo(text) || !ensureCallerDigits()) {
        flowState.stage = "sales_phone_collect";
        return buildNextInstructions();
      }
      flowState.stage = "sales_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "sales_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
      }
      flowState.data.callback_phone = digits;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      flowState.stage = "sales_phone_confirm_new";
      return buildNextInstructions();
    }
    if (flowState.stage === "sales_phone_confirm_new") {
      if (isYes(text)) {
        flowState.stage = "sales_done";
        return buildNextInstructions();
      }
      flowState.stage = "sales_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "support_issue") {
      flowState.data.issue_type = text;
      flowState.data.issue_desc = text;
      flowState.stage = "support_product";
      return buildNextInstructions();
    }
    if (flowState.stage === "support_product") {
      const { brand, model } = extractBrandModel(text);
      flowState.data.product_brand = brand || flowState.data.product_brand;
      flowState.data.product_model = model || flowState.data.product_model;
      flowState.stage = "support_name";
      return buildNextInstructions();
    }
    if (flowState.stage === "support_name") {
      flowState.data.full_name = text;
      flowState.stage = "support_phone_confirm";
      return buildNextInstructions();
    }
    if (flowState.stage === "support_phone_confirm") {
      if (isYes(text)) {
        flowState.data.callback_phone = ensureCallerDigits();
        flowState.stage = "support_done";
        return buildNextInstructions();
      }
      if (isNo(text) || !ensureCallerDigits()) {
        flowState.stage = "support_phone_collect";
        return buildNextInstructions();
      }
      flowState.stage = "support_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "support_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
      }
      flowState.data.callback_phone = digits;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      flowState.stage = "support_phone_confirm_new";
      return buildNextInstructions();
    }
    if (flowState.stage === "support_phone_confirm_new") {
      if (isYes(text)) {
        flowState.stage = "support_done";
        return buildNextInstructions();
      }
      flowState.stage = "support_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "delivery_name") {
      flowState.data.full_name = text;
      flowState.stage = "delivery_phone_confirm";
      return buildNextInstructions();
    }
    if (flowState.stage === "delivery_phone_confirm") {
      if (isYes(text)) {
        flowState.data.callback_phone = ensureCallerDigits();
        flowState.stage = "delivery_done";
        return buildNextInstructions();
      }
      if (isNo(text) || !ensureCallerDigits()) {
        flowState.stage = "delivery_phone_collect";
        return buildNextInstructions();
      }
      flowState.stage = "delivery_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "delivery_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
      }
      flowState.data.callback_phone = digits;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      flowState.stage = "delivery_phone_confirm_new";
      return buildNextInstructions();
    }
    if (flowState.stage === "delivery_phone_confirm_new") {
      if (isYes(text)) {
        flowState.stage = "delivery_done";
        return buildNextInstructions();
      }
      flowState.stage = "delivery_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "message_target") {
      flowState.data.message_target = text;
      flowState.stage = "message_name";
      return buildNextInstructions();
    }
    if (flowState.stage === "message_name") {
      flowState.data.full_name = text;
      flowState.stage = "message_body";
      return buildNextInstructions();
    }
    if (flowState.stage === "message_body") {
      flowState.data.message_body = text;
      flowState.stage = "message_phone_confirm";
      return buildNextInstructions();
    }
    if (flowState.stage === "message_phone_confirm") {
      if (isYes(text)) {
        flowState.data.callback_phone = ensureCallerDigits();
        flowState.stage = "message_done";
        return buildNextInstructions();
      }
      if (isNo(text) || !ensureCallerDigits()) {
        flowState.stage = "message_phone_collect";
        return buildNextInstructions();
      }
      flowState.stage = "message_phone_collect";
      return buildNextInstructions();
    }
    if (flowState.stage === "message_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
      }
      flowState.data.callback_phone = digits;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      flowState.stage = "message_phone_confirm_new";
      return buildNextInstructions();
    }
    if (flowState.stage === "message_phone_confirm_new") {
      if (isYes(text)) {
        flowState.stage = "message_done";
        return buildNextInstructions();
      }
      flowState.stage = "message_phone_collect";
      return buildNextInstructions();
    }
    return "";
  };

  /**
   * Normalize an input transcript for duplicate detection.
   * This helper removes common greetings and filler words,
   * strips punctuation and extra whitespace, and lowercases the result.
   * It allows us to compare two caller utterances for semantic equality
   * even if they differ slightly in casing or punctuation. We define
   * greetings that should not trigger a new response (e.g. "היי", "שלום", "ביי").
   */
  const normalizeTranscript = (s) => {
    try {
      let t = String(s || "").toLowerCase();
      // Remove punctuation
      t = t.replace(/[\.,!?\-–—;:'"\u05be]/g, " ");
      // Replace multiple spaces
      t = t.replace(/\s+/g, " ").trim();
      // Remove common greetings and filler words at start or end
      const greetings = [
        "היי",
        "הי",
        "שלום",
        "ביי",
        "היי שלום",
        "היי, שלום",
        "היי שלום לך",
        "שלום לך",
        "ביי שלום",
        "ביי, שלום",
        "אה",
        "אה, שלום",
        "אה שלום",
        "אה, שלום לך",
        "אה שלום לך",
        // English greetings and farewells to ignore
        "hi",
        "hello",
        "bye",
        "bye-bye",
        "bye bye",
        "bye, bye"
      ];
      // Remove greeting phrases from beginning
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
    const fillerPhrases = [
      "תודה",
      "תודה רבה",
      "כן",
      "סבבה",
      "בבקשה",
      "בסדר",
      "תודה על הקופון"
    ];
    return fillerPhrases.some(
      (fp) => normalized === fp || normalized.startsWith(fp + " ") || normalized.endsWith(" " + fp)
    );
  };

  // Keep track of normalized caller utterances to prevent duplicate responses
  let lastCallerNormalized = "";
  let lastRequestedCallerNormalized = "";

  const printCallerFinal = (text) => {
    const t = String(text || "").trim();
    if (!t) return;
    if (t === lastCallerFinal) return;
    lastCallerFinal = t;
    pushTurn("caller", t);
    // Update proxy instructions for next response (flow-driven)
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
  // awaitingResponse indicates the assistant is currently speaking. While true
  // we pause forwarding caller audio to OpenAI. When false, we can send
  // audio and, if there is a pending user utterance, trigger a response.
  let awaitingResponse = false;
  // pendingResponseRequest is flagged whenever we detect a new caller
  // utterance (final transcript). It is consumed on the next response.done.
  let pendingResponseRequest = false;

  // When flushing buffered audio frames back to OpenAI after the assistant
  // finishes speaking, we temporarily ignore any resulting transcripts.
  let isFlushingBufferedAudio = false;

  const requestAssistantResponse = (reason = "") => {
    // Only send a response if the OpenAI WS is ready
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;

    // Don't start a new response while another is in flight
    if (awaitingResponse) {
      debug(`[${connTag}] response.request ignored (awaitingResponse=true) reason=${reason}`);
      return;
    }

    // Reset flags: we're starting a new response now
    awaitingResponse = true;
    pendingResponseRequest = false;

    // Mark that we've responded to the most recent caller utterance
    lastRequestedCallerFinal = lastCallerFinal;
    lastRequestedCallerNormalized = lastCallerNormalized;

    // Build dynamic instructions for this turn.
    let instructions = proxyInstructions;
    if (!instructions) {
      try {
        instructions = buildNextInstructions();
      } catch (_) {
        instructions = getPrompt(
          "MASTER_PROMPT",
          "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
        );
      }
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

    // Prepare the realtime session settings. We allow specifying a voice
    // style and speaking rate via environment variables. If a style is
    // provided, we pass an object with id, style and rate to the API.
    const session = {
      modalities: ["audio", "text"],
      voice:
        OPENAI_VOICE_STYLE || OPENAI_SPEAKING_RATE !== 1.0
          ? {
              id: OPENAI_VOICE,
              // Only include style if non-empty to avoid overriding defaults.
              ...(OPENAI_VOICE_STYLE ? { style: OPENAI_VOICE_STYLE } : {}),
              // Only include rate if not 1.0 (default)
              ...(OPENAI_SPEAKING_RATE !== 1.0 ? { rate: OPENAI_SPEAKING_RATE } : {})
            }
          : OPENAI_VOICE,
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

    // Optional: log intermediate bot transcript parts if provided. This can help debug misread numbers.
    try {
      if (MB_LOG_TRANSCRIPTS && msg && typeof msg.transcript === "string" && msg.type && String(msg.type).startsWith("response.audio_transcript.delta")) {
        always(`[BOT_PART][${connTag}]`, msg.transcript.trim());
      }
    } catch (_) {
      /* ignore logging errors */
    }

    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error event`, msg);
      const errCode = msg && msg.error && msg.error.code ? String(msg.error.code) : "";
      if (errCode === "conversation_already_has_active_response") {
        // The assistant is still speaking. Do not queue a new response here; the
        // caller_final handler will decide if another response is needed. We simply
        // keep awaitingResponse=true to prevent sending another response before
        // the current one finishes.
        awaitingResponse = true;
        // Do not set pendingResponseRequest here — rely on new caller_final events.
      }
      return;
    }

    // -----------------------------
    // BOT FINAL (clean)
    // -----------------------------
    if (msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) printBotFinal(t);
      return;
    }

    // -----------------------------
    // CALLER FINAL (robust)
    // -----------------------------
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
        // Normalize the utterance to remove greetings/punctuation for duplicate detection
        const normalized = normalizeTranscript(utterance);
        // Count meaningful words in the normalized text
        const wordCount = normalized.split(/\s+/).filter(Boolean).length;
        // If we are currently flushing buffered audio, ignore any transcripts generated
        // by the flush. These partial echoes of prior audio should not trigger new responses.
        if (isFlushingBufferedAudio) {
          // still update lastCallerNormalized for diagnostics, but do not queue a response
          lastCallerNormalized = normalized;
          lastCallerFinal = utterance;
          return;
        }
        printCallerFinal(utterance);
        // Update latest normalized utterance
        lastCallerNormalized = normalized;
        // Determine if the utterance contains any meaningful keywords (for routing)
        const keywordList = [
          "קופון",
          "תקלה",
          "בעיה",
          "שירות",
          "החלפה",
          "החזרה",
          "לא עובד",
          "משלוח",
          "אספקה",
          "שליח",
          "הזמנה",
          "הגיע",
          "לא הגיע",
          "מוביל",
          "מחיר",
          "לקנות",
          "רכישה",
          "מוצר",
          "דגם",
          "מידה",
          "צבע",
          "מלאי",
          "מבצע",
          "קנייה",
          "קניה"
        ];
        const hasKeyword = keywordList.some((kw) => normalized.includes(kw));
        if (normalized) {
          let isDup = false;
          // Consider it a duplicate if the new normalized utterance is identical to the
          // last one we responded to, or one contains the other. This avoids
          // triggering multiple responses for slight transcription differences.
          if (lastRequestedCallerNormalized) {
            if (normalized === lastRequestedCallerNormalized) {
              isDup = true;
            } else if (normalized.startsWith(lastRequestedCallerNormalized)) {
              isDup = true;
            } else if (lastRequestedCallerNormalized.startsWith(normalized)) {
              isDup = true;
            }
          }
          const allowShortReply =
            flowState && flowState.stage && !String(flowState.stage).endsWith("done");
          const hasPhone = Boolean(extractPhoneCandidates(normalized));
          const meaningfulShort =
            isYes(normalized) || isNo(normalized) || hasPhone || (normalized && !isFillerOnly(normalized));
          if (!isDup) {
            if (allowShortReply) {
              if (meaningfulShort) pendingResponseRequest = true;
            } else if (wordCount >= 5 || hasKeyword) {
              pendingResponseRequest = true;
            }
          }
        }
        return;
      }
    }

    // -----------------------------
    // Turn boundary events
    // -----------------------------
    if (msg.type === "input_audio_buffer.speech_stopped") {
      // Ignore speech_stopped events for response timing. We'll respond after
      // the assistant finishes speaking (response.done) based on pendingResponseRequest.
      return;
    }

    // response lifecycle
    if (msg.type === "response.done") {
      awaitingResponse = false;
      // Flush any buffered audio frames that arrived while assistant was speaking.
      // We mark that we are flushing so that any resulting transcriptions do not
      // trigger a new response inadvertently.
      if (Array.isArray(pausedAudioBuffer) && pausedAudioBuffer.length > 0) {
        isFlushingBufferedAudio = true;
        while (pausedAudioBuffer.length > 0) {
          const audioFrame = pausedAudioBuffer.shift();
          safeOpenAISend({ type: "input_audio_buffer.append", audio: audioFrame });
        }
        // give OpenAI some time to process the flush before accepting new transcripts
        // we don't await here, but we reset the flag on the next tick
        setTimeout(() => {
          isFlushingBufferedAudio = false;
        }, 50);
      }
      // If we have a pending caller utterance, and we haven't already
      // responded to it, send one response now. We rely on the check
      // lastCallerFinal !== lastRequestedCallerFinal to ensure we respond
      // exactly once per caller utterance.
      if (pendingResponseRequest && lastCallerFinal !== lastRequestedCallerFinal) {
        debug(`[${connTag}] response.done -> draining pendingResponseRequest`);
        pendingResponseRequest = false;
        requestAssistantResponse("pending_after_done");
      }
      if (flowState.shouldHangup && !sentCallEnded && flowState.finalEvent) {
        sentCallEnded = true;
        endedAt = endedAt || nowIso();
        const payload = buildFinalPayload();
        sendWebhookEvent(flowState.finalEvent, payload, { wait_for_recording: true });
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
      // If the assistant is currently speaking, buffer the audio instead of
      // sending it immediately. This prevents the model from listening during
      // its own response.
      if (awaitingResponse) {
        pausedAudioBuffer.push(payload);
        // cap buffer to avoid unbounded growth
        if (pausedAudioBuffer.length > 400) {
          pausedAudioBuffer.splice(0, pausedAudioBuffer.length - 400);
        }
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

      if (!sentCallEnded) {
        sentCallEnded = true;
        const fallbackEvent =
          route === "sales"
            ? "sales_lead"
            : route === "support"
            ? "support_ticket"
            : route === "delivery"
            ? "delivery_after_hours"
            : route === "message"
            ? "message_taken"
            : "call_ended";
        const finalEvent = flowState.finalEvent || fallbackEvent;
        const payload = flowState.finalEvent ? buildFinalPayload() : {
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
          transcript: transcriptTurns
        };
        await sendWebhookEvent(finalEvent, payload, { wait_for_recording: true });
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
      sendWebhookEvent(
        "call_abandoned",
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
          bot_last_utterance: lastBotFinal,
          transcript: transcriptTurns,
          recording_url_public
        },
        { wait_for_recording: true }
      );
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
