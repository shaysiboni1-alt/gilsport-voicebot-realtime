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

const FALLBACK_EMPTY_INSTRUCTIONS = "סליחה, לא הבנתי. תוכלו לחזור בבקשה?";
const FALLBACK_ROUTING_CLARIFY =
  "כדי לעזור במדויק—זה לגבי התעניינות במוצר, שירות/תקלה/אחריות, משלוח/אספקה, או להשאיר הודעה למישהו מהצוות?";
const FALLBACK_PHONE_MISSING_DIGIT =
  "נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?";
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

async function completeTwilioCall(callSid) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return false;
  if (!callSid) return false;
  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${encodeURIComponent(
      callSid
    )}.json`;
    const auth = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString(
      "base64"
    );
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
    if (!listResp.ok) return res.status(404).send("no recording found for callSid");
    const listJson = await listResp.json();
    const rec = (listJson.recordings || [])[0];
    if (!rec || !rec.sid) return res.status(404).send("no recording found for callSid");

    // Twilio media (mp3)
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
  let hangupRequested = false;

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
    const normalized = normalizePhoneDigits(t);
    return isValidPhoneDigits(normalized) ? normalized : "";
  };

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
      text = text.replace(new RegExp(`\\b${word}\\b`, "g"), digit);
    }
    let digits = text.replace(/\D+/g, "");
    if (digits.startsWith("972") && digits.length > 3) {
      digits = "0" + digits.slice(3);
    }
    if (digits.startsWith("0") && digits.length > 10) {
      digits = digits.slice(0, 10);
    }
    return digits;
  };

  const isValidPhoneDigits = (digits) => {
    const d = String(digits || "").replace(/\D+/g, "");
    return d.length === 10 && d.startsWith("0");
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
      "עברית בלבד. תמיד בלשון רבים וללא פנייה מגדרית.",
      "זרימת שיחה ליניארית: בכל שלב שאלה אחת בלבד, להמתין למענה מלא לפני מעבר לשלב הבא.",
      "לא לשאול שוב שאלה שכבר נענתה, ולא ליזום שאלות כלליות או איפוס שיחה.",
      "הזרימה נקבעת לפי השלב והטקסט מהשיטס בלבד; אין להחליט על שלבים חדשים.",
      "אין להמציא שמות, מספרים או פרטים שלא נאמרו או שלא קיימים בשיטס.",
      "כאשר מציינים מספר טלפון, הקריאי ספרה־ספרה בלבד.",
      "את חייבת להקריא בדיוק את הטקסט שמופיע תחת SAY מילה במילה, בלי להוסיף כלום.",
      doNotSayText ? `DO_NOT_SAY (כללים מחייבים):\n${doNotSayText}` : ""
    ].filter(Boolean);
    if (!sayText) {
      return buildFlowInstructions(FALLBACK_EMPTY_INSTRUCTIONS);
    }
    const say = `SAY:\n${sayText}`;
    return [...rules, ...extra.filter(Boolean), say].filter(Boolean).join("\n\n").trim();
  };

  // --------------------------------------------------
  // Flow helpers driven by Sheets (no hardcoded answers)
  // --------------------------------------------------

  const getDoNotSayRowByTopic = (topic) => {
    const t = String(topic || "").trim();
    if (!t) return null;
    const rows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
    return rows.find((r) => String(r.forbidden_topic || "").trim() === t) || null;
  };

  const getPriceClaimAndCouponFromSheets = () => {
    // We derive the price-claim + coupon from the safe response in DO_NOT_SAY (מחירים/הצעות).
    const row = getDoNotSayRowByTopic("מחירים/הצעות");
    const safe = row ? String(row.safe_response_he || "").trim() : "";
    if (!safe) return { priceClaim: "", couponDigitsSpaced: "" };
    // Extract a spaced coupon if present (e.g. 5 5 5 5).
    const m = safe.match(/(\d\s+\d\s+\d\s+\d)/);
    const couponDigitsSpaced = m ? m[1].trim() : "";
    // Price claim is the first sentence (up to first period), but keep original if no period.
    let priceClaim = safe;
    const dot = safe.indexOf(".");
    if (dot !== -1) priceClaim = safe.slice(0, dot + 1).trim();
    return { priceClaim, couponDigitsSpaced };
  };

  const buildCarrierOfferTextFromSheets = (carriersList) => {
    // FLOW_DELIVERY_AFTER_HOURS exists in PROMPTS. We render {carriers}.
    const tmpl = getFlowTextOrFallback(
      "FLOW_DELIVERY_AFTER_HOURS",
      "אם אתם ממתינים לאספקה לאותו יום אחרי שעות הפעילות, אפשר לפנות למובילים: {carriers}. רוצים שאמסור את המספר?"
    );
    return renderFlowText(tmpl, { carriers: (carriersList || []).join(", ") });
  };

  const findDeliverySameDayMatch = (text) => {
    const t = String(text || "").trim();
    if (!t) return false;
    const rows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
    const low = t.toLowerCase();
    for (const r of rows) {
      const cond = String(r.rule || r.condition_rule || "").trim();
      // Only apply for after_hours_same_day_only rules
      if (cond && cond !== "after_hours_same_day_only") continue;
      const kws = String(r.condition_keywords || "").split(",").map((x) => x.trim()).filter(Boolean);
      if (!kws.length) continue;
      if (kws.some((kw) => kw && low.includes(kw.toLowerCase()))) return true;
    }
    return false;
  };

  const detectMessageTargetInText = (text) => {
    const t = String(text || "").trim();
    if (!t) return "";
    // Patterns like: "הודעה ל..." / "הודעה עבור ..."
    const m = t.match(/(?:הודעה\s+(?:ל|עבור)\s+)([^,.\n\r]{2,40})/);

    if (!m) return "";
    let name = String(m[1] || "").trim();
    name = name.replace(/\b(בבקשה|תודה)\b/g, "").replace(/\s+/g, " ").trim();
    if (name.length < 2) return "";
    return name;
  };

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

  const renderFlowText = (template, vars = {}) => {
    if (!template) return "";
    return template.replace(/\{(\w+)\}/g, (match, k) =>
      Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : match
    );
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

  const collected = {
    product_type: "",
    product_model: "",
    product_brand: "",
    issue_desc: "",
    issue_topic: "",
    message_target: "",
    message_body: "",
    delivery_desc: "",
    delivery_topic: "",
    after_hours: false,
    full_name: "",
    callback_phone: ""
  };

  const extractNameCandidate = (text) => {
    let t = String(text || "").trim();
    if (!t) return "";
    const phoneCandidate = extractPhoneCandidates(t);
    if (phoneCandidate) {
      t = t.replace(phoneCandidate, "").trim();
    }
    const lowered = t.toLowerCase();
    const nameMarkers = ["השם שלי", "קוראים לי", "שמי", "אני"];
    for (const marker of nameMarkers) {
      if (lowered.includes(marker)) {
        const idx = lowered.lastIndexOf(marker);
        t = t.slice(idx + marker.length).trim();
        break;
      }
    }
    t = t.replace(/[0-9]/g, "").replace(/\s+/g, " ").trim();
    if (!t || t.length < 2 || t.length > 40) return "";
    const filler = ["תודה", "אוקיי", "אוקי", "כן", "לא", "ביי", "שלום", "בסדר", "סבבה"];
    if (filler.includes(t)) return "";
    if (!/[A-Za-z\u0590-\u05FF]/.test(t)) return "";
    return t;
  };

  const isExplicitNamePhrase = (text) =>
    /(השם שלי|קוראים לי|שמי)/.test(String(text || ""));

  const extractIssueTopic = (text) => {
    const t = String(text || "");
    if (/מסך|תצוגה|צג/.test(t)) return "תקלה במסך";
    if (/מנוע|רע(ש|שים)|רעש|חריקה/.test(t)) return "תקלה במנוע";
    if (/לא נדלק|לא עובד|לא מגיב/.test(t)) return "לא עובד";
    if (/חשמל|כבל|תקע|ספק/.test(t)) return "בעיה חשמלית";
    return "";
  };

  const extractDeliveryTopic = (text) => {
    const t = String(text || "");
    if (/לא הגיע|לא קיבל|לא הגיעו/.test(t)) return "לא הגיע";
    if (/איחור|מאחר/.test(t)) return "איחור";
    if (/פגום|שבור|קרוע/.test(t)) return "נזק במשלוח";
    if (/תיאום|תאריך|שעה/.test(t)) return "תיאום משלוח";
    return "";
  };

  const buildSummary = () => {
    const brandModel = [flowState.data.product_brand, flowState.data.product_model]
      .filter((v) => String(v || "").trim())
      .join(" ");
    if (flowState.route === "sales") {
      const parts = [
        flowState.data.product_type ? `התעניינות: ${flowState.data.product_type}` : "התעניינות",
        brandModel
      ].filter(Boolean);
      return parts.join(" | ");
    }
    if (flowState.route === "support") {
      const parts = [
        flowState.data.issue_desc ? `שירות: ${flowState.data.issue_desc}` : "שירות",
        brandModel
      ].filter(Boolean);
      return parts.join(" | ");
    }
    if (flowState.route === "delivery") {
      const afterHoursText = flowState.afterHours ? "כן" : "לא";
      const parts = [
        flowState.data.delivery_desc ? `משלוח: ${flowState.data.delivery_desc}` : "משלוח",
        `אחרי שעות: ${afterHoursText}`
      ];
      return parts.join(" | ");
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
      return `הודעה ל־${target}: ${bodyShort || "ללא תוכן"}`;
    }
    return "פנייה כללית";
  };

  const flowState = {
    stage: "routing",
    askedRouting: false,
    route: "other",
    afterHours: false,
    collected,
    data: collected,
    finalEvent: "",
    finalSummary: "",
    finalPayload: null,
    shouldHangup: false,
    stageAdvanced: false,
    phoneConfirmed: false,
    finalPayloadSent: false,
    doneLocked: false,
    allowFinalResponse: false
  };

  const ensureCallerDigits = () => {
    const callerRaw = String(caller || "").trim();
    if (!callerRaw) return "";
    const digits = normalizePhoneDigits(callerRaw);
    return isValidPhoneDigits(digits) ? digits : "";
  };

  const buildFinalPayload = () => {
    const ended = endedAt || nowIso();
    const callerDigits = ensureCallerDigits();
    const recording_url_public = makeRecordingPublicUrl(callSid);
    const caller_id = isValidPhoneDigits(callerDigits) ? callerDigits : "";
    const callbackFromData = isValidPhoneDigits(flowState.data.callback_phone)
      ? flowState.data.callback_phone
      : "";
    const callback_phone =
      callbackFromData ||
      (flowState.phoneConfirmed && caller_id ? caller_id : "");
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
      product_type: flowState.data.product_type || "",
      product_brand: flowState.data.product_brand || "",
      product_model: flowState.data.product_model || "",
      issue_desc: flowState.data.issue_desc || "",
      issue_topic: flowState.data.issue_topic || "",
      delivery_desc: flowState.data.delivery_desc || "",
      delivery_topic: flowState.data.delivery_topic || "",
      after_hours: Boolean(flowState.afterHours),
      message_target: flowState.data.message_target || "",
      message_body: flowState.data.message_body || "",
      caller_last_utterance: lastCallerFinal,
      bot_last_utterance: lastBotFinal,
      transcript: transcriptTurns,
      recognized_phones: recognizedPhones,

      // Route-specific friendly fields (for Make/CRM expectations)
      event_name: flowState.finalEvent || "",
      timestamp: ended,
      identified_phone: caller_id,
      additional_phone: callbackFromData && callbackFromData != caller_id ? callbackFromData : "",
      carriers_info_given: Boolean(flowState.data.carriers_info_given),
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
    const callbackCandidate = isValidPhoneDigits(flowState.data.callback_phone)
      ? flowState.data.callback_phone
      : "";
    const fallbackCallback =
      callbackCandidate ||
      (flowState.phoneConfirmed && caller_id ? caller_id : "");
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
    if (!merged.callSid) merged.callSid = callSid || "";
    if (!merged.streamSid) merged.streamSid = twilioStreamSid || "";
    if (!merged.caller) merged.caller = caller || "";
    if (!merged.caller_id) merged.caller_id = caller_id || "";
    if (!merged.called) merged.called = called || "";
    if (!merged.started_at) merged.started_at = fallbackStartedAt;
    if (!merged.ended_at) merged.ended_at = fallbackEndedAt;
    if (!merged.route) merged.route = flowState.route || route || "";
    if (!merged.stage) merged.stage = flowState.stage || "";
    if (!merged.full_name) merged.full_name = flowState.data.full_name || "";
    if (!merged.callback_phone) merged.callback_phone = fallbackCallback || "";
    if (!merged.recording_url_public)
      merged.recording_url_public = makeRecordingPublicUrl(callSid);
    if (!merged.collected) merged.collected = flowState.data;
    return merged;
  };

  const buildNextInstructions = () => {
    const callerDigits = ensureCallerDigits();
    const spacedCaller = callerDigits ? formatSpacedDigits(callerDigits) : "";

    if (flowState.stage === "routing") {
      return buildFlowInstructions(
        getFlowTextOrFallback(
          "ROUTING_PROMPT",
          getFlowTextOrFallback("FLOW_ROUTING_CLARIFY", FALLBACK_ROUTING_CLARIFY)
        )
      );
    }
    if (flowState.stage === "routing_clarify") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_ROUTING_CLARIFY", FALLBACK_ROUTING_CLARIFY));
    }

    // SALES
    if (flowState.stage === "sales_product") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SALES_PRODUCT", ""));
    }
    if (flowState.stage === "sales_name") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SALES_NAME", ""));
    }
    if (flowState.stage === "sales_model_want") {
      const { priceClaim, couponDigitsSpaced } = getPriceClaimAndCouponFromSheets();
      const couponLine = couponDigitsSpaced ? `קוד הקופון לרכישה באתר הוא ${couponDigitsSpaced}.` : "";
      const tmpl = getFlowTextOrFallback(
        "FLOW_SALES_MODEL_WANT",
        "{priceClaim} {couponLine} האם יש דגם ספציפי?"
      );
      const text = renderFlowText(tmpl, { priceClaim, couponLine });
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "sales_model_collect") {
      const tmpl = getFlowTextOrFallback("FLOW_SALES_MODEL_COLLECT", "אם כן—מה הדגם?");
      return buildFlowInstructions(tmpl);
    }
    if (flowState.stage === "sales_brand_want") {
      const tmpl = getFlowTextOrFallback("FLOW_SALES_BRAND_WANT", "האם יש מותג ספציפי?");
      return buildFlowInstructions(tmpl);
    }
    if (flowState.stage === "sales_brand_collect") {
      const tmpl = getFlowTextOrFallback("FLOW_SALES_BRAND_COLLECT", "אם כן—מה שם המותג?");
      return buildFlowInstructions(tmpl);
    }
    if (flowState.stage === "sales_phone_confirm") {
      const text = spacedCaller
        ? renderFlowText(getFlowTextOrFallback("FLOW_SALES_PHONE_CONFIRM", ""), { caller_id: spacedCaller })
        : getFlowTextOrFallback("FLOW_SALES_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "sales_phone_collect") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SALES_PHONE_COLLECT", ""));
    }
    if (flowState.stage === "sales_phone_confirm_new") {
      const spaced = isValidPhoneDigits(flowState.data.callback_phone)
        ? formatSpacedDigits(flowState.data.callback_phone)
        : "";
      const text = spaced
        ? renderFlowText(getFlowTextOrFallback("FLOW_SALES_PHONE_CONFIRM_NEW", ""), { number: spaced })
        : getFlowTextOrFallback("FLOW_SALES_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "sales_done") {
      flowState.finalEvent = "מתעניין במכירות";
      flowState.finalSummary = "מתעניין במכירות";
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SALES_DONE", ""));
    }

    // SUPPORT
    if (flowState.stage === "support_issue_desc") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SUPPORT_ISSUE_DESC", ""));
    }
    if (flowState.stage === "support_model_collect") {
      const tmpl = getFlowTextOrFallback("FLOW_SUPPORT_MODEL_COLLECT", "כדי שאעביר לשירות בצורה מדויקת—מה הדגם?");
      return buildFlowInstructions(tmpl);
    }
    if (flowState.stage === "support_brand_collect") {
      const tmpl = getFlowTextOrFallback("FLOW_SUPPORT_BRAND_COLLECT", "והאם יש מותג? אם ידוע, מה שם המותג?");
      return buildFlowInstructions(tmpl);
    }
    if (flowState.stage === "support_importer_offer") {
      const importer = flowState.data._importerMatch || null;
      const tmpl = getFlowTextOrFallback(
        "FLOW_SUPPORT_IMPORTER_OFFER",
        "יש לנו מספר ישיר ליבואן. רוצים שאמסור אותו?"
      );
      return buildFlowInstructions(tmpl, importer ? [`מותג: ${importer.brand}`] : []);
    }
    if (flowState.stage === "support_importer_give") {
      const importer = flowState.data._importerMatch || null;
      const num = importer && importer.phone ? formatSpacedDigits(normalizePhoneDigits(importer.phone)) : "";
      const tmpl = getFlowTextOrFallback(
        "FLOW_SUPPORT_IMPORTER_GIVE",
        "המספר הישיר הוא: {number}."
      );
      const text = renderFlowText(tmpl, { number: num });
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "support_name") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SUPPORT_NAME", ""));
    }
    if (flowState.stage === "support_phone_confirm") {
      const text = spacedCaller
        ? renderFlowText(getFlowTextOrFallback("FLOW_SUPPORT_PHONE_CONFIRM", ""), { caller_id: spacedCaller })
        : getFlowTextOrFallback("FLOW_SUPPORT_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "support_phone_collect") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SUPPORT_PHONE_COLLECT", ""));
    }
    if (flowState.stage === "support_phone_confirm_new") {
      const spaced = isValidPhoneDigits(flowState.data.callback_phone)
        ? formatSpacedDigits(flowState.data.callback_phone)
        : "";
      const text = spaced
        ? renderFlowText(getFlowTextOrFallback("FLOW_SUPPORT_PHONE_CONFIRM_NEW", ""), { number: spaced })
        : getFlowTextOrFallback("FLOW_SUPPORT_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "support_done") {
      flowState.finalEvent = "שירות לקוחות תקלה";
      flowState.finalSummary = "שירות לקוחות תקלה";
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_SUPPORT_DONE", ""));
    }

    // DELIVERY
    if (flowState.stage === "delivery_carrier_offer") {
      const carriers = buildCarrierList();
      const text = buildCarrierOfferTextFromSheets(carriers);
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "delivery_desc") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_DELIVERY_DESC", ""));
    }
    if (flowState.stage === "delivery_name") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_DELIVERY_NAME", ""));
    }
    if (flowState.stage === "delivery_phone_confirm") {
      const text = spacedCaller
        ? renderFlowText(getFlowTextOrFallback("FLOW_DELIVERY_PHONE_CONFIRM", ""), { caller_id: spacedCaller })
        : getFlowTextOrFallback("FLOW_DELIVERY_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "delivery_phone_collect") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_DELIVERY_PHONE_COLLECT", ""));
    }
    if (flowState.stage === "delivery_phone_confirm_new") {
      const spaced = isValidPhoneDigits(flowState.data.callback_phone)
        ? formatSpacedDigits(flowState.data.callback_phone)
        : "";
      const text = spaced
        ? renderFlowText(getFlowTextOrFallback("FLOW_DELIVERY_PHONE_CONFIRM_NEW", ""), { number: spaced })
        : getFlowTextOrFallback("FLOW_DELIVERY_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "delivery_done") {
      flowState.finalEvent = "אספקה / משלוח";
      flowState.finalSummary = "אספקה / משלוח";
      flowState.shouldHangup = true;
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_DELIVERY_DONE", ""));
    }

    // MESSAGE
    if (flowState.stage === "message_target") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_MESSAGE_TARGET", ""));
    }
    if (flowState.stage === "message_body") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_MESSAGE_BODY", ""));
    }
    if (flowState.stage === "message_name") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_MESSAGE_NAME", ""));
    }
    if (flowState.stage === "message_phone_confirm") {
      const text = spacedCaller
        ? renderFlowText(getFlowTextOrFallback("FLOW_MESSAGE_PHONE_CONFIRM", ""), { caller_id: spacedCaller })
        : getFlowTextOrFallback("FLOW_MESSAGE_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "message_phone_collect") {
      return buildFlowInstructions(getFlowTextOrFallback("FLOW_MESSAGE_PHONE_COLLECT", ""));
    }
    if (flowState.stage === "message_phone_confirm_new") {
      const spaced = isValidPhoneDigits(flowState.data.callback_phone)
        ? formatSpacedDigits(flowState.data.callback_phone)
        : "";
      const text = spaced
        ? renderFlowText(getFlowTextOrFallback("FLOW_MESSAGE_PHONE_CONFIRM_NEW", ""), { number: spaced })
        : getFlowTextOrFallback("FLOW_MESSAGE_PHONE_COLLECT", "");
      return buildFlowInstructions(text);
    }
    if (flowState.stage === "message_done") {
      flowState.finalEvent = `הודעה עבור – ${flowState.data.message_target || ""}`.trim();
      flowState.finalSummary = flowState.finalEvent;
      flowState.shouldHangup = true;
      return buildFlowInstructions(
        renderFlowText(getFlowTextOrFallback("FLOW_MESSAGE_DONE", ""), {
          target: flowState.data.message_target || "הצוות"
        })
      );
    }

    return buildFlowInstructions(getFlowTextOrFallback("FLOW_NAME_INVALID", FALLBACK_EMPTY_INSTRUCTIONS));
  };

  const processCallerUtterance = (utterance) => {
    const text = String(utterance || "").trim();
    if (!text) return "";

    if (isExplicitNamePhrase(text)) {
      const nameCandidate = extractNameCandidate(text);
      if (nameCandidate) flowState.data.full_name = nameCandidate;
    }

    const maybePhone = extractPhoneCandidates(text);
    if (maybePhone && !recognizedPhones.includes(maybePhone)) recognizedPhones.push(maybePhone);

    if (flowState.stage === "routing" || flowState.stage === "routing_clarify") {
      const routeCandidate = extractRoute(text) || "";
      if (routeCandidate) {
        flowState.route = routeCandidate;
        route = routeCandidate;

        if (routeCandidate === "message" && !flowState.data.message_target) {
          const t = detectMessageTargetInText(text);
          if (t) flowState.data.message_target = t;
        }

        if (routeCandidate === "delivery") {
          flowState.afterHours = isAfterHours();
          flowState.data.after_hours = Boolean(flowState.afterHours);
          if (flowState.afterHours && findDeliverySameDayMatch(text)) {
            flowState.data._deliverySameDayAfterHours = true;
          }
        }

        if (routeCandidate === "sales") flowState.stage = "sales_product";
        else if (routeCandidate === "support") flowState.stage = "support_issue_desc";
        else if (routeCandidate === "delivery") {
          flowState.stage = flowState.data._deliverySameDayAfterHours ? "delivery_carrier_offer" : "delivery_desc";
        } else flowState.stage = "message_target";

        return buildNextInstructions();
      }

      if (!flowState.askedRouting) {
        flowState.askedRouting = true;
        flowState.stage = "routing_clarify";
        return buildNextInstructions();
      }

      flowState.route = "message";
      route = "message";
      flowState.stage = "message_target";
      return buildNextInstructions();
    }

    // SALES
    if (flowState.route === "sales") {
      if (flowState.stage === "sales_product") {
        flowState.data.product_type = text;
        const bm = extractBrandModel(text);
        if (bm.brand) flowState.data.product_brand = bm.brand;
        if (bm.model) flowState.data.product_model = bm.model;
        flowState.stage = "sales_name";
        return buildNextInstructions();
      }
      if (flowState.stage === "sales_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_NAME_INVALID", FALLBACK_NAME_INVALID));
        }
        flowState.data.full_name = nameCandidate;
        if (typeof flowState.data.sales_model_known === "undefined") {
          flowState.data.sales_model_known = null;
        }
        flowState.stage = "sales_model_want";
        return buildNextInstructions();
      }
      if (flowState.stage === "sales_model_want") {
        if (isYes(text)) {
          flowState.data.sales_model_known = true;
          flowState.stage = "sales_model_collect";
          return buildNextInstructions();
        }
        if (isNo(text)) {
          flowState.data.sales_model_known = false;
          flowState.stage = "sales_brand_want";
          return buildNextInstructions();
        }
        const maybe = text;
        if (maybe && maybe.length >= 2) {
          flowState.data.sales_model_known = true;
          flowState.data.product_model = maybe;
          flowState.stage = "sales_brand_want";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }
      if (flowState.stage === "sales_model_collect") {
        flowState.data.product_model = text;
        flowState.stage = "sales_brand_want";
        return buildNextInstructions();
      }
      if (flowState.stage === "sales_brand_want") {
        if (isYes(text)) {
          flowState.data.sales_brand_known = true;
          flowState.stage = "sales_brand_collect";
          return buildNextInstructions();
        }
        if (isNo(text)) {
          flowState.data.sales_brand_known = false;
          flowState.stage = "sales_phone_confirm";
          return buildNextInstructions();
        }
        const maybe = text;
        if (maybe && maybe.length >= 2) {
          flowState.data.sales_brand_known = true;
          flowState.data.product_brand = maybe;
          flowState.stage = "sales_phone_confirm";
          return buildNextInstructions();
        }
        return buildNextInstructions();
      }
      if (flowState.stage === "sales_brand_collect") {
        flowState.data.product_brand = text;
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
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_PHONE_MISSING_DIGIT", FALLBACK_PHONE_MISSING_DIGIT));
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

    // SUPPORT
    if (flowState.route === "support") {
      if (flowState.stage === "support_issue_desc") {
        flowState.data.issue_desc = text;
        if (!flowState.data.issue_topic) flowState.data.issue_topic = extractIssueTopic(text);
        flowState.stage = "support_model_collect";
        return buildNextInstructions();
      }
      if (flowState.stage === "support_model_collect") {
        flowState.data.product_model = text;
        flowState.stage = "support_brand_collect";
        return buildNextInstructions();
      }
      if (flowState.stage === "support_brand_collect") {
        if (!isNo(text) && text.length >= 2) {
          flowState.data.product_brand = text;
        }
        const importer = findExactImporter(flowState.data.product_brand);
        if (importer && importer.phone) {
          flowState.data._importerMatch = importer;
          flowState.data._importerOffered = true;
          flowState.stage = "support_importer_offer";
          return buildNextInstructions();
        }
        flowState.stage = "support_name";
        return buildNextInstructions();
      }
      if (flowState.stage === "support_importer_offer") {
        if (isYes(text)) {
          flowState.stage = "support_importer_give";
          return buildNextInstructions();
        }
        flowState.stage = "support_name";
        return buildNextInstructions();
      }
      if (flowState.stage === "support_importer_give") {
        flowState.stage = "support_name";
        return buildNextInstructions();
      }
      if (flowState.stage === "support_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_NAME_INVALID", FALLBACK_NAME_INVALID));
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
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_PHONE_MISSING_DIGIT", FALLBACK_PHONE_MISSING_DIGIT));
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

    // DELIVERY
    if (flowState.route === "delivery") {
      if (flowState.stage === "delivery_carrier_offer") {
        flowState.data.carriers_info_offered = true;
        flowState.data.carriers_info_given = isYes(text);
        flowState.stage = "delivery_desc";
        return buildNextInstructions();
      }
      if (flowState.stage === "delivery_desc") {
        flowState.data.delivery_desc = text;
        if (!flowState.data.delivery_topic) flowState.data.delivery_topic = extractDeliveryTopic(text);
        flowState.afterHours = isAfterHours();
        flowState.data.after_hours = Boolean(flowState.afterHours);
        if (flowState.afterHours && findDeliverySameDayMatch(text)) {
          flowState.data._deliverySameDayAfterHours = true;
        }
        flowState.stage = "delivery_name";
        return buildNextInstructions();
      }
      if (flowState.stage === "delivery_name") {
        const nameCandidate = extractNameCandidate(text);
        if (!nameCandidate) {
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_NAME_INVALID", FALLBACK_NAME_INVALID));
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
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_PHONE_MISSING_DIGIT", FALLBACK_PHONE_MISSING_DIGIT));
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

    // MESSAGE
    if (flowState.route === "message") {
      if (flowState.stage === "message_target") {
        if (!flowState.data.message_target) flowState.data.message_target = text;
        flowState.stage = "message_body";
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
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_NAME_INVALID", FALLBACK_NAME_INVALID));
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
          return buildFlowInstructions(getFlowTextOrFallback("FLOW_PHONE_MISSING_DIGIT", FALLBACK_PHONE_MISSING_DIGIT));
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
        "hi",
        "hello",
        "bye",
        "bye-bye",
        "bye bye",
        "bye, bye"
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
        instructions = buildFlowInstructions(FALLBACK_EMPTY_INSTRUCTIONS);
      }
    }

    if (!String(instructions || "").trim()) {
      instructions = buildFlowInstructions(FALLBACK_EMPTY_INSTRUCTIONS);
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
    flowState.stageAdvanced = false;
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

    if (!SHEETS.loaded_at) {
      debug(`[${connTag}] Sheets not loaded yet. Loading now...`);
      await loadSheets();
    }

    if (
      (!SHEETS.loaded_at ||
        (!Object.keys(SHEETS.prompts || {}).length &&
          !Object.keys(SHEETS.settings || {}).length)) &&
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
    const openingFromSheet = Boolean(
      String((SHEETS.settings || {}).OPENING_SCRIPT || "").trim()
    );
    const masterFromSheet = Boolean(
      String((SHEETS.prompts || {}).MASTER_PROMPT || "").trim()
    );

    always(`[${connTag}] SOURCES`, {
      sheets_loaded_at: SHEETS.loaded_at,
      opening_from: openingFromSheet ? "SETTINGS.OPENING_SCRIPT" : "FALLBACK.DEFAULT",
      master_from: masterFromSheet ? "PROMPTS.MASTER_PROMPT" : "FALLBACK.DEFAULT",
      opening_preview: preview(openingScript, 220),
      master_preview: preview(masterPrompt, 220)
    });

    // IMPORTANT FIX:
    // Realtime session.update does NOT accept session.voice_style / session.speaking_rate (unknown_parameter).
    // We intentionally do not send them, and we also defensively delete them if present.
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

    // Defensive strip in case any future merge adds these fields:
    if (Object.prototype.hasOwnProperty.call(session, "voice_style")) delete session.voice_style;
    if (Object.prototype.hasOwnProperty.call(session, "speaking_rate")) delete session.speaking_rate;

    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    safeOpenAISend({ type: "session.update", session });

    // Opening line verbatim
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
          if (lastRequestedCallerNormalized) {
            if (normalized === lastRequestedCallerNormalized) {
              isDup = true;
            } else if (normalized.startsWith(lastRequestedCallerNormalized)) {
              isDup = true;
            } else if (lastRequestedCallerNormalized.startsWith(normalized)) {
              isDup = true;
            }
          }

          const allowShortReplyStages = new Set([
            "sales_product",
            "sales_name",
            "sales_phone_confirm",
            "sales_phone_confirm_new",
            "support_issue_desc",
            "support_product",
            "support_name",
            "support_phone_confirm",
            "support_phone_confirm_new",
            "delivery_desc",
            "delivery_name",
            "delivery_phone_confirm",
            "delivery_phone_confirm_new",
            "message_target",
            "message_name",
            "message_body",
            "message_phone_confirm",
            "message_phone_confirm_new"
          ]);
          const allowShortReply =
            flowState && flowState.stage && allowShortReplyStages.has(String(flowState.stage));
          const hasPhone = Boolean(extractPhoneCandidates(normalized));
          const meaningfulShort =
            isYes(normalized) || isNo(normalized) || hasPhone || (normalized && !isFillerOnly(normalized));

          if (!isDup) {
            const shouldRespond =
              (allowShortReply && meaningfulShort) || (!allowShortReply && (wordCount >= 5 || hasKeyword));
            if (shouldRespond) {
              if (awaitingResponse) {
                safeOpenAISend({ type: "response.cancel" });
                awaitingResponse = false;
              }
              pendingResponseRequest = false;
              requestAssistantResponse("caller_final");
            }
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

      if (
        flowState.shouldHangup &&
        flowState.finalEvent &&
        !sentCallEnded &&
        !flowState.finalPayloadSent
      ) {
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

      if (!sentCallEnded && !flowState.finalPayloadSent) {
        if (!flowState.stage.endsWith("_done")) {
          sentCallEnded = true;
          if (!hangupRequested) {
            hangupRequested = true;
            completeTwilioCall(callSid);
          }
          try {
            if (openaiWs) openaiWs.close();
          } catch (_) {}
          return;
        }
        sentCallEnded = true;
        const canSendFinal = Boolean(flowState.finalEvent);
        const fallbackEvent =
          route === "sales"
            ? "sales_lead"
            : route === "support"
            ? "support_ticket"
            : route === "delivery"
            ? "delivery_ticket"
            : route === "message"
            ? "message_taken"
            : "call_ended";
        const finalEvent = canSendFinal ? flowState.finalEvent : fallbackEvent;
        const payload = applyWebhookDefaults(
          canSendFinal
            ? buildFinalPayload()
            : {
                callSid,
                streamSid: twilioStreamSid,
                caller,
                called,
                started_at: startedAt,
                ended_at: endedAt,
                language,
                route,
                stage: flowState.stage,
                caller_last_utterance: lastCallerFinal,
                bot_last_utterance: lastBotFinal,
                transcript: transcriptTurns,
                recording_url_public: makeRecordingPublicUrl(callSid),
                collected: flowState.data
              }
        );
        if (canSendFinal) {
          flowState.finalPayloadSent = true;
        }
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
    if (!sentCallEnded && !sentCallAbandoned) {
      sentCallAbandoned = true;
      endedAt = endedAt || nowIso();
      const recording_url_public = makeRecordingPublicUrl(callSid);
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
          route,
          stage: flowState.stage,
          caller_last_utterance: lastCallerFinal,
          bot_last_utterance: lastBotFinal,
          transcript: transcriptTurns,
          collected: flowState.data,
          recording_url_public
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
