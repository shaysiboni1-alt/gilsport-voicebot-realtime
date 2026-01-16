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
const MB_RECORDING_WAIT_MS = envNum("MB_RECORDING_WAIT_MS", 25000);
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
const getText = (key) =>
  (SHEETS.settings && SHEETS.settings[key]) || (SHEETS.prompts && SHEETS.prompts[key]) || null;

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
  let hangupRequested = false;

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

  const attemptHangup = async (sid) => {
    if (!sid) return false;
    always(`[HANGUP] attempt callSid=${sid}`);
    const ok = await completeTwilioCall(sid);
    if (ok) {
      always(`[HANGUP] success callSid=${sid}`);
    } else {
      error(`[HANGUP] error callSid=${sid}`);
    }
    return ok;
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

  const isValidBrandModelValue = (value) => {
    const t = String(value || "").trim().toLowerCase();
    if (!t) return false;
    const invalid = new Set(["שלביא אותו", "הוא", "כן", "לא יודע"]);
    if (invalid.has(t)) return false;
    if (t.length < 2) return false;
    return true;
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

  const parseHoursRange = (s) => {
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

  const parseBusinessHours = (s) => {
    const raw = String(s || "");
    if (!raw) return { weekday: null, friday: null };
    const parts = raw
      .split("|")
      .map((p) => p.trim())
      .filter(Boolean);
    const weekday = parseHoursRange(parts[0] || "");
    const friday = parseHoursRange(parts[1] || "") || weekday;
    return { weekday, friday };
  };

  const isAfterHours = () => {
    const hoursStr =
      getSetting("BUSINESS_HOURS", "") ||
      getSetting("HOURS", "") ||
      getSetting("WORKING_HOURS", "") ||
      "";
    const parsed = parseBusinessHours(hoursStr);
    if (!parsed.weekday) return false; // if unknown, do not force after-hours
    // Use local time in TIME_ZONE
    const now = new Date();
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone: TIME_ZONE,
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
      weekday: "short"
    }).formatToParts(now);
    const hh = Number(parts.find((p) => p.type === "hour")?.value || 0);
    const mm = Number(parts.find((p) => p.type === "minute")?.value || 0);
    const weekday = String(parts.find((p) => p.type === "weekday")?.value || "").toLowerCase();
    const cur = hh * 60 + mm;
    if (weekday.startsWith("sat")) return true;
    const useRange = weekday.startsWith("fri") ? parsed.friday : parsed.weekday;
    if (!useRange) return false;
    return cur < useRange.start || cur > useRange.end;
  };

  const buildFlowInstructions = (sayText, strict = true) => {
    const baseText = String(sayText || "").trim() || FALLBACK_EMPTY_INSTRUCTIONS;
    const prefix = strict
      ? "החזירי אך ורק את המשפט הבא. אסור להוסיף אפילו מילה אחת:\n"
      : "החזירי בדיוק את הטקסט הבא:\n";
    return `${prefix}${baseText}`;
  };

  const getFlowText = (key) => String(getText(key) || "").trim();
  const getFlowTextOrFallback = (key, fallback) => getFlowText(key) || fallback;

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

  const normalizeSayText = (text) => {
    const t = String(text || "")
      .replace(/[\u2013\u2014\-–—.,!?;:"'()\[\]{}]/g, " ")
      .replace(/\s+/g, " ")
      .trim()
      .toLowerCase();
    const withoutNiqq = t.normalize("NFD").replace(/[\u0591-\u05C7]/g, "");
    return withoutNiqq.replace(/[^\p{L}\p{N}\s]/gu, " ").replace(/\s+/g, " ").trim();
  };

  const isIgnorableUtterance = (text) => {
    const t = normalizeSayText(text);
    if (!t) return false;
    const ignorable = new Set([
      "bye",
      "goodbye",
      "thank you",
      "thanks",
      "ok",
      "okay",
      "again",
      "you said it",
      "ביי",
      "תודה",
      "סבבה",
      "אוקיי",
      "אוקי",
      "בסדר",
      "לא משנה"
    ]);
    return ignorable.has(t);
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

  const getOpeningScript = () => getSetting("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");
  const getClosingScript = () => getFlowText("CLOSING_SCRIPT");

  const STAGES = {
    routing: {
      prompts: {
        default: {
          sayKey: "FLOW_ROUTING",
          fallback: "באיזה נושא אפשר לעזור?"
        }
      },
      buildSayText: ({ hasOpened }) => {
        const opening = hasOpened ? "" : getOpeningScript();
        const prompt = getFlowTextOrFallback("FLOW_ROUTING", "באיזה נושא אפשר לעזור?");
        return [opening, prompt].filter(Boolean).join(" ").trim();
      }
    },
    routing_clarify: {
      prompts: {
        default: {
          sayKey: "FLOW_ROUTING_CLARIFY",
          fallback: FALLBACK_ROUTING_CLARIFY
        }
      }
    },
    sales_product: {
      prompts: {
        default: {
          sayKey: "FLOW_SALES_PRODUCT",
          fallback: "בשמחה. על איזה מוצר אתם מתעניינים? סוג מוצר, ואם יש—דגם ושם מותג."
        }
      }
    },
    sales_name: {
      prompts: {
        default: {
          sayKey: "FLOW_SALES_NAME",
          fallback: "מעולה, תודה. כדי שנחזור אליכם—מה השם המלא שלכם?"
        },
        invalid_name: {
          sayKey: "FLOW_NAME_INVALID",
          fallback: FALLBACK_NAME_INVALID
        }
      }
    },
    sales_phone_confirm: {
      prompts: {
        default: {
          sayKey: "FLOW_SALES_PHONE_CONFIRM",
          fallback: "האם לחזור אליכם למספר הזה: {caller_id} ?"
        },
        collect: {
          sayKey: "FLOW_SALES_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callerDigits }) => {
        if (!callerDigits) {
          return getFlowTextOrFallback(
            "FLOW_SALES_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_SALES_PHONE_CONFIRM",
            "האם לחזור אליכם למספר הזה: {caller_id} ?"
          ),
          { caller_id: formatSpacedDigits(callerDigits) }
        );
      }
    },
    sales_phone_collect: {
      prompts: {
        default: {
          sayKey: "FLOW_SALES_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        },
        invalid_phone: {
          sayKey: "FLOW_PHONE_MISSING_DIGIT",
          fallback: FALLBACK_PHONE_MISSING_DIGIT
        }
      }
    },
    sales_phone_confirm_new: {
      prompts: {
        default: {
          sayKey: "FLOW_SALES_PHONE_CONFIRM_NEW",
          fallback: "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
        },
        collect: {
          sayKey: "FLOW_SALES_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callbackPhone }) => {
        if (!isValidPhoneDigits(callbackPhone)) {
          return getFlowTextOrFallback(
            "FLOW_SALES_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_SALES_PHONE_CONFIRM_NEW",
            "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
          ),
          { number: formatSpacedDigits(callbackPhone) }
        );
      }
    },
    sales_done: {
      prompts: {
        default: {
          sayKey: "FLOW_SALES_DONE",
          fallback:
            "מעולה. העברתי את הפרטים למחלקת המכירות, ויחזרו אליכם בהקדם. תודה רבה ויום טוב."
        }
      },
      finalEvent: "sales_lead",
      finalSummary: "התעניינות במוצר"
    },
    support_issue_desc: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_ISSUE_DESC",
          fallback:
            "כדי שאעביר לשירות בצורה מדויקת—מה סוג התקלה ומה מהות התקלה בכמה מילים?"
        }
      }
    },
    support_product: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_PRODUCT",
          fallback: "ועל איזה מוצר זה? דגם ושם מותג."
        }
      }
    },
    support_name: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_NAME",
          fallback: "מה השם המלא שלכם?"
        },
        invalid_name: {
          sayKey: "FLOW_NAME_INVALID",
          fallback: FALLBACK_NAME_INVALID
        }
      }
    },
    support_phone_confirm: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_PHONE_CONFIRM",
          fallback: "האם לחזור אליכם למספר הזה: {caller_id} ?"
        },
        collect: {
          sayKey: "FLOW_SUPPORT_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callerDigits }) => {
        if (!callerDigits) {
          return getFlowTextOrFallback(
            "FLOW_SUPPORT_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_SUPPORT_PHONE_CONFIRM",
            "האם לחזור אליכם למספר הזה: {caller_id} ?"
          ),
          { caller_id: formatSpacedDigits(callerDigits) }
        );
      }
    },
    support_phone_collect: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        },
        invalid_phone: {
          sayKey: "FLOW_PHONE_MISSING_DIGIT",
          fallback: FALLBACK_PHONE_MISSING_DIGIT
        }
      }
    },
    support_phone_confirm_new: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_PHONE_CONFIRM_NEW",
          fallback: "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
        },
        collect: {
          sayKey: "FLOW_SUPPORT_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callbackPhone }) => {
        if (!isValidPhoneDigits(callbackPhone)) {
          return getFlowTextOrFallback(
            "FLOW_SUPPORT_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_SUPPORT_PHONE_CONFIRM_NEW",
            "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
          ),
          { number: formatSpacedDigits(callbackPhone) }
        );
      }
    },
    support_done: {
      prompts: {
        default: {
          sayKey: "FLOW_SUPPORT_DONE",
          fallback:
            "מעולה. שלחתי את הפרטים למחלקת השירות, ויחזרו אליכם בהקדם. תודה רבה ויום טוב."
        }
      },
      finalEvent: "support_ticket",
      finalSummary: "פניית שירות/תקלה"
    },
    delivery_desc: {
      prompts: {
        default: {
          sayKey: "FLOW_DELIVERY_DESC",
          fallback: "מה הבקשה שלכם לגבי משלוח או אספקה? תארו בקצרה מה אתם צריכים."
        }
      }
    },
    delivery_name: {
      prompts: {
        default: {
          sayKey: "FLOW_DELIVERY_NAME",
          fallback: "כדי לטפל בפנייה לגבי משלוח, מה השם המלא שלכם?"
        },
        invalid_name: {
          sayKey: "FLOW_NAME_INVALID",
          fallback: FALLBACK_NAME_INVALID
        },
        intro: {
          sayKey: "FLOW_DELIVERY_INTRO",
          fallback: ""
        },
        after_hours: {
          sayKey: "FLOW_DELIVERY_AFTER_HOURS",
          fallback: "אלו מספרי המובילים: {carriers}"
        }
      },
      buildSayText: ({ afterHours, carriers }) => {
        const afterHoursText =
          afterHours && carriers.length
            ? renderFlowText(
                getFlowTextOrFallback(
                  "FLOW_DELIVERY_AFTER_HOURS",
                  "אלו מספרי המובילים: {carriers}"
                ),
                { carriers: carriers.join(", ") }
              )
            : "";
        const intro = getFlowTextOrFallback("FLOW_DELIVERY_INTRO", "");
        const askName = getFlowTextOrFallback(
          "FLOW_DELIVERY_NAME",
          "כדי לטפל בפנייה לגבי משלוח, מה השם המלא שלכם?"
        );
        return [afterHoursText, intro, askName].filter(Boolean).join(" ").trim();
      }
    },
    delivery_phone_confirm: {
      prompts: {
        default: {
          sayKey: "FLOW_DELIVERY_PHONE_CONFIRM",
          fallback: "האם לחזור אליכם למספר הזה: {caller_id} ?"
        },
        collect: {
          sayKey: "FLOW_DELIVERY_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callerDigits }) => {
        if (!callerDigits) {
          return getFlowTextOrFallback(
            "FLOW_DELIVERY_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_DELIVERY_PHONE_CONFIRM",
            "האם לחזור אליכם למספר הזה: {caller_id} ?"
          ),
          { caller_id: formatSpacedDigits(callerDigits) }
        );
      }
    },
    delivery_phone_collect: {
      prompts: {
        default: {
          sayKey: "FLOW_DELIVERY_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        },
        invalid_phone: {
          sayKey: "FLOW_PHONE_MISSING_DIGIT",
          fallback: FALLBACK_PHONE_MISSING_DIGIT
        }
      }
    },
    delivery_phone_confirm_new: {
      prompts: {
        default: {
          sayKey: "FLOW_DELIVERY_PHONE_CONFIRM_NEW",
          fallback: "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
        },
        collect: {
          sayKey: "FLOW_DELIVERY_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callbackPhone }) => {
        if (!isValidPhoneDigits(callbackPhone)) {
          return getFlowTextOrFallback(
            "FLOW_DELIVERY_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_DELIVERY_PHONE_CONFIRM_NEW",
            "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
          ),
          { number: formatSpacedDigits(callbackPhone) }
        );
      }
    },
    delivery_done: {
      prompts: {
        default: {
          sayKey: "FLOW_DELIVERY_DONE",
          fallback: "תודה. העברתי את הפרטים למחלקת אספקה, ויחזרו אליכם בהקדם. יום טוב."
        }
      },
      finalEvent: "delivery_ticket",
      finalSummary: "פניית אספקה/משלוח"
    },
    message_target: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_TARGET",
          fallback: "בשמחה. למי מיועדת ההודעה? (שם עובד/מנהל)"
        }
      }
    },
    message_name: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_NAME",
          fallback: "מה השם המלא שלכם?"
        },
        invalid_name: {
          sayKey: "FLOW_NAME_INVALID",
          fallback: FALLBACK_NAME_INVALID
        }
      }
    },
    message_body: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_BODY",
          fallback: "מה מהות ההודעה? תאמרו את זה בקצרה."
        }
      }
    },
    message_phone_confirm: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_PHONE_CONFIRM",
          fallback: "האם לחזור אליכם למספר הזה: {caller_id} ?"
        },
        collect: {
          sayKey: "FLOW_MESSAGE_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callerDigits }) => {
        if (!callerDigits) {
          return getFlowTextOrFallback(
            "FLOW_MESSAGE_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_MESSAGE_PHONE_CONFIRM",
            "האם לחזור אליכם למספר הזה: {caller_id} ?"
          ),
          { caller_id: formatSpacedDigits(callerDigits) }
        );
      }
    },
    message_phone_collect: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        },
        invalid_phone: {
          sayKey: "FLOW_PHONE_MISSING_DIGIT",
          fallback: FALLBACK_PHONE_MISSING_DIGIT
        }
      }
    },
    message_phone_confirm_new: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_PHONE_CONFIRM_NEW",
          fallback: "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
        },
        collect: {
          sayKey: "FLOW_MESSAGE_PHONE_COLLECT",
          fallback: "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
        }
      },
      buildSayText: ({ callbackPhone }) => {
        if (!isValidPhoneDigits(callbackPhone)) {
          return getFlowTextOrFallback(
            "FLOW_MESSAGE_PHONE_COLLECT",
            "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה."
          );
        }
        return renderFlowText(
          getFlowTextOrFallback(
            "FLOW_MESSAGE_PHONE_CONFIRM_NEW",
            "רק לוודא—המספר לחזרה הוא: {number}. נכון?"
          ),
          { number: formatSpacedDigits(callbackPhone) }
        );
      }
    },
    message_done: {
      prompts: {
        default: {
          sayKey: "FLOW_MESSAGE_DONE",
          fallback: "תודה. העברתי את ההודעה ל־{target} ויחזרו אליכם בהקדם. יום טוב."
        }
      },
      finalEvent: "message_taken",
      finalSummary: "הודעה ללקוח"
    }
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
    t = t.replace(/[.,!?;:"'(){}\[\]\-–—]/g, " ").replace(/\s+/g, " ").trim();
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
    const lowerClean = t.toLowerCase();
    if (filler.includes(lowerClean)) return "";
    const parts = t.split(" ").filter(Boolean);
    if (parts.length > 5) return "";
    if (!/[A-Za-z\u0590-\u05FF]/.test(t)) return "";
    return t;
  };

  const extractMessageTarget = (text) => {
    let t = String(text || "").trim();
    if (!t) return "";
    t = t.replace(/[.,!?;:"'(){}\[\]\-–—]/g, " ").replace(/\s+/g, " ").trim();
    if (!t) return "";
    if (t.startsWith("ל")) {
      t = t.slice(1).trim();
    }
    const parts = t.split(/\s+/).filter(Boolean);
    return parts[0] ? parts[0].trim() : "";
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
    route: "other",
    afterHours: false,
    collected,
    data: collected,
    finalEvent: "",
    finalSummary: "",
    finalPayload: null,
    shouldHangup: false,
    phoneConfirmed: false,
    finalPayloadSent: false,
    doneLocked: false,
    expectedSayText: "",
    expectedSayNormalized: "",
    expectedSayId: "",
    expectedSayAttempts: 0,
    guardFailed: false,
    hasOpened: false,
    pendingFinalWebhook: false,
    webhookIds: new Set()
  };

  const ensureCallerDigits = () => {
    const callerRaw = String(caller || "").trim();
    if (!callerRaw) return "";
    const digits = normalizePhoneDigits(callerRaw);
    return isValidPhoneDigits(digits) ? digits : "";
  };

  const isRouteComplete = (routeName, data, phoneConfirmed) => {
    const hasText = (v) => String(v || "").trim() !== "";
    const hasPhone = phoneConfirmed && isValidPhoneDigits(data.callback_phone);
    if (routeName === "sales") {
      return hasText(data.product_type) && hasText(data.full_name) && hasPhone;
    }
    if (routeName === "support") {
      return (
        hasText(data.issue_desc) &&
        hasText(data.product_brand) &&
        hasText(data.product_model) &&
        hasText(data.full_name) &&
        hasPhone
      );
    }
    if (routeName === "delivery") {
      return hasText(data.delivery_desc) && hasText(data.full_name) && hasPhone;
    }
    if (routeName === "message") {
      return (
        hasText(data.message_target) &&
        hasText(data.message_body) &&
        hasText(data.full_name) &&
        hasPhone
      );
    }
    return false;
  };

  const getNextMissingStage = (routeName, data, phoneConfirmed) => {
    const hasText = (v) => String(v || "").trim() !== "";
    const callerDigits = ensureCallerDigits();
    const getPhoneStage = () => {
      if (phoneConfirmed) return "";
      if (callerDigits) return `${routeName}_phone_confirm`;
      if (isValidPhoneDigits(data.callback_phone)) return `${routeName}_phone_confirm_new`;
      return `${routeName}_phone_collect`;
    };
    if (routeName === "sales") {
      if (!hasText(data.product_type)) return "sales_product";
      if (!hasText(data.full_name)) return "sales_name";
      return getPhoneStage();
    }
    if (routeName === "support") {
      if (!hasText(data.issue_desc)) return "support_issue_desc";
      if (!hasText(data.product_brand) || !hasText(data.product_model)) return "support_product";
      if (!hasText(data.full_name)) return "support_name";
      return getPhoneStage();
    }
    if (routeName === "delivery") {
      if (!hasText(data.delivery_desc)) return "delivery_desc";
      if (!hasText(data.full_name)) return "delivery_name";
      return getPhoneStage();
    }
    if (routeName === "message") {
      if (!hasText(data.message_target)) return "message_target";
      if (!hasText(data.full_name)) return "message_name";
      if (!hasText(data.message_body)) return "message_body";
      return getPhoneStage();
    }
    return "routing";
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

  const sendWebhookOnce = async (event, payload, opts = {}) => {
    if (!event) return false;
    const idKey = `${payload?.callSid || callSid || ""}:${event}`;
    if (flowState.webhookIds.has(idKey)) return false;
    flowState.webhookIds.add(idKey);
    return sendWebhookEvent(event, payload, opts);
  };

  const logStageChange = (prevStage, nextStage, reason) => {
    always(`[FSM] route=${flowState.route} stage=${prevStage} -> next=${nextStage} reason=${reason}`);
  };

  const logSay = (sayKey, sayText) => {
    always(`[FSM] sayKey=${sayKey || "unknown"} sayLen=${String(sayText || "").length} sayPreview=${preview(sayText, 160)}`);
  };

  const logCollected = () => {
    always(`[FSM] collected=${preview(JSON.stringify(flowState.data || {}), 200)}`);
  };

  const getStagePrompt = (stage, promptType = "default", options = {}) => {
    const config = STAGES[stage];
    if (!config) {
      return {
        sayKey: "FLOW_FALLBACK",
        sayText: getFlowTextOrFallback("FLOW_FALLBACK", FALLBACK_EMPTY_INSTRUCTIONS)
      };
    }
    if (promptType === "default" && typeof config.buildSayText === "function") {
      return {
        sayKey: config.prompts?.default?.sayKey || "",
        sayText: config.buildSayText(options)
      };
    }
    const prompt = (config.prompts && config.prompts[promptType]) || config.prompts?.default;
    const sayKey = prompt?.sayKey || "";
    const fallback = prompt?.fallback || FALLBACK_EMPTY_INSTRUCTIONS;
    const template = getFlowTextOrFallback(sayKey, fallback);
    const sayText = renderFlowText(template, options.vars || {});
    return { sayKey, sayText };
  };

  const composeDoneSayText = (stage) => {
    const config = STAGES[stage];
    const closing = getClosingScript();
    if (!config) {
      return getFlowTextOrFallback("FLOW_FALLBACK", FALLBACK_EMPTY_INSTRUCTIONS);
    }
    if (stage === "message_done") {
      const target = flowState.data.message_target || "הצוות";
      const base = renderFlowText(
        getFlowTextOrFallback(
          config.prompts.default.sayKey,
          config.prompts.default.fallback
        ),
        { target }
      );
      return [base, closing].filter(Boolean).join(" ").trim();
    }
    if (stage === "support_done") {
      const base = getFlowTextOrFallback(
        config.prompts.default.sayKey,
        config.prompts.default.fallback
      );
      const importer = findExactImporter(flowState.data.product_brand);
      const extra = [];
      if (importer && importer.phone) {
        const spaced = formatSpacedDigits(normalizePhoneDigits(importer.phone));
        const supplierText = renderFlowText(getFlowText("FLOW_SUPPORT_SUPPLIER_OPTIONAL"), {
          brand: importer.brand,
          number: spaced
        });
        if (supplierText) extra.push(supplierText);
      }
      return [base, ...extra, closing].filter(Boolean).join(" ").trim();
    }
    const base = getFlowTextOrFallback(
      config.prompts.default.sayKey,
      config.prompts.default.fallback
    );
    return [base, closing].filter(Boolean).join(" ").trim();
  };

  const setStage = (nextStage, reason) => {
    const prevStage = flowState.stage;
    flowState.stage = nextStage;
    logStageChange(prevStage, nextStage, reason);
    if (String(nextStage || "").endsWith("_done")) {
      flowState.doneLocked = true;
      flowState.pendingFinalWebhook = true;
      flowState.shouldHangup = true;
      flowState.finalEvent = STAGES[nextStage]?.finalEvent || "";
      if (nextStage === "message_done") {
        flowState.finalSummary = `הודעה עבור ${flowState.data.message_target || "הצוות"}`;
      } else {
        flowState.finalSummary = STAGES[nextStage]?.finalSummary || "";
      }
      always(
        `[FSM] done_enter stage=${nextStage} finalEvent=${flowState.finalEvent || "missing"}`
      );
    }
  };

  const handleRouting = (utterance) => {
    if (flowState.route !== "other") {
      return { nextStage: flowState.stage, promptType: "default", reason: "routing_locked" };
    }
    const routeCandidate = extractRoute(utterance);
    if (routeCandidate) {
      flowState.route = routeCandidate;
      const nextStage =
        routeCandidate === "sales"
          ? "sales_product"
          : routeCandidate === "support"
          ? "support_issue_desc"
          : routeCandidate === "delivery"
          ? "delivery_desc"
          : "message_target";
      route = flowState.route;
      return { nextStage, promptType: "default", reason: "routing_match" };
    }
    return { nextStage: "routing_clarify", promptType: "default", reason: "routing_clarify" };
  };

  const processCallerUtterance = (utterance) => {
    const text = String(utterance || "").trim();
    if (!text) return { nextStage: flowState.stage, promptType: "default", reason: "empty" };
    const inlinePhone = extractPhoneCandidates(text);
    if (inlinePhone && !flowState.data.callback_phone) {
      flowState.data.callback_phone = inlinePhone;
    }
    if (isExplicitNamePhrase(text)) {
      const nameCandidate = extractNameCandidate(text);
      if (nameCandidate) {
        flowState.data.full_name = nameCandidate;
      }
    }

    if (flowState.stage === "routing" || flowState.stage === "routing_clarify") {
      return handleRouting(text);
    }

    if (flowState.stage === "sales_product") {
      flowState.data.product_type = text;
      const { brand, model } = extractBrandModel(text);
      if (brand && isValidBrandModelValue(brand)) flowState.data.product_brand = brand;
      if (model && isValidBrandModelValue(model)) flowState.data.product_model = model;
      return { nextStage: "sales_name", promptType: "default", reason: "sales_product" };
    }
    if (flowState.stage === "sales_name") {
      const nameCandidate = extractNameCandidate(text);
      if (!nameCandidate) {
        return { nextStage: "sales_name", promptType: "invalid_name", reason: "name_invalid" };
      }
      flowState.data.full_name = nameCandidate;
      const callerDigits = ensureCallerDigits();
      if (callerDigits) {
        return { nextStage: "sales_phone_confirm", promptType: "default", reason: "phone_confirm" };
      }
      if (isValidPhoneDigits(flowState.data.callback_phone)) {
        return {
          nextStage: "sales_phone_confirm_new",
          promptType: "default",
          reason: "phone_confirm_new"
        };
      }
      return { nextStage: "sales_phone_collect", promptType: "default", reason: "phone_collect" };
    }
    if (flowState.stage === "sales_phone_confirm") {
      const callerDigits = ensureCallerDigits();
      if (isYes(text) && callerDigits) {
        flowState.data.callback_phone = callerDigits;
        flowState.phoneConfirmed = true;
        return { nextStage: "sales_done", promptType: "default", reason: "phone_confirm_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "sales_phone_collect", promptType: "default", reason: "phone_confirm_no" };
    }
    if (flowState.stage === "sales_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return {
          nextStage: "sales_phone_collect",
          promptType: "invalid_phone",
          reason: "phone_invalid"
        };
      }
      flowState.data.callback_phone = digits;
      flowState.phoneConfirmed = false;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      return { nextStage: "sales_phone_confirm_new", promptType: "default", reason: "phone_collect" };
    }
    if (flowState.stage === "sales_phone_confirm_new") {
      if (isYes(text) && isValidPhoneDigits(flowState.data.callback_phone)) {
        flowState.phoneConfirmed = true;
        return { nextStage: "sales_done", promptType: "default", reason: "phone_confirm_new_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "sales_phone_collect", promptType: "default", reason: "phone_confirm_new_no" };
    }
    if (flowState.stage === "support_issue_desc") {
      flowState.data.issue_desc = text;
      if (!flowState.data.issue_topic) {
        flowState.data.issue_topic = extractIssueTopic(text);
      }
      const { brand, model } = extractBrandModel(text);
      if (brand && isValidBrandModelValue(brand)) flowState.data.product_brand = brand;
      if (model && isValidBrandModelValue(model)) flowState.data.product_model = model;
      if (flowState.data.product_brand && flowState.data.product_model) {
        return { nextStage: "support_name", promptType: "default", reason: "support_has_product" };
      }
      return { nextStage: "support_product", promptType: "default", reason: "support_need_product" };
    }
    if (flowState.stage === "support_product") {
      const { brand, model } = extractBrandModel(text);
      const validBrand = brand && isValidBrandModelValue(brand) ? brand : "";
      const validModel = model && isValidBrandModelValue(model) ? model : "";
      if (validBrand) flowState.data.product_brand = validBrand;
      if (validModel) flowState.data.product_model = validModel;
      if (flowState.data.product_brand && flowState.data.product_model) {
        return { nextStage: "support_name", promptType: "default", reason: "support_product_ok" };
      }
      return { nextStage: "support_product", promptType: "default", reason: "support_product_missing" };
    }
    if (flowState.stage === "support_name") {
      const nameCandidate = extractNameCandidate(text);
      if (!nameCandidate) {
        return { nextStage: "support_name", promptType: "invalid_name", reason: "name_invalid" };
      }
      flowState.data.full_name = nameCandidate;
      const callerDigits = ensureCallerDigits();
      if (callerDigits) {
        return { nextStage: "support_phone_confirm", promptType: "default", reason: "phone_confirm" };
      }
      if (isValidPhoneDigits(flowState.data.callback_phone)) {
        return {
          nextStage: "support_phone_confirm_new",
          promptType: "default",
          reason: "phone_confirm_new"
        };
      }
      return { nextStage: "support_phone_collect", promptType: "default", reason: "phone_collect" };
    }
    if (flowState.stage === "support_phone_confirm") {
      const callerDigits = ensureCallerDigits();
      if (isYes(text) && callerDigits) {
        flowState.data.callback_phone = callerDigits;
        flowState.phoneConfirmed = true;
        return { nextStage: "support_done", promptType: "default", reason: "phone_confirm_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "support_phone_collect", promptType: "default", reason: "phone_confirm_no" };
    }
    if (flowState.stage === "support_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return {
          nextStage: "support_phone_collect",
          promptType: "invalid_phone",
          reason: "phone_invalid"
        };
      }
      flowState.data.callback_phone = digits;
      flowState.phoneConfirmed = false;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      return {
        nextStage: "support_phone_confirm_new",
        promptType: "default",
        reason: "phone_collect"
      };
    }
    if (flowState.stage === "support_phone_confirm_new") {
      if (isYes(text) && isValidPhoneDigits(flowState.data.callback_phone)) {
        flowState.phoneConfirmed = true;
        return { nextStage: "support_done", promptType: "default", reason: "phone_confirm_new_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "support_phone_collect", promptType: "default", reason: "phone_confirm_new_no" };
    }
    if (flowState.stage === "delivery_desc") {
      flowState.data.delivery_desc = text;
      if (!flowState.data.delivery_topic) {
        flowState.data.delivery_topic = extractDeliveryTopic(text);
      }
      return { nextStage: "delivery_name", promptType: "default", reason: "delivery_desc" };
    }
    if (flowState.stage === "delivery_name") {
      flowState.data.after_hours = Boolean(flowState.afterHours);
      const nameCandidate = extractNameCandidate(text);
      if (!nameCandidate) {
        return { nextStage: "delivery_name", promptType: "invalid_name", reason: "name_invalid" };
      }
      flowState.data.full_name = nameCandidate;
      const callerDigits = ensureCallerDigits();
      if (callerDigits) {
        return { nextStage: "delivery_phone_confirm", promptType: "default", reason: "phone_confirm" };
      }
      if (isValidPhoneDigits(flowState.data.callback_phone)) {
        return {
          nextStage: "delivery_phone_confirm_new",
          promptType: "default",
          reason: "phone_confirm_new"
        };
      }
      return { nextStage: "delivery_phone_collect", promptType: "default", reason: "phone_collect" };
    }
    if (flowState.stage === "delivery_phone_confirm") {
      const callerDigits = ensureCallerDigits();
      if (isYes(text) && callerDigits) {
        flowState.data.callback_phone = callerDigits;
        flowState.phoneConfirmed = true;
        return { nextStage: "delivery_done", promptType: "default", reason: "phone_confirm_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "delivery_phone_collect", promptType: "default", reason: "phone_confirm_no" };
    }
    if (flowState.stage === "delivery_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return {
          nextStage: "delivery_phone_collect",
          promptType: "invalid_phone",
          reason: "phone_invalid"
        };
      }
      flowState.data.callback_phone = digits;
      flowState.phoneConfirmed = false;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      return {
        nextStage: "delivery_phone_confirm_new",
        promptType: "default",
        reason: "phone_collect"
      };
    }
    if (flowState.stage === "delivery_phone_confirm_new") {
      if (isYes(text) && isValidPhoneDigits(flowState.data.callback_phone)) {
        flowState.phoneConfirmed = true;
        return { nextStage: "delivery_done", promptType: "default", reason: "phone_confirm_new_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "delivery_phone_collect", promptType: "default", reason: "phone_confirm_new_no" };
    }
    if (flowState.stage === "message_target") {
      const target = extractMessageTarget(text);
      if (!target) {
        return { nextStage: "message_target", promptType: "default", reason: "message_target_missing" };
      }
      flowState.data.message_target = target;
      return { nextStage: "message_name", promptType: "default", reason: "message_target" };
    }
    if (flowState.stage === "message_name") {
      const nameCandidate = extractNameCandidate(text);
      if (!nameCandidate) {
        return { nextStage: "message_name", promptType: "invalid_name", reason: "name_invalid" };
      }
      flowState.data.full_name = nameCandidate;
      return { nextStage: "message_body", promptType: "default", reason: "message_name" };
    }
    if (flowState.stage === "message_body") {
      flowState.data.message_body = text;
      const callerDigits = ensureCallerDigits();
      if (callerDigits) {
        return { nextStage: "message_phone_confirm", promptType: "default", reason: "phone_confirm" };
      }
      if (isValidPhoneDigits(flowState.data.callback_phone)) {
        return {
          nextStage: "message_phone_confirm_new",
          promptType: "default",
          reason: "phone_confirm_new"
        };
      }
      return { nextStage: "message_phone_collect", promptType: "default", reason: "phone_collect" };
    }
    if (flowState.stage === "message_phone_confirm") {
      const callerDigits = ensureCallerDigits();
      if (isYes(text) && callerDigits) {
        flowState.data.callback_phone = callerDigits;
        flowState.phoneConfirmed = true;
        return { nextStage: "message_done", promptType: "default", reason: "phone_confirm_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "message_phone_collect", promptType: "default", reason: "phone_confirm_no" };
    }
    if (flowState.stage === "message_phone_collect") {
      const digits = extractPhoneCandidates(text);
      if (!isValidPhoneDigits(digits)) {
        return {
          nextStage: "message_phone_collect",
          promptType: "invalid_phone",
          reason: "phone_invalid"
        };
      }
      flowState.data.callback_phone = digits;
      flowState.phoneConfirmed = false;
      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
      return {
        nextStage: "message_phone_confirm_new",
        promptType: "default",
        reason: "phone_collect"
      };
    }
    if (flowState.stage === "message_phone_confirm_new") {
      if (isYes(text) && isValidPhoneDigits(flowState.data.callback_phone)) {
        flowState.phoneConfirmed = true;
        return { nextStage: "message_done", promptType: "default", reason: "phone_confirm_new_yes" };
      }
      flowState.phoneConfirmed = false;
      return { nextStage: "message_phone_collect", promptType: "default", reason: "phone_confirm_new_no" };
    }
    return { nextStage: flowState.stage, promptType: "default", reason: "no_transition" };
  };

  const printCallerFinal = (text) => {
    const t = String(text || "").trim();
    if (!t) return;
    if (t === lastCallerFinal) return;
    lastCallerFinal = t;
    pushTurn("caller", t);
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

  const onCallerFinal = async (text, options = {}) => {
    if (flowState.doneLocked && !options.forceSayText) {
      return;
    }
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
      return;
    }
    if (isIgnorableUtterance(text) && !options.forceSayText) {
      always(`[FSM] ignorable_utterance stage=${flowState.stage} text=${preview(text, 80)}`);
      return;
    }

    const utterance = String(text || "").trim();
    const callerDigits = ensureCallerDigits();
    const afterHours = isAfterHours();
    const carriers = afterHours ? buildCarrierList() : [];
    flowState.afterHours = afterHours;

    let nextStage = flowState.stage;
    let promptType = "default";
    let reason = "force_say";

    if (!options.forceSayText) {
      const decision = processCallerUtterance(utterance);
      nextStage = decision.nextStage || flowState.stage;
      promptType = decision.promptType || "default";
      reason = decision.reason || "caller_final";
    }

    if (nextStage !== flowState.stage) {
      setStage(nextStage, reason);
    } else {
      logStageChange(flowState.stage, nextStage, reason);
    }

    let sayText = "";
    let sayKey = "";
    if (options.forceSayText) {
      sayText = options.forceSayText;
      sayKey = options.sayKey || "FORCED_TEXT";
    } else if (flowState.stage.endsWith("_done")) {
      sayText = composeDoneSayText(flowState.stage);
      sayKey = STAGES[flowState.stage]?.prompts?.default?.sayKey || "";
    } else {
      const prompt = getStagePrompt(flowState.stage, promptType, {
        callerDigits,
        callbackPhone: flowState.data.callback_phone,
        afterHours,
        carriers,
        hasOpened: flowState.hasOpened
      });
      sayText = prompt.sayText;
      sayKey = prompt.sayKey;
    }

    if (!sayText) {
      return;
    }

    if (!flowState.hasOpened && flowState.stage === "routing") {
      flowState.hasOpened = true;
    }

    logSay(sayKey, sayText);
    logCollected();

    if (awaitingResponse) {
      safeOpenAISend({ type: "response.cancel" });
      awaitingResponse = false;
    }

    await sleep(120);
    awaitingResponse = true;

    const responseId = crypto.randomUUID();
    flowState.expectedSayId = responseId;
    flowState.expectedSayText = sayText;
    flowState.expectedSayNormalized = normalizeSayText(sayText);
    flowState.expectedSayAttempts = options.guardAttempt || 0;
    allowedResponseCorrelationIds.add(responseId);

    const instructions = buildFlowInstructions(sayText, options.strict);
    always(`[FSM] route=${flowState.route} stage=${flowState.stage} -> next=${flowState.stage} reason=response_create`);

    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions,
        metadata: {
          correlation_id: responseId
        }
      }
    });
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
  // audio and, after caller_final events, trigger a response.
  let awaitingResponse = false;

  // When flushing buffered audio frames back to OpenAI after the assistant
  // finishes speaking, we temporarily ignore any resulting transcripts.
  let isFlushingBufferedAudio = false;

  const allowedResponseCorrelationIds = new Set();
  const blockedResponseIds = new Set();
  let activeResponseId = "";

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

    if (
      (!SHEETS.loaded_at ||
        (!Object.keys(SHEETS.prompts || {}).length &&
          !Object.keys(SHEETS.settings || {}).length)) &&
      !warnedSheetsEmpty
    ) {
      warnedSheetsEmpty = true;
      console.warn("[WARNING] Sheets not loaded or empty; using fallbacks.");
    }

    // ✅ Master prompt stays in PROMPTS
    const masterPrompt = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
    );

    // ✅ Opening script comes from SETTINGS
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

    // Prepare the realtime session settings.
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

    // ✅ enable caller transcription
    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    safeOpenAISend({ type: "session.update", session });

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

  openaiWs.on("message", async (data) => {
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
      }
      return;
    }

    if (msg.type === "response.created") {
      const correlationId = msg?.response?.metadata?.correlation_id || "";
      const responseId = msg?.response?.id || "";
      if (!correlationId || !allowedResponseCorrelationIds.has(correlationId)) {
        always(`[AUTO_RESPONSE_BLOCKED] response_id=${responseId || "unknown"} correlation_id=${correlationId || "missing"}`);
        if (responseId) blockedResponseIds.add(responseId);
        safeOpenAISend({ type: "response.cancel", response_id: responseId });
        return;
      }
      allowedResponseCorrelationIds.delete(correlationId);
      activeResponseId = responseId;
      return;
    }

    // -----------------------------
    // BOT FINAL (clean)
    // -----------------------------
    if (msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) {
        printBotFinal(t);
    if (!flowState.guardFailed && flowState.expectedSayNormalized) {
          const actualNormalized = normalizeSayText(t);
          const expectedNormalized = flowState.expectedSayNormalized;
          const matches =
            actualNormalized === expectedNormalized ||
            actualNormalized.includes(expectedNormalized) ||
            expectedNormalized.includes(actualNormalized);
          if (!matches) {
            const expectedHash = crypto
              .createHash("sha256")
              .update(expectedNormalized)
              .digest("hex")
              .slice(0, 10);
            always(
              `[FSM_GUARD] deviation expectedHash=${expectedHash} expectedPreview=${preview(
                flowState.expectedSayText,
                120
              )} actualPreview=${preview(t, 120)}`
            );
            if (activeResponseId) blockedResponseIds.add(activeResponseId);
            safeOpenAISend({ type: "response.cancel", response_id: activeResponseId });
            if (flowState.expectedSayAttempts >= 1) {
              flowState.guardFailed = true;
              flowState.finalEvent = "failed_guard";
              flowState.pendingFinalWebhook = true;
              const failSafeText =
                "לא מצליחה להמשיך כרגע, אשמח שנציג יחזור אליכם";
              await onCallerFinal("", {
                forceSayText: failSafeText,
                strict: true,
                guardAttempt: flowState.expectedSayAttempts + 1,
                sayKey: "FLOW_GUARD_FAILSAFE"
              });
            } else {
              await onCallerFinal("", {
                forceSayText: flowState.expectedSayText,
                strict: true,
                guardAttempt: flowState.expectedSayAttempts + 1,
                sayKey: "FLOW_GUARD_RETRY"
              });
            }
            return;
          }
        }
      }
      return;
    }

    // -----------------------------
    // CALLER FINAL (deterministic)
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
        if (isFlushingBufferedAudio) {
          lastCallerFinal = utterance;
          return;
        }
        printCallerFinal(utterance);
        await onCallerFinal(utterance);
        return;
      }
    }

    // -----------------------------
    // Turn boundary events
    // -----------------------------
    if (msg.type === "input_audio_buffer.speech_stopped") {
      // Ignore speech_stopped events for response timing. We'll respond only after
      // caller_final events.
      return;
    }

    // response lifecycle
    if (msg.type === "response.done") {
      awaitingResponse = false;
      allowedResponseCorrelationIds.delete(flowState.expectedSayId);
      if (activeResponseId) {
        blockedResponseIds.delete(activeResponseId);
      }
      activeResponseId = "";
      if (allowedResponseCorrelationIds.size > 50) {
        allowedResponseCorrelationIds.clear();
      }
      if (blockedResponseIds.size > 50) {
        blockedResponseIds.clear();
      }
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
      if (flowState.pendingFinalWebhook && !flowState.finalPayloadSent && !sentCallEnded) {
        const routeComplete = isRouteComplete(
          flowState.route,
          flowState.data,
          flowState.phoneConfirmed
        );
        if (!flowState.guardFailed && (!flowState.stage.endsWith("_done") || !routeComplete)) {
          return;
        }
        if (!flowState.finalEvent) return;
        sentCallEnded = true;
        flowState.finalPayloadSent = true;
        endedAt = endedAt || nowIso();
        const payload = applyWebhookDefaults(buildFinalPayload());
        await sendWebhookOnce(flowState.finalEvent, payload, { wait_for_recording: true });
        if (!hangupRequested) {
          hangupRequested = true;
          await attemptHangup(callSid);
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
      const responseId = msg.response_id || msg?.response?.id || activeResponseId || "";
      if (!responseId || blockedResponseIds.has(responseId)) {
        return;
      }

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
      // call_started webhook suppressed (final-only mode)
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

      if (!sentCallEnded && !flowState.finalPayloadSent) {
        sentCallEnded = true;
        sentCallAbandoned = true;
        const payload = applyWebhookDefaults(buildFinalPayload());
        payload.stage = flowState.stage;
        payload.route = flowState.route;
        try {
          await sendWebhookOnce("call_abandoned", payload, { wait_for_recording: false });
        } catch (_) {}
        flowState.finalPayloadSent = true;
        if (!hangupRequested) {
          hangupRequested = true;
          await attemptHangup(callSid);
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
    // If socket closed unexpectedly and we never sent call_ended -> abandoned
    if (!sentCallEnded && !sentCallAbandoned) {
      sentCallAbandoned = true;
      endedAt = endedAt || nowIso();
      const recording_url_public = makeRecordingPublicUrl(callSid);
      sendWebhookOnce(
        "call_abandoned",
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
      flowState.finalPayloadSent = true;
    }
    if (!hangupRequested && (flowState.finalPayloadSent || sentCallEnded)) {
      hangupRequested = true;
      attemptHangup(callSid);
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
