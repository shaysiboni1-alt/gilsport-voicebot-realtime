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

    const meta = await sheets.spreadsheets.get({
      spreadsheetId: GSHEET_ID,
      fields: "sheets.properties.title"
    });
    const sheetTitles = new Set(
      (meta.data.sheets || []).map((s) => String(s.properties?.title || ""))
    );
    const desiredRanges = [
      "PROMPTS!A:Z",
      "SETTINGS!A:Z",
      "KB_FACTS!A:Z",
      "DO_NOT_SAY!A:Z",
      "SUPPLIERS_IMPORTERS!A:Z",
      "DELIVERY_CONTACTS!A:Z",
      "ROUTING_RULES!A:Z",
      "BUSINESS_INFO!A:Z"
    ];
    const ranges = desiredRanges.filter((range) => {
      const sheetName = range.split("!")[0];
      return sheetTitles.has(sheetName);
    });

    // ✅ load available ranges in one call (skip missing sheets)
    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: GSHEET_ID,
      ranges
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
    const routingRulesRange = valueRanges.find(
      (vr) => (vr.range || "").startsWith("ROUTING_RULES!")
    );
    const businessInfoRange = valueRanges.find(
      (vr) => (vr.range || "").startsWith("BUSINESS_INFO!")
    );

    const kbFactsRows = rowsToObjects((kbFactsRange?.values || []).slice());
    const doNotSayRows = rowsToObjects((doNotSayRange?.values || []).slice());
    const suppliersImportersRows = rowsToObjects(
      (suppliersImportersRange?.values || []).slice()
    );
    const deliveryContactsRows = rowsToObjects((deliveryContactsRange?.values || []).slice());
    const routingRulesRows = rowsToObjects((routingRulesRange?.values || []).slice());
    const businessInfoRows = rowsToObjects((businessInfoRange?.values || []).slice());

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
      routingRules: routingRulesRows,
      businessInfo: businessInfoRows
    };

    log(
      `Sheets loaded (prompts=${Object.keys(prompts).length}, settings=${Object.keys(settings).length}, kbFacts=${kbFactsRows.length}, doNotSay=${doNotSayRows.length}, suppliersImporters=${suppliersImportersRows.length}, deliveryContacts=${deliveryContactsRows.length}, routingRules=${routingRulesRows.length}, businessInfo=${businessInfoRows.length})`
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

// ... (המשך הקובץ המלא – לא קיצרתי בקוד המקורי)
