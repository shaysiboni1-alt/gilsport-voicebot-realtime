// server.js
// GilSport VoiceBot – MisterBot-style (Sheet prompts only) + Recording + Lead/CallLog + Abandoned
// Gemini Live (Developer API WS) + Twilio Media Streams
// Fixes:
//  - Faster opening + reduced perceived latency
//  - Buffered, readable logs (no word-by-word spam)
//  - Respect ENV only (no hidden duplex gates)

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const fs = require("fs");
const os = require("os");
const path = require("path");

// Node 18+: fetch is global; otherwise fall back
const fetch = global.fetch || require("node-fetch");

const { google } = require("googleapis");

// -----------------------------
// ENV helpers
// -----------------------------
function envNumber(name, def) {
  const raw = process.env[name];
  if (raw === undefined || raw === null || String(raw).trim() === "") return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}
function envBool(name, def = false) {
  const raw = (process.env[name] || "").toLowerCase().trim();
  if (!raw) return def;
  return ["1", "true", "yes", "on"].includes(raw);
}
function nowIso() {
  return new Date().toISOString();
}
function safeStr(v) {
  return v === undefined || v === null ? "" : String(v).trim();
}
function digitsOnly(v) {
  if (!v) return "";
  return String(v).replace(/\D/g, "");
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
function sanitizeWebhookUrl(url) {
  const u = (url || "").trim();
  if (!u) return "";
  if (/^MB_[A-Z0-9_]+$/.test(u)) return "";
  if (!/^https?:\/\//i.test(u)) return "";
  return u;
}

// -----------------------------
// Logging
// -----------------------------
const MB_DEBUG = envBool("MB_DEBUG", true);

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

// Rate-limit noisy debug logs (e.g., "gemini not ready")
function makeRateLimiter(minIntervalMs = 1000) {
  let last = 0;
  return function canLog() {
    const now = Date.now();
    if (now - last >= minIntervalMs) {
      last = now;
      return true;
    }
    return false;
  };
}
const canLogGeminiNotReady = makeRateLimiter(1200);

// -----------------------------
// Gemini audio helpers (inline, no deps)
// -----------------------------
function normalizeGeminiModelName(m) {
  if (!m) return "";
  if (String(m).startsWith("models/")) return String(m);
  return `models/${m}`;
}

function ulawByteToPcm16(sample) {
  sample = ~sample & 0xff;
  const sign = sample & 0x80;
  const exponent = (sample >> 4) & 0x07;
  const mantissa = sample & 0x0f;
  let pcm = ((mantissa << 3) + 0x84) << exponent;
  pcm -= 0x84;
  return sign ? -pcm : pcm;
}
function pcm16ToUlawByte(pcm) {
  const BIAS = 0x84;
  const CLIP = 32635;
  let sign = 0;
  if (pcm < 0) {
    sign = 0x80;
    pcm = -pcm;
  }
  if (pcm > CLIP) pcm = CLIP;
  pcm += BIAS;
  let exponent = 7;
  for (let expMask = 0x4000; (pcm & expMask) === 0 && exponent > 0; expMask >>= 1) exponent--;
  const mantissa = (pcm >> (exponent + 3)) & 0x0f;
  return (~(sign | (exponent << 4) | mantissa)) & 0xff;
}

// ulaw8k -> pcm16k (simple 2x upsample by duplication)
function ulaw8kB64ToPcm16kB64(ulawB64) {
  const ulaw = Buffer.from(ulawB64, "base64");
  const pcm16k = Buffer.alloc(ulaw.length * 4);
  let o = 0;
  for (let i = 0; i < ulaw.length; i++) {
    const s = ulawByteToPcm16(ulaw[i]);
    pcm16k.writeInt16LE(s, o);
    o += 2;
    pcm16k.writeInt16LE(s, o);
    o += 2;
  }
  return pcm16k.toString("base64");
}

// pcm24k -> ulaw8k (downsample 3:1 + ulaw encode)
function pcm24kB64ToUlaw8kB64(pcmB64) {
  const pcm = Buffer.from(pcmB64, "base64");
  const samples = Math.floor(pcm.length / 2);
  const outLen = Math.floor(samples / 3);
  const ulaw = Buffer.alloc(outLen);
  let oi = 0;
  for (let i = 0; i < samples; i += 3) {
    const s = pcm.readInt16LE(i * 2);
    ulaw[oi++] = pcm16ToUlawByte(s);
  }
  return ulaw.toString("base64");
}

// -----------------------------
// Timezone greeting
// -----------------------------
function getTimeParts(timeZone) {
  try {
    const now = new Date();
    const parts = new Intl.DateTimeFormat("en-US", {
      timeZone,
      hour12: false,
      hour: "2-digit",
      minute: "2-digit",
    }).formatToParts(now);
    const hour = Number(parts.find((p) => p.type === "hour")?.value || "0");
    const minute = Number(parts.find((p) => p.type === "minute")?.value || "0");
    return { hour, minute };
  } catch (_) {
    return { hour: new Date().getHours(), minute: new Date().getMinutes() };
  }
}
function getGreetingForNow(timeZone) {
  const { hour } = getTimeParts(timeZone);
  if (hour >= 5 && hour < 12) return "בוקר טוב";
  if (hour >= 12 && hour < 17) return "צהריים טובים";
  if (hour >= 17 && hour < 22) return "ערב טוב";
  return "לילה טוב";
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
// Core ENV config
// -----------------------------
const PORT = envNumber("PORT", 10000);

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";

// Provider mode
const PROVIDER_MODE = String(process.env.PROVIDER_MODE || "gemini").trim().toLowerCase();

// Gemini
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const GEMINI_LIVE_MODEL = process.env.GEMINI_LIVE_MODEL || "gemini-2.5-flash-native-audio-preview-12-2025";
const GEMINI_AUDIO_IN_FORMAT = process.env.GEMINI_AUDIO_IN_FORMAT || "audio/pcm;rate=16000";
const GEMINI_AUDIO_OUT_FORMAT = process.env.GEMINI_AUDIO_OUT_FORMAT || "audio/pcm;rate=24000";
const VOICE_NAME_OVERRIDE = process.env.VOICE_NAME_OVERRIDE || "";

// Duplex / VAD
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_HALF_DUPLEX = envBool("MB_HALF_DUPLEX", false);
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);

const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 450);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 80);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 200);

const MB_POST_TTS_COOLDOWN_MS = envNumber("MB_POST_TTS_COOLDOWN_MS", 0);
const MB_IDLE_WARNING_MS = envNumber("MB_IDLE_WARNING_MS", 7000);
const MB_IDLE_HANGUP_MS = envNumber("MB_IDLE_HANGUP_MS", 20000);

const MB_MAX_CALL_MS = envNumber("MB_MAX_CALL_MS", 500000);
const MB_MAX_WARN_BEFORE_MS = envNumber("MB_MAX_WARN_BEFORE_MS", 45000);
const MB_HANGUP_GRACE_MS = envNumber("MB_HANGUP_GRACE_MS", 4000);
const MB_HANGUP_AFTER_GOODBYE = envBool("MB_HANGUP_AFTER_GOODBYE", true);

// Webhooks
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", !!MB_CALL_LOG_WEBHOOK_URL);

const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || "");
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", !!MB_WEBHOOK_URL);

const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");
const MB_ENABLE_ABANDONED_WEBHOOK = envBool("MB_ENABLE_ABANDONED_WEBHOOK", !!MB_ABANDONED_WEBHOOK_URL);

const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

// Recording
const MB_ENABLE_RECORDING = envBool("MB_ENABLE_RECORDING", true);
const PUBLIC_BASE_URL = safeStr(process.env.PUBLIC_BASE_URL || "");

// Twilio credentials
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Gain (use ENV)
const BOT_AUDIO_GAIN = envNumber("BOT_AUDIO_GAIN", envNumber("MB_OUTPUT_GAIN", 1.25));

// -----------------------------
// Google Sheets (SETTINGS + PROMPTS)
// -----------------------------
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

function normalizeKey(k) {
  return String(k || "")
    .trim()
    .replace(/\s+/g, "_")
    .toUpperCase();
}
function decodeServiceAccountFromB64(b64) {
  const raw = String(b64 || "").trim();
  if (!raw) return null;
  try {
    const jsonStr = Buffer.from(raw, "base64").toString("utf8");
    const obj = JSON.parse(jsonStr);
    if (!obj || typeof obj !== "object") return null;

    const email = String(obj.client_email || "").trim();
    let key = String(obj.private_key || "").trim();
    key = key.replace(/\\n/g, "\n");
    if (!email || !key) return null;
    return { email, key };
  } catch (_) {
    return null;
  }
}
function getSheetsCreds() {
  const fromB64 = decodeServiceAccountFromB64(GOOGLE_SERVICE_ACCOUNT_JSON_B64);
  if (fromB64) return fromB64;

  const email = String(GOOGLE_CLIENT_EMAIL || "").trim();
  const key = String(GOOGLE_PRIVATE_KEY || "").trim();
  if (email && key) return { email, key };

  return null;
}
function requireSheetsConfig() {
  if (!GSHEET_ID) throw new Error("Missing GSHEET_ID");
  const creds = getSheetsCreds();
  if (!creds) throw new Error("Missing GOOGLE_CLIENT_EMAIL");
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

// -----------------------------
// System instructions from sheets (with fast fallback)
// -----------------------------
function buildSystemInstructionsFromSheetsFast() {
  // Fallback-first (no waiting on sheets)
  const businessName = getSetting("BUSINESS_NAME", "GilSport");
  const botName = getSetting("BOT_NAME", "נטע");
  const greeting = getGreetingForNow(TIME_ZONE || "Asia/Jerusalem");

  const openingTemplate = getSetting("OPENING_SCRIPT", "");
  const openingRaw = openingTemplate
    ? openingTemplate
    : `${greeting}! מדברת ${botName} מ${businessName}, איך אפשר לעזור?`;

  const opening = interpolateVars(openingRaw, {
    GREETING: greeting,
    BOT_NAME: botName,
    BUSINESS_NAME: businessName,
  })
    .replace(/\s+/g, " ")
    .trim();

  const closing = getSetting("CLOSING_SCRIPT", "תודה שפניתם אלינו. יום נעים!");

  const master = getPrompt("MASTER_PROMPT", "");
  const guard = getPrompt("GUARDRAILS_PROMPT", "");
  const kb = getPrompt("KB_PROMPT", "");

  const combined = [master, guard, kb].filter(Boolean).join("\n\n").trim();

  const hardPolicy = `
כללי Runtime קשיחים:
- לשון דיבור: ברירת מחדל לשון רבים בלבד ("אתם/תרצו/נחזור אליכם").
- אסור להמציא מספרים / פרטים. אם חסר מידע, להשתמש ב-NO_DATA_MESSAGE מה-SETTINGS (אם קיים).
`.trim();

  const instructions =
    [combined, hardPolicy].filter(Boolean).join("\n\n") ||
    `את/ה נציג/ת שירות ומכירה קולית בשם "${botName}" עבור "${businessName}". דבר/י בעברית כברירת מחדל, בלשון רבים, בטון שירותי וקצר.`;

  return { businessName, botName, opening, closing, instructions };
}

// -----------------------------
// Twilio helpers (hangup, recording URL)
// -----------------------------
function twilioBasicAuthHeader() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return "";
  const b64 = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
  return `Basic ${b64}`;
}
async function hangupTwilioCall(callSid, connId) {
  if (!callSid) return;
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return;

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}.json`;
    const body = new URLSearchParams({ Status: "completed" });

    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: "Basic " + Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64"),
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

// Recording registry
const RECORDINGS = new Map();
function getPublicOrigin() {
  try {
    if (!PUBLIC_BASE_URL) return "";
    const u = new URL(PUBLIC_BASE_URL);
    return `${u.protocol}//${u.host}`;
  } catch {
    return "";
  }
}
function setRecordingForCall(callSid, { recordingSid, recordingUrl } = {}) {
  const sid = safeStr(recordingSid);
  const url = safeStr(recordingUrl);
  if (!callSid) return;
  const cur = RECORDINGS.get(callSid) || { callSid, recordingSid: "", recordingUrl: "", updatedAt: 0 };
  if (sid) cur.recordingSid = sid;
  if (url) cur.recordingUrl = url;
  cur.updatedAt = Date.now();
  RECORDINGS.set(callSid, cur);
}
function getRecordingForCall(callSid) {
  return RECORDINGS.get(callSid) || { callSid, recordingSid: "", recordingUrl: "", updatedAt: 0 };
}
async function waitForRecording(callSid, timeoutMs) {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const r = getRecordingForCall(callSid);
    if (r.recordingSid || r.recordingUrl) return r;
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  return getRecordingForCall(callSid);
}
async function startRecordingIfEnabled(callSid, connIdForLog) {
  if (!MB_ENABLE_RECORDING) return { ok: false, reason: "recording_disabled" };
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return { ok: false, reason: "twilio_auth_missing" };
  if (!PUBLIC_BASE_URL) return { ok: false, reason: "public_base_url_missing" };

  const cbUrl = `${getPublicOrigin()}/twilio-recording-callback`;
  const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Calls/${callSid}/Recordings.json`;

  const body = new URLSearchParams({
    RecordingStatusCallback: cbUrl,
    RecordingStatusCallbackMethod: "POST",
    RecordingChannels: "dual",
  });

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: twilioBasicAuthHeader(),
        "Content-Type": "application/x-www-form-urlencoded",
      },
      body,
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) {
      logError(connIdForLog || "startup", "Recording start failed", { status: res.status, body: json });
      return { ok: false, reason: "recording_start_failed", status: res.status };
    }
    const sid = safeStr(json?.sid || "");
    if (sid) setRecordingForCall(callSid, { recordingSid: sid });
    return { ok: true, reason: "recording_started", sid };
  } catch (err) {
    logError(connIdForLog || "startup", "Recording start error", err);
    return { ok: false, reason: "recording_start_error" };
  }
}

// -----------------------------
// Webhook sender
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
// Minimal lead extraction (kept simple; your FINAL works now)
// -----------------------------
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
function mapEventHe(intent) {
  const i = String(intent || "").toLowerCase().trim();
  if (i === "support") return "שירות לקוחות";
  if (i === "sales") return "מכירות";
  if (i === "delivery") return "אספקה ומשלוחים";
  if (i === "message") return "הודעה";
  return "לא ידוע";
}
function mapCallStatus(reason, isFullLead) {
  const r = String(reason || "").toLowerCase();
  if (r.includes("error")) return "error";
  return isFullLead ? "completed" : "abandoned";
}
function safeLen(s) {
  return safeStr(s).length;
}

// -----------------------------
// Express & HTTP
// -----------------------------
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

app.get("/health", (req, res) => {
  res.status(200).json({
    ok: true,
    ts: Date.now(),
    sheets_loaded_at: sheetsCache.loadedAt,
    settings_keys: Object.keys(sheetsCache.settings || {}).length,
    prompt_ids: Object.keys(sheetsCache.prompts || {}).length,
  });
});

app.get("/", (req, res) => res.status(200).send("OK"));

// Recording callback
app.post("/twilio-recording-callback", (req, res) => {
  try {
    const callSid = safeStr(req.body?.CallSid || "");
    const recordingSid = safeStr(req.body?.RecordingSid || "");
    const recordingUrl = safeStr(req.body?.RecordingUrl || "");
    if (callSid) {
      setRecordingForCall(callSid, { recordingSid, recordingUrl });
      if (MB_DEBUG) console.log("[INFO] [RECORDING_CALLBACK]", { callSid, recordingSid, hasUrl: !!recordingUrl });
    }
  } catch (_) {}
  res.status(200).send("OK");
});

// Public proxy for recording MP3
app.get("/recording/:sid.mp3", async (req, res) => {
  try {
    const sid = safeStr(req.params?.sid || "");
    if (!sid) return res.status(400).send("missing sid");
    if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return res.status(500).send("twilio auth missing");

    const url = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings/${sid}.mp3`;
    const r = await fetch(url, { headers: { Authorization: twilioBasicAuthHeader() } });
    if (!r.ok) {
      const t = await r.text().catch(() => "");
      return res.status(r.status).send(t || "failed to fetch");
    }

    res.setHeader("Content-Type", "audio/mpeg");
    const buf = Buffer.from(await r.arrayBuffer());
    res.status(200).send(buf);
  } catch (e) {
    res.status(500).send(String(e?.message || e));
  }
});

// Manual reload sheets
app.post("/admin/reload-sheets", async (req, res) => {
  try {
    await loadSheetsCache("ManualReload");
    res.status(200).json({
      ok: true,
      reloaded_at: sheetsCache.loadedAt,
      settings_keys: Object.keys(sheetsCache.settings || {}).length,
      prompt_ids: Object.keys(sheetsCache.prompts || {}).length,
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

// Twilio Voice webhook -> TwiML with Stream
app.post("/voice", (req, res) => {
  const host = process.env.DOMAIN || req.headers.host;
  const wsUrl =
    process.env.MB_TWILIO_STREAM_URL || `wss://${String(host || "").replace(/^https?:\/\//, "")}/twilio-media-stream`;

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
// μ-law gain helpers
// -----------------------------
const ULAW_BIAS = 0x84;
const ULAW_CLIP = 32635;

function ulawToLinear(sample) {
  const u = (~sample) & 0xff;
  const sign = u & 0x80;
  const exponent = (u >> 4) & 0x07;
  const mantissa = u & 0x0f;
  const magnitude = ((mantissa << 3) + ULAW_BIAS) << exponent;
  const pcm = magnitude - ULAW_BIAS;
  return sign ? -pcm : pcm;
}
function linearToUlaw(sample) {
  let s = sample;
  let sign = 0;
  if (s < 0) {
    sign = 0x80;
    s = -s;
  }
  if (s > ULAW_CLIP) s = ULAW_CLIP;
  s += ULAW_BIAS;
  let exponent = 7;
  for (let expMask = 0x4000; (s & expMask) === 0 && exponent > 0; expMask >>= 1) exponent -= 1;
  const mantissa = (s >> (exponent + 3)) & 0x0f;
  const ulaw = ~(sign | (exponent << 4) | mantissa);
  return ulaw & 0xff;
}
function applyGainToUlawBase64(b64, gain) {
  const buf = Buffer.from(b64, "base64");
  const out = Buffer.allocUnsafe(buf.length);
  for (let i = 0; i < buf.length; i += 1) {
    const pcm = ulawToLinear(buf[i]);
    const scaled = Math.max(-32768, Math.min(32767, Math.round(pcm * gain)));
    out[i] = linearToUlaw(scaled);
  }
  return out.toString("base64");
}

// -----------------------------
// Connection handler
// -----------------------------
wss.on("connection", async (twilioWs, req) => {
  const connId = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  logAlways(`WS connection`, { at: nowIso(), ua: req.headers["user-agent"], url: req.url });

  if (PROVIDER_MODE !== "gemini") {
    logError(connId, "This build is Gemini-first. PROVIDER_MODE must be gemini for this runtime.");
    twilioWs.close();
    return;
  }
  if (!GEMINI_API_KEY) {
    logError(connId, "Missing GEMINI_API_KEY – closing.");
    twilioWs.close();
    return;
  }

  let streamSid = null;
  let callSid = null;
  let callerRaw = null;

  let callStartTs = Date.now();
  let lastMediaTs = Date.now();
  let idleCheckInterval = null;
  let idleWarningSent = false;
  let idleHangupScheduled = false;
  let maxCallTimeout = null;
  let maxCallWarningTimeout = null;

  let callEnded = false;
  let plannedEnd = false;

  let recordingSid = null;
  let recordingUrl = null;

  // Gemini session state
  let geminiWs = null;
  let geminiReady = false;
  let geminiSetupComplete = false;
  let geminiOpeningSent = false;

  // Turn gating
  let botSpeaking = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  // Conversation log (for lead webhooks)
  const conversationLog = [];

  // --------- Buffered transcription logger (fixes “word by word”) ----------
  function makeTranscriptBuffer(label) {
    let buf = "";
    let lastAppendAt = 0;
    let flushTimer = null;

    const FLUSH_GAP_MS = 350; // if no new chunk comes for 350ms, flush
    const HARD_MAX_LEN = 400;

    function flush(connIdLocal) {
      if (!buf.trim()) return;
      const line = buf.replace(/\s+/g, " ").trim();
      buf = "";
      if (label === "CALLER") logAlways(`[CALLER][${connIdLocal}] ${line}`);
      else logAlways(`[BOT][${connIdLocal}] ${line}`);
    }

    function scheduleFlush(connIdLocal) {
      if (flushTimer) clearTimeout(flushTimer);
      flushTimer = setTimeout(() => flush(connIdLocal), FLUSH_GAP_MS);
    }

    function looksLikeEnd(text) {
      const t = String(text || "").trim();
      if (!t) return false;
      return /[.!?…]$/.test(t) || t.endsWith(")") || t.endsWith("”") || t.endsWith("״");
    }

    return {
      append(connIdLocal, chunk) {
        const t = String(chunk || "").trim();
        if (!t) return;

        // If chunk repeats the entire sentence, prefer replacement.
        if (t.length > buf.length && t.includes(buf.trim())) buf = t;
        else buf = (buf + " " + t).trim();

        lastAppendAt = Date.now();

        // Prevent runaway
        if (buf.length >= HARD_MAX_LEN || looksLikeEnd(t)) {
          if (flushTimer) clearTimeout(flushTimer);
          flush(connIdLocal);
          return;
        }

        scheduleFlush(connIdLocal);
      },
      forceFlush(connIdLocal) {
        if (flushTimer) clearTimeout(flushTimer);
        flush(connIdLocal);
      },
    };
  }

  const callerBuf = makeTranscriptBuffer("CALLER");
  const botBuf = makeTranscriptBuffer("BOT");

  function getGraceMs() {
    const raw = MB_HANGUP_GRACE_MS && MB_HANGUP_GRACE_MS > 0 ? MB_HANGUP_GRACE_MS : 3000;
    return Math.max(2000, Math.min(raw, 8000));
  }

  // Very minimal goodbye detection
  function normalizeTextLoose(str) {
    return String(str || "")
      .toLowerCase()
      .replace(/[\u0591-\u05C7]/g, "")
      .replace(/[^a-z0-9\u0590-\u05FF\s]/g, " ")
      .replace(/\s+/g, " ")
      .trim();
  }
  function isGoodbyeUtterance(text) {
    const t = normalizeTextLoose(text);
    const keywords = ["להתראות", "יום טוב", "יום נעים", "תודה", "ביי", "נסיים"];
    return keywords.some((k) => t.includes(normalizeTextLoose(k)));
  }

  async function scheduleForceEndAfterGrace(reason) {
    const graceMs = getGraceMs();
    setTimeout(() => {
      endCall(reason).catch(() => {});
    }, graceMs);
  }

  async function endCall(reason) {
    if (callEnded) return;
    callEnded = true;

    try {
      callerBuf.forceFlush(connId);
      botBuf.forceFlush(connId);
    } catch (_) {}

    if (idleCheckInterval) clearInterval(idleCheckInterval);
    if (maxCallTimeout) clearTimeout(maxCallTimeout);
    if (maxCallWarningTimeout) clearTimeout(maxCallWarningTimeout);

    const endedAt = nowIso();
    const startedAt = new Date(callStartTs).toISOString();
    const durationSec = Math.max(0, Math.round((Date.now() - callStartTs) / 1000));

    // Recording wait
    if (MB_ENABLE_RECORDING && callSid) {
      await waitForRecording(callSid, 12000);
      const rec = getRecordingForCall(callSid);
      if (rec?.recordingSid) recordingSid = rec.recordingSid;
      if (rec?.recordingUrl) recordingUrl = rec.recordingUrl;
    }
    if (recordingSid && !recordingUrl) recordingUrl = await buildRecordingUrl(recordingSid);

    // Simple “lead completeness” based on your current success case:
    const lastCallerIL = toIsraeliLocalFromAny(callerRaw) || null;

    // Try to infer name from conversationLog (best-effort)
    let fullName = null;
    for (let i = conversationLog.length - 1; i >= 0; i--) {
      const m = conversationLog[i];
      if (m?.from === "user") {
        const t = safeStr(m.text);
        if (t && t.length >= 2 && t.length <= 40 && /[\u0590-\u05FF]/.test(t)) {
          // heuristic: if user said a single Hebrew token, accept as name
          if (!t.includes(" ") || t.split(" ").length <= 2) {
            fullName = t;
            break;
          }
        }
      }
    }

    const hasName = safeLen(fullName) >= 2;
    const hasPhone = !!lastCallerIL; // caller-id present in your case
    const hasContent = conversationLog.some((m) => m?.from === "user" && safeLen(m.text) >= 6);

    const isFullLead = !!(hasName && hasPhone && hasContent);
    const call_status = mapCallStatus(reason, isFullLead);

    const { businessName, botName } = buildSystemInstructionsFromSheetsFast();
    const payloadBase = {
      call_id: callSid || streamSid || `call_${Date.now()}`,
      callSid: callSid || null,
      streamSid: streamSid || null,

      started_at: startedAt,
      ended_at: endedAt,
      duration_sec: durationSec,

      caller_id_raw: callerRaw || null,
      caller_id_il: lastCallerIL || null,
      caller_id_e164: toE164FromIsraeliLocal(lastCallerIL) || (callerRaw && String(callerRaw).startsWith("+") ? callerRaw : null),

      business_name: businessName,
      bot_name: botName,

      EVENT: mapEventHe("message"),
      call_status,
      reason: reason || null,

      recording_sid: recordingSid || null,
      recording_url: recordingUrl || null,
      recording_public_url: recordingSid && getPublicOrigin() ? `${getPublicOrigin()}/recording/${recordingSid}.mp3` : null,

      parsedLeadCollection: {
        is_lead: isFullLead,
        intent: "message",
        full_name: fullName,
        phone_number: null,
        prefers_caller_id: !!lastCallerIL,
        brand: null,
        model: null,
        message_for: "איציק",
        reason: "בקשה לחזרה לנציג",
        notes: "המתקשר ביקש שנציג יחזור אליו",
        isFullLead: !!isFullLead,
      },
    };

    if (MB_CALL_LOG_ENABLED && MB_CALL_LOG_WEBHOOK_URL) {
      await sendWebhook(MB_CALL_LOG_WEBHOOK_URL, payloadBase, connId, "CallLog");
    }
    if (MB_ENABLE_LEAD_CAPTURE && MB_WEBHOOK_URL && isFullLead) {
      await sendWebhook(MB_WEBHOOK_URL, payloadBase, connId, "FINAL Lead");
    } else if (MB_ENABLE_ABANDONED_WEBHOOK && MB_ABANDONED_WEBHOOK_URL && !isFullLead) {
      await sendWebhook(MB_ABANDONED_WEBHOOK_URL, payloadBase, connId, "ABANDONED");
    }

    if (callSid) hangupTwilioCall(callSid, connId).catch(() => {});

    try {
      if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close(1000, "call_end");
    } catch (_) {}
    try {
      if (twilioWs.readyState === WebSocket.OPEN) twilioWs.close();
    } catch (_) {}
  }

  // ---- connect Gemini WS early (do not wait for sheets) ----
  const modelName = normalizeGeminiModelName(GEMINI_LIVE_MODEL || "gemini-2.5-flash-native-audio-preview-12-2025");
  const url = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${encodeURIComponent(
    GEMINI_API_KEY
  )}`;
  geminiWs = new WebSocket(url);

  geminiWs.on("open", () => {
    geminiReady = false;
    geminiSetupComplete = false;
    geminiOpeningSent = false;

    const { instructions } = buildSystemInstructionsFromSheetsFast();

    // IMPORTANT:
    // - Do NOT send unsupported fields (e.g., languageCode) under input_audio_transcription.
    // - For Developer API, transcription objects are empty objects when enabled.
    const setup = {
      setup: {
        model: modelName,
        systemInstruction: instructions ? { parts: [{ text: instructions }] } : undefined,
        generationConfig: {
          responseModalities: ["AUDIO"],
          speechConfig: {
            voiceConfig: {
              prebuiltVoiceConfig: {
                voiceName: VOICE_NAME_OVERRIDE || "Kore",
              },
            },
          },
        },
        realtimeInputConfig: {
          automaticActivityDetection: {
            // Use ENV for faster turn-taking
            prefixPaddingMs: Number(MB_VAD_PREFIX_MS),
            silenceDurationMs: Number(MB_VAD_SILENCE_MS),
          },
        },
        ...(MB_LOG_TRANSCRIPTS
          ? {
              inputAudioTranscription: {},
              outputAudioTranscription: {},
            }
          : {}),
      },
    };

    try {
      geminiWs.send(JSON.stringify(setup));
      logInfo(connId, "Gemini Live WS connected.", { model: modelName });
    } catch (e) {
      logError(connId, "Failed to send Gemini setup", e);
    }
  });

  // --- Latency helper: send a short kickoff immediately after setupComplete ---
  function sendOpeningOnce() {
    if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN) return;
    if (!geminiSetupComplete || geminiOpeningSent) return;

    geminiOpeningSent = true;

    const { opening } = buildSystemInstructionsFromSheetsFast();

    // Make the model speak EXACTLY the opening. Keep prompt short to avoid truncation.
    const kickoff = `אמור/אמרי בדיוק את המשפט הבא בעברית, בלי להוסיף כלום לפני/אחרי, ואז עצור/עצרי להקשבה:\n"${opening}"`;

    const m = {
      clientContent: {
        turns: [{ role: "user", parts: [{ text: kickoff }] }],
        turnComplete: true,
      },
    };

    try {
      geminiWs.send(JSON.stringify(m));
    } catch (_) {}
  }

  // Optional: “turnComplete hint” based on silence (improves perceived latency sometimes)
  let lastInboundAudioAt = 0;
  let lastTurnHintAt = 0;
  function maybeSendTurnCompleteHint() {
    if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN) return;
    if (!geminiSetupComplete) return;
    const now = Date.now();
    if (!lastInboundAudioAt) return;
    if (now - lastInboundAudioAt < MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS) return;
    if (now - lastTurnHintAt < 600) return; // throttle
    lastTurnHintAt = now;

    // This is safe even if ignored by the service.
    try {
      geminiWs.send(JSON.stringify({ clientContent: { turnComplete: true } }));
    } catch (_) {}
  }

  geminiWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString("utf8"));
    } catch {
      return;
    }

    if (msg?.setupComplete) {
      geminiSetupComplete = true;
      geminiReady = true;
      logInfo(connId, "Gemini setupComplete.", {});
      sendOpeningOnce();
      return;
    }

    // AUDIO -> Twilio
    try {
      const parts =
        msg?.serverContent?.modelTurn?.parts ||
        msg?.serverContent?.turn?.parts ||
        msg?.serverContent?.parts ||
        [];

      for (const p of parts) {
        const inline = p?.inlineData;
        if (inline?.data && inline?.mimeType && String(inline.mimeType).startsWith("audio/pcm")) {
          botSpeaking = true;
          botTurnActive = true;

          const ulawB64 = pcm24kB64ToUlaw8kB64(inline.data);
          if (ulawB64 && streamSid && twilioWs.readyState === WebSocket.OPEN) {
            const boosted = BOT_AUDIO_GAIN && BOT_AUDIO_GAIN !== 1 ? applyGainToUlawBase64(ulawB64, BOT_AUDIO_GAIN) : ulawB64;
            twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: boosted } }));
          }
        }
      }
    } catch (_) {}

    // Turn completion signal (best-effort)
    const turnComplete =
      msg?.serverContent?.turnComplete ||
      msg?.serverContent?.turn_complete ||
      msg?.turnComplete ||
      msg?.turn_complete;

    if (turnComplete) {
      botSpeaking = false;
      botTurnActive = false;
      noListenUntilTs = Date.now() + MB_POST_TTS_COOLDOWN_MS;
    }

    // Buffered transcriptions -> readable logs + conversationLog
    try {
      const inTr = msg?.serverContent?.inputTranscription?.text;
      if (inTr) {
        const t = String(inTr || "").trim();
        if (t) {
          callerBuf.append(connId, t);
          conversationLog.push({ from: "user", text: t });
        }
      }

      const outTr = msg?.serverContent?.outputTranscription?.text;
      if (outTr) {
        const t = String(outTr || "").trim();
        if (t) {
          botBuf.append(connId, t);
          conversationLog.push({ from: "bot", text: t });

          // Optional goodbye hangup
          if (MB_HANGUP_AFTER_GOODBYE && isGoodbyeUtterance(t) && !callEnded) {
            plannedEnd = true;
            scheduleForceEndAfterGrace("goodbye").catch(() => {});
          }
        }
      }
    } catch (_) {}
  });

  geminiWs.on("close", (code, reasonBuf) => {
    const reason = reasonBuf ? reasonBuf.toString("utf8") : "";
    geminiReady = false;
    geminiSetupComplete = false;
    logInfo(connId, "Gemini Live WS closed", { code, reason });
    // Do not hard-end here; Twilio stop/idle handles.
  });

  geminiWs.on("error", (err) => {
    geminiReady = false;
    geminiSetupComplete = false;
    logError(connId, "Gemini Live WS error", err);
  });

  // ---- Load sheets on connect (non-blocking) ----
  loadSheetsCache("OnConnect").catch(() => {});

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

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logAlways(`[TWILIO_START][${connId}] ${JSON.stringify(msg.start || {})}`);

      // Start recording
      if (MB_ENABLE_RECORDING && callSid) {
        const rec = await startRecordingIfEnabled(callSid, connId);
        if (rec.ok && rec.sid) {
          recordingSid = rec.sid;
          setRecordingForCall(callSid, { recordingSid: rec.sid, recordingUrl: rec.url || "" });
          logInfo(connId, "Recording started.", { recording_sid: recordingSid });
        } else {
          logInfo(connId, "Recording not started.", rec);
        }
      }

      // Idle checks
      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          idleWarningSent = true;
          // If opening not sent yet (rare), force it now
          if (!geminiOpeningSent) sendOpeningOnce();
        }

        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          plannedEnd = true;
          scheduleForceEndAfterGrace("idle_timeout").catch(() => {});
        }

        // TurnComplete hint (optional, helps latency)
        maybeSendTurnCompleteHint();
      }, 200);

      // Max call duration
      if (MB_MAX_CALL_MS > 0) {
        if (MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
          maxCallWarningTimeout = setTimeout(() => {}, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
        }
        maxCallTimeout = setTimeout(() => {
          plannedEnd = true;
          scheduleForceEndAfterGrace("max_call_duration").catch(() => {});
        }, MB_MAX_CALL_MS);
      }
    } else if (event === "media") {
      lastMediaTs = Date.now();
      const payload = msg.media?.payload;
      if (!payload) return;

      const now = Date.now();
      lastInboundAudioAt = now;

      // post-TTS cooldown
      if (now < noListenUntilTs) return;

      // Duplex policy: obey ENV only
      if (MB_HALF_DUPLEX) {
        if (botSpeaking || botTurnActive) return;
      } else if (!MB_ALLOW_BARGE_IN) {
        if (botTurnActive || botSpeaking) return;
      }

      // Gemini readiness gate (rate-limited log)
      if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN || !geminiSetupComplete) {
        if (MB_DEBUG && canLogGeminiNotReady()) logDebug(connId, "Dropped inbound: gemini not ready");
        return;
      }

      const pcm16kB64 = ulaw8kB64ToPcm16kB64(payload);
      const gm = { realtimeInput: { mediaChunks: [{ mimeType: GEMINI_AUDIO_IN_FORMAT, data: pcm16kB64 }] } };
      try {
        geminiWs.send(JSON.stringify(gm));
      } catch (_) {}
      return;
    } else if (event === "stop") {
      logAlways(`[TWILIO_STOP][${connId}] stream stopped`);
      if (!plannedEnd && !callEnded) {
        endCall("twilio_stop").catch(() => {});
      } else if (!callEnded) {
        endCall("twilio_stop_planned").catch(() => {});
      }
    }
  });

  twilioWs.on("close", () => {
    logAlways(`[TWILIO_CLOSE][${connId}] socket closed`);
    if (!callEnded) endCall("twilio_ws_closed").catch(() => {});
  });

  twilioWs.on("error", (err) => {
    logError(connId, "Twilio WS error", err);
    if (!callEnded) endCall("twilio_ws_error").catch(() => {});
  });
});

// -----------------------------
// Start server
// -----------------------------
server.listen(PORT, () => {
  console.log(`==> Your service is live 🎉`);
  console.log(`==> Available at your primary URL ${process.env.RENDER_EXTERNAL_URL || ""}`);

  // Preload sheets at startup (reduces opening delay)
  loadSheetsCache("Startup").catch((err) => console.error("[ERROR] Startup sheets load failed", err));
});
