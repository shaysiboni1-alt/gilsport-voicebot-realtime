// server.js
// GilSport VoiceBot – MisterBot-style (Sheet prompts only) + Recording + Lead/CallLog + Abandoned
// Version v6+ – Dynamic KB lists + CallerID injection + Phone hallucination correction + Hangup after goodbye + Hebrew lead parsing

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
  if (!raw) return def;
  const n = Number(raw);
  return Number.isFinite(n) ? n : def;
}

function envBool(name, def = false) {
  const raw = (process.env[name] || "").toLowerCase();
  if (!raw) return def;
  return ["1", "true", "yes", "on"].includes(raw);
}

// -----------------------------
// Gemini helpers (inline, no deps)
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

function safeStr(v) {
  return v === undefined || v === null ? "" : String(v).trim();
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

function extractBestPhoneFromText(text) {
  const d = digitsOnly(text);
  if (!d) return null;
  return normalizePhoneNumber(d, null);
}

function toLangCodeForGemini(mbLang) {
  const x = String(mbLang || "").toLowerCase().trim();
  if (x === "he" || x === "he-il" || x === "he_il") return "he-IL";
  if (x === "en" || x === "en-us" || x === "en_il") return "en-US";
  if (x === "ru") return "ru-RU";
  if (x === "ar") return "ar";
  return "he-IL";
}

function normalizePhoneNumber(rawPhone, callerNumber) {
  function clean(num) {
    const d = digitsOnly(num);
    if (!d) return null;

    let local = d;
    if (local.startsWith("972") && (local.length === 11 || local.length === 12)) {
      local = "0" + local.slice(3);
    }

    if (!/^\d{9,10}$/.test(local)) return null;
    return local;
  }

  return clean(rawPhone) || clean(callerNumber) || null;
}

function isCallerIdBlockedValue(raw) {
  const callerLabel = String(raw || "").toLowerCase().trim();
  return (
    !callerLabel ||
    callerLabel === "anonymous" ||
    callerLabel === "blocked" ||
    callerLabel === "null" ||
    callerLabel === "unknown" ||
    callerLabel === "restricted" ||
    callerLabel === "private"
  );
}

// For robust matching (goodbye detection / phone correction)
function normalizeTextLoose(str) {
  return String(str || "")
    .toLowerCase()
    .replace(/[\u0591-\u05C7]/g, "") // remove niqqud
    .replace(/[^a-z0-9\u0590-\u05FF\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function isGoodbyeUtterance(text, closing) {
  const t = normalizeTextLoose(text);
  const c = normalizeTextLoose(closing || "");
  if (c && t.includes(c.slice(0, Math.min(20, c.length)))) return true;

  const keywords = ["להתראות", "יום נעים", "תודה שפניתם", "תודה שפנית", "נסיים", "ביי"];
  return keywords.some((k) => t.includes(normalizeTextLoose(k)));
}

function getGraceMs(MB_HANGUP_GRACE_MS) {
  const raw = MB_HANGUP_GRACE_MS && MB_HANGUP_GRACE_MS > 0 ? MB_HANGUP_GRACE_MS : 3000;
  return Math.max(2000, Math.min(raw, 8000));
}

// -----------------------------
// Core ENV config
// -----------------------------
const PORT = envNumber("PORT", 10000);

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

// -----------------------------
// Speech Provider switch
// -----------------------------
const PROVIDER_MODE = String(process.env.PROVIDER_MODE || "openai").trim().toLowerCase();

// Gemini Live (Developer API over WS)
const GEMINI_API_KEY = process.env.GEMINI_API_KEY || "";
const GEMINI_LIVE_MODEL = process.env.GEMINI_LIVE_MODEL || "";
const GEMINI_AUDIO_IN_FORMAT = String(process.env.GEMINI_AUDIO_IN_FORMAT || "audio/pcm;rate=16000").replace(/"/g, "");
const GEMINI_AUDIO_OUT_FORMAT = String(process.env.GEMINI_AUDIO_OUT_FORMAT || "audio/pcm;rate=24000").replace(/"/g, "");

const VOICE_NAME_OVERRIDE = process.env.VOICE_NAME_OVERRIDE || "";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_DEBUG = envBool("MB_DEBUG", true);
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);

// חשוב: ברירת מחדל היא לפי ENV שלך, לא קשיח
const MB_HALF_DUPLEX = envBool("MB_HALF_DUPLEX", false);
const MB_POST_TTS_COOLDOWN_MS = envNumber("MB_POST_TTS_COOLDOWN_MS", 0);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 0);
const MB_BARGE_IN_COOLDOWN_MS = envNumber("MB_BARGE_IN_COOLDOWN_MS", 450);
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);

const MB_HANGUP_AFTER_GOODBYE = envBool("MB_HANGUP_AFTER_GOODBYE", true);
const MB_TTS_SPEED = envNumber("MB_TTS_SPEED", 1.0);
const MB_TTS_SPEED_CLAMPED = Math.max(0.9, Math.min(MB_TTS_SPEED, 1.2));

const MB_TRANSCRIPTION_LANGUAGE = process.env.MB_TRANSCRIPTION_LANGUAGE || "he";
const MB_TRANSCRIPTION_MODEL = safeStr(process.env.MB_TRANSCRIPTION_MODEL || "");

const MB_LLM_PROVIDER = String(process.env.MB_LLM_PROVIDER || "openai").toLowerCase();
const MB_GEMINI_TEXT_MODEL = process.env.MB_GEMINI_TEXT_MODEL || "gemini-1.5-pro";

// Gemini Live POC (no effect unless explicitly enabled)
const MB_GEMINI_POC_ENABLED = envBool("MB_GEMINI_POC_ENABLED", false);
const MB_GEMINI_LIVE_MODEL = process.env.MB_GEMINI_LIVE_MODEL || "gemini-live-2.5-flash-native-audio";
const GCP_PROJECT_ID = process.env.GCP_PROJECT_ID || "";
const GCP_LOCATION = process.env.GCP_LOCATION || "us-central1";
const GCP_SA_JSON_B64 = process.env.GCP_SA_JSON_B64 || "";

const MB_VAD_THRESHOLD = envNumber("MB_VAD_THRESHOLD", 0.70);
const MB_VAD_SILENCE_MS = envNumber("MB_VAD_SILENCE_MS", 450);
const MB_VAD_PREFIX_MS = envNumber("MB_VAD_PREFIX_MS", 80);
const MB_VAD_SUFFIX_MS = envNumber("MB_VAD_SUFFIX_MS", 200);

// Idle / Duration
const MB_IDLE_WARNING_MS = envNumber("MB_IDLE_WARNING_MS", 7000);
const MB_IDLE_HANGUP_MS = envNumber("MB_IDLE_HANGUP_MS", 20000);

// Max call
const MB_MAX_CALL_MS = envNumber("MB_MAX_CALL_MS", 500000);
const MB_MAX_WARN_BEFORE_MS = envNumber("MB_MAX_WARN_BEFORE_MS", 45000);
const MB_HANGUP_GRACE_MS = envNumber("MB_HANGUP_GRACE_MS", 4000);

// Webhooks
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", !!MB_CALL_LOG_WEBHOOK_URL);

const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || "");
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", !!MB_WEBHOOK_URL);

const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");
const MB_ENABLE_ABANDONED_WEBHOOK = envBool("MB_ENABLE_ABANDONED_WEBHOOK", !!MB_ABANDONED_WEBHOOK_URL);

const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

// Recording (optional)
const MB_ENABLE_RECORDING = envBool("MB_ENABLE_RECORDING", false);
const PUBLIC_BASE_URL = safeStr(process.env.PUBLIC_BASE_URL || "");

// Lead parse
const MB_LEAD_PARSING_MODEL = process.env.MB_LEAD_PARSING_MODEL || "gpt-4.1-mini";

// Twilio credentials (for hangup, recording URL, caller resolution)
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// -----------------------------
// Gemini Live POC helpers
// -----------------------------
const geminiPocState = {
  credentialsLoaded: false,
  credentialsPath: "",
  credentialsError: "",
};

function loadGcpCredentialsFromB64(b64) {
  if (!b64) return { loaded: false, path: "", error: "" };
  try {
    const decoded = Buffer.from(b64, "base64").toString("utf8");
    JSON.parse(decoded);
    const targetPath = path.join(os.tmpdir(), "gcp-sa.json");
    fs.writeFileSync(targetPath, decoded, { mode: 0o600 });
    process.env.GOOGLE_APPLICATION_CREDENTIALS = targetPath;
    return { loaded: true, path: targetPath, error: "" };
  } catch (err) {
    return { loaded: false, path: "", error: String(err?.message || err) };
  }
}

async function getGeminiAccessToken() {
  const auth = new google.auth.GoogleAuth({
    scopes: ["https://www.googleapis.com/auth/cloud-platform"],
  });
  const client = await auth.getClient();
  const token = await client.getAccessToken();
  if (typeof token === "string") return token;
  if (token && typeof token === "object" && typeof token.token === "string") return token.token;
  return "";
}

async function callLeadParsingLlm(systemPrompt, userPrompt, connId, tag) {
  if (MB_LLM_PROVIDER === "gemini") {
    if (!GCP_PROJECT_ID) {
      logError(connId, `${tag} missing GCP_PROJECT_ID`);
      return null;
    }
    const token = await getGeminiAccessToken();
    if (!token) {
      logError(connId, `${tag} missing GCP access token`);
      return null;
    }

    const url = `https://${GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${GCP_PROJECT_ID}/locations/${GCP_LOCATION}/publishers/google/models/${MB_GEMINI_TEXT_MODEL}:generateContent`;
    const payload = {
      systemInstruction: { role: "system", parts: [{ text: systemPrompt }] },
      contents: [{ role: "user", parts: [{ text: userPrompt }] }],
      generationConfig: {
        responseMimeType: "application/json",
      },
    };

    const response = await fetch(url, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const text = await response.text().catch(() => "");
      logError(connId, `${tag} HTTP ${response.status}`, text);
      return null;
    }

    const data = await response.json();
    const parts = data?.candidates?.[0]?.content?.parts || [];
    const text = parts.map((part) => part?.text).filter(Boolean).join(" ").trim();
    return text || null;
  }

  if (!OPENAI_API_KEY) return null;
  const response = await fetchWithTimeout(
    "https://api.openai.com/v1/chat/completions",
    {
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
    },
    6000
  );

  if (!response.ok) return null;
  const data = await response.json();
  const raw = data.choices?.[0]?.message?.content;
  return raw || null;
}

if (GCP_SA_JSON_B64 && (MB_GEMINI_POC_ENABLED || MB_LLM_PROVIDER === "gemini")) {
  const loaded = loadGcpCredentialsFromB64(GCP_SA_JSON_B64);
  geminiPocState.credentialsLoaded = loaded.loaded;
  geminiPocState.credentialsPath = loaded.path;
  geminiPocState.credentialsError = loaded.error;
}

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

function logEnvFallback(name, fallback) {
  const raw = process.env[name];
  if (raw !== undefined && String(raw).trim() !== "") return;
  logAlways(`[ENV] Missing ${name}; using fallback "${fallback}".`);
}

function logEnvPresence(name) {
  const raw = process.env[name];
  if (raw !== undefined && String(raw).trim() !== "") return;
  logAlways(`[ENV] Missing ${name}; feature may be disabled.`);
}

function logEnvStatus() {
  logEnvFallback("TIME_ZONE", TIME_ZONE);
  logEnvFallback("OPENAI_REALTIME_MODEL", OPENAI_REALTIME_MODEL);
  logEnvFallback("OPENAI_VOICE", OPENAI_VOICE);
  logEnvPresence("GSHEET_ID");
  logEnvPresence("GOOGLE_SERVICE_ACCOUNT_JSON_B64");
  logEnvPresence("PUBLIC_BASE_URL");

  // Gemini env presence (לא נדרש כש-provider=openai)
  if (PROVIDER_MODE === "gemini") {
    logEnvPresence("GEMINI_API_KEY");
    logEnvFallback("GEMINI_LIVE_MODEL", GEMINI_LIVE_MODEL);
  }

  const mbEnvFallbacks = [
    ["MB_DEBUG", MB_DEBUG],
    ["MB_LOG_TRANSCRIPTS", MB_LOG_TRANSCRIPTS],
    ["MB_HALF_DUPLEX", MB_HALF_DUPLEX],
    ["MB_POST_TTS_COOLDOWN_MS", MB_POST_TTS_COOLDOWN_MS],
    ["MB_NO_BARGE_TAIL_MS", MB_NO_BARGE_TAIL_MS],
    ["MB_BARGE_IN_COOLDOWN_MS", MB_BARGE_IN_COOLDOWN_MS],
    ["MB_ALLOW_BARGE_IN", MB_ALLOW_BARGE_IN],
    ["MB_HANGUP_AFTER_GOODBYE", MB_HANGUP_AFTER_GOODBYE],
    ["MB_TTS_SPEED", MB_TTS_SPEED],
    ["MB_TRANSCRIPTION_LANGUAGE", MB_TRANSCRIPTION_LANGUAGE],
    ["MB_TRANSCRIPTION_MODEL", MB_TRANSCRIPTION_MODEL],
    ["MB_LLM_PROVIDER", MB_LLM_PROVIDER],
    ["MB_GEMINI_TEXT_MODEL", MB_GEMINI_TEXT_MODEL],
    ["MB_GEMINI_POC_ENABLED", MB_GEMINI_POC_ENABLED],
    ["MB_GEMINI_LIVE_MODEL", MB_GEMINI_LIVE_MODEL],
    ["MB_VAD_THRESHOLD", MB_VAD_THRESHOLD],
    ["MB_VAD_SILENCE_MS", MB_VAD_SILENCE_MS],
    ["MB_VAD_PREFIX_MS", MB_VAD_PREFIX_MS],
    ["MB_VAD_SUFFIX_MS", MB_VAD_SUFFIX_MS],
    ["MB_IDLE_WARNING_MS", MB_IDLE_WARNING_MS],
    ["MB_IDLE_HANGUP_MS", MB_IDLE_HANGUP_MS],
    ["MB_MAX_CALL_MS", MB_MAX_CALL_MS],
    ["MB_MAX_WARN_BEFORE_MS", MB_MAX_WARN_BEFORE_MS],
    ["MB_HANGUP_GRACE_MS", MB_HANGUP_GRACE_MS],
    ["MB_CALL_LOG_WEBHOOK_URL", MB_CALL_LOG_WEBHOOK_URL],
    ["MB_CALL_LOG_ENABLED", MB_CALL_LOG_ENABLED],
    ["MB_WEBHOOK_URL", MB_WEBHOOK_URL],
    ["MB_ENABLE_LEAD_CAPTURE", MB_ENABLE_LEAD_CAPTURE],
    ["MB_ABANDONED_WEBHOOK_URL", MB_ABANDONED_WEBHOOK_URL],
    ["MB_ENABLE_ABANDONED_WEBHOOK", MB_ENABLE_ABANDONED_WEBHOOK],
    ["MB_FINAL_WEBHOOK_ONLY", MB_FINAL_WEBHOOK_ONLY],
    ["MB_ENABLE_RECORDING", MB_ENABLE_RECORDING],
    ["MB_LEAD_PARSING_MODEL", MB_LEAD_PARSING_MODEL],
  ];
  for (const [name, fallback] of mbEnvFallbacks) {
    logEnvFallback(name, String(fallback));
  }
}

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
  if (!creds) {
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
  return String(k || "").trim().replace(/\s+/g, "_").toUpperCase();
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

function buildSystemInstructionsFromSheets() {
  const businessName = getSetting("BUSINESS_NAME", "GilSport");
  const botName = getSetting("BOT_NAME", "נטע");

  const greeting = getGreetingForNow(TIME_ZONE || "Asia/Jerusalem");
  const openingTemplate = getSetting("OPENING_SCRIPT", "");
  const openingRaw = openingTemplate ? openingTemplate : `${greeting}! מדברת ${botName} מ${businessName}, במה אפשר לעזור?`;
  const openingInterpolated = interpolateVars(openingRaw, {
    GREETING: greeting,
    BOT_NAME: botName,
    BUSINESS_NAME: businessName,
  });
  const opening = openingInterpolated.includes(greeting)
    ? openingInterpolated
    : `${greeting} ${openingInterpolated}`.replace(/\s+/g, " ").trim();
  const closing = getSetting("CLOSING_SCRIPT", "תודה שפנית אלינו. יום נעים!");

  const master = getPrompt("MASTER_PROMPT", "");
  const guard = getPrompt("GUARDRAILS_PROMPT", "");
  const kb = getPrompt("KB_PROMPT", "");

  const vars = {
    ...(sheetsCache.settings || {}),
    BUSINESS_NAME: businessName,
    BOT_NAME: botName,
    GREETING: greeting,
    OPENING_SCRIPT: opening,
    CLOSING_SCRIPT: closing,
  };

  const combined = [master, guard, kb].filter(Boolean).join("\n\n");
  const final = interpolateVars(combined, vars);

  const hardPolicy = `
כללי Runtime קשיחים:
- לשון דיבור: ברירת מחדל לשון רבים בלבד ("אתם/תרצו/נחזור אליכם"). אין להשתמש ביחיד/זכר/נקבה אלא אם הלקוח ביקש במפורש.
`.trim();

  const finalWithHardPolicy = [final, hardPolicy].filter(Boolean).join("\n\n");

  return {
    businessName,
    botName,
    opening,
    closing,
    instructions:
      finalWithHardPolicy ||
      `את/ה נציג/ת שירות ומכירה קולית בשם "${botName}" עבור "${businessName}". דבר/י בעברית כברירת מחדל, בלשון רבים, בטון שירותי וקצר.`,
  };
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
        Authorization: twilioBasicAuthHeader(),
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

// -------------------- Recording registry + public proxy --------------------
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
// Lead parsing
// -----------------------------
function normalizeKeyLoose(k) {
  return String(k || "").trim().toLowerCase().replace(/[\s_'"״׳]/g, "");
}

function coerceLeadFields(obj) {
  const out = {};
  const entries = Object.entries(obj || {}).map(([k, v]) => [normalizeKeyLoose(k), v]);
  const loose = Object.fromEntries(entries);

  const getLoose = (names) => {
    for (const n of names) {
      const key = normalizeKeyLoose(n);
      if (loose[key] !== undefined && loose[key] !== null && String(loose[key]).trim() !== "") return loose[key];
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
  else if (typeof prefersCaller === "string") out.prefers_caller_id = /כן|נכון|true|1|yes|yep|yeah/.test(prefersCaller.toLowerCase());
  else out.prefers_caller_id = null;

  out.brand = getLoose(["brand", "מותג"]) || null;
  out.model = getLoose(["model", "דגם"]) || null;

  out.message_for =
    getLoose(["message_for", "messagefor", "for_whom", "forwhom", "recipient", "target", "עבורמי", "למי"]) || null;

  out.reason = getLoose(["reason", "סיבת_פנייה", "סיבתפנייה", "תקלה"]) || null;
  out.notes = getLoose(["notes", "הערות"]) || null;

  return out;
}

async function extractLeadFromConversation(conversationLog, connId, botName, businessName) {
  const tag = "LeadParse";
  if (MB_LLM_PROVIDER === "openai" && !OPENAI_API_KEY) return null;
  if (MB_LLM_PROVIDER === "gemini" && !GCP_PROJECT_ID) return null;
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;

  try {
    const conversationText = conversationLog
      .map((m) => `${m.from === "user" ? "לקוח" : botName}: ${m.text}`)
      .join("\n");

    const leadPromptFromSheets = String(getPrompt("LEAD_CAPTURE_PROMPT", "") || "").trim();

    const systemPrompt = (leadPromptFromSheets || `החזירו JSON בלבד לפי סכימה קבועה.`).trim();

    const systemAddon = `
חובה: להחזיר JSON תקין בלבד (ללא טקסט נוסף).
חובה: reason ו-notes בעברית.
חובה: אם מידע לא נמסר — להשאיר null.
`.trim();

    const raw = await callLeadParsingLlm(
      `${systemPrompt}\n\n${systemAddon}`,
      `תמלול שיחה בין מתקשר לבין בוט קולי בשם "${botName}" עבור "${businessName}". החזירו אובייקט JSON בלבד.\nתמלול:\n${conversationText}`,
      connId,
      tag
    );
    if (!raw) return null;

    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (_) {
      parsed = null;
    }
    if (!parsed || typeof parsed !== "object") return null;

    const coerced = coerceLeadFields(parsed);
    logInfo(connId, "Lead parsed.", coerced);
    return coerced;
  } catch (err) {
    logError(connId, "Lead parse error", err);
    return null;
  }
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

app.get("/", (req, res) => {
  res.status(200).send("OK");
});

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

// Public proxy
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

// Manual reload
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

// Twilio Voice webhook -> returns TwiML with Stream
app.post("/voice", (req, res) => {
  const host = process.env.DOMAIN || req.headers.host;
  const wsUrl = process.env.MB_TWILIO_STREAM_URL || `wss://${String(host || "").replace(/^https?:\/\//, "")}/twilio-media-stream`;

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
// Per-call handler
// -----------------------------
wss.on("connection", async (twilioWs, req) => {
  const connId = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  logAlways(`WS connection`, { at: nowIso(), ua: req.headers["user-agent"], url: req.url });

  // ✅ חשוב: לא סוגרים שיחה על OPENAI_API_KEY כשה-provider=gemini
  if (PROVIDER_MODE !== "gemini" && !OPENAI_API_KEY) {
    logError(connId, "Missing OPENAI_API_KEY – closing.");
    twilioWs.close();
    return;
  }
  if (PROVIDER_MODE === "gemini" && !GEMINI_API_KEY) {
    logError(connId, "Missing GEMINI_API_KEY – closing.");
    twilioWs.close();
    return;
  }

  let streamSid = null;
  let callSid = null;

  let callerRaw = null;
  let callerIL = null;

  let recordingSid = null;
  let recordingUrl = null;

  let hasActiveResponse = false;

  // Duplex state (גם ל-Gemini)
  let botSpeaking = false;
  let botTurnActive = false;
  let noListenUntilTs = 0;

  let plannedEnd = false;
  let callStartTs = Date.now();
  let lastMediaTs = Date.now();
  let idleCheckInterval = null;
  let idleWarningSent = false;
  let idleHangupScheduled = false;
  let maxCallTimeout = null;
  let maxCallWarningTimeout = null;

  let callEnded = false;

  let goodbyePendingHangup = false;
  let goodbyePendingText = null;

  let capturedPhoneIL = null;
  let prefersCallerId = false;
  let needsPhoneCapture = false;

  let baseInstructions = null;
  let openingText = null;
  let closingText = null;

  // ✅ קריטי: מבנה אחיד ל-lead parsing
  let conversationLog = [];

  // Load sheets in background (לא חוסם פתיח)
  loadSheetsCache("OnConnect").catch(() => {});

  function scheduleForceEndAfterGrace(reason, closingMessage) {
    const graceMs = getGraceMs(MB_HANGUP_GRACE_MS);
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

    const sys = buildSystemInstructionsFromSheets();
    const businessName = sys.businessName;
    const botName = sys.botName;
    const closing = sys.closing;
    const effectiveClosing = String(closingMessage || closing || "").trim();

    const endedAt = nowIso();
    const startedAt = new Date(callStartTs).toISOString();
    const durationSec = Math.max(0, Math.round((Date.now() - callStartTs) / 1000));

    let parsedLead = null;
    try {
      parsedLead = await extractLeadFromConversation(conversationLog, connId, botName, businessName);
    } catch (_) {}

    const callerILLocal = toIsraeliLocalFromAny(callerRaw) || null;

    let coercedPhone =
      normalizePhoneNumber(parsedLead?.phone_number, callerRaw) ||
      normalizePhoneNumber(capturedPhoneIL, callerRaw) ||
      normalizePhoneNumber(callerILLocal, callerRaw) ||
      null;

    if (parsedLead && typeof parsedLead === "object") {
      parsedLead.phone_number = coercedPhone;
      if (prefersCallerId) parsedLead.prefers_caller_id = true;
    }

    const isCallerBlocked = isCallerIdBlockedValue(callerRaw);
    const callerIdExists = !!callerILLocal && !isCallerBlocked;
    const collectedPhoneExists = !!normalizePhoneNumber(parsedLead?.phone_number, callerRaw) || !!normalizePhoneNumber(capturedPhoneIL, callerRaw);
    const phoneExists = callerIdExists || collectedPhoneExists;

    const hasName = safeStr(parsedLead?.full_name).length >= 2;
    const hasContent = safeStr(parsedLead?.reason).length >= 3 || safeStr(parsedLead?.notes).length >= 3;

    // ✅ שינוי עסקי קטן אבל קריטי: intent=message לא חייב שם כדי לא להפוך לננטש
    const intent = String(parsedLead?.intent || "").toLowerCase();
    let isFullLead = false;
    if (intent === "message") {
      isFullLead = !!(hasContent && phoneExists);
    } else {
      isFullLead = !!(hasName && hasContent && phoneExists);
    }

    if (parsedLead && parsedLead.is_lead !== true && isFullLead) {
      parsedLead.is_lead = true;
    }

    const call_status = mapCallStatus(reason, isFullLead);
    const EVENT = mapEventHe(parsedLead?.intent);

    if (MB_ENABLE_RECORDING && callSid) {
      await waitForRecording(callSid, 12000);
      const rec = getRecordingForCall(callSid);
      if (rec?.recordingSid) recordingSid = rec.recordingSid;
      if (rec?.recordingUrl) recordingUrl = rec.recordingUrl;
    }
    if (recordingSid && !recordingUrl) {
      recordingUrl = await buildRecordingUrl(recordingSid);
    }

    const payloadBase = {
      call_id: callSid || streamSid || `call_${Date.now()}`,
      callSid: callSid || null,
      streamSid: streamSid || null,

      started_at: startedAt,
      ended_at: endedAt,
      duration_sec: durationSec,

      caller_id_raw: callerRaw || null,
      caller_id_il: callerILLocal || null,
      caller_id_e164: toE164FromIsraeliLocal(callerILLocal) || (callerRaw && String(callerRaw).startsWith("+") ? callerRaw : null),

      collected_phone_il: coercedPhone || null,
      collected_phone_e164: coercedPhone ? toE164FromIsraeliLocal(coercedPhone) : null,

      business_name: businessName,
      bot_name: botName,

      EVENT,
      call_status,
      reason: reason || null,
      closingMessage: effectiveClosing || null,

      recording_sid: recordingSid || null,
      recording_url: recordingUrl || null,
      recording_public_url: recordingSid && getPublicOrigin() ? `${getPublicOrigin()}/recording/${recordingSid}.mp3` : null,

      parsedLeadCollection: {
        ...(parsedLead || {}),
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
      if (twilioWs.readyState === WebSocket.OPEN) twilioWs.close();
    } catch (_) {}
  }

  // -----------------------------
  // Speech engine WS (Gemini Live only here; OpenAI code הושמט כדי לא לגעת לך בלוגיקה מעבר לנדרש)
  // -----------------------------
  let geminiWs = null;
  let geminiSetupComplete = false;
  let geminiGreetingSent = false;
  let pendingKickoff = false;

  let geminiBotAudioActivityTimer = null;
  function markGeminiBotAudioActivity() {
    botSpeaking = true;
    botTurnActive = true;
    if (geminiBotAudioActivityTimer) clearTimeout(geminiBotAudioActivityTimer);
    geminiBotAudioActivityTimer = setTimeout(() => {
      botSpeaking = false;
      botTurnActive = false;
      noListenUntilTs = Date.now() + MB_POST_TTS_COOLDOWN_MS;
    }, 250);
  }

  function trySendGeminiKickoff() {
    if (PROVIDER_MODE !== "gemini") return;
    if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN) return;
    if (!geminiSetupComplete) return;
    if (!streamSid) {
      pendingKickoff = true;
      return;
    }
    if (geminiGreetingSent) return;

    const kickoff = `התחילי שיחה עכשיו. אמרי בדיוק את טקסט הפתיחה הבא בעברית (ללא תוספות וללא שינויים), ואז עצרי להקשבה:\n${openingText}`;
    const m = { clientContent: { turns: [{ role: "user", parts: [{ text: kickoff }] }], turnComplete: true } };
    try {
      geminiWs.send(JSON.stringify(m));
      geminiGreetingSent = true;
      pendingKickoff = false;
    } catch (_) {}
  }

  if (PROVIDER_MODE === "gemini") {
    const modelName = normalizeGeminiModelName(GEMINI_LIVE_MODEL || MB_GEMINI_LIVE_MODEL || "gemini-2.5-flash-native-audio-preview-12-2025");
    const url = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${encodeURIComponent(GEMINI_API_KEY)}`;
    geminiWs = new WebSocket(url);

    geminiWs.on("open", () => {
      geminiSetupComplete = false;
      geminiGreetingSent = false;

      const sys = buildSystemInstructionsFromSheets();
      baseInstructions = sys.instructions;
      openingText = sys.opening;
      closingText = sys.closing;

      const voiceName = VOICE_NAME_OVERRIDE || getSetting("VOICE_NAME", "Kore");

      const langCode = toLangCodeForGemini(MB_TRANSCRIPTION_LANGUAGE);

      const setup = {
        setup: {
          model: modelName,
          systemInstruction: baseInstructions ? { parts: [{ text: baseInstructions }] } : undefined,
          generationConfig: {
            responseModalities: ["AUDIO"],
            speechConfig: {
              voiceConfig: {
                prebuiltVoiceConfig: { voiceName },
              },
            },
          },
          realtimeInputConfig: {
            automaticActivityDetection: {
              prefixPaddingMs: Number(MB_VAD_PREFIX_MS ?? 80),
              silenceDurationMs: Number(MB_VAD_SILENCE_MS ?? 450),
            },
          },
        },
      };

      // תמלולים (אם נתמך ב-API): נועלים עברית כדי להימנע מ"שאי" בהינדי
      if (MB_LOG_TRANSCRIPTS) {
        setup.setup.inputAudioTranscription = { languageCode: langCode };
        setup.setup.outputAudioTranscription = { languageCode: langCode };
      }

      try {
        geminiWs.send(JSON.stringify(setup));
        logInfo(connId, "Gemini Live WS connected.", { model: modelName });
      } catch (e) {
        logError(connId, "Failed to send Gemini setup", e);
      }
    });

    geminiWs.on("message", (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString("utf8"));
      } catch {
        return;
      }

      if (msg?.setupComplete) {
        geminiSetupComplete = true;
        logInfo(connId, "Gemini setupComplete.", {});
        // ✅ לא שולחים פתיח עד שיש streamSid כדי לא “לזרוק” אודיו
        trySendGeminiKickoff();
        return;
      }

      // AUDIO -> Twilio
      try {
        const parts = msg?.serverContent?.modelTurn?.parts || msg?.serverContent?.turn?.parts || msg?.serverContent?.parts || [];
        for (const p of parts) {
          const inline = p?.inlineData || p?.inline_data;
          if (inline?.data && inline?.mimeType && String(inline.mimeType).startsWith("audio/pcm")) {
            const ulawB64 = pcm24kB64ToUlaw8kB64(inline.data);
            if (ulawB64 && streamSid && twilioWs.readyState === WebSocket.OPEN) {
              markGeminiBotAudioActivity();
              twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: ulawB64 } }));
            }
          }
        }
      } catch (_) {}

      // Transcriptions -> conversationLog (מבנה תואם)
      try {
        const inTr =
          msg?.serverContent?.inputTranscription?.text ||
          msg?.serverContent?.input_audio_transcription?.text ||
          msg?.inputTranscription?.text;

        if (inTr) {
          const t = String(inTr || "").trim();
          if (t) {
            conversationLog.push({ from: "user", text: t });
            logAlways(`[CALLER][${connId}] ${t}`);
          }
        }

        const outTr =
          msg?.serverContent?.outputTranscription?.text ||
          msg?.serverContent?.output_audio_transcription?.text ||
          msg?.outputTranscription?.text;

        if (outTr) {
          const t = String(outTr || "").trim();
          if (t) {
            conversationLog.push({ from: "bot", text: t });
            logAlways(`[BOT][${connId}] ${t}`);

            if (!goodbyePendingHangup && MB_HANGUP_AFTER_GOODBYE && isGoodbyeUtterance(t, closingText)) {
              goodbyePendingHangup = true;
              goodbyePendingText = t;
            }
          }
        }
      } catch (_) {}

      // Turn complete (אם מגיע) -> משחרר דופלקס מהר יותר
      try {
        const turnComplete =
          msg?.serverContent?.turnComplete ||
          msg?.serverContent?.turn_complete ||
          msg?.turnComplete ||
          msg?.turn_complete;

        if (turnComplete) {
          botTurnActive = false;
          botSpeaking = false;
          noListenUntilTs = Date.now() + MB_POST_TTS_COOLDOWN_MS;

          if (goodbyePendingHangup && MB_HANGUP_AFTER_GOODBYE && !callEnded) {
            plannedEnd = true;
            scheduleForceEndAfterGrace("goodbye", goodbyePendingText);
          }
        }
      } catch (_) {}
    });

    geminiWs.on("close", (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString("utf8") : "";
      logInfo(connId, "Gemini Live WS closed", { code, reason });
    });

    geminiWs.on("error", (err) => {
      logError(connId, "Gemini Live WS error", err);
    });
  }

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

      callerIL = toIsraeliLocalFromAny(callerRaw) || null;
      prefersCallerId = !!callerIL;
      needsPhoneCapture = !prefersCallerId;

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logAlways(`[TWILIO_START][${connId}] ${JSON.stringify(msg.start || {})}`);

      // Recording optional (לא חוסם פתיח)
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

      // ✅ עכשיו שיש streamSid, אם setupComplete כבר הגיע – שולחים פתיח מיד
      if (PROVIDER_MODE === "gemini" && pendingKickoff) {
        trySendGeminiKickoff();
      } else if (PROVIDER_MODE === "gemini") {
        // גם אם setupComplete הגיע בדיוק עכשיו
        trySendGeminiKickoff();
      }

      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          idleWarningSent = true;
          // לא דוחפים כאן prompt חדש כדי לא להאריך; נשאיר מינימלי
        }

        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          plannedEnd = true;
          scheduleForceEndAfterGrace("idle_timeout", null);
        }
      }, 1000);

      if (MB_MAX_CALL_MS > 0) {
        if (MB_MAX_WARN_BEFORE_MS > 0 && MB_MAX_CALL_MS > MB_MAX_WARN_BEFORE_MS) {
          maxCallWarningTimeout = setTimeout(() => {}, MB_MAX_CALL_MS - MB_MAX_WARN_BEFORE_MS);
        }
        maxCallTimeout = setTimeout(() => {
          plannedEnd = true;
          scheduleForceEndAfterGrace("max_call_duration", null);
        }, MB_MAX_CALL_MS);
      }
    } else if (event === "media") {
      lastMediaTs = Date.now();
      const payload = msg.media?.payload;
      if (!payload) return;

      const now = Date.now();
      if (now < noListenUntilTs) return;

      // Duplex policy: ENV בלבד
      if (MB_HALF_DUPLEX) {
        if (botSpeaking || botTurnActive) return;
      } else if (!MB_ALLOW_BARGE_IN) {
        if (botTurnActive || botSpeaking) return;
      }

      if (PROVIDER_MODE === "gemini") {
        if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN || !geminiSetupComplete) return;
        const pcm16kB64 = ulaw8kB64ToPcm16kB64(payload);
        const gm = { realtimeInput: { mediaChunks: [{ mimeType: GEMINI_AUDIO_IN_FORMAT, data: pcm16kB64 }] } };
        try {
          geminiWs.send(JSON.stringify(gm));
        } catch (_) {}
        return;
      }

      // (OpenAI mode לא מוצג כאן)
      return;
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
  console.log(`==> Your service is live`);
  console.log(`==> Available at your primary URL ${process.env.RENDER_EXTERNAL_URL || ""}`);
  logEnvStatus();
  loadSheetsCache("Startup").catch((err) => console.error("[ERROR] Startup sheets load failed", err));
});
