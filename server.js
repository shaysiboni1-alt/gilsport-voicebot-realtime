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
  return (v === undefined || v === null) ? "" : String(v).trim();
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

// Legacy: "0 5 0 3 ..." (kept for backward compat if needed)
function formatDigitsForTts(d) {
  const s = digitsOnly(d);
  if (!s) return "";
  return s.split("").join(" ");
}

// Speech-friendly formatting for phone validation: digit-by-digit with spaces.
// IMPORTANT: keep leading zeros and never drop digits.
// Example: 0503222237 -> "0 5 0 3 2 2 2 2 3 7"
function formatDigitsForHebrewSpeech(d) {
  const s = digitsOnly(d);
  return s ? s.split("").join(" ") : "";
}

// Return last 4 digits (keeps full number for storage, but speaks only last4 for privacy).
function last4Digits(d) {
  const s = digitsOnly(d);
  if (!s) return "";
  return s.length <= 4 ? s : s.slice(-4);
}

// Speech-friendly formatting for last4 validation: digit-by-digit with spaces.
// Example: 0503222237 -> "2 2 3 7"
function formatLast4ForHebrewSpeech(d) {
  const s = last4Digits(d);
  return s ? s.split("").join(" ") : "";
}


// Extract a digit string from Hebrew digit-words in model output.
// Supports common variants and strips niqqud/punctuation.
function extractHebrewSpokenDigits(text) {
  const s = String(text || "");
  if (!s) return "";

  // Remove niqqud and normalize separators to spaces
  const cleaned = s
    .replace(/[\u0591-\u05C7]/g, "")
    .replace(/["'`]/g, "")
    .replace(/[\(\)\[\]{}<>]/g, " ")
    .replace(/[.,;:!?/\\|\-_=+]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const map = {
    "אפס": "0",
    "אפסים": "0",
    "0": "0",
    "אחד": "1",
    "אחת": "1",
    "1": "1",
    "שתיים": "2",
    "שניים": "2",
    "שתים": "2",
    "2": "2",
    "שלוש": "3",
    "שלושה": "3",
    "3": "3",
    "ארבע": "4",
    "ארבעה": "4",
    "4": "4",
    "חמש": "5",
    "חמישה": "5",
    "5": "5",
    "שש": "6",
    "שישה": "6",
    "6": "6",
    "שבע": "7",
    "שבעה": "7",
    "7": "7",
    "שמונה": "8",
    "8": "8",
    "תשע": "9",
    "תשעה": "9",
    "9": "9",
  };

  const toks = cleaned.split(" ");
  let out = "";
  for (const tok of toks) {
    const t = tok.trim();
    if (!t) continue;
    if (map[t] != null) {
      out += map[t];
      continue;
    }
    // Also accept short numeric chunks like "03" "050" etc.
    const dd = digitsOnly(t);
    if (dd && dd.length <= 4) out += dd;
  }
  return out;
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

    // Basic validation: digits length 9-10 only (no advanced rules)
    if (!/^\d{9,10}$/.test(local)) return null;
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

// Additional speech validity gate to prevent background noise / fillers from triggering turns.
// Returns true when the transcript is too short / low-signal to be treated as a real user turn.
function isLowValueUtterance(raw) {
  const t = String(raw || "").trim();
  if (!t) return true;

  const norm = normalizeTextLoose(t); // lowercase, no niqqud, punctuation stripped
  if (!norm) return true;

  // Count words on the loose-normalized text
  const words = norm.split(" ").filter(Boolean);
  const hasDigits = /\d/.test(norm);
  const hasHebrew = /[\u0590-\u05FF]/.test(norm);
  const hasLatin = /[a-zA-Z]/.test(norm);
  const hebrewLetters = (norm.match(/[\u0590-\u05FF]/g) || []).length;
  const lettersOnly = norm.replace(/[^a-z֐-׿]/g, "");

  // Common fillers / acknowledgements (Hebrew + English) that should not advance the flow.
  const fillers = new Set([
    "אה",
    "אהה",
    "אממ",
    "הממ",
    "אהמ",
    "אוקיי",
    "אוקי",
    "בסדר",
    "כן",
    "לא",
    "טוב",
    "אחלה",
    "ok",
    "okay",
    "yes",
    "no",
  ]);

  if (words.length <= 1 && fillers.has(norm)) return true;

  // Latin-only short utterances without digits are noise for our flow.
  if (hasLatin && !hasHebrew && !hasDigits) return true;

  // Very short utterances with no digits are almost always noise / breath.
  if (!hasDigits) {
    if (words.length <= 2 && (!hasHebrew || hebrewLetters < 3)) return true;
    if (words.length < 2) return true;
    if (lettersOnly.length < 6) return true;
    if (norm.length < 6) return true;
  }

  return false;
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

function isEnglishRequest(text) {
  const t = normalizeTextLoose(text);
  if (!t) return false;
  return t.includes("english") || t.includes("in english") || t.includes("אנגלית") || t.includes("באנגלית");
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

function extractDigitSequences(text) {
  const s = String(text || "");
  const matches = s.match(/\d{7,12}/g);
  return matches || [];
}

function levenshtein(a, b) {
  a = String(a || "");
  b = String(b || "");
  if (a === b) return 0;
  const al = a.length;
  const bl = b.length;
  if (al === 0) return bl;
  if (bl === 0) return al;

  const dp = Array.from({ length: al + 1 }, () => new Array(bl + 1).fill(0));
  for (let i = 0; i <= al; i++) dp[i][0] = i;
  for (let j = 0; j <= bl; j++) dp[0][j] = j;

  for (let i = 1; i <= al; i++) {
    for (let j = 1; j <= bl; j++) {
      const cost = a[i - 1] === b[j - 1] ? 0 : 1;
      dp[i][j] = Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost);
    }
  }
  return dp[al][bl];
}

// -----------------------------
// Core ENV config
// -----------------------------
const PORT = envNumber("PORT", 10000);

const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const MB_DEBUG = envBool("MB_DEBUG", true);
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_NO_BARGE_TAIL_MS = envNumber("MB_NO_BARGE_TAIL_MS", 1600);
const MB_BARGE_IN_COOLDOWN_MS = envNumber("MB_BARGE_IN_COOLDOWN_MS", 500);
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

// Webhooks
const MB_CALL_LOG_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_CALL_LOG_WEBHOOK_URL || "");
const MB_CALL_LOG_ENABLED = envBool("MB_CALL_LOG_ENABLED", !!MB_CALL_LOG_WEBHOOK_URL);

const MB_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_WEBHOOK_URL || "");
const MB_ENABLE_LEAD_CAPTURE = envBool("MB_ENABLE_LEAD_CAPTURE", !!MB_WEBHOOK_URL);

const MB_ABANDONED_WEBHOOK_URL = sanitizeWebhookUrl(process.env.MB_ABANDONED_WEBHOOK_URL || "");
const MB_ENABLE_ABANDONED_WEBHOOK = envBool("MB_ENABLE_ABANDONED_WEBHOOK", !!MB_ABANDONED_WEBHOOK_URL);

const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

// Recording (optional)
// If enabled, the server will start a Twilio dual-channel recording for each call and expose
// a public proxy URL via PUBLIC_BASE_URL/recording/:sid.mp3 (Twilio auth is kept server-side).
// IMPORTANT: We only read these ENV vars; we do not rename or repurpose any existing ENV.
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

  const mbEnvFallbacks = [
    ["MB_DEBUG", MB_DEBUG],
    ["MB_LOG_TRANSCRIPTS", MB_LOG_TRANSCRIPTS],
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

function getInputAudioTranscriptionConfig(connId) {
  if (!MB_TRANSCRIPTION_MODEL) {
    logAlways("[ENV] Missing MB_TRANSCRIPTION_MODEL; transcription disabled");
    return null;
  }
  if (MB_TRANSCRIPTION_MODEL.toLowerCase().includes("whisper")) {
    logError(connId, "[ENV] MB_TRANSCRIPTION_MODEL contains whisper; transcription disabled", MB_TRANSCRIPTION_MODEL);
    return null;
  }
  return {
    model: MB_TRANSCRIPTION_MODEL,
    language: MB_TRANSCRIPTION_LANGUAGE,
  };
}

function isSttInvalidModelError(msg) {
  const err = msg && msg.error ? msg.error : {};
  const code = String(err.code || "").toLowerCase();
  const message = String(err.message || "").toLowerCase();
  const param = String(err.param || "").toLowerCase();
  const invalid = code.includes("invalid_model") || message.includes("invalid_model");
  const sttField = param.includes("input_audio_transcription") || message.includes("input_audio_transcription");
  return invalid && sttField;
}

// G.711 μ-law helpers for bot audio gain (output_audio_format is g711_ulaw).
const BOT_AUDIO_GAIN = 1.25;
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
  for (let expMask = 0x4000; (s & expMask) === 0 && exponent > 0; expMask >>= 1) {
    exponent -= 1;
  }
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

function buildSystemInstructionsFromSheets() {
  const businessName = getSetting("BUSINESS_NAME", "GilSport");
  const botName = getSetting("BOT_NAME", "נטע");

  const greeting = getGreetingForNow(TIME_ZONE || "Asia/Jerusalem");
  const openingTemplate = getSetting("OPENING_SCRIPT", "");
  const openingRaw = openingTemplate
    ? openingTemplate
    : `${greeting}! מדברת ${botName} מ${businessName}, במה אפשר לעזור?`;
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

    // Canonical runtime values (override SETTINGS if edited accidentally)
    BUSINESS_NAME: businessName,
    BOT_NAME: botName,
    GREETING: greeting,
    OPENING_SCRIPT: opening,
    CLOSING_SCRIPT: closing,

    // Explicit aliases (optional but clearer)
    WEBSITE_URL: getSetting("WEBSITE_URL", ""),
    MAIN_PHONE: getSetting("MAIN_PHONE", ""),
    WORKING_HOURS: getSetting("WORKING_HOURS", ""),
    BUSINESS_ADDRESS: getSetting("BUSINESS_ADDRESS", ""),
    AFTER_HOURS_DELIVERY_RULE: getSetting("AFTER_HOURS_DELIVERY_RULE", ""),
    SALES_COUPON_CODE: getSetting("SALES_COUPON_CODE", ""),
    PRICE_CLAIM_SENTENCE: getSetting("PRICE_CLAIM_SENTENCE", ""),
    NO_DATA_MESSAGE: getSetting("NO_DATA_MESSAGE", ""),

    // Dynamic lists derived from SETTINGS
    DELIVERY_PHONES_LIST: buildDeliveryPhonesList(sheetsCache.settings),
    IMPORTERS_LIST: buildImportersList(sheetsCache.settings),

    // Backward-compatible placeholders
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

  // Hard guardrails injected at runtime (even if PROMPTS are accidentally loosened).
  // Keep this minimal and deterministic.
  const hardPolicy = `
כללי Runtime קשיחים:
- קופון: מותר למסור קוד קופון אך ורק מתוך SETTINGS (SALES_COUPON_CODE). איסור מוחלט להמציא קוד. אם הערך חסר/ריק—להשתמש ב-NO_DATA_MESSAGE.
- טענת מחיר/השוואה: מותר להגיב אך ורק במשפט מתוך SETTINGS (PRICE_CLAIM_SENTENCE). איסור מוחלט להמציא משפט חלופי. אם הערך חסר/ריק—להשתמש ב-NO_DATA_MESSAGE.
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

// -------------------- Recording registry + public proxy --------------------
// Twilio sends RecordingStatusCallback events asynchronously; we keep the latest per CallSid.
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

function twilioBasicAuthHeader() {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return "";
  const b64 = Buffer.from(`${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`).toString("base64");
  return `Basic ${b64}`;
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
// Lead parsing via Chat Completions
// -----------------------------
function normalizeKeyLoose(k) {
  return String(k || "")
    .trim()
    .toLowerCase()
    .replace(/[\s_'"״׳]/g, "");
}

// Loose normalizer for free-text comparisons (Hebrew+Latin):
// - lowercases
// - strips niqqud
// - removes non-alphanumeric/non-Hebrew
function normalizeLoose(s) {
  return String(s || "")
    .toLowerCase()
    // niqqud + cantillation
    .replace(/[\u0591-\u05C7]/g, "")
    // keep hebrew/latin/digits
    .replace(/[^a-z0-9\u0590-\u05FF]+/g, "")
    .trim();
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
  // Message routing target (intent="message")
  out.message_for = getLoose([
    "message_for",
    "messagefor",
    "for_whom",
    "forwhom",
    "recipient",
    "target",
    "עבורמי",
    "עבור מי",
    "למי",
    "אל מי",
  ]) || null;
  out.reason = getLoose(["reason", "סיבת_פנייה", "סיבתפנייה", "תקלה"]) || null;
  out.notes = getLoose(["notes", "הערות"]) || null;

  return out;
}

function hasHebrew(s) {
  return /[\u0590-\u05FF]/.test(String(s || ""));
}

function mostlyLatin(s) {
  const t = String(s || "").trim();
  if (!t) return false;
  const letters = (t.match(/[a-zA-Z]/g) || []).length;
  const heb = (t.match(/[\u0590-\u05FF]/g) || []).length;
  return letters >= 3 && heb === 0;
}

async function ensureHebrewLeadFields(lead, conversationText, connId, botName, businessName) {
  // Goal: keep webhook human-friendly in Hebrew, even if the caller spoke some English.
  // We do NOT force Hebrew for phone numbers; only narrative fields.
  if (MB_LLM_PROVIDER === "openai" && !OPENAI_API_KEY) return lead;
  if (MB_LLM_PROVIDER === "gemini" && !GCP_PROJECT_ID) return lead;
  if (!lead || typeof lead !== "object") return lead;

  const needs = mostlyLatin(lead.full_name) || mostlyLatin(lead.reason) || mostlyLatin(lead.notes);
  if (!needs) return lead;

  try {
    const systemPrompt = `
You normalize call lead objects.
Return ONLY valid JSON (no extra text).
Output MUST keep the exact schema keys:
	{"is_lead":boolean,"intent":"sales"|"support"|"delivery"|"message"|"unknown","full_name":string|null,"phone_number":string|null,"prefers_caller_id":boolean|null,"brand":string|null,"model":string|null,"message_for":string|null,"reason":string|null,"notes":string|null}
Rules:
- Translate reason and notes to Hebrew, professional and clear.
- Expand notes to be explicit (what happened, what was requested, any number given such as importer/delivery).
- Keep phone_number exactly as-is.
- Keep full_name as spoken; if it is Latin and a Hebrew equivalent is clear from transcript, prefer Hebrew.
- If unknown, keep null.
JSON only.
`.trim();

    const userPrompt = `
Business: ${businessName}
Bot: ${botName}

Transcript:
${conversationText}

Current lead object:
${JSON.stringify(lead)}
`.trim();

    const raw = await callLeadParsingLlm(systemPrompt, userPrompt, connId, "LeadHebrew");
    if (!raw) return lead;
    let parsed;
    try {
      parsed = JSON.parse(raw);
    } catch (_) {
      parsed = null;
    }
    if (!parsed || typeof parsed !== "object") return lead;
    const coerced = coerceLeadFields(parsed);
    // Prefer ensured Hebrew narratives, but don't erase an existing Hebrew reason/notes.
    if (coerced.reason && (!lead.reason || mostlyLatin(lead.reason) || !hasHebrew(lead.reason))) lead.reason = coerced.reason;
    if (coerced.notes && (!lead.notes || mostlyLatin(lead.notes) || !hasHebrew(lead.notes))) lead.notes = coerced.notes;
    if (coerced.full_name && mostlyLatin(lead.full_name) && hasHebrew(coerced.full_name)) lead.full_name = coerced.full_name;
    return lead;
  } catch (err) {
    logError(connId, "Lead Hebrew normalize error", err);
    return lead;
  }
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

    const systemPrompt = (
      leadPromptFromSheets ||
      `החזירו JSON בלבד לפי סכימה קבועה.`
    ).trim();

    const systemAddon = `
חובה: להחזיר JSON תקין בלבד (ללא טקסט נוסף).
חובה: השדות reason ו-notes בעברית (אפשר לתרגם מתוכן השיחה), כולל ציון מפורש אם נמסר מספר יבואן/מוביל.
	חובה: phone_number (אם קיים) חייב להיות מספר ישראלי מלא (0XXXXXXXXX/0XXXXXXXXXX לאחר normalize) — 4 ספרות אחרונות בלבד אינן טלפון תקין ואסור להחזיר אותן כשדה phone_number.
	חובה: אם intent="message" — מלאו message_for (עבור מי ההודעה) במפורש.
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
    const ensured = await ensureHebrewLeadFields(coerced, conversationText, connId, botName, businessName);
    logInfo(connId, "Lead parsed.", ensured);
    return ensured;
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

function mapEventHe(intent) {
  const i = String(intent || "").toLowerCase().trim();
  if (i === "support") return "שירות לקוחות";
  if (i === "sales") return "מכירות";
  if (i === "delivery") return "אספקה ומשלוחים";
  if (i === "message") return "הודעה";
  return "לא ידוע";
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

// -------------------- Gemini Live POC (disabled by default) --------------------
app.get("/poc/gemini/health", (req, res) => {
  res.status(200).json({
    enabled: MB_GEMINI_POC_ENABLED,
    project: GCP_PROJECT_ID || "",
    location: GCP_LOCATION,
    model: MB_GEMINI_LIVE_MODEL,
    has_credentials: !!(process.env.GOOGLE_APPLICATION_CREDENTIALS || GCP_SA_JSON_B64),
    credentials_loaded: geminiPocState.credentialsLoaded,
    timestamp: nowIso(),
  });
});

app.post("/poc/gemini/live-test", async (req, res) => {
  if (!MB_GEMINI_POC_ENABLED) {
    return res.status(404).json({ ok: false, error: "Gemini POC disabled" });
  }
  if (!GCP_PROJECT_ID) {
    return res.status(400).json({ ok: false, error: "Missing GCP_PROJECT_ID" });
  }

  const speakText = safeStr(req.body?.speak_text || "שלום, מדברת נטע. איך אפשר לעזור?");
  const expectLanguage = safeStr(req.body?.expect_language || "he-IL");
  const startAt = Date.now();
  let firstAudioAt = null;
  let audioBytes = 0;
  let audioFrames = 0;
  let textParts = [];
  let streamError = "";

  try {
    const token = await getGeminiAccessToken();
    if (!token) {
      return res.status(500).json({ ok: false, error: "Failed to obtain GCP access token" });
    }

    const url = `wss://${GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${GCP_PROJECT_ID}/locations/${GCP_LOCATION}/publishers/google/models/${MB_GEMINI_LIVE_MODEL}:streamGenerateContent`;
    const setupMessage = {
      setup: {
        model: MB_GEMINI_LIVE_MODEL,
        generation_config: {
          response_modalities: ["AUDIO"],
          audio_config: {
            audio_encoding: "MULAW",
            sample_rate_hertz: 8000,
          },
        },
      },
    };
    const contentMessage = {
      client_content: {
        turns: [
          {
            role: "user",
            parts: [
              {
                text: `Please respond in ${expectLanguage} with natural, fluent Hebrew speech. Speak this text: ${speakText}`,
              },
            ],
          },
        ],
        turn_complete: true,
      },
    };

    await new Promise((resolve) => {
      let resolved = false;
      const ws = new WebSocket(url, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      });

      const finish = () => {
        if (resolved) return;
        resolved = true;
        try {
          ws.close();
        } catch (_) {
          // ignore
        }
        resolve();
      };

      const timeout = setTimeout(() => {
        streamError = streamError || "Gemini Live timeout";
        finish();
      }, 20000);

      ws.on("open", () => {
        ws.send(JSON.stringify(setupMessage));
        ws.send(JSON.stringify(contentMessage));
      });

      ws.on("message", (data) => {
        let message;
        try {
          const raw = typeof data === "string" ? data : data.toString("utf8");
          message = JSON.parse(raw);
        } catch (_) {
          return;
        }

        const serverContent = message?.serverContent || message?.server_content;
        const modelTurn = serverContent?.modelTurn || serverContent?.model_turn;
        const parts = modelTurn?.parts || [];
        for (const part of parts) {
          if (part?.text) {
            textParts.push(part.text);
          }
          const inlineData = part?.inlineData || part?.inline_data;
          const audioData = inlineData?.data;
          if (audioData) {
            if (!firstAudioAt) firstAudioAt = Date.now();
            const buf = Buffer.from(audioData, "base64");
            audioBytes += buf.length;
            audioFrames += 1;
          }
        }

        const turnComplete =
          serverContent?.turnComplete ||
          serverContent?.turn_complete ||
          message?.turnComplete ||
          message?.turn_complete;

        if (turnComplete) {
          clearTimeout(timeout);
          finish();
        }
      });

      ws.on("error", (err) => {
        streamError = String(err?.message || err);
        clearTimeout(timeout);
        finish();
      });

      ws.on("close", () => {
        clearTimeout(timeout);
        finish();
      });
    });
  } catch (err) {
    streamError = String(err?.message || err);
  }

  const finishedAt = Date.now();
  const latencyMs = firstAudioAt ? firstAudioAt - startAt : null;
  const audioReceived = audioBytes > 0;

  res.status(200).json({
    ok: !streamError,
    session_opened: !streamError,
    model: MB_GEMINI_LIVE_MODEL,
    latency_ms: latencyMs,
    total_duration_ms: finishedAt - startAt,
    audio_received: audioReceived || audioFrames > 0,
    audio_bytes: audioBytes,
    text: textParts.join(" ").trim(),
    error: streamError || undefined,
  });
});

app.post("/poc/gemini/stt-test", async (req, res) => {
  if (!MB_GEMINI_POC_ENABLED) {
    return res.status(404).json({ ok: false, error: "Gemini POC disabled" });
  }
  if (!GCP_PROJECT_ID) {
    return res.status(400).json({ ok: false, error: "Missing GCP_PROJECT_ID" });
  }

  const audioB64 = safeStr(req.body?.audio_b64 || "");
  const mimeType = safeStr(req.body?.mime_type || "audio/wav");
  const prompt = safeStr(req.body?.prompt || "Transcribe this audio in Hebrew.");
  if (!audioB64) {
    return res.status(400).json({ ok: false, error: "Missing audio_b64" });
  }

  try {
    const token = await getGeminiAccessToken();
    if (!token) {
      return res.status(500).json({ ok: false, error: "Failed to obtain GCP access token" });
    }

    const url = `https://${GCP_LOCATION}-aiplatform.googleapis.com/v1beta1/projects/${GCP_PROJECT_ID}/locations/${GCP_LOCATION}/publishers/google/models/${MB_GEMINI_LIVE_MODEL}:generateContent`;
    const payload = {
      contents: [
        {
          role: "user",
          parts: [
            { text: prompt },
            { inlineData: { data: audioB64, mimeType } },
          ],
        },
      ],
      generationConfig: {
        responseModalities: ["TEXT"],
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
      const errText = await response.text().catch(() => "");
      return res.status(response.status || 500).json({
        ok: false,
        error: errText || "Gemini STT request failed",
        status: response.status,
      });
    }

    const data = await response.json();
    const parts = data?.candidates?.[0]?.content?.parts || [];
    const transcript = parts.map((part) => part?.text).filter(Boolean).join(" ").trim();

    return res.status(200).json({
      ok: true,
      model: MB_GEMINI_LIVE_MODEL,
      transcript,
    });
  } catch (err) {
    return res.status(500).json({ ok: false, error: String(err?.message || err) });
  }
});

// -------------------- Recording callback + public proxy --------------------
// Twilio posts recording status updates asynchronously. We store RecordingSid/RecordingUrl
// keyed by CallSid and expose a public, auth-free proxy URL for consumption by your webhook.
app.post("/twilio-recording-callback", (req, res) => {
  try {
    const callSid = safeStr(req.body?.CallSid || "");
    const recordingSid = safeStr(req.body?.RecordingSid || "");
    const recordingUrl = safeStr(req.body?.RecordingUrl || "");

    if (callSid) {
      setRecordingForCall(callSid, { recordingSid, recordingUrl });
      if (MB_DEBUG) console.log("[INFO] [RECORDING_CALLBACK]", { callSid, recordingSid, hasUrl: !!recordingUrl });
    }
  } catch (e) {
    // ignore
  }

  res.status(200).send("OK");
});

// Public proxy for Twilio recording MP3. Keeps Twilio auth server-side.
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

// Manual reload of Google Sheets cache (admin)
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
    res.status(500).json({
      ok: false,
      error: String(err?.message || err),
    });
  }
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
// Lead post-processing helpers (minimal, deterministic)
// -----------------------------
function extractMessageForFromConversation(conversationLog) {
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return null;
  for (let i = 0; i < conversationLog.length - 1; i++) {
    const cur = conversationLog[i];
    const next = conversationLog[i + 1];
    if (!cur || !next) continue;
    if (cur.from !== "bot" || next.from !== "user") continue;
    const b = normalizeTextLoose(cur.text || "");
    if (!b) continue;
    // Any variant of "for whom is the message"
    if (b.includes(normalizeTextLoose("עבור מי")) || b.includes(normalizeTextLoose("למי ההודעה")) || b.includes(normalizeTextLoose("ההודעה מיועדת"))) {
      const ans = safeStr(next.text);
      return ans && ans.length >= 2 ? ans : null;
    }
  }
  return null;
}

function extractBrandModelFallback(conversationLog) {
  const out = { brand: null, model: null };
  if (!Array.isArray(conversationLog) || conversationLog.length === 0) return out;

  const userText = conversationLog
    .filter((m) => m && m.from === "user")
    .map((m) => String(m.text || "").trim())
    .filter(Boolean)
    .join(" \n");

  const norm = normalizeTextLoose(userText);
  if (!norm) return out;

  // If the caller explicitly says there is no brand/model, capture that verbatim-ish.
  if (/(אין|לא\s*יודע|לא\s*בטוח).{0,12}(מותג)/.test(norm)) out.brand = "אין מותג";
  if (/(אין|לא\s*יודע|לא\s*בטוח).{0,12}(דגם)/.test(norm)) out.model = "אין דגם";

  // If the caller gave a product type as "model" (common: "אופניים", "הליכון"), keep it.
  // We do not attempt deep NLP here; this is only a fallback when fields are empty.
  if (!out.model) {
    const candidates = (userText.match(/[\u0590-\u05FF]{3,}/g) || []).slice(0, 8);
    if (candidates.length) out.model = candidates.join(" ").slice(0, 60);
  }

  return out;
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

  let streamSid = null;
  let callSid = null;
  let callerRaw = null;
  let callerIL = null;

  let recordingSid = null;
  let recordingUrl = null;

  let openAiReady = false;
  let hasActiveResponse = false;
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

  // When the bot says the closing line, we want to hang up automatically (so Twilio doesn't keep the line open)
  let goodbyePendingHangup = false;
  let goodbyePendingText = null;

  let capturedPhoneIL = null;
  let phoneHallucinationCorrectionSent = false;

  let preferredGrammar = "plural";
  let prefersCallerId = false;
  let needsPhoneCapture = false;
  let genderInstructionSent = false;
  let baseInstructions = null;

  let conversationLog = [];

  // Allowed business phone numbers (delivery/importers/main + caller-id). Used to prevent / correct hallucinated digits.
  let allowedPhonesDigits = new Set();

  function refreshAllowedPhonesFromSheets() {
    const s = sheetsCache.settings || {};
    const out = new Set();

    const pushMaybe = (val) => {
      const d = digitsOnly(val);
      if (!d) return;
      if (d.length >= 7 && d.length <= 12) out.add(d);
    };

    pushMaybe(getSetting("MAIN_PHONE", ""));

    for (const [k, v] of Object.entries(s)) {
      if (!k) continue;
      const key = String(k).toUpperCase();
      if (key.endsWith("_PHONE") || key.startsWith("DELIVERY_PHONE_")) pushMaybe(v);
    }

    allowedPhonesDigits = out;
  }

  function bestAllowedPhoneMatch(seqDigits) {
    const seq = digitsOnly(seqDigits);
    if (!seq || seq.length < 7 || seq.length > 12) return null;

    let best = null;
    let bestScore = Infinity;

    for (const a of allowedPhonesDigits) {
      if (!a) continue;
      if (a === seq) return null;

      // Off-by-one where one string contains the other
      if (Math.abs(a.length - seq.length) <= 1 && (a.includes(seq) || seq.includes(a))) {
        return { said: seq, expected: a, score: 0 };
      }

      if (Math.abs(a.length - seq.length) > 2) continue;
      const dist = levenshtein(a, seq);
      if (dist < bestScore) {
        bestScore = dist;
        best = a;
      }
    }

    if (!best) return null;
    if (bestScore <= 1) return { said: seq, expected: best, score: bestScore };
    return null;
  }

  function maybeCorrectHallucinatedPhone(botText) {
    if (phoneHallucinationCorrectionSent) return false;
    if (!botText) return false;
    if (!allowedPhonesDigits || allowedPhonesDigits.size === 0) return false;

    const seqs = extractDigitSequences(botText);
    // Also handle common speech formatting like "0 5 0 ..." or Hebrew digit-words.
    const joined = digitsOnly(botText);
    if (joined && joined.length >= 7 && joined.length <= 12) seqs.push(joined);
    const hebSpoken = extractHebrewSpokenDigits(botText);
    if (hebSpoken && hebSpoken.length >= 7 && hebSpoken.length <= 12) seqs.push(hebSpoken);
    for (const seq of seqs) {
      const m = bestAllowedPhoneMatch(seq);
      if (!m) continue;

      phoneHallucinationCorrectionSent = true;
      const expectedSpoken = formatLast4ForHebrewSpeech(m.expected);

      logError(connId, "Model spoke a business phone number incorrectly; forcing correction.", m);

      // Attempt to cancel only if a response is currently active; otherwise ignore.
      if (hasActiveResponse) {
        try {
          openAiWs.send(JSON.stringify({ type: "response.cancel" }));
        } catch (_) {}
      }

      hasActiveResponse = false;
      botSpeaking = false;
      botTurnActive = false;

      sendModelPrompt(
        openAiWs,
        `תיקון חובה: 4 הספרות האחרונות הנכונות הן "${expectedSpoken}". הקריאו אותן בדיוק ספרה-ספרה ושאלו: "זה נכון?" בלי תוספות.`,
        "business_phone_correction"
      );

      return true;
    }
    return false;
  }

  function isGoodbyeUtterance(text) {
    const { closing } = buildSystemInstructionsFromSheets();
    const t = normalizeTextLoose(text);
    const c = normalizeTextLoose(closing);
    if (c && t.includes(c.slice(0, Math.min(20, c.length)))) return true;

    const keywords = ["להתראות", "יום נעים", "תודה שפניתם", "תודה שפנית", "נסיים", "ביי"];
    return keywords.some((k) => t.includes(normalizeTextLoose(k)));
  }

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
    if (/(אני\s*(?:גבר|בן)|פנה\s*אלי\s*בלשון\s*זכר|בלשון\s*זכר|תדבר\s*אלי\s*בלשון\s*זכר)/.test(t)) return "masculine";
    if (/(אני\s*(?:אישה|בת)|פני\s*אלי\s*בלשון\s*נקבה|בלשון\s*נקבה|תדברי\s*אלי\s*בלשון\s*נקבה)/.test(t)) return "feminine";
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

  const sessionAddonQueue = [];
  function queueSessionAddon(text, label) {
    if (!text) return;
    sessionAddonQueue.push({ text, label: label || "addon" });
    flushSessionAddons();
  }
  function flushSessionAddons() {
    if (!openAiReady || openAiWs.readyState !== WebSocket.OPEN) return;
    if (!baseInstructions) return;
    if (sessionAddonQueue.length === 0) return;

    const combinedAddons = sessionAddonQueue.map((x) => x.text).join("\n\n");
    sessionAddonQueue.length = 0;
    updateSessionInstructions(openAiWs, combinedAddons, "queued_addons");
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

    const callerILLocal = toIsraeliLocalFromAny(callerRaw) || null;

    let coercedPhone =
      normalizePhoneNumber(parsedLead?.phone_number, callerRaw) ||
      normalizePhoneNumber(capturedPhoneIL, callerRaw) ||
      normalizePhoneNumber(callerILLocal, callerRaw) ||
      null;

    if (prefersCallerId) {
      coercedPhone = null;
    }

    if (parsedLead && typeof parsedLead === "object") {
      parsedLead.phone_number = coercedPhone;
      if (prefersCallerId) parsedLead.prefers_caller_id = true;

	  // Ensure message target is captured as a separate field (intent="message").
	  if (String(parsedLead.intent || "").toLowerCase() === "message") {
	    if (!safeStr(parsedLead.message_for)) {
	      parsedLead.message_for = extractMessageForFromConversation(conversationLog) || null;
	    }
	  }

	  // Ensure brand/model are never null for sales/support: if caller said "no brand/model", keep that.
	  const intent = String(parsedLead.intent || "").toLowerCase();
	  if (intent === "sales" || intent === "support") {
	    const fb = extractBrandModelFallback(conversationLog);
	    if (!safeStr(parsedLead.brand)) parsedLead.brand = fb.brand || null;
	    if (!safeStr(parsedLead.model)) parsedLead.model = fb.model || null;
	  }
    }

    // If the model forgot to set is_lead=true on a completed "message" flow,
    // we still want the webhook to go to the FINAL webhook (not ABANDONED).
    // Keep this override narrowly scoped to intent="message" to avoid changing
    // behavior in other flows.
    const isCallerBlocked = isCallerIdBlockedValue(callerRaw);

    const hasName = safeStr(parsedLead?.full_name).length >= 2;
    const hasContent = safeStr(parsedLead?.reason || parsedLead?.notes).length > 0;
    const hasPhone = !!coercedPhone && digitsOnly(coercedPhone).length >= 9;
    const intent = String(parsedLead?.intent || "").toLowerCase();
    const hasMessageFor = safeStr(parsedLead?.message_for).length > 0;
    const phoneRequired = isCallerBlocked;

    const isFullLead = !!(
      hasName &&
      hasContent &&
      (!phoneRequired || hasPhone) &&
      (intent !== "message" || hasMessageFor)
    );

    if (parsedLead && parsedLead.is_lead !== true && isFullLead) {
      parsedLead.is_lead = true;
    }
    const call_status = mapCallStatus(reason, plannedEnd);

    // Wait (briefly) for Twilio recording callback to arrive, so webhooks can include a recording link.
    if (MB_ENABLE_RECORDING && callSid) {
      await waitForRecording(callSid, 12000);
      const rec = getRecordingForCall(callSid);
      if (rec?.recordingSid) recordingSid = rec.recordingSid;
      if (rec?.recordingUrl) recordingUrl = rec.recordingUrl;
    }

    if (recordingSid && !recordingUrl) {
      recordingUrl = await buildRecordingUrl(recordingSid);
    }

    const EVENT = mapEventHe(parsedLead?.intent);

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

      EVENT,
      call_status,
      reason: reason || null,
      closingMessage: effectiveClosing || null,

      recording_sid: recordingSid || null,
      recording_url: recordingUrl || null,

      // Public, auth-free proxy URL (Twilio media is fetched server-side using TWILIO_AUTH_TOKEN)
      recording_public_url:
        recordingSid && getPublicOrigin() ? `${getPublicOrigin()}/recording/${recordingSid}.mp3` : null,

      // IMPORTANT: Do NOT include raw transcripts / conversation logs in webhook payloads.

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

    // IMPORTANT: Close OpenAI WS when the call ends (prevents later session_expired noise)
    try {
      if (openAiWs) {
        if (openAiWs.readyState === WebSocket.OPEN) {
          openAiWs.close(1000, "call_end");
        } else if (openAiWs.readyState === WebSocket.CONNECTING) {
          if (typeof openAiWs.terminate === "function") openAiWs.terminate();
        }
      }
    } catch (_) {}

    try {
      if (twilioWs.readyState === WebSocket.OPEN) twilioWs.close();
    } catch (_) {}
  }

  // Load sheets early and refresh allowed phone list once available
  loadSheetsCache("OnConnect")
    .then(() => refreshAllowedPhonesFromSheets())
    .catch(() => {});

  // -----------------------------
  // OpenAI Realtime WS
  // -----------------------------
  const openAiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  let sttEnabled = false;
  let sttDisabledByError = false;

  openAiWs.on("open", () => {
    openAiReady = true;
    const { opening, instructions } = buildSystemInstructionsFromSheets();
    baseInstructions = instructions;
    const inputAudioTranscription = getInputAudioTranscriptionConfig(connId);
    sttEnabled = !!inputAudioTranscription;

    const effectiveSilenceMs = MB_VAD_SILENCE_MS + MB_VAD_SUFFIX_MS;

    openAiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
          model: OPENAI_REALTIME_MODEL,
          modalities: ["audio", "text"],
          voice: OPENAI_VOICE,
          speed: MB_TTS_SPEED_CLAMPED,
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          ...(inputAudioTranscription ? { input_audio_transcription: inputAudioTranscription } : {}),
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

    if (MB_DEBUG) logInfo(connId, "TTS speed set", { speed: MB_TTS_SPEED_CLAMPED });

    flushSessionAddons();

    sendModelPrompt(
      openAiWs,
      `פתחו את השיחה עם הלקוח במשפט הבא (אפשר לשנות מעט את הניסוח אבל לא להאריך): "${opening}" ואז עצרו והמתינו לתשובתם.`,
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
        noListenUntilTs = Date.now() + (MB_ALLOW_BARGE_IN ? MB_BARGE_IN_COOLDOWN_MS : MB_NO_BARGE_TAIL_MS);
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

          // 1) Correct hallucinated business numbers (importer/delivery/main/caller-id) before anything else
          if (maybeCorrectHallucinatedPhone(text)) {
            currentBotText = "";
            break;
          }

          // 2) If the bot said a goodbye, hang up after audio finishes
          if (!goodbyePendingHangup && MB_HANGUP_AFTER_GOODBYE && isGoodbyeUtterance(text)) {
            goodbyePendingHangup = true;
            goodbyePendingText = text;
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
        noListenUntilTs = now + (MB_ALLOW_BARGE_IN ? MB_BARGE_IN_COOLDOWN_MS : MB_NO_BARGE_TAIL_MS);

        if (twilioWs.readyState === WebSocket.OPEN) {
          const boosted = applyGainToUlawBase64(b64, BOT_AUDIO_GAIN);
          twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: boosted } }));
        }
        break;
      }

      case "response.audio.done":
        botSpeaking = false;
        botTurnActive = false;

        // Always apply a small post-TTS cooldown to avoid echo / background noise false turns.
        noListenUntilTs = Date.now() + (MB_ALLOW_BARGE_IN ? MB_BARGE_IN_COOLDOWN_MS : MB_NO_BARGE_TAIL_MS);

        if (goodbyePendingHangup && MB_HANGUP_AFTER_GOODBYE && !callEnded) {
          plannedEnd = true;
          scheduleForceEndAfterGrace("goodbye", goodbyePendingText);
        }
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
        const hasHebrew = /[\u0590-\u05FF]/.test(raw);
        const hasLatin = /[a-zA-Z]/.test(raw);
        const englishRequested =
          isEnglishRequest(raw) ||
          conversationLog.some((m) => m.from === "user" && isEnglishRequest(m.text || ""));

        if (hasLatin && !hasHebrew && !englishRequested) {
          break;
        }
        if (isTranscriptGarbage(raw, hasRealUserYet)) {
          logDebug(connId, `Filtered garbage transcript: "${raw}"`);
          break;
        }

        // Additional gate: ignore low-signal utterances (breath/noise/fillers) so the bot does not respond "on its own".
        if (isLowValueUtterance(raw)) {
          logDebug(connId, `Filtered low-value utterance: "${raw}"`);
          break;
        }

        const t = raw.replace(/\s+/g, " ").replace(/\s+([,.:;!?])/g, "$1").trim();
        if (!t) break;

        conversationLog.push({ from: "user", text: t });
        logAlways(`[CALLER][${connId}] ${t}`);

        const gPref = detectGenderPreference(t);
        if (gPref && gPref !== preferredGrammar) {
          preferredGrammar = gPref;
          const addon =
            gPref === "masculine"
              ? 'הלקוח ביקש לשון זכר. דברו בלשון זכר ברבים ("אתם"). אל תתנצלו ואל תדגישו את זה, פשוט התאימו ניסוח.'
              : 'הלקוחה ביקשה לשון נקבה. דברו בלשון נקבה ברבים ("אתן"). אל תתנצלו ואל תדגישו את זה, פשוט התאימו ניסוח.';
          if (!genderInstructionSent) {
            genderInstructionSent = true;
            updateSessionInstructions(openAiWs, addon, "gender_pref");
          }
        }

        const phoneFromSpeech = needsPhoneCapture ? extractBestPhoneFromText(t) : null;
        if (phoneFromSpeech) {
          capturedPhoneIL = phoneFromSpeech;
          logDebug(connId, `Captured phone from speech: ${capturedPhoneIL}`);
          try {
            const d = digitsOnly(capturedPhoneIL);
            if (d && d.length >= 7 && d.length <= 12) allowedPhonesDigits.add(d);
          } catch (_) {}
        }

        break;
      }

      case "error":
        logError(connId, "OpenAI error event", msg);

        if (sttEnabled && !sttDisabledByError && isSttInvalidModelError(msg)) {
          sttDisabledByError = true;
          sttEnabled = false;
          logError(connId, "transcription model not supported, continuing without STT");
          if (openAiWs.readyState === WebSocket.OPEN) {
            openAiWs.send(
              JSON.stringify({
                type: "session.update",
                session: { input_audio_transcription: null },
              })
            );
          }
        }

        // If the Realtime session hit max duration, end the call cleanly.
        if (msg && msg.error && msg.error.code === "session_expired") {
          plannedEnd = true;
          // Avoid trying to keep streaming into an expired session
          hasActiveResponse = false;
          botSpeaking = false;
          botTurnActive = false;
          noListenUntilTs = 0;
          endCall("openai_session_expired", null).catch(() => {});
          break;
        }

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

      // Refresh phones from sheets (if cache already loaded) and add caller-id to the allowed set
      refreshAllowedPhonesFromSheets();

      callerIL = toIsraeliLocalFromAny(callerRaw) || null;
      prefersCallerId = !!callerIL;
      needsPhoneCapture = !prefersCallerId;
      if (callerIL) {
        allowedPhonesDigits.add(digitsOnly(callerIL));
        queueSessionAddon(
          'אם השיחה הגיעה עם מספר מזוהה: אסור לבקש מספר טלפון, אסור לבצע ולידציה, ואסור להקריא ספרות. מותר לומר רק: "אם נצטרך לחזור אליכם, נחזור למספר שממנו התקשרתם".',
          "caller_id"
        );
      } else {
        queueSessionAddon(
          "אם אין מספר מזוהה: מותר לשאול שאלה אחת בלבד לקבלת מספר טלפון (באורך 9–10 ספרות). אין ולידציה מתקדמת ואין חזרה על הספרות.",
          "no_caller_id"
        );
      }

      callStartTs = Date.now();
      lastMediaTs = Date.now();

      logAlways(`[TWILIO_START][${connId}] ${JSON.stringify(msg.start || {})}`);

      // Start Twilio recording (optional). Recording callbacks arrive asynchronously to /twilio-recording-callback.
      if (MB_ENABLE_RECORDING && callSid) {
        const rec = await startRecordingIfEnabled(callSid);
        if (rec.ok && rec.sid) {
          recordingSid = rec.sid;
          setRecordingForCall(callSid, { recordingSid: rec.sid, recordingUrl: rec.url || "" });
          logInfo(connId, "Recording started.", { recording_sid: recordingSid });
        } else {
          logInfo(connId, "Recording not started.", rec);
        }
      }

      idleCheckInterval = setInterval(() => {
        const now = Date.now();
        const sinceMedia = now - lastMediaTs;

        if (!idleWarningSent && sinceMedia >= MB_IDLE_WARNING_MS && !callEnded) {
          idleWarningSent = true;
          sendModelPrompt(openAiWs, `אני כאן, אם תרצו להמשיך.`, "idle_warning");
        }

        if (!idleHangupScheduled && sinceMedia >= MB_IDLE_HANGUP_MS && !callEnded) {
          idleHangupScheduled = true;
          plannedEnd = true;
          sendModelPrompt(openAiWs, `נראה שאין מענה. אפשר להתקשר שוב או להשאיר פרטים לחזרה.`, "idle_timeout");
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

      // Always respect a short post-TTS cooldown (even when barge-in is enabled) to avoid false turns from echo/noise.
      if (now < noListenUntilTs) return;

      // If barge-in is disabled, block user audio while the bot is speaking or has an active turn.
      if (!MB_ALLOW_BARGE_IN) {
        if (botTurnActive || botSpeaking) return;
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
  console.log(`==> Your service is live`);
  console.log(`==> Available at your primary URL ${process.env.RENDER_EXTERNAL_URL || ""}`);
  logEnvStatus();
  loadSheetsCache("Startup").catch((err) => console.error("[ERROR] Startup sheets load failed", err));
});
