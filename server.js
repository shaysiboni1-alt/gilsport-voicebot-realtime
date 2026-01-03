import express from "express";
import http from "http";
import { WebSocketServer } from "ws";
import { google } from "googleapis";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

// ===================== ENV =====================
const ENV = {
  GOOGLE_SERVICE_ACCOUNT_JSON: process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "",
  GSHEET_ID: process.env.GSHEET_ID || "",
  GSHEET_CACHE_TTL_SEC: Number(process.env.GSHEET_CACHE_TTL_SEC || 60),
  TIME_ZONE: process.env.TIME_ZONE || "Asia/Jerusalem",

  GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
  GEMINI_MODEL: process.env.GEMINI_MODEL || "models/gemini-2.0-flash-exp",
  GEMINI_TIMEOUT_MS: Number(process.env.GEMINI_TIMEOUT_MS || 9000),
  GEMINI_MIN_CONF: Number(process.env.GEMINI_MIN_CONF || 0.65),

  MAKE_SEND_WA_URL: process.env.MAKE_SEND_WA_URL || "",
  MAKE_LEAD_URL: process.env.MAKE_LEAD_URL || "",
  MAKE_SUPPORT_URL: process.env.MAKE_SUPPORT_URL || "",
  MAKE_ABANDONED_URL: process.env.MAKE_ABANDONED_URL || "",

  KB_MAX_PAGES: Number(process.env.KB_MAX_PAGES || 12),
  KB_MAX_CHARS_PER_PAGE: Number(process.env.KB_MAX_CHARS_PER_PAGE || 6000),
  KB_CACHE_TTL_SEC: Number(process.env.KB_CACHE_TTL_SEC || 900),

  TWILIO_STREAM_LOG_EVERY_N_MEDIA: Number(process.env.TWILIO_STREAM_LOG_EVERY_N_MEDIA || 50),

  // ✅ Test tone settings
  TEST_TONE_ON_START: String(process.env.TEST_TONE_ON_START || "true").toLowerCase() === "true",
  TEST_TONE_FREQ_HZ: Number(process.env.TEST_TONE_FREQ_HZ || 440),
  TEST_TONE_MS: Number(process.env.TEST_TONE_MS || 650),

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),
};

let LAST_GEMINI_ERROR = "";
let LAST_KB_ERROR = "";

// ===================== Logging =====================
function log(level, ...args) {
  const levels = ["debug", "info", "warn", "error"];
  const cur = levels.indexOf(ENV.LOG_LEVEL);
  const idx = levels.indexOf(level);
  if (idx === -1) return;
  if (cur === -1 || idx >= cur) console.log(`[${level.toUpperCase()}]`, ...args);
}

// ===================== Utils =====================
function safeJsonParse(maybeJson) {
  try {
    if (!maybeJson) return { ok: false, error: "empty" };
    return { ok: true, value: JSON.parse(maybeJson) };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}
function nowIso() {
  return new Date().toISOString();
}
function normalizeText(s) {
  return String(s || "").toLowerCase().replace(/\s+/g, " ").trim();
}
function splitKeywords(cell) {
  return String(cell || "")
    .split(/[,|\n]/g)
    .map((x) => x.trim())
    .filter(Boolean);
}
function normalizeRowKeys(row) {
  const out = {};
  for (const [k, v] of Object.entries(row || {})) {
    const nk = String(k || "").trim().toLowerCase().replace(/\s+/g, "_");
    out[nk] = v ?? "";
  }
  return out;
}
function pickTextByLang(row, lang, fallbackLang = "he") {
  const r = normalizeRowKeys(row);
  const candidates = [r[lang], r[`text_${lang}`], r.text, r.value, r[fallbackLang], r[`text_${fallbackLang}`]].filter(
    (x) => String(x || "").trim()
  );
  return String(candidates[0] || "").trim();
}
function detectLanguage(text, supportedCsv = "he,en,ru,ar") {
  const t = String(text || "");
  const supported = supportedCsv.split(",").map((x) => x.trim()).filter(Boolean);
  const hasArabic = /[\u0600-\u06FF]/.test(t);
  const hasCyr = /[\u0400-\u04FF]/.test(t);
  const hasHeb = /[\u0590-\u05FF]/.test(t);
  if (hasArabic && supported.includes("ar")) return "ar";
  if (hasCyr && supported.includes("ru")) return "ru";
  if (hasHeb && supported.includes("he")) return "he";
  if (supported.includes("en")) return "en";
  return supported[0] || "he";
}
async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), Math.max(1, timeoutMs));
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
}

// ===================== Google Sheets (Service Account) =====================
function getServiceAccountAuth() {
  const raw = ENV.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON missing");

  let creds;
  const parsed = safeJsonParse(raw);
  if (parsed.ok) creds = parsed.value;
  else creds = JSON.parse(Buffer.from(raw, "base64").toString("utf8"));

  if (!creds?.client_email || !creds?.private_key) {
    throw new Error("Service account JSON missing client_email/private_key");
  }

  return new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
}

async function fetchSheetTab(auth, sheetId, tabName) {
  const sheets = google.sheets({ version: "v4", auth });
  const res = await sheets.spreadsheets.values.get({ spreadsheetId: sheetId, range: tabName });
  const rows = res.data.values || [];
  if (!rows.length) return [];
  const headers = rows[0].map((h) => String(h || "").trim());
  return rows.slice(1).map((r) => {
    const obj = {};
    headers.forEach((h, i) => (obj[h] = r[i] ?? ""));
    return obj;
  });
}

// ===================== Config Loader + Cache =====================
let CONFIG_CACHE = { loaded_at: 0, data: null };

async function loadConfigFromSheet(force = false) {
  const ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - CONFIG_CACHE.loaded_at < ttlMs;

  if (!force && CONFIG_CACHE.data && fresh) {
    return { ok: true, from_cache: true, loaded_at: new Date(CONFIG_CACHE.loaded_at).toISOString(), ...CONFIG_CACHE.data };
  }

  if (!ENV.GSHEET_ID) throw new Error("GSHEET_ID missing");

  const auth = getServiceAccountAuth();
  await auth.authorize();

  const tabs = ["SETTINGS", "BUSINESS_INFO", "ROUTING_RULES", "SALES_SCRIPT", "SUPPORT_SCRIPT", "SUPPLIERS", "MAKE_PAYLOADS_SPEC", "PROMPTS"];
  const results = {};
  for (const tab of tabs) results[tab] = await fetchSheetTab(auth, ENV.GSHEET_ID, tab);

  const settings = {};
  for (const row of results.SETTINGS || []) {
    const r = normalizeRowKeys(row);
    const k = String(r.key || "").trim();
    if (k) settings[k] = String(r.value ?? "").trim();
  }

  const cfg = {
    settings,
    business_info: results.BUSINESS_INFO || [],
    routing_rules: (results.ROUTING_RULES || []).map(normalizeRowKeys),
    sales_script: (results.SALES_SCRIPT || []).map(normalizeRowKeys),
    support_script: (results.SUPPORT_SCRIPT || []).map(normalizeRowKeys),
    suppliers: (results.SUPPLIERS || []).map(normalizeRowKeys),
    make_payloads_spec: (results.MAKE_PAYLOADS_SPEC || []).map(normalizeRowKeys),
    prompts: (results.PROMPTS || []).map(normalizeRowKeys),
    overview: {
      BUSINESS_NAME: settings.BUSINESS_NAME || "",
      DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "he",
      SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "he,en,ru,ar",
      SITE_BASE_URL: settings.SITE_BASE_URL || "",
      MAIN_PHONE: settings.MAIN_PHONE || "",
      BRANCHES: settings.BRANCHES || "",
    },
  };

  CONFIG_CACHE = { loaded_at: Date.now(), data: cfg };
  return { ok: true, from_cache: false, loaded_at: nowIso(), ...cfg };
}

// ===================== Minimal endpoints =====================
app.get("/health", (req, res) => res.json({ ok: true, time: nowIso() }));

app.get("/twilio-media-stream", (req, res) => {
  res.status(426).send("Use WebSocket (wss) to connect to /twilio-media-stream");
});

// ===================== Twilio Media Streams WebSocket =====================

// ---- µ-law encoder for 8kHz tone (Twilio expects PCMU by default) ----
function linearToMuLawSample(sample) {
  const MU_LAW_MAX = 0x1fff;
  const BIAS = 0x84;

  let sign = (sample >> 8) & 0x80;
  if (sign !== 0) sample = -sample;
  if (sample > MU_LAW_MAX) sample = MU_LAW_MAX;

  sample += BIAS;

  let exponent = 7;
  for (let expMask = 0x4000; (sample & expMask) === 0 && exponent > 0; exponent--, expMask >>= 1) {}

  const mantissa = (sample >> (exponent + 3)) & 0x0f;
  const muLawByte = ~(sign | (exponent << 4) | mantissa);
  return muLawByte & 0xff;
}

function genMuLawSineBase64(freqHz, ms, sampleRate = 8000) {
  const totalSamples = Math.floor((ms / 1000) * sampleRate);
  const pcmu = Buffer.alloc(totalSamples);

  const amp = 12000; // safe amplitude
  for (let i = 0; i < totalSamples; i++) {
    const t = i / sampleRate;
    const s = Math.floor(Math.sin(2 * Math.PI * freqHz * t) * amp);
    pcmu[i] = linearToMuLawSample(s);
  }

  return pcmu.toString("base64");
}

function sendOutboundAudio(ws, streamSid, base64Pcmu) {
  // Send in 20ms frames: 8000hz => 160 samples => 160 bytes in PCMU
  const bytes = Buffer.from(base64Pcmu, "base64");
  const frameSize = 160;
  let offset = 0;

  const sendFrame = () => {
    if (ws.readyState !== ws.OPEN) return;
    if (offset >= bytes.length) return;

    const chunk = bytes.subarray(offset, offset + frameSize);
    offset += frameSize;

    const payload = chunk.toString("base64");
    ws.send(
      JSON.stringify({
        event: "media",
        streamSid,
        media: {
          payload,
          track: "outbound",
        },
      })
    );

    setTimeout(sendFrame, 20);
  };

  sendFrame();
}

const server = http.createServer(app);
const wss = new WebSocketServer({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  const url = req.url || "";
  if (url.startsWith("/twilio-media-stream")) {
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
    return;
  }
  socket.destroy();
});

wss.on("connection", (ws, req) => {
  const peer = req?.socket?.remoteAddress || "unknown";
  const path = req?.url || "";
  const connId = `tw_${Math.random().toString(36).slice(2, 8)}${Date.now().toString(36).slice(-4)}`;

  let streamSid = "";
  let callSid = "";
  let mediaCount = 0;

  log("info", `[WS][${connId}] connected`, { peer, path });

  const pingTimer = setInterval(() => {
    try {
      if (ws.readyState === ws.OPEN) ws.ping();
    } catch {}
  }, 25000);

  ws.on("message", (buf) => {
    const txt = Buffer.isBuffer(buf) ? buf.toString("utf8") : String(buf || "");
    const parsed = safeJsonParse(txt);
    if (!parsed.ok) return;

    const msg = parsed.value || {};
    const ev = String(msg.event || "").toLowerCase();

    if (ev === "start") {
      streamSid = msg?.start?.streamSid || "";
      callSid = msg?.start?.callSid || "";
      const custom = msg?.start?.customParameters || {};
      log("info", `[WS][${connId}] start`, { streamSid, callSid, customParameters: custom });

      // ✅ Send test tone to prove outbound audio works
      if (ENV.TEST_TONE_ON_START && streamSid) {
        const b64 = genMuLawSineBase64(ENV.TEST_TONE_FREQ_HZ, ENV.TEST_TONE_MS);
        log("info", `[WS][${connId}] sending test tone`, { freq: ENV.TEST_TONE_FREQ_HZ, ms: ENV.TEST_TONE_MS });
        sendOutboundAudio(ws, streamSid, b64);
      }
      return;
    }

    if (ev === "media") {
      mediaCount += 1;
      if (mediaCount === 1 || mediaCount % ENV.TWILIO_STREAM_LOG_EVERY_N_MEDIA === 0) {
        log("debug", `[WS][${connId}] media`, {
          streamSid,
          callSid,
          mediaCount,
          track: msg?.media?.track,
          chunkBytesB64: (msg?.media?.payload || "").length,
        });
      }
      return;
    }

    if (ev === "stop") {
      log("info", `[WS][${connId}] stop`, { streamSid, callSid, mediaCount, stop: msg?.stop || {} });
      try {
        ws.close();
      } catch {}
      return;
    }
  });

  ws.on("close", () => {
    clearInterval(pingTimer);
    log("info", `[WS][${connId}] closed`, { streamSid, callSid, mediaCount });
  });

  ws.on("error", (err) => {
    log("error", `[WS][${connId}] error`, err?.message || String(err));
  });
});

// ===================== Boot =====================
server.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
});
