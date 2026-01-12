// server.js
// GilSport Realtime VoiceBot – Neta based
// Render + Twilio Media Streams + OpenAI Realtime
// Single Source of Truth: Google Sheets

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");

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

// Voice: normalize to lowercase + validate (prevents "Alloy" bug)
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
const MB_DEBUG = envBool("MB_DEBUG", false);

const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.65);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);
const MB_NO_BARGE_TAIL_MS = envNum("MB_NO_BARGE_TAIL_MS", 1600);
const MB_ALLOW_BARGE_IN = envBool("MB_ALLOW_BARGE_IN", false);

const MB_IDLE_WARNING_MS = envNum("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNum("MB_IDLE_HANGUP_MS", 90000);

const MB_MAX_CALL_MS = envNum("MB_MAX_CALL_MS", 5 * 60 * 1000);

// NEW: transcript logging flags (safe defaults)
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);

// ALWAYS logs (not dependent on MB_DEBUG)
const always = (...a) => console.log("[ALWAYS]", ...a);

// --------------------------------------------------
// Runtime diagnostics (no behavior changes)
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
  prompts: {}
};

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

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: GSHEET_ID,
      range: "PROMPTS!A:Z"
    });

    const rows = res.data.values || [];
    const headers = rows.shift() || [];

    const prompts = {};
    for (const r of rows) {
      const row = {};
      headers.forEach((h, i) => (row[h] = r[i] || ""));
      if (row.prompt_id && row.content_he) {
        prompts[String(row.prompt_id).trim()] = String(row.content_he);
      }
    }

    SHEETS = {
      loaded_at: new Date().toISOString(),
      prompts
    };

    log(`Sheets loaded (${Object.keys(prompts).length} prompts)`);
  } catch (e) {
    error("Sheets load failed", e.message);
  }
}

const getPrompt = (id, fallback = "") =>
  String(SHEETS.prompts[id] || fallback).trim();

const preview = (s, n = 120) => {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  return t.length > n ? t.slice(0, n) + "..." : t;
};

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
    prompts: Object.keys(SHEETS.prompts).length
  });
});

// quick ENV diagnostics (safe – no secrets)
app.get("/diag/env", (_, res) => {
  res.json({
    ok: true,
    booted_at: RUNTIME.booted_at,

    has_OPENAI_API_KEY: Boolean(OPENAI_API_KEY),
    OPENAI_REALTIME_MODEL,
    OPENAI_VOICE,

    has_GSHEET_ID: Boolean(GSHEET_ID),
    has_GOOGLE_SERVICE_ACCOUNT_JSON_B64: Boolean(GOOGLE_SERVICE_ACCOUNT_JSON_B64),

    MB_DEBUG,
    MB_WEBHOOK_URL_present: Boolean(MB_WEBHOOK_URL),

    MB_LOG_TRANSCRIPTS,
    MB_ENABLE_TRANSCRIPTION,
    MB_TRANSCRIPTION_MODEL,

    sheets_loaded_at: SHEETS.loaded_at,
    prompts_count: Object.keys(SHEETS.prompts).length
  });
});

// NEW: prompt diagnostics (helps catch OPENING_SCRIPT mismatch/timing)
app.get("/diag/prompts", (_, res) => {
  const keys = Object.keys(SHEETS.prompts).sort();
  res.json({
    ok: true,
    sheets_loaded_at: SHEETS.loaded_at,
    prompts_count: keys.length,
    prompt_ids: keys,
    opening_preview: preview(getPrompt("OPENING_SCRIPT", "")),
    master_preview: preview(getPrompt("MASTER_PROMPT", ""))
  });
});

// runtime counters
app.get("/diag/runtime", (_, res) => {
  res.json({
    ok: true,
    ...RUNTIME
  });
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
  let twilioMediaFrames = 0;
  let twilioMediaBytesB64 = 0;
  let openaiAudioDeltas = 0;
  let openaiAudioBytesB64 = 0;
  let lastStatsAt = Date.now();

  // Transcript buffers
  let lastCallerText = "";
  let botTextAccum = "";
  let botTextLastPrintedAt = 0;

  const maybePrintStats = (force = false) => {
    const now = Date.now();
    if (!force && now - lastStatsAt < 5000) return;
    lastStatsAt = now;

    debug(`[${connTag}] STATS`, {
      twilioStreamSid,
      openaiReady,
      pendingAudio: pendingAudio.length,
      twilioMediaFrames,
      twilioMediaBytesB64,
      openaiAudioDeltas,
      openaiAudioBytesB64
    });
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

    // ✅ Surgical: ensure sheets are loaded BEFORE we read OPENING_SCRIPT / MASTER_PROMPT
    if (!SHEETS.loaded_at) {
      debug(`[${connTag}] Sheets not loaded yet. Loading now before prompts...`);
      await loadSheets();
      debug(`[${connTag}] Sheets loaded_at=${SHEETS.loaded_at || null}`);
    }

    const masterPrompt = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
    );
    const openingScript = getPrompt("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.");

    always(`[${connTag}] PROMPTS`, {
      sheets_loaded_at: SHEETS.loaded_at,
      master_preview: preview(masterPrompt),
      opening_preview: preview(openingScript)
    });

    // Configure session
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

    // Ask for transcription (caller side)
    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    safeOpenAISend({ type: "session.update", session });

    // Opening message
    safeOpenAISend({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{ type: "input_text", text: openingScript }]
      }
    });

    // Ask for response (audio+text)
    safeOpenAISend({
      type: "response.create",
      response: { modalities: ["audio", "text"] }
    });

    if (pendingAudio.length) {
      debug(`[${connTag}] Flushing buffered audio frames: ${pendingAudio.length}`);
    }
    while (pendingAudio.length > 0 && openaiWs && openaiWs.readyState === WebSocket.OPEN) {
      const audio = pendingAudio.shift();
      safeOpenAISend({ type: "input_audio_buffer.append", audio });
    }

    maybePrintStats(true);
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
    maybePrintStats(true);
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

    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error event`, msg);
      return;
    }

    if (msg.type === "session.created" || msg.type === "session.updated") {
      debug(
        `[${connTag}] OpenAI ${msg.type}`,
        msg.session
          ? {
              voice: msg.session.voice,
              input_audio_format: msg.session.input_audio_format,
              output_audio_format: msg.session.output_audio_format,
              has_transcription: Boolean(msg.session.input_audio_transcription)
            }
          : msg
      );
      return;
    }

    // -----------------------------
    // Caller transcription logging
    // -----------------------------
    // Different accounts/models sometimes emit slightly different type names.
    // We accept a few common ones.
    if (MB_LOG_TRANSCRIPTS) {
      const t = msg.type || "";
      const possibleTranscript =
        msg.transcript ||
        msg.text ||
        msg?.item?.content?.[0]?.transcript ||
        msg?.item?.content?.[0]?.text ||
        "";

      const isCallerTranscriptEvent =
        t.includes("input_audio_transcription") ||
        t.includes("conversation.item.input_audio_transcription") ||
        t.includes("input_audio_transcript");

      if (isCallerTranscriptEvent && possibleTranscript) {
        lastCallerText = String(possibleTranscript).trim();
        if (lastCallerText) {
          always(`[CALLER][${connTag}]`, lastCallerText);
        }
        return;
      }
    }

    // -----------------------------
    // Bot text logging (when model returns text deltas)
    // -----------------------------
    if (MB_LOG_TRANSCRIPTS && msg.type === "response.text.delta" && msg.delta) {
      botTextAccum += String(msg.delta);
      const now = Date.now();
      // print every ~700ms to keep logs readable
      if (now - botTextLastPrintedAt > 700) {
        const chunk = botTextAccum.trim();
        if (chunk) always(`[BOT][${connTag}]`, preview(chunk, 400));
        botTextLastPrintedAt = now;
      }
      return;
    }

    if (MB_LOG_TRANSCRIPTS && (msg.type === "response.text.done" || msg.type === "response.done")) {
      const doneText =
        (msg?.response?.output_text && String(msg.response.output_text)) ||
        botTextAccum ||
        "";
      const final = String(doneText).trim();
      if (final) always(`[BOT_FINAL][${connTag}]`, preview(final, 1200));
      botTextAccum = "";
      return;
    }

    // -----------------------------
    // Audio back to Twilio
    // -----------------------------
    if (msg.type === "response.audio.delta") {
      if (!twilioStreamSid) return;

      const delta = msg.delta || "";
      openaiAudioDeltas += 1;
      openaiAudioBytesB64 += delta.length;

      safeTwilioSend({
        event: "media",
        streamSid: twilioStreamSid,
        media: { payload: delta }
      });

      maybePrintStats(false);
      return;
    }
  });

  twilioWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      error(`[${connTag}] Twilio message JSON parse failed`, e.message);
      return;
    }

    if (msg.event === "start" && msg.start?.streamSid) {
      twilioStreamSid = msg.start.streamSid;
      debug(`[${connTag}] Twilio stream started`, {
        streamSid: twilioStreamSid,
        callSid: msg.start?.callSid,
        tracks: msg.start?.tracks,
        mediaFormat: msg.start?.mediaFormat
      });
      maybePrintStats(true);
      return;
    }

    if (msg.event === "media" && msg.media?.payload) {
      const payload = msg.media.payload;

      twilioMediaFrames += 1;
      twilioMediaBytesB64 += payload.length;

      if (!openaiReady || !openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
        pendingAudio.push(payload);
        if (pendingAudio.length > 400) pendingAudio.splice(0, pendingAudio.length - 400);
        maybePrintStats(false);
        return;
      }

      safeOpenAISend({
        type: "input_audio_buffer.append",
        audio: payload
      });

      maybePrintStats(false);
      return;
    }

    if (msg.event === "stop") {
      debug(`[${connTag}] Twilio stream stopped`);
      maybePrintStats(true);
      try {
        if (openaiWs) openaiWs.close();
      } catch (_) {}
      return;
    }
  });

  twilioWs.on("error", (e) => {
    RUNTIME.ws_errors += 1;
    error(`[${connTag}] Twilio websocket error`, e?.message || e);
    maybePrintStats(true);
    try {
      if (openaiWs) openaiWs.close();
    } catch (_) {}
  });

  twilioWs.on("close", () => {
    RUNTIME.ws_closed += 1;
    RUNTIME.last_ws_close_at = new Date().toISOString();
    debug(`[${connTag}] Twilio closed`);
    maybePrintStats(true);
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
    MB_LOG_TRANSCRIPTS,
    MB_ENABLE_TRANSCRIPTION,
    MB_TRANSCRIPTION_MODEL
  });
});
