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
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

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

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);

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
        prompts[row.prompt_id] = row.content_he;
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
  // ---- Minimal critical fix:
  // Twilio starts sending "media" immediately. OpenAI may still be CONNECTING.
  // We buffer audio until OpenAI websocket is OPEN, and we never send on CONNECTING.
  let twilioStreamSid = null;
  let openaiReady = false;
  const pendingAudio = []; // array of base64 g711_ulaw payloads

  // Telemetry only (no behavior changes)
  const connTag = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;
  let twilioMediaFrames = 0;
  let twilioMediaBytesB64 = 0;
  let openaiAudioDeltas = 0;
  let openaiAudioBytesB64 = 0;
  let lastStatsAt = Date.now();

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

  // NOTE: declare openaiWs variable early so closures can reference safely
  let openaiWs = null;

  // Helper: safe send to OpenAI
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

  // Helper: safe send to Twilio
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

  debug(`[${connTag}] Twilio WS connected (media stream). ip=${req?.socket?.remoteAddress || "?"}`);
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

  openaiWs.on("open", () => {
    debug(`[${connTag}] OpenAI connected`);
    openaiReady = true;

    // Configure session
    safeOpenAISend({
      type: "session.update",
      session: {
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
        instructions: getPrompt(
          "MASTER_PROMPT",
          "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
        )
      }
    });

    // Opening message
    safeOpenAISend({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [
          {
            type: "input_text",
            text: getPrompt("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.")
          }
        ]
      }
    });

    safeOpenAISend({ type: "response.create" });

    // Flush buffered audio that arrived before OpenAI was ready
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
    error(`[${connTag}] OpenAI websocket error`, e?.message || e);
    try {
      twilioWs.close();
    } catch (_) {}
  });

  openaiWs.on("close", () => {
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

    // Surface important OpenAI events for debugging format/noise
    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error event`, msg);
      return;
    }
    if (msg.type === "session.created" || msg.type === "session.updated") {
      debug(`[${connTag}] OpenAI ${msg.type}`, msg.session ? {
        voice: msg.session.voice,
        input_audio_format: msg.session.input_audio_format,
        output_audio_format: msg.session.output_audio_format
      } : msg);
      return;
    }

    // Send audio back to Twilio
    if (msg.type === "response.audio.delta") {
      // Twilio needs the ORIGINAL Twilio streamSid (from start), not something from OpenAI
      if (!twilioStreamSid) {
        return;
      }

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

    // Capture streamSid from Twilio "start"
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

    // Forward audio to OpenAI (buffer if OpenAI not ready yet)
    if (msg.event === "media" && msg.media?.payload) {
      const payload = msg.media.payload;

      twilioMediaFrames += 1;
      twilioMediaBytesB64 += payload.length;

      // If OpenAI isn't open yet, buffer to prevent "readyState 0 (CONNECTING)" crash
      if (!openaiReady || !openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
        pendingAudio.push(payload);

        // Safety: keep buffer bounded (prevents memory blow if OpenAI never opens)
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

    // Optional: handle stop
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
    error(`[${connTag}] Twilio websocket error`, e?.message || e);
    maybePrintStats(true);
    try {
      if (openaiWs) openaiWs.close();
    } catch (_) {}
  });

  twilioWs.on("close", () => {
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
});
