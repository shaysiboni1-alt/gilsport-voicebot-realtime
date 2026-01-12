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
// ENV
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

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);

// --------------------------------------------------
// Sheets
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
    error("Sheets load failed", e?.message || String(e));
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

// OPTIONAL: אם אי פעם תחזיר TwiML מהרנדר עצמו
app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  const wsUrl = `wss://${host}/twilio-media-stream`;

  res.type("text/xml").send(`
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
    </Stream>
  </Connect>
</Response>
`.trim());
});

const server = http.createServer(app);

// --------------------------------------------------
// WebSocket – Upgrade handled manually
// --------------------------------------------------
const wss = new WebSocket.Server({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  if (req.url === "/twilio-media-stream") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  } else {
    socket.destroy();
  }
});

wss.on("connection", (twilioWs) => {
  log("[WS] Twilio connected");

  let twilioStreamSid = null;
  let openaiWs = null;

  const safeClose = () => {
    try { twilioWs.close(); } catch {}
    try { openaiWs?.close(); } catch {}
  };

  if (!OPENAI_API_KEY) {
    error("OPENAI_API_KEY missing — closing call");
    return safeClose();
  }

  openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(OPENAI_REALTIME_MODEL)}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1"
      }
    }
  );

  openaiWs.on("open", () => {
    debug("OpenAI connected");

    // session
    openaiWs.send(JSON.stringify({
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
    }));

    // פתיח (כמו שהיה אצלך)
    openaiWs.send(JSON.stringify({
      type: "conversation.item.create",
      item: {
        type: "message",
        role: "user",
        content: [{
          type: "input_text",
          text: getPrompt("OPENING_SCRIPT", "שלום, מדברת נטע מגיל ספורט.")
        }]
      }
    }));

    openaiWs.send(JSON.stringify({ type: "response.create" }));
  });

  openaiWs.on("close", () => {
    debug("OpenAI closed");
    safeClose();
  });

  openaiWs.on("error", (e) => {
    error("OpenAI WS error", e?.message || String(e));
    safeClose();
  });

  twilioWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      error("Twilio WS JSON parse failed", e?.message || String(e));
      return;
    }

    if (msg.event === "start") {
      twilioStreamSid = msg?.start?.streamSid || msg?.streamSid || null;
      log("[WS] start", twilioStreamSid || "(no streamSid)");
      return;
    }

    if (msg.event === "stop") {
      log("[WS] stop");
      return safeClose();
    }

    if (msg.event === "media" && msg.media?.payload) {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) {
        openaiWs.send(JSON.stringify({
          type: "input_audio_buffer.append",
          audio: msg.media.payload
        }));
      }
    }
  });

  twilioWs.on("close", () => {
    debug("Twilio WS closed");
    safeClose();
  });

  twilioWs.on("error", (e) => {
    error("Twilio WS error", e?.message || String(e));
    safeClose();
  });

  openaiWs.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      error("OpenAI JSON parse failed", e?.message || String(e));
      return;
    }

    if (msg.type === "response.audio.delta") {
      // חובה: להשתמש ב-streamSid של Twilio, לא של OpenAI
      if (!twilioStreamSid) return;

      try {
        twilioWs.send(JSON.stringify({
          event: "media",
          streamSid: twilioStreamSid,
          media: { payload: msg.delta }
        }));
      } catch (e) {
        error("Failed sending audio to Twilio", e?.message || String(e));
      }
    }

    // אופציונלי: לוג שקט רק לדיבאג
    if (MB_DEBUG && msg.type && msg.type.startsWith("error")) {
      debug("OpenAI msg", msg);
    }
  });
});

// --------------------------------------------------
// Start
// --------------------------------------------------
server.listen(PORT, () => {
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
});
