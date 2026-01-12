// server.js
// GilSport VoiceBot – Realtime, יציב, בלי רעש
// Twilio Media Streams + OpenAI Realtime
// FULL FILE – NO PARTS

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

// ===============================
// CONFIG
// ===============================
const PORT = process.env.PORT || 10000;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_MODEL =
  process.env.OPENAI_REALTIME_MODEL ||
  "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

// ===============================
// APP
// ===============================
const app = express();
app.use(express.json());

app.get("/health", (_, res) => res.json({ ok: true }));

const server = http.createServer(app);

// ===============================
// WEBSOCKET SERVER
// ===============================
const wss = new WebSocket.Server({ noServer: true });

server.on("upgrade", (req, socket, head) => {
  let pathname;
  try {
    pathname = new URL(req.url, "http://localhost").pathname;
  } catch {
    socket.destroy();
    return;
  }

  if (pathname === "/twilio-media-stream") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws);
    });
  } else {
    socket.destroy();
  }
});

wss.on("connection", (twilioWs) => {
  console.log("🟢 Twilio WS connected");

  let streamSid = null;

  // -------------------------------
  // OpenAI WS
  // -------------------------------
  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1"
      }
    }
  );

  openaiWs.on("open", () => {
    console.log("🟣 OpenAI WS connected");

    openaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        modalities: ["audio", "text"],
        voice: OPENAI_VOICE,
        input_audio_format: "g711_ulaw",
        output_audio_format: "g711_ulaw",
        turn_detection: { type: "server_vad" },
        instructions:
          "את מדברת בעברית בלבד. את נטע, עוזרת קולית של גיל ספורט. דברי רגוע וברור."
      }
    }));

    openaiWs.send(JSON.stringify({
      type: "response.create"
    }));
  });

  // -------------------------------
  // FROM TWILIO
  // -------------------------------
  twilioWs.on("message", (msg) => {
    const data = JSON.parse(msg.toString());

    if (data.event === "start") {
      streamSid = data.start.streamSid;
      console.log("▶️ streamSid:", streamSid);
      return;
    }

    if (data.event === "media" && data.media?.payload) {
      openaiWs.send(JSON.stringify({
        type: "input_audio_buffer.append",
        audio: data.media.payload
      }));
    }
  });

  // -------------------------------
  // FROM OPENAI
  // -------------------------------
  openaiWs.on("message", (msg) => {
    const data = JSON.parse(msg.toString());

    if (data.type === "response.audio.delta" && streamSid) {
      twilioWs.send(JSON.stringify({
        event: "media",
        streamSid,
        media: { payload: data.delta }
      }));
    }
  });

  twilioWs.on("close", () => {
    console.log("🔴 Twilio WS closed");
    openaiWs.close();
  });
});

// ===============================
// START
// ===============================
server.listen(PORT, () => {
  console.log(`🚀 GilSport VoiceBot running on ${PORT}`);
});
