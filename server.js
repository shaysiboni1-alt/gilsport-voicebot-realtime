/**
 * GilSport Realtime VoiceBot – BASELINE WORKING
 * Twilio Media Streams + OpenAI Realtime
 * This file MUST work before adding any logic.
 */

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");

// =======================
// ENV
// =======================
const PORT = process.env.PORT || 10000;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

if (!OPENAI_API_KEY) {
  console.error("❌ OPENAI_API_KEY missing");
  process.exit(1);
}

// =======================
// Express
// =======================
const app = express();
app.use(express.urlencoded({ extended: false }));
app.use(express.json());

// ---- Health
app.get("/health", (_, res) => {
  res.json({ ok: true });
});

// ---- Twilio Voice → TwiML
app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;

  const twiml = `
<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://${host}/twilio-media-stream">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
    </Stream>
  </Connect>
</Response>
`;

  res.type("text/xml").send(twiml.trim());
});

// =======================
// HTTP Server
// =======================
const server = http.createServer(app);

// =======================
// WebSocket Server (Twilio)
// =======================
const wss = new WebSocket.Server({
  server,
  path: "/twilio-media-stream",
});

wss.on("connection", (twilioWs) => {
  console.log("📞 Twilio Media Stream connected");

  // ---- OpenAI Realtime WS
  const openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
    {
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta": "realtime=v1",
      },
    }
  );

  // ---- OpenAI OPEN
  openaiWs.on("open", () => {
    console.log("🤖 OpenAI Realtime connected");

    // Session config
    openaiWs.send(
      JSON.stringify({
        type: "session.update",
        session: {
          modalities: ["audio", "text"],
          voice: OPENAI_VOICE,
          input_audio_format: "g711_ulaw",
          output_audio_format: "g711_ulaw",
          instructions:
            "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו בעברית, קצר, ברור, אנושי.",
        },
      })
    );

    // Opening message
    openaiWs.send(
      JSON.stringify({
        type: "conversation.item.create",
        item: {
          type: "message",
          role: "user",
          content: [
            {
              type: "input_text",
              text: "שלום, הגעתם לגיל ספורט. איך אפשר לעזור?",
            },
          ],
        },
      })
    );

    openaiWs.send(JSON.stringify({ type: "response.create" }));
  });

  // ---- Twilio → OpenAI (audio in)
  twilioWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());

    if (msg.event === "media" && msg.media?.payload) {
      openaiWs.send(
        JSON.stringify({
          type: "input_audio_buffer.append",
          audio: msg.media.payload,
        })
      );
    }

    if (msg.event === "stop") {
      console.log("📴 Twilio stream stopped");
      openaiWs.close();
    }
  });

  // ---- OpenAI → Twilio (audio out)
  openaiWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());

    if (msg.type === "response.audio.delta" && msg.delta) {
      twilioWs.send(
        JSON.stringify({
          event: "media",
          media: { payload: msg.delta },
        })
      );
    }
  });

  openaiWs.on("close", () => {
    console.log("🤖 OpenAI closed");
    twilioWs.close();
  });

  openaiWs.on("error", (err) => {
    console.error("❌ OpenAI error", err);
    twilioWs.close();
  });
});

// =======================
// START
// =======================
server.listen(PORT, () => {
  console.log(`🚀 GilSport VoiceBot listening on port ${PORT}`);
});
