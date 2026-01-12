/**
 * GilSport Realtime VoiceBot – PRODUCTION
 * Based on MisterBot (Neta) – hardened & stable
 */

import express from "express";
import http from "http";
import WebSocket from "ws";
import fetch from "node-fetch";
import { Buffer } from "buffer";

/* ===================== ENV ===================== */

const {
  OPENAI_API_KEY,
  OPENAI_VOICE = "alloy",
  TIME_ZONE = "Asia/Jerusalem",
  GSHEET_ID,
  GOOGLE_SERVICE_ACCOUNT_JSON_B64,
  MB_WEBHOOK_URL,
  MB_DEBUG = "false",
} = process.env;

if (!OPENAI_API_KEY) console.error("❌ Missing OPENAI_API_KEY in ENV.");
if (!GSHEET_ID) console.error("❌ Missing GSHEET_ID in ENV.");
if (!GOOGLE_SERVICE_ACCOUNT_JSON_B64)
  console.error("❌ Missing GOOGLE_SERVICE_ACCOUNT_JSON_B64 in ENV.");

/* ===================== APP ===================== */

const app = express();
app.use(express.json());

const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

/* ===================== SHEETS (stub – already works) ===================== */

let PROMPTS = [];

async function loadSheets() {
  // אצלך זה כבר עובד – לא נוגעים
  PROMPTS = [{ id: "opening", text: "שלום, הגעתם לגיל ספורט." }];
  console.log(`[INFO] Sheets loaded (${PROMPTS.length} prompts)`);
}

loadSheets();

/* ===================== HEALTH ===================== */

app.get("/", (_req, res) => {
  res.json({
    ok: true,
    sheets_loaded_at: new Date().toISOString(),
    prompts: PROMPTS.length,
  });
});

app.post("/sheets/reload", async (_req, res) => {
  await loadSheets();
  res.json({ ok: true, reloaded: true, at: new Date().toISOString() });
});

/* ===================== WS HANDLER ===================== */

wss.on("connection", (twilioWs) => {
  console.log("[WS] Twilio connected");

  let openaiWs = null;
  let openaiReady = false;
  const openaiQueue = [];

  /* ---------- OpenAI WS ---------- */

  function connectOpenAI() {
    console.log("[OPENAI] connecting...");

    openaiWs = new WebSocket(
      "wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview",
      {
        headers: {
          Authorization: `Bearer ${OPENAI_API_KEY}`,
          "OpenAI-Beta": "realtime=v1",
        },
      }
    );

    openaiWs.on("open", () => {
      openaiReady = true;
      console.log("[OPENAI] connected");

      // Flush queue
      while (openaiQueue.length > 0) {
        const msg = openaiQueue.shift();
        openaiWs.send(msg);
      }

      console.log("[OPENAI] ready -> queue flushed");
    });

    openaiWs.on("message", (data) => {
      // Audio back to Twilio
      try {
        const msg = JSON.parse(data.toString());
        if (msg.type === "response.audio.chunk") {
          twilioWs.send(
            JSON.stringify({
              event: "media",
              media: { payload: msg.chunk },
            })
          );
        }
      } catch {}
    });

    openaiWs.on("close", () => {
      console.log("[OPENAI] closed");
      openaiReady = false;
    });

    openaiWs.on("error", (err) => {
      console.error("[OPENAI] error", err.message);
    });
  }

  connectOpenAI();

  /* ---------- SAFE SEND ---------- */

  function sendToOpenAI(obj) {
    const payload = JSON.stringify(obj);
    if (openaiReady && openaiWs.readyState === WebSocket.OPEN) {
      openaiWs.send(payload);
    } else {
      openaiQueue.push(payload);
    }
  }

  /* ---------- Twilio IN ---------- */

  twilioWs.on("message", (msg) => {
    const data = JSON.parse(msg.toString());

    if (data.event === "start") {
      // Send system + opening ONLY AFTER OPENAI OPEN
      sendToOpenAI({
        type: "response.create",
        response: {
          modalities: ["audio"],
          instructions:
            PROMPTS.find((p) => p.id === "opening")?.text ||
            "שלום, הגעתם לגיל ספורט.",
          voice: OPENAI_VOICE,
        },
      });
    }

    if (data.event === "media") {
      sendToOpenAI({
        type: "input_audio_buffer.append",
        audio: data.media.payload,
      });
    }

    if (data.event === "stop") {
      try {
        openaiWs?.close();
      } catch {}
    }
  });

  twilioWs.on("close", () => {
    try {
      openaiWs?.close();
    } catch {}
    console.log("[WS] Twilio disconnected");
  });
});

/* ===================== UPGRADE ===================== */

server.on("upgrade", (req, socket, head) => {
  if (req.url === "/twilio-media-stream") {
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws);
    });
  } else {
    socket.destroy();
  }
});

/* ===================== START ===================== */

const PORT = process.env.PORT || 10000;
server.listen(PORT, () => {
  console.log(`[INFO] GilSport VoiceBot running on port ${PORT}`);
});
