// server.js
// Twilio Media Streams <-> Gemini Live (Realtime) bridge
// - In: Twilio sends 8kHz mu-law frames
// - Convert to PCM16 8kHz -> resample to PCM16 16kHz -> send to Gemini realtimeInput.audio
// - Out: Gemini returns PCM16 (often 24kHz) -> resample to PCM16 8kHz -> mu-law -> send to Twilio "media"

"use strict";

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { GoogleGenAI } = require("@google/genai");

const PORT = process.env.PORT || 10000;

// ===== ENV =====
const GEMINI_API_KEY =
  process.env.GEMINI_API_KEY ||
  process.env.GOOGLE_API_KEY ||
  process.env.GOOGLE_GENAI_API_KEY ||
  "";

const GEMINI_MODEL =
  process.env.GEMINI_MODEL || "gemini-2.0-flash-live-001"; // change if you use another live model

const OPENING_TEXT =
  process.env.OPENING_TEXT ||
  "שָׁלוֹם, הִגַּעְתֶּם לְ־גִּיל סְפּוֹרְט. מְדַבֶּרֶת נֶטַע. אֵיךְ אֶפְשָׁר לַעֲזוֹר לָכֶם?";

const SYSTEM_PROMPT =
  process.env.SYSTEM_PROMPT ||
  [
    "You are 'Netta', a realtime voice agent for GilSport.",
    "You can speak Hebrew, English, Russian, and Arabic. Mirror the caller language naturally.",
    "Be short, fast, helpful. Ask one question at a time.",
    "If user asks to switch language, switch immediately.",
  ].join("\n");

// ===== HTTP app =====
const app = express();

// Root
app.get("/", (req, res) => res.status(200).send("OK"));

// Render health check hits /health (per your log)
app.get("/health", (req, res) => res.status(200).send("OK"));

// Quick visibility into env/config (no secrets)
app.get("/config-check", (req, res) => {
  res.status(200).json({
    ok: true,
    port: PORT,
    hasGeminiKey: Boolean(GEMINI_API_KEY),
    geminiModel: GEMINI_MODEL,
    hasOpeningText: Boolean(OPENING_TEXT),
    hasSystemPrompt: Boolean(SYSTEM_PROMPT),
  });
});

const server = http.createServer(app);

// ===== WS server for Twilio =====
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

function logInfo(...args) {
  console.log("[INFO]", ...args);
}
function logWarn(...args) {
  console.warn("[WARN]", ...args);
}
function logErr(...args) {
  console.error("[ERR]", ...args);
}

// ---------- Audio helpers (mu-law, resample) ----------

// mu-law decode/encode (G.711 mu-law)
const MULAW_MAX = 0x1fff;
const BIAS = 33;

function mulawToLinearSample(muLawByte) {
  let u = (~muLawByte) & 0xff;
  let sign = u & 0x80;
  let exponent = (u >> 4) & 0x07;
  let mantissa = u & 0x0f;
  let sample = ((mantissa << 1) + 1) << (exponent + 2);
  sample -= BIAS;
  return sign ? -sample : sample;
}

function linearToMulawSample(sample) {
  // clamp
  let s = sample;
  if (s > 32767) s = 32767;
  if (s < -32768) s = -32768;

  let sign = 0;
  if (s < 0) {
    sign = 0x80;
    s = -s;
  }

  // add bias
  s = s + BIAS;
  if (s > MULAW_MAX) s = MULAW_MAX;

  // find exponent
  let exponent = 7;
  for (let exp = 0; exp < 8; exp++) {
    if (s <= (0x1f << (exp + 3))) {
      exponent = exp;
      break;
    }
  }

  let mantissa = (s >> (exponent + 3)) & 0x0f;
  let mu = ~(sign | (exponent << 4) | mantissa) & 0xff;
  return mu;
}

function mulawBytesToPcm16Buffer(muLawBuf) {
  const out = Buffer.alloc(muLawBuf.length * 2);
  for (let i = 0; i < muLawBuf.length; i++) {
    const sample = mulawToLinearSample(muLawBuf[i]);
    out.writeInt16LE(sample, i * 2);
  }
  return out;
}

function pcm16BufferToMulawBytes(pcmBuf) {
  const samples = pcmBuf.length / 2;
  const out = Buffer.alloc(samples);
  for (let i = 0; i < samples; i++) {
    const sample = pcmBuf.readInt16LE(i * 2);
    out[i] = linearToMulawSample(sample);
  }
  return out;
}

// Simple linear resampler for PCM16LE mono
function resamplePcm16Linear(pcmBuf, inRate, outRate) {
  if (inRate === outRate) return pcmBuf;

  const inSamples = pcmBuf.length / 2;
  const inArr = new Int16Array(inSamples);
  for (let i = 0; i < inSamples; i++) inArr[i] = pcmBuf.readInt16LE(i * 2);

  const ratio = outRate / inRate;
  const outSamples = Math.max(1, Math.floor(inSamples * ratio));
  const outBuf = Buffer.alloc(outSamples * 2);

  for (let i = 0; i < outSamples; i++) {
    const srcPos = i / ratio;
    const i0 = Math.floor(srcPos);
    const i1 = Math.min(i0 + 1, inSamples - 1);
    const t = srcPos - i0;

    const s0 = inArr[i0];
    const s1 = inArr[i1];
    const v = Math.round(s0 + (s1 - s0) * t);

    outBuf.writeInt16LE(v, i * 2);
  }
  return outBuf;
}

// Split buffer into chunks
function chunkBuffer(buf, chunkBytes) {
  const chunks = [];
  for (let i = 0; i < buf.length; i += chunkBytes) {
    chunks.push(buf.subarray(i, Math.min(i + chunkBytes, buf.length)));
  }
  return chunks;
}

// Twilio expects 20ms frames: 8kHz => 160 samples => 160 mu-law bytes
const TWILIO_FRAME_MS = 20;
const TWILIO_RATE = 8000;
const TWILIO_FRAME_BYTES = Math.floor((TWILIO_RATE * TWILIO_FRAME_MS) / 1000); // 160 bytes

// Gemini input target
const GEMINI_IN_RATE = 16000;

// We'll assume Gemini outputs 24000 PCM unless detected otherwise
const GEMINI_OUT_RATE_DEFAULT = 24000;

// ---------- Gemini Live session wrapper ----------

async function createGeminiLiveSession() {
  if (!GEMINI_API_KEY) throw new Error("Missing GEMINI_API_KEY / GOOGLE_API_KEY");

  const ai = new GoogleGenAI({ apiKey: GEMINI_API_KEY });

  const config = {
    systemInstruction: SYSTEM_PROMPT,
    generationConfig: {
      responseModalities: ["AUDIO"], // we want audio back
      // You can tune later: temperature, etc.
    },
  };

  // connect returns a session with sendRealtimeInput() and receive() async iterator
  const session = await ai.live.connect({
    model: GEMINI_MODEL,
    config,
  });

  return session;
}

// ---------- Main WS handling ----------

wss.on("connection", async (ws, req) => {
  const connId = `tw_${Math.random().toString(36).slice(2, 12)}`;
  logInfo(`[WS][${connId}] connected`, { peer: req.socket.remoteAddress, path: req.url });

  let streamSid = null;
  let callSid = null;

  let geminiSession = null;
  let geminiReceiverTask = null;

  // playback queue of mu-law frames to send to Twilio paced
  let playQueue = [];
  let playTimer = null;
  let interrupted = false;

  function startPlaybackPump() {
    if (playTimer) return;
    playTimer = setInterval(() => {
      if (ws.readyState !== WebSocket.OPEN) return;
      if (playQueue.length === 0) return;

      // send one 20ms frame per tick
      const frame = playQueue.shift();
      const payloadB64 = frame.toString("base64");

      const msg = {
        event: "media",
        streamSid,
        media: { payload: payloadB64 },
      };
      ws.send(JSON.stringify(msg));
    }, TWILIO_FRAME_MS);
  }

  function stopPlaybackPump() {
    if (playTimer) clearInterval(playTimer);
    playTimer = null;
    playQueue = [];
  }

  function enqueuePcmToTwilio(pcmBuf, pcmRate) {
    // resample to 8k
    const pcm8k = resamplePcm16Linear(pcmBuf, pcmRate, TWILIO_RATE);
    // to mu-law bytes
    const mu = pcm16BufferToMulawBytes(pcm8k);
    // chunk into 20ms frames (160 bytes)
    const frames = chunkBuffer(mu, TWILIO_FRAME_BYTES);
    for (const f of frames) playQueue.push(f);
    startPlaybackPump();
  }

  async function startGemini(customParameters) {
    geminiSession = await createGeminiLiveSession();

    // Start receiver loop
    geminiReceiverTask = (async () => {
      try {
        for await (const msg of geminiSession.receive()) {
          // If the model says it's interrupted, drop queued playback (barge-in)
          if (msg?.serverContent?.interrupted) {
            interrupted = true;
            stopPlaybackPump();
          }

          // Audio from model is usually in: msg.serverContent.modelTurn.parts[].inlineData
          const parts = msg?.serverContent?.modelTurn?.parts || [];
          for (const p of parts) {
            const inline = p.inlineData;
            if (inline?.data && typeof inline.data === "string") {
              // We don't always get mimeType reliably; assume PCM16 @ 24000 unless specified
              const mime = inline.mimeType || "";
              let outRate = GEMINI_OUT_RATE_DEFAULT;

              const m = /rate=(\d+)/.exec(mime);
              if (m) outRate = parseInt(m[1], 10);

              const pcm = Buffer.from(inline.data, "base64");
              enqueuePcmToTwilio(pcm, outRate);
            }
          }
        }
      } catch (e) {
        logErr(`[WS][${connId}] gemini receive loop error`, e?.message || e);
      }
    })();

    // Trigger opening from Gemini (NOT Twilio)
    const opener =
      (customParameters?.opening_text && String(customParameters.opening_text)) || OPENING_TEXT;

    await geminiSession.sendRealtimeInput({
      text: opener,
    });
  }

  async function closeAll() {
    try {
      stopPlaybackPump();
      if (geminiSession) {
        try {
          // Tell Gemini audio stream ended
          await geminiSession.sendRealtimeInput({ audioStreamEnd: true });
        } catch {}
        try {
          await geminiSession.close();
        } catch {}
      }
    } finally {
      geminiSession = null;
      geminiReceiverTask = null;
    }
  }

  ws.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString("utf8"));
    } catch {
      return;
    }

    if (msg.event === "start") {
      streamSid = msg.start?.streamSid || null;
      callSid = msg.start?.callSid || null;

      const customParameters = msg.start?.customParameters || {};

      logInfo(`[WS][${connId}] start`, { streamSid, callSid, customParameters });

      try {
        await startGemini(customParameters);
        logInfo(`[WS][${connId}] gemini live connected`);
      } catch (e) {
        logErr(`[WS][${connId}] failed to start gemini`, e?.message || e);
      }
      return;
    }

    if (msg.event === "media") {
      if (!geminiSession) return;

      // Twilio media payload is base64 mu-law 8kHz mono
      const b64 = msg.media?.payload;
      if (!b64) return;

      const muLaw = Buffer.from(b64, "base64");
      const pcm8k = mulawBytesToPcm16Buffer(muLaw);
      const pcm16k = resamplePcm16Linear(pcm8k, TWILIO_RATE, GEMINI_IN_RATE);

      try {
        await geminiSession.sendRealtimeInput({
          audio: {
            mimeType: `audio/pcm;rate=${GEMINI_IN_RATE}`,
            data: pcm16k.toString("base64"),
          },
        });
      } catch (e) {
        // If Gemini disconnects, avoid crashing
        logWarn(`[WS][${connId}] sendRealtimeInput audio failed`, e?.message || e);
      }
      return;
    }

    if (msg.event === "stop") {
      logInfo(`[WS][${connId}] stop`, {
        streamSid: msg.stop?.streamSid,
        callSid: msg.stop?.callSid,
      });
      await closeAll();
      return;
    }
  });

  ws.on("close", async () => {
    logInfo(`[WS][${connId}] closed`, { streamSid, callSid });
    await closeAll();
  });

  ws.on("error", async (err) => {
    logErr(`[WS][${connId}] ws error`, err?.message || err);
    await closeAll();
  });
});

server.listen(PORT, () => {
  console.log("[BOOT] listening on", PORT);
});
