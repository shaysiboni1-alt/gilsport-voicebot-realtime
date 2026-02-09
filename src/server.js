// src/server.js
"use strict";

const express = require("express");

const { env } = require("./config/env");
const { logger } = require("./utils/logger");

const { healthRouter } = require("./routes/health");
const { adminReloadRouter } = require("./routes/adminReloadSheets");
const { recordingsRouter } = require("./routes/recordings");
const { twilioStatusRouter } = require("./routes/twilioStatus");

const { loadSSOT } = require("./ssot/ssotClient");
const { installTwilioMediaWs } = require("./ws/twilioMediaWs");

// Lead/Recording support (does not affect audio pipeline)
const { setRecordingForCall } = require("./utils/recordingRegistry");
const { proxyRecordingMp3 } = require("./utils/twilioRecordings");

const app = express();

// JSON payloads (webhooks/admin)
app.use(express.json({ limit: "1mb" }));

// Twilio RecordingStatusCallback sends x-www-form-urlencoded
app.use(express.urlencoded({ extended: false }));

// IMPORTANT: Render health/probe hits GET /
// Must return fast 200 so Render detects the open HTTP port.
app.get("/", (req, res) => {
  res.status(200).send("ok");
});

// --- Routers ---
app.use(healthRouter);
app.use(adminReloadRouter);
app.use(twilioStatusRouter);

// Public proxy endpoints (existing)
app.use(recordingsRouter);

// Canonical recording proxy endpoint (alias): /recording/:sid.mp3
app.get("/recording/:sid.mp3", async (req, res) => {
  const sid = String(req.params.sid || "").trim();
  if (!sid) return res.status(400).send("missing_sid");
  try {
    await proxyRecordingMp3(sid, res, logger);
  } catch (e) {
    logger.warn("recording proxy failed", { err: String(e) });
    if (!res.headersSent) res.status(500).send("proxy_error");
  }
});

// Twilio async recording callback
app.post("/twilio-recording-callback", (req, res) => {
  try {
    const callSid = String(req.body?.CallSid || "").trim();
    const recordingSid = String(req.body?.RecordingSid || "").trim();
    const recordingUrl = String(req.body?.RecordingUrl || "").trim();

    if (callSid) {
      setRecordingForCall(callSid, {
        recordingSid: recordingSid || null,
        recordingUrl: recordingUrl || null
      });
    }

    // Respond quickly (Twilio expects fast ACK)
    res.status(200).json({ ok: true });
  } catch (e) {
    logger.warn("twilio-recording-callback error", { err: String(e) });
    res.status(200).json({ ok: true });
  }
});

app.use((req, res) => {
  res.status(404).json({ error: "not_found" });
});

// IMPORTANT: bind explicitly to 0.0.0.0 for Render/containers
const server = app.listen(env.PORT, "0.0.0.0", async () => {
  logger.info("Service started", {
    port: env.PORT,
    provider_mode: env.PROVIDER_MODE
  });

  // Best-effort preload SSOT
  try {
    await loadSSOT(false);
  } catch (err) {
    logger.error("SSOT preload failed", { error: err?.message || String(err) });
  }
});

// IMPORTANT: attach WS upgrade handler to the real HTTP server
installTwilioMediaWs(server);
