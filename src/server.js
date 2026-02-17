"use strict";

const express = require("express");
const path = require("path");

const { env } = require("./config/env");
const { logger } = require("./utils/logger");

const { healthRouter } = require("./routes/health");
const { adminReloadRouter } = require("./routes/adminReloadSheets");
const { recordingsRouter } = require("./routes/recordings");
const { twilioStatusRouter } = require("./routes/twilioStatus");

const { loadSSOT } = require("./ssot/ssotClient");
const { installTwilioMediaWs } = require("./ws/twilioMediaWs");

const { ensureCallerMemorySchema } = require("./memory/callerMemory");
const { setRecordingForCall } = require("./utils/recordingRegistry");
const { proxyRecordingMp3 } = require("./utils/twilioRecordings");

const app = express();

// Body parsers
app.use(express.json({ limit: "1mb" }));
app.use(express.urlencoded({ extended: false }));

// Static recordings
const recordingsDir =
  process.env.LOCAL_RECORDINGS_DIR ||
  path.join(__dirname, "..", "recordings");

app.use("/recordings", express.static(recordingsDir));

// Health root (Render probe)
app.get("/", (req, res) => {
  res.status(200).send("ok");
});

// Routers
app.use(healthRouter);
app.use(adminReloadRouter);
app.use(twilioStatusRouter);
app.use(recordingsRouter);

// Canonical recording proxy
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

// Twilio recording callback
app.post("/twilio-recording-callback", (req, res) => {
  try {
    const callSid = String(req.body?.CallSid || "").trim();
    const recordingSid = String(req.body?.RecordingSid || "").trim();
    const recordingUrl = String(req.body?.RecordingUrl || "").trim();

    if (callSid) {
      setRecordingForCall(callSid, {
        recordingSid: recordingSid || null,
        recordingUrl: recordingUrl || null,
      });
    }

    res.status(200).json({ ok: true });
  } catch (e) {
    logger.warn("twilio-recording-callback error", { err: String(e) });
    res.status(200).json({ ok: true });
  }
});

// 404 fallback
app.use((req, res) => {
  res.status(404).json({ error: "not_found" });
});

// Start server
const server = app.listen(env.PORT, "0.0.0.0", async () => {
  logger.info("Service started", {
    port: env.PORT,
    provider_mode: env.PROVIDER_MODE,
  });

  try {
    await loadSSOT(false);
  } catch (err) {
    logger.error("SSOT preload failed", {
      error: err?.message || String(err),
    });
  }

  try {
    await ensureCallerMemorySchema();
    logger.info("Caller memory schema ready");
  } catch (err) {
    logger.warn("Caller memory schema init failed", {
      error: err?.message || String(err),
    });
  }
});

// Attach Twilio WS
installTwilioMediaWs(server);
