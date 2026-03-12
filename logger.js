// src/routes/twilioStatus.js
"use strict";

const express = require("express");
const { logger } = require("../utils/logger");

const twilioStatusRouter = express.Router();

/**
 * Twilio "Call status changes" webhook endpoint.
 *
 * Twilio expects this URL to return HTTP 200 quickly. If the handler hangs,
 * Twilio will show Warning 15003 / HTTP 502 (timeout ~30s).
 *
 * We intentionally keep this endpoint minimal and non-blocking.
 */
function handleTwilioStatus(req, res) {
  try {
    const body = req.body && typeof req.body === "object" ? req.body : {};
    const meta = {
      callSid: body.CallSid || body.callSid || null,
      callStatus: body.CallStatus || body.callStatus || null,
      from: body.From || body.from || null,
      to: body.To || body.to || null,
      apiVersion: body.ApiVersion || body.apiVersion || null,
    };

    logger.info("Twilio status webhook", meta);
  } catch (e) {
    logger.warn("Twilio status webhook parse error", { err: String(e?.message || e) });
  }

  // Always respond immediately.
  res.status(200).type("text/plain").send("ok");
}

// Twilio sends POST; we also allow GET for quick manual checks.
twilioStatusRouter.post("/twilio/status", handleTwilioStatus);
twilioStatusRouter.get("/twilio/status", handleTwilioStatus);

module.exports = { twilioStatusRouter };
