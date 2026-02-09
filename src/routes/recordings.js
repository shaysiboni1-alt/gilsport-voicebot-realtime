// src/routes/recordings.js
"use strict";

const express = require("express");
const { env } = require("../config/env");
const { logger } = require("../utils/logger");

const recordingsRouter = express.Router();

/**
 * Public proxy for Twilio recording media.
 * Why: Twilio Recording media URLs require basic auth (AccountSid/AuthToken).
 * This endpoint fetches the mp3 from Twilio with auth, and streams it publicly.
 *
 * URL: GET /recordings/:recordingSid.mp3
 *
 * NOTE (robustness): some clients (and older webhook payloads) mistakenly call
 *   /recording/<RecordingSid>.mp3
 *   /recordings/<RecordingSid>.mp3.mp3
 * We normalize the recording SID defensively.
 */
recordingsRouter.get("/recordings/:recordingSid.mp3", async (req, res) => {
  let recordingSid = String(req.params.recordingSid || "").trim();
  // tolerate accidental suffixes
  if (recordingSid.toLowerCase().endsWith(".mp3")) {
    recordingSid = recordingSid.slice(0, -4);
  }
  if (!recordingSid) return res.status(400).send("missing recordingSid");

  const accountSid = env.TWILIO_ACCOUNT_SID;
  const authToken = env.TWILIO_AUTH_TOKEN;

  if (!accountSid || !authToken) {
    return res.status(500).send("missing TWILIO creds");
  }

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(
      accountSid
    )}/Recordings/${encodeURIComponent(recordingSid)}.mp3`;

    const basic = Buffer.from(`${accountSid}:${authToken}`).toString("base64");

    const r = await fetch(url, {
      method: "GET",
      headers: {
        authorization: `Basic ${basic}`,
        "user-agent": "voicebot-blank/recording-proxy"
      }
    });

    if (!r.ok) {
      const body = await r.text().catch(() => "");
      logger.warn("Recording proxy fetch failed", {
        status: r.status,
        recordingSid,
        body: body?.slice(0, 240)
      });
      return res.status(502).send("twilio fetch failed");
    }

    res.status(200);
    res.setHeader("content-type", "audio/mpeg");
    res.setHeader("cache-control", "public, max-age=31536000, immutable");

    // Stream through
    const buf = Buffer.from(await r.arrayBuffer());
    res.send(buf);
  } catch (err) {
    logger.error("Recording proxy error", { recordingSid, error: err?.message || String(err) });
    res.status(500).send("proxy error");
  }
});

// Compatibility aliases:
// 1) /recording/<RecordingSid>
// 2) /recording/<RecordingSid>.mp3
// (Older payloads and some users paste links in this form)
function redirectToCanonicalRecording(req, res) {
  let recordingSid = String(req.params.recordingSid || "").trim();
  if (recordingSid.toLowerCase().endsWith(".mp3")) {
    recordingSid = recordingSid.slice(0, -4);
  }
  if (!recordingSid) return res.status(400).send("missing recordingSid");
  return res.redirect(302, `/recordings/${encodeURIComponent(recordingSid)}.mp3`);
}

recordingsRouter.get("/recording/:recordingSid", redirectToCanonicalRecording);
recordingsRouter.get("/recording/:recordingSid.mp3", redirectToCanonicalRecording);

module.exports = { recordingsRouter };
