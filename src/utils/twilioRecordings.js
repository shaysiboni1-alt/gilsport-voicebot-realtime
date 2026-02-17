"use strict";

const { Readable } = require("node:stream");
const fs = require("fs");
const path = require("path");

// Check boolean-like env
function isTrue(v) {
  return String(v || "").toLowerCase() === "true";
}

// Build Authorization header for Twilio
function twilioAuthHeader() {
  const sid = process.env.TWILIO_ACCOUNT_SID || "";
  const token = process.env.TWILIO_AUTH_TOKEN || "";
  const basic = Buffer.from(`${sid}:${token}`).toString("base64");
  return `Basic ${basic}`;
}

// Build base URL for Twilio API
function twilioBase() {
  const sid = process.env.TWILIO_ACCOUNT_SID;
  return `https://api.twilio.com/2010-04-01/Accounts/${sid}`;
}

// Start a recording for a call
async function startCallRecording(callSid, logger) {
  const enabled = isTrue(process.env.MB_ENABLE_RECORDING);
  if (!enabled) {
    return { ok: false, recordingSid: null, reason: "recording_disabled" };
  }

  if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) {
    logger?.warn?.("TWILIO creds missing; cannot start recording");
    return { ok: false, recordingSid: null, reason: "twilio_creds_missing" };
  }

  const base = String(process.env.PUBLIC_BASE_URL || "").trim().replace(/\/+$/, "");
  if (!base) {
    logger?.warn?.("PUBLIC_BASE_URL missing; cannot configure recording callback");
    return { ok: false, recordingSid: null, reason: "public_base_url_missing" };
  }

  try {
    const url = `${twilioBase()}/Calls/${encodeURIComponent(callSid)}/Recordings.json`;
    const body = new URLSearchParams({
      RecordingStatusCallback: `${base}/twilio-recording-callback`,
      RecordingStatusCallbackMethod: "POST",
      RecordingStatusCallbackEvent: "completed",
      RecordingChannels: "dual",
    });

    const resp = await fetch(url, {
      method: "POST",
      headers: {
        authorization: twilioAuthHeader(),
        "content-type": "application/x-www-form-urlencoded",
      },
      body,
    });

    const txt = await resp.text().catch(() => "");
    if (!resp.ok) {
      logger?.warn?.("Twilio start recording failed", {
        status: resp.status,
        body: txt?.slice?.(0, 300),
      });
      return { ok: false, recordingSid: null, reason: `twilio_${resp.status}` };
    }

    const j = JSON.parse(txt);
    return { ok: true, recordingSid: j.sid || null, reason: "started" };
  } catch (e) {
    logger?.warn?.("Twilio start recording exception", { err: String(e) });
    return { ok: false, recordingSid: null, reason: "twilio_start_exception" };
  }
}

// Build public proxy URL for a recording (fallback)
function publicRecordingUrl(recordingSid) {
  const base = process.env.PUBLIC_BASE_URL || "";
  if (!base || !recordingSid) return null;
  return `${base.replace(/\/$/, "")}/recording/${recordingSid}.mp3`;
}

// Hang up a Twilio call
async function hangupCall(callSid, logger) {
  if (!process.env.TWILIO_ACCOUNT_SID || !process.env.TWILIO_AUTH_TOKEN) return false;
  try {
    const url = `${twilioBase()}/Calls/${encodeURIComponent(callSid)}.json`;
    const body = new URLSearchParams({ Status: "completed" });
    const resp = await fetch(url, {
      method: "POST",
      headers: {
        authorization: twilioAuthHeader(),
        "content-type": "application/x-www-form-urlencoded",
      },
      body,
    });
    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      logger?.warn?.("Twilio hangup failed", { status: resp.status, body: t?.slice?.(0, 250) });
      return false;
    }
    return true;
  } catch (e) {
    logger?.warn?.("Twilio hangup exception", { err: String(e) });
    return false;
  }
}

// Streaming proxy for Twilio recording MP3 with timeout
async function proxyRecordingMp3(recordingSid, res, logger) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  if (!accountSid || !authToken) {
    res.statusCode = 503;
    res.end("twilio_not_configured");
    return;
  }

  const timeoutMs = Number(process.env.RECORDING_PROXY_TIMEOUT_MS || 20000);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Recordings/${encodeURIComponent(
      recordingSid
    )}.mp3`;

    const resp = await fetch(url, {
      method: "GET",
      headers: { authorization: twilioAuthHeader() },
      signal: controller.signal,
    });

    if (!resp.ok) {
      const t = await resp.text().catch(() => "");
      res.statusCode = resp.status;
      res.end(t || `twilio_${resp.status}`);
      return;
    }

    res.setHeader("content-type", "audio/mpeg");
    res.setHeader("cache-control", "public, max-age=31536000, immutable");

    const nodeStream = Readable.fromWeb(resp.body);

    nodeStream.on("error", (e) => {
      logger?.warn?.("recording proxy stream error", { err: String(e), recordingSid });
      try {
        res.end();
      } catch {}
    });

    nodeStream.pipe(res);
  } catch (e) {
    const name = String(e?.name || "").toLowerCase();
    if (name.includes("abort")) {
      logger?.warn?.("recording proxy timeout", { recordingSid, timeoutMs });
      res.statusCode = 504;
      res.end("recording_fetch_timeout");
      return;
    }
    logger?.warn?.("recording proxy exception", { err: String(e), recordingSid });
    res.statusCode = 500;
    res.end("proxy_error");
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Download the MP3 recording from Twilio and save it locally.
 * Returns { filePath, publicUrl } on success, or null on failure.
 */
async function downloadRecording(recordingSid, logger) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  if (!accountSid || !authToken || !recordingSid) return null;
  try {
    const url = `https://api.twilio.com/2010-04-01/Accounts/${accountSid}/Recordings/${encodeURIComponent(
      recordingSid
    )}.mp3`;
    const resp = await fetch(url, {
      method: "GET",
      headers: { authorization: twilioAuthHeader() },
    });
    if (!resp.ok) {
      const txt = await resp.text().catch(() => "");
      logger?.warn?.("Twilio download recording failed", {
        status: resp.status,
        body: txt?.slice?.(0, 300),
      });
      return null;
    }
    const buffer = Buffer.from(await resp.arrayBuffer());
    const recordingsDir =
      process.env.LOCAL_RECORDINGS_DIR ||
      path.join(__dirname, "..", "..", "recordings");
    fs.mkdirSync(recordingsDir, { recursive: true });
    const filePath = path.join(recordingsDir, `${recordingSid}.mp3`);
    fs.writeFileSync(filePath, buffer);
    const base = process.env.PUBLIC_BASE_URL || "";
    const publicUrl = `${base.replace(/\/$/, "")}/recordings/${recordingSid}.mp3`;
    return { filePath, publicUrl };
  } catch (e) {
    logger?.warn?.("Twilio download recording exception", {
      err: String(e),
      recordingSid,
    });
    return null;
  }
}

module.exports = {
  startCallRecording,
  publicRecordingUrl,
  hangupCall,
  proxyRecordingMp3,
  downloadRecording,
};
