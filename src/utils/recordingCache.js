"use strict";

const fs = require("fs");
const path = require("path");
const { Readable } = require("stream");

// Best-effort local cache for Twilio recordings (mp3).
// Goals:
// - Never block call flow; cache is optional.
// - Stream from Twilio -> disk with hard timeouts (connect + total).
// - Serve cached files quickly via server route.

const DEFAULT_CONNECT_TIMEOUT_MS = 8000;
const DEFAULT_TOTAL_TIMEOUT_MS = 20000;

function hasTwilioCreds() {
  return Boolean(
    process.env.TWILIO_ACCOUNT_SID &&
      String(process.env.TWILIO_ACCOUNT_SID).trim() &&
      process.env.TWILIO_AUTH_TOKEN &&
      String(process.env.TWILIO_AUTH_TOKEN).trim()
  );
}

function getCacheDir() {
  return process.env.RECORDING_CACHE_DIR || path.join(process.cwd(), "data", "recordings");
}

function ensureCacheDir() {
  const dir = getCacheDir();
  fs.mkdirSync(dir, { recursive: true });
  return dir;
}

function recordingPath(recordingSid, ext = "mp3") {
  const dir = ensureCacheDir();
  return path.join(dir, `${recordingSid}.${ext}`);
}

function hasCached(recordingSid) {
  try {
    const p = recordingPath(recordingSid);
    return fs.existsSync(p) && fs.statSync(p).size > 0;
  } catch {
    return false;
  }
}

function twilioAuthHeader() {
  const sid = String(process.env.TWILIO_ACCOUNT_SID || "").trim();
  const token = String(process.env.TWILIO_AUTH_TOKEN || "").trim();
  const b64 = Buffer.from(`${sid}:${token}`, "utf8").toString("base64");
  return `Basic ${b64}`;
}

async function fetchWithRedirect(url, opts) {
  // Follow up to 3 redirects manually (Twilio may redirect media to CDN).
  let current = url;
  for (let i = 0; i < 4; i++) {
    const r = await fetch(current, { ...opts, redirect: "manual" });
    if (r.status >= 300 && r.status < 400) {
      const loc = r.headers.get("location");
      if (!loc) return r;
      current = new URL(loc, current).toString();
      continue;
    }
    return r;
  }
  return fetch(url, opts);
}

async function getRecordingStatus(recordingSid, { timeoutMs } = {}) {
  if (!hasTwilioCreds()) return { ok: false, status: "no_creds" };
  const sid = String(process.env.TWILIO_ACCOUNT_SID || "").trim();
  const url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(
    sid
  )}/Recordings/${encodeURIComponent(recordingSid)}.json`;

  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(),
    Math.max(1000, Number(timeoutMs || process.env.RECORDING_STATUS_TIMEOUT_MS || DEFAULT_CONNECT_TIMEOUT_MS))
  );

  try {
    const r = await fetchWithRedirect(url, {
      method: "GET",
      headers: { Authorization: twilioAuthHeader(), Accept: "application/json" },
      signal: controller.signal,
    });
    if (!r.ok) return { ok: false, status: `http_${r.status}` };
    const data = await r.json().catch(() => null);
    const status = data && typeof data.status === "string" ? data.status : "unknown";
    return { ok: true, status, data };
  } catch (e) {
    return { ok: false, status: e && e.name === "AbortError" ? "timeout" : "error", err: String(e) };
  } finally {
    clearTimeout(timer);
  }
}

async function downloadToFile(recordingSid, filePath, opts = {}) {
  if (!hasTwilioCreds()) throw new Error("Missing TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN");

  const sid = String(process.env.TWILIO_ACCOUNT_SID || "").trim();
  const mp3Url = `https://api.twilio.com/2010-04-01/Accounts/${encodeURIComponent(
    sid
  )}/Recordings/${encodeURIComponent(recordingSid)}.mp3`;

  const totalTimeoutMs = Number(
    opts.totalTimeoutMs || process.env.RECORDING_TOTAL_TIMEOUT_MS || DEFAULT_TOTAL_TIMEOUT_MS
  );

  const controller = new AbortController();
  const hardTimer = setTimeout(() => controller.abort(), Math.max(1000, totalTimeoutMs));

  let r;
  try {
    // IMPORTANT: keep hardTimer active until the stream finishes.
    r = await fetchWithRedirect(mp3Url, {
      method: "GET",
      headers: { Authorization: twilioAuthHeader(), Accept: "audio/mpeg" },
      signal: controller.signal,
    });

    if (!r.ok) {
      throw new Error(`Twilio fetch failed: ${r.status} ${r.statusText}`);
    }

    const ws = fs.createWriteStream(filePath);

    await new Promise((resolve, reject) => {
      const onErr = (err) => reject(err);
      ws.on("error", onErr);

      if (!r.body) return reject(new Error("Twilio response has no body"));

      let rs;
      try {
        rs = Readable.fromWeb(r.body);
      } catch {
        rs = r.body;
      }
      rs.on("error", onErr);

      rs.pipe(ws);
      ws.on("finish", resolve);
    });

    return { ok: true, mp3Url, filePath };
  } finally {
    clearTimeout(hardTimer);
  }
}

module.exports = {
  hasTwilioCreds,
  getCacheDir,
  recordingPath,
  hasCached,
  getRecordingStatus,
  downloadToFile,
};
