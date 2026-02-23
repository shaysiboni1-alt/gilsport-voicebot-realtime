"use strict";

const { logger } = require("../utils/logger");

function getUrlForLabel(label) {
  switch (label) {
    case "CALL_LOG":
      return process.env.CALL_LOG_WEBHOOK_URL || null;
    case "FINAL":
      return process.env.FINAL_WEBHOOK_URL || null;
    case "ABANDONED":
      return process.env.ABANDONED_WEBHOOK_URL || null;
    case "CONFIG_PUBLISHED":
      // optional; if not set we treat as skipped
      return process.env.CONFIG_PUBLISHED_WEBHOOK_URL || null;
    default:
      return null;
  }
}

function timeoutMs() {
  const raw = process.env.WEBHOOK_TIMEOUT_MS || process.env.RECORDING_PROXY_TIMEOUT_MS || "15000";
  const n = Number(raw);
  return Number.isFinite(n) && n > 0 ? n : 15000;
}

async function deliver(label, payload) {
  const url = getUrlForLabel(label);
  if (!url) {
    logger.info("Webhook skipped (no URL)", { label });
    return { ok: true, skipped: true, status: 0 };
  }

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs());

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify(payload ?? {}),
      signal: controller.signal,
    });

    logger.info("Webhook delivered", { label, status: res.status });
    return { ok: res.ok, status: res.status };
  } catch (err) {
    logger.error("Webhook delivery failed", {
      label,
      error: err && err.message ? err.message : String(err),
    });
    return { ok: false, status: 0, error: err && err.message ? err.message : String(err) };
  } finally {
    clearTimeout(t);
  }
}

module.exports = {
  deliver,
};
