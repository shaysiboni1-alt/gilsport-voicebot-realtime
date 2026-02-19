"use strict";

// Minimal JSON logger (no external deps).
// - Matches the shape you saw in Render logs: {time, level, msg, meta}
// - Provides both `logger` and `getLogger()` for compatibility.
// Stage A: avoid "[object Object]" by safely stringifying object messages.

const util = require("util");

function nowIso() {
  return new Date().toISOString();
}

function safeJsonStringify(value) {
  try {
    return JSON.stringify(value);
  } catch {
    // circular / non-serializable
    try {
      return util.inspect(value, { depth: 5, breakLength: 120, compact: true });
    } catch {
      return String(value);
    }
  }
}

function normalizeMsg(msg) {
  if (msg === undefined || msg === null) return "";
  if (typeof msg === "string") return msg;
  if (typeof msg === "number" || typeof msg === "boolean" || typeof msg === "bigint") return String(msg);
  if (msg instanceof Error) return msg.stack || msg.message || String(msg);

  // objects / arrays / etc.
  return safeJsonStringify(msg);
}

function normalizeMeta(meta) {
  if (!meta) return undefined;
  if (typeof meta !== "object") return { meta: String(meta) };

  // Ensure meta is serializable (avoid crashing the logger).
  try {
    JSON.stringify(meta);
    return meta;
  } catch {
    return { meta: safeJsonStringify(meta) };
  }
}

function emit(level, msg, meta) {
  const line = {
    time: nowIso(),
    level,
    msg: normalizeMsg(msg),
  };

  const m = normalizeMeta(meta);
  if (m && typeof m === "object" && Object.keys(m).length) {
    line.meta = m;
  }

  const s = safeJsonStringify(line);

  // Render captures stdout/stderr.
  if (level === "error") console.error(s);
  else console.log(s);
}

const logger = {
  info: (msg, meta) => emit("info", msg, meta),
  debug: (msg, meta) => emit("debug", msg, meta),
  warn: (msg, meta) => emit("warn", msg, meta),
  error: (msg, meta) => emit("error", msg, meta),
};

function getLogger() {
  return logger;
}

module.exports = { logger, getLogger };
