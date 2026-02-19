"use strict";

// JSON logger (no external deps).
// Shape: { time, level, msg, meta, ts_ms, mono_ms }
// - Adds monotonic time (mono_ms) for reliable latency deltas.
// - Adds epoch millis (ts_ms) for easier analytics.
// - Adds child logger for sticky meta (callSid/streamSid/etc).
// - Avoids "[object Object]" by safely stringifying messages/meta.

const util = require("util");

function nowIso() {
  return new Date().toISOString();
}

function epochMs() {
  return Date.now();
}

// Monotonic clock relative to process start (ms).
function monoMs() {
  // process.hrtime.bigint() is monotonic.
  try {
    return Number(process.hrtime.bigint() / 1000000n);
  } catch {
    // Fallback: less accurate, but always available.
    const [s, ns] = process.hrtime();
    return Math.round(s * 1000 + ns / 1e6);
  }
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

function mergeMeta(baseMeta, meta) {
  const a = normalizeMeta(baseMeta);
  const b = normalizeMeta(meta);

  if (!a && !b) return undefined;
  if (!a) return b;
  if (!b) return a;

  // Both objects
  const out = { ...a, ...b };
  return Object.keys(out).length ? out : undefined;
}

function emit(level, msg, meta, baseMeta) {
  const line = {
    time: nowIso(),
    ts_ms: epochMs(),
    mono_ms: monoMs(),
    level,
    msg: normalizeMsg(msg),
  };

  const merged = mergeMeta(baseMeta, meta);
  if (merged && typeof merged === "object" && Object.keys(merged).length) {
    line.meta = merged;
  }

  const s = safeJsonStringify(line);

  // Render captures stdout/stderr.
  if (level === "error") console.error(s);
  else console.log(s);
}

function makeLogger(baseMeta) {
  const api = {
    info: (msg, meta) => emit("info", msg, meta, baseMeta),
    debug: (msg, meta) => emit("debug", msg, meta, baseMeta),
    warn: (msg, meta) => emit("warn", msg, meta, baseMeta),
    error: (msg, meta) => emit("error", msg, meta, baseMeta),

    // Create a child logger with sticky meta (callSid/streamSid/etc).
    child: (childMeta) => makeLogger(mergeMeta(baseMeta, childMeta)),
  };

  return api;
}

const logger = makeLogger(undefined);

function getLogger() {
  return logger;
}

module.exports = { logger, getLogger };
