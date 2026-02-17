"use strict";

// Minimal JSON logger (no external deps).
// - Matches the shape you saw in Render logs: {time, level, msg, meta}
// - Provides both `logger` and `getLogger()` for compatibility.

function nowIso() {
  return new Date().toISOString();
}

function emit(level, msg, meta) {
  const line = {
    time: nowIso(),
    level,
    msg: String(msg ?? ""),
  };
  if (meta && typeof meta === "object" && Object.keys(meta).length) {
    line.meta = meta;
  }

  // Render captures stdout/stderr.
  const s = JSON.stringify(line);
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
