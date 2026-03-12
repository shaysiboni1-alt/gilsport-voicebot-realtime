"use strict";

function nowIso() {
  return new Date().toISOString();
}

function isPlainObject(v) {
  return !!v && typeof v === "object" && !Array.isArray(v);
}

function safeJson(value) {
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return { value: String(value) };
  }
}

function isTruthyEnv(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "y";
}

function shouldSuppressReasoningText(text) {
  const t = String(text || "").trim();
  if (!t) return false;

  if (/^\*\*/.test(t)) return true;

  if (
    /\b(Addressing the Request|Refining the Approach|Initiating Callback Request|Concluding the Details|Uttering the Specific Phrase)\b/i.test(
      t
    )
  ) {
    return true;
  }

  if (
    /\bI(?:'| a)m now\b/i.test(t) ||
    /\bMy next step will be\b/i.test(t) ||
    /\bI've determined\b/i.test(t)
  ) {
    return true;
  }

  return false;
}

function normalizeArgs(msg, meta) {
  let outMsg = msg;
  let outMeta = meta;

  if (isPlainObject(msg) && typeof msg.msg === "string") {
    outMsg = msg.msg;
    outMeta = isPlainObject(msg.meta)
      ? { ...(isPlainObject(meta) ? meta : {}), ...msg.meta }
      : meta;
  } else if (isPlainObject(msg) && meta === undefined) {
    outMsg = msg.message || msg.event || "object";
    outMeta = msg;
  }

  if (outMsg === undefined || outMsg === null) outMsg = "";
  if (typeof outMsg !== "string") outMsg = String(outMsg);

  if (!isPlainObject(outMeta)) outMeta = undefined;

  return { msg: outMsg, meta: outMeta };
}

function emit(level, msg, meta) {
  const normalized = normalizeArgs(msg, meta);
  const debugEnabled = isTruthyEnv(process.env.MB_DEBUG);

  if (!debugEnabled && normalized.msg === "Gemini text") {
    const t = normalized.meta?.t;
    if (shouldSuppressReasoningText(t)) return;
  }

  const line = {
    time: nowIso(),
    level,
    msg: normalized.msg,
  };

  if (normalized.meta && Object.keys(normalized.meta).length) {
    line.meta = safeJson(normalized.meta);
  }

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

module.exports = {
  logger,
  getLogger,
  shouldSuppressReasoningText,
};
