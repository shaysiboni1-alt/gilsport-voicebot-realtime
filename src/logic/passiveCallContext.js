"use strict";

const {
  basicNormalize,
  isAffirmativeHebrew,
  isClosingPhrase,
} = require("./hebrewNlp");
const { extractCallerName, sanitizeCandidate } = require("./nameExtractor");

function nowIso() {
  return new Date().toISOString();
}

function normalizeCallerId(caller) {
  const s = String(caller || "").trim();
  const low = s.toLowerCase();

  if (!s) return { value: "", withheld: true };

  if (
    ["anonymous", "restricted", "unavailable", "unknown", "private", "withheld"].includes(
      low
    )
  ) {
    return { value: s, withheld: true };
  }

  const digits = s.replace(/\D/g, "");
  return { value: s, withheld: digits.length < 5 };
}

function extractPhone(text) {
  const digits = String(text || "").replace(/\D/g, "");
  if (!digits) return "";

  if (digits.startsWith("972") && digits.length === 12) return `+${digits}`;
  if (digits.startsWith("0") && digits.length === 10) return `+972${digits.slice(1)}`;
  if (digits.length >= 9 && digits.length <= 13) return digits;

  return "";
}

function wasLastAssistantAskingForName(transcript) {
  const rows = Array.isArray(transcript) ? transcript : [];
  for (let i = rows.length - 1; i >= 0; i -= 1) {
    const row = rows[i];
    if (row?.role !== "assistant") continue;
    const text = String(row?.normalized || row?.text || "").trim();
    if (!text) return false;
    return /מה\s*השם|איך\s*קוראים|מי\s*מדבר|מי\s*מדברת|שמך|איך\s*קוראים\s*לכם|איך\s*קוראים\s*לך/u.test(text);
  }
  return false;
}

function createPassiveCallContext({
  callSid,
  streamSid,
  caller,
  called,
  source,
  caller_profile,
}) {
  const callerInfo = normalizeCallerId(caller);
  const memoryName = sanitizeCandidate(caller_profile?.display_name || caller_profile?.full_name || "") || "";

  return {
    callSid: callSid || "",
    streamSid: streamSid || "",
    source: source || "VoiceBot_Blank",
    caller_raw: callerInfo.value,
    caller_withheld: callerInfo.withheld,
    called: called || "",
    started_at: nowIso(),
    ended_at: null,
    returning_caller: !!caller_profile,
    returning_name: memoryName,
    returning_last_subject: caller_profile?.last_subject || "",
    returning_last_ended_at: caller_profile?.last_ended_at || null,
    language_locked: "he",
    language_observed: "he",
    name: "",
    callback_number: callerInfo.withheld ? "" : callerInfo.value,
    affirmed_callback_number: false,
    callback_requested: false,
    closing_detected: false,
    has_request: false,
    transcript: [],
  };
}

function appendUtterance(ctx, u) {
  if (!ctx) return;

  const role = u?.role || "";
  const text = String(u?.text || "");
  const normalized = String(u?.normalized || text || "");
  const lang = u?.lang || "unknown";

  ctx.transcript.push({
    role,
    text,
    normalized,
    lang,
    ts: nowIso(),
  });

  if (role !== "user") return;

  if (lang && lang !== "unknown") ctx.language_observed = lang;

  const found = extractCallerName({
    userText: normalized,
    lastBotUtterance: wasLastAssistantAskingForName(ctx.transcript) ? "איך קוראים לכם" : "",
  });
  if (found?.name) ctx.name = found.name;

  const phone = extractPhone(normalized);
  if (phone) ctx.callback_number = phone;

  if (/לחזור\s+אליי|שיחזרו\s+אליי|תחזרו\s+אליי|בקשת\s+חזרה/u.test(normalized)) {
    ctx.callback_requested = true;
  }

  if (isAffirmativeHebrew(normalized)) ctx.affirmed_callback_number = true;
  if (isClosingPhrase(normalized)) ctx.closing_detected = true;
  if (normalized.length >= 6) ctx.has_request = true;
}

function finalizeCtx(ctx) {
  if (!ctx) return null;
  ctx.ended_at = nowIso();
  return ctx;
}

function buildPassiveContext({ meta, ssot }) {
  const callMeta = meta || {};
  const ctx = createPassiveCallContext({
    callSid: callMeta.callSid,
    streamSid: callMeta.streamSid,
    caller: callMeta.caller,
    called: callMeta.called,
    source: callMeta.source,
    caller_profile: callMeta.caller_profile || null,
  });

  if (callMeta.startTs) {
    ctx.started_at = new Date(callMeta.startTs).toISOString();
  }

  if (ssot && typeof ssot.getSetting === "function") {
    ctx.time_zone = ssot.getSetting("TIME_ZONE") || null;
    ctx.supported_languages = ssot.getSetting("SUPPORTED_LANGUAGES") || null;
  }

  return ctx;
}

module.exports = {
  createPassiveCallContext,
  buildPassiveContext,
  appendUtterance,
  finalizeCtx,
};
