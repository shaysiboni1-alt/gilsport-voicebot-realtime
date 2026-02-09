"use strict";

// src/logic/passiveCallContext.js
// Passive, non-breaking call context aggregator.
// Goal: capture name / callback number / request readiness deterministically,
// while letting SSOT-driven LLM run the conversation.

function nowIso() {
  return new Date().toISOString();
}

function normalizeCallerId(caller) {
  const s = (caller || "").trim();
  const low = s.toLowerCase();
  if (!s) return { value: "", withheld: true };

  // Common Twilio/telephony withheld values
  if (
    low === "anonymous" ||
    low === "restricted" ||
    low === "unavailable" ||
    low === "unknown" ||
    low === "private" ||
    low === "withheld"
  ) {
    return { value: s, withheld: true };
  }

  const digits = s.replace(/\D/g, "");
  // If it has enough digits, treat as not withheld
  return { value: s, withheld: digits.length < 5 };
}

function extractNameHe(text) {
  const t = (text || "").trim();
  if (!t) return "";

  // Common patterns
  const m = t.match(/(?:קוראים לי|השם שלי(?: זה)?|שמי|אני)\s+([^\n,.!?]{2,40})/);
  if (m && m[1]) return m[1].trim();

  // Fallback: short single token, no digits
  if (t.length <= 25 && !/[0-9]/.test(t)) {
    return t.replace(/^אה+[, ]*/g, "").trim();
  }
  return "";
}

function extractPhone(text) {
  const digits = (text || "").replace(/\D/g, "");
  if (!digits) return "";

  // Israeli heuristic
  if (digits.length >= 9 && digits.length <= 13) {
    if (digits.startsWith("972") && digits.length === 12) return "+" + digits;
    if (digits.startsWith("0") && digits.length === 10) return "+972" + digits.slice(1);
    return digits;
  }
  return "";
}

function createPassiveCallContext({ callSid, streamSid, caller, called, source }) {
  const callerInfo = normalizeCallerId(caller);

  return {
    callSid: callSid || "",
    streamSid: streamSid || "",
    source: source || "VoiceBot_Blank",
    caller_raw: callerInfo.value,
    caller_withheld: callerInfo.withheld,
    called: called || "",
    started_at: nowIso(),
    ended_at: null,

    // Lead fields
    name: "",
    callback_number: callerInfo.withheld ? "" : callerInfo.value,
    has_request: false,

    // Conversation tracking
    transcript: [], // {role,text,normalized,lang,ts}
  };
}

function appendUtterance(ctx, u) {
  if (!ctx) return;

  const role = u?.role || "";
  const text = String(u?.text || "");
  const normalized = u?.normalized;
  const lang = u?.lang;

  ctx.transcript.push({
    role,
    text,
    normalized,
    lang,
    ts: nowIso(),
  });

  if (role !== "user") return;

  const effective = String(normalized || text).trim();
  if (!effective) return;

  // 1) Name capture (first time only)
  if (!ctx.name) {
    const n = extractNameHe(effective);
    if (n) ctx.name = n;
    return;
  }

  // 2) After we have a name: mark request present if user said something meaningful
  if (effective.length >= 6) ctx.has_request = true;

  // 3) Callback number if withheld and not captured yet
  if (ctx.caller_withheld && !ctx.callback_number) {
    const p = extractPhone(effective);
    if (p) ctx.callback_number = p;
  }
}

function finalizeCtx(ctx) {
  if (!ctx) return null;
  ctx.ended_at = nowIso();
  return ctx;
}

module.exports = {
  createPassiveCallContext,
  appendUtterance,
  finalizeCtx,
};
