// src/stage4/finalizePipeline.js
"use strict";

/*
  FINALIZE PIPELINE (GilSport-style, adapted to VoiceBot_Blank)

  Non-negotiables:
  - Must not affect audio (runs post-call)
  - Always send CALL_LOG (if enabled via ENV)
  - Send FINAL xor ABANDONED
  - Lead parsing is post-call via SSOT LEAD_PARSER_PROMPT (JSON-only)
  - Attach recording_url_public/recording_sid/recording_provider when available (best-effort)

  Decision rule (locked):
  - FINAL: full_name + subject + callback_to_number
  - ABANDONED: callback_to_number only
*/

const { parseLeadPostcall } = require("./postcallLeadParser");

function isTrue(v) {
  return v === true || String(v).toLowerCase() === "true";
}

function safeStr(v) {
  const s = typeof v === "string" ? v.trim() : "";
  return s || null;
}

function secondsFromMs(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n < 0) return null;
  return Math.round(n / 1000);
}

function buildTranscriptFromConversationLog(conversationLog) {
  const rows = Array.isArray(conversationLog) ? conversationLog : [];
  return rows
    .map((r) => {
      const role = String(r?.role || "").toUpperCase();
      const text = String(r?.text || "").trim();
      if (!text) return "";
      return `${role}: ${text}`;
    })
    .filter(Boolean)
    .join("\n");
}

function normalizeParsedLead(parsed, call) {
  const lead = {
    full_name: safeStr(parsed?.full_name),
    subject: safeStr(parsed?.subject),
    callback_to_number: safeStr(parsed?.callback_to_number),
    notes: safeStr(parsed?.notes),
  };

  // GilSport parity: if caller ID exists and is not withheld, allow it to be the callback number.
  if (!lead.callback_to_number) {
    const caller = safeStr(call?.caller);
    const withheld = !!call?.caller_withheld;
    if (caller && !withheld) lead.callback_to_number = caller;
  }

  return lead;
}

function decideEvent(lead) {
  const hasPhone = !!safeStr(lead?.callback_to_number);
  const hasName = !!safeStr(lead?.full_name);
  const hasSubject = !!safeStr(lead?.subject);

  if (hasPhone && hasName && hasSubject) return { event: "FINAL", decision_reason: "ok" };
  if (hasPhone) return { event: "ABANDONED", decision_reason: "phone_only" };
  return { event: "ABANDONED", decision_reason: "missing_phone" };
}

function uniq(arr) {
  return Array.from(new Set(arr.filter(Boolean)));
}

function enhanceSubjectDeterministic({ subject, conversationLog }) {
  const base = safeStr(subject);
  const rows = Array.isArray(conversationLog) ? conversationLog : [];
  const userText = rows
    .filter((r) => String(r?.role || "") === "user")
    .map((r) => String(r?.text || "").trim())
    .filter(Boolean)
    .join(" \n");

  if (!userText) return base;

  const t = userText.replace(/\s+/g, " ").trim();
  const tl = t.toLowerCase();

  const flags = {
    reports: /(\bדוח|\bדוחות|דו"?חות)/.test(t),
    vat: /(מע"?מ|מעמ)/.test(t),
    incomeTax: /(מס\s*הכנסה)/.test(t),
    urgent: /(דחוף|בהקדם)/.test(t),
  };

  // Years like 2024
  const years = uniq((t.match(/\b(19|20)\d{2}\b/g) || []));

  // Period fragments / ranges (keep as-is; do NOT guess)
  const periodFragments = uniq((t.match(/\b\d{1,2}\s*-\s*\d{1,2}\b/g) || []).map((s) => s.replace(/\s+/g, "")));
  const weirdFragments = uniq((t.match(/\b\d{2}\s*-\s*\d\b/g) || []).map((s) => s.replace(/\s+/g, "")));
  const singleDigits = uniq((t.match(/\b\d\b/g) || []));

  const parts = [];

  // Build a richer subject (still concise) from what was actually said.
  if (flags.reports) {
    const taxParts = [];
    if (flags.vat) taxParts.push('מע"מ');
    if (flags.incomeTax) taxParts.push("מס הכנסה");
    const taxStr = taxParts.length ? ` (${taxParts.join(" + ")})` : "";
    parts.push(`דוחות${taxStr}`);
  }

  if (years.length) parts.push(`שנה/ים: ${years.join(", ")}`);

  // If we have explicit ranges, prefer those. If we only have fragments like 20-2 + 5, keep them verbatim.
  const allPeriods = uniq([...periodFragments, ...weirdFragments]);
  if (allPeriods.length) {
    parts.push(`תקופות: ${allPeriods.join(", ")}`);
  } else {
    // Only add single digits if there's context around "תקופות" to avoid noise.
    if (tl.includes("תקופ") && singleDigits.length) {
      parts.push(`תקופות (כפי שנאמר): ${singleDigits.join(", ")}`);
    }
  }

  if (flags.urgent) parts.push("דחוף");

  // If we built nothing new, keep as-is.
  if (!parts.length) return base;

  const enriched = parts.join(" – ");

  // Merge with existing subject if it contains additional useful info.
  if (base) {
    const baseNorm = base.toLowerCase();
    // If base already includes most of what we built, keep base.
    const overlap = parts.every((p) => baseNorm.includes(p.toLowerCase().split(" ")[0]));
    if (overlap && base.length >= enriched.length) return base;
    // Otherwise: combine without duplicating.
    if (baseNorm.includes("דוח") || baseNorm.includes("דוחות")) {
      return `${base} – ${enriched}`.slice(0, 180);
    }
    return `${enriched} – ${base}`.slice(0, 180);
  }

  return enriched.slice(0, 180);
}

async function finalizePipeline({ snapshot, ssot, env, logger, senders }) {
  const log = logger || console;

  const call = {
    callSid: snapshot?.call?.callSid || snapshot?.callSid || null,
    streamSid: snapshot?.call?.streamSid || snapshot?.streamSid || null,
    caller: snapshot?.call?.caller || snapshot?.caller || null,
    caller_withheld: !!(snapshot?.call?.caller_withheld ?? snapshot?.caller_withheld),
    called: snapshot?.call?.called || snapshot?.called || null,
    source: snapshot?.call?.source || snapshot?.source || "VoiceBot_Blank",
    started_at: snapshot?.call?.started_at || snapshot?.started_at || null,
    ended_at: snapshot?.call?.ended_at || snapshot?.ended_at || null,
    duration_ms: snapshot?.call?.duration_ms ?? snapshot?.duration_ms ?? null,
    duration_sec:
      snapshot?.call?.duration_sec ??
      secondsFromMs(snapshot?.call?.duration_ms ?? snapshot?.duration_ms),
    finalize_reason: snapshot?.call?.finalize_reason || snapshot?.finalize_reason || null,
    passive_context: snapshot?.call?.passive_context || null,
  };

  const conversationLog = Array.isArray(snapshot?.conversationLog)
    ? snapshot.conversationLog
    : Array.isArray(snapshot?.call?.conversationLog)
      ? snapshot.call.conversationLog
      : [];

  // 1) Resolve recording (best-effort) BEFORE webhooks so all events can include it.
  let recording = {
    recording_provider: null,
    recording_sid: null,
    recording_url_public: null,
  };
  try {
    if (env.MB_ENABLE_RECORDING && typeof senders?.resolveRecording === "function") {
      recording = await senders.resolveRecording(call.callSid);
    }
  } catch (e) {
    log.warn?.("Resolve recording failed", e?.message || e);
  }

  // 2) Post-call lead parsing (SSOT prompt inside parseLeadPostcall)
  let parsedRaw = null;
  try {
    const transcriptText = buildTranscriptFromConversationLog(conversationLog);
    const shouldParse = isTrue(env.LEAD_PARSER_ENABLED) || !!env.LEAD_PARSER_ENABLED;

    if (shouldParse) {
      parsedRaw = await parseLeadPostcall({
        transcriptText,
        ssot,
        known: {
          caller_id_e164: safeStr(call?.caller) || null,
        },
        env,
        logger: log,
      });
    }
  } catch (e) {
    log.warn?.("Lead postcall parsing failed", e?.message || e);
  }

  const parsedLead = normalizeParsedLead(parsedRaw || {}, call);

  // Deterministic enrichment: make subject include key details actually said (without guessing).
  parsedLead.subject = enhanceSubjectDeterministic({
    subject: parsedLead.subject,
    conversationLog,
  });

  // 3) Payload base (shared by all webhooks)
  const payloadBase = {
    call,
    parsedLead,
    conversationLog,
    recording_provider: safeStr(recording?.recording_provider),
    recording_sid: safeStr(recording?.recording_sid),
    recording_url_public: safeStr(recording?.recording_url_public),
  };

  // 4) CALL_LOG (always if enabled)
  try {
    if (env.CALL_LOG_AT_START === "true" && env.CALL_LOG_MODE === "start") {
      // already sent elsewhere
    }
    if (isTrue(env.CALL_LOG_AT_END)) {
      if (env.CALL_LOG_WEBHOOK_URL && typeof senders?.sendCallLog === "function") {
        await senders.sendCallLog({ event: "CALL_LOG", ...payloadBase });
      }
    }
  } catch (e) {
    log.warn?.("CALL_LOG webhook failed", e?.message || e);
  }

  // 5) FINAL xor ABANDONED (deterministic)
  const { event, decision_reason } = decideEvent(parsedLead);
  const finalPayload = {
    event,
    decision_reason,
    ...payloadBase,
  };

  if (event === "FINAL") {
    if (env.FINAL_WEBHOOK_URL && typeof senders?.sendFinal === "function") {
      await senders.sendFinal(finalPayload);
    }
  } else {
    if (env.ABANDONED_WEBHOOK_URL && typeof senders?.sendAbandoned === "function") {
      await senders.sendAbandoned(finalPayload);
    }
  }

  return { status: "ok", event };
}

module.exports = { finalizePipeline };
