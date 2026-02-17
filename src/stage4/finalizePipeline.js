"use strict";

/*
  Finalize Pipeline for VoiceBot_Blank

  Resolves recording metadata, parses the conversation post‑call, normalizes and
  validates lead fields (including name and phone number), decides if the lead is
  FINAL or ABANDONED, sends webhooks, and updates the caller memory.
*/

const { parseLeadPostcall } = require("./postcallLeadParser");
const { upsertCallerProfile } = require("../memory/callerMemory");
const { detectIntent } = require("../logic/intentRouter");
const { downloadRecording } = require("../utils/twilioRecordings");

// Helpers
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

// Build a transcript for the LLM
function buildTranscript(conversationLog) {
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

// Derive a fallback name from conversation if the LLM fails
function deriveDisplayNameFromConversationLog(conversationLog) {
  const rows = Array.isArray(conversationLog) ? conversationLog : [];
  for (const r of rows) {
    if (String(r?.role || "").toLowerCase() !== "user") continue;
    let t = String(r?.text || "").trim();
    if (!t) continue;
    t = t.replace(/[\u200e\u200f\u202a-\u202e]/g, "").trim();
    t = t.replace(/^[\p{P}\p{S}\s]+|[\p{P}\p{S}\s]+$/gu, "").trim();
    if (!t) continue;
    if (/[0-9]/.test(t)) continue;
    if (t.length > 24) continue;
    const words = t.split(/\s+/).filter(Boolean);
    if (words.length === 0 || words.length > 3) continue;
    if (t.length < 2) continue;
    if (!/\p{L}/u.test(t)) continue;
    return words[0];
  }
  return null;
}

// Normalize lead fields and apply fallbacks
function normalizeLead(parsed, call, knownFullName, knownPhone, conversationLog, ssot) {
  const out = {
    intent: null,
    full_name: null,
    callback_to_number: null,
    subject: null,
    notes: null,
    brand: null,
    model: null,
  };
  if (parsed && typeof parsed === "object") {
    for (const k of Object.keys(out)) {
      const v = parsed[k];
      if (v && typeof v === "string") {
        const s = v.trim();
        out[k] = s || null;
      }
    }
  }
  // Fill missing name from known or heuristic; only accept Hebrew/Latin names
  if (!out.full_name) {
    let candidate = knownFullName ? safeStr(knownFullName) : null;
    if (!candidate) {
      const derived = deriveDisplayNameFromConversationLog(conversationLog);
      candidate = derived ? safeStr(derived) : null;
    }
    if (candidate && /^[\p{Script=Hebrew}\p{Script=Latin}\s]{2,40}$/u.test(candidate)) {
      out.full_name = candidate;
    }
  }
  // Fill missing phone from known or caller ID (if not withheld)
  if (!out.callback_to_number) {
    if (knownPhone) {
      out.callback_to_number = safeStr(knownPhone);
    } else {
      const caller = safeStr(call?.caller);
      const withheld = !!call?.caller_withheld;
      if (caller && !withheld) out.callback_to_number = caller;
    }
  }
  // Fallback intent detection if missing
  if (!out.intent) {
    try {
      const transcript = buildTranscript(conversationLog);
      const fallback = detectIntent({ text: transcript, intents: ssot?.intents || [] });
      out.intent = fallback?.intent_id || null;
    } catch { /* ignore */ }
  }
  return out;
}

// Derive a fallback subject from the last user utterance
function deriveSubjectFromConversationLog(conversationLog) {
  const rows = Array.isArray(conversationLog) ? conversationLog : [];
  const userUtterances = rows
    .filter((r) => String(r?.role || "").toLowerCase() === "user")
    .map((r) => String(r?.text || "").trim())
    .filter(Boolean);
  if (!userUtterances.length) return null;
  const last = userUtterances[userUtterances.length - 1];
  return last.length > 180 ? last.slice(0, 180) : last;
}

// Validate that a callback number appears in conversation
function appearsInConversation(num, convLog) {
  const digits = (num || "").replace(/\D/g, "");
  return convLog.some(({ text }) => text && text.replace(/\D/g, "").includes(digits));
}

// Decide FINAL vs ABANDONED
function decideEvent(lead) {
  const nameVal = safeStr(lead?.full_name);
  const phoneVal = safeStr(lead?.callback_to_number);
  const subjVal = safeStr(lead?.subject);
  const hasName =
    nameVal &&
    nameVal.length >= 2 &&
    /^[\p{Script=Hebrew}\p{Script=Latin}\s]+$/u.test(nameVal);
  const hasPhone = !!phoneVal;
  const hasSubject = !!subjVal;
  if (hasName && hasPhone && hasSubject) {
    return { event: "FINAL", decision_reason: "ok" };
  }
  if (!hasName && hasPhone && hasSubject) {
    return { event: "ABANDONED", decision_reason: "no_name" };
  }
  if (!hasPhone && hasSubject) {
    return { event: "ABANDONED", decision_reason: "no_phone" };
  }
  if (hasPhone && !hasSubject) {
    return { event: "ABANDONED", decision_reason: "no_subject" };
  }
  return { event: "ABANDONED", decision_reason: "partial" };
}

async function finalizePipeline({ snapshot, ssot, env, logger, senders }) {
  const log = logger || console;
  try {
    // Build call metadata
    const call = {
      callSid: snapshot?.call?.callSid || snapshot?.callSid || null,
      streamSid: snapshot?.call?.streamSid || snapshot?.streamSid || null,
      caller: snapshot?.call?.caller || snapshot?.caller || null,
      caller_withheld: !!(snapshot?.call?.caller_withheld ?? snapshot?.caller_withheld),
      called: snapshot?.call?.called || snapshot?.called || null,
      source: snapshot?.call?.source || snapshot?.source || "VoiceBot_Blank",
      started_at: snapshot?.call?.started_at || snapshot?.started_at || null,
      ended_at: snapshot?.call?.ended_at || snapshot?.ended_at || null,
      duration_ms:
        snapshot?.call?.duration_ms ?? snapshot?.duration_ms ?? null,
      duration_sec:
        snapshot?.call?.duration_sec ??
        secondsFromMs(snapshot?.call?.duration_ms ?? snapshot?.duration_ms),
      finalize_reason: snapshot?.call?.finalize_reason || snapshot?.finalize_reason || null,
      passive_context: snapshot?.call?.passive_context || null,
    };
    // Extract conversation log
    const conversationLog = Array.isArray(snapshot?.conversationLog)
      ? snapshot.conversationLog
      : Array.isArray(snapshot?.call?.conversationLog)
        ? snapshot.call.conversationLog
        : [];
    // Resolve recording
    let recording = {
      recording_provider: null,
      recording_sid: null,
      recording_url_public: null,
    };
    try {
      if (env.MB_ENABLE_RECORDING && typeof senders?.resolveRecording === "function") {
        recording = await senders.resolveRecording();
      }
    } catch (e) {
      log.warn?.("Resolve recording failed", e?.message || e);
    }
    // Download recording
    if (recording?.recording_sid) {
      try {
        const downloaded = await downloadRecording(recording.recording_sid, log);
        if (downloaded?.publicUrl) {
          recording.recording_url_public = downloaded.publicUrl;
        }
      } catch (e) {
        log.warn?.("Local download of recording failed", e?.message || e);
      }
    }
    // Build transcript and parse lead
    const transcript = buildTranscript(conversationLog);
    const knownFullName = safeStr(call?.passive_context?.name);
    const knownPhone = safeStr(call?.passive_context?.callback_number);
    let parsed = null;
    if (isTrue(env.LEAD_PARSER_ENABLED) || env.LEAD_PARSER_ENABLED) {
      try {
        parsed = await parseLeadPostcall({
          transcriptText: transcript,
          turns: conversationLog,
          ssot,
          known: { full_name: knownFullName, callback_to_number: knownPhone },
        });
      } catch (e) {
        log.warn?.("Postcall lead parsing failed", e?.message || e);
      }
    }
    // Normalize and apply fallbacks
    const parsedLead = normalizeLead(parsed || {}, call, knownFullName, knownPhone, conversationLog, ssot);
    // Derive subject if missing
    if (!parsedLead.subject) {
      const derived = deriveSubjectFromConversationLog(conversationLog);
      if (derived) parsedLead.subject = derived;
    }
    // Validate callback number: ensure it appears in conversation; otherwise fallback
    if (
      parsedLead.callback_to_number &&
      parsedLead.callback_to_number !== call.caller &&
      !appearsInConversation(parsedLead.callback_to_number, conversationLog)
    ) {
      parsedLead.callback_to_number = call.caller;
    }
    // Decide event
    const { event, decision_reason } = decideEvent(parsedLead);
    const call_status = event === "FINAL" ? "completed" : "abandoned";
    // Build payload
    const payloadBase = {
      call,
      call_status,
      status: call_status,
      event,
      decision_reason,
      intent: safeStr(parsedLead.intent),
      recording_provider: safeStr(recording?.recording_provider),
      recording_sid: safeStr(recording?.recording_sid),
      recording_url_public: safeStr(recording?.recording_url_public),
      conversationLog,
      parsedLeadCollection: {
        intent: safeStr(parsedLead.intent),
        full_name: safeStr(parsedLead.full_name),
        callback_to_number: safeStr(parsedLead.callback_to_number),
        subject: safeStr(parsedLead.subject),
        notes: safeStr(parsedLead.notes),
        brand: safeStr(parsedLead.brand),
        model: safeStr(parsedLead.model),
        isFullLead: event === "FINAL",
      },
    };
    // Send CALL_LOG
    try {
      if (isTrue(env.CALL_LOG_AT_END)) {
        if (env.CALL_LOG_WEBHOOK_URL && typeof senders?.sendCallLog === "function") {
          await senders.sendCallLog({ ...payloadBase, label: "CALL_LOG" });
        }
      }
    } catch (e) {
      log.warn?.("CALL_LOG webhook failed", e?.message || e);
    }
    // Send FINAL/ABANDONED
    try {
      if (event === "FINAL") {
        if (env.FINAL_WEBHOOK_URL && typeof senders?.sendFinal === "function") {
          await senders.sendFinal({ ...payloadBase });
        }
      } else {
        if (env.ABANDONED_WEBHOOK_URL && typeof senders?.sendAbandoned === "function") {
          await senders.sendAbandoned({ ...payloadBase });
        }
      }
    } catch (e) {
      log.warn?.("Lead webhook failed", e?.message || e);
    }
    // Update caller memory
    try {
      const displayName =
        safeStr(parsedLead.full_name) ||
        deriveDisplayNameFromConversationLog(conversationLog) ||
        null;
      await upsertCallerProfile({
        caller: call?.caller,
        full_name: displayName,
        last_subject: safeStr(parsedLead.subject),
        last_notes: safeStr(parsedLead.notes),
        callSid: call?.callSid,
      });
    } catch (e) {
      log.debug?.("Caller memory update failed", {
        message: e?.message || String(e),
        code: e?.code,
        detail: e?.detail,
        hint: e?.hint,
        where: e?.where,
      });
    }
    return { status: "ok", event };
  } catch (e) {
    log.warn?.("finalizePipeline error", e?.message || e);
    return { status: "error", event: "ERROR" };
  }
}

module.exports = { finalizePipeline };
