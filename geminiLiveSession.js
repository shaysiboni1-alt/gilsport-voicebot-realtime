"use strict";

const { parseLeadPostcall } = require("./postcallLeadParser");
const { upsertCallerProfile } = require("../memory/callerMemory");
const { detectIntent } = require("../logic/intentRouter");
const { downloadRecording } = require("../utils/twilioRecordings");
const { sanitizeCandidate } = require("../logic/nameExtractor");

const NON_LEAD_INTENT_IDS = new Set([
  "other",
  "meta_voice_question",
  "caller_correction",
  "ask_contact_info",
]);

const NON_LEAD_INTENT_TYPES = new Set(["info", "other"]);
const INFO_ONLY_INTENT_IDS = new Set(["ask_contact_info", "meta_voice_question"]);

const SUBJECT_STOPWORDS_HE = new Set([
  "כן",
  "לא",
  "אוקיי",
  "אוקי",
  "טוב",
  "בסדר",
  "הבנתי",
  "שלום",
  "הלו",
  "רגע",
  "תודה",
  "אישה",
  "בת",
  "אני",
]);

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

function appearsInConversation(num, convLog) {
  const digits = (num || "").replace(/\D/g, "");
  if (!digits) return false;
  return (Array.isArray(convLog) ? convLog : []).some(
    ({ text }) => text && String(text).replace(/\D/g, "").includes(digits)
  );
}

function cleanText(v) {
  return String(v || "")
    .replace(/[\u200e\u200f\u202a-\u202e]/g, "")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function normalizePhone(v) {
  const s = cleanText(v);
  if (!s) return null;
  const plus = s.startsWith("+") ? "+" : "";
  const digits = s.replace(/\D/g, "");
  if (!digits || digits.length < 8 || digits.length > 16) return null;
  return `${plus}${digits}`;
}

function isPlausibleFullName(name) {
  const cleaned = cleanText(name);
  if (!cleaned) return false;
  const sanitized = sanitizeCandidate(cleaned);
  if (!sanitized) return false;
  return sanitized === cleaned;
}

function isWeakSubjectValue(subject) {
  const s = cleanText(subject);
  if (!s) return true;
  if (s.length < 3 || s.length > 120) return true;

  const words = s.split(/\s+/).filter(Boolean);
  if (!words.length || words.length > 12) return true;

  if (words.length === 1 && SUBJECT_STOPWORDS_HE.has(words[0])) return true;
  if (/^[\p{P}\p{S}\s]+$/u.test(s)) return true;
  if (/^(כן|לא|אוקיי|אוקי|טוב|בסדר|תודה|שלום|הלו|רגע)$/u.test(s)) return true;
  if (/^(אני\s+(אישה|בת|צריך|צריכה|רוצה)|זה\s+אני)$/u.test(s)) return true;
  if (/(מבטא|קול|איך את נשמעת|למה את מדברת ככה|מי את|את בוט|את רובוט)/u.test(s)) return true;

  return false;
}

function getIntentDef(lead, ssot) {
  const intentId = safeStr(lead?.intent);
  if (!intentId) return null;
  const intents = Array.isArray(ssot?.intents) ? ssot.intents : [];
  return intents.find((it) => safeStr(it?.intent_id) === intentId) || null;
}

function isLeadIntent(lead, ssot) {
  const intentId = safeStr(lead?.intent);
  if (!intentId) return false;
  if (NON_LEAD_INTENT_IDS.has(intentId)) return false;

  const def = getIntentDef(lead, ssot);
  const type = safeStr(def?.intent_type);
  if (type && NON_LEAD_INTENT_TYPES.has(type)) return false;

  return true;
}

function isInfoOnlyIntent(lead, ssot) {
  const intentId = safeStr(lead?.intent);
  if (INFO_ONLY_INTENT_IDS.has(intentId)) return true;

  const def = getIntentDef(lead, ssot);
  return safeStr(def?.intent_type) === "info";
}

function collapseHebrewLetterSpacing(text) {
  let s = String(text || "");
  for (let i = 0; i < 4; i += 1) {
    s = s.replace(/([\u0590-\u05FF])\s+(?=[\u0590-\u05FF])/g, "$1");
  }
  return s.replace(/\s{2,}/g, " ").trim();
}

function deriveExplicitNameFromConversationLog(conversationLog) {
  const rows = Array.isArray(conversationLog) ? conversationLog : [];
  const userRows = rows.filter(
    (r) => String(r?.role || "").toLowerCase() === "user"
  );

  for (const row of userRows) {
    const raw = cleanText(row?.text || "");
    if (!raw) continue;

    const normalized = collapseHebrewLetterSpacing(raw);

    const patterns = [
      /(?:^|\b)קוראים\s+לי\s+(.+)$/u,
      /(?:^|\b)שמי\s+(.+)$/u,
      /(?:^|\b)השם\s+שלי\s+(.+)$/u,
    ];

    for (const re of patterns) {
      const m = normalized.match(re);
      if (!m || !m[1]) continue;

      const cand = sanitizeCandidate(cleanText(m[1]));
      if (cand) return cand;
    }
  }

  return null;
}

function buildConversationSignals(conversationLog, call) {
  const rows = Array.isArray(conversationLog) ? conversationLog : [];
  const userTexts = rows
    .filter((r) => String(r?.role || "").toLowerCase() === "user")
    .map((r) => cleanText(r?.text))
    .filter(Boolean);

  const joined = collapseHebrewLetterSpacing(userTexts.join(" \n "));

  const hasLeadKeywords =
    /(דוח|דו"ח|אישור|מסמך|מסמכים|פגישה|פנייה|להשאיר הודעה|מרגריטה|ריטה|החזר מס|מס הכנסה|רווח והפסד|מאזן|טופס|שיחזרו|לחזור אליי|תחזרו אליי|בקשת חזרה)/u.test(
      joined
    );

  return {
    userTexts,
    joined,
    callbackRequested:
      /לחזור\s+אליי|תחזרו\s+אליי|שיחזרו\s+אליי|בקשת\s+חזרה|call me back|callback/i.test(
        joined
      ),
    explicitPhoneProvided: userTexts.some((t) =>
      /\d{7,}/.test(String(t).replace(/\D/g, ""))
    ),
    explicitCallbackConfirmation:
      /\b(כן|נכון|אוקיי|אוקי|בסדר|בטח|יאללה|sure|yes|correct)\b/i.test(joined),
    looksLikeMetaOnly:
      /(מבטא|קול|מי את|את בוט|את רובוט)/u.test(joined) &&
      !hasLeadKeywords,
    hasUsableCallerId: !!safeStr(call?.caller) && !call?.caller_withheld,
    hasLeadKeywords,
  };
}

function normalizeLead(
  parsed,
  call,
  knownFullName,
  knownPhone,
  conversationLog,
  ssot
) {
  const out = {
    intent: null,
    full_name: null,
    callback_to_number: null,
    subject: null,
    notes: null,
    brand: null,
    model: null,
    _name_source: null,
    _phone_source: null,
    _subject_source: null,
  };

  if (parsed && typeof parsed === "object") {
    for (const k of [
      "intent",
      "full_name",
      "callback_to_number",
      "subject",
      "notes",
      "brand",
      "model",
    ]) {
      const v = parsed[k];
      if (v && typeof v === "string") out[k] = v.trim() || null;
    }
  }

  if (isPlausibleFullName(out.full_name)) {
    out._name_source = "parsed";
  } else {
    out.full_name = null;
  }

  if (!out.full_name) {
    const candidateKnown = knownFullName ? safeStr(knownFullName) : null;
    if (candidateKnown && isPlausibleFullName(candidateKnown)) {
      out.full_name = candidateKnown;
      out._name_source = "memory";
    }
  }

  if (!out.full_name) {
    const derived = deriveExplicitNameFromConversationLog(conversationLog);
    if (derived && isPlausibleFullName(derived)) {
      out.full_name = derived;
      out._name_source = "conversation_explicit";
    }
  }

  const parsedPhone = normalizePhone(out.callback_to_number);
  if (parsedPhone) {
    out.callback_to_number = parsedPhone;
    out._phone_source = appearsInConversation(parsedPhone, conversationLog)
      ? "explicit"
      : "parsed";
  } else {
    out.callback_to_number = null;
  }

  if (!out.callback_to_number) {
    const candidateKnownPhone = normalizePhone(knownPhone);
    if (candidateKnownPhone) {
      out.callback_to_number = candidateKnownPhone;
      out._phone_source = appearsInConversation(candidateKnownPhone, conversationLog)
        ? "explicit"
        : "memory";
    } else if (safeStr(call?.caller) && !call?.caller_withheld) {
      out.callback_to_number = normalizePhone(call.caller);
      out._phone_source = "caller_id";
    }
  }

  const parsedSubject = safeStr(out.subject);
  if (parsedSubject && !isWeakSubjectValue(parsedSubject)) {
    out.subject = cleanText(parsedSubject);
    out._subject_source = "parsed";
  } else {
    out.subject = null;
  }

  if (!out.intent) {
    try {
      const transcript = buildTranscript(conversationLog);
      const fallback = detectIntent({
        text: transcript,
        intents: ssot?.intents || [],
        opts: { forceLang: safeStr(call?.language_locked) || undefined },
      });
      out.intent = fallback?.intent_id || null;
    } catch {}
  }

  return out;
}

function decideEvent(lead, ssot, call, signals) {
  const hasStrongName = isPlausibleFullName(lead?.full_name);
  const hasStrongSubject = !isWeakSubjectValue(lead?.subject || "");
  const parsedLeadIntent = isLeadIntent(lead, ssot);
  const infoOnlyIntent = isInfoOnlyIntent(lead, ssot);

  const hasCallbackPath =
    !!lead?.callback_to_number &&
    (lead?._phone_source === "explicit" ||
      lead?._phone_source === "memory" ||
      lead?._phone_source === "caller_id" ||
      lead?._phone_source === "parsed");

  const hasBusinessLeadEvidence =
    hasStrongSubject ||
    signals.callbackRequested ||
    signals.hasLeadKeywords;

  if (signals.looksLikeMetaOnly && !hasBusinessLeadEvidence) {
    return { event: "ABANDONED", decision_reason: "meta_only" };
  }

  if (infoOnlyIntent && !hasBusinessLeadEvidence) {
    return { event: "NO_LEAD", decision_reason: "info_only" };
  }

  if (!hasStrongSubject && !signals.callbackRequested && !signals.hasLeadKeywords) {
    return { event: "NO_LEAD", decision_reason: "non_lead_intent" };
  }

  if (call?.caller_withheld) {
    if (!lead?.callback_to_number || lead?._phone_source !== "explicit") {
      return {
        event: "ABANDONED",
        decision_reason: "withheld_without_explicit_phone",
      };
    }
  } else if (!hasCallbackPath) {
    return { event: "ABANDONED", decision_reason: "no_callback_number" };
  }

  if (!hasStrongName) {
    return { event: "ABANDONED", decision_reason: "no_reliable_name" };
  }

  if (hasStrongSubject && hasStrongName && hasCallbackPath) {
    return {
      event: "FINAL",
      decision_reason: parsedLeadIntent
        ? "confirmed_lead"
        : "lead_by_business_evidence",
    };
  }

  if (hasBusinessLeadEvidence) {
    return { event: "ABANDONED", decision_reason: "partial_lead" };
  }

  return { event: "NO_LEAD", decision_reason: "non_lead_intent" };
}

async function finalizePipeline({ snapshot, ssot, env, logger, senders }) {
  const log = logger || console;

  try {
    const call = {
      callSid: snapshot?.call?.callSid || snapshot?.callSid || null,
      streamSid: snapshot?.call?.streamSid || snapshot?.streamSid || null,
      caller: snapshot?.call?.caller || snapshot?.caller || null,
      caller_withheld: !!(
        snapshot?.call?.caller_withheld ?? snapshot?.caller_withheld
      ),
      called: snapshot?.call?.called || snapshot?.called || null,
      source: snapshot?.call?.source || snapshot?.source || "VoiceBot_Blank",
      started_at: snapshot?.call?.started_at || snapshot?.started_at || null,
      ended_at: snapshot?.call?.ended_at || snapshot?.ended_at || null,
      duration_ms:
        snapshot?.call?.duration_ms ?? snapshot?.duration_ms ?? null,
      duration_sec:
        snapshot?.call?.duration_sec ??
        secondsFromMs(snapshot?.call?.duration_ms ?? snapshot?.duration_ms),
      finalize_reason:
        snapshot?.call?.finalize_reason || snapshot?.finalize_reason || null,
      passive_context: snapshot?.call?.passive_context || null,
      language_locked: snapshot?.call?.language_locked || null,
    };

    const conversationLog = Array.isArray(snapshot?.conversationLog)
      ? snapshot.conversationLog
      : Array.isArray(snapshot?.call?.conversationLog)
        ? snapshot.call.conversationLog
        : [];

    let recording = {
      recording_provider: null,
      recording_sid: null,
      recording_url_public: null,
    };

    try {
      if (
        env.MB_ENABLE_RECORDING &&
        typeof senders?.resolveRecording === "function"
      ) {
        recording = await senders.resolveRecording();
      }
    } catch (e) {
      log.warn("Resolve recording failed", { error: e?.message || String(e) });
    }

    if (recording?.recording_sid) {
      try {
        const downloaded = await downloadRecording(recording.recording_sid, log);
        if (downloaded?.publicUrl) {
          recording.recording_url_public = downloaded.publicUrl;
        }
      } catch (e) {
        log.warn("Local download of recording failed", {
          error: e?.message || String(e),
        });
      }
    }

    const transcript = buildTranscript(conversationLog);
    const knownFullName =
      safeStr(call?.passive_context?.name) ||
      safeStr(call?.passive_context?.returning_name);
    const knownPhone = safeStr(call?.passive_context?.callback_number);

    let parsed = null;
    if (isTrue(env.LEAD_PARSER_ENABLED) || env.LEAD_PARSER_ENABLED) {
      try {
        parsed = await parseLeadPostcall({
          transcriptText: transcript,
          turns: conversationLog,
          ssot,
          known: {
            full_name: knownFullName,
            callback_to_number: knownPhone,
          },
        });
      } catch (e) {
        log.warn("Postcall lead parsing failed", {
          error: e?.message || String(e),
        });
      }
    }

    const parsedLead = normalizeLead(
      parsed || {},
      call,
      knownFullName,
      knownPhone,
      conversationLog,
      ssot
    );

    const signals = buildConversationSignals(conversationLog, call);
    const { event, decision_reason } = decideEvent(
      parsedLead,
      ssot,
      call,
      signals
    );

    if (env.MB_LOG_FINALIZE_DECISIONS) {
      log.info("FINALIZE_DECISION", {
        callSid: call.callSid,
        language_locked: call.language_locked,
        known_full_name: knownFullName,
        known_phone: knownPhone,
        parsedLead,
        decision_reason,
        event,
      });
    }

    const call_status =
      event === "FINAL"
        ? "completed"
        : event === "ABANDONED"
          ? "abandoned"
          : "no_lead";

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

    try {
      if (
        isTrue(env.CALL_LOG_AT_END) &&
        env.CALL_LOG_WEBHOOK_URL &&
        typeof senders?.sendCallLog === "function"
      ) {
        await senders.sendCallLog({ ...payloadBase, label: "CALL_LOG" });
      }
    } catch (e) {
      log.warn("CALL_LOG webhook failed", { error: e?.message || String(e) });
    }

    try {
      if (event === "FINAL") {
        if (env.FINAL_WEBHOOK_URL && typeof senders?.sendFinal === "function") {
          await senders.sendFinal({ ...payloadBase });
        }
      } else if (event === "ABANDONED") {
        if (
          env.ABANDONED_WEBHOOK_URL &&
          typeof senders?.sendAbandoned === "function"
        ) {
          await senders.sendAbandoned({ ...payloadBase });
        }
      }
    } catch (e) {
      log.warn("Lead webhook failed", { error: e?.message || String(e) });
    }

    try {
      const displayName =
        safeStr(parsedLead.full_name) ||
        (knownFullName && isPlausibleFullName(knownFullName)
          ? knownFullName
          : null) ||
        null;

      await upsertCallerProfile({
        caller: call?.caller,
        full_name: displayName,
        last_subject: safeStr(parsedLead.subject),
        last_notes: safeStr(parsedLead.notes),
        callSid: call?.callSid,
      });
    } catch (e) {
      log.debug("Caller memory update failed", {
        message: e?.message || String(e),
        code: e?.code,
        detail: e?.detail,
        hint: e?.hint,
        where: e?.where,
      });
    }

    return { status: "ok", event };
  } catch (e) {
    log.warn("finalizePipeline error", { error: e?.message || String(e) });
    return { status: "error", event: "ERROR" };
  }
}

module.exports = { finalizePipeline };
