"use strict";

const { logger } = require("../utils/logger");
const { normalizeUtterance } = require("./hebrewNlp");

function splitTriggersCell(value) {
  return String(value || "")
    .split("|")
    .map((s) => s.trim())
    .filter(Boolean);
}

function unique(arr) {
  return Array.from(new Set(arr.filter(Boolean)));
}

function buildVariants(text) {
  const base = normalizeUtterance(text || "");
  const variants = unique([
    base.raw,
    base.normalized,
    base.normalized_for_numbers,
    String(base.normalized || "").replace(/\s+/g, ""),
  ]).map((s) => String(s || "").trim());

  return {
    lang: base.lang,
    normalized: base.normalized,
    variants: variants.filter(Boolean),
  };
}

function scoreTriggerAgainstVariants(trigger, variants) {
  const t = normalizeUtterance(trigger || "").normalized;
  if (!t) return { score: 0, matched: null };

  const compactTrigger = t.replace(/\s+/g, "");

  for (const v of variants) {
    const nv = normalizeUtterance(v).normalized;
    const compact = nv.replace(/\s+/g, "");

    if (nv.includes(t)) {
      return { score: t.length >= 5 ? 8 : 6, matched: trigger };
    }

    if (compact.includes(compactTrigger) && compactTrigger.length >= 3) {
      return { score: 6, matched: trigger };
    }
  }

  return { score: 0, matched: null };
}

function detectIntent(input, maybeIntents, maybeOpts = {}) {
  let textRaw = "";
  let intents = [];
  let opts = maybeOpts || {};

  if (typeof input === "object" && input !== null && !Array.isArray(input)) {
    textRaw = String(input.text || "");
    intents = Array.isArray(input.intents) ? input.intents : [];
    opts = { ...input, text: undefined, intents: undefined };
  } else {
    textRaw = String(input || "");
    intents = Array.isArray(maybeIntents) ? maybeIntents : [];
  }

  if (!intents.length) {
    return {
      intent_id: "other",
      intent_type: "other",
      score: 0,
      priority: 0,
      matched_triggers: [],
    };
  }

  const prepared = buildVariants(textRaw);
  const lang =
    opts.forceLang ||
    prepared.lang ||
    "unknown";

  let best = null;

  for (const it of intents) {
    const intentId = String(it?.intent_id || "").trim();
    const intentType = String(it?.intent_type || "").trim() || "other";
    const priority = Number(it?.priority ?? 0) || 0;
    if (!intentId) continue;

    const triggersCell =
      lang === "he"
        ? it?.triggers_he
        : lang === "ru"
          ? it?.triggers_ru
          : it?.triggers_en;

    const triggers = splitTriggersCell(triggersCell);
    if (!triggers.length) continue;

    let score = 0;
    const matched = [];

    for (const tr of triggers) {
      const res = scoreTriggerAgainstVariants(tr, prepared.variants);
      if (res.score > 0) {
        score += res.score;
        matched.push(res.matched);
      }
    }

    // semantic-ish fallback for common accounting phrases after Hebrew normalization
    const nv = prepared.normalized;
    const compact = nv.replace(/\s+/g, "");

    if (
      intentId === "reports_request" &&
      (
        /דוחות|דוח|מסמכים|רווח והפסד/u.test(nv) ||
        compact.includes("רווחוהפסד")
      )
    ) {
      score += 4;
      matched.push('דו"ח');
    }

    if (
      intentId === "reach_margarita" &&
      (/מרגריטה|ריטה/u.test(nv) || compact.includes("מרגריטה"))
    ) {
      score += 4;
      matched.push("מרגריטה");
    }

    if (
      intentId === "callback_request" &&
      (/לחזור|תחזור|יחזרו|שיחזרו/u.test(nv) || compact.includes("לחזור"))
    ) {
      score += 4;
      matched.push("לחזור");
    }

    if (score <= 0) continue;

    const candidate = {
      intent_id: intentId,
      intent_type: intentType,
      score,
      priority,
      matched_triggers: unique(matched).slice(0, 8),
    };

    if (!best) {
      best = candidate;
      continue;
    }

    if (candidate.score > best.score) best = candidate;
    else if (candidate.score === best.score) {
      if (candidate.priority > best.priority) best = candidate;
      else if (candidate.priority === best.priority) {
        if (candidate.intent_id.localeCompare(best.intent_id) < 0) best = candidate;
      }
    }
  }

  if (!best) {
    return {
      intent_id: "other",
      intent_type: "other",
      score: 0,
      priority: 0,
      matched_triggers: [],
    };
  }

  if (opts.logDebug) {
    logger.info("INTENT_DEBUG", {
      lang,
      normalized: prepared.normalized,
      variants: prepared.variants,
      best,
    });
  }

  return best;
}

module.exports = { detectIntent };
