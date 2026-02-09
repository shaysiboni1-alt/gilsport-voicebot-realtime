"use strict";

const { logger } = require("../utils/logger");
const {
  detectLang,
  normalizeHebrew,
  normalizeLatin,
  buildHebrewTokenSet,
  splitTriggersCell
} = require("../utils/textNlp");

/**
 * Expected SSOT intents schema (current sheet):
 * [
 *   {
 *     intent_id: "reports_request",
 *     intent_type: "documents",
 *     priority: 90,
 *     triggers_he: "דוחות|דוח|מסמכים|...",
 *     triggers_en: "reports|report|documents|...",
 *     triggers_ru: "...",
 *   }
 * ]
 *
 * Returns:
 * { intent_id, intent_type, score, priority, matched_triggers }
 */
function detectIntent(utteranceText, intents, opts = {}) {
  const textRaw = utteranceText || "";
  const rows = Array.isArray(intents) ? intents : [];

  // If no intents configured, return fallback
  if (!rows.length) {
    return {
      intent_id: "other",
      intent_type: "other",
      score: 0,
      priority: 0,
      matched_triggers: []
    };
  }

  const lang = opts.forceLang || detectLang(textRaw);

  const norm =
    lang === "he" ? normalizeHebrew(textRaw) : normalizeLatin(textRaw);

  const tokenSetHe = lang === "he" ? buildHebrewTokenSet(norm) : null;

  // Scoring:
  // - phrase/includes match gets higher weight
  // - token match gets medium weight
  // - ties broken by priority desc, then intent_id
  let best = null;

  for (const it of rows) {
    const intentId = String(it?.intent_id || "").trim();
    const intentType = String(it?.intent_type || "").trim();
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

    for (const tr0 of triggers) {
      const tr = lang === "he" ? normalizeHebrew(tr0) : normalizeLatin(tr0);
      if (!tr) continue;

      // Phrase match (substring) — works well for multi-word triggers
      if (tr.length >= 2 && norm.includes(tr)) {
        score += tr.length >= 6 ? 6 : 4; // longer phrase => stronger
        matched.push(tr0);
        continue;
      }

      // Keyword-style match
      if (lang === "he") {
        // Use tokenSetHe (includes conservative stemming)
        // If trigger is a single token, check membership
        if (!tr.includes(" ") && tokenSetHe && tokenSetHe.has(tr)) {
          score += 3;
          matched.push(tr0);
          continue;
        }
      } else {
        // Latin/cyrillic token match (split)
        const tokens = tr.split(" ").filter(Boolean);
        for (const tk of tokens) {
          if (tk.length >= 2 && norm.split(" ").includes(tk)) {
            score += 2;
            matched.push(tr0);
            break;
          }
        }
      }
    }

    if (score <= 0) continue;

    const candidate = {
      intent_id: intentId,
      intent_type: intentType || "other",
      score,
      priority,
      matched_triggers: Array.from(new Set(matched)).slice(0, 8)
    };

    if (!best) {
      best = candidate;
      continue;
    }

    // Compare
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
      matched_triggers: []
    };
  }

  // Optional debug hook
  if (opts.logDebug) {
    logger.info("INTENT_DEBUG", { lang, norm, best });
  }

  return best;
}

module.exports = { detectIntent };
