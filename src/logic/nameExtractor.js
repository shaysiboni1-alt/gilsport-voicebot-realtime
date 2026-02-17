// src/logic/nameExtractor.js
"use strict";

/**
 * Deterministic caller-name extractor.
 * Goal: capture ONLY when high confidence it's the caller's name.
 *
 * Supported: Hebrew / English / Russian (by script).
 * Does NOT guess names.
 */

const HEBREW_RE = /[\u0590-\u05FF]/;
const LATIN_RE = /[A-Za-z]/;
const CYRILLIC_RE = /[\u0400-\u04FF]/;

const STOPWORDS_HE = new Set([
  "כן","לא","אוקיי","אוקי","טוב","בסדר","סבבה","אה","אממ","הממ","רגע","שלום","היי","הלו",
  "מה","מי","אני","קוראים","לי","שמי","זה","כאן","מדבר","מדברת","איתך","איתך"
]);

function isSupportedScript(t) {
  return HEBREW_RE.test(t) || LATIN_RE.test(t) || CYRILLIC_RE.test(t);
}

function stripPunct(s) {
  return String(s || "")
    .replace(/[\u200f\u200e]/g, "")
    .replace(/[“”„״'"]/g, "")
    .replace(/[.,!?;:()\[\]{}<>]/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function sanitizeCandidate(raw) {
  const t = stripPunct(raw);
  if (!t) return null;
  if (/\d/.test(t)) return null;

  // allow 1-2 tokens only (e.g., "שי", "שי סיבוני")
  const parts = t.split(/\s+/).filter(Boolean);
  if (parts.length < 1 || parts.length > 2) return null;

  // length guardrails
  if (t.length < 2 || t.length > 30) return null;

  // stopwords-only rejection
  if (parts.length === 1 && STOPWORDS_HE.has(parts[0])) return null;

  // supported scripts only
  if (!isSupportedScript(t)) return null;

  return parts.join(" ");
}

function lastBotAskedForName(lastBotUtterance) {
  const t = stripPunct(lastBotUtterance || "");
  if (!t) return false;
  // very conservative: only explicit name questions
  return /מה\s*השם|איך\s*קוראים|מי\s*מדבר|מי\s*מדברת|שמך|שמך\s*בבקשה/i.test(t);
}

/**
 * @param {object} params
 * @param {string} params.userText Raw user utterance
 * @param {string|null} params.lastBotUtterance Last assistant utterance (if any)
 * @returns {{name:string, reason:string}|null}
 */
function extractCallerName({ userText, lastBotUtterance }) {
  const raw = String(userText || "").trim();
  if (!raw) return null;

  // explicit self-intro patterns
  const patterns = [
    { re: /\bקוראים\s+לי\s+(.+)$/i, reason: "explicit_korim_li" },
    { re: /\bשמי\s+(.+)$/i, reason: "explicit_shmi" },
    { re: /\bאני\s+(.+)$/i, reason: "explicit_ani" },
    { re: /\bזה\s+(.+)$/i, reason: "explicit_ze" },
  ];

  for (const p of patterns) {
    const m = raw.match(p.re);
    if (m && m[1]) {
      const cand = sanitizeCandidate(m[1]);
      if (cand) return { name: cand, reason: p.reason };
    }
  }

  // direct short answer to a name question (not tied to OPENING; any name question)
  if (lastBotAskedForName(lastBotUtterance)) {
    const cand = sanitizeCandidate(raw);
    if (cand) return { name: cand, reason: "direct_answer_to_name_question" };
  }

  return null;
}

module.exports = {
  extractCallerName,
  lastBotAskedForName,
  sanitizeCandidate,
};
