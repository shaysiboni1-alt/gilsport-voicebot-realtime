// src/logic/nameExtractor.js
"use strict";

/**
 * Deterministic caller-name extractor.
 * Goal: capture ONLY when high confidence it's the caller's name.
 *
 * Supported: Hebrew / English / Russian (by script).
 * Does NOT guess names.
 *
 * Patch:
 * - Harden against false positives like "שמר", "שמר את הכל", "שמור הכל" being captured as a name.
 */

const HEBREW_RE = /[\u0590-\u05FF]/;
const LATIN_RE = /[A-Za-z]/;
const CYRILLIC_RE = /[\u0400-\u04FF]/;

// Hebrew stopwords / fillers / common non-name tokens.
// Keep conservative; reject only when it's clearly not a name.
const STOPWORDS_HE = new Set([
  "כן", "לא", "אוקיי", "אוקי", "טוב", "בסדר", "סבבה", "אה", "אממ", "הממ", "רגע",
  "שלום", "היי", "הלו",
  "מה", "מי", "אני", "קוראים", "לי", "שמי", "זה", "כאן", "מדבר", "מדברת", "איתך",

  // Hardening: imperative/verb-like tokens that frequently appear in calls and are not names
  "שמר", "שמור", "שמרי", "שמרו", "תשמור", "תשמרי", "תשמרו",
  "תבדוק", "בדוק", "תעזור", "עזור", "תשלח", "שלח", "תשלחי", "תשלחו",
  "תקבע", "קבע", "תקבעי", "תקבעו",
  "תעשה", "עשה", "תעשי", "תעשו",
  "תן", "תני", "תנו",

  // Common placeholders / non-names
  "אין", "אנונימי",
]);

// Domain stopwords: common product words that should never be captured as personal names.
// Keep short + high-signal to avoid false rejections.
const PRODUCT_STOPWORDS_HE = new Set([
  "הליכון",
  "הליכונים",
  "אופניים",
  "אופני",
  "ספינינג",
  "אליפטיקל",
  "טריינר",
  "קרוס",
  "מכשיר",
  "מכשירים",
  "משקולות",
  "דמבל",
  "דאמבל",
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

function stripCommonHebrewPrefixForChecks(token) {
  // Strip a single leading Hebrew prefix (ב/ל/מ/כ/ה/ש) ONLY for validation checks.
  // Example: "בהליכון" => "הליכון" so we can block product words captured as names.
  if (!token || token.length < 3) return token;
  const first = token[0];
  if (!"בלמכהש".includes(first)) return token;
  const rest = token.slice(1);
  if (!HEBREW_RE.test(rest)) return token;
  return rest;
}

// Reject obvious command phrases that are not names ("שמר את הכל", "שמור הכל", etc.)
function looksLikeHebrewImperativeSavePhrase(raw) {
  const t = stripPunct(raw);
  if (!t) return false;

  // Start-of-utterance imperative
  if (/^(שמר|שמור|תשמור|שמרי|תשמרי|שמרו|תשמרו)\b/.test(t)) return true;

  // Anywhere "save" + object (very common)
  if (/\b(שמר|שמור|תשמור|שמרי|תשמרי|שמרו|תשמרו)\b\s*(את\b)?\s*(הכל|זה|אותו|אותה|אותם|אותן)\b/.test(t)) {
    return true;
  }

  return false;
}

function sanitizeCandidate(raw) {
  const t = stripPunct(raw);
  if (!t) return null;
  if (/\d/.test(t)) return null;

  // Hard reject common non-name phrases (prevents "שמר את הכל" => "שמר")
  if (looksLikeHebrewImperativeSavePhrase(t)) return null;

  // allow 1-2 tokens only (e.g., "שי", "שי סיבוני")
  const parts = t.split(/\s+/).filter(Boolean);
  if (parts.length < 1 || parts.length > 2) return null;

  // If we captured "<name> ואני/מתעניין/..." keep only the first token.
  if (parts.length === 2) {
    const second = parts[1];
    const dropSecondIf = new Set([
      "ואני",
      "ואנחנו",
      "ואנוכי",
      "מתעניין",
      "מתעניינת",
      "מעוניין",
      "מעוניינת",
      "רוצה",
      "צריך",
      "צריכה",
      "באתי",
      "הגעתי",
    ]);
    if (dropSecondIf.has(second)) {
      parts.pop();
    }
  }

  // length guardrails (after token adjustment)
  const joined = parts.join(" ");
  if (joined.length < 2 || joined.length > 30) return null;

  // stopwords-only rejection (single token)
  if (parts.length === 1 && STOPWORDS_HE.has(parts[0])) return null;

  // Block obvious domain/product words (including prefixed forms like "בהליכון")
  for (const tok of parts) {
    const base = stripCommonHebrewPrefixForChecks(tok);
    if (PRODUCT_STOPWORDS_HE.has(tok) || PRODUCT_STOPWORDS_HE.has(base)) return null;
  }

  // supported scripts only
  if (!isSupportedScript(joined)) return null;

  // If Hebrew candidate contains the direct object marker "את" (rare in names) -> reject
  if (HEBREW_RE.test(joined) && parts.includes("את")) return null;

  return joined;
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

  // Hard reject obvious save/command phrases up-front (prevents regex capturing first token as "name")
  if (looksLikeHebrewImperativeSavePhrase(raw)) return null;

  // explicit self-intro patterns
  const patterns = [
    { re: /\bקוראים\s+לי\s+(.+)$/i, reason: "explicit_korim_li" },
    { re: /\bשמי\s+(.+)$/i, reason: "explicit_shmi" },
    // "אני <name>" only if it is a single short token (avoid capturing sentences like "אני מתעניין אצלכם")
    { re: /(?:^|\s)אני\s+([\u0590-\u05FF]{2,15})\s*$/i, reason: "explicit_ani_single" },
    { re: /\bהשם\s+שלי\s*(?:הוא|זה)?\s+(.+)$/i, reason: "explicit_hashem_sheli" },
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
