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
  "מעוניין", "מתעניין", "רוצה", "צריך", "צריכה", "מעוניינת", "מתעניינת", "מתקשר", "מתקשרת",

  // Hardening: imperative/verb-like tokens that frequently appear in calls and are not names
  "שמר", "שמור", "שמרי", "שמרו", "תשמור", "תשמרי", "תשמרו",
  "תבדוק", "בדוק", "תעזור", "עזור", "תשלח", "שלח", "תשלחי", "תשלחו",
  "תקבע", "קבע", "תקבעי", "תקבעו",
  "תעשה", "עשה", "תעשי", "תעשו",
  "תן", "תני", "תנו",
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


function normalizeToken(tok) {
  const t = stripPunct(tok || "");
  return t.replace(/^ו/, "").trim();
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

  // Reject common Hebrew preposition+"ה" prefix that often indicates a noun/phrase,
  // e.g. "בהליכון" ("in the treadmill"). Keep narrow to avoid rejecting names like "ברק".
  if (t.length >= 4 && t[0] === "ב" && t[1] === "ה") return null;

  // Hard reject common non-name phrases (prevents "שמר את הכל" => "שמר")
  if (looksLikeHebrewImperativeSavePhrase(t)) return null;

  // allow 1-2 tokens only (e.g., "שי", "שי סיבוני")
  const parts = t.split(/\s+/).filter(Boolean);
  if (parts.length < 1 || parts.length > 2) return null;
  // Reject phrases like "אני מתעניין" mistakenly captured as a name.
  if (parts.length >= 1) {
    const p1 = normalizeToken(parts[0]);
    if (["אני", "אנחנו"].includes(p1)) return null;
  }
  // If any token is a known stopword (e.g., "מתעניין", "מתקשר"), treat as not-a-name.
  if (parts.some((p) => STOPWORDS_HE.has(normalizeToken(p)))) return null;


  // length guardrails
  if (t.length < 2 || t.length > 30) return null;

  // Cleanup: remove trailing conjunction artifacts commonly produced by STT,
  // e.g. "שי ואני" -> "שי"
  if (parts.length === 2) {
    const p1 = parts[0];
    const p2 = parts[1];

    // Normalize common conjunction forms
    const normP2 = p2.replace(/^ו/, ""); // remove leading "ו" (and)
    if (normP2 === "אני" || normP2 === "אנחנו") {
      return p1;
    }
  }

  // stopwords-only rejection (single token)
  if (parts.length === 1 && STOPWORDS_HE.has(parts[0])) return null;

  // supported scripts only
  if (!isSupportedScript(t)) return null;

  // If Hebrew candidate contains the direct object marker "את" (rare in names) -> reject
  if (HEBREW_RE.test(t) && parts.includes("את")) return null;

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

  // Hard reject obvious save/command phrases up-front (prevents regex capturing first token as "name")
  if (looksLikeHebrewImperativeSavePhrase(raw)) return null;

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
