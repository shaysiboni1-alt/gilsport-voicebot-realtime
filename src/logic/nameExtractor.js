// src/logic/nameExtractor.js
"use strict";

/**
 * Deterministic caller-name extractor.
 *
 * Design goals:
 * - Capture a caller name ONLY when confidence is high.
 * - Never "guess" from context; rely only on explicit self-identification patterns
 *   or a direct, plausible answer immediately after a name question.
 * - Resist false-positives from product names, intents, verbs, and short filler.
 *
 * Output:
 *   { name: string, confidence: "high", reason: string, nameLocked: boolean } | null
 */

const HEBREW_RE = /[\u0590-\u05FF]/;
const LATIN_RE = /[A-Za-z]/;
const CYRILLIC_RE = /[\u0400-\u04FF]/;

const MAX_NAME_CHARS = 30;

// Common Hebrew fillers / non-names.
const STOPWORDS_HE = new Set([
  "כן",
  "לא",
  "אוקיי",
  "אוקי",
  "בסדר",
  "טוב",
  "היי",
  "שלום",
  "תודה",
  "תודה רבה",
  "מה",
  "מי",
  "אני",
  "אנחנו",
  "אתם",
  "את",
  "אתה",
  "כאן",
  "זה",
  "זאת",
  "השם",
  "שמי",
  "קוראים",
  "לי",
  "שלי",
  "מדבר",
  "מדברת",
  "מדברים",
  "מדברות",
  "רק",
  "סתם",
  "בסך הכל",
  "בטח",
  "ברור",
]);

// Product-ish / domain-ish tokens that should never be accepted as a person's name.
// Keep small + high-signal. Add as needed.
const NON_NAME_KEYWORDS_HE = new Set([
  "הליכון",
  "אופניים",
  "אליפטיקל",
  "קרוס",
  "משקולות",
  "דמבל",
  "דאמבל",
  "מולטי",
  "סמית",
  "מכשיר",
  "מוצר",
  "דגם",
  "סוג",
  "מחיר",
  "מבצע",
  "מכירה",
  "מלאי",
  "תיקון",
  "שירות",
  "אחריות",
  "משלוח",
  "הובלה",
]);

function _stripQuotes(s) {
  return s.replace(/^["'“”׳״]+|["'“”׳״]+$/g, "");
}

function _normalizeWhitespace(s) {
  return s.replace(/\s+/g, " ").trim();
}

function _cleanupName(raw) {
  if (!raw) return "";
  let s = String(raw);
  s = s.replace(/[.,!?;:()\[\]{}<>]/g, " ");
  s = _stripQuotes(s);
  s = _normalizeWhitespace(s);
  // remove leading "אני" if model/user repeats it
  s = s.replace(/^(?:אני)\s+/u, "");
  return s;
}

function _tokenize(s) {
  return _normalizeWhitespace(s).split(" ").filter(Boolean);
}

function _scriptKind(s) {
  if (HEBREW_RE.test(s)) return "he";
  if (CYRILLIC_RE.test(s)) return "ru";
  if (LATIN_RE.test(s)) return "en";
  return "other";
}

function _containsDigit(s) {
  return /\d/.test(s);
}

function _looksLikeSingleLetter(s) {
  return s.length === 1 && (LATIN_RE.test(s) || CYRILLIC_RE.test(s) || HEBREW_RE.test(s));
}

function _isPlausibleNameCandidate(candidate) {
  const s = _cleanupName(candidate);
  if (!s) return false;
  if (s.length > MAX_NAME_CHARS) return false;
  if (_containsDigit(s)) return false;

  const kind = _scriptKind(s);
  if (kind === "other") return false;

  // Allow: 1-3 tokens. (e.g., "שי", "שיר", "דוד לוי")
  const tokens = _tokenize(s);
  if (tokens.length < 1 || tokens.length > 3) return false;

  // Reject if any token is clearly not a name.
  for (const t of tokens) {
    const tt = t.trim();
    if (!tt) return false;
    if (_looksLikeSingleLetter(tt)) return false;

    // Heuristic: names rarely start with a prefix preposition like "ב" in Hebrew,
    // and "בהליכון" is a common false positive in this domain.
    if (kind === "he" && tt.length >= 2 && tt.startsWith("ב") && HEBREW_RE.test(tt)) {
      // Allow common legitimate names that start with ב (e.g., "בר", "בן", "בני") by exception:
      if (!["בר", "בן", "בני", "בלה", "ביאנה", "ברוך"].includes(tt)) return false;
    }

    if (kind === "he") {
      if (STOPWORDS_HE.has(tt)) return false;
      if (NON_NAME_KEYWORDS_HE.has(tt)) return false;
      if (NON_NAME_KEYWORDS_HE.has(tt.replace(/^ב/, ""))) return false; // e.g., "בהליכון"
    }

    // Disallow tokens that contain punctuation after cleanup (shouldn't happen, but defensive)
    if (/[^ \u0590-\u05FFA-Za-z\u0400-\u04FF\-']/u.test(tt)) return false;
  }

  // Reject if full string equals a stopword phrase
  if (kind === "he" && STOPWORDS_HE.has(s)) return false;

  return true;
}

/**
 * Extract name from text using explicit self-identification patterns.
 * Returns null when not confident.
 */
function extractNameFromText(text) {
  if (!text) return null;
  const raw = _normalizeWhitespace(String(text));
  if (!raw) return null;

  // Strong explicit patterns: "קוראים לי X", "שמי X", "השם שלי X", "השם שלי זה X"
  // Keep patterns deterministic and conservative.
  const patterns = [
    { re: /\b(?:קוראים לי|שמי|שמי הוא|שמי זה)\s+(.+)$/u, reason: "explicit_self_identification", locked: true },
    { re: /\b(?:השם שלי)\s+(?:הוא|זה)?\s*(.+)$/u, reason: "explicit_self_identification", locked: true },
  ];

  for (const p of patterns) {
    const m = raw.match(p.re);
    if (!m || !m[1]) continue;

    const candidate = _cleanupName(m[1]);
    if (!_isPlausibleNameCandidate(candidate)) continue;

    return {
      name: candidate,
      confidence: "high",
      reason: p.reason,
      nameLocked: p.locked,
    };
  }

  return null;
}

/**
 * Extract name from a direct answer after the bot asked for the caller's name.
 * This is allowed only when the entire utterance looks like a plausible name,
 * with no extra words that suggest a different intent.
 */
function extractNameFromDirectAnswer(text) {
  if (!text) return null;
  const raw = _cleanupName(text);
  if (!raw) return null;

  // Must be short and name-like.
  // Accept one or two tokens; three tokens allowed only if short (e.g., "דוד בן לוי")
  const tokens = _tokenize(raw);
  if (tokens.length < 1 || tokens.length > 3) return null;
  if (raw.length > 24) return null;

  if (!_isPlausibleNameCandidate(raw)) return null;

  return {
    name: raw,
    confidence: "high",
    reason: "direct_answer_to_name_question",
    // Direct answers are considered locked IF they are plausible.
    nameLocked: true,
  };
}

module.exports = {
  extractNameFromText,
  extractNameFromDirectAnswer,
};
