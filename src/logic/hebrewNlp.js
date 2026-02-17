"use strict";

// -----------------------------------------------------------------------------
// Hebrew Normalization + Light NLP (Deterministic)
// -----------------------------------------------------------------------------
// This module is intentionally deterministic (no extra LLM calls) to avoid
// adding latency or new failure modes during live calls.

// Strip Hebrew diacritics (nikud + cantillation)
const HEBREW_DIACRITICS_RE = /[\u0591-\u05C7]/g;

// Common punctuation variants
const PUNCT_NORMALIZE_MAP = new Map([
  ["׳", "'"],
  ["״", '"'],
  ["–", "-"],
  ["—", "-"],
  ["−", "-"],
  ["…", "..."],
  ["“", '"'],
  ["”", '"'],
  ["‘", "'"],
  ["’", "'"],
]);

// Hebrew digit words that often appear in phone numbers
const HE_DIGIT_WORDS = new Map([
  ["אפס", "0"],
  ["אפסים", "0"],
  ["אחת", "1"],
  ["אחד", "1"],
  ["שתיים", "2"],
  ["שתים", "2"],
  ["שניים", "2"],
  ["שנים", "2"],
  ["שלוש", "3"],
  ["ארבע", "4"],
  ["חמש", "5"],
  ["שש", "6"],
  ["שבע", "7"],
  ["שמונה", "8"],
  ["תשע", "9"],
]);

function safeStr(x) {
  if (x === undefined || x === null) return "";
  return String(x);
}

function normalizePunctuation(s) {
  let out = s;
  for (const [from, to] of PUNCT_NORMALIZE_MAP.entries()) {
    out = out.split(from).join(to);
  }
  return out;
}

function collapseWhitespace(s) {
  return s.replace(/\s+/g, " ").trim();
}

function stripDiacriticsHebrew(s) {
  return s.replace(HEBREW_DIACRITICS_RE, "");
}

function basicNormalize(s) {
  let out = safeStr(s);
  out = out.replace(/\u200f|\u200e|\ufeff/g, ""); // bidi marks / BOM
  out = stripDiacriticsHebrew(out);
  out = normalizePunctuation(out);
  out = collapseWhitespace(out);
  return out;
}

function detectLanguageRough(s) {
  const t = safeStr(s);
  if (!t) return "unknown";
  if (/[\u0590-\u05FF]/.test(t)) return "he";
  if (/[A-Za-z]/.test(t)) return "en";
  if (/[\u0400-\u04FF]/.test(t)) return "ru";
  return "unknown";
}

// Convert sequences of Hebrew digit words into digits, when they look like a phone number.
// Example: "אפס שתיים חמש שלוש" -> "0253"
function hebrewDigitWordsToDigits(text) {
  const s = safeStr(text);
  if (!s) return "";

  const tokens = s.split(/\s+/g);
  const out = [];

  for (let i = 0; i < tokens.length; i++) {
    const tok = tokens[i];
    const clean = tok.replace(/[^\u0590-\u05FFA-Za-z0-9]/g, "");
    if (HE_DIGIT_WORDS.has(clean)) {
      out.push(HE_DIGIT_WORDS.get(clean));
      continue;
    }
    // pass-through digits
    if (/^\d+$/.test(clean)) {
      out.push(clean);
      continue;
    }
    // separator marker (keeps output readable)
    out.push(tok);
  }

  // If we ended up with many digits, compact them.
  const joined = out.join(" ");
  const compactDigits = joined.replace(/(\d)\s+(?=\d)/g, "$1");
  return compactDigits;
}

// Main function used by runtime:
// - raw: original transcript
// - normalized: cleaned transcript, good for matching triggers
// - normalized_for_numbers: additionally converts hebrew digit words
function normalizeUtterance(text) {
  const raw = safeStr(text);
  const normalized = basicNormalize(raw);
  const lang = detectLanguageRough(normalized);
  const normalized_for_numbers = lang === "he" ? hebrewDigitWordsToDigits(normalized) : normalized;

  return {
    raw,
    normalized,
    normalized_for_numbers,
    lang,
  };
}

module.exports = {
  normalizeUtterance,
  detectLanguageRough,
  basicNormalize,
  hebrewDigitWordsToDigits,
};
