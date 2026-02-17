"use strict";

/**
 * Lightweight Hebrew normalization + NLP helpers (deterministic, fast).
 * Goals:
 * - robust intent matching with noisy ASR (speakerphone, accents)
 * - conservative normalization (avoid aggressive meaning changes)
 * - no external deps
 */

// Hebrew niqqud + cantillation marks
const HEBREW_DIACRITICS_RE = /[\u0591-\u05BD\u05BF\u05C1-\u05C2\u05C4-\u05C5\u05C7]/g;

// Turn punctuation into spaces for stable tokenization
const PUNCT_TO_SPACE_RE = /[“”"׳׳'`‘’.,;:!?()\[\]{}<>|\\\/\-–—_+=*~^]/g;

// Collapse whitespace
const WS_RE = /\s+/g;

// Hebrew final forms -> regular
const HEBREW_FINAL_MAP = new Map([
  ["ך", "כ"],
  ["ם", "מ"],
  ["ן", "נ"],
  ["ף", "פ"],
  ["ץ", "צ"]
]);

function safeStr(x) {
  if (x === undefined || x === null) return "";
  return String(x);
}

function stripDiacritics(s) {
  return s.replace(HEBREW_DIACRITICS_RE, "");
}

function normalizeFinalForms(s) {
  let out = "";
  for (const ch of s) out += HEBREW_FINAL_MAP.get(ch) || ch;
  return out;
}

function normalizeCommonQuotes(s) {
  // Normalize common Hebrew quotes/geresh to plain apostrophe-like or space
  return s.replace(/[״“”]/g, '"').replace(/[׳`‘’]/g, "'");
}

function normalizeHebrew(s) {
  // Keep Hebrew letters, numbers, basic latin; remove diacritics; normalize finals; normalize punctuation to spaces.
  let t = safeStr(s);
  t = normalizeCommonQuotes(t);
  t = stripDiacritics(t);
  t = normalizeFinalForms(t);
  t = t.replace(PUNCT_TO_SPACE_RE, " ");
  t = t.replace(WS_RE, " ").trim();

  // A few very common ASR confusions (conservative)
  // Example: שואות -> שעות
  t = t.replace(/\bשואות\b/g, "שעות");

  return t;
}

function normalizeLatin(s) {
  let t = safeStr(s).toLowerCase();
  t = t.replace(PUNCT_TO_SPACE_RE, " ");
  t = t.replace(WS_RE, " ").trim();
  return t;
}

function detectLang(text) {
  const s = safeStr(text);
  if (!s) return "unknown";
  // Hebrew block
  if (/[\u0590-\u05FF]/.test(s)) return "he";
  // Cyrillic (ru)
  if (/[\u0400-\u04FF]/.test(s)) return "ru";
  // Basic Latin
  if (/[A-Za-z]/.test(s)) return "en";
  return "unknown";
}

function tokenizeNormalized(normText) {
  if (!normText) return [];
  return normText.split(" ").map((x) => x.trim()).filter(Boolean);
}

/**
 * Very lightweight Hebrew stemmer:
 * - remove single-letter prefixes (ו/ה/ב/ל/כ/מ/ש) once
 * - remove a few suffixes (ים/ות/ה/ך/כם/כן/נו/תי) once
 * This is intentionally conservative.
 */
function hebrewStem(token) {
  let t = token || "";
  if (!t) return t;

  // Prefixes
  const prefixes = ["ו", "ה", "ב", "ל", "כ", "מ", "ש"];
  if (t.length >= 3 && prefixes.includes(t[0])) t = t.slice(1);

  // Suffixes (ordered longer -> shorter)
  const suffixes = ["יות", "ים", "ות", "כם", "כן", "נו", "תי", "ך", "ה"];
  for (const suf of suffixes) {
    if (t.length >= 4 && t.endsWith(suf)) {
      t = t.slice(0, -suf.length);
      break;
    }
  }

  return t;
}

function buildHebrewTokenSet(normText) {
  const toks = tokenizeNormalized(normText);
  const set = new Set();
  for (const tok of toks) {
    set.add(tok);
    const stem = hebrewStem(tok);
    if (stem && stem !== tok) set.add(stem);
  }
  return set;
}

function splitTriggersCell(cell) {
  const s = safeStr(cell).trim();
  if (!s) return [];
  // Support: | , ; newline
  return s
    .split(/[\|\n,;]+/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

module.exports = {
  detectLang,
  normalizeHebrew,
  normalizeLatin,
  tokenizeNormalized,
  buildHebrewTokenSet,
  splitTriggersCell
};
