"use strict";

/**
 * Deterministic caller-name extractor.
 * Goal: capture ONLY when high confidence it's the caller's name.
 */

const HEBREW_RE = /[\u0590-\u05FF]/;
const LATIN_RE = /[A-Za-z]/;
const CYRILLIC_RE = /[\u0400-\u04FF]/;

const STOPWORDS_HE = new Set([
  "כן","לא","אוקיי","אוקי","טוב","בסדר","סבבה","אה","אממ","הממ","רגע","שלום","היי","הלו",
  "מה","מי","אני","קוראים","לי","שמי","זה","כאן","מדבר","מדברת","איתך","אתך","נעים",
  "אישה","בת","גברת","אדוני","רוצה","רציתי","צריך","צריכה","צריכים","צריכות","מחפש","מחפשת",
  "משרד","מרגריטה","ריטה","דוח","דוחות","אישור","אישורים","מסמך","מסמכים","שירות","פעילות"
]);

const INVALID_SINGLE_TOKEN_HE = new Set([
  "אני","אישה","בת","גבר","ילד","ילדה","גברת","אדוני","שלום","הלו","רגע","כן","לא",
  "טוב","בסדר","רוצה","רציתי","צריך","צריכה","צריכים","צריכות","מבקש","מבקשת","מחפש","מחפשת",
  "דוח","דוחות","אישור","אישורים","מסמך","מסמכים","משרד","מרגריטה","ריטה","מייל","טלפון",
  "שעות","פעילות","עזרה","בעיה","חזרה","מבטא","קול"
]);

const INVALID_ANY_TOKEN_HE = new Set([
  "אני","היא","הוא","אנחנו","אתם","אתן","בת","אישה","גברת","אדוני","צריך","צריכה",
  "צריכים","צריכות","רוצה","רציתי","מחפש","מחפשת","שלום","הלו","כן","לא","משרד","מרגריטה"
]);

function isSupportedScript(t) {
  return HEBREW_RE.test(t) || LATIN_RE.test(t) || CYRILLIC_RE.test(t);
}

function stripPunct(s) {
  return String(s || "")
    .replace(/[\u200f\u200e]/g, "")
    .replace(/[“”„״'"`´]/g, "")
    .replace(/[.,!?;:()\[\]{}<>\/\\-]/g, " ")
    .replace(/\s{2,}/g, " ")
    .trim();
}

function hasMixedScripts(t) {
  const hasHebrew = HEBREW_RE.test(t);
  const hasLatin = LATIN_RE.test(t);
  const hasCyrillic = CYRILLIC_RE.test(t);
  return [hasHebrew, hasLatin, hasCyrillic].filter(Boolean).length > 1;
}

function isLikelyInvalidHebrewToken(token) {
  const t = String(token || "").trim();
  if (!t) return true;
  if (INVALID_ANY_TOKEN_HE.has(t)) return true;
  if (/^[\u0590-\u05FF]$/.test(t)) return true;
  if (/^(שלי|שלכם|שלכן|שלו|שלה|פה|כאן|זה|זאת|הזה|הזאת)$/u.test(t)) return true;
  return false;
}

function sanitizeCandidate(raw) {
  const t = stripPunct(raw);
  if (!t) return null;
  if (/\d/.test(t)) return null;
  if (!isSupportedScript(t)) return null;
  if (hasMixedScripts(t)) return null;

  const parts = t.split(/\s+/).filter(Boolean);
  if (parts.length < 1 || parts.length > 2) return null;
  if (t.length < 2 || t.length > 30) return null;

  for (const p of parts) {
    if (p.length < 2) return null;
    if (!/^[\p{L}]+$/u.test(p)) return null;
  }

  if (parts.length === 1) {
    if (STOPWORDS_HE.has(parts[0]) || INVALID_SINGLE_TOKEN_HE.has(parts[0])) {
      return null;
    }
  }

  if (HEBREW_RE.test(t)) {
    for (const p of parts) {
      if (isLikelyInvalidHebrewToken(p)) return null;
    }
  }

  return parts.join(" ");
}

function lastBotAskedForName(lastBotUtterance) {
  const t = stripPunct(lastBotUtterance || "");
  if (!t) return false;
  return /מה\s*השם|איך\s*קוראים|מי\s*מדבר|מי\s*מדברת|שמך|שמך\s*בבקשה|איך\s*קוראים\s*לכם|איך\s*קוראים\s*לך/i.test(t);
}

function extractCallerName({ userText, lastBotUtterance }) {
  const raw = String(userText || "").trim();
  if (!raw) return null;

  const patterns = [
    { re: /(?:^|\b)קוראים\s+לי\s+(.+)$/i, reason: "explicit_korim_li" },
    { re: /(?:^|\b)שמי\s+(.+)$/i, reason: "explicit_shmi" },
    { re: /(?:^|\b)השם\s+שלי\s+(.+)$/i, reason: "explicit_hashem_sheli" },
    { re: /(?:^|\b)my\s+name\s+is\s+(.+)$/i, reason: "explicit_my_name_is" },
    { re: /(?:^|\b)меня\s+зовут\s+(.+)$/i, reason: "explicit_menya_zovut" },
  ];

  for (const p of patterns) {
    const m = raw.match(p.re);
    if (!m || !m[1]) continue;
    const cand = sanitizeCandidate(m[1]);
    if (cand) return { name: cand, reason: p.reason };
  }

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
