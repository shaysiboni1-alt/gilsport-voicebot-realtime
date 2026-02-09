// src/stage4/postcallLeadParser.js
"use strict";

// Post-call lead parsing (LLM) similar to GilSport style.
// Uses Gemini generateContent (API key) and forces STRICT JSON output.

const { env } = require("../config/env");
const { logger } = require("../utils/logger");

function buildTranscript(turns) {
  if (!Array.isArray(turns)) return "";
  return turns
    .filter((t) => t && typeof t.text === "string" && t.text.trim())
    .map((t) => `${t.role === "user" ? "USER" : "BOT"}: ${t.text.trim()}`)
    .join("\n");
}

function safeJsonExtract(text) {
  if (!text || typeof text !== "string") return null;
  const first = text.indexOf("{");
  const last = text.lastIndexOf("}");
  if (first === -1 || last === -1 || last <= first) return null;
  const slice = text.slice(first, last + 1);
  try {
    return JSON.parse(slice);
  } catch {
    return null;
  }
}

function defaultPrompt(known = {}) {
  // Target payload fields requested:
  // full_name, subject, reason, phone_additional, parsing_summary
  const knownName = typeof known.full_name === "string" && known.full_name.trim() ? known.full_name.trim() : null;
  return (
    'החזירו JSON תקין בלבד (ללא טקסט נוסף) לפי הסכמה ' +
    '{"full_name":string|null,"subject":string|null,"reason":string|null,"phone_additional":string|null,"parsing_summary":string|null} ' +
    'על בסיס השיחה בלבד, בעברית תקנית ומנורמלת וללא המצאות. ' +
    (knownName ? `שם ידוע מהמערכת (מועדף על פני השערות): "${knownName}". אם קיים שם ידוע מהמערכת—החזירו אותו כ-full_name אפילו אם התמלול כולל מילים קודמות שנראות כמו שם. ` : '') +
    'full_name הוא תמיד שם הפונה שמדבר כעת ורק אם נאמר במפורש שם של אדם (לא מוצר/תקלה/מושג) ואם אין ודאות גבוהה—null. ' +
    'subject הוא כותרת תמציתית אך עשירה שמכילה את פרטי המפתח שנאמרו בפועל (מה רוצים/איזה מסמכים/לאילו גופים/תקופות/דחיפות אם צוין), בלי להמציא. אם חסר פרט—פשוט לא לכלול אותו. אם לא ברור—null. ' +
    'reason הוא משפט קצר וברור שמתאר את מהות הפנייה בפועל, ללא משפטי מערכת. אם לא ברור—null. ' +
    'phone_additional ימולא רק אם נאמר במפורש מספר טלפון מלא בן 9–10 ספרות בשיחה (כולל מקרה Phone Override) אחרת null. ' +
    'parsing_summary הוא סיכום תמציתי (משפט 1–2) של מה שנאמר ונדרש טיפול, בלי ציון חוסרים ובלי המצאות; אם אין מספיק מידע—null. ' +
    'כלל עקביות: אם נתון לא נאמר בשיחה—להחזיר null ולא לנחש.'
  );
}

async function callGeminiForJson({ prompt, transcript }) {
  const apiKey = env.GEMINI_API_KEY;
  const model = env.LEAD_PARSER_MODEL || "gemini-1.5-flash";
  if (!apiKey) throw new Error("GEMINI_API_KEY missing");

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const body = {
    contents: [
      {
        role: "user",
        parts: [
          { text: `${prompt}\n\n=== תמלול שיחה (USER/BOT) ===\n${transcript}` },
        ],
      },
    ],
    generationConfig: {
      temperature: 0.1,
      topP: 0.9,
      maxOutputTokens: 512,
    },
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) {
    const txt = await res.text().catch(() => "");
    throw new Error(`Gemini lead parser HTTP ${res.status}: ${txt.slice(0, 300)}`);
  }

  const json = await res.json();
  const text =
    json?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") || "";
  return safeJsonExtract(text);
}

function normalizeParsedLead(raw) {
  const out = {
    full_name: null,
    subject: null,
    reason: null,
    phone_additional: null,
    parsing_summary: null,
  };
  if (!raw || typeof raw !== "object") return out;
  for (const k of Object.keys(out)) {
    const v = raw[k];
    if (v === null) continue;
    if (typeof v === "string") {
      const s = v.trim();
      out[k] = s ? s : null;
    }
  }
  return out;
}

async function parseLeadPostcall({ turns, transcriptText, ssot, known }) {
  if (!env.LEAD_PARSER_ENABLED) return null;
  const transcript = (typeof transcriptText === "string" && transcriptText.trim()) ? transcriptText.trim() : buildTranscript(turns);
  if (!transcript) return null;

  // IMPORTANT: Do NOT fall back to LEAD_CAPTURE_PROMPT here.
  // LEAD_CAPTURE_PROMPT is for realtime dialogue and may not be JSON-only.
  // Post-call parsing must be deterministic JSON.
  const prompt = (ssot?.prompts?.LEAD_PARSER_PROMPT || "").trim() || defaultPrompt(known);

  try {
    const raw = await callGeminiForJson({ prompt, transcript });
    const parsed = normalizeParsedLead(raw);
    logger.info({ msg: "Postcall lead parsed", meta: { ok: !!raw } });
    return parsed;
  } catch (e) {
    logger.warn({
      msg: "Postcall lead parse failed",
      meta: { err: e && (e.message || String(e)) },
    });
    return null;
  }
}

module.exports = {
  parseLeadPostcall,
};
