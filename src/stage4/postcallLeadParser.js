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
  /*
    Target payload fields requested (GilSport-style):
    {
      "intent": string|null,
      "full_name": string|null,
      "callback_to_number": string|null,
      "subject": string|null,
      "notes": string|null,
      "brand": string|null,
      "model": string|null,
      "parsing_summary": string|null
    }

    - intent: כוונת הפונה כפי שנאמרה בשיחה (למשל "שאלת מידע", "תמיכה טכנית", "מכירה"). אם לא ברור – null.
    - full_name: שם הפונה כפי שנאמר במפורש. אם לא נאמר או לא בטוח – null.
    - callback_to_number: מספר טלפון מלא (בין 9–13 ספרות או בפורמט +972) שנאמר בשיחה ומיועד לחזרה. אם לא נאמר – null.
    - subject: כותרת תמציתית אך עשירה שמכילה את פרטי המפתח שנאמרו בפועל, ללא המצאות.
    - notes: הערות או פרטים נוספים שנאמרו ויכולים לעזור לטיפול (למשל דגם/מוצר/מועד) אך אינם חלק מהכותרת. אם אין – null.
    - brand + model: אם נאמרו מותג או דגם, החזירו אותם בשדות אלו. אחרת – null.
    - parsing_summary: משפט קצר (1–2) שמתאר את מהות השיחה ותמצית הצורך בפועל.
  */
  const knownName =
    typeof known.full_name === "string" && known.full_name.trim()
      ? known.full_name.trim()
      : null;
  const knownPhone =
    typeof known.callback_to_number === "string" && known.callback_to_number.trim()
      ? known.callback_to_number.trim()
      : null;
  let pref = "";
  if (knownName) {
    pref += `שם ידוע מהמערכת (מועדף על פני השערות): "${knownName}". `;
  }
  if (knownPhone) {
    pref += `מספר טלפון ידוע מהמערכת (מועדף על פני השערות): "${knownPhone}". `;
  }
  return (
    'החזירו JSON תקין בלבד (ללא טקסט נוסף) לפי הסכמה ' +
    '{"intent":string|null,"full_name":string|null,"callback_to_number":string|null,"subject":string|null,"notes":string|null,"brand":string|null,"model":string|null,"parsing_summary":string|null} ' +
    'על בסיס השיחה בלבד, בעברית תקנית ומנורמלת וללא המצאות. ' +
    pref +
    'intent הוא כוונת הפונה (מה הוא רוצה לבצע או לקבל) כפי שנאמרה, ללא ניחושים. אם לא נאמר – null. ' +
    'full_name הוא תמיד שם האדם שמדבר. אם נאמר מספר שמות, החזירו את השם האחרון שנאמר. אם לא נאמר או לא ברור – null. ' +
    'callback_to_number יכיל רק אם נאמר במפורש מספר טלפון מלא (9–13 ספרות או בפורמט +972) בשיחה; אם יש מספר ידוע מהמערכת – החזירו אותו אם לא נאמר אחר. אחרת – null. ' +
    'subject הוא כותרת תמציתית אך עשירה של מה שהפונה מבקש, ללא המצאות. ' +
    'notes הם פרטים נוספים או הבהרות שאינם חלק מהכותרת (למשל דגם, שנה, מועד), אם נאמרו; אחרת – null. ' +
    'brand ו-model ימולאו רק אם נאמרו במפורש מותג או דגם בשיחה. אם לא נאמר – null. ' +
    'parsing_summary הוא משפט קצר (1–2) שמתאר את מהות הפנייה ומה צריך לעשות, ללא ציון חוסרים וללא המצאות. ' +
    'כלל עקביות: אם נתון לא נאמר בשיחה – להחזיר null ולא לנחש.'
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
        parts: [{ text: `${prompt}\n\n=== תמלול שיחה (USER/BOT) ===\n${transcript}` }],
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
  const text = json?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") || "";
  return safeJsonExtract(text);
}

function normalizeParsedLead(raw) {
  const out = {
    intent: null,
    full_name: null,
    callback_to_number: null,
    subject: null,
    notes: null,
    brand: null,
    model: null,
    parsing_summary: null,
  };
  if (!raw || typeof raw !== "object") return out;
  for (const k of Object.keys(out)) {
    const v = raw[k];
    if (v == null) continue;
    if (typeof v === "string") {
      const s = v.trim();
      out[k] = s ? s : null;
    }
  }
  return out;
}

async function parseLeadPostcall({ turns, transcriptText, ssot, known }) {
  if (!env.LEAD_PARSER_ENABLED) return null;
  const transcript =
    typeof transcriptText === "string" && transcriptText.trim()
      ? transcriptText.trim()
      : buildTranscript(turns);
  if (!transcript) return null;

  // IMPORTANT: Do NOT fall back to LEAD_CAPTURE_PROMPT here.
  // LEAD_CAPTURE_PROMPT is for realtime dialogue and may not be JSON-only.
  // Post-call parsing must be deterministic JSON.
  // When using the SSOT prompt, ensure it requests the extended fields (intent, callback_to_number, notes, brand, model).
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
