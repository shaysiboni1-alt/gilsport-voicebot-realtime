// src/stage4/postcallLeadParser.js
"use strict";

// Post-call lead parsing (LLM) - STRICT JSON.
// Hardened to prevent hallucinated names (e.g. "אשמח") and to respect known caller name.

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

  // remove ```json fences if present
  const cleaned = text
    .replace(/```json/gi, "```")
    .replace(/```/g, "");

  const first = cleaned.indexOf("{");
  const last = cleaned.lastIndexOf("}");
  if (first === -1 || last === -1 || last <= first) return null;

  const slice = cleaned.slice(first, last + 1);
  try {
    return JSON.parse(slice);
  } catch {
    return null;
  }
}

function normalizeStringOrNull(v) {
  if (typeof v !== "string") return null;
  const s = v.trim();
  return s ? s : null;
}

// Detect if SSOT prompt is the "4 fields only" deterministic schema you pasted
function wantsFourFieldsOnly(promptText) {
  const p = String(promptText || "");
  return (
    p.includes('{"full_name":string|null,"subject":string|null,"callback_to_number":string|null,"notes":string|null}')
    && !p.includes('"intent"')
    && !p.includes('"brand"')
    && !p.includes('"model"')
  );
}

// Allow name only if explicitly stated in the transcript in a deterministic way
function extractExplicitNameFromTranscript(transcript) {
  const t = String(transcript || "");
  if (!t) return null;

  // Look only at USER lines
  const userLines = t
    .split("\n")
    .filter((ln) => ln.startsWith("USER:"))
    .map((ln) => ln.slice(5).trim())
    .filter(Boolean);

  const patterns = [
    /\b(?:קוראים\s+לי)\s+([^\n\r]+)$/i,
    /\b(?:שמי)\s+([^\n\r]+)$/i,
    /\b(?:השם\s+שלי)\s+([^\n\r]+)$/i,
  ];

  for (const line of userLines) {
    for (const re of patterns) {
      const m = line.match(re);
      if (m && m[1]) {
        const cand = String(m[1]).trim();
        if (cand && cand.length >= 2 && cand.length <= 40) return cand;
      }
    }
  }

  return null;
}

function defaultPrompt(known = {}) {
  const knownName = normalizeStringOrNull(known?.full_name);
  const knownPhone = normalizeStringOrNull(known?.callback_to_number);

  let pref = "";
  if (knownName) pref += `שם ידוע מהמערכת (אם לא נאמר במפורש בשיחה – אל תמציא): "${knownName}". `;
  if (knownPhone) pref += `מספר ידוע מהמערכת (מותר להחזיר רק אם אושר/נאמר במפורש או כחלק ממדיניות המערכת): "${knownPhone}". `;

  return (
    'החזירו JSON תקין בלבד (ללא טקסט נוסף) לפי הסכמה ' +
    '{"full_name":string|null,"subject":string|null,"callback_to_number":string|null,"notes":string|null} ' +
    'על בסיס דברי המשתמש בלבד, ללא המצאות. ' +
    pref +
    'full_name יוחזר רק אם המשתמש אמר במפורש "קוראים לי ___" או "שמי ___" או "השם שלי ___". אחרת null. ' +
    'subject עד 6 מילים. ' +
    'notes משפט אחד בעברית תקנית על מה שביקש. ' +
    'callback_to_number יוחזר רק אם המשתמש מסר מספר מלא או אישר במפורש חזרה למספר שממנו התקשר לאחר שנשאל. אחרת null.'
  );
}

async function callGeminiForJson({ prompt, transcript }) {
  const apiKey = env.GEMINI_API_KEY;
  const model = env.LEAD_PARSER_MODEL || "gemini-2.0-flash";
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
      temperature: 0.0,
      topP: 0.9,
      maxOutputTokens: 384,
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

// Normalize to your minimal schema, and keep room for the caller to extend later
function normalizeParsedLead4(raw) {
  return {
    full_name: normalizeStringOrNull(raw?.full_name),
    subject: normalizeStringOrNull(raw?.subject),
    callback_to_number: normalizeStringOrNull(raw?.callback_to_number),
    notes: normalizeStringOrNull(raw?.notes),
  };
}

// Normalize to the extended schema used by finalizePipeline
function normalizeParsedLeadExtended(raw) {
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
    out[k] = normalizeStringOrNull(raw[k]);
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

  const ssotPrompt = String(ssot?.prompts?.LEAD_PARSER_PROMPT || "").trim();
  const prompt = ssotPrompt || defaultPrompt(known);

  try {
    const raw = await callGeminiForJson({ prompt, transcript });

    // If SSOT prompt is 4-fields-only → convert to extended shape (finalize expects it)
    let parsed;
    if (wantsFourFieldsOnly(prompt)) {
      const four = normalizeParsedLead4(raw || {});
      parsed = {
        intent: null,
        full_name: four.full_name,
        callback_to_number: four.callback_to_number,
        subject: four.subject,
        notes: four.notes,
        brand: null,
        model: null,
        parsing_summary: null,
      };
    } else {
      parsed = normalizeParsedLeadExtended(raw || {});
    }

    // HARDEN: name is allowed only if explicitly stated in transcript
    const knownName = normalizeStringOrNull(known?.full_name);
    const explicit = extractExplicitNameFromTranscript(transcript);

    if (knownName) {
      // Prefer system-known name; never let the LLM overwrite it
      parsed.full_name = knownName;
    } else {
      // No known name: accept only explicit transcript pattern
      parsed.full_name = explicit ? explicit : null;
    }

    logger.info({ msg: "Postcall lead parsed", meta: { ok: !!raw, has_known_name: !!knownName } });
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
