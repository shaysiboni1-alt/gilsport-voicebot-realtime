"use strict";

function opt(name, def = "") {
  const v = process.env[name];
  return v === undefined || v === null ? def : v;
}
function optInt(name, def) {
  const v = process.env[name];
  if (v === undefined || v === null || v === "") return def;
  const n = parseInt(v, 10);
  if (Number.isNaN(n)) throw new Error(`Invalid int env var ${name}: ${v}`);
  return n;
}
function optFloat(name, def) {
  const v = process.env[name];
  if (v === undefined || v === null || v === "") return def;
  const n = parseFloat(v);
  if (Number.isNaN(n)) throw new Error(`Invalid float env var ${name}: ${v}`);
  return n;
}
function optBool(name, def) {
  const v = process.env[name];
  if (v === undefined || v === null || v === "") return def;
  return String(v).toLowerCase() === "true";
}

const env = {
  PORT: optInt("PORT", 10000),
  PROVIDER_MODE: opt("PROVIDER_MODE", "gemini"),
  TIME_ZONE: opt("TIME_ZONE", "Asia/Jerusalem"),
  PUBLIC_BASE_URL: opt("PUBLIC_BASE_URL", ""),

  GSHEET_ID: opt("GSHEET_ID", ""),
  GOOGLE_SERVICE_ACCOUNT_JSON_B64: opt("GOOGLE_SERVICE_ACCOUNT_JSON_B64", ""),
  SSOT_TTL_MS: optInt("SSOT_TTL_MS", 60000),

  GEMINI_API_KEY: opt("GEMINI_API_KEY", ""),
  GEMINI_LIVE_MODEL: opt("GEMINI_LIVE_MODEL", ""),
  GEMINI_LOCATION: opt("GEMINI_LOCATION", "us-central1"),
  GEMINI_PROJECT_ID: opt("GEMINI_PROJECT_ID", ""),
  GEMINI_VERTEX_ENABLED: optBool("GEMINI_VERTEX_ENABLED", false),
  GEMINI_AUDIO_IN_FORMAT: opt("GEMINI_AUDIO_IN_FORMAT", "ulaw8k"),
  GEMINI_AUDIO_OUT_FORMAT: opt("GEMINI_AUDIO_OUT_FORMAT", "ulaw8k"),

  TWILIO_ACCOUNT_SID: opt("TWILIO_ACCOUNT_SID", ""),
  TWILIO_AUTH_TOKEN: opt("TWILIO_AUTH_TOKEN", ""),

  CALL_LOG_WEBHOOK_URL: opt("CALL_LOG_WEBHOOK_URL", ""),
  CALL_LOG_AT_START: optBool("CALL_LOG_AT_START", false),
  CALL_LOG_AT_END: optBool("CALL_LOG_AT_END", true),
  CALL_LOG_MODE: opt("CALL_LOG_MODE", "start"),
  FINAL_WEBHOOK_URL: opt("FINAL_WEBHOOK_URL", ""),
  FINAL_ON_STOP: optBool("FINAL_ON_STOP", true),
  ABANDONED_WEBHOOK_URL: opt("ABANDONED_WEBHOOK_URL", ""),

  MB_VAD_PREFIX_MS: optInt("MB_VAD_PREFIX_MS", 40),
  MB_VAD_SILENCE_MS: optInt("MB_VAD_SILENCE_MS", 120),
  MB_VAD_THRESHOLD: optFloat("MB_VAD_THRESHOLD", 0.52),

  SILENCE_T1_MS: optInt("SILENCE_T1_MS", 5000),
  SILENCE_T2_MS: optInt("SILENCE_T2_MS", 9000),
  SILENCE_T3_MS: optInt("SILENCE_T3_MS", 14000),
  SILENCE_PROMPT_1: opt("SILENCE_PROMPT_1", ""),
  SILENCE_PROMPT_2: opt("SILENCE_PROMPT_2", ""),
  SILENCE_PROMPT_3: opt("SILENCE_PROMPT_3", ""),

  MB_DEBUG: optBool("MB_DEBUG", false),
  MB_LOG_TRANSCRIPTS: optBool("MB_LOG_TRANSCRIPTS", true),
  MB_LOG_TURNS: optBool("MB_LOG_TURNS", true),
  MB_LOG_TURNS_MAX_CHARS: optInt("MB_LOG_TURNS_MAX_CHARS", 900),
  MB_LOG_ASSISTANT_TEXT: optBool("MB_LOG_ASSISTANT_TEXT", false),
  MB_LOG_LANGUAGE_DECISIONS: optBool("MB_LOG_LANGUAGE_DECISIONS", true),
  MB_LOG_FINALIZE_DECISIONS: optBool("MB_LOG_FINALIZE_DECISIONS", true),

  LEAD_PARSER_ENABLED: optBool("LEAD_PARSER_ENABLED", true),
  LEAD_PARSER_MODE: opt("LEAD_PARSER_MODE", "postcall"),
  LEAD_PARSER_MODEL: opt("LEAD_PARSER_MODEL", ""),
  LEAD_SUMMARY_STYLE: opt("LEAD_SUMMARY_STYLE", "crm_short"),

  MB_ENABLE_RECORDING: optBool("MB_ENABLE_RECORDING", false),
  FORCE_HANGUP_AFTER_CLOSE: optBool("FORCE_HANGUP_AFTER_CLOSE", true),
  HANGUP_AFTER_CLOSE_GRACE_MS: optInt("HANGUP_AFTER_CLOSE_GRACE_MS", 15000),

  VOICE_NAME_OVERRIDE: opt("VOICE_NAME_OVERRIDE", "Kore"),

  MB_DEFAULT_LANGUAGE: opt("MB_DEFAULT_LANGUAGE", "he"),
  MB_LANGUAGE_LOCK_ENABLED: optBool("MB_LANGUAGE_LOCK_ENABLED", true),
  MB_LANGUAGE_SWITCH_MIN_CONSECUTIVE_UTTERANCES: optInt("MB_LANGUAGE_SWITCH_MIN_CONSECUTIVE_UTTERANCES", 2),
  MB_USER_UTTERANCE_FLUSH_MS: optInt("MB_USER_UTTERANCE_FLUSH_MS", 700),
  MB_BOT_UTTERANCE_FLUSH_MS: optInt("MB_BOT_UTTERANCE_FLUSH_MS", 900),
  MB_MIN_STABLE_UTTERANCE_CHARS: optInt("MB_MIN_STABLE_UTTERANCE_CHARS", 4),
  MB_NUMERIC_CONTINUATION_GRACE_MS: optInt("MB_NUMERIC_CONTINUATION_GRACE_MS", 900),
  MB_LOW_LATENCY_MODE: optBool("MB_LOW_LATENCY_MODE", true),
};

module.exports = { env };
