"use strict";

const WebSocket = require("ws");
const { env } = require("../config/env");
const { logger } = require("../utils/logger");
const { ulaw8kB64ToPcm16kB64, pcm24kB64ToUlaw8kB64 } = require("./twilioGeminiAudio");
const { detectIntent } = require("../logic/intentRouter");
const { normalizeUtterance } = require("../logic/hebrewNlp");
const { extractCallerName } = require("../logic/nameExtractor");
const { finalizePipeline } = require("../stage4/finalizePipeline");
const { updateCallerDisplayName } = require("../memory/callerMemory");
const { startCallRecording, publicRecordingUrl, hangupCall } = require("../utils/twilioRecordings");
const { setRecordingForCall, waitForRecording, getRecordingForCall } = require("../utils/recordingRegistry");

// Optional (exists in your repo). We use it if present, but do not depend on it for core flow.
let passiveCallContext = null;
try {
  // eslint-disable-next-line global-require
  passiveCallContext = require("../logic/passiveCallContext");
} catch { /* ignore */ }

// -----------------------------------------------------------------------------
// Helpers (baseline-safe)
// -----------------------------------------------------------------------------

function normalizeModelName(m) {
  if (!m) return "";
  if (m.startsWith("models/")) return m;
  return `models/${m}`;
}

function liveWsUrl() {
  const key = env.GEMINI_API_KEY;
  if (!key) throw new Error("Missing GEMINI_API_KEY");
  return `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${encodeURIComponent(
    key
  )}`;
}

function safeStr(x) {
  if (x === undefined || x === null) return "";
  return String(x).trim();
}

function clampNum(n, min, max, fallback) {
  const v = Number(n);
  if (Number.isNaN(v)) return fallback;
  return Math.max(min, Math.min(max, v));
}

function isClosingUtterance(text) {
  const t = safeStr(text);
  if (!t) return false;

  // Hebrew closings
  if (/(תודה\s*ו?להתראות|להתראות|ביי|נתראה)/.test(t)) return true;

  // English closings
  const tl = t.toLowerCase();
  if (/(thank(s)?\b.*(bye|goodbye)|\bbye\b|\bgoodbye\b)/.test(tl)) return true;

  return false;
}

function applyTemplate(tpl, vars) {
  const s = safeStr(tpl);
  if (!s) return "";

  // Support lowercase placeholders too: {display_name} / {CALLER_NAME} / etc.
  return s.replace(/\{([A-Za-z0-9_]+)\}/g, (_, keyRaw) => {
    const key = String(keyRaw || "");
    if (!key) return "";
    const direct = vars?.[key];
    if (direct !== undefined && direct !== null) return String(direct);

    const upper = vars?.[key.toUpperCase()];
    if (upper !== undefined && upper !== null) return String(upper);

    const lower = vars?.[key.toLowerCase()];
    if (lower !== undefined && lower !== null) return String(lower);

    return "";
  });
}

function buildSettingsContext(settings) {
  const keys = Object.keys(settings || {}).sort();
  const lines = keys.map((k) => `${k}: ${safeStr(settings[k])}`);
  return lines.join("\n").trim();
}

function buildIntentsContext(intents) {
  const rows = Array.isArray(intents) ? intents.slice() : [];

  rows.sort((a, b) => {
    const pa = Number(a?.priority ?? 0);
    const pb = Number(b?.priority ?? 0);
    if (pb !== pa) return pb - pa;
    return String(a?.intent_id ?? "").localeCompare(String(b?.intent_id ?? ""));
  });

  const lines = rows.map((it) => {
    const id = safeStr(it.intent_id);
    const type = safeStr(it.intent_type);
    const pr = Number(it.priority ?? 0) || 0;
    const he = safeStr(it.triggers_he);
    const en = safeStr(it.triggers_en);
    const ru = safeStr(it.triggers_ru);
    return `- ${id} | type=${type} | priority=${pr} | triggers_he=${he} | triggers_en=${en} | triggers_ru=${ru}`;
  });

  return lines.join("\n").trim();
}

function buildSystemInstructionFromSSOT(ssot, runtimeMeta) {
  const settings = ssot?.settings || {};
  const prompts = ssot?.prompts || {};
  const intents = ssot?.intents || [];

  const defaultLang = safeStr(settings.DEFAULT_LANGUAGE) || "he";

  const sections = [];

  sections.push(
    [
      "IDENTITY (NON-NEGOTIABLE):",
      "- You are NOT a generic model and must NEVER describe yourself as an AI/LLM/model.",
      "- You are the business phone assistant defined by SETTINGS and PROMPTS.",
      "- Speak naturally and briefly.",
      "- Prefer Hebrew by default unless the caller requests otherwise.",
      "",
      "STRICT OUTPUT POLICY (CRITICAL):",
      "- NEVER output analysis, explanations, markdown, or meta-commentary.",
      "- DO NOT 'think out loud'.",
      "- When you speak, speak only the final user-facing sentence(s).",
      "- If you are asked to say a specific sentence verbatim, say it immediately and exactly, with no additions."
    ].join("\n")
  );

  // Caller memory policy: if we already have a caller name, do not re-ask it.
  const callerName = safeStr(runtimeMeta?.caller_name) || safeStr(runtimeMeta?.display_name) || "";
  if (callerName) {
    sections.push(
      [
        "CALLER MEMORY POLICY:",
        `- Known caller name: "${callerName}"`,
        "- Treat it as correct unless the caller explicitly corrects it.",
        "- Do NOT ask the caller for their name again."
      ].join("\n")
    );
  } else {
    sections.push(
      [
        "CALLER MEMORY POLICY:",
        "- If you do NOT know the caller name, you may ask for it once, politely.",
        "- If the caller says their name, accept it and do not ask again."
      ].join("\n")
    );
  }

  const master = safeStr(prompts.MASTER_PROMPT);
  const guardrails = safeStr(prompts.GUARDRAILS_PROMPT);
  const kb = safeStr(prompts.KB_PROMPT);
  const lead = safeStr(prompts.LEAD_CAPTURE_PROMPT);
  const intentRouter = safeStr(prompts.INTENT_ROUTER_PROMPT);

  if (master) sections.push(`MASTER_PROMPT:\n${master}`);
  if (guardrails) sections.push(`GUARDRAILS_PROMPT:\n${guardrails}`);
  if (kb) sections.push(`KB_PROMPT:\n${kb}`);
  if (lead) sections.push(`LEAD_CAPTURE_PROMPT:\n${lead}`);
  if (intentRouter) sections.push(`INTENT_ROUTER_PROMPT:\n${intentRouter}`);

  const settingsContext = buildSettingsContext(settings);
  if (settingsContext) sections.push(`SETTINGS_CONTEXT (SOURCE OF TRUTH):\n${settingsContext}`);

  const intentsContext = buildIntentsContext(intents);
  if (intentsContext) sections.push(`INTENTS_TABLE:\n${intentsContext}`);

  sections.push(
    [
      "LANGUAGE POLICY:",
      `- default_language=${defaultLang}`,
      "- If the caller speaks another supported language (he/en/ru), switch to it.",
      "- If the caller uses an unsupported language, apologize briefly and ask to continue in Hebrew/English/Russian."
    ].join("\n")
  );

  return sections.filter(Boolean).join("\n\n---\n\n").trim();
}

function computeGreetingHebrew(timeZone) {
  const tz = timeZone || "Asia/Jerusalem";

  const hourStr = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hour: "2-digit",
    hour12: false
  }).format(new Date());

  const hour = Number(hourStr);
  if (Number.isNaN(hour)) return "שלום";

  if (hour >= 5 && hour < 11) return "בוקר טוב";
  if (hour >= 11 && hour < 17) return "צהריים טובים";
  if (hour >= 17 && hour < 22) return "ערב טוב";
  return "לילה טוב";
}

function getOpeningScriptFromSSOT(ssot, vars) {
  const settings = ssot?.settings || {};
  // If the caller is recognized (returning caller), prefer OPENING_SCRIPT_RETURNING when available.
  const isReturning = Boolean(vars?.RETURNING_CALLER) || Boolean(vars?.returning_caller);
  const returningTpl = safeStr(settings.OPENING_SCRIPT_RETURNING);
  const defaultTpl = safeStr(settings.OPENING_SCRIPT);
  const tpl = (isReturning && returningTpl) ? returningTpl : (defaultTpl || "שלום! איך נוכל לעזור?");

  const merged = {
    BUSINESS_NAME: safeStr(settings.BUSINESS_NAME),
    BOT_NAME: safeStr(settings.BOT_NAME),
    CALLER_NAME: safeStr(vars?.CALLER_NAME),
    DISPLAY_NAME: safeStr(vars?.CALLER_NAME), // alias
    display_name: safeStr(vars?.CALLER_NAME), // alias (lowercase)
    MAIN_PHONE: safeStr(settings.MAIN_PHONE),
    BUSINESS_EMAIL: safeStr(settings.BUSINESS_EMAIL),
    BUSINESS_ADDRESS: safeStr(settings.BUSINESS_ADDRESS),
    WORKING_HOURS: safeStr(settings.WORKING_HOURS),
    BUSINESS_WEBSITE_URL: safeStr(settings.BUSINESS_WEBSITE_URL),
    VOICE_NAME: safeStr(settings.VOICE_NAME),
    GREETING: safeStr(vars?.GREETING),
    ...vars
  };

  const filled = applyTemplate(tpl, merged).trim();
  return filled || "שלום! איך נוכל לעזור?";
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeCallerId(caller) {
  const s = (caller || "").trim();
  const low = s.toLowerCase();
  if (!s) return { value: "", withheld: true };
  if (low === "anonymous" || low === "restricted" || low === "unavailable" || low === "unknown") {
    return { value: s, withheld: true };
  }
  const digits = s.replace(/\D/g, "");
  return { value: s, withheld: digits.length < 5 };
}

function isTruthyEnv(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "y";
}

// -----------------------------------------------------------------------------
// Webhook delivery (best-effort)
// -----------------------------------------------------------------------------

async function deliverWebhook(url, payload, label) {
  if (!url) return;
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    });
    logger.info("Webhook delivered", { label, status: resp.status });
  } catch (e) {
    logger.warn("Webhook delivery failed", { label, error: String(e) });
  }
}

// -----------------------------------------------------------------------------
// Session
// -----------------------------------------------------------------------------

class GeminiLiveSession {
  constructor({ onGeminiAudioUlaw8kBase64, onGeminiText, onTranscript, meta, ssot }) {
    this.onGeminiAudioUlaw8kBase64 = onGeminiAudioUlaw8kBase64;
    this.onGeminiText = onGeminiText;
    this.onTranscript = onTranscript;

    this.meta = meta || {};
    this.ssot = ssot || {};

    this.ws = null;
    this.ready = false;
    this.closed = false;
    this._greetingSent = false;

    // Transcript aggregation
    this._trBuf = { user: "", bot: "" };
    this._trLastChunk = { user: "", bot: "" };
    this._trTimer = { user: null, bot: null };

    // Call state (conversation log + metadata). Lead extraction is POST-CALL via LLM.
    const callerInfo = normalizeCallerId(this.meta?.caller || "");

    this._call = {
      callSid: safeStr(this.meta?.callSid),
      streamSid: safeStr(this.meta?.streamSid),
      source: safeStr(this.meta?.source) || "VoiceBot_Blank",
      caller_raw: callerInfo.value,
      caller_withheld: callerInfo.withheld,
      called: safeStr(this.meta?.called),
      started_at: nowIso(),
      ended_at: null,

      // Conversation log (GilSport-style)
      // { role: 'user'|'assistant', text: string, ts: iso }
      conversationLog: [],

      // recording (best-effort)
      recording_sid: "",

      finalized: false
    };

    // Optional passive context aggregator (best-effort)
    this._passiveCtx = null;
    try {
      if (passiveCallContext?.createPassiveCallContext) {
        this._passiveCtx = passiveCallContext.createPassiveCallContext({
          callSid: this._call.callSid,
          streamSid: this._call.streamSid,
          caller: this._call.caller_raw,
          called: this._call.called,
          source: this._call.source,
          caller_profile: this.meta?.caller_profile || null
        });
      }
    } catch { /* ignore */ }

    // Canonical: after CLOSING is fully spoken, we initiate hangup from our side.
    this._hangupScheduled = false;
  }

  start() {
    if (this.ws) return;

    const url = liveWsUrl();
    this.ws = new WebSocket(url);

    this.ws.on("open", async () => {
      logger.info("Gemini Live WS connected", this.meta);

      // Recording: start best-effort (must NOT affect voice)
      try {
        const r = await startCallRecording(this._call.callSid, logger);
        if (r?.ok && r.recordingSid) {
          this._call.recording_sid = String(r.recordingSid);
          setRecordingForCall(this._call.callSid, { recordingSid: this._call.recording_sid });
          logger.info("Recording started + stored in registry", { callSid: this._call.callSid, recordingSid: this._call.recording_sid });
        }
      } catch (e) {
        logger.warn("startCallRecording failed", { err: String(e) });
      }

      const callerProfile = this.meta?.caller_profile || null;
      const callerName = safeStr(callerProfile?.display_name) || "";
      const systemText = buildSystemInstructionFromSSOT(this.ssot, {
        caller_name: callerName,
        display_name: callerName
      });

      // VAD tuning (keep ENV names locked; clamp to safe ranges)
      const vadPrefix = clampNum(env.MB_VAD_PREFIX_MS ?? 120, 50, 600, 120);
      const vadSilence = clampNum(env.MB_VAD_SILENCE_MS ?? 450, 200, 1500, 450);

      const setup = {
        setup: {
          model: normalizeModelName(env.GEMINI_LIVE_MODEL),
          systemInstruction: systemText ? { parts: [{ text: systemText }] } : undefined,

          generationConfig: {
            responseModalities: ["AUDIO"],
            // keep concise and fast
            temperature: 0.2,
            speechConfig: {
              voiceConfig: {
                prebuiltVoiceConfig: {
                  voiceName: env.VOICE_NAME_OVERRIDE || safeStr(this.ssot?.settings?.VOICE_NAME) || "Kore"
                }
              }
            }
          },

          realtimeInputConfig: {
            automaticActivityDetection: {
              prefixPaddingMs: vadPrefix,
              silenceDurationMs: vadSilence
            }
          },

          ...(env.MB_LOG_TRANSCRIPTS ? { inputAudioTranscription: {}, outputAudioTranscription: {} } : {})
        }
      };

      try {
        this.ws.send(JSON.stringify(setup));
        this.ready = true;
      } catch (e) {
        logger.error("Failed to send Gemini setup", { ...this.meta, error: e.message });
      }
    });

    this.ws.on("message", (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString("utf8"));
      } catch {
        return;
      }

      // Send proactive opening exactly once, as early as possible.
      // We send it on setupComplete if present; otherwise we will send it once we see any serverContent.
      if ((msg?.setupComplete || msg?.serverContent) && !this._greetingSent) {
        this._greetingSent = true;
        this._sendProactiveOpening();
        // do not return; we still want to process audio in same frame if exists
      }

      // AUDIO from Gemini -> Twilio
      try {
        const parts =
          msg?.serverContent?.modelTurn?.parts ||
          msg?.serverContent?.turn?.parts ||
          msg?.serverContent?.parts ||
          [];

        for (const p of parts) {
          const inline = p?.inlineData;
          if (!inline || !inline?.data || !inline?.mimeType) continue;

          if (String(inline.mimeType).startsWith("audio/pcm")) {
            const ulawB64 = pcm24kB64ToUlaw8kB64(inline.data);
            if (ulawB64 && this.onGeminiAudioUlaw8kBase64) {
              this.onGeminiAudioUlaw8kBase64(ulawB64);
            }
          }
        }
      } catch (e) {
        logger.debug("Gemini message parse error", { ...this.meta, error: e.message });
      }

      // Optional text parts (debug only; should be empty with strict policy, but we keep it)
      try {
        const parts =
          msg?.serverContent?.modelTurn?.parts ||
          msg?.serverContent?.turn?.parts ||
          msg?.serverContent?.parts ||
          [];

        for (const p of parts) {
          const t = p?.text;
          if (t && this.onGeminiText) this.onGeminiText(String(t));
        }
      } catch { /* ignore */ }

      // Transcriptions (aggregated)
      try {
        const inTr = msg?.serverContent?.inputTranscription?.text;
        if (inTr) this._onTranscriptChunk("user", String(inTr));

        const outTr = msg?.serverContent?.outputTranscription?.text;
        if (outTr) this._onTranscriptChunk("bot", String(outTr));
      } catch { /* ignore */ }
    });

    this.ws.on("close", async (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString("utf8") : "";
      this.closed = true;
      this.ready = false;

      this._flushTranscript("user");
      this._flushTranscript("bot");

      logger.info("Gemini Live WS closed", { ...this.meta, code, reason });

      // Finalize: always best-effort, never throws outward
      await this._finalizeOnce("gemini_ws_close");
    });

    this.ws.on("error", (err) => {
      logger.error("Gemini Live WS error", { ...this.meta, error: err.message });
    });
  }

  _onTranscriptChunk(who, chunk) {
    if (!env.MB_LOG_TRANSCRIPTS) return;

    const c = chunk || "";
    if (!c) return;

    if (c === this._trLastChunk[who]) return;
    this._trLastChunk[who] = c;

    this._trBuf[who] = (this._trBuf[who] + c).slice(-800);

    if (this._trTimer[who]) clearTimeout(this._trTimer[who]);
    this._trTimer[who] = setTimeout(() => this._flushTranscript(who), 350);
  }

  _flushTranscript(who) {
    if (!env.MB_LOG_TRANSCRIPTS) return;

    if (this._trTimer[who]) {
      clearTimeout(this._trTimer[who]);
      this._trTimer[who] = null;
    }

    const text = (this._trBuf[who] || "").trim();
    this._trBuf[who] = "";
    if (!text) return;

    const nlp = normalizeUtterance(text);

    // Append to conversation log (GilSport-style)
    try {
      const role = who === "user" ? "user" : "assistant";
      this._call.conversationLog.push({ role, text: nlp.raw, ts: nowIso() });
    } catch { /* ignore */ }

    // Passive context aggregation (best-effort)
    try {
      if (this._passiveCtx && passiveCallContext?.appendUtterance) {
        passiveCallContext.appendUtterance(this._passiveCtx, {
          role: who === "user" ? "user" : "assistant",
          text: nlp.raw,
          normalized: nlp.normalized,
          lang: nlp.lang
        });
      }
    } catch { /* ignore */ }

    logger.info(`UTTERANCE ${who}`, {
      ...this.meta,
      text: nlp.raw,
      normalized: nlp.normalized,
      lang: nlp.lang
    });

    // Deterministic caller name capture (runs on every user utterance).
    // Does NOT depend on the opening; only persists on high-confidence patterns.
    if (who === "user") {
      try {
        const callerId = safeStr(this.meta?.caller) || "";
        if (callerId) {
          // Find the last assistant utterance BEFORE this user turn (conservative).
          let lastBot = "";
          try {
            const logArr = Array.isArray(this._call?.conversationLog) ? this._call.conversationLog : [];
            for (let i = logArr.length - 2; i >= 0; i--) {
              const it = logArr[i];
              if (it && it.role === "assistant" && it.text) { lastBot = String(it.text); break; }
            }
          } catch { /* ignore */ }

          const found = extractCallerName({ userText: nlp.raw, lastBotUtterance: lastBot });
          if (found && found.name) {
            // Minimal normalization (super conservative)
            let normalizedName = String(found.name).trim();
            if (normalizedName === "שאי") normalizedName = "שי"; // fix known STT confusion

            const existing = safeStr(this.meta?.caller_profile?.display_name) || "";
            if (!existing || existing !== normalizedName) {
              // Best-effort DB write, must never block call flow.
              updateCallerDisplayName(callerId, normalizedName).catch(() => {});
              // Update in-memory immediately for same-call usage.
              if (!this.meta.caller_profile) this.meta.caller_profile = {};
              this.meta.caller_profile.display_name = normalizedName;

              logger.info("CALLER_NAME_CAPTURED", {
                ...this.meta,
                caller: callerId,
                name: normalizedName,
                confidence_reason: found.reason,
                source_utterance: nlp.raw
              });
            }
          }
        }
      } catch { /* swallow */ }
    }

    // Canonical: after the closing is spoken, initiate a proactive hangup.
    // We only do this once per call. Delay is ENV-controlled to avoid cutting the audio.
    if (
      who === "bot" &&
      env.FORCE_HANGUP_AFTER_CLOSE &&
      !this._hangupScheduled &&
      isClosingUtterance(nlp.raw)
    ) {
      const callSid = safeStr(this._call?.callSid) || safeStr(this.meta?.callSid);
      if (callSid) {
        this._hangupScheduled = true;
        const graceMs = Math.max(15000, Number(env.HANGUP_AFTER_CLOSE_GRACE_MS || 15000));
        setTimeout(() => {
          hangupCall(callSid, logger).catch(() => {});
        }, graceMs);
        logger.info("Proactive hangup scheduled", { ...this.meta, callSid, delay_ms: graceMs });
      }
    }

    if (who === "user") {
      const intent = detectIntent({
        text: nlp.normalized || nlp.raw,
        intents: this.ssot?.intents || []
      });

      logger.info("INTENT_DETECTED", {
        ...this.meta,
        text: nlp.raw,
        normalized: nlp.normalized,
        lang: nlp.lang,
        intent
      });
    }

    // NOTE: deterministic lead capture removed by design.
    // Lead extraction happens post-call via SSOT LEAD_PARSER_PROMPT.

    if (this.onTranscript) {
      this.onTranscript({ who, text: nlp.raw, normalized: nlp.normalized, lang: nlp.lang });
    }
  }

  _sendProactiveOpening() {
    if (!this.ws || this.closed || !this.ready) return;

    const tz = env.TIME_ZONE || "Asia/Jerusalem";
    const greeting = computeGreetingHebrew(tz);

    const callerProfile = this.meta?.caller_profile || null;
    let callerName = safeStr(callerProfile?.display_name) || "";
    if (callerName === "שאי") callerName = "שי";

    // total_calls is the number of *previously completed* calls we have stored.
    // We want the 2nd call (total_calls >= 1) to already be treated as "returning".
    const totalCalls = Number(callerProfile?.total_calls ?? 0);
    const isReturning = totalCalls > 0;

    let opening = getOpeningScriptFromSSOT(this.ssot, {
      GREETING: greeting,
      CALLER_NAME: callerName,
      DISPLAY_NAME: callerName,
      display_name: callerName,
      returning_caller: isReturning,
      RETURNING_CALLER: isReturning
    });

    // Conservative cleanup (do not rewrite authored scripts)
    opening = String(opening)
      .replace(/\s{2,}/g, " ")
      .replace(/\s+,/g, ",")
      .replace(/,\s+,/g, ",")
      .trim();

    // Critical: keep kickoff ultra-short to avoid the model "thinking out loud".
    const userKickoff =
      `אמרי עכשיו בדיוק את המשפט הבא בלבד, מילה במילה, בלי שום תוספת, ואז עצרי להקשבה:\n` +
      opening;

    const msg = {
      clientContent: {
        turns: [{ role: "user", parts: [{ text: userKickoff }] }],
        turnComplete: true
      }
    };

    try {
      this.ws.send(JSON.stringify(msg));
      logger.info("Proactive opening sent", { ...this.meta, greeting, opening_len: opening.length });
    } catch (e) {
      logger.debug("Failed sending proactive opening", { ...this.meta, error: e.message });
    }
  }

  sendUlaw8kFromTwilio(ulaw8kB64) {
    if (!this.ws || this.closed || !this.ready) return;

    const pcm16kB64 = ulaw8kB64ToPcm16kB64(ulaw8kB64);

    const msg = {
      realtimeInput: {
        mediaChunks: [{ mimeType: "audio/pcm;rate=16000", data: pcm16kB64 }]
      }
    };

    try {
      this.ws.send(JSON.stringify(msg));
    } catch (e) {
      logger.debug("Failed sending audio to Gemini", { ...this.meta, error: e.message });
    }
  }

  endInput() {
    if (!this.ws || this.closed) return;
    try {
      this.ws.send(JSON.stringify({ realtimeInput: { audioStreamEnd: true } }));
    } catch { /* ignore */ }
  }

  async _finalizeOnce(reason) {
    if (this._call.finalized) return;
    this._call.finalized = true;

    try {
      this._call.ended_at = nowIso();
      const durationMs = Date.now() - new Date(this._call.started_at).getTime();

      const callMeta = {
        callSid: this._call.callSid,
        streamSid: this._call.streamSid,
        caller: this._call.caller_raw,
        called: this._call.called,
        source: this._call.source,
        started_at: this._call.started_at,
        ended_at: this._call.ended_at,
        duration_ms: durationMs,
        caller_withheld: this._call.caller_withheld,
        finalize_reason: reason || ""
      };

      // optional passive context (non-breaking)
      if (this._passiveCtx && passiveCallContext?.finalizeCtx) {
        try {
          callMeta.passive_context = passiveCallContext.finalizeCtx(this._passiveCtx);
        } catch { /* ignore */ }
      } else if (passiveCallContext?.buildPassiveContext) {
        // fallback to legacy build
        try {
          callMeta.passive_context = passiveCallContext.buildPassiveContext({
            meta: this.meta,
            ssot: this.ssot
          });
        } catch { /* ignore */ }
      }

      const snapshot = {
        call: callMeta,
        conversationLog: this._call.conversationLog || []
      };

      await finalizePipeline({
        snapshot,
        ssot: this.ssot,
        env,
        logger,
        senders: {
          sendCallLog: (payload) => deliverWebhook(env.CALL_LOG_WEBHOOK_URL, payload, "CALL_LOG"),
          sendFinal: (payload) => deliverWebhook(env.FINAL_WEBHOOK_URL, payload, "FINAL"),
          sendAbandoned: (payload) => deliverWebhook(env.ABANDONED_WEBHOOK_URL, payload, "ABANDONED"),
          resolveRecording: async () => {
            if (!isTruthyEnv(env.MB_ENABLE_RECORDING)) {
              return { recording_provider: null, recording_sid: null, recording_url_public: null };
            }

            // Wait a bit for Twilio callback to arrive (best-effort)
            await waitForRecording(this._call.callSid, 12000);
            const rec = getRecordingForCall(this._call.callSid);
            const sid = safeStr(rec?.recordingSid || this._call.recording_sid) || null;
            const url = sid ? publicRecordingUrl(sid) : null;

            // cache for later (best-effort)
            if (sid) this._call.recording_sid = sid;

            return {
              recording_provider: sid ? "twilio" : null,
              recording_sid: sid,
              recording_url_public: url,
            };
          },
        }
      });
    } catch (e) {
      logger.warn("Finalize failed", { error: String(e) });
    }
  }

  stop() {
    // Finalize (best-effort), then close Gemini WS.
    this._finalizeOnce("stop_called").catch(() => {});

    if (!this.ws) return;
    try {
      this.ws.close();
    } catch { /* ignore */ }
  }
}

module.exports = { GeminiLiveSession };
