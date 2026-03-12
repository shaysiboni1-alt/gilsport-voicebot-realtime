"use strict";

const WebSocket = require("ws");
const { env } = require("../config/env");
const { logger } = require("../utils/logger");
const {
  ulaw8kB64ToPcm16kB64,
  pcm24kB64ToUlaw8kB64,
} = require("./twilioGeminiAudio");
const { detectIntent } = require("../logic/intentRouter");
const {
  normalizeUtterance,
  detectExplicitLanguageSwitch,
} = require("../logic/hebrewNlp");
const { extractCallerName } = require("../logic/nameExtractor");
const { finalizePipeline } = require("../stage4/finalizePipeline");
const { updateCallerDisplayName } = require("../memory/callerMemory");
const {
  startCallRecording,
  publicRecordingUrl,
  hangupCall,
} = require("../utils/twilioRecordings");
const {
  setRecordingForCall,
  waitForRecording,
  getRecordingForCall,
} = require("../utils/recordingRegistry");
const { getCachedOpening } = require("../logic/openingBuilder");

let passiveCallContext = null;
try {
  passiveCallContext = require("../logic/passiveCallContext");
} catch {
  passiveCallContext = null;
}

function normalizeModelName(m) {
  if (!m) return "";
  return m.startsWith("models/") ? m : `models/${m}`;
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

function nowIso() {
  return new Date().toISOString();
}

function clampNum(n, min, max, fallback) {
  const v = Number(n);
  if (Number.isNaN(v)) return fallback;
  return Math.max(min, Math.min(max, v));
}

function normalizeCallerId(caller) {
  const s = (caller || "").trim();
  const low = s.toLowerCase();

  if (!s) return { value: "", withheld: true };

  if (
    ["anonymous", "restricted", "unavailable", "unknown", "private", "withheld"].includes(
      low
    )
  ) {
    return { value: s, withheld: true };
  }

  const digits = s.replace(/\D/g, "");
  return { value: s, withheld: digits.length < 5 };
}

function isTruthyEnv(v) {
  const s = String(v ?? "").trim().toLowerCase();
  return s === "true" || s === "1" || s === "yes" || s === "y";
}

function isClosingUtterance(text) {
  const t = safeStr(text);
  if (!t) return false;
  if (/(תודה\s*ו?להתראות|להתראות|ביי|נתראה|יום טוב|המשך יום נעים|אשמח לעזור שוב)/.test(t)) return true;
  if (/(спасибо.*до свидания|до свидания|пока)/i.test(t)) return true;
  if (/(thank(s)?\b.*(bye|goodbye)|\bbye\b|\bgoodbye\b)/i.test(t)) return true;
  return false;
}

function buildSettingsContext(settings) {
  const keys = Object.keys(settings || {}).sort();
  return keys.map((k) => `${k}: ${safeStr(settings[k])}`).join("\n").trim();
}

function buildIntentsContext(intents) {
  const rows = Array.isArray(intents) ? intents.slice() : [];
  rows.sort((a, b) => {
    const pa = Number(a?.priority ?? 0);
    const pb = Number(b?.priority ?? 0);
    if (pb !== pa) return pb - pa;
    return String(a?.intent_id ?? "").localeCompare(String(a?.intent_id ?? ""));
  });

  return rows
    .map(
      (it) =>
        `- ${safeStr(it.intent_id)} | type=${safeStr(it.intent_type)} | priority=${Number(it.priority ?? 0) || 0} | triggers_he=${safeStr(it.triggers_he)} | triggers_en=${safeStr(it.triggers_en)} | triggers_ru=${safeStr(it.triggers_ru)}`
    )
    .join("\n")
    .trim();
}

function buildSystemInstructionFromSSOT(ssot, runtimeMeta) {
  const settings = ssot?.settings || {};
  const prompts = ssot?.prompts || {};
  const intents = ssot?.intents || [];

  const defaultLang =
    safeStr(runtimeMeta?.language_locked) ||
    safeStr(settings.DEFAULT_LANGUAGE) ||
    "he";
  const callerName =
    safeStr(runtimeMeta?.caller_name) ||
    safeStr(runtimeMeta?.display_name) ||
    "";
  const callerWithheld = !!runtimeMeta?.caller_withheld;

  const sections = [];

  sections.push([
    "IDENTITY (NON-NEGOTIABLE):",
    "- You are the business phone assistant defined by SETTINGS and PROMPTS.",
    "- Never identify as an AI, model, assistant model, or LLM.",
    "- Speak briefly, naturally, and only as a customer-facing phone representative.",
    "- NEVER output analysis, internal planning, reasoning, markdown, bullets, JSON, stage labels, or notes.",
    "- NEVER say things like 'I understand', 'I will', 'I'm now', 'I've processed', 'composing', 'confirming', or any meta explanation.",
    "- Output ONLY the final customer-facing sentence(s) to be spoken aloud.",
    "- If you are about to say anything meta, stop and instead say the customer-facing sentence only.",
  ].join("\n"));

  sections.push([
    "LANGUAGE POLICY (HARD RULE):",
    `- locked_language=${defaultLang}`,
    "- Start and stay in Hebrew by default.",
    "- Do NOT switch language because of accent, pronunciation, or a foreign-sounding name.",
    "- Switch language only if the caller explicitly asks to switch, or clearly speaks in a supported language for multiple turns.",
    "- If in doubt, remain in Hebrew.",
  ].join("\n"));

  sections.push([
    "DIALOG POLICY (HARD RULE):",
    "- Ask only ONE question at a time.",
    "- Never bundle multiple data-collection questions into one turn.",
    "- Prefer short, focused follow-up questions.",
    "- If the caller corrects you, apologize briefly, correct course, and continue naturally.",
    "- If the caller says something like 'אני אישה' or 'אני בת', do NOT treat it as a name.",
    "- If the caller corrects gender/name confusion, acknowledge briefly and then ask for the name again only if needed for the request.",
    "- If the call is only for information, answer briefly and do not force lead capture.",
    "- If the caller confirms callback to the identified number, immediately acknowledge, close politely, and end the flow.",
  ].join("\n"));

  if (callerName) {
    sections.push([
      "CALLER MEMORY POLICY:",
      `- Known caller name: "${callerName}"`,
      "- Treat it as correct unless the caller explicitly corrects it.",
      "- Do not ask for the caller name again if it is already known.",
    ].join("\n"));
  }

  if (callerWithheld) {
    sections.push([
      "WITHHELD NUMBER POLICY:",
      "- The caller number is withheld/private.",
      "- If the caller leaves a request or asks for a callback, you MUST collect a callback number explicitly.",
      "- Do not say you will return to the identified number because there is no usable caller ID.",
    ].join("\n"));
  }

  if (prompts.MASTER_PROMPT) {
    sections.push(`MASTER_PROMPT:\n${safeStr(prompts.MASTER_PROMPT)}`);
  }
  if (prompts.GUARDRAILS_PROMPT) {
    sections.push(`GUARDRAILS_PROMPT:\n${safeStr(prompts.GUARDRAILS_PROMPT)}`);
  }
  if (prompts.KB_PROMPT) {
    sections.push(`KB_PROMPT:\n${safeStr(prompts.KB_PROMPT)}`);
  }
  if (prompts.LEAD_CAPTURE_PROMPT) {
    sections.push(`LEAD_CAPTURE_PROMPT:\n${safeStr(prompts.LEAD_CAPTURE_PROMPT)}`);
  }
  if (prompts.INTENT_ROUTER_PROMPT) {
    sections.push(`INTENT_ROUTER_PROMPT:\n${safeStr(prompts.INTENT_ROUTER_PROMPT)}`);
  }

  const settingsContext = buildSettingsContext(settings);
  if (settingsContext) sections.push(`SETTINGS_CONTEXT:\n${settingsContext}`);

  const intentsContext = buildIntentsContext(intents);
  if (intentsContext) sections.push(`INTENTS_TABLE:\n${intentsContext}`);

  return sections.filter(Boolean).join("\n\n---\n\n").trim();
}

function looksLikeReasoningText(text) {
  const t = safeStr(text);
  if (!t) return false;
  return (
    /\*\*.+\*\*/.test(t) ||
    /\b(Composing the Response|Confirming|Implementing|Addressing|Gathering|Finalizing|Prioritizing|Initiating|Acknowledge|Pinpointing|Reasoning|I(?:'| a)m now|I've|I have successfully|I will now|The user is asking|triggering the|based on the context|SETTINGS_CONTEXT|OPENING_SCRIPT|INTENT_ROUTER_PROMPT|LEAD_CAPTURE_PROMPT)\b/i.test(
      t
    )
  );
}

function scrubReasoningText(text) {
  if (!looksLikeReasoningText(text)) return safeStr(text);
  const quoted = safeStr(text).match(/["“](.+?)["”]/);
  if (quoted && quoted[1] && !looksLikeReasoningText(quoted[1])) {
    return quoted[1].trim();
  }
  return "";
}

function isAffirmativeUtterance(text) {
  const t = safeStr(text);
  if (!t) return false;
  return /^(אה,\s*)?(כן([.!?,\s]|$)|נכון([.!?,\s]|$)|אוקיי([.!?,\s]|$)|אוקי([.!?,\s]|$)|בסדר([.!?,\s]|$)|בטח([.!?,\s]|$)|יאללה([.!?,\s]|$))+/u.test(
    t
  );
}

async function deliverWebhook(url, payload, label) {
  if (!url) return;
  try {
    const resp = await fetch(url, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload),
    });
    logger.info("Webhook delivered", { label, status: resp.status });
  } catch (e) {
    logger.warn("Webhook delivery failed", { label, error: String(e) });
  }
}

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
    this._hangupScheduled = false;
    this._awaitingCallbackConfirmation = false;
    this._closingSentAfterCallback = false;

    this._langState = {
      lockedLanguage: safeStr(env.MB_DEFAULT_LANGUAGE) || "he",
      candidateLanguage: null,
      candidateHits: 0,
      minConsecutive: Math.max(
        2,
        Number(env.MB_LANGUAGE_SWITCH_MIN_CONSECUTIVE_UTTERANCES || 2)
      ),
    };

    this._trBuf = {
      user: { text: "", timer: null, lastChunk: "", lastTs: 0 },
      bot: { text: "", timer: null, lastChunk: "", lastTs: 0 },
    };

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
      conversationLog: [],
      recording_sid: "",
      finalized: false,
    };

    this._passiveCtx = null;
    try {
      if (passiveCallContext?.createPassiveCallContext) {
        this._passiveCtx = passiveCallContext.createPassiveCallContext({
          callSid: this._call.callSid,
          streamSid: this._call.streamSid,
          caller: this._call.caller_raw,
          called: this._call.called,
          source: this._call.source,
          caller_profile: this.meta?.caller_profile || null,
        });
      }
    } catch {}
  }

  start() {
    if (this.ws) return;

    this.ws = new WebSocket(liveWsUrl());

    this.ws.on("open", async () => {
      logger.info("Gemini Live WS connected", this.meta);

      try {
        const r = await startCallRecording(this._call.callSid, logger);
        if (r?.ok && r.recordingSid) {
          this._call.recording_sid = String(r.recordingSid);
          setRecordingForCall(this._call.callSid, {
            recordingSid: this._call.recording_sid,
          });
          logger.info("Recording started + stored in registry", {
            callSid: this._call.callSid,
            recordingSid: this._call.recording_sid,
          });
        }
      } catch (e) {
        logger.warn("startCallRecording failed", { err: String(e) });
      }

      const callerProfile = this.meta?.caller_profile || null;
      const callerName = safeStr(callerProfile?.display_name) || "";

      const systemText = buildSystemInstructionFromSSOT(this.ssot, {
        caller_name: callerName,
        display_name: callerName,
        language_locked: this._langState.lockedLanguage,
        caller_withheld: this._call.caller_withheld,
      });

      const vadPrefix = clampNum(env.MB_VAD_PREFIX_MS ?? 40, 20, 600, 40);
      const vadSilence = clampNum(env.MB_VAD_SILENCE_MS ?? 120, 80, 1500, 120);

      const setup = {
        setup: {
          model: normalizeModelName(env.GEMINI_LIVE_MODEL),
          systemInstruction: systemText
            ? { parts: [{ text: systemText }] }
            : undefined,
          generationConfig: {
            responseModalities: ["AUDIO"],
            temperature: 0.1,
            speechConfig: {
              voiceConfig: {
                prebuiltVoiceConfig: {
                  voiceName:
                    env.VOICE_NAME_OVERRIDE ||
                    safeStr(this.ssot?.settings?.VOICE_NAME) ||
                    "Kore",
                },
              },
            },
          },
          realtimeInputConfig: {
            automaticActivityDetection: {
              prefixPaddingMs: vadPrefix,
              silenceDurationMs: vadSilence,
            },
          },
          ...(env.MB_LOG_TRANSCRIPTS ? { inputAudioTranscription: {} } : {}),
        },
      };

      try {
        this.ws.send(JSON.stringify(setup));
        this.ready = true;
      } catch (e) {
        logger.error("Failed to send Gemini setup", {
          ...this.meta,
          error: e.message,
        });
      }
    });

    this.ws.on("message", (data) => {
      let msg;
      try {
        msg = JSON.parse(data.toString("utf8"));
      } catch {
        return;
      }

      if ((msg?.setupComplete || msg?.serverContent) && !this._greetingSent) {
        this._greetingSent = true;
        this._sendProactiveOpening();
      }

      try {
        const parts =
          msg?.serverContent?.modelTurn?.parts ||
          msg?.serverContent?.turn?.parts ||
          msg?.serverContent?.parts ||
          [];

        for (const p of parts) {
          const inline = p?.inlineData;
          if (
            inline?.data &&
            String(inline?.mimeType || "").startsWith("audio/pcm")
          ) {
            const ulawB64 = pcm24kB64ToUlaw8kB64(inline.data);
            if (ulawB64 && this.onGeminiAudioUlaw8kBase64) {
              this.onGeminiAudioUlaw8kBase64(ulawB64);
            }
          }

          if (p?.text) {
            const cleaned = scrubReasoningText(String(p.text));
            if (cleaned && this.onGeminiText) this.onGeminiText(cleaned);
            if (cleaned && env.MB_LOG_TRANSCRIPTS) {
              this._onTranscriptChunk("bot", cleaned);
            }
          }
        }
      } catch (e) {
        logger.debug("Gemini message parse error", {
          ...this.meta,
          error: e.message,
        });
      }

      try {
        const inTr = msg?.serverContent?.inputTranscription?.text;
        if (inTr) this._onTranscriptChunk("user", String(inTr));

        const outTr = msg?.serverContent?.outputTranscription?.text;
        const cleanedOut = scrubReasoningText(String(outTr || ""));
        if (cleanedOut) this._onTranscriptChunk("bot", cleanedOut);
      } catch {}
    });

    this.ws.on("close", async (code, reasonBuf) => {
      const reason = reasonBuf ? reasonBuf.toString("utf8") : "";
      this.closed = true;
      this.ready = false;

      this._flushTranscript("user");
      this._flushTranscript("bot");

      logger.info("Gemini Live WS closed", { ...this.meta, code, reason });

      await this._finalizeOnce("gemini_ws_close");
    });

    this.ws.on("error", (err) => {
      logger.error("Gemini Live WS error", {
        ...this.meta,
        error: err.message,
      });
    });
  }

  _scheduleFlush(who) {
    const holder = this._trBuf[who];
    if (holder.timer) clearTimeout(holder.timer);
    holder.timer = setTimeout(() => this._flushTranscript(who), who === "user" ? 220 : 260);
  }

  _onTranscriptChunk(who, chunk) {
    if (!env.MB_LOG_TRANSCRIPTS) return;

    const c = safeStr(chunk);
    if (!c) return;
    if (who === "bot" && looksLikeReasoningText(c)) return;

    const holder = this._trBuf[who];
    if (holder.lastChunk === c) return;

    holder.lastChunk = c;
    holder.lastTs = Date.now();

    if (!holder.text) {
      holder.text = c;
    } else if (holder.text.endsWith(c)) {
      // noop
    } else if (c.startsWith(holder.text)) {
      holder.text = c;
    } else {
      holder.text = `${holder.text} ${c}`.replace(/\s{2,}/g, " ");
    }

    this._scheduleFlush(who);
  }

  _applyLanguageDecision(nlp) {
    const explicitSwitch = detectExplicitLanguageSwitch(
      nlp.raw || nlp.normalized || ""
    );

    if (explicitSwitch) {
      this._langState.lockedLanguage = explicitSwitch;
      this._langState.candidateLanguage = null;
      this._langState.candidateHits = 0;
    } else if (
      nlp.lang &&
      nlp.lang !== "unknown" &&
      nlp.lang !== this._langState.lockedLanguage
    ) {
      if (nlp.lang === this._langState.candidateLanguage) {
        this._langState.candidateHits += 1;
      } else {
        this._langState.candidateLanguage = nlp.lang;
        this._langState.candidateHits = 1;
      }

      if (this._langState.candidateHits >= this._langState.minConsecutive) {
        this._langState.lockedLanguage = this._langState.candidateLanguage;
        this._langState.candidateLanguage = null;
        this._langState.candidateHits = 0;
      }
    } else {
      this._langState.candidateLanguage = null;
      this._langState.candidateHits = 0;
    }

    logger.info("LANGUAGE_DECISION", {
      ...this.meta,
      observed_lang: nlp.lang,
      observed_confidence: nlp.lang_confidence,
      explicit_switch: explicitSwitch,
      locked_language: this._langState.lockedLanguage,
      candidate_language: this._langState.candidateLanguage,
      candidate_hits: this._langState.candidateHits,
    });
  }

  _sendImmediateCallbackClosing() {
    if (!this.ws || this.closed || !this.ready) return;
    if (this._closingSentAfterCallback) return;

    this._closingSentAfterCallback = true;

    const msg = {
      clientContent: {
        turns: [
          {
            role: "user",
            parts: [
              {
                text:
                  "הלקוח אישר לחזור למספר. אמרי עכשיו משפט סיום קצר בעברית, אשרי שהפנייה נרשמה ושהמשרד יחזור, בלי שאלות נוספות ובלי שום טקסט נוסף.",
              },
            ],
          },
        ],
        turnComplete: true,
      },
    };

    try {
      this.ws.send(JSON.stringify(msg));
    } catch (e) {
      logger.debug("Failed sending immediate callback closing", {
        ...this.meta,
        error: e.message,
      });
    }
  }

  _flushTranscript(who) {
    if (!env.MB_LOG_TRANSCRIPTS) return;

    const holder = this._trBuf[who];
    if (holder.timer) {
      clearTimeout(holder.timer);
      holder.timer = null;
    }

    const text = safeStr(holder.text);
    holder.text = "";
    if (!text) return;

    const nlp = normalizeUtterance(text);
    if (who === "bot" && looksLikeReasoningText(nlp.raw || nlp.normalized)) {
      return;
    }

    const role = who === "user" ? "user" : "assistant";

    this._call.conversationLog.push({
      role,
      text: nlp.normalized || nlp.raw,
      ts: nowIso(),
    });

    try {
      if (this._passiveCtx && passiveCallContext?.appendUtterance) {
        passiveCallContext.appendUtterance(this._passiveCtx, {
          role,
          text: nlp.raw,
          normalized: nlp.normalized,
          lang: nlp.lang,
        });
      }
    } catch {}

    if (who === "user") {
      this._applyLanguageDecision(nlp);
    }

    logger.info(`UTTERANCE ${who}`, {
      ...this.meta,
      text: nlp.raw,
      normalized: nlp.normalized,
      lang: nlp.lang,
      language_locked: this._langState.lockedLanguage,
      lang_confidence: nlp.lang_confidence,
    });

    if (who === "user") {
      try {
        const callerId = safeStr(this.meta?.caller) || "";
        if (callerId) {
          let lastBot = "";
          const logArr = Array.isArray(this._call?.conversationLog)
            ? this._call.conversationLog
            : [];

          for (let i = logArr.length - 2; i >= 0; i -= 1) {
            const it = logArr[i];
            if (it?.role === "assistant" && it.text) {
              lastBot = String(it.text);
              break;
            }
          }

          const found = extractCallerName({
            userText: nlp.normalized || nlp.raw,
            lastBotUtterance: lastBot,
          });

          if (found?.name) {
            const normalizedName =
              String(found.name).trim() === "שאי"
                ? "שי"
                : String(found.name).trim();

            const existing = safeStr(this.meta?.caller_profile?.display_name) || "";

            if (!existing || existing !== normalizedName) {
              updateCallerDisplayName(callerId, normalizedName).catch(() => {});
              if (!this.meta.caller_profile) this.meta.caller_profile = {};
              this.meta.caller_profile.display_name = normalizedName;

              logger.info("CALLER_NAME_CAPTURED", {
                ...this.meta,
                caller: callerId,
                name: normalizedName,
                confidence_reason: found.reason,
                source_utterance: nlp.raw,
              });
            }
          }
        }
      } catch {}

      if (this._awaitingCallbackConfirmation && isAffirmativeUtterance(nlp.normalized || nlp.raw)) {
        this._awaitingCallbackConfirmation = false;
        this._sendImmediateCallbackClosing();
      }

      const intent = detectIntent({
        text: nlp.normalized || nlp.raw,
        intents: this.ssot?.intents || [],
      });

      logger.info("INTENT_DETECTED", {
        ...this.meta,
        text: nlp.raw,
        normalized: nlp.normalized,
        lang: nlp.lang,
        language_locked: this._langState.lockedLanguage,
        intent,
      });
    }

    if (who === "bot") {
      const botText = nlp.normalized || nlp.raw;

      if (
        /(לחזור למספר|לחזור אל המספר|לחזור למספר המזוהה|האם לחזור למספר|שנחזור למספר)/u.test(
          botText
        )
      ) {
        this._awaitingCallbackConfirmation = true;
      }

      if (
        env.FORCE_HANGUP_AFTER_CLOSE &&
        !this._hangupScheduled &&
        isClosingUtterance(botText)
      ) {
        const callSid =
          safeStr(this._call?.callSid) || safeStr(this.meta?.callSid);

        if (callSid) {
          this._hangupScheduled = true;
          const graceMs = Math.max(
            15000,
            Number(env.HANGUP_AFTER_CLOSE_GRACE_MS || 15000)
          );

          setTimeout(() => {
            hangupCall(callSid, logger).catch(() => {});
          }, graceMs);

          logger.info("Proactive hangup scheduled", {
            ...this.meta,
            callSid,
            delay_ms: graceMs,
          });
        }
      }
    }

    if (this.onTranscript) {
      this.onTranscript({
        who,
        text: nlp.raw,
        normalized: nlp.normalized,
        lang: nlp.lang,
      });
    }
  }

  _sendProactiveOpening() {
    if (!this.ws || this.closed || !this.ready) return;

    const callerProfile = this.meta?.caller_profile || null;
    let callerName = safeStr(callerProfile?.display_name) || "";
    if (callerName === "שאי") callerName = "שי";

    const totalCalls = Number(callerProfile?.total_calls ?? 0);
    const isReturning = totalCalls > 0;

    const openingPack = getCachedOpening({
      ssot: this.ssot,
      callerName,
      isReturning,
      timeZone: env.TIME_ZONE || "Asia/Jerusalem",
      ttlMs: Number(env.MB_OPENING_CACHE_TTL_MS || 300000),
    });

    const opening = openingPack.opening;

    const userKickoff = [
      "ענה עכשיו רק במשפט הבא, בדיוק כפי שהוא, בלי הקדמה, בלי הסבר, בלי מחשבות בקול ובלי שום טקסט נוסף.",
      "אחרי המשפט עצור והמתן ללקוח.",
      opening,
    ].join("\n");

    const msg = {
      clientContent: {
        turns: [{ role: "user", parts: [{ text: userKickoff }] }],
        turnComplete: true,
      },
    };

    try {
      this.ws.send(JSON.stringify(msg));
      logger.info("Proactive opening sent", {
        ...this.meta,
        greeting: openingPack.greeting,
        opening_len: opening.length,
        language_locked: this._langState.lockedLanguage,
        opening_cache_hit: openingPack.cache_hit,
      });
    } catch (e) {
      logger.debug("Failed sending proactive opening", {
        ...this.meta,
        error: e.message,
      });
    }
  }

  sendUlaw8kFromTwilio(ulaw8kB64) {
    if (!this.ws || this.closed || !this.ready) return;

    const pcm16kB64 = ulaw8kB64ToPcm16kB64(ulaw8kB64);
    const msg = {
      realtimeInput: {
        mediaChunks: [
          {
            mimeType: "audio/pcm;rate=16000",
            data: pcm16kB64,
          },
        ],
      },
    };

    try {
      this.ws.send(JSON.stringify(msg));
    } catch (e) {
      logger.debug("Failed sending audio to Gemini", {
        ...this.meta,
        error: e.message,
      });
    }
  }

  endInput() {
    if (!this.ws || this.closed) return;

    try {
      this.ws.send(JSON.stringify({ realtimeInput: { audioStreamEnd: true } }));
    } catch {}
  }

  async _finalizeOnce(reason) {
    if (this._call.finalized) return;
    this._call.finalized = true;

    try {
      this._call.ended_at = nowIso();
      const durationMs =
        Date.now() - new Date(this._call.started_at).getTime();

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
        finalize_reason: reason || "",
        language_locked: this._langState.lockedLanguage,
      };

      if (this._passiveCtx && passiveCallContext?.finalizeCtx) {
        try {
          callMeta.passive_context = passiveCallContext.finalizeCtx(
            this._passiveCtx
          );
        } catch {}
      }

      const snapshot = {
        call: callMeta,
        conversationLog: this._call.conversationLog || [],
      };

      await finalizePipeline({
        snapshot,
        ssot: this.ssot,
        env,
        logger,
        senders: {
          sendCallLog: (payload) =>
            deliverWebhook(env.CALL_LOG_WEBHOOK_URL, payload, "CALL_LOG"),
          sendFinal: (payload) =>
            deliverWebhook(env.FINAL_WEBHOOK_URL, payload, "FINAL"),
          sendAbandoned: (payload) =>
            deliverWebhook(env.ABANDONED_WEBHOOK_URL, payload, "ABANDONED"),
          resolveRecording: async () => {
            if (!isTruthyEnv(env.MB_ENABLE_RECORDING)) {
              return {
                recording_provider: null,
                recording_sid: null,
                recording_url_public: null,
              };
            }

            await waitForRecording(this._call.callSid, 12000);

            const rec = getRecordingForCall(this._call.callSid);
            const sid =
              safeStr(rec?.recordingSid || this._call.recording_sid) || null;
            const url = sid ? publicRecordingUrl(sid) : null;

            if (sid) this._call.recording_sid = sid;

            return {
              recording_provider: sid ? "twilio" : null,
              recording_sid: sid,
              recording_url_public: url,
            };
          },
        },
      });
    } catch (e) {
      logger.warn("Finalize failed", { error: String(e) });
    }
  }

  stop() {
    this._finalizeOnce("stop_called").catch(() => {});
    if (!this.ws) return;

    try {
      this.ws.close();
    } catch {}
  }
}

module.exports = { GeminiLiveSession };
