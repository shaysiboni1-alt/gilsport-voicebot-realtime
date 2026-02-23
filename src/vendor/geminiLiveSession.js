"use strict";

const { logger } = require("../utils/logger");
const { sleep } = require("../utils/sleep");
const { nowMs } = require("../utils/time");
const { ulaw8kB64ToPcm16kB64, pcm16kB64ToUlaw8kB64 } = require("../utils/audio");
const { normalizeUtterance } = require("../logic/normalizeUtterance");
const { detectIntentDeterministic } = require("../logic/intentRouter");
const { extractLeadDeterministic } = require("../logic/leadExtractor");
const { extractCallerName } = require("../logic/nameExtractor");
const { callerMemory } = require("../storage/callerMemory");
const { recordingsRegistry } = require("../storage/recordingsRegistry");
const { webhookClient } = require("../webhooks/webhookClient");

// NOTE: This file is the Gemini Live session orchestrator.
// It bridges Twilio Media Streams <-> Gemini Live WebSocket.

class GeminiLiveSession {
  constructor({ wsUrl, apiKey, meta, sendToTwilioMedia, onClose }) {
    this.wsUrl = wsUrl;
    this.apiKey = apiKey;

    this.meta = meta || {};
    this.sendToTwilioMedia = sendToTwilioMedia;
    this.onClose = onClose;

    this.ws = null;
    this.ready = false;
    this.closed = false;

    this.startTs = nowMs();
    this.streamSid = meta?.streamSid || null;
    this.callSid = meta?.callSid || null;

    this.userTranscript = [];
    this.botTranscript = [];
    this.conversationLog = [];

    this.lastUserUtteranceAt = 0;
    this.lastBotUtteranceAt = 0;

    this.lastBotAskedForName = false;
    this.callerProfile = null;

    this.currentIntent = { intent_id: "other", intent_type: "other", score: 0, priority: 0, matched_triggers: [] };

    // Used for latency probes
    this.pendingLatencyMarks = new Map(); // key -> { startedAtMs, meta }

    // Recording
    this.recordingStarted = false;
    this.recordingSid = null;

    // Guards
    this._closing = false;
  }

  async start() {
    await this._loadCallerProfile();
    await this._connect();
    await this._startRecordingIfNeeded();
    await this._sendProactiveOpening();
  }

  async _loadCallerProfile() {
    try {
      const callerId = this.meta?.caller || null;
      if (!callerId) return;

      await callerMemory.ensureSchema();
      const profile = await callerMemory.upsertAndGetProfile(callerId);

      this.callerProfile = profile || null;
      logger.info("Caller profile ready", { ...this.meta, caller_profile: this.callerProfile });
    } catch (e) {
      logger.debug("Failed loading caller profile", { ...this.meta, error: e.message });
    }
  }

  async _connect() {
    const WebSocket = require("ws");

    const headers = {
      "Content-Type": "application/json"
    };

    // Gemini Live WS URL already includes auth params in wsUrl (or uses header).
    // Keep it consistent with the rest of the codebase.
    const url = this.wsUrl;

    this.ws = new WebSocket(url, { headers });

    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("Gemini Live WS connect timeout")), 15000);

      this.ws.on("open", () => {
        clearTimeout(t);
        this.ready = true;
        logger.info("Gemini Live WS connected", { ...this.meta, caller_profile: this.callerProfile });
        resolve();
      });

      this.ws.on("error", (err) => {
        clearTimeout(t);
        reject(err);
      });
    });

    this.ws.on("message", (data) => this._onMessage(data));
    this.ws.on("close", (code, reason) => this._onClose(code, reason?.toString?.() || ""));
    this.ws.on("error", (err) => {
      logger.debug("Gemini Live WS error", { ...this.meta, error: err.message });
    });

    // Send initial config if needed (kept minimal here).
    // The provider config is managed elsewhere (ENV + SSOT).
  }

  async _startRecordingIfNeeded() {
    try {
      if (this.recordingStarted) return;

      // Twilio callSid is available in meta; actual recording start is managed by Twilio webhook in this runtime.
      // Here we only wait for registry to contain the recordingSid if it was started.
      const callSid = this.meta?.callSid;
      if (!callSid) return;

      // Wait briefly for recordingSid to be registered (race-safe).
      const maxWaitMs = 3000;
      const pollEveryMs = 150;
      const startedAt = nowMs();

      while (nowMs() - startedAt < maxWaitMs) {
        const rec = recordingsRegistry.get(callSid);
        if (rec?.recordingSid) {
          this.recordingStarted = true;
          this.recordingSid = rec.recordingSid;
          logger.info("Recording started + stored in registry", { ...this.meta, recordingSid: this.recordingSid });
          return;
        }
        await sleep(pollEveryMs);
      }
    } catch (e) {
      logger.debug("Failed starting/reading recording", { ...this.meta, error: e.message });
    }
  }

  async _sendProactiveOpening() {
    // Send a proactive opening immediately after Gemini connects.
    // Uses SSOT opening prompt already baked in the system prompt elsewhere.
    const greeting = this._pickGreeting();
    const openingRaw =
      (process.env.MB_OPENING_TEXT || "").trim() ||
      "שלום, מדברת נטע, המזכירה הוירטואלית. איך אפשר לעזור?";

    const opening = `${greeting}, ${openingRaw}`.replace(/\s+/g, " ").replace(",,", ",").trim();

    // Keep kickoff ultra-short to avoid the model "thinking out loud".
    const userKickoff = `אמרי בקול את המשפט הבא בלבד ואז עצרי להקשבה:\n${opening}`;

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
        mediaChunks: [
          {
            mimeType: "audio/pcm;rate=16000",
            data: pcm16kB64
          }
        ]
      }
    };

    try {
      this.ws.send(JSON.stringify(msg));
    } catch (e) {
      // Avoid spamming logs on transient WS issues.
      logger.debug("Failed sending audio to Gemini", { ...this.meta, error: e.message });
    }
  }

  _onMessage(data) {
    if (this.closed) return;

    let msg;
    try {
      msg = JSON.parse(data.toString("utf-8"));
    } catch (e) {
      logger.debug("Non-JSON from Gemini", { ...this.meta });
      return;
    }

    // Audio chunks out
    // Gemini Live streams audio as base64 PCM16 at 16kHz (commonly).
    // We convert to Twilio ulaw8k and send.
    const serverContent = msg?.serverContent;
    if (serverContent?.modelTurn?.parts?.length) {
      for (const part of serverContent.modelTurn.parts) {
        // Audio part
        if (part?.inlineData?.mimeType?.startsWith("audio/") && part?.inlineData?.data) {
          // Measure latency for first audio out if a mark exists
          this._maybeMarkFirstAudioOut();

          const pcmB64 = part.inlineData.data;
          const ulawB64 = pcm16kB64ToUlaw8kB64(pcmB64);

          this.sendToTwilioMedia(ulawB64);
        }

        // Text part (for transcripts/logging)
        if (typeof part?.text === "string" && part.text.trim()) {
          const text = part.text.trim();
          this._appendBotUtterance(text);
        }
      }
    }

    // User transcripts from ASR
    if (serverContent?.inputTranscription?.text) {
      const text = (serverContent.inputTranscription.text || "").trim();
      if (text) this._appendUserUtterance(text);
    }

    // Some providers emit explicit turnComplete signals
    if (serverContent?.turnComplete) {
      // no-op: we process in streaming fashion
    }

    // If the socket indicates end-of-session
    if (msg?.event === "sessionEnded") {
      this.close();
    }
  }

  _appendUserUtterance(text) {
    const normalized = normalizeUtterance(text);
    const lang = this._guessLang(normalized);

    this.lastUserUtteranceAt = nowMs();

    this.userTranscript.push(normalized);
    this.conversationLog.push({ role: "user", text: normalized, ts_ms: this.lastUserUtteranceAt });

    logger.info("UTTERANCE user", { ...this.meta, caller_profile: this.callerProfile, text, normalized, lang });
    logger.info("TRANSCRIPT user", { streamSid: this.meta?.streamSid, callSid: this.meta?.callSid, text });

    // If bot asked for name last, attempt to capture caller name deterministically
    if (this.lastBotAskedForName) {
      const isKnownName = (name) => {
        const n = String(name || "").trim();
        if (!n) return false;
        const BAD = new Set(["לא", "כן", "אוקיי", "אוקי", "שלום", "היי", "הי", "תודה"]);
        if (BAD.has(n)) return false;
        if (n.length >= 4 && n[0] === "ב" && n[1] === "ה") return false;
        return true;
      };

      // Never override a known, stable caller name.
      if (isKnownName(this.callerProfile?.display_name)) {
        this.lastBotAskedForName = false;
      } else {
      const found = extractCallerName(normalized);

      if (found?.name) {
        // Update caller profile in DB
        this._setCallerName(found.name, found.reason, normalized);
      }

      // Reset after one attempt (avoid repeatedly classifying arbitrary utterances as name)
      this.lastBotAskedForName = false;
      }
    }

    // Detect intent deterministically based on SSOT triggers/priority
    try {
      const detected = detectIntentDeterministic(normalized);
      this.currentIntent = detected || this.currentIntent;

      logger.info("INTENT_DETECTED", {
        ...this.meta,
        caller_profile: this.callerProfile,
        text,
        normalized,
        lang,
        intent: this.currentIntent
      });
    } catch (e) {
      logger.debug("Intent detection error", { ...this.meta, error: e.message });
    }

    // Mark a latency probe: time from user utterance -> first audio out
    // Use streamSid+ts to disambiguate bursts.
    const key = `u2a:${this.lastUserUtteranceAt}`;
    this.pendingLatencyMarks.set(key, {
      startedAtMs: this.lastUserUtteranceAt,
      meta: { ...this.meta, caller_profile: this.callerProfile }
    });
  }

  _appendBotUtterance(text) {
    const normalized = normalizeUtterance(text);
    const lang = this._guessLang(normalized);

    this.lastBotUtteranceAt = nowMs();

    this.botTranscript.push(normalized);
    this.conversationLog.push({ role: "bot", text: normalized, ts_ms: this.lastBotUtteranceAt });

    logger.info("UTTERANCE bot", { ...this.meta, caller_profile: this.callerProfile, text, normalized, lang });
    logger.info("TRANSCRIPT bot", { streamSid: this.meta?.streamSid, callSid: this.meta?.callSid, text });

    // Heuristic: if bot asks for name, mark so next user utterance is interpreted as name
    if (this._botAskedForName(normalized)) {
      this.lastBotAskedForName = true;
    }
  }

  async _setCallerName(name, confidence_reason, source_utterance) {
    try {
      if (!this.callerProfile?.caller_id) return;

      // Update in DB
      await callerMemory.setDisplayName(this.callerProfile.caller_id, name);

      // Refresh local profile
      this.callerProfile = await callerMemory.getProfile(this.callerProfile.caller_id);

      logger.info("CALLER_NAME_CAPTURED", {
        ...this.meta,
        caller_profile: this.callerProfile,
        name,
        confidence_reason,
        source_utterance
      });
    } catch (e) {
      logger.debug("Failed setting caller name", { ...this.meta, error: e.message });
    }
  }

  _botAskedForName(text) {
    // Hebrew variants: "מי מדבר", "מה שמך", "איך קוראים לך"
    // Keep it strict to avoid false positives.
    const t = (text || "").replace(/\s+/g, " ").trim();
    if (!t) return false;

    return (
      /מי\s+מדבר/i.test(t) ||
      /מה\s+שמך/i.test(t) ||
      /איך\s+קוראים\s+לך/i.test(t) ||
      /מה\s+השם\s+שלך/i.test(t)
    );
  }

  _maybeMarkFirstAudioOut() {
    // Consume the oldest pending u2a mark (first audio out for latest user utterance)
    if (!this.pendingLatencyMarks.size) return;

    // pick earliest startedAtMs
    let oldestKey = null;
    let oldestTs = Infinity;
    for (const [k, v] of this.pendingLatencyMarks.entries()) {
      if (v.startedAtMs < oldestTs) {
        oldestTs = v.startedAtMs;
        oldestKey = k;
      }
    }
    if (!oldestKey) return;

    const mark = this.pendingLatencyMarks.get(oldestKey);
    this.pendingLatencyMarks.delete(oldestKey);

    const delta = nowMs() - mark.startedAtMs;
    logger.info("LATENCY first_audio_out", { ...mark.meta, delta_ms: delta });
  }

  async close() {
    if (this.closed) return;
    this.closed = true;

    try {
      this.ws?.close?.();
    } catch (_) {}

    try {
      await this._postCallProcessing();
    } catch (e) {
      logger.debug("Postcall processing failed", { ...this.meta, error: e.message });
    }

    try {
      this.onClose?.();
    } catch (_) {}
  }

  _onClose(code, reason) {
    if (this.closed) return;

    logger.info("Gemini Live WS closed", { ...this.meta, caller_profile: this.callerProfile, code, reason });
    this.close();
  }

  async _postCallProcessing() {
    // Determine FINAL vs ABANDONED based on whether we have a meaningful user utterance and/or lead completeness
    const callSid = this.meta?.callSid;
    const caller = this.meta?.caller;
    const called = this.meta?.called;

    const conversationLog = this.conversationLog.slice();
    const fullConversationText = conversationLog.map((x) => `${x.role}: ${x.text}`).join("\n");

    let parsedLead = null;
    let leadOk = false;

    try {
      parsedLead = extractLeadDeterministic(fullConversationText);
      leadOk = !!parsedLead;
      logger.info(
        JSON.stringify({
          msg: "Postcall lead parsed",
          meta: { ok: leadOk, has_known_name: !!(this.callerProfile?.display_name) }
        })
      );
    } catch (e) {
      logger.debug("Lead parse failed", { ...this.meta, error: e.message });
    }

    const hasUserUtterance = this.userTranscript.length > 0;
    const decision = hasUserUtterance ? "FINAL" : "ABANDONED";

    // Attach recording url if known
    const recordingSid = recordingsRegistry.get(callSid)?.recordingSid || this.recordingSid || null;
    const recordingProvider = recordingSid ? "twilio" : null;
    const recordingUrlPublic =
      recordingSid && process.env.PUBLIC_BASE_URL
        ? `${process.env.PUBLIC_BASE_URL.replace(/\/+$/, "")}/recordings/${recordingSid}`
        : null;

    const payload = {
      call: {
        call_sid: callSid,
        call_status: "completed",
        status: "completed",
        event: decision,
        decision_reason: hasUserUtterance ? "ok" : "no_user_utterance",
        intent: this.currentIntent?.intent_id || "other",
        recording_provider: recordingProvider,
        recording_sid: recordingSid,
        recording_url_public: recordingUrlPublic
      },
      conversationLog,
      parsedLeadCollection: parsedLead || {}
    };

    // Deliver required webhooks
    try {
      await webhookClient.deliver("CALL_LOG", payload);
      logger.info("Webhook delivered", { label: "CALL_LOG", status: 200 });
    } catch (e) {
      logger.debug("Webhook CALL_LOG failed", { ...this.meta, error: e.message });
    }

    try {
      await webhookClient.deliver(decision, payload);
      logger.info("Webhook delivered", { label: decision, status: 200 });
    } catch (e) {
      logger.debug(`Webhook ${decision} failed`, { ...this.meta, error: e.message });
    }
  }

  _pickGreeting() {
    // Basic greeting selection by local time (server time).
    const h = new Date().getHours();
    if (h >= 5 && h < 12) return "בוקר טוב";
    if (h >= 12 && h < 18) return "צהריים טובים";
    return "ערב טוב";
  }

  _guessLang(text) {
    // Minimal heuristic: presence of Hebrew letters
    if (/[א-ת]/.test(text)) return "he";
    return "en";
  }
}

module.exports = { GeminiLiveSession };
