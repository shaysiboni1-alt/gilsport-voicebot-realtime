"use strict";

const WebSocket = require("ws");
const fetch = require("node-fetch");
const { logger } = require("../utils/logger");
const { sleep } = require("../utils/sleep");
const { safeJsonParse } = require("../utils/safeJsonParse");
const { normalizeUtterance } = require("../utils/textNlp");
const { callerMemory } = require("../storage/callerMemory");
const { extractNameFromText, extractNameFromDirectAnswer } = require("../logic/nameExtractor");
const { recordCallSidRecordingSid } = require("../recording/registry");
const { twilioStartCallRecording } = require("../recording/twilioRecording");

class GeminiLiveSession {
  constructor({
    ws,
    streamSid,
    callSid,
    caller,
    called,
    source,
    ssot,
    callerProfile,
    twilioAuth,
    enableRecording,
  }) {
    this.ws = ws;
    this.streamSid = streamSid;
    this.callSid = callSid;
    this.caller = caller;
    this.called = called;
    this.source = source;
    this.ssot = ssot;
    this.twilioAuth = twilioAuth;
    this.enableRecording = enableRecording;

    this.geminiWs = null;
    this.isClosed = false;

    this.callerProfile = callerProfile || null;

    this.pendingLatencyMarks = [];
    this.turnState = {
      lastUserUtteranceAtMs: null,
      firstAudioOutMarked: false,
    };

    this.lastBotAskedForName = false;
  }

  async start() {
    await callerMemory.ensureSchema();

    // Make sure we have an up-to-date caller profile (and bump total_calls)
    if (this.caller) {
      this.callerProfile = await callerMemory.upsertAndGetProfile(
        this.caller,
        this.callerProfile?.display_name || null
      );
    }

    await this._connect();

    // Start recording asynchronously (do not block the first prompt / audio).
    this._startRecordingIfNeeded().catch((err) => {
      logger.warn("Recording start failed (non-blocking)", { err: String(err) });
    });

    await this._sendProactiveOpening();
  }

  async close(code = 1000, reason = "") {
    if (this.isClosed) return;
    this.isClosed = true;

    try {
      if (this.geminiWs && this.geminiWs.readyState === WebSocket.OPEN) {
        this.geminiWs.close(code, reason);
      }
    } catch (_) {}

    this.geminiWs = null;
  }

  async onTwilioMedia(message) {
    if (this.isClosed) return;

    // Twilio sends base64 audio payloads
    if (!message?.media?.payload) return;

    const audioB64 = message.media.payload;

    // Forward audio to Gemini/Proxy
    this._sendToGemini({
      type: "input_audio_buffer.append",
      audio: audioB64,
    });
  }

  async _connect() {
    const url = process.env.GEMINI_LIVE_WS_URL;
    if (!url) throw new Error("Missing GEMINI_LIVE_WS_URL");

    const headers = {
      "x-stream-sid": this.streamSid || "",
      "x-call-sid": this.callSid || "",
    };

    this.geminiWs = new WebSocket(url, { headers });

    const openP = new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error("Gemini WS connect timeout")), 15000);
      this.geminiWs.once("open", () => {
        clearTimeout(t);
        resolve();
      });
      this.geminiWs.once("error", (err) => {
        clearTimeout(t);
        reject(err);
      });
    });

    await openP;

    this.geminiWs.on("message", (data) => {
      try {
        this._onGeminiMessage(String(data));
      } catch (err) {
        logger.warn("Gemini WS message handler error", { err: String(err) });
      }
    });

    this.geminiWs.on("close", (code, reason) => {
      logger.info("Gemini Live WS closed", {
        streamSid: this.streamSid,
        callSid: this.callSid,
        caller: this.caller,
        called: this.called,
        source: this.source,
        caller_profile: this.callerProfile,
        code,
        reason: reason ? String(reason) : "",
      });
    });

    logger.info("Gemini Live WS connected", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      caller: this.caller,
      called: this.called,
      source: this.source,
      caller_profile: this.callerProfile,
    });
  }

  async _startRecordingIfNeeded() {
    if (!this.enableRecording) return;
    if (!this.twilioAuth?.accountSid || !this.twilioAuth?.authToken) return;
    if (!this.callSid) return;

    try {
      const started = await twilioStartCallRecording({
        accountSid: this.twilioAuth.accountSid,
        authToken: this.twilioAuth.authToken,
        callSid: this.callSid,
      });

      if (started?.recordingSid) {
        recordCallSidRecordingSid(this.callSid, started.recordingSid);
        logger.info("Recording started + stored in registry", {
          callSid: this.callSid,
          recordingSid: started.recordingSid,
        });
      }
    } catch (err) {
      logger.warn("Failed to start recording", { err: String(err), callSid: this.callSid });
    }
  }

  async _sendProactiveOpening() {
    const greeting = this._getGreeting();

    const knownName =
      this.callerProfile?.name_locked && this.callerProfile?.display_name
        ? this.callerProfile.display_name
        : null;

    // This is the first "kick" to the model/proxy to speak quickly.
    // Keep it short to minimize perceived latency.
    const opening =
      knownName
        ? `${greeting} ${knownName}, נעים לשמוע מכם שוב. איך נוכל לעזור?`
        : `${greeting}, מדברת נטע, המזכירה הוירטואלית של גיל ספורט. מי מדבר בבקשה?`;

    this._sendToGemini({
      type: "input_text",
      text: opening,
      meta: {
        opening: true,
      },
    });

    logger.info("Proactive opening sent", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      caller: this.caller,
      called: this.called,
      source: this.source,
      caller_profile: this.callerProfile,
      greeting,
      opening_len: opening.length,
    });
  }

  _getGreeting() {
    // Simple greeting based on local time could be implemented here;
    // keep deterministic for now.
    return "ערב טוב";
  }

  _sendToGemini(payload) {
    if (!this.geminiWs || this.geminiWs.readyState !== WebSocket.OPEN) return;
    this.geminiWs.send(JSON.stringify(payload));
  }

  _onGeminiMessage(raw) {
    const msg = safeJsonParse(raw);
    if (!msg) return;

    if (msg.type === "transcript") {
      const who = msg.who || "unknown";
      const text = msg.text || "";
      const normalized = normalizeUtterance(text);

      if (who === "user") {
        this._appendUserUtterance(text, normalized);
      } else if (who === "bot") {
        this._appendBotUtterance(text, normalized);
      }
      return;
    }

    if (msg.type === "audio") {
      // Forward audio to Twilio
      if (msg.audio) {
        this._maybeMarkFirstAudioOut();
        this.ws.send(
          JSON.stringify({
            event: "media",
            streamSid: this.streamSid,
            media: { payload: msg.audio },
          })
        );
      }
      return;
    }

    if (msg.type === "event" && msg.name === "bot_asked_for_name") {
      this.lastBotAskedForName = true;
      return;
    }
  }

  _appendBotUtterance(text, normalized) {
    logger.info("UTTERANCE bot", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      caller: this.caller,
      called: this.called,
      source: this.source,
      caller_profile: this.callerProfile,
      text,
      normalized,
      lang: "he",
    });

    logger.info("TRANSCRIPT bot", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      text,
    });

    // Name question heuristic (fallback if proxy doesn't emit bot_asked_for_name)
    if (this._botAskedForName(normalized)) {
      this.lastBotAskedForName = true;
    }
  }

  async _appendUserUtterance(text, normalized) {
    logger.info("UTTERANCE user", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      caller: this.caller,
      called: this.called,
      source: this.source,
      caller_profile: this.callerProfile,
      text,
      normalized,
      lang: "he",
    });

    logger.info("TRANSCRIPT user", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      text,
    });

    // Caller-name capture (strict + deterministic):
    // 1) Explicit self-identification ("קוראים לי X", "השם שלי X", "שמי X")
    // 2) Direct short answer immediately after the bot asked for the name
    // Never overwrite a locked name in caller memory.
    if (this.callerProfile && !this.callerProfile.name_locked) {
      const explicit = extractNameFromText(normalized);

      if (explicit?.name) {
        // Explicit self-identification is treated as locked.
        await this._setCallerName(explicit.name, explicit.reason, normalized, true);
        this.lastBotAskedForName = false;
      }
    }

    if (this.lastBotAskedForName) {
      // Reset after one attempt (avoid repeatedly classifying arbitrary utterances as name)
      this.lastBotAskedForName = false;

      if (this.callerProfile && this.callerProfile.name_locked) return;

      const found = extractNameFromDirectAnswer(normalized);

      if (found?.name) {
        // Direct answers are locked only if they pass strict plausibility checks in the extractor.
        await this._setCallerName(found.name, found.reason, normalized, true);
      }
    }

    // Latency mark start point (from the moment we got a final user transcript)
    this.pendingLatencyMarks.push({
      type: "user_final",
      atMs: Date.now(),
    });
    this.turnState.lastUserUtteranceAtMs = Date.now();
    this.turnState.firstAudioOutMarked = false;
  }

  async _setCallerName(name, reason, sourceUtterance, nameLocked = true) {
    if (!this.callerProfile?.caller_id) return;

    try {
      await callerMemory.setDisplayName(this.callerProfile.caller_id, name, { nameLocked });
      this.callerProfile = await callerMemory.getProfile(this.callerProfile.caller_id);

      logger.info("CALLER_NAME_CAPTURED", {
        streamSid: this.streamSid,
        callSid: this.callSid,
        caller: this.caller,
        called: this.called,
        source: this.source,
        caller_profile: this.callerProfile,
        name,
        confidence_reason: reason,
        source_utterance: sourceUtterance,
      });
    } catch (err) {
      logger.warn("Failed to set caller name", { err: String(err), name });
    }
  }

  _botAskedForName(text) {
    const t = String(text || "").toLowerCase();

    const patterns = [
      "מי מדבר",
      "איך קוראים",
      "איך קוראים לך",
      "מה השם שלך",
      "מה שמך",
      "עם מי אני מדברת",
      "עם מי אני מדבר",
      "מי על הקו",
    ];

    return patterns.some((p) => t.includes(p));
  }

  _maybeMarkFirstAudioOut() {
    if (this.turnState.firstAudioOutMarked) return;
    this.turnState.firstAudioOutMarked = true;

    const now = Date.now();
    const lastUserAt = this.turnState.lastUserUtteranceAtMs;
    if (!lastUserAt) return;

    const delta_ms = now - lastUserAt;

    logger.info("LATENCY first_audio_out", {
      streamSid: this.streamSid,
      callSid: this.callSid,
      caller: this.caller,
      called: this.called,
      source: this.source,
      caller_profile: this.callerProfile,
      delta_ms,
    });
  }
}

module.exports = { GeminiLiveSession };
