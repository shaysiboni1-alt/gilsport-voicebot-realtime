"use strict";

// In-memory registry: CallSid -> { recordingSid, recordingUrl, updatedAt }
// Best-effort only (per canonical spec). If the process restarts, data may be lost.

const RECORDINGS = new Map();

function setRecordingForCall(call_id, { recordingSid, recordingUrl } = {}) {
  const key = String(call_id || "").trim();
  if (!key) return;

  const prev = RECORDINGS.get(key) || {};
  const next = {
    recordingSid: recordingSid ?? prev.recordingSid ?? null,
    recordingUrl: recordingUrl ?? prev.recordingUrl ?? null,
    updatedAt: Date.now(),
  };
  RECORDINGS.set(key, next);
}

function getRecordingForCall(call_id) {
  const key = String(call_id || "").trim();
  if (!key) return { recordingSid: null, recordingUrl: null };
  const rec = RECORDINGS.get(key) || {};
  return {
    recordingSid: rec.recordingSid ?? null,
    recordingUrl: rec.recordingUrl ?? null,
  };
}

async function waitForRecording(call_id, timeoutMs = 12000) {
  const key = String(call_id || "").trim();
  if (!key) return { recordingSid: null, recordingUrl: null };

  const started = Date.now();
  while (Date.now() - started < timeoutMs) {
    const rec = getRecordingForCall(key);
    // IMPORTANT:
    // Twilio may return a RecordingSid immediately when recording is started,
    // but the MP3 is not necessarily ready yet. The canonical completion signal
    // we can rely on is RecordingUrl arriving via RecordingStatusCallback.
    // לכן — מחכים ל-recordingUrl, ולא חוזרים מיד רק בגלל recordingSid.
    if (rec.recordingUrl) return rec;
    await new Promise((r) => setTimeout(r, 250));
  }
  return getRecordingForCall(key);
}

module.exports = {
  setRecordingForCall,
  getRecordingForCall,
  waitForRecording,
};
