// src/storage/callerMemory.js
"use strict";

/**
 * Compatibility wrapper for caller memory.
 *
 * The canonical implementation lives under src/memory/callerMemory.js.
 * This wrapper exposes the API expected by vendor/geminiLiveSession.js.
 *
 * Exports:
 *   { callerMemory: { ensureSchema, upsertAndGetProfile, getProfile, setDisplayName } }
 */

const {
  initCallerMemory,
  getCallerProfile,
  upsertCallerProfile,
  updateCallerDisplayName,
} = require("../memory/callerMemory");

async function ensureSchema() {
  await initCallerMemory();
}

async function upsertAndGetProfile(caller_id, display_name = null) {
  if (!caller_id) return null;
  await upsertCallerProfile(caller_id, display_name);
  return await getCallerProfile(caller_id);
}

async function getProfile(caller_id) {
  if (!caller_id) return null;
  return await getCallerProfile(caller_id);
}

async function setDisplayName(caller_id, display_name, opts = {}) {
  const profile = await getCallerProfile(caller_id);
  if (!profile) return null;

  const nameLocked = opts && typeof opts.nameLocked === "boolean" ? opts.nameLocked : false;
  return await updateCallerDisplayName(profile, display_name, nameLocked);
}

module.exports = {
  callerMemory: {
    ensureSchema,
    upsertAndGetProfile,
    getProfile,
    setDisplayName,
  },
};
