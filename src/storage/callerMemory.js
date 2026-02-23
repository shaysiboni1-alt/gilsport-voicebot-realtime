"use strict";

// Backwards-compatible adapter.
// Some modules historically imported "../storage/callerMemory".
// The canonical implementation lives under src/memory/callerMemory.js.

const mem = require("../memory/callerMemory");

async function initCallerMemory() {
  return mem.initCallerMemory();
}

async function ensureCallerProfile(callerId) {
  return mem.ensureCallerProfile(callerId);
}

async function getCallerProfile(callerId) {
  return mem.getCallerProfile(callerId);
}

async function upsertCallerProfile(callerId, patch = {}) {
  return mem.upsertCallerProfile(callerId, patch);
}

async function updateCallerDisplayName(callerId, displayName, metaPatch = null) {
  return mem.updateCallerDisplayName(callerId, displayName, metaPatch);
}

// Aliases used in older code
async function getProfile(callerId) {
  return getCallerProfile(callerId);
}

async function setDisplayName(callerId, displayName, metaPatch = null) {
  return updateCallerDisplayName(callerId, displayName, metaPatch);
}

module.exports = {
  initCallerMemory,
  ensureCallerProfile,
  getCallerProfile,
  upsertCallerProfile,
  updateCallerDisplayName,
  // aliases
  getProfile,
  setDisplayName,
};
