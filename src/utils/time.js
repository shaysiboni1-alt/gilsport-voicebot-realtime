"use strict";

function nowIso() {
  return new Date().toISOString();
}

function nowMs() {
  return Date.now();
}

module.exports = {
  nowIso,
  nowMs,
};
