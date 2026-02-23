"use strict";

function sleep(ms) {
  const n = Number(ms);
  const delay = Number.isFinite(n) && n >= 0 ? n : 0;
  return new Promise((resolve) => setTimeout(resolve, delay));
}

module.exports = { sleep };
