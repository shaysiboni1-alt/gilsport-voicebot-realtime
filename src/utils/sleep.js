"use strict";

/**
 * Async sleep helper.
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
  const n = Number(ms);
  if (!Number.isFinite(n) || n <= 0) return Promise.resolve();
  return new Promise((resolve) => setTimeout(resolve, n));
}

module.exports = { sleep };
