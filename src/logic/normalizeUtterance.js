"use strict";

// Backwards-compatible re-export.
// The canonical deterministic implementation lives in hebrewNlp.js.

const { normalizeUtterance } = require("./hebrewNlp");

module.exports = { normalizeUtterance };
