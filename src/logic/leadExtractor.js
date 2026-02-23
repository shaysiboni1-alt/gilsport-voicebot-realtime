"use strict";

// Minimal deterministic lead extractor used by legacy runtime paths.
// The canonical lead parsing pipeline is implemented in stage4/postcallLeadParser.js.

function extractLeadDeterministic(_conversationLog) {
  return {
    full_name: null,
    subject: null,
    callback_to_number: null,
    notes: null,
  };
}

module.exports = { extractLeadDeterministic };
