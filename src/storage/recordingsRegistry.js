"use strict";

/**
 * Compatibility layer.
 *
 * The canonical implementation lives in `src/utils/recordingRegistry.js`.
 * Some runtime/vendor files import `../storage/recordingsRegistry` and expect
 * an object named `recordingsRegistry` with methods.
 */

const impl = require("../utils/recordingRegistry");

const recordingsRegistry = {
  setRecordingForCall: impl.setRecordingForCall,
  getRecordingForCall: impl.getRecordingForCall,
  clearRecordingForCall: impl.clearRecordingForCall,
  listRecordings: impl.listRecordings,
};

module.exports = {
  recordingsRegistry,
  ...recordingsRegistry,
};
