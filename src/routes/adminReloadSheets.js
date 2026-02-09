"use strict";

const express = require("express");

// POST /admin/reload-sheets
// Forces SSOT reload from Google Sheets.
// Returns basic diagnostics so you can see it worked in Render logs.

function adminReloadRouter(ssotClient) {
  const router = express.Router();

  router.post("/admin/reload-sheets", async (req, res) => {
    try {
      if (!ssotClient || typeof ssotClient.loadSSOT !== "function") {
        return res.status(500).json({ ok: false, error: "SSOT client not available" });
      }

      const t0 = Date.now();
      const ssot = await ssotClient.loadSSOT(true); // force reload
      const ms = Date.now() - t0;

      const settings_keys = Object.keys(ssot?.settings || {}).length;
      const prompts_keys = Object.keys(ssot?.prompts || {}).length;
      const intents = Array.isArray(ssot?.intents) ? ssot.intents.length : 0;

      return res.json({ ok: true, ms, settings_keys, prompts_keys, intents });
    } catch (e) {
      return res.status(500).json({ ok: false, error: e.message || String(e) });
    }
  });

  return router;
}

module.exports = { adminReloadRouter };
