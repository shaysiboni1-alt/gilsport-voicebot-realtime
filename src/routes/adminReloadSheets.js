"use strict";

const express = require("express");
const { loadSSOT } = require("../ssot/ssotClient");

// POST /admin/reload-sheets
// Forces SSOT reload from Google Sheets.
// Protected by x-admin-token header (must match TWILIO_AUTH_TOKEN env)

const router = express.Router();

router.post("/admin/reload-sheets", async (req, res) => {
  try {
    const adminToken = String(req.headers["x-admin-token"] || "").trim();
    const expectedToken = String(process.env.TWILIO_AUTH_TOKEN || "").trim();

    if (!expectedToken || adminToken !== expectedToken) {
      return res.status(401).json({ ok: false, error: "unauthorized" });
    }

    const t0 = Date.now();
    const ssot = await loadSSOT(true); // force reload
    const ms = Date.now() - t0;

    const settings_keys = Object.keys(ssot?.settings || {}).length;
    const prompts_keys = Object.keys(ssot?.prompts || {}).length;
    const intents = Array.isArray(ssot?.intents) ? ssot.intents.length : 0;

    return res.json({
      ok: true,
      ms,
      reloaded_at: new Date().toISOString(),
      settings_keys,
      prompts_keys,
      intents,
    });

  } catch (e) {
    return res.status(500).json({
      ok: false,
      error: e?.message || String(e),
    });
  }
});

module.exports = { adminReloadRouter: router };
