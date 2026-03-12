"use strict";

const express = require("express");
const { env } = require("../config/env");

const router = express.Router();

router.get("/health", (req, res) => {
  return res.status(200).json({
    status: "ok",
    service: "VoiceBot_Blank",
    provider_mode: env.PROVIDER_MODE,
    time: new Date().toISOString()
  });
});

module.exports = { healthRouter: router };
