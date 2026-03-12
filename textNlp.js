"use strict";

const { google } = require("googleapis");
const { env } = require("../config/env");
const { logger } = require("../utils/logger");

let CACHE = {
  loaded_at: null,
  expires_at: 0,
  settings: {},
  prompts: {},
  intents: []
};

function stripOuterQuotes(s) {
  if (typeof s !== "string") return s;
  const t = s.trim();
  if (
    (t.startsWith('"') && t.endsWith('"')) ||
    (t.startsWith("'") && t.endsWith("'"))
  ) {
    return t.slice(1, -1);
  }
  return t;
}

function b64ToJson(b64) {
  const raw = stripOuterQuotes(b64 || "");
  if (!raw) return null;
  const jsonStr = Buffer.from(raw, "base64").toString("utf8");
  return JSON.parse(jsonStr);
}

function isCacheValid() {
  return Date.now() < (CACHE.expires_at || 0) && CACHE.loaded_at;
}

async function getSheetsClient() {
  const sheetId = (env.GSHEET_ID || "").trim();
  if (!sheetId) throw new Error("Missing GSHEET_ID");

  const sa = b64ToJson(env.GOOGLE_SERVICE_ACCOUNT_JSON_B64);
  if (!sa || !sa.client_email || !sa.private_key) {
    throw new Error("Missing/invalid GOOGLE_SERVICE_ACCOUNT_JSON_B64");
  }

  const auth = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  });

  const sheets = google.sheets({ version: "v4", auth });
  return { sheets, sheetId };
}

function dropHeader(values) {
  if (!values || values.length === 0) return [];
  return values.slice(1);
}

function rowsToKV(rows) {
  const out = {};
  for (const r of rows || []) {
    const k = (r?.[0] ?? "").toString().trim();
    const v = r?.[1];
    if (!k) continue;
    out[k] = v === undefined || v === null ? "" : String(v);
  }
  return out;
}

function rowsToPrompts(rows) {
  const out = {};
  for (const r of rows || []) {
    const id = (r?.[0] ?? "").toString().trim();
    const content = r?.[1];
    if (!id) continue;
    out[id] = content === undefined || content === null ? "" : String(content);
  }
  return out;
}

function rowsToIntents(rows) {
  const intents = [];
  for (const r of rows || []) {
    const intent_id = (r?.[0] ?? "").toString().trim();
    if (!intent_id) continue;
    intents.push({
      intent_id,
      intent_type: (r?.[1] ?? "").toString().trim(),
      priority: Number((r?.[2] ?? "0").toString().trim()) || 0,
      triggers_he: (r?.[3] ?? "").toString(),
      triggers_en: (r?.[4] ?? "").toString(),
      triggers_ru: (r?.[5] ?? "").toString()
    });
  }
  return intents;
}

/**
 * loadSSOT(force)
 * - force=false: respects cache ttl
 * - force=true : reloads now
 */
async function loadSSOT(force = false) {
  const ttl = env.SSOT_TTL_MS || 60000;
  if (!force && isCacheValid()) return CACHE;

  const startedAt = Date.now();
  const { sheets, sheetId } = await getSheetsClient();

  // IMPORTANT: rely on returned order, not vr.range string
  const ranges = ["SETTINGS!A:B", "PROMPTS!A:B", "INTENTS!A:F"];

  const resp = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: sheetId,
    ranges
  });

  const vrs = resp?.data?.valueRanges || [];

  // Google may return range strings like SETTINGS!A1:B200 - don't key by it.
  const settingsVals = vrs?.[0]?.values || [];
  const promptsVals = vrs?.[1]?.values || [];
  const intentsVals = vrs?.[2]?.values || [];

  const settingsRows = dropHeader(settingsVals);
  const promptsRows = dropHeader(promptsVals);
  const intentsRows = dropHeader(intentsVals);

  const settings = rowsToKV(settingsRows);
  const prompts = rowsToPrompts(promptsRows);
  const intents = rowsToIntents(intentsRows);

  CACHE = {
    loaded_at: new Date().toISOString(),
    expires_at: Date.now() + ttl,
    settings,
    prompts,
    intents
  };

  logger.info("SSOT loaded", {
    settings_keys: Object.keys(settings).length,
    prompts_keys: Object.keys(prompts).length,
    intents: intents.length,
    ms: Date.now() - startedAt,
    ranges_returned: vrs.map((x) => x.range)
  });

  return CACHE;
}

function getSSOT() {
  return CACHE;
}

module.exports = { loadSSOT, getSSOT };
