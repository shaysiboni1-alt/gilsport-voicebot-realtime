// server.js
// GilSport VoiceBot Realtime - Config Loader + Health
// Render + Google Sheets (Service Account JSON in ENV)
//
// Endpoints:
//   GET /health        -> ok
//   GET /config-check  -> reads Sheet tabs, validates structure, returns summary JSON
//
// ENV required:
//   GOOGLE_SERVICE_ACCOUNT_JSON  (JSON string OR base64 JSON)
//   GSHEET_ID
//
// ENV optional:
//   PORT
//   GSHEET_CACHE_TTL_SEC (default 30)
//   TIME_ZONE (default Asia/Jerusalem)

import express from "express";
import { google } from "googleapis";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = Number(process.env.PORT || 10000);

const ENV = {
  GOOGLE_SERVICE_ACCOUNT_JSON: process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "",
  GSHEET_ID: process.env.GSHEET_ID || "",
  GSHEET_CACHE_TTL_SEC: Number(process.env.GSHEET_CACHE_TTL_SEC || 30),
  TIME_ZONE: process.env.TIME_ZONE || "Asia/Jerusalem",
};

// -------------------- Helpers --------------------
function nowIso() {
  return new Date().toISOString();
}

function safeJsonParse(s) {
  try {
    return { ok: true, value: JSON.parse(s) };
  } catch (e) {
    return { ok: false, error: String(e?.message || e) };
  }
}

function maybeBase64Decode(s) {
  // If user pasted base64 in ENV, this tries to decode it.
  try {
    const buf = Buffer.from(s, "base64");
    const decoded = buf.toString("utf8");
    // Heuristic: decoded should look like JSON
    if (decoded.trim().startsWith("{") && decoded.includes('"type"')) return decoded;
    return null;
  } catch {
    return null;
  }
}

function normalizeServiceAccount(sa) {
  // Fix private_key newlines when copied through ENV
  if (sa?.private_key && typeof sa.private_key === "string") {
    sa.private_key = sa.private_key.replace(/\\n/g, "\n");
  }
  return sa;
}

function splitCsv(s) {
  if (!s) return [];
  return String(s)
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
}

function toNumberOrNull(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

// -------------------- Google Sheets Client --------------------
let sheetsClient = null;

function getSheetsClientOrThrow() {
  if (sheetsClient) return sheetsClient;

  if (!ENV.GOOGLE_SERVICE_ACCOUNT_JSON) {
    throw new Error("Missing ENV: GOOGLE_SERVICE_ACCOUNT_JSON");
  }

  let raw = ENV.GOOGLE_SERVICE_ACCOUNT_JSON.trim();

  // Try JSON parse as-is
  let parsed = safeJsonParse(raw);
  if (!parsed.ok) {
    // Try base64 decode then parse
    const decoded = maybeBase64Decode(raw);
    if (!decoded) {
      throw new Error(`GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON (and not base64 JSON): ${parsed.error}`);
    }
    parsed = safeJsonParse(decoded);
    if (!parsed.ok) {
      throw new Error(`GOOGLE_SERVICE_ACCOUNT_JSON base64 decoded but still invalid JSON: ${parsed.error}`);
    }
  }

  const sa = normalizeServiceAccount(parsed.value);

  if (!sa?.client_email || !sa?.private_key) {
    throw new Error("Service account JSON missing client_email/private_key");
  }

  const jwt = new google.auth.JWT({
    email: sa.client_email,
    key: sa.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });

  sheetsClient = google.sheets({ version: "v4", auth: jwt });
  return sheetsClient;
}

// -------------------- Cache --------------------
const cache = {
  loadedAt: 0,
  ttlMs: 0,
  data: null,
  meta: null,
};

function isCacheValid() {
  if (!cache.data) return false;
  const age = Date.now() - cache.loadedAt;
  return age >= 0 && age < cache.ttlMs;
}

async function fetchRange(spreadsheetId, rangeA1) {
  const sheets = getSheetsClientOrThrow();
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId,
    range: rangeA1,
    valueRenderOption: "UNFORMATTED_VALUE",
    dateTimeRenderOption: "FORMATTED_STRING",
  });
  return res.data.values || [];
}

function tableToObjects(values) {
  // expects first row headers
  if (!values || values.length === 0) return [];
  const headers = values[0].map((h) => String(h || "").trim());
  const rows = [];
  for (let i = 1; i < values.length; i++) {
    const row = values[i];
    const obj = {};
    for (let c = 0; c < headers.length; c++) {
      const key = headers[c];
      if (!key) continue;
      obj[key] = row?.[c] ?? "";
    }
    // skip fully empty rows
    const hasAny = Object.values(obj).some((v) => String(v ?? "").trim() !== "");
    if (hasAny) rows.push(obj);
  }
  return rows;
}

// -------------------- Load full config from Sheet --------------------
async function loadSheetConfig() {
  if (!ENV.GSHEET_ID) throw new Error("Missing ENV: GSHEET_ID");

  const spreadsheetId = ENV.GSHEET_ID;

  // Tabs we expect (based on what you built)
  const tabs = [
    { name: "SETTINGS", range: "A1:C200" },
    { name: "BUSINESS_INFO", range: "A1:B200" },
    { name: "ROUTING_RULES", range: "A1:E200" },
    { name: "SALES_SCRIPT", range: "A1:F300" },
    { name: "SUPPORT_SCRIPT", range: "A1:C200" },
    { name: "SUPPLIERS", range: "A1:E300" },
    { name: "MAKE_PAYLOADS_SPEC", range: "A1:D200" },
    { name: "PROMPTS", range: "A1:C500" } // you wrote a long prompt there; table might be different—still safe to fetch
  ];

  const out = {
    _meta: {
      loaded_at: nowIso(),
      sheet_id: spreadsheetId,
      cache_ttl_sec: ENV.GSHEET_CACHE_TTL_SEC,
      time_zone: ENV.TIME_ZONE,
    },
    SETTINGS: null,
    BUSINESS_INFO: null,
    ROUTING_RULES: null,
    SALES_SCRIPT: null,
    SUPPORT_SCRIPT: null,
    SUPPLIERS: null,
    MAKE_PAYLOADS_SPEC: null,
    PROMPTS: null,
  };

  // Fetch each tab
  for (const t of tabs) {
    const rangeA1 = `${t.name}!${t.range}`;
    const values = await fetchRange(spreadsheetId, rangeA1);

    // For PROMPTS sometimes it’s not a strict table; still we return raw values + best-effort parse.
    if (t.name === "PROMPTS") {
      out.PROMPTS = {
        raw_values: values,
        rows: tableToObjects(values),
      };
      continue;
    }

    out[t.name] = tableToObjects(values);
  }

  // Build convenient maps from SETTINGS + BUSINESS_INFO
  const settingsMap = {};
  if (Array.isArray(out.SETTINGS)) {
    for (const r of out.SETTINGS) {
      const k = String(r.key || "").trim();
      const v = r.value ?? "";
      if (k) settingsMap[k] = v;
    }
  }

  const businessInfoMap = {};
  if (Array.isArray(out.BUSINESS_INFO)) {
    for (const r of out.BUSINESS_INFO) {
      const k = String(r.field || "").trim();
      const v = r.value ?? "";
      if (k) businessInfoMap[k] = v;
    }
  }

  // Basic validations (non-breaking)
  const requiredSettingKeys = [
    "BUSINESS_NAME",
    "DEFAULT_LANGUAGE",
    "SUPPORTED_LANGUAGES",
    "OPENING_TEXT",
    "CLOSING_TEXT",
    "OUTPUT_GAIN_DB",
    "IDLE_WARNING_SEC",
    "IDLE_HANGUP_SEC",
    "MAX_CALL_SEC",
    "MAKE_SEND_WA_URL",
    "MAKE_LEAD_URL",
    "MAKE_SUPPORT_URL",
    "MAKE_ABANDONED_URL",
    "SITE_BASE_URL",
  ];

  const missingSettings = requiredSettingKeys.filter((k) => !(k in settingsMap) || String(settingsMap[k]).trim() === "");

  const supportedLangs = splitCsv(settingsMap.SUPPORTED_LANGUAGES);
  const defaultLang = String(settingsMap.DEFAULT_LANGUAGE || "").trim();
  const languagesOk = defaultLang && supportedLangs.includes(defaultLang);

  // numeric sanity
  const outputGain = toNumberOrNull(settingsMap.OUTPUT_GAIN_DB);
  const idleWarn = toNumberOrNull(settingsMap.IDLE_WARNING_SEC);
  const idleHang = toNumberOrNull(settingsMap.IDLE_HANGUP_SEC);
  const maxCall = toNumberOrNull(settingsMap.MAX_CALL_SEC);

  const numericWarnings = [];
  if (outputGain === null) numericWarnings.push("OUTPUT_GAIN_DB is not a number");
  if (idleWarn === null) numericWarnings.push("IDLE_WARNING_SEC is not a number");
  if (idleHang === null) numericWarnings.push("IDLE_HANGUP_SEC is not a number");
  if (maxCall === null) numericWarnings.push("MAX_CALL_SEC is not a number");
  if (idleWarn !== null && idleHang !== null && idleHang <= idleWarn) numericWarnings.push("IDLE_HANGUP_SEC should be > IDLE_WARNING_SEC");

  out._meta.validation = {
    missing_settings_keys: missingSettings,
    languages_ok: languagesOk,
    default_language: defaultLang,
    supported_languages: supportedLangs,
    numeric_warnings: numericWarnings,
    counts: {
      SETTINGS_rows: Array.isArray(out.SETTINGS) ? out.SETTINGS.length : 0,
      BUSINESS_INFO_rows: Array.isArray(out.BUSINESS_INFO) ? out.BUSINESS_INFO.length : 0,
      ROUTING_RULES_rows: Array.isArray(out.ROUTING_RULES) ? out.ROUTING_RULES.length : 0,
      SALES_SCRIPT_rows: Array.isArray(out.SALES_SCRIPT) ? out.SALES_SCRIPT.length : 0,
      SUPPORT_SCRIPT_rows: Array.isArray(out.SUPPORT_SCRIPT) ? out.SUPPORT_SCRIPT.length : 0,
      SUPPLIERS_rows: Array.isArray(out.SUPPLIERS) ? out.SUPPLIERS.length : 0,
      MAKE_PAYLOADS_SPEC_rows: Array.isArray(out.MAKE_PAYLOADS_SPEC) ? out.MAKE_PAYLOADS_SPEC.length : 0,
      PROMPTS_raw_rows: Array.isArray(out.PROMPTS?.raw_values) ? out.PROMPTS.raw_values.length : 0,
      PROMPTS_table_rows: Array.isArray(out.PROMPTS?.rows) ? out.PROMPTS.rows.length : 0
    }
  };

  // Store maps too (useful for later bot logic)
  out._maps = {
    settings: settingsMap,
    business_info: businessInfoMap,
  };

  return out;
}

async function getConfigCached() {
  if (isCacheValid()) return { fromCache: true, config: cache.data, meta: cache.meta };

  const cfg = await loadSheetConfig();
  cache.data = cfg;
  cache.meta = cfg?._meta || null;
  cache.loadedAt = Date.now();
  cache.ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;

  return { fromCache: false, config: cfg, meta: cache.meta };
}

// -------------------- Routes --------------------
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "gilsport-voicebot-realtime",
    time: nowIso(),
  });
});

app.get("/config-check", async (req, res) => {
  try {
    const { fromCache, config } = await getConfigCached();

    // Return a SAFE summary (no secrets)
    const settings = config?._maps?.settings || {};
    const business = config?._maps?.business_info || {};

    res.json({
      ok: true,
      from_cache: fromCache,
      loaded_at: config?._meta?.loaded_at,
      sheet_id: config?._meta?.sheet_id,
      validation: config?._meta?.validation,
      // helpful “at a glance” values
      overview: {
        BUSINESS_NAME: settings.BUSINESS_NAME || "",
        DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "",
        SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "",
        SITE_BASE_URL: settings.SITE_BASE_URL || "",
        MAIN_PHONE: business.MAIN_PHONE || "",
        BRANCHES: business.BRANCHES || "",
      }
    });
  } catch (e) {
    res.status(500).json({
      ok: false,
      error: String(e?.message || e),
    });
  }
});

// Root
app.get("/", (req, res) => {
  res.type("text/plain").send("GilSport VoiceBot Realtime - up. Try /health or /config-check");
});

// -------------------- Start --------------------
app.listen(PORT, () => {
  console.log(`[BOOT] Listening on :${PORT}`);
  console.log(`[BOOT] /health ready`);
  console.log(`[BOOT] /config-check ready (GSHEET_ID=${ENV.GSHEET_ID ? "set" : "missing"})`);
});
