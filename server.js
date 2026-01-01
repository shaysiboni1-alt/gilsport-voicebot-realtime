// server.js
// GilSport VoiceBot Realtime - Config Loader + Health + Routing (NO Gemini/Twilio yet)
//
// Endpoints:
//   GET  /health
//   GET  /config-check     -> reads all tabs, validation summary
//   POST /route            -> route decision: sales/support/unknown
//   POST /simulate         -> very basic "dry-run" reply based on route
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
  try {
    const decoded = Buffer.from(s, "base64").toString("utf8");
    if (decoded.trim().startsWith("{") && decoded.includes('"type"')) return decoded;
    return null;
  } catch {
    return null;
  }
}
function normalizeServiceAccount(sa) {
  if (sa?.private_key && typeof sa.private_key === "string") {
    sa.private_key = sa.private_key.replace(/\\n/g, "\n");
  }
  return sa;
}
function splitByDelims(s) {
  // allows patterns like: "קנייה; מחיר | הזמנה, רכישה"
  return String(s || "")
    .split(/[,;|]/g)
    .map((x) => x.trim())
    .filter(Boolean);
}
function normText(s) {
  return String(s || "").toLowerCase().trim();
}
function containsAny(text, patterns) {
  const t = normText(text);
  for (const p of patterns) {
    const pp = normText(p);
    if (!pp) continue;
    if (t.includes(pp)) return pp;
  }
  return null;
}
function pick(obj, keys) {
  for (const k of keys) {
    if (obj && obj[k] !== undefined && obj[k] !== null && String(obj[k]).trim() !== "") return obj[k];
  }
  return "";
}

// -------------------- Google Sheets Client --------------------
let sheetsClient = null;

function getSheetsClientOrThrow() {
  if (sheetsClient) return sheetsClient;

  if (!ENV.GOOGLE_SERVICE_ACCOUNT_JSON) {
    throw new Error("Missing ENV: GOOGLE_SERVICE_ACCOUNT_JSON");
  }

  const raw = ENV.GOOGLE_SERVICE_ACCOUNT_JSON.trim();
  let parsed = safeJsonParse(raw);

  if (!parsed.ok) {
    const decoded = maybeBase64Decode(raw);
    if (!decoded) throw new Error(`Invalid GOOGLE_SERVICE_ACCOUNT_JSON: ${parsed.error}`);
    parsed = safeJsonParse(decoded);
    if (!parsed.ok) throw new Error(`Invalid base64 GOOGLE_SERVICE_ACCOUNT_JSON: ${parsed.error}`);
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
  if (!values || values.length === 0) return [];
  const headers = (values[0] || []).map((h) => String(h || "").trim());
  const rows = [];
  for (let i = 1; i < values.length; i++) {
    const row = values[i] || [];
    const obj = {};
    for (let c = 0; c < headers.length; c++) {
      const key = headers[c];
      if (!key) continue;
      obj[key] = row?.[c] ?? "";
    }
    const hasAny = Object.values(obj).some((v) => String(v ?? "").trim() !== "");
    if (hasAny) rows.push(obj);
  }
  return rows;
}

// -------------------- Cache --------------------
const cache = {
  loadedAt: 0,
  ttlMs: 0,
  data: null,
};
function isCacheValid() {
  if (!cache.data) return false;
  const age = Date.now() - cache.loadedAt;
  return age >= 0 && age < cache.ttlMs;
}

async function loadSheetConfig() {
  if (!ENV.GSHEET_ID) throw new Error("Missing ENV: GSHEET_ID");

  const spreadsheetId = ENV.GSHEET_ID;

  const tabs = [
    { name: "SETTINGS", range: "A1:C200" },
    { name: "BUSINESS_INFO", range: "A1:B200" },
    { name: "ROUTING_RULES", range: "A1:E200" },
    { name: "SALES_SCRIPT", range: "A1:F400" },
    { name: "SUPPORT_SCRIPT", range: "A1:C300" },
    { name: "SUPPLIERS", range: "A1:E400" },
    { name: "MAKE_PAYLOADS_SPEC", range: "A1:D200" },
    { name: "PROMPTS", range: "A1:Z800" },
  ];

  const out = {
    _meta: {
      loaded_at: nowIso(),
      sheet_id: spreadsheetId,
      cache_ttl_sec: ENV.GSHEET_CACHE_TTL_SEC,
      time_zone: ENV.TIME_ZONE,
    },
    SETTINGS: [],
    BUSINESS_INFO: [],
    ROUTING_RULES: [],
    SALES_SCRIPT: [],
    SUPPORT_SCRIPT: [],
    SUPPLIERS: [],
    MAKE_PAYLOADS_SPEC: [],
    PROMPTS: { raw_values: [], rows: [] },
    _maps: { settings: {}, business: {}, prompts: {} },
  };

  // Fetch each tab
  for (const t of tabs) {
    const rangeA1 = `${t.name}!${t.range}`;
    const values = await fetchRange(spreadsheetId, rangeA1);

    if (t.name === "PROMPTS") {
      const rows = tableToObjects(values);
      out.PROMPTS = { raw_values: values, rows };
      // best-effort prompt map: try columns like key/text OR Key/Text etc
      const pmap = {};
      for (const r of rows) {
        const k = String(pick(r, ["key", "Key", "KEY"]) || "").trim();
        const txt = String(pick(r, ["text", "Text", "TEXT", "value", "Value"]) || "").trim();
        if (k) pmap[k] = txt;
      }
      out._maps.prompts = pmap;
      continue;
    }

    out[t.name] = tableToObjects(values);
  }

  // Build settings map (expects columns key/value)
  const settingsMap = {};
  for (const r of out.SETTINGS) {
    const k = String(pick(r, ["key", "Key", "KEY"]) || "").trim();
    const v = String(pick(r, ["value", "Value", "VALUE"]) ?? "").trim();
    if (k) settingsMap[k] = v;
  }

  // Build business map (field/value OR key/value)
  const businessMap = {};
  for (const r of out.BUSINESS_INFO) {
    const k = String(pick(r, ["field", "Field", "key", "Key"]) || "").trim();
    const v = String(pick(r, ["value", "Value"]) ?? "").trim();
    if (k) businessMap[k] = v;
  }

  out._maps.settings = settingsMap;
  out._maps.business = businessMap;

  // Validation (non-blocking)
  const requiredSettingKeys = [
    "BUSINESS_NAME",
    "DEFAULT_LANGUAGE",
    "SUPPORTED_LANGUAGES",
    "OPENING_TEXT",
    "CLOSING_TEXT",
    "SITE_BASE_URL",
    "MAKE_SEND_WA_URL",
    "MAKE_LEAD_URL",
    "MAKE_SUPPORT_URL",
    "MAKE_ABANDONED_URL",
  ];

  const missing = requiredSettingKeys.filter((k) => !(k in settingsMap) || String(settingsMap[k]).trim() === "");
  const supportedLangs = String(settingsMap.SUPPORTED_LANGUAGES || "")
    .split(",")
    .map((x) => x.trim())
    .filter(Boolean);
  const defaultLang = String(settingsMap.DEFAULT_LANGUAGE || "").trim();
  const languagesOk = defaultLang && supportedLangs.includes(defaultLang);

  out._meta.validation = {
    missing_settings_keys: missing,
    languages_ok: !!languagesOk,
    default_language: defaultLang,
    supported_languages: supportedLangs,
    numeric_warnings: [],
    counts: {
      SETTINGS_rows: out.SETTINGS.length,
      BUSINESS_INFO_rows: out.BUSINESS_INFO.length,
      ROUTING_RULES_rows: out.ROUTING_RULES.length,
      SALES_SCRIPT_rows: out.SALES_SCRIPT.length,
      SUPPORT_SCRIPT_rows: out.SUPPORT_SCRIPT.length,
      SUPPLIERS_rows: out.SUPPLIERS.length,
      MAKE_PAYLOADS_SPEC_rows: out.MAKE_PAYLOADS_SPEC.length,
      PROMPTS_raw_rows: out.PROMPTS.raw_values.length,
      PROMPTS_table_rows: out.PROMPTS.rows.length,
    },
  };

  return out;
}

async function getConfigCached() {
  if (isCacheValid()) return { fromCache: true, config: cache.data };

  const cfg = await loadSheetConfig();
  cache.data = cfg;
  cache.loadedAt = Date.now();
  cache.ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  return { fromCache: false, config: cfg };
}

// -------------------- Routing Engine (NO LLM) --------------------
function decideRoute(text, routingRules) {
  const t = String(text || "").trim();
  if (!t) return { route: "unknown", matched: null, confidence: 0 };

  // Expect rules with columns like: intent, route, description (from your spec)
  // We match if text contains any token from intent (split by , ; |)
  let best = null;

  for (const r of routingRules || []) {
    const intentRaw = pick(r, ["intent", "Intent", "INTENT"]);
    const routeRaw = pick(r, ["route", "Route", "ROUTE"]);
    const desc = pick(r, ["description", "Description", "DESC"]) || "";

    const patterns = splitByDelims(intentRaw);
    const hit = containsAny(t, patterns);
    if (!hit) continue;

    // score: longer match wins
    const score = hit.length;

    if (!best || score > best.score) {
      best = {
        score,
        route: String(routeRaw || "").trim() || "unknown",
        matched: {
          intent: String(intentRaw || ""),
          matched_token: hit,
          description: String(desc || ""),
        },
      };
    }
  }

  if (!best) return { route: "unknown", matched: null, confidence: 0 };

  // Basic confidence: token length bucket
  const confidence = best.score >= 6 ? 0.8 : best.score >= 3 ? 0.6 : 0.4;

  return { route: best.route, matched: best.matched, confidence };
}

// -------------------- Responses (Dry-run) --------------------
function getText(cfg, key, fallback) {
  const v = cfg?._maps?.settings?.[key] || cfg?._maps?.prompts?.[key];
  return String(v || fallback || "").trim();
}

function simulateReply(cfg, route, userText) {
  const opening = getText(cfg, "OPENING_TEXT", "שָׁלוֹם, אֵיךְ אֶפְשָׁר לַעֲזוֹר?");
  const closing = getText(cfg, "CLOSING_TEXT", "תּוֹדָה שֶׁפָּנִיתֶם. יוֹם טוֹב!");

  const askMore = getText(cfg, "ASK_MORE_HELP", "הַאִם יֵשׁ עוֹד מַשֶּׁהוּ שֶׁאֶפְשָׁר לַעֲזוֹר?");
  const salesNudge = getText(
    cfg,
    "SALES_NEXT_STEP",
    "כְּדֵי שֶׁאֶעֱזֹר בְּדִיּוּק—עַל אֵיזֶה מוּצָר מְדֻבָּר? אֶפְשָׁר לְתָאֵר אוֹ לְתֵת שֵׁם/דֶּגֶם. אִם תִּרְצוּ, אֶשְׁלַח גַּם קִישּׁוּר בְּוָאטְסְאַפּ."
  );
  const supportNudge = getText(
    cfg,
    "SUPPORT_NEXT_STEP",
    "בְּסֵדֶר. תּוּכְלוּ לְתָאֵר אֶת הַתַּקָּלָה בְּמִשְׁפָּט אֶחָד? אִם תִּרְצוּ, אֶפְתַּח פְּנִיָּה לַצֶּוֶת וַאֲבַקֵּשׁ שֶׁיַּחְזְרוּ אֲלֵיכֶם."
  );
  const clarify = getText(
    cfg,
    "CLARIFY_INTENT",
    "רַק לְוִדּוּי: זֶה בִּירוּר לִרְכִישָׁה/מְחִיר, אוֹ עֶזְרָה בִּתְמִיכָה/תַּקָּלָה?"
  );

  // Dry-run only: no real actions yet
  const actions = [];

  let reply = "";
  if (!userText || !String(userText).trim()) {
    reply = opening;
  } else if (route === "sales") {
    reply = salesNudge;
    actions.push({ type: "OFFER_WHATSAPP_LINK", note: "Later will call MAKE_SEND_WA_URL with product url" });
    actions.push({ type: "OFFER_LEAD_CAPTURE", note: "Later will call MAKE_LEAD_URL if user wants order" });
  } else if (route === "support") {
    reply = supportNudge;
    actions.push({ type: "OFFER_SUPPORT_TICKET", note: "Later will call MAKE_SUPPORT_URL with issue summary" });
  } else {
    reply = clarify;
  }

  // In voice we usually ask if anything else, and then close if user says no (later stage)
  return { reply, actions, suggested_close: closing, ask_more: askMore };
}

// -------------------- Routes --------------------
app.get("/health", (_req, res) => {
  res.json({ ok: true, service: "gilsport-voicebot-realtime", time: nowIso() });
});

app.get("/config-check", async (_req, res) => {
  try {
    const { config, fromCache } = await getConfigCached();
    const settings = config?._maps?.settings || {};
    const business = config?._maps?.business || {};

    res.json({
      ok: true,
      from_cache: fromCache,
      loaded_at: config?._meta?.loaded_at,
      sheet_id: config?._meta?.sheet_id,
      validation: config?._meta?.validation,
      overview: {
        BUSINESS_NAME: settings.BUSINESS_NAME || "",
        DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "",
        SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "",
        SITE_BASE_URL: settings.SITE_BASE_URL || "",
        MAIN_PHONE: business.MAIN_PHONE || "",
        BRANCHES: business.BRANCHES || "",
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post("/route", async (req, res) => {
  try {
    const text = String(req.body?.text || "");
    const { config } = await getConfigCached();

    const decision = decideRoute(text, config.ROUTING_RULES);

    res.json({
      ok: true,
      input: { text },
      decision,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.post("/simulate", async (req, res) => {
  try {
    const text = String(req.body?.text || "");
    const { config } = await getConfigCached();

    const decision = decideRoute(text, config.ROUTING_RULES);
    const sim = simulateReply(config, decision.route, text);

    res.json({
      ok: true,
      input: { text },
      decision,
      simulation: sim,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

app.get("/", (_req, res) => {
  res.type("text/plain").send("GilSport VoiceBot Realtime - up. Try /health, /config-check, POST /route, POST /simulate");
});

// -------------------- Start --------------------
app.listen(PORT, () => {
  console.log(`[BOOT] Listening on :${PORT}`);
});
