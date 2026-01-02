// server.js
//
// GilSport VoiceBot Realtime - Config + Router service
// - /health
// - /config-check
// - /route  (keywords first, Gemini fallback)
//
// Google Sheets access via Service Account (JWT) – NO GVIZ
// Node 18+ (Render uses Node 22.x)

import express from "express";
import { google } from "googleapis";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

// ===================== ENV =====================
const ENV = {
  GOOGLE_SERVICE_ACCOUNT_JSON: process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "",
  GSHEET_ID: process.env.GSHEET_ID || "",
  GSHEET_CACHE_TTL_SEC: Number(process.env.GSHEET_CACHE_TTL_SEC || 60),
  TIME_ZONE: process.env.TIME_ZONE || "Asia/Jerusalem",

  GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
  // IMPORTANT: Must include "models/"
  GEMINI_MODEL: process.env.GEMINI_MODEL || "models/gemini-1.5-flash",

  // network tuning
  GEMINI_TIMEOUT_MS: Number(process.env.GEMINI_TIMEOUT_MS || 9000),

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),
};

let LAST_GEMINI_ROUTER_ERROR = "";

// ===================== Logging =====================
const LEVELS = { error: 0, warn: 1, info: 2, debug: 3 };
function log(level, msg, extra) {
  const cur = LEVELS[ENV.LOG_LEVEL] ?? 2;
  const lv = LEVELS[level] ?? 2;
  if (lv > cur) return;
  const base = `[${level.toUpperCase()}] ${msg}`;
  if (extra !== undefined) console.log(base, extra);
  else console.log(base);
}

// ===================== Utils =====================
function safeJsonParse(maybeJson) {
  try {
    if (!maybeJson) return { ok: false, error: "empty" };
    return { ok: true, value: JSON.parse(maybeJson) };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}

function nowIso() {
  return new Date().toISOString();
}

function normalizeText(s) {
  return String(s || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function splitKeywords(cell) {
  return String(cell || "")
    .split(/[,|\n]/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

function stripCodeFences(s) {
  const t = String(s || "").trim();
  // remove ```json ... ```
  if (t.startsWith("```")) {
    return t.replace(/^```[a-zA-Z]*\n?/, "").replace(/```$/, "").trim();
  }
  return t;
}

// ===================== Google Sheets (Service Account) =====================
function getServiceAccountAuth() {
  const raw = ENV.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON missing");

  let creds;
  const parsed = safeJsonParse(raw);
  if (parsed.ok) {
    creds = parsed.value;
  } else {
    // base64 support
    const buf = Buffer.from(raw, "base64");
    creds = JSON.parse(buf.toString("utf8"));
  }

  return new google.auth.JWT({
    email: creds.client_email,
    key: creds.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"],
  });
}

async function fetchSheetTab(auth, sheetId, tabName) {
  const sheets = google.sheets({ version: "v4", auth });

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: sheetId,
    range: tabName,
  });

  const rows = res.data.values || [];
  if (!rows.length) return [];

  const headers = rows[0];
  return rows.slice(1).map((r) => {
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = r[i] ?? "";
    });
    return obj;
  });
}

// ===================== Config Loader + Cache =====================
let CACHE = {
  loaded_at: 0,
  data: null,
};

async function loadConfigFromSheet(force = false) {
  const ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - CACHE.loaded_at < ttlMs;

  if (!force && CACHE.data && fresh) {
    return {
      ok: true,
      from_cache: true,
      loaded_at: new Date(CACHE.loaded_at).toISOString(),
      ...CACHE.data,
    };
  }

  if (!ENV.GSHEET_ID) throw new Error("GSHEET_ID missing");

  const auth = getServiceAccountAuth();
  await auth.authorize();

  const tabs = [
    "SETTINGS",
    "BUSINESS_INFO",
    "ROUTING_RULES",
    "SALES_SCRIPT",
    "SUPPORT_SCRIPT",
    "SUPPLIERS",
    "MAKE_PAYLOADS_SPEC",
    "PROMPTS",
  ];

  const results = {};
  for (const tab of tabs) {
    results[tab] = await fetchSheetTab(auth, ENV.GSHEET_ID, tab);
  }

  const settings = {};
  for (const r of results.SETTINGS || []) {
    const k = String(r.key || "").trim();
    if (k) settings[k] = String(r.value ?? "").trim();
  }

  // Convenience: if BUSINESS_INFO has field/value rows, map them too (optional)
  const businessInfoMap = {};
  for (const r of results.BUSINESS_INFO || []) {
    const f = String(r.field || "").trim();
    if (f) businessInfoMap[f] = String(r.value ?? "").trim();
  }

  const cfg = {
    settings,
    business_info: results.BUSINESS_INFO || [],
    business_info_map: businessInfoMap,
    routing_rules: results.ROUTING_RULES || [],
    sales_script: results.SALES_SCRIPT || [],
    support_script: results.SUPPORT_SCRIPT || [],
    suppliers: results.SUPPLIERS || [],
    make_payloads_spec: results.MAKE_PAYLOADS_SPEC || [],
    prompts: results.PROMPTS || [],
    overview: {
      BUSINESS_NAME: settings.BUSINESS_NAME || "",
      DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "he",
      SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "he",
      SITE_BASE_URL: settings.SITE_BASE_URL || "",
      MAIN_PHONE: settings.MAIN_PHONE || businessInfoMap.MAIN_PHONE || "",
      BRANCHES: settings.BRANCHES || businessInfoMap.BRANCHES || "",
    },
  };

  CACHE = {
    loaded_at: Date.now(),
    data: cfg,
  };

  return { ok: true, from_cache: false, loaded_at: nowIso(), ...cfg };
}

// ===================== Router (Keywords) =====================
function routeByKeywords(text, rules) {
  const t = normalizeText(text);
  if (!t) return null;

  const list = (rules || [])
    .map((r) => ({
      priority: Number(r.priority || 0),
      route: String(r.route || "").trim(),
      keywords: splitKeywords(r.keywords || ""),
      question_if_ambiguous: String(r.question_if_ambiguous || "").trim(),
    }))
    .filter((r) => r.route && r.keywords.length)
    .sort((a, b) => b.priority - a.priority);

  for (const rule of list) {
    for (const kw of rule.keywords) {
      if (t.includes(normalizeText(kw))) {
        return {
          route: rule.route,
          matched: kw,
          confidence: 1,
          by: "sheet_keywords",
          question: rule.question_if_ambiguous || null,
          priority: rule.priority,
        };
      }
    }
  }
  return null;
}

function getAmbiguousQuestionFromSheet(rules) {
  // choose highest priority row that has a question_if_ambiguous
  const list = (rules || [])
    .map((r) => ({
      priority: Number(r.priority || 0),
      route: String(r.route || "").trim(),
      question_if_ambiguous: String(r.question_if_ambiguous || "").trim(),
    }))
    .filter((r) => r.question_if_ambiguous)
    .sort((a, b) => b.priority - a.priority);

  // Prefer explicit "ambiguous" rows, else any question is fine
  const amb = list.find((r) => r.route === "ambiguous");
  return (amb?.question_if_ambiguous || list[0]?.question_if_ambiguous || "").trim();
}

// ===================== Gemini Fallback =====================
async function geminiRoute(text) {
  LAST_GEMINI_ROUTER_ERROR = "";

  if (!ENV.GEMINI_API_KEY) {
    LAST_GEMINI_ROUTER_ERROR = "GEMINI_API_KEY missing";
    throw new Error(LAST_GEMINI_ROUTER_ERROR);
  }
  if (!ENV.GEMINI_MODEL || !String(ENV.GEMINI_MODEL).startsWith("models/")) {
    LAST_GEMINI_ROUTER_ERROR = `GEMINI_MODEL invalid (must start with "models/"): ${ENV.GEMINI_MODEL}`;
    throw new Error(LAST_GEMINI_ROUTER_ERROR);
  }

  const url = `https://generativelanguage.googleapis.com/v1beta/${ENV.GEMINI_MODEL}:generateContent?key=${encodeURIComponent(
    ENV.GEMINI_API_KEY
  )}`;

  const prompt = [
    "You are a routing classifier for a retail sports store phone assistant.",
    "Return ONLY valid JSON (no markdown, no code fences).",
    'Schema: {"route":"sales|support|ambiguous","confidence":0.0-1.0,"reason":"short"}',
    "Rules:",
    "- sales: product interest, pricing, models, recommendations, buying, availability, promotions.",
    "- support: issues, defects, warranty, returns, shipping problems, complaints, service/repair.",
    "- ambiguous: unclear, general, or needs clarification.",
    "User text:",
    text,
  ].join("\n");

  const body = {
    contents: [
      {
        role: "user",
        parts: [{ text: prompt }],
      },
    ],
    generationConfig: {
      temperature: 0,
      maxOutputTokens: 200,
    },
  };

  const controller = new AbortController();
  const to = setTimeout(() => controller.abort(), ENV.GEMINI_TIMEOUT_MS);

  let resp;
  let rawText = "";
  try {
    resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    rawText = await resp.text();

    if (!resp.ok) {
      // Log a short chunk of the body for debugging
      const chunk = rawText?.slice(0, 600);
      LAST_GEMINI_ROUTER_ERROR = `Gemini HTTP ${resp.status} ${resp.statusText} | body: ${chunk}`;
      throw new Error(LAST_GEMINI_ROUTER_ERROR);
    }
  } catch (e) {
    if (e?.name === "AbortError") {
      LAST_GEMINI_ROUTER_ERROR = `Gemini timeout after ${ENV.GEMINI_TIMEOUT_MS}ms`;
      throw new Error(LAST_GEMINI_ROUTER_ERROR);
    }
    LAST_GEMINI_ROUTER_ERROR = e?.message || String(e);
    throw new Error(LAST_GEMINI_ROUTER_ERROR);
  } finally {
    clearTimeout(to);
  }

  // Parse response: take the first candidate text
  let textOut = "";
  try {
    const j = JSON.parse(rawText);
    textOut =
      j?.candidates?.[0]?.content?.parts?.map((p) => p?.text || "").join("") ||
      j?.candidates?.[0]?.content?.parts?.[0]?.text ||
      "";
  } catch (e) {
    const chunk = rawText?.slice(0, 600);
    LAST_GEMINI_ROUTER_ERROR = `Gemini response not JSON (outer). body: ${chunk}`;
    throw new Error(LAST_GEMINI_ROUTER_ERROR);
  }

  const cleaned = stripCodeFences(textOut);
  const parsed = safeJsonParse(cleaned);
  if (!parsed.ok) {
    LAST_GEMINI_ROUTER_ERROR = `Gemini inner JSON parse failed: ${parsed.error} | text: ${cleaned.slice(
      0,
      400
    )}`;
    throw new Error(LAST_GEMINI_ROUTER_ERROR);
  }

  const route = String(parsed.value?.route || "").trim();
  const confidence = Number(parsed.value?.confidence ?? 0);
  const okRoute = route === "sales" || route === "support" || route === "ambiguous";
  if (!okRoute) {
    LAST_GEMINI_ROUTER_ERROR = `Gemini returned invalid route: ${route}`;
    throw new Error(LAST_GEMINI_ROUTER_ERROR);
  }

  return {
    route,
    confidence: Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0,
    reason: String(parsed.value?.reason || "").trim(),
  };
}

// ===================== Endpoints =====================
app.get("/health", (req, res) => {
  res.json({ ok: true, time: nowIso() });
});

app.get("/config-check", async (req, res) => {
  try {
    const cfg = await loadConfigFromSheet(true);
    res.json({
      ok: true,
      from_cache: cfg.from_cache,
      loaded_at: cfg.loaded_at,
      overview: cfg.overview,
      counts: {
        SETTINGS: Object.keys(cfg.settings).length,
        ROUTING_RULES: cfg.routing_rules.length,
        SALES_SCRIPT: cfg.sales_script.length,
        SUPPORT_SCRIPT: cfg.support_script.length,
      },
      gemini: {
        configured: Boolean(ENV.GEMINI_API_KEY),
        model: ENV.GEMINI_MODEL,
        timeout_ms: ENV.GEMINI_TIMEOUT_MS,
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/route", async (req, res) => {
  const debug = String(req.query?.debug || "") === "1";

  try {
    const text = String(req.body?.text || "").trim();
    const cfg = await loadConfigFromSheet(false);

    // 1) Sheet keywords first
    const bySheet = routeByKeywords(text, cfg.routing_rules);
    if (bySheet) {
      return res.json({ ok: true, decision: bySheet });
    }

    // 2) Gemini fallback (only if not found in sheet)
    try {
      const g = await geminiRoute(text);

      // If ambiguous, attach question from sheet (best available)
      const question =
        g.route === "ambiguous" ? getAmbiguousQuestionFromSheet(cfg.routing_rules) || null : null;

      const decision = {
        route: g.route,
        confidence: g.confidence,
        by: "gemini",
        question,
      };

      // keep output minimal, but log reason in debug level
      log("debug", "Gemini decision", { decision, reason: g.reason });

      return res.json({ ok: true, decision });
    } catch (ge) {
      // IMPORTANT: log the actual Gemini error so Render shows it
      log("error", "Gemini router failed", LAST_GEMINI_ROUTER_ERROR || ge?.message || String(ge));

      const resp = {
        ok: true,
        decision: {
          route: "unknown",
          confidence: 0,
          by: "gemini_failed",
        },
      };

      // Only if debug=1, include error detail (does not change normal contract)
      if (debug) {
        resp.debug = {
          gemini_error: LAST_GEMINI_ROUTER_ERROR || ge?.message || String(ge),
          gemini_model: ENV.GEMINI_MODEL,
          timeout_ms: ENV.GEMINI_TIMEOUT_MS,
        };
      }

      return res.json(resp);
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
  console.log(
    `[BOOT] gemini model=${ENV.GEMINI_MODEL} api_key=${ENV.GEMINI_API_KEY ? "set" : "missing"}`
  );
});
