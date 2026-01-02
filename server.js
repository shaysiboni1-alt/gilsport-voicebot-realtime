// server.js
//
// GilSport VoiceBot Realtime - Config + Router service
// - /health
// - /config-check
// - /route  (keywords first, Gemini fallback)
//
// Notes:
// - Google Sheet is the source of truth (client-controlled).
// - Router is HYBRID: keywords -> Gemini (if enabled).
//
// Node 18+ (Render uses Node 22.x by default)

import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

// ===================== ENV =====================
const ENV = {
  GOOGLE_SERVICE_ACCOUNT_JSON: process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "",
  GSHEET_ID: process.env.GSHEET_ID || "",
  GSHEET_CACHE_TTL_SEC: Number(process.env.GSHEET_CACHE_TTL_SEC || 60),
  TIME_ZONE: process.env.TIME_ZONE || "Asia/Jerusalem",

  // Gemini Router (optional / hybrid)
  GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
  GEMINI_MODEL: process.env.GEMINI_MODEL || "gemini-1.5-flash",

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),
};

// Debug: last Gemini router error (if any)
let LAST_GEMINI_ROUTER_ERROR = "";

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
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function splitKeywords(cell) {
  // supports: comma-separated, newline-separated
  const raw = String(cell || "").trim();
  if (!raw) return [];
  return raw
    .split(/[,|\n]/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

function isDebug() {
  return ENV.LOG_LEVEL === "debug";
}

// ===================== Google Sheet (GVIZ) =====================
// We use the public GVIZ endpoint; sheet must be shared with the service account.
// Tabs expected: SETTINGS, BUSINESS_INFO, ROUTING_RULES, SALES_SCRIPT, SUPPORT_SCRIPT, SUPPLIERS, MAKE_PAYLOADS_SPEC, PROMPTS

function buildGvizUrl(sheetId, tabName) {
  // GVIZ JSON is wrapped; we'll parse out the table via a helper below.
  return `https://docs.google.com/spreadsheets/d/${encodeURIComponent(
    sheetId
  )}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent(tabName)}`;
}

function gvizToRows(gvizText) {
  // GVIZ returns: "/*O_o*/\ngoogle.visualization.Query.setResponse({...});"
  const match = String(gvizText || "").match(/setResponse\(([\s\S]*?)\);?$/);
  if (!match) return { ok: false, error: "GVIZ wrapper not found" };
  const parsed = safeJsonParse(match[1]);
  if (!parsed.ok) return { ok: false, error: parsed.error };

  const table = parsed.value?.table;
  const cols = (table?.cols || []).map((c) => c?.label || "");
  const rows = table?.rows || [];

  const out = rows.map((r) => {
    const obj = {};
    r.c?.forEach((cell, idx) => {
      const key = cols[idx] || `col_${idx}`;
      obj[key] = cell?.v ?? "";
    });
    return obj;
  });

  return { ok: true, rows: out, cols };
}

// ===================== Config Loader + Cache =====================
let CACHE = {
  loaded_at: 0,
  data: null,
};

async function fetchTab(sheetId, tabName) {
  const url = buildGvizUrl(sheetId, tabName);
  const r = await fetch(url);
  const txt = await r.text();
  if (!r.ok) {
    return { ok: false, error: `GVIZ fetch failed (${r.status}) for ${tabName}` };
  }
  const parsed = gvizToRows(txt);
  if (!parsed.ok) return { ok: false, error: parsed.error };
  return { ok: true, rows: parsed.rows, cols: parsed.cols };
}

function rowsToKeyValue(rows) {
  // expects columns: key, value
  const out = {};
  for (const r of rows || []) {
    const key = String(r.key || r.KEY || "").trim();
    if (!key) continue;
    out[key] = String(r.value ?? r.VALUE ?? "").trim();
  }
  return out;
}

async function loadConfigFromSheet(force = false) {
  const ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - CACHE.loaded_at < ttlMs;

  if (!force && CACHE.data && fresh) {
    return { ok: true, from_cache: true, loaded_at: new Date(CACHE.loaded_at).toISOString(), ...CACHE.data };
  }

  if (!ENV.GSHEET_ID) {
    throw new Error("GSHEET_ID missing");
  }

  // Pull tabs
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
    const ft = await fetchTab(ENV.GSHEET_ID, tab);
    if (!ft.ok) throw new Error(ft.error);
    results[tab] = ft.rows;
  }

  const settings = rowsToKeyValue(results.SETTINGS);
  const business_info = results.BUSINESS_INFO || [];
  const routing_rules = results.ROUTING_RULES || [];
  const sales_script = results.SALES_SCRIPT || [];
  const support_script = results.SUPPORT_SCRIPT || [];
  const suppliers = results.SUPPLIERS || [];
  const make_payloads_spec = results.MAKE_PAYLOADS_SPEC || [];
  const prompts = results.PROMPTS || [];

  const cfg = {
    settings,
    business_info,
    routing_rules,
    sales_script,
    support_script,
    suppliers,
    make_payloads_spec,
    prompts,
    overview: {
      BUSINESS_NAME: settings.BUSINESS_NAME || "",
      DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "he",
      SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "he",
      SITE_BASE_URL: settings.SITE_BASE_URL || "",
      MAIN_PHONE: settings.MAIN_PHONE || "",
      BRANCHES: settings.BRANCHES || "",
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

  // Build rule list
  const list = (rules || [])
    .map((r) => ({
      priority: Number(r.priority ?? r.PRIORITY ?? 0),
      route: String(r.route ?? r.ROUTE ?? "").trim(),
      keywords: splitKeywords(r.keywords ?? r.KEYWORDS ?? ""),
      question_if_ambiguous: String(r.question_if_ambiguous ?? r.QUESTION_IF_AMBIGUOUS ?? "").trim(),
      notes: String(r.notes ?? r.NOTES ?? "").trim(),
    }))
    .filter((r) => r.route && r.keywords.length);

  // Higher priority first
  list.sort((a, b) => (b.priority || 0) - (a.priority || 0));

  for (const rule of list) {
    for (const kw of rule.keywords) {
      const k = normalizeText(kw);
      if (!k) continue;
      if (t.includes(k)) {
        const route = rule.route;
        return {
          route,
          matched: kw,
          confidence: 1,
          by: "sheet_keywords",
          question: String(rule.question_if_ambiguous || "").trim() || null,
          priority: Number(rule.priority ?? null),
        };
      }
    }
  }

  return null;
}

// ===================== Router (Gemini fallback) =====================
async function geminiRoute(text, cfg, rules) {
  LAST_GEMINI_ROUTER_ERROR = "";
  if (!ENV.GEMINI_API_KEY) {
    LAST_GEMINI_ROUTER_ERROR = "GEMINI_API_KEY is missing";
    return null;
  }

  const routesList = [...new Set((rules || []).map((r) => r.route).filter(Boolean))];
  if (!routesList.length) {
    LAST_GEMINI_ROUTER_ERROR = "No routes found in ROUTING_RULES";
    return null;
  }

  const businessName = cfg?.settings?.BUSINESS_NAME || cfg?.overview?.BUSINESS_NAME || "העסק";
  const prompt = `
You are a call routing classifier for a Hebrew voicebot.
Business: ${businessName}

Allowed routes: ${routesList.join(", ")}

User message (Hebrew):
"${text}"

Return STRICT JSON only (no markdown, no commentary) with this schema:
{
  "route": "sales|support|ambiguous",
  "confidence": 0..1,
  "reason": "short explanation in Hebrew",
  "question": "ONLY if route=ambiguous: a short clarifying question in Hebrew"
}

Rules:
- Choose sales for pricing, models, products, recommendations, buying intent.
- Choose support for problems, malfunction, warranty/service, delivery issues, repairs.
- Choose ambiguous only if you truly cannot decide.
- Do not output any extra keys.
`.trim();

  const body = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    generationConfig: {
      temperature: 0.1,
      response_mime_type: "application/json",
    },
  };

  const url = `https://generativelanguage.googleapis.com/v1beta/models/${ENV.GEMINI_MODEL}:generateContent?key=${ENV.GEMINI_API_KEY}`;

  try {
    const r = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    const json = await r.json();

    if (!r.ok) {
      LAST_GEMINI_ROUTER_ERROR = `Gemini API HTTP ${r.status}: ${JSON.stringify(json).slice(0, 300)}`;
      return null;
    }

    const cand = json?.candidates?.[0];
    if (!cand?.content?.parts?.length) {
      LAST_GEMINI_ROUTER_ERROR = "Gemini returned no content parts";
      return null;
    }

    const raw = cand.content.parts.map((p) => p.text || "").join("\n").trim();

    // Try to extract an object in case Gemini wraps output (should not, but defensive)
    const objMatch = raw.match(/\{[\s\S]*\}/);
    const parsed = safeJsonParse(objMatch ? objMatch[0] : null);
    if (!parsed.ok) {
      LAST_GEMINI_ROUTER_ERROR = `Gemini output not JSON: ${String(raw).slice(0, 300)}`;
      return null;
    }

    const route = String(parsed.value?.route || "").trim();
    const confidence = Number(parsed.value?.confidence ?? 0);
    const reason = String(parsed.value?.reason || "").trim();
    const question = String(parsed.value?.question || "").trim();

    if (!routesList.includes(route)) {
      LAST_GEMINI_ROUTER_ERROR = `Gemini route not in allowed list: ${route}`;
      return null;
    }

    return {
      route,
      confidence: Number.isFinite(confidence) ? confidence : null,
      reason,
      question: question || null,
    };
  } catch (e) {
    LAST_GEMINI_ROUTER_ERROR = `Gemini fetch/parse error: ${e?.message || String(e)}`;
    return null;
  }
}

// ===================== Endpoints =====================
app.get("/health", (req, res) => {
  res.json({
    ok: true,
    service: "gilsport-voicebot-realtime",
    time: nowIso(),
  });
});

app.get("/config-check", async (req, res) => {
  try {
    // validate service account JSON (optional; depends on your code path)
    const sa = ENV.GOOGLE_SERVICE_ACCOUNT_JSON;
    const parsedSA = safeJsonParse(sa);
    const isBase64Json = (() => {
      try {
        const buf = Buffer.from(sa, "base64");
        const s = buf.toString("utf8");
        const p2 = safeJsonParse(s);
        return p2.ok;
      } catch {
        return false;
      }
    })();

    if (sa && !parsedSA.ok && !isBase64Json) {
      return res.json({
        ok: false,
        error: `GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON (and not base64 JSON): ${parsedSA.error}`,
      });
    }

    const cfg = await loadConfigFromSheet(true);

    // Basic validation summary
    const missing = [];
    const required = [
      "BUSINESS_NAME",
      "DEFAULT_LANGUAGE",
      "SUPPORTED_LANGUAGES",
      "SITE_BASE_URL",
      "MAIN_PHONE",
      "BRANCHES",
    ];
    for (const k of required) {
      if (!String(cfg.settings?.[k] || "").trim()) missing.push(k);
    }

    const supported = String(cfg.settings?.SUPPORTED_LANGUAGES || "he")
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);

    const defaultLang = String(cfg.settings?.DEFAULT_LANGUAGE || "he").trim();

    return res.json({
      ok: true,
      from_cache: cfg.from_cache,
      loaded_at: cfg.loaded_at,
      sheet_id: ENV.GSHEET_ID,
      validation: {
        missing_settings_keys: missing,
        languages_ok: supported.includes(defaultLang),
        default_language: defaultLang,
        supported_languages: supported,
        numeric_warnings: [],
        counts: {
          SETTINGS_rows: (cfg.settings ? Object.keys(cfg.settings).length : 0),
          BUSINESS_INFO_rows: (cfg.business_info || []).length,
          ROUTING_RULES_rows: (cfg.routing_rules || []).length,
          SALES_SCRIPT_rows: (cfg.sales_script || []).length,
          SUPPORT_SCRIPT_rows: (cfg.support_script || []).length,
          SUPPLIERS_rows: (cfg.suppliers || []).length,
          MAKE_PAYLOADS_SPEC_rows: (cfg.make_payloads_spec || []).length,
          PROMPTS_rows: (cfg.prompts || []).length,
        },
      },
      overview: cfg.overview,
      router_llm: {
        enabled: Boolean(ENV.GEMINI_API_KEY),
        provider: "gemini",
        model: ENV.GEMINI_MODEL,
      },
      google_service_account_json: Boolean(ENV.GOOGLE_SERVICE_ACCOUNT_JSON),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.post("/route", async (req, res) => {
  const text = String(req.body?.text || "").trim();

  try {
    const cfg = await loadConfigFromSheet(false);
    const rules = cfg.routing_rules || [];

    // 1) Fast path: deterministic keyword routing from the sheet
    const bySheet = routeByKeywords(text, rules);
    if (bySheet) {
      return res.json({
        ok: true,
        input: { text },
        decision: {
          route: bySheet.route,
          matched: bySheet.matched,
          confidence: bySheet.confidence,
          by: bySheet.by,
          question: bySheet.question || null,
          priority: bySheet.priority ?? null,
        },
      });
    }

    // 2) Fallback: Gemini router (hybrid mode)
    const allowedRoutes = ["sales", "support", "ambiguous"];
    const byGemini = await geminiRoute(text, cfg, rules);

    if (byGemini && allowedRoutes.includes(byGemini.route)) {
      let question = byGemini.question || null;

      // If Gemini says "ambiguous" but didn't provide a question – use the sheet's question_if_ambiguous (if exists).
      if (byGemini.route === "ambiguous" && !question) {
        const ambRule = rules.find(
          (r) =>
            String(r.route || "").trim() === "ambiguous" &&
            String(r.question_if_ambiguous || "").trim()
        );
        question = ambRule ? String(ambRule.question_if_ambiguous).trim() : null;
      }

      return res.json({
        ok: true,
        input: { text },
        decision: {
          route: byGemini.route,
          matched: null,
          confidence: byGemini.confidence ?? null,
          by: "gemini",
          question,
          reason: byGemini.reason || null,
        },
      });
    }

    const geminiEnabled = Boolean(ENV.GEMINI_API_KEY);
    const by =
      geminiEnabled && LAST_GEMINI_ROUTER_ERROR ? "gemini_failed" : "none";

    return res.json({
      ok: true,
      input: { text },
      decision: {
        route: "unknown",
        matched: null,
        confidence: 0,
        by,
        error: geminiEnabled ? LAST_GEMINI_ROUTER_ERROR || null : null,
      },
    });
  } catch (e) {
    return res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

app.get("/", (req, res) => {
  res
    .status(200)
    .send("GilSport VoiceBot Realtime - up. Try /health or /config-check");
});

app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
});
