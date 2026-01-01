// server.js (ESM)
// GilSport VoiceBot Realtime - Config Loader + Router (Sheet keywords + Gemini fallback)
// Node 18+ (Render uses Node 22) - fetch is available globally.

import express from "express";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

// ===================== ENV =====================
const ENV = {
  TIME_ZONE: process.env.TIME_ZONE || "Asia/Jerusalem",

  // Google Sheet config
  GSHEET_ID: process.env.GSHEET_ID || "",
  GSHEET_CACHE_TTL_SEC: Number(process.env.GSHEET_CACHE_TTL_SEC || "60"),

  // Service account JSON: raw JSON or base64(JSON)
  GOOGLE_SERVICE_ACCOUNT_JSON: process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "",

  // Router LLM (Gemini)
  GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
  GEMINI_MODEL: process.env.GEMINI_MODEL || "gemini-1.5-flash",

  // Debug
  MB_DEBUG: String(process.env.MB_DEBUG || "").toLowerCase() === "true",
};

function log(...args) {
  if (ENV.MB_DEBUG) console.log("[DEBUG]", ...args);
}

// ===================== Helpers =====================
function nowISO() {
  return new Date().toISOString();
}

function safeJsonParse(str) {
  try {
    return { ok: true, value: JSON.parse(str) };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}

function maybeBase64ToString(s) {
  // detect base64-ish (no braces) and try decode
  if (!s) return s;
  const trimmed = String(s).trim();
  if (trimmed.startsWith("{")) return trimmed;
  // Try base64 decode
  try {
    const buf = Buffer.from(trimmed, "base64");
    const decoded = buf.toString("utf8").trim();
    if (decoded.startsWith("{")) return decoded;
  } catch (_) {}
  return trimmed;
}

function normalizeText(input) {
  // Remove niqqud + cantillation marks, punctuation, unify spaces
  // Niqqud range: \u0591-\u05C7
  const s = String(input || "")
    .normalize("NFKC")
    .replace(/[\u0591-\u05C7]/g, "") // remove Hebrew diacritics
    .replace(/[^\p{L}\p{N}\s]/gu, " ") // keep letters/numbers/spaces
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
  return s;
}

function splitKeywordsCell(cell) {
  // Accept commas, semicolons, newlines
  const raw = String(cell || "");
  return raw
    .split(/[,;\n]/g)
    .map((x) => normalizeText(x))
    .map((x) => x.trim())
    .filter(Boolean);
}

function routeByKeywords(text, routingRules) {
  const norm = normalizeText(text);
  if (!norm) return null;

  // Highest priority first (lower number = higher priority)
  const sorted = [...routingRules].sort((a, b) => {
    const pa = Number(a.priority ?? 9999);
    const pb = Number(b.priority ?? 9999);
    return pa - pb;
  });

  for (const rule of sorted) {
    const route = String(rule.route || "").trim();
    const keywords = splitKeywordsCell(rule.keywords || "");
    if (!route || keywords.length === 0) continue;

    for (const kw of keywords) {
      if (!kw) continue;
      if (norm.includes(kw)) {
        return {
          route,
          matched: kw,
          confidence: 1,
          by: "sheet_keywords",
        };
      }
    }
  }

  return null;
}

// ===================== Google Sheets (via gviz export) =====================
// We avoid googleapis + OAuth complexity. Works for PUBLIC sheets or "Anyone with link" access.
// If your sheet is private, this still works if the sheet is shared with a service account
// AND published? Not always. Your current config-check works, so we keep same approach.

async function fetchGvizSheet(sheetId, tabName) {
  // gviz endpoint returns JS-wrapped JSON - we extract the JSON part
  const url = `https://docs.google.com/spreadsheets/d/${encodeURIComponent(
    sheetId
  )}/gviz/tq?tqx=out:json&sheet=${encodeURIComponent(tabName)}`;

  const res = await fetch(url, { method: "GET" });
  if (!res.ok) throw new Error(`GVIZ fetch failed (${res.status}) for ${tabName}`);
  const text = await res.text();

  // Extract JSON: google.visualization.Query.setResponse(...)
  const m = text.match(/setResponse\(([\s\S]*?)\);\s*$/);
  if (!m) throw new Error(`GVIZ parse failed for ${tabName}`);
  const payloadStr = m[1];

  const parsed = safeJsonParse(payloadStr);
  if (!parsed.ok) throw new Error(`GVIZ JSON parse failed for ${tabName}: ${parsed.error}`);

  return parsed.value;
}

function gvizToRows(gvizObj) {
  const table = gvizObj?.table;
  const cols = (table?.cols || []).map((c) => c?.label || c?.id || "");
  const rows = (table?.rows || []).map((r) => {
    const obj = {};
    (r.c || []).forEach((cell, idx) => {
      const key = cols[idx] || `col_${idx}`;
      obj[key] = cell?.v ?? "";
    });
    return obj;
  });
  return { cols, rows };
}

// ===================== Config Cache =====================
let CONFIG_CACHE = {
  loaded_at: null,
  expires_at: 0,
  data: null,
};

async function loadConfigFromSheet(force = false) {
  const now = Date.now();
  if (!force && CONFIG_CACHE.data && now < CONFIG_CACHE.expires_at) {
    return { from_cache: true, ...CONFIG_CACHE.data, loaded_at: CONFIG_CACHE.loaded_at };
  }

  if (!ENV.GSHEET_ID) throw new Error("GSHEET_ID is missing");

  // Tabs we care for routing + overview
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

  const out = {};
  const counts = {};
  for (const tab of tabs) {
    const gviz = await fetchGvizSheet(ENV.GSHEET_ID, tab);
    const { rows } = gvizToRows(gviz);
    out[tab] = rows;
    counts[`${tab}_rows`] = rows.length;
  }

  // Build overview basics from SETTINGS (key/value)
  const settingsRows = out.SETTINGS || [];
  const settings = {};
  for (const r of settingsRows) {
    const k = String(r.key || r.KEY || r.Key || "").trim();
    const v = String(r.value || r.VALUE || r.Value || "").trim();
    if (k) settings[k] = v;
  }

  const overview = {
    BUSINESS_NAME: settings.BUSINESS_NAME || "",
    DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "he",
    SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "he",
    SITE_BASE_URL: settings.SITE_BASE_URL || "",
    MAIN_PHONE: settings.MAIN_PHONE || "",
    BRANCHES: settings.BRANCHES || "",
  };

  const validation = {
    missing_settings_keys: [],
    languages_ok: true,
    default_language: overview.DEFAULT_LANGUAGE,
    supported_languages: overview.SUPPORTED_LANGUAGES.split(",").map((x) => x.trim()).filter(Boolean),
    numeric_warnings: [],
    counts,
  };

  // minimal validation for keys you already care about
  const required = ["BUSINESS_NAME", "DEFAULT_LANGUAGE", "SUPPORTED_LANGUAGES"];
  for (const rk of required) {
    if (!settings[rk]) validation.missing_settings_keys.push(rk);
  }

  const data = {
    ok: true,
    sheet_id: ENV.GSHEET_ID,
    settings,
    overview,
    validation,
    routing_rules: (out.ROUTING_RULES || []).map((r) => ({
      priority: r.priority ?? r.PRIORITY ?? r.Priority ?? 9999,
      route: r.route ?? r.ROUTE ?? r.Route ?? "",
      keywords: r.keywords ?? r.KEYWORDS ?? r.Keywords ?? "",
      question_if_ambiguous:
        r.question_if_ambiguous ?? r.QUESTION_IF_AMBIGUOUS ?? r.Question_if_ambiguous ?? "",
      notes: r.notes ?? r.NOTES ?? r.Notes ?? "",
    })),
  };

  CONFIG_CACHE = {
    loaded_at: nowISO(),
    expires_at: now + ENV.GSHEET_CACHE_TTL_SEC * 1000,
    data,
  };

  return { from_cache: false, ...data, loaded_at: CONFIG_CACHE.loaded_at };
}

// ===================== Gemini Router Fallback =====================
async function geminiRoute(text, allowedRoutes) {
  if (!ENV.GEMINI_API_KEY) {
    return null; // not enabled
  }

  const model = ENV.GEMINI_MODEL || "gemini-1.5-flash";
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(ENV.GEMINI_API_KEY)}`;

  const routesList = allowedRoutes.filter(Boolean);
  const system = `
You are a routing classifier for a Hebrew retail business phone bot.
Return ONLY strict JSON with keys: route, confidence, reason.
route must be one of: ${routesList.join(", ")}.
confidence is a number 0..1.
If unclear between sales/support, choose "ambiguous" (never "unknown").
`.trim();

  const user = `
User text:
${text}
`.trim();

  const body = {
    contents: [
      { role: "user", parts: [{ text: system + "\n\n" + user }] }
    ],
    generationConfig: {
      temperature: 0.2,
      maxOutputTokens: 120,
    },
  };

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  const raw = await res.text();
  if (!res.ok) {
    log("Gemini error:", res.status, raw.slice(0, 500));
    return null;
  }

  // Extract text from response
  let data;
  try {
    data = JSON.parse(raw);
  } catch (e) {
    log("Gemini JSON parse fail:", e?.message, raw.slice(0, 200));
    return null;
  }

  const outText =
    data?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("") || "";

  // Gemini sometimes wraps JSON in ```json ... ```
  const jsonMatch = outText.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    log("Gemini output not JSON:", outText.slice(0, 200));
    return null;
  }

  const parsed = safeJsonParse(jsonMatch[0]);
  if (!parsed.ok) {
    log("Gemini JSON invalid:", parsed.error, jsonMatch[0].slice(0, 200));
    return null;
  }

  const route = String(parsed.value.route || "").trim();
  const conf = Number(parsed.value.confidence ?? 0);

  if (!routesList.includes(route)) return null;

  return {
    route,
    matched: null,
    confidence: isFinite(conf) ? Math.max(0, Math.min(1, conf)) : 0.5,
    by: "gemini_fallback",
    reason: String(parsed.value.reason || ""),
  };
}

// ===================== Routes =====================
app.get("/", (req, res) => {
  res.status(200).send("GilSport VoiceBot Realtime - up. Try /health or /config-check");
});

app.get("/health", (req, res) => {
  res.json({ ok: true, service: "gilsport-voicebot-realtime", time: nowISO() });
});

app.get("/config-check", async (req, res) => {
  // Also validate GOOGLE_SERVICE_ACCOUNT_JSON if present (optional)
  const saRaw = ENV.GOOGLE_SERVICE_ACCOUNT_JSON;
  let saValid = null;
  if (saRaw) {
    const decoded = maybeBase64ToString(saRaw);
    const parsed = safeJsonParse(decoded);
    saValid = parsed.ok ? true : `invalid: ${parsed.error}`;
  }

  try {
    const cfg = await loadConfigFromSheet(false);
    res.json({
      ok: true,
      from_cache: cfg.from_cache,
      loaded_at: cfg.loaded_at,
      sheet_id: cfg.sheet_id,
      validation: cfg.validation,
      overview: cfg.overview,
      router_llm: {
        enabled: Boolean(ENV.GEMINI_API_KEY),
        provider: "gemini",
        model: ENV.GEMINI_MODEL,
      },
      google_service_account_json: saValid,
    });
  } catch (e) {
    res.status(200).json({
      ok: false,
      error: e?.message || String(e),
      router_llm: {
        enabled: Boolean(ENV.GEMINI_API_KEY),
        provider: "gemini",
        model: ENV.GEMINI_MODEL,
      },
      google_service_account_json: saValid,
    });
  }
});

app.post("/route", async (req, res) => {
  const text = String(req.body?.text || "").trim();

  try {
    const cfg = await loadConfigFromSheet(false);

    // 1) Sheet keyword routing
    const bySheet = routeByKeywords(text, cfg.routing_rules || []);
    if (bySheet) {
      return res.json({
        ok: true,
        input: { text },
        decision: {
          route: bySheet.route,
          matched: bySheet.matched,
          confidence: bySheet.confidence,
          by: bySheet.by,
        },
      });
    }

    // 2) Gemini fallback (if enabled)
    const allowedRoutes = ["sales", "support", "ambiguous"];
    const byGemini = await geminiRoute(text, allowedRoutes);
    if (byGemini) {
      return res.json({
        ok: true,
        input: { text },
        decision: {
          route: byGemini.route,
          matched: null,
          confidence: byGemini.confidence,
          by: byGemini.by,
          reason: byGemini.reason || "",
        },
      });
    }

    // 3) Final fallback
    return res.json({
      ok: true,
      input: { text },
      decision: {
        route: "unknown",
        matched: null,
        confidence: 0,
        by: "none",
      },
    });
  } catch (e) {
    return res.status(200).json({
      ok: false,
      error: e?.message || String(e),
    });
  }
});

app.listen(PORT, () => {
  console.log(`[INFO] Server listening on port ${PORT}`);
  console.log(`[INFO] ${nowISO()}`);
});
