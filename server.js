// server.js
//
// GilSport VoiceBot Realtime - Config + Router service
// - /health
// - /config-check
// - /route  (keywords first, Gemini fallback)
// - /models (optional debug helper)
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
  // Example: models/gemini-2.0-flash-exp
  GEMINI_MODEL: process.env.GEMINI_MODEL || "models/gemini-2.0-flash-exp",
  GEMINI_TIMEOUT_MS: Number(process.env.GEMINI_TIMEOUT_MS || 9000),

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),
};

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
  return String(s || "").toLowerCase().replace(/\s+/g, " ").trim();
}

function splitKeywords(cell) {
  return String(cell || "")
    .split(/[,|\n]/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

function wantDebug(req) {
  const q = String(req.query?.debug || "").trim();
  return q === "1" || q.toLowerCase() === "true";
}

function normalizeGeminiModelName(name) {
  const raw = String(name || "").trim();
  if (!raw) return "";
  if (raw.startsWith("models/")) return raw;
  return `models/${raw}`;
}

function truncate(s, max = 600) {
  const str = String(s || "");
  if (str.length <= max) return str;
  return str.slice(0, max) + `… (truncated ${str.length - max} chars)`;
}

function extractFirstJsonObject(text) {
  // Best-effort: find first {...} block
  const s = String(text || "");
  const start = s.indexOf("{");
  if (start === -1) return null;
  let depth = 0;
  for (let i = start; i < s.length; i++) {
    const ch = s[i];
    if (ch === "{") depth++;
    if (ch === "}") depth--;
    if (depth === 0) {
      return s.slice(start, i + 1);
    }
  }
  return null;
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

  const cfg = {
    settings,
    business_info: results.BUSINESS_INFO || [],
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

// ===================== Gemini (Fallback Router) =====================
function buildGeminiRoutingPrompt(text) {
  // Hard JSON instructions + minimal
  return [
    "You are a routing classifier for a business voicebot.",
    "Return ONLY JSON. No markdown. No explanations. No extra keys.",
    'Schema exactly: {"route":"sales|support|ambiguous","confidence":0-1,"reason":"short"}',
    "Rules:",
    "- sales: buying, prices, products, availability, models, orders, links, WhatsApp link request.",
    "- support: problems, malfunctions, defects, delivery issues, warranty, exchanges/returns, complaints, service.",
    "- ambiguous: unclear / generic / not enough info.",
    "",
    `User text: ${JSON.stringify(String(text || ""))}`,
  ].join("\n");
}

async function geminiRoute(text, timeoutMs, debug = false) {
  LAST_GEMINI_ROUTER_ERROR = "";

  if (!ENV.GEMINI_API_KEY) {
    LAST_GEMINI_ROUTER_ERROR = "GEMINI_API_KEY missing";
    return { ok: false, error: LAST_GEMINI_ROUTER_ERROR };
  }

  const model = normalizeGeminiModelName(ENV.GEMINI_MODEL);
  if (!model) {
    LAST_GEMINI_ROUTER_ERROR = "GEMINI_MODEL missing";
    return { ok: false, error: LAST_GEMINI_ROUTER_ERROR };
  }

  const url = `https://generativelanguage.googleapis.com/v1beta/${model}:generateContent?key=${encodeURIComponent(
    ENV.GEMINI_API_KEY
  )}`;

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  let rawCandidate = "";

  try {
    const payload = {
      contents: [
        {
          role: "user",
          parts: [{ text: buildGeminiRoutingPrompt(text) }],
        },
      ],
      generationConfig: {
        temperature: 0,
        maxOutputTokens: 140,
        // IMPORTANT: force JSON output when supported
        responseMimeType: "application/json",
      },
    };

    const resp = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });

    const bodyText = await resp.text();

    if (!resp.ok) {
      LAST_GEMINI_ROUTER_ERROR = `Gemini HTTP ${resp.status} ${resp.statusText} | body: ${bodyText}`;
      return { ok: false, error: LAST_GEMINI_ROUTER_ERROR, model };
    }

    // Parse response envelope
    let body;
    try {
      body = JSON.parse(bodyText);
    } catch {
      LAST_GEMINI_ROUTER_ERROR = "Gemini envelope parse failed (invalid JSON from API)";
      return { ok: false, error: LAST_GEMINI_ROUTER_ERROR, model };
    }

    rawCandidate = String(body?.candidates?.[0]?.content?.parts?.[0]?.text ?? "").trim();
    if (!rawCandidate) {
      LAST_GEMINI_ROUTER_ERROR = "Gemini returned empty candidate text";
      return { ok: false, error: LAST_GEMINI_ROUTER_ERROR, model, raw: debug ? bodyText : undefined };
    }

    // Try strict JSON parse
    let parsed = safeJsonParse(rawCandidate);
    if (!parsed.ok) {
      // fallback: extract first {..}
      const extracted = extractFirstJsonObject(rawCandidate);
      if (extracted) parsed = safeJsonParse(extracted);
    }

    if (!parsed.ok) {
      LAST_GEMINI_ROUTER_ERROR = "Gemini parse failed (non-JSON response from model)";
      return {
        ok: false,
        error: LAST_GEMINI_ROUTER_ERROR,
        model,
        raw_candidate: debug ? truncate(rawCandidate, 900) : undefined,
      };
    }

    const json = parsed.value;
    const route = String(json.route || "").trim();
    const confidence = Number(json.confidence ?? 0);
    const reason = String(json.reason || "").trim();

    if (!["sales", "support", "ambiguous"].includes(route)) {
      LAST_GEMINI_ROUTER_ERROR = `Gemini returned invalid route: ${route}`;
      return {
        ok: false,
        error: LAST_GEMINI_ROUTER_ERROR,
        model,
        raw_candidate: debug ? truncate(rawCandidate, 900) : undefined,
      };
    }

    return {
      ok: true,
      decision: {
        route,
        confidence: Number.isFinite(confidence) ? confidence : 0,
        reason,
        by: "gemini",
        model,
      },
      raw_candidate: debug ? truncate(rawCandidate, 900) : undefined,
    };
  } catch (e) {
    const msg =
      e?.name === "AbortError"
        ? `Gemini timeout after ${timeoutMs}ms`
        : `Gemini request failed: ${e?.message || String(e)}`;
    LAST_GEMINI_ROUTER_ERROR = msg;
    return { ok: false, error: msg, model, raw_candidate: debug ? truncate(rawCandidate, 900) : undefined };
  } finally {
    clearTimeout(timer);
  }
}

// ===================== /models helper =====================
app.get("/models", async (req, res) => {
  try {
    if (!ENV.GEMINI_API_KEY) {
      return res.status(400).json({ ok: false, error: "GEMINI_API_KEY missing" });
    }
    const url = `https://generativelanguage.googleapis.com/v1beta/models?key=${encodeURIComponent(
      ENV.GEMINI_API_KEY
    )}`;

    const resp = await fetch(url, { method: "GET" });
    const txt = await resp.text();
    if (!resp.ok) {
      return res.status(resp.status).json({
        ok: false,
        error: `ListModels failed: ${resp.status} ${resp.statusText}`,
        body: txt,
      });
    }
    const data = JSON.parse(txt);
    const models = (data.models || []).map((m) => ({
      name: m.name,
      supportedGenerationMethods: m.supportedGenerationMethods,
    }));
    res.json({ ok: true, models });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

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
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post("/route", async (req, res) => {
  const debug = wantDebug(req);

  try {
    const text = String(req.body?.text || "").trim();
    const cfg = await loadConfigFromSheet(false);

    // 1) Sheet keywords (primary)
    const bySheet = routeByKeywords(text, cfg.routing_rules);
    if (bySheet) {
      return res.json({ ok: true, decision: bySheet });
    }

    // 2) Gemini fallback (hybrid)
    const g = await geminiRoute(text, ENV.GEMINI_TIMEOUT_MS, debug);
    if (g.ok) {
      if (g.decision.route === "ambiguous") {
        const q =
          (cfg.routing_rules || [])
            .map((r) => String(r.question_if_ambiguous || "").trim())
            .find(Boolean) || null;

        return res.json({
          ok: true,
          decision: {
            route: "ambiguous",
            confidence: g.decision.confidence,
            by: "gemini",
            question: q,
          },
          ...(debug
            ? { debug: { gemini_model: g.decision.model, gemini_reason: g.decision.reason, raw_candidate: g.raw_candidate } }
            : {}),
        });
      }

      return res.json({
        ok: true,
        decision: {
          route: g.decision.route,
          confidence: g.decision.confidence,
          by: "gemini",
        },
        ...(debug
          ? { debug: { gemini_model: g.decision.model, gemini_reason: g.decision.reason, raw_candidate: g.raw_candidate } }
          : {}),
      });
    }

    // Gemini failed
    return res.json({
      ok: true,
      decision: {
        route: "unknown",
        confidence: 0,
        by: "gemini_failed",
      },
      ...(debug
        ? {
            debug: {
              gemini_error: g.error || LAST_GEMINI_ROUTER_ERROR,
              gemini_model: g.model || normalizeGeminiModelName(ENV.GEMINI_MODEL),
              timeout_ms: ENV.GEMINI_TIMEOUT_MS,
              raw_candidate: g.raw_candidate,
            },
          }
        : {}),
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
  console.log(`[BOOT] GEMINI_MODEL=${normalizeGeminiModelName(ENV.GEMINI_MODEL)}`);
});
