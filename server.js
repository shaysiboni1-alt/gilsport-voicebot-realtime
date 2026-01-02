// server.js
//
// GilSport VoiceBot Realtime - Config + Router service
// - /health
// - /config-check
// - /route  (keywords first, Gemini fallback)
// - /site-scan (safe website fetch -> clean text)
//
// Google Sheets access via Service Account (JWT) – NO GVIZ
// Node 18+ (Render uses Node 22.x)

import express from "express";
import { google } from "googleapis";
import crypto from "crypto";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;

// ===================== ENV =====================
const ENV = {
  // Sheets
  GOOGLE_SERVICE_ACCOUNT_JSON: process.env.GOOGLE_SERVICE_ACCOUNT_JSON || "",
  GSHEET_ID: process.env.GSHEET_ID || "",
  GSHEET_CACHE_TTL_SEC: Number(process.env.GSHEET_CACHE_TTL_SEC || 60),
  TIME_ZONE: process.env.TIME_ZONE || "Asia/Jerusalem",

  // Gemini (fallback router)
  GEMINI_API_KEY: process.env.GEMINI_API_KEY || "",
  GEMINI_MODEL: process.env.GEMINI_MODEL || "models/gemini-2.0-flash-exp",
  GEMINI_TIMEOUT_MS: Number(process.env.GEMINI_TIMEOUT_MS || 9000),

  // Make dispatch (optional)
  ENABLE_MAKE_DISPATCH: String(process.env.ENABLE_MAKE_DISPATCH || "false").toLowerCase() === "true",
  MAKE_SEND_WA_URL: process.env.MAKE_SEND_WA_URL || "",
  MAKE_LEAD_URL: process.env.MAKE_LEAD_URL || "",
  MAKE_SUPPORT_URL: process.env.MAKE_SUPPORT_URL || "",
  MAKE_ABANDONED_URL: process.env.MAKE_ABANDONED_URL || "",
  MAKE_TIMEOUT_MS: Number(process.env.MAKE_TIMEOUT_MS || 7000),

  // Website live scan (optional)
  ENABLE_SITE_SCAN: String(process.env.ENABLE_SITE_SCAN || "true").toLowerCase() === "true",
  SITE_SCAN_MAX_CHARS: Number(process.env.SITE_SCAN_MAX_CHARS || 12000),
  SITE_SCAN_TIMEOUT_MS: Number(process.env.SITE_SCAN_TIMEOUT_MS || 8000),
  SITE_SCAN_USER_AGENT: process.env.SITE_SCAN_USER_AGENT || "GilSportVoiceBot/1.0 (+router)",

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(), // debug | info | warn | error
};

// ===================== Logging =====================
const LEVELS = { debug: 10, info: 20, warn: 30, error: 40 };
function log(level, msg, extra) {
  if ((LEVELS[level] || 999) < (LEVELS[ENV.LOG_LEVEL] || 20)) return;
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
  return String(s || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

function splitKeywords(cell) {
  return String(cell || "")
    .split(/[,|\n]/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

function clampStr(s, max) {
  const str = String(s || "");
  if (str.length <= max) return str;
  return str.slice(0, max);
}

// Extract JSON object even if model returns it inside ```json ... ``` or surrounded by text
function extractFirstJsonObject(text) {
  const t = String(text || "").trim();
  if (!t) return null;

  // Try fenced block first
  const fenced = t.match(/```(?:json)?\s*([\s\S]*?)\s*```/i);
  if (fenced && fenced[1]) {
    const parsed = safeJsonParse(fenced[1]);
    if (parsed.ok) return parsed.value;
  }

  // Try direct parse
  {
    const parsed = safeJsonParse(t);
    if (parsed.ok) return parsed.value;
  }

  // Try substring between first { and last }
  const first = t.indexOf("{");
  const last = t.lastIndexOf("}");
  if (first >= 0 && last > first) {
    const sub = t.slice(first, last + 1);
    const parsed = safeJsonParse(sub);
    if (parsed.ok) return parsed.value;
  }

  return null;
}

function sha1(s) {
  return crypto.createHash("sha1").update(String(s || "")).digest("hex");
}

async function fetchWithTimeout(url, opts = {}, timeoutMs = 8000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, { ...opts, signal: controller.signal });
    return res;
  } finally {
    clearTimeout(id);
  }
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

  if (!creds?.client_email || !creds?.private_key) {
    throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON invalid (missing client_email/private_key)");
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

// ===================== Website Live Scan (Safe) =====================
const SITE_CACHE = new Map(); // key -> { at, text }
function getAllowedSiteBase(cfg) {
  const base = (cfg?.overview?.SITE_BASE_URL || cfg?.settings?.SITE_BASE_URL || "").trim();
  if (!base) return "";
  try {
    const u = new URL(base);
    return u.origin;
  } catch {
    return "";
  }
}

function isUrlAllowed(targetUrl, allowedOrigin) {
  try {
    const u = new URL(targetUrl);
    return allowedOrigin && u.origin === allowedOrigin;
  } catch {
    return false;
  }
}

function htmlToText(html) {
  // super simple sanitizer for routing context (not perfect, but safe and fast)
  const noScript = html
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<!--[\s\S]*?-->/g, " ");
  const noTags = noScript.replace(/<\/?[^>]+>/g, " ");
  return noTags.replace(/\s+/g, " ").trim();
}

async function siteScan(cfg, url, force = false) {
  if (!ENV.ENABLE_SITE_SCAN) throw new Error("site scan disabled");
  const allowedOrigin = getAllowedSiteBase(cfg);
  if (!allowedOrigin) throw new Error("SITE_BASE_URL missing in SETTINGS");

  // default to homepage if url missing
  const targetUrl = url ? String(url) : allowedOrigin + "/";
  if (!isUrlAllowed(targetUrl, allowedOrigin)) {
    throw new Error(`URL not allowed (must be within ${allowedOrigin})`);
  }

  const key = sha1(targetUrl);
  const cached = SITE_CACHE.get(key);
  const ttlMs = 60 * 1000; // 60s cache for "real-time but not spam"
  if (!force && cached && Date.now() - cached.at < ttlMs) {
    return { ok: true, from_cache: true, url: targetUrl, text: cached.text };
  }

  const res = await fetchWithTimeout(
    targetUrl,
    {
      method: "GET",
      headers: {
        "User-Agent": ENV.SITE_SCAN_USER_AGENT,
        Accept: "text/html,application/xhtml+xml",
      },
    },
    ENV.SITE_SCAN_TIMEOUT_MS
  );

  if (!res.ok) {
    throw new Error(`site fetch failed: HTTP ${res.status}`);
  }

  const html = await res.text();
  const text = clampStr(htmlToText(html), ENV.SITE_SCAN_MAX_CHARS);

  SITE_CACHE.set(key, { at: Date.now(), text });
  return { ok: true, from_cache: false, url: targetUrl, text };
}

// ===================== Gemini Fallback Router =====================
async function geminiRouteDecision(cfg, text, opts = {}) {
  const apiKey = ENV.GEMINI_API_KEY;
  const model = String(ENV.GEMINI_MODEL || "").trim();
  if (!apiKey) throw new Error("GEMINI_API_KEY missing");
  if (!model) throw new Error("GEMINI_MODEL missing");

  const timeoutMs = Number(opts.timeoutMs || ENV.GEMINI_TIMEOUT_MS || 9000);

  // Optionally add tiny website context (safe) to improve routing
  let siteSnippet = "";
  try {
    if (String(cfg?.settings?.ENABLE_SITE_CONTEXT_FOR_ROUTER || "").toLowerCase() === "true") {
      const allowedOrigin = getAllowedSiteBase(cfg);
      if (allowedOrigin) {
        const scan = await siteScan(cfg, allowedOrigin + "/", false);
        siteSnippet = clampStr(scan.text, 2500);
      }
    }
  } catch (e) {
    // don't fail routing because of site snippet
    log("warn", "site context for router failed", e?.message || String(e));
  }

  const allowedRoutes = ["sales", "support", "unknown"];

  const prompt = `
You are a strict router for an Israeli sports store phone bot (GilSport).
Decide which route should handle the user's text:
- "sales": product advice, buying, prices, stock, recommendations, store info, branches, opening hours
- "support": existing order, returns, warranty, delivery issues, complaints, technical help after purchase
- "unknown": unclear or unrelated

Return ONLY a JSON object with this exact schema:
{"route":"sales|support|unknown","confidence":0..1,"reason":"short reason"}

User text: """${String(text || "").trim()}"""

Extra business hints:
- Business name: ${cfg?.overview?.BUSINESS_NAME || "GilSport"}
- Website: ${cfg?.overview?.SITE_BASE_URL || ""}
${siteSnippet ? `- Website snippet (may help): "${siteSnippet}"` : ""}

Rules:
- Output must be JSON only (no markdown, no code fences).
- If unsure, use route "unknown" with confidence <= 0.5
`.trim();

  const endpoint = `https://generativelanguage.googleapis.com/v1beta/${model}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const body = {
    contents: [{ role: "user", parts: [{ text: prompt }] }],
    generationConfig: {
      temperature: 0.1,
      maxOutputTokens: 200,
    },
  };

  const res = await fetchWithTimeout(
    endpoint,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
    timeoutMs
  );

  const raw = await res.text();
  if (!res.ok) {
    throw new Error(`Gemini HTTP ${res.status} ${res.statusText} | body: ${raw}`);
  }

  // Gemini returns JSON wrapper; we need candidate text
  const parsed = safeJsonParse(raw);
  if (!parsed.ok) {
    throw new Error("Gemini response is not JSON (API wrapper parse failed)");
  }

  const candidateText =
    parsed.value?.candidates?.[0]?.content?.parts?.map((p) => p?.text || "").join("\n") || "";

  const obj = extractFirstJsonObject(candidateText);
  if (!obj || typeof obj !== "object") {
    throw new Error("Gemini parse failed (non-JSON response from model)");
  }

  const route = String(obj.route || "").trim();
  const confidence = Number(obj.confidence ?? 0);
  const reason = String(obj.reason || "").trim();

  if (!allowedRoutes.includes(route)) {
    return {
      ok: true,
      decision: { route: "unknown", confidence: 0, by: "gemini", reason: "invalid route" },
      debug: { gemini_model: model, raw_candidate: candidateText },
    };
  }

  return {
    ok: true,
    decision: {
      route,
      confidence: Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0,
      by: "gemini",
      reason: reason || null,
    },
    debug: { gemini_model: model, raw_candidate: candidateText, gemini_reason: reason || "" },
  };
}

// ===================== Make Dispatch (Optional) =====================
function getMakeUrlForRoute(cfg, route) {
  // priority: SETTINGS -> ENV fallback
  const s = cfg?.settings || {};
  const bySettings = {
    send_wa: s.MAKE_SEND_WA_URL || "",
    lead: s.MAKE_LEAD_URL || "",
    support: s.MAKE_SUPPORT_URL || "",
    abandoned: s.MAKE_ABANDONED_URL || "",
  };

  const byEnv = {
    send_wa: ENV.MAKE_SEND_WA_URL,
    lead: ENV.MAKE_LEAD_URL,
    support: ENV.MAKE_SUPPORT_URL,
    abandoned: ENV.MAKE_ABANDONED_URL,
  };

  // simple mapping for now
  if (route === "sales") return bySettings.lead || byEnv.lead || "";
  if (route === "support") return bySettings.support || byEnv.support || "";
  if (route === "abandoned") return bySettings.abandoned || byEnv.abandoned || "";

  return "";
}

async function postToMake(url, payload) {
  const res = await fetchWithTimeout(
    url,
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    },
    ENV.MAKE_TIMEOUT_MS
  );

  const txt = await res.text().catch(() => "");
  if (!res.ok) {
    throw new Error(`Make HTTP ${res.status} ${res.statusText} | body: ${txt}`);
  }
  return { ok: true, status: res.status, body: txt };
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
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Safe website scan (for your “real-time site” requirement)
app.get("/site-scan", async (req, res) => {
  try {
    const cfg = await loadConfigFromSheet(false);
    const url = req.query?.url ? String(req.query.url) : "";
    const force = String(req.query?.force || "0") === "1";
    const out = await siteScan(cfg, url, force);
    res.json({ ok: true, ...out });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

app.post("/route", async (req, res) => {
  const debugWanted = String(req.query?.debug || "0") === "1";
  const dispatchWanted = String(req.query?.dispatch || "0") === "1";

  try {
    const text = String(req.body?.text || "").trim();
    const caller = String(req.body?.caller || "").trim(); // optional, orchestrator may pass it
    const cfg = await loadConfigFromSheet(false);

    // 1) Keywords first
    const bySheet = routeByKeywords(text, cfg.routing_rules);
    if (bySheet) {
      // optional Make dispatch
      let make = null;
      if (ENV.ENABLE_MAKE_DISPATCH && dispatchWanted) {
        const url = getMakeUrlForRoute(cfg, bySheet.route);
        if (url) {
          try {
            const payload = {
              event: "route_decision",
              route: bySheet.route,
              by: bySheet.by,
              confidence: bySheet.confidence,
              matched: bySheet.matched || null,
              question: bySheet.question || null,
              text,
              caller: caller || null,
              time: nowIso(),
            };
            make = await postToMake(url, payload);
          } catch (e) {
            make = { ok: false, error: e.message };
          }
        }
      }

      return res.json({
        ok: true,
        decision: bySheet,
        ...(make ? { make } : {}),
      });
    }

    // 2) Gemini fallback
    if (ENV.GEMINI_API_KEY) {
      try {
        const g = await geminiRouteDecision(cfg, text, { timeoutMs: ENV.GEMINI_TIMEOUT_MS });
        const decision = g.decision;

        // optional Make dispatch
        let make = null;
        if (ENV.ENABLE_MAKE_DISPATCH && dispatchWanted) {
          const url = getMakeUrlForRoute(cfg, decision.route);
          if (url && decision.route !== "unknown") {
            try {
              const payload = {
                event: "route_decision",
                route: decision.route,
                by: decision.by,
                confidence: decision.confidence,
                reason: decision.reason || null,
                text,
                caller: caller || null,
                time: nowIso(),
              };
              make = await postToMake(url, payload);
            } catch (e) {
              make = { ok: false, error: e.message };
            }
          }
        }

        return res.json({
          ok: true,
          decision: { route: decision.route, confidence: decision.confidence, by: "gemini" },
          ...(debugWanted
            ? {
                debug: {
                  gemini_model: g.debug?.gemini_model,
                  gemini_reason: g.debug?.gemini_reason || "",
                  raw_candidate: g.debug?.raw_candidate || "",
                  timeout_ms: ENV.GEMINI_TIMEOUT_MS,
                },
              }
            : {}),
          ...(make ? { make } : {}),
        });
      } catch (e) {
        // Gemini failed -> fall through to unknown, include debug if requested
        return res.json({
          ok: true,
          decision: { route: "unknown", confidence: 0, by: "gemini_failed" },
          ...(debugWanted
            ? {
                debug: {
                  gemini_error: e.message,
                  gemini_model: ENV.GEMINI_MODEL,
                  timeout_ms: ENV.GEMINI_TIMEOUT_MS,
                },
              }
            : {}),
        });
      }
    }

    // 3) No Gemini configured -> unknown
    return res.json({
      ok: true,
      decision: { route: "unknown", confidence: 0, by: "none" },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
});
