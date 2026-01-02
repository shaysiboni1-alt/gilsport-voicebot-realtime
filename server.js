// server.js
//
// GilSport VoiceBot Realtime - Config + Router + Flow Engine (+ Site Scan KB)
// - /health
// - /config-check
// - /route        (keywords first, Gemini fallback + optional site KB context)
// - /dialog/next  (text dialog engine based on SALES_SCRIPT / SUPPORT_SCRIPT)
// - /kb/refresh   (controlled real-time site scan -> cached KB text)
// - /event/send   (send events to Make, URLs from Sheets SETTINGS first, ENV fallback)
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
  // MUST be full model name like: "models/gemini-2.0-flash-exp"
  GEMINI_MODEL: process.env.GEMINI_MODEL || "models/gemini-2.0-flash-exp",

  // Optional: allow turning debug on without redeploy
  DEBUG: String(process.env.DEBUG || "").toLowerCase() === "true",

  // Optional: Make URL fallback (if not present in Sheets SETTINGS)
  MAKE_SEND_WA_URL: process.env.MAKE_SEND_WA_URL || "",
  MAKE_LEAD_URL: process.env.MAKE_LEAD_URL || "",
  MAKE_SUPPORT_URL: process.env.MAKE_SUPPORT_URL || "",
  MAKE_ABANDONED_URL: process.env.MAKE_ABANDONED_URL || "",

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),

  // ===== Site scan options (safe defaults) =====
  // If not set, will use SETTINGS.SITE_BASE_URL from Sheets
  SITE_BASE_URL: process.env.SITE_BASE_URL || "",
  KB_CACHE_TTL_SEC: Number(process.env.KB_CACHE_TTL_SEC || 600), // 10 min
  KB_MAX_PAGES: Number(process.env.KB_MAX_PAGES || 12),
  KB_MAX_CHARS: Number(process.env.KB_MAX_CHARS || 22000), // keep short
  KB_FETCH_TIMEOUT_MS: Number(process.env.KB_FETCH_TIMEOUT_MS || 7000),
};

// ===================== Simple Logger =====================
function log(level, ...args) {
  const order = { error: 0, warn: 1, info: 2, debug: 3 };
  const cur = order[ENV.LOG_LEVEL] ?? 2;
  const lvl = order[level] ?? 2;
  if (lvl <= cur) console.log(`[${level.toUpperCase()}]`, ...args);
}

function nowIso() {
  return new Date().toISOString();
}

function safeJsonParse(maybeJson) {
  try {
    if (!maybeJson) return { ok: false, error: "empty" };
    return { ok: true, value: JSON.parse(maybeJson) };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
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

function pickLangText(row, lang) {
  // Supports columns: he/en/ru/ar OR text_he/text_en...
  const l = String(lang || "he").trim();
  const direct = row?.[l];
  if (direct) return String(direct);

  const alt = row?.[`text_${l}`];
  if (alt) return String(alt);

  // Fallback: "text" or "value"
  if (row?.text) return String(row.text);
  if (row?.value) return String(row.value);
  return "";
}

function clamp(n, a, b) {
  const x = Number(n);
  if (!Number.isFinite(x)) return a;
  return Math.min(b, Math.max(a, x));
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
let SHEET_CACHE = {
  loaded_at: 0,
  data: null,
};

async function loadConfigFromSheet(force = false) {
  const ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - SHEET_CACHE.loaded_at < ttlMs;

  if (!force && SHEET_CACHE.data && fresh) {
    return {
      ok: true,
      from_cache: true,
      loaded_at: new Date(SHEET_CACHE.loaded_at).toISOString(),
      ...SHEET_CACHE.data,
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
      SITE_BASE_URL: settings.SITE_BASE_URL || settings.SITE_URL || "",
      MAIN_PHONE: settings.MAIN_PHONE || "",
      BRANCHES: settings.BRANCHES || "",
    },
  };

  SHEET_CACHE = {
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

// ===================== Site KB (Real-time scan with cache) =====================
let KB_CACHE = {
  loaded_at: 0,
  base_url: "",
  text: "",
  pages: [],
  last_error: "",
};

function getSiteBaseUrl(cfg) {
  const fromSheet = cfg?.overview?.SITE_BASE_URL || cfg?.settings?.SITE_BASE_URL || "";
  const base = (ENV.SITE_BASE_URL || fromSheet || "").trim();
  return base;
}

function sameOrigin(a, b) {
  try {
    const ua = new URL(a);
    const ub = new URL(b);
    return ua.origin === ub.origin;
  } catch {
    return false;
  }
}

function stripHtmlToText(html) {
  const noScript = html
    .replace(/<script\b[^<]*(?:(?!<\/script>)<[^<]*)*<\/script>/gi, " ")
    .replace(/<style\b[^<]*(?:(?!<\/style>)<[^<]*)*<\/style>/gi, " ");
  const text = noScript
    .replace(/<\/(p|div|li|h1|h2|h3|h4|br)\s*>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\s+/g, " ")
    .replace(/\n\s+/g, "\n")
    .trim();
  return text;
}

function extractLinks(html, currentUrl, baseUrl) {
  const links = [];
  const re = /href\s*=\s*["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(html))) {
    const raw = m[1].trim();
    if (!raw) continue;
    if (raw.startsWith("#")) continue;
    if (raw.startsWith("mailto:") || raw.startsWith("tel:")) continue;
    if (raw.startsWith("javascript:")) continue;

    try {
      const abs = new URL(raw, currentUrl).toString();
      // keep only same origin + within base origin
      if (!sameOrigin(abs, baseUrl)) continue;

      // skip obvious assets
      if (/\.(png|jpg|jpeg|gif|webp|svg|pdf|zip|rar|mp4|mp3|wav)(\?|#|$)/i.test(abs))
        continue;

      links.push(abs);
    } catch {
      // ignore
    }
  }
  return links;
}

async function fetchWithTimeout(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "GET",
      headers: {
        "User-Agent": "GilSportVoiceBotKB/1.0",
        Accept: "text/html,application/xhtml+xml",
      },
      signal: controller.signal,
    });
    return res;
  } finally {
    clearTimeout(t);
  }
}

async function refreshSiteKb(cfg, force = false) {
  const baseUrl = getSiteBaseUrl(cfg);
  if (!baseUrl) {
    KB_CACHE.last_error = "SITE_BASE_URL missing (Sheets SETTINGS.SITE_BASE_URL or ENV.SITE_BASE_URL)";
    return { ok: false, error: KB_CACHE.last_error };
  }

  const ttlMs = Math.max(30, ENV.KB_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - KB_CACHE.loaded_at < ttlMs;
  if (!force && KB_CACHE.text && fresh && KB_CACHE.base_url === baseUrl) {
    return {
      ok: true,
      from_cache: true,
      loaded_at: new Date(KB_CACHE.loaded_at).toISOString(),
      base_url: KB_CACHE.base_url,
      pages: KB_CACHE.pages,
      chars: KB_CACHE.text.length,
    };
  }

  const maxPages = clamp(ENV.KB_MAX_PAGES, 1, 50);
  const maxChars = clamp(ENV.KB_MAX_CHARS, 3000, 80000);
  const timeoutMs = clamp(ENV.KB_FETCH_TIMEOUT_MS, 1500, 15000);

  const queue = [baseUrl];
  const seen = new Set();
  const pages = [];
  let agg = "";

  KB_CACHE.last_error = "";

  while (queue.length && pages.length < maxPages && agg.length < maxChars) {
    const url = queue.shift();
    if (!url) break;
    if (seen.has(url)) continue;
    seen.add(url);

    try {
      const res = await fetchWithTimeout(url, timeoutMs);
      if (!res.ok) continue;

      const ct = String(res.headers.get("content-type") || "").toLowerCase();
      if (!ct.includes("text/html")) continue;

      const html = await res.text();
      const text = stripHtmlToText(html);

      if (text && text.length > 60) {
        pages.push(url);
        agg += `\n\n=== PAGE: ${url} ===\n${text}\n`;
      }

      const links = extractLinks(html, url, baseUrl);
      for (const l of links) {
        if (!seen.has(l) && queue.length < 250) queue.push(l);
      }
    } catch (e) {
      KB_CACHE.last_error = e?.message || String(e);
      continue;
    }
  }

  // trim
  if (agg.length > maxChars) agg = agg.slice(0, maxChars);

  KB_CACHE = {
    loaded_at: Date.now(),
    base_url: baseUrl,
    text: agg.trim(),
    pages,
    last_error: KB_CACHE.last_error || "",
  };

  return {
    ok: true,
    from_cache: false,
    loaded_at: nowIso(),
    base_url: KB_CACHE.base_url,
    pages: KB_CACHE.pages,
    chars: KB_CACHE.text.length,
    last_error: KB_CACHE.last_error || "",
  };
}

// ===================== Gemini Router (Fallback) =====================
function extractFirstJsonObject(s) {
  const text = String(s || "");
  // Remove code fences if exist
  const cleaned = text.replace(/```[\s\S]*?```/g, (m) => m.replace(/```/g, ""));
  // Find first { ... } balanced-ish
  const start = cleaned.indexOf("{");
  if (start < 0) return null;
  let depth = 0;
  for (let i = start; i < cleaned.length; i++) {
    const c = cleaned[i];
    if (c === "{") depth++;
    if (c === "}") depth--;
    if (depth === 0) {
      const candidate = cleaned.slice(start, i + 1);
      const p = safeJsonParse(candidate);
      if (p.ok) return p.value;
      return null;
    }
  }
  return null;
}

async function geminiGenerateJson({ apiKey, model, system, user, timeoutMs = 9000 }) {
  if (!apiKey) throw new Error("GEMINI_API_KEY missing");
  if (!model) throw new Error("GEMINI_MODEL missing");

  const url = `https://generativelanguage.googleapis.com/v1beta/${encodeURIComponent(
    model
  )}:generateContent?key=${encodeURIComponent(apiKey)}`;

  const body = {
    contents: [
      {
        role: "user",
        parts: [{ text: `${system}\n\nUSER:\n${user}` }],
      },
    ],
    generationConfig: {
      temperature: 0.2,
      topP: 0.9,
      maxOutputTokens: 220,
    },
  };

  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);

  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });

    const raw = await res.text();
    if (!res.ok) {
      throw new Error(`Gemini HTTP ${res.status} ${res.statusText} | body: ${raw}`);
    }

    const parsed = safeJsonParse(raw);
    if (!parsed.ok) throw new Error(`Gemini parse failed (non-JSON response from API): ${parsed.error}`);

    const data = parsed.value;
    const cand = data?.candidates?.[0];
    const parts = cand?.content?.parts || [];
    const textOut = parts.map((p) => p?.text || "").join("\n").trim();

    return { ok: true, raw_api: data, textOut };
  } finally {
    clearTimeout(t);
  }
}

function buildGeminiRouteSystemPrompt(cfg, { includeSiteKb = false } = {}) {
  const businessName = cfg?.overview?.BUSINESS_NAME || "העסק";
  const supported = cfg?.overview?.SUPPORTED_LANGUAGES || "he";
  const siteBase = cfg?.overview?.SITE_BASE_URL || cfg?.settings?.SITE_BASE_URL || "";

  const brief = [
    `You are a routing classifier for a voice bot of "${businessName}".`,
    `Your job: decide route = "sales" or "support" or "unknown".`,
    `Return STRICT JSON ONLY (no markdown, no code fences).`,
    `JSON schema: {"route":"sales|support|unknown","confidence":0-1,"reason":"short english reason"}`,
    `Rules:`,
    `- "sales": buying, product advice, prices, sizes, stock, new customer, general purchase intent.`,
    `- "support": order status, returns, warranty, problems, complaints, shipping issues, existing customer issues.`,
    `- "unknown": greetings only, unclear, unrelated.`,
    `Supported languages: ${supported}. Input may be Hebrew.`,
    siteBase ? `Website base: ${siteBase}` : "",
    includeSiteKb ? `You may use the provided SITE_KB context if relevant.` : "",
  ]
    .filter(Boolean)
    .join("\n");

  return brief;
}

function buildGeminiUserPrompt(text, cfg) {
  const businessName = cfg?.overview?.BUSINESS_NAME || "";
  const phones = (cfg?.business_info || []).slice(0, 8);

  const bizLines = [];
  if (businessName) bizLines.push(`BUSINESS_NAME: ${businessName}`);
  if (phones.length) {
    bizLines.push(
      `BUSINESS_INFO (partial): ${phones
        .map((r) => Object.entries(r).slice(0, 6).map(([k, v]) => `${k}=${String(v).trim()}`).join(", "))
        .join(" | ")}`
    );
  }

  const siteKb = KB_CACHE?.text ? `\n\nSITE_KB:\n${KB_CACHE.text.slice(0, 12000)}` : "";

  return [
    bizLines.length ? bizLines.join("\n") : "",
    siteKb ? siteKb : "",
    `\n\nUSER_TEXT:\n${text}`,
  ]
    .filter(Boolean)
    .join("\n");
}

async function routeByGemini(text, cfg, { timeoutMs = 9000 } = {}) {
  const system = buildGeminiRouteSystemPrompt(cfg, { includeSiteKb: Boolean(KB_CACHE?.text) });
  const user = buildGeminiUserPrompt(text, cfg);

  const r = await geminiGenerateJson({
    apiKey: ENV.GEMINI_API_KEY,
    model: ENV.GEMINI_MODEL,
    system,
    user,
    timeoutMs,
  });

  const parsed = extractFirstJsonObject(r.textOut);
  if (!parsed) {
    throw new Error("Gemini parse failed (model output did not contain valid JSON)");
  }

  const route = String(parsed.route || "unknown").trim();
  const conf = clamp(parsed.confidence ?? 0, 0, 1);
  const reason = String(parsed.reason || "").trim();

  return {
    ok: true,
    decision: {
      route: ["sales", "support", "unknown"].includes(route) ? route : "unknown",
      confidence: conf,
      by: "gemini",
    },
    debug: {
      gemini_model: ENV.GEMINI_MODEL,
      gemini_reason: reason || null,
      raw_candidate: JSON.stringify(parsed),
      timeout_ms: timeoutMs,
    },
  };
}

// ===================== Flow Engine (Dialog) =====================
// We keep it flexible: scripts can be any schema; we try to use best-effort fields.
// Expected useful columns (recommended):
// - block_id (unique)
// - order (number) OR priority
// - he/en/ru/ar OR text_he/text_en...
// - next_block_id OR next OR next_id
// - expect (e.g., "name", "phone", "free_text") - optional
// - event (e.g., "lead", "support", "send_whatsapp_link") - optional

const SESSIONS = new Map();

function newSessionId() {
  return `s_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 8)}`;
}

function sortScriptRows(rows) {
  const arr = Array.isArray(rows) ? rows.slice() : [];
  arr.sort((a, b) => {
    const oa = Number(a.order || a.priority || 0);
    const ob = Number(b.order || b.priority || 0);
    if (oa !== ob) return oa - ob;
    // fallback stable-ish
    return String(a.block_id || "").localeCompare(String(b.block_id || ""));
  });
  return arr;
}

function indexScriptById(rows) {
  const map = new Map();
  for (const r of rows || []) {
    const id = String(r.block_id || r.id || "").trim();
    if (id) map.set(id, r);
  }
  return map;
}

function resolveNextBlockId(row) {
  return String(row.next_block_id || row.next || row.next_id || "").trim() || null;
}

function renderTemplate(text, vars = {}) {
  let out = String(text || "");
  // {{var}} replacements
  out = out.replace(/\{\{\s*([a-zA-Z0-9_]+)\s*\}\}/g, (_, k) => {
    const v = vars?.[k];
    return v == null ? "" : String(v);
  });
  return out.trim();
}

function buildDialogReply({ cfg, session, row, lang }) {
  const settings = cfg?.settings || {};
  const businessName = cfg?.overview?.BUSINESS_NAME || settings.BUSINESS_NAME || "";

  const vars = {
    business_name: businessName,
    BUSINESS_NAME: businessName,
    ...(session?.vars || {}),
  };

  const say = renderTemplate(pickLangText(row, lang), vars);

  const expect = String(row.expect || row.expected || "").trim() || null;
  const event = String(row.event || row.make_event || "").trim() || null;

  return {
    ok: true,
    session_id: session.session_id,
    route: session.route,
    step: session.step,
    language: lang,
    bot_say: say,
    expect,
    event,
    block_id: String(row.block_id || row.id || "").trim() || null,
  };
}

function chooseScript(cfg, route) {
  const r = String(route || "unknown").trim();
  if (r === "support") return { name: "SUPPORT_SCRIPT", rows: cfg.support_script || [] };
  if (r === "sales") return { name: "SALES_SCRIPT", rows: cfg.sales_script || [] };
  return { name: "UNKNOWN", rows: [] };
}

function getDefaultLanguage(cfg) {
  return String(cfg?.overview?.DEFAULT_LANGUAGE || cfg?.settings?.DEFAULT_LANGUAGE || "he").trim() || "he";
}

// ===================== Make Events (prepared) =====================
function getMakeUrlByEvent(cfg, eventName) {
  const settings = cfg?.settings || {};
  const ev = String(eventName || "").trim();

  // Prefer Sheets SETTINGS
  if (ev === "send_whatsapp_link") return settings.MAKE_SEND_WA_URL || ENV.MAKE_SEND_WA_URL || "";
  if (ev === "lead") return settings.MAKE_LEAD_URL || ENV.MAKE_LEAD_URL || "";
  if (ev === "support") return settings.MAKE_SUPPORT_URL || ENV.MAKE_SUPPORT_URL || "";
  if (ev === "abandoned") return settings.MAKE_ABANDONED_URL || ENV.MAKE_ABANDONED_URL || "";

  // generic: settings key could match
  const k = `MAKE_${ev.toUpperCase()}_URL`;
  return settings[k] || "";
}

async function postJson(url, payload, timeoutMs = 8000) {
  if (!url) throw new Error("Make URL missing");
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {}),
      signal: controller.signal,
    });
    const txt = await res.text();
    return { ok: res.ok, status: res.status, body: txt };
  } finally {
    clearTimeout(t);
  }
}

// ===================== Endpoints =====================
app.get("/health", (req, res) => {
  res.json({ ok: true, time: nowIso() });
});

// Config check - minimal
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
        SUPPLIERS: cfg.suppliers.length,
        MAKE_PAYLOADS_SPEC: cfg.make_payloads_spec.length,
        PROMPTS: cfg.prompts.length,
      },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Lightweight config subset (useful for your quick checks)
app.get("/config", async (req, res) => {
  try {
    const cfg = await loadConfigFromSheet(false);
    res.json({
      ok: true,
      from_cache: cfg.from_cache,
      loaded_at: cfg.loaded_at,
      routing_rules: cfg.routing_rules.length,
      overview: cfg.overview,
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Refresh site KB (controlled scan)
app.post("/kb/refresh", async (req, res) => {
  try {
    const force = Boolean(req.body?.force);
    const cfg = await loadConfigFromSheet(false);
    const r = await refreshSiteKb(cfg, force);
    res.json(r);
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Route: keywords first, Gemini fallback
app.post("/route", async (req, res) => {
  const wantDebug = ENV.DEBUG || String(req.query.debug || "").toLowerCase() === "true";
  try {
    const text = String(req.body?.text || "").trim();
    const cfg = await loadConfigFromSheet(false);

    // 1) keywords
    const bySheet = routeByKeywords(text, cfg.routing_rules);
    if (bySheet) {
      return res.json({ ok: true, decision: bySheet });
    }

    // 2) Ensure site KB is available (best-effort, no hard fail)
    try {
      if (!KB_CACHE.text) await refreshSiteKb(cfg, false);
    } catch (e) {
      log("warn", "KB refresh skipped/failed:", e?.message || String(e));
    }

    // 3) gemini fallback
    try {
      const g = await routeByGemini(text, cfg, { timeoutMs: 9000 });
      if (wantDebug) return res.json({ ok: true, decision: g.decision, debug: g.debug });
      return res.json({ ok: true, decision: g.decision });
    } catch (e) {
      const debug = {
        gemini_error: e?.message || String(e),
        gemini_model: ENV.GEMINI_MODEL,
        timeout_ms: 9000,
      };
      if (wantDebug) {
        return res.json({
          ok: true,
          decision: { route: "unknown", confidence: 0, by: "gemini_failed" },
          debug,
        });
      }
      return res.json({
        ok: true,
        decision: { route: "unknown", confidence: 0, by: "gemini_failed" },
      });
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Dialog engine: returns what bot should say next (text-only stage)
app.post("/dialog/next", async (req, res) => {
  const wantDebug = ENV.DEBUG || String(req.query.debug || "").toLowerCase() === "true";
  try {
    const cfg = await loadConfigFromSheet(false);

    const userText = String(req.body?.user_text || req.body?.text || "").trim();
    const providedRoute = String(req.body?.route || "").trim();
    const providedLang = String(req.body?.language || "").trim();
    const sessionIdIn = String(req.body?.session_id || "").trim();

    const lang = providedLang || getDefaultLanguage(cfg);

    // session load/create
    let session;
    if (sessionIdIn && SESSIONS.has(sessionIdIn)) {
      session = SESSIONS.get(sessionIdIn);
    } else {
      // decide route: if not provided -> call our router quickly
      let route = providedRoute;
      if (!route) {
        const bySheet = routeByKeywords(userText, cfg.routing_rules);
        if (bySheet?.route) route = bySheet.route;
        else {
          // fallback gemini (no crash if fails)
          try {
            if (!KB_CACHE.text) await refreshSiteKb(cfg, false);
          } catch {}
          try {
            const g = await routeByGemini(userText, cfg, { timeoutMs: 7000 });
            route = g?.decision?.route || "unknown";
          } catch {
            route = "unknown";
          }
        }
      }

      session = {
        session_id: sessionIdIn || newSessionId(),
        created_at: nowIso(),
        route,
        step: 0,
        last_block_id: null,
        vars: {
          // place to store caller_id, name, phone later
          caller_id: String(req.body?.caller_id || "").trim() || null,
        },
      };
      SESSIONS.set(session.session_id, session);
    }

    // choose script
    const script = chooseScript(cfg, session.route);
    const rowsSorted = sortScriptRows(script.rows);
    const byId = indexScriptById(script.rows);

    // if no script rows -> graceful
    if (!rowsSorted.length) {
      const fallback = cfg?.settings?.IDLE_WARNING_TEXT || "איך אפשר לעזור לכם?";
      return res.json({
        ok: true,
        session_id: session.session_id,
        route: session.route,
        step: session.step,
        language: lang,
        bot_say: fallback,
        expect: null,
        event: null,
        block_id: null,
        ...(wantDebug ? { debug: { script: script.name, rows: 0 } } : {}),
      });
    }

    // Determine next row:
    // - if first time -> row0
    // - else: try next_block_id from last row, else advance by step
    let row;
    if (session.step === 0) {
      row = rowsSorted[0];
    } else if (session.last_block_id && byId.has(session.last_block_id)) {
      const lastRow = byId.get(session.last_block_id);
      const nextId = resolveNextBlockId(lastRow);
      if (nextId && byId.has(nextId)) row = byId.get(nextId);
      else {
        const idx = rowsSorted.findIndex((r) => String(r.block_id || r.id || "").trim() === session.last_block_id);
        row = rowsSorted[Math.min(rowsSorted.length - 1, Math.max(0, idx + 1))];
      }
    } else {
      row = rowsSorted[Math.min(rowsSorted.length - 1, session.step)];
    }

    // Update session pointers
    const rowId = String(row.block_id || row.id || "").trim() || null;
    session.last_block_id = rowId;
    session.step = session.step + 1;
    SESSIONS.set(session.session_id, session);

    const reply = buildDialogReply({ cfg, session, row, lang });

    if (wantDebug) {
      reply.debug = {
        script: script.name,
        row_id: rowId,
        next_hint: resolveNextBlockId(row),
        cached_kb_chars: KB_CACHE?.text?.length || 0,
      };
    }

    return res.json(reply);
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// Send Make event (prepared; you can call it from your orchestrator later)
app.post("/event/send", async (req, res) => {
  const wantDebug = ENV.DEBUG || String(req.query.debug || "").toLowerCase() === "true";
  try {
    const cfg = await loadConfigFromSheet(false);
    const event = String(req.body?.event || "").trim(); // lead | support | send_whatsapp_link | abandoned
    const payload = req.body?.payload || {};

    if (!event) return res.status(400).json({ ok: false, error: "event is required" });

    const url = getMakeUrlByEvent(cfg, event);
    if (!url) return res.status(400).json({ ok: false, error: `Make URL missing for event: ${event}` });

    const r = await postJson(url, payload, 9000);

    if (wantDebug) {
      return res.json({ ok: r.ok, status: r.status, body: r.body, debug: { event, url_used: url } });
    }
    return res.json({ ok: r.ok, status: r.status });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.message || String(e) });
  }
});

// ===================== Boot =====================
app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
  console.log(`[BOOT] GEMINI_MODEL=${ENV.GEMINI_MODEL}`);
});
