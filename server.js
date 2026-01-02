// server.js
//
// GilSport VoiceBot Realtime - Config + Router + Dialog + Website KB
// - /health
// - /config-check
// - /route   (keywords first, Gemini fallback)
// - /kb      (crawl + cache website pages)
// - /dialog  (session-based dialogue using SALES/SUPPORT scripts + Make events)
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
  // IMPORTANT: must be full name like: models/gemini-2.0-flash-exp
  GEMINI_MODEL: process.env.GEMINI_MODEL || "models/gemini-2.0-flash-exp",
  GEMINI_TIMEOUT_MS: Number(process.env.GEMINI_TIMEOUT_MS || 9000),
  GEMINI_MIN_CONF: Number(process.env.GEMINI_MIN_CONF || 0.65),

  // Optional: allow overriding Make URLs from ENV; if empty -> use SETTINGS
  MAKE_SEND_WA_URL: process.env.MAKE_SEND_WA_URL || "",
  MAKE_LEAD_URL: process.env.MAKE_LEAD_URL || "",
  MAKE_SUPPORT_URL: process.env.MAKE_SUPPORT_URL || "",
  MAKE_ABANDONED_URL: process.env.MAKE_ABANDONED_URL || "",

  // Website KB crawling limits
  KB_MAX_PAGES: Number(process.env.KB_MAX_PAGES || 12),
  KB_MAX_CHARS_PER_PAGE: Number(process.env.KB_MAX_CHARS_PER_PAGE || 6000),
  KB_CACHE_TTL_SEC: Number(process.env.KB_CACHE_TTL_SEC || 900),

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),
};

let LAST_GEMINI_ERROR = "";
let LAST_KB_ERROR = "";

// ===================== Logging =====================
function log(level, ...args) {
  const levels = ["debug", "info", "warn", "error"];
  const cur = levels.indexOf(ENV.LOG_LEVEL);
  const idx = levels.indexOf(level);
  if (idx === -1) return;
  if (cur === -1 || idx >= cur) console.log(`[${level.toUpperCase()}]`, ...args);
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

function normalizeRowKeys(row) {
  const out = {};
  for (const [k, v] of Object.entries(row || {})) {
    const nk = String(k || "")
      .trim()
      .toLowerCase()
      .replace(/\s+/g, "_");
    out[nk] = v ?? "";
  }
  return out;
}

function pickTextByLang(row, lang, fallbackLang = "he") {
  // supports columns: he,en,ru,ar or text or value
  const r = normalizeRowKeys(row);
  const candidates = [
    r[lang],
    r[`text_${lang}`],
    r.text,
    r.value,
    r[fallbackLang],
    r[`text_${fallbackLang}`],
  ].filter((x) => String(x || "").trim());
  return String(candidates[0] || "").trim();
}

function detectLanguage(text, supportedCsv = "he,en,ru,ar") {
  const t = String(text || "");
  const supported = supportedCsv.split(",").map((x) => x.trim()).filter(Boolean);

  const hasArabic = /[\u0600-\u06FF]/.test(t);
  const hasCyr = /[\u0400-\u04FF]/.test(t);
  const hasHeb = /[\u0590-\u05FF]/.test(t);

  if (hasArabic && supported.includes("ar")) return "ar";
  if (hasCyr && supported.includes("ru")) return "ru";
  if (hasHeb && supported.includes("he")) return "he";
  if (supported.includes("en")) return "en";
  return supported[0] || "he";
}

async function fetchWithTimeout(url, options = {}, timeoutMs = 8000) {
  const ctrl = new AbortController();
  const id = setTimeout(() => ctrl.abort(), Math.max(1, timeoutMs));
  try {
    const res = await fetch(url, { ...options, signal: ctrl.signal });
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
    throw new Error("Service account JSON missing client_email/private_key");
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

  const headers = rows[0].map((h) => String(h || "").trim());
  return rows.slice(1).map((r) => {
    const obj = {};
    headers.forEach((h, i) => {
      obj[h] = r[i] ?? "";
    });
    return obj;
  });
}

// ===================== Config Loader + Cache =====================
let CONFIG_CACHE = { loaded_at: 0, data: null };

async function loadConfigFromSheet(force = false) {
  const ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - CONFIG_CACHE.loaded_at < ttlMs;

  if (!force && CONFIG_CACHE.data && fresh) {
    return {
      ok: true,
      from_cache: true,
      loaded_at: new Date(CONFIG_CACHE.loaded_at).toISOString(),
      ...CONFIG_CACHE.data,
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
  for (const row of results.SETTINGS || []) {
    const r = normalizeRowKeys(row);
    const k = String(r.key || "").trim();
    if (k) settings[k] = String(r.value ?? "").trim();
  }

  const cfg = {
    settings,
    business_info: results.BUSINESS_INFO || [],
    routing_rules: (results.ROUTING_RULES || []).map(normalizeRowKeys),
    sales_script: (results.SALES_SCRIPT || []).map(normalizeRowKeys),
    support_script: (results.SUPPORT_SCRIPT || []).map(normalizeRowKeys),
    suppliers: (results.SUPPLIERS || []).map(normalizeRowKeys),
    make_payloads_spec: (results.MAKE_PAYLOADS_SPEC || []).map(normalizeRowKeys),
    prompts: (results.PROMPTS || []).map(normalizeRowKeys),
    overview: {
      BUSINESS_NAME: settings.BUSINESS_NAME || "",
      DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "he",
      SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "he,en,ru,ar",
      SITE_BASE_URL: settings.SITE_BASE_URL || "",
      MAIN_PHONE: settings.MAIN_PHONE || "",
      BRANCHES: settings.BRANCHES || "",
    },
  };

  CONFIG_CACHE = { loaded_at: Date.now(), data: cfg };
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

// ===================== Gemini Fallback Router =====================
// Uses Google Generative Language API (v1beta) generateContent
// We force a JSON output. Still handle markdown fences / non-json safely.
function extractFirstJsonObject(s) {
  const text = String(s || "").trim();
  if (!text) return null;

  // remove ```json fences
  const unfenced = text.replace(/```json|```/gi, "").trim();

  // direct parse
  const direct = safeJsonParse(unfenced);
  if (direct.ok) return direct.value;

  // try find first {...}
  const start = unfenced.indexOf("{");
  const end = unfenced.lastIndexOf("}");
  if (start !== -1 && end !== -1 && end > start) {
    const mid = unfenced.slice(start, end + 1);
    const parsed = safeJsonParse(mid);
    if (parsed.ok) return parsed.value;
  }
  return null;
}

async function geminiRoute(text, allowedRoutes = ["sales", "support"]) {
  LAST_GEMINI_ERROR = "";

  if (!ENV.GEMINI_API_KEY) {
    LAST_GEMINI_ERROR = "GEMINI_API_KEY missing";
    return { ok: false, error: LAST_GEMINI_ERROR };
  }

  const model = ENV.GEMINI_MODEL; // e.g. models/gemini-2.0-flash-exp
  const url =
    `https://generativelanguage.googleapis.com/v1beta/${encodeURIComponent(model)}:generateContent?key=` +
    encodeURIComponent(ENV.GEMINI_API_KEY);

  const system = [
    "You are a strict router for a Hebrew voice bot.",
    `Return ONLY a JSON object with keys: route, confidence, reason.`,
    `route must be one of: ${allowedRoutes.join(", ")}.`,
    `confidence is a number 0..1.`,
    "If not sure, choose the most likely route with lower confidence.",
    "Do not add any extra text or markdown.",
  ].join(" ");

  const body = {
    contents: [
      {
        role: "user",
        parts: [{ text: `${system}\n\nUser text: ${text}` }],
      },
    ],
    generationConfig: {
      temperature: 0,
      maxOutputTokens: 120,
    },
  };

  let res;
  let rawBody = "";
  try {
    res = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      },
      ENV.GEMINI_TIMEOUT_MS
    );

    rawBody = await res.text();

    if (!res.ok) {
      LAST_GEMINI_ERROR = `Gemini HTTP ${res.status} ${res.statusText} | body: ${rawBody}`;
      return { ok: false, error: LAST_GEMINI_ERROR };
    }

    const parsed = safeJsonParse(rawBody);
    if (!parsed.ok) {
      LAST_GEMINI_ERROR = `Gemini response not JSON (API wrapper)`;
      return { ok: false, error: LAST_GEMINI_ERROR, raw: rawBody };
    }

    const data = parsed.value;
    const candText =
      data?.candidates?.[0]?.content?.parts?.map((p) => p.text).join("\n") || "";

    const obj = extractFirstJsonObject(candText);
    if (!obj || typeof obj !== "object") {
      LAST_GEMINI_ERROR = `Gemini parse failed (non-JSON response from model)`;
      return { ok: false, error: LAST_GEMINI_ERROR, raw_candidate: candText };
    }

    const route = String(obj.route || "").trim().toLowerCase();
    const confidence = Number(obj.confidence ?? 0);
    const reason = String(obj.reason || "").trim();

    if (!allowedRoutes.includes(route)) {
      LAST_GEMINI_ERROR = `Gemini returned invalid route: ${route}`;
      return { ok: false, error: LAST_GEMINI_ERROR, raw_candidate: candText };
    }

    return { ok: true, route, confidence, reason, raw_candidate: candText };
  } catch (e) {
    LAST_GEMINI_ERROR = `Gemini exception: ${e?.message || String(e)}`;
    return { ok: false, error: LAST_GEMINI_ERROR };
  }
}

// ===================== Website KB (crawl + cache + search) =====================
let KB_CACHE = {
  loaded_at: 0,
  base_url: "",
  pages: [], // [{url, text}]
  chars: 0,
  last_error: "",
};

function stripHtml(html) {
  const s = String(html || "");
  // remove scripts/styles
  const noScripts = s.replace(/<script[\s\S]*?<\/script>/gi, " ").replace(/<style[\s\S]*?<\/style>/gi, " ");
  // remove tags
  const noTags = noScripts.replace(/<\/?[^>]+>/g, " ");
  // decode basic entities
  const decoded = noTags
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
  return decoded.replace(/\s+/g, " ").trim();
}

function normalizeUrl(base, href) {
  try {
    const u = new URL(href, base);
    // keep same origin only
    const b = new URL(base);
    if (u.origin !== b.origin) return null;
    // drop hash
    u.hash = "";
    return u.toString();
  } catch {
    return null;
  }
}

function extractLinks(baseUrl, html) {
  const links = new Set();
  const re = /href\s*=\s*["']([^"']+)["']/gi;
  let m;
  while ((m = re.exec(String(html || "")))) {
    const href = m[1];
    const nu = normalizeUrl(baseUrl, href);
    if (nu) links.add(nu);
  }
  return [...links];
}

async function crawlWebsite(force, cfg) {
  const ttlMs = Math.max(1, ENV.KB_CACHE_TTL_SEC) * 1000;
  const fresh = Date.now() - KB_CACHE.loaded_at < ttlMs;
  const baseUrl = cfg?.overview?.SITE_BASE_URL || cfg?.settings?.SITE_BASE_URL || "";

  if (!baseUrl) {
    KB_CACHE.last_error = "SITE_BASE_URL missing in SETTINGS";
    return { ok: false, error: KB_CACHE.last_error };
  }

  if (!force && KB_CACHE.pages.length && fresh && KB_CACHE.base_url === baseUrl) {
    return {
      ok: true,
      from_cache: true,
      loaded_at: new Date(KB_CACHE.loaded_at).toISOString(),
      base_url: KB_CACHE.base_url,
      pages: KB_CACHE.pages.map((p) => p.url),
      chars: KB_CACHE.chars,
      last_error: KB_CACHE.last_error || "",
    };
  }

  LAST_KB_ERROR = "";
  KB_CACHE = { loaded_at: 0, base_url: baseUrl, pages: [], chars: 0, last_error: "" };

  const queue = [baseUrl];
  const seen = new Set();

  try {
    while (queue.length && KB_CACHE.pages.length < ENV.KB_MAX_PAGES) {
      const url = queue.shift();
      if (!url || seen.has(url)) continue;
      seen.add(url);

      let html = "";
      try {
        const res = await fetchWithTimeout(url, { method: "GET" }, 9000);
        if (!res.ok) continue;
        html = await res.text();
      } catch {
        continue;
      }

      const text = stripHtml(html).slice(0, ENV.KB_MAX_CHARS_PER_PAGE);
      if (text.length > 80) {
        KB_CACHE.pages.push({ url, text });
        KB_CACHE.chars += text.length;
      }

      const links = extractLinks(baseUrl, html);
      for (const l of links) {
        if (!seen.has(l) && queue.length < 200) queue.push(l);
      }
    }

    KB_CACHE.loaded_at = Date.now();
    return {
      ok: true,
      from_cache: false,
      loaded_at: nowIso(),
      base_url: KB_CACHE.base_url,
      pages: KB_CACHE.pages.map((p) => p.url),
      chars: KB_CACHE.chars,
      last_error: KB_CACHE.last_error || "",
    };
  } catch (e) {
    LAST_KB_ERROR = e?.message || String(e);
    KB_CACHE.last_error = LAST_KB_ERROR;
    return { ok: false, error: LAST_KB_ERROR };
  }
}

function kbSearch(query, topK = 3) {
  const q = normalizeText(query);
  if (!q || !KB_CACHE.pages.length) return [];

  const qTerms = q.split(" ").filter(Boolean).slice(0, 8);

  const scored = KB_CACHE.pages
    .map((p) => {
      const t = normalizeText(p.text);
      let score = 0;
      for (const term of qTerms) {
        if (term.length < 3) continue;
        if (t.includes(term)) score += 1;
      }
      return { url: p.url, text: p.text, score };
    })
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, topK);

  return scored;
}

// ===================== Make Events =====================
function getMakeUrl(cfg, keyName) {
  // ENV override wins; else from SETTINGS
  if (keyName === "MAKE_SEND_WA_URL" && ENV.MAKE_SEND_WA_URL) return ENV.MAKE_SEND_WA_URL;
  if (keyName === "MAKE_LEAD_URL" && ENV.MAKE_LEAD_URL) return ENV.MAKE_LEAD_URL;
  if (keyName === "MAKE_SUPPORT_URL" && ENV.MAKE_SUPPORT_URL) return ENV.MAKE_SUPPORT_URL;
  if (keyName === "MAKE_ABANDONED_URL" && ENV.MAKE_ABANDONED_URL) return ENV.MAKE_ABANDONED_URL;

  return String(cfg?.settings?.[keyName] || "").trim();
}

async function sendMakeEvent(url, payload) {
  if (!url) return { ok: false, error: "empty_url" };

  try {
    const res = await fetchWithTimeout(
      url,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      },
      2500
    );
    const text = await res.text().catch(() => "");
    return { ok: res.ok, status: res.status, body: text.slice(0, 400) };
  } catch (e) {
    return { ok: false, error: e?.message || String(e) };
  }
}

// ===================== Session Store (in-memory) =====================
const SESSIONS = new Map(); // session_id -> {route, step, lang, ...}
const SESSION_TTL_MS = 30 * 60 * 1000;

function gcSessions() {
  const now = Date.now();
  for (const [sid, s] of SESSIONS.entries()) {
    if (!s?.updated_at || now - s.updated_at > SESSION_TTL_MS) SESSIONS.delete(sid);
  }
}

function newSessionId() {
  return `s_${Math.random().toString(36).slice(2, 10)}_${Math.random().toString(36).slice(2, 10)}`;
}

// ===================== Script Helpers =====================
function getScriptRows(cfg, route) {
  if (route === "sales") return cfg.sales_script || [];
  if (route === "support") return cfg.support_script || [];
  return [];
}

function getOpeningText(cfg, lang) {
  const t = String(cfg?.settings?.OPENING_TEXT || "").trim();
  if (t) return t;
  // fallback generic
  const bn = cfg?.overview?.BUSINESS_NAME || "גיל ספורט";
  return `שָׁלוֹם, הִגַּעְתֶּם לְ־${bn}. אֵיךְ אֶפְשָׁר לַעֲזוֹר?`;
}

function getClosingText(cfg) {
  const t = String(cfg?.settings?.CLOSING_TEXT || "").trim();
  if (t) return t;
  return "תּוֹדָה שֶׁפְּנִיתֶם. יוֹם נָעִים!";
}

function findBlockByStep(rows, step) {
  // Supports either:
  // - column "step" numeric
  // - or block_id order in sheet (row index)
  if (!rows.length) return null;

  const withStep = rows.filter((r) => String(r.step || "").trim() !== "");
  if (withStep.length) {
    const target = withStep.find((r) => Number(r.step) === Number(step));
    return target || null;
  }

  // fallback: row index mapping (step 0 => row0)
  const idx = Math.max(0, Number(step));
  return rows[idx] || null;
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

app.post("/route", async (req, res) => {
  try {
    const text = String(req.body?.text || "").trim();
    const cfg = await loadConfigFromSheet(false);

    const bySheet = routeByKeywords(text, cfg.routing_rules);
    if (bySheet) return res.json({ ok: true, decision: bySheet });

    // Gemini fallback (hybrid)
    const gem = await geminiRoute(text, ["sales", "support"]);
    if (gem.ok && gem.confidence >= ENV.GEMINI_MIN_CONF) {
      return res.json({
        ok: true,
        decision: { route: gem.route, confidence: gem.confidence, by: "gemini" },
        debug: { gemini_model: ENV.GEMINI_MODEL, gemini_reason: gem.reason, raw_candidate: gem.raw_candidate },
      });
    }

    return res.json({
      ok: true,
      decision: {
        route: "unknown",
        confidence: 0,
        by: gem.ok ? "gemini_low_conf" : "gemini_failed",
      },
      debug: gem.ok
        ? { gemini_model: ENV.GEMINI_MODEL, gemini_reason: gem.reason, raw_candidate: gem.raw_candidate }
        : { gemini_error: gem.error, gemini_model: ENV.GEMINI_MODEL, timeout_ms: ENV.GEMINI_TIMEOUT_MS },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Website KB crawl
app.post("/kb", async (req, res) => {
  try {
    const force = Boolean(req.body?.force);
    const cfg = await loadConfigFromSheet(false);
    const out = await crawlWebsite(force, cfg);
    res.json(out);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Dialog engine (session-based)
app.post("/dialog", async (req, res) => {
  try {
    gcSessions();

    const cfg = await loadConfigFromSheet(false);

    const userText = String(req.body?.user_text || req.body?.text || "").trim();
    const incomingSessionId = String(req.body?.session_id || "").trim();
    const caller = String(req.body?.caller || req.body?.caller_id || "").trim();

    const supported = cfg?.overview?.SUPPORTED_LANGUAGES || "he,en,ru,ar";
    const lang = String(req.body?.language || "").trim() || detectLanguage(userText, supported);

    let sid = incomingSessionId || newSessionId();
    let session = SESSIONS.get(sid);

    if (!session) {
      session = {
        session_id: sid,
        route: "unknown",
        step: 0,
        language: lang,
        created_at: Date.now(),
        updated_at: Date.now(),
        last_user_text: "",
      };
      SESSIONS.set(sid, session);
    }

    session.updated_at = Date.now();
    if (userText) session.last_user_text = userText;

    // If empty input: return opening
    if (!userText && session.step === 0) {
      const opening = getOpeningText(cfg, lang);
      session.step = 1;
      return res.json({
        ok: true,
        session_id: sid,
        route: session.route,
        step: session.step,
        language: lang,
        bot_say: opening,
        expect: "user_text",
        event: null,
        block_id: "OPENING",
      });
    }

    // Determine route if unknown
    let decision = null;
    if (session.route === "unknown") {
      const bySheet = routeByKeywords(userText, cfg.routing_rules);
      if (bySheet) {
        decision = bySheet;
      } else {
        const gem = await geminiRoute(userText, ["sales", "support"]);
        if (gem.ok && gem.confidence >= ENV.GEMINI_MIN_CONF) {
          decision = { route: gem.route, confidence: gem.confidence, by: "gemini", reason: gem.reason };
        } else {
          decision = { route: "unknown", confidence: 0, by: gem.ok ? "gemini_low_conf" : "gemini_failed" };
        }
      }

      session.route = decision.route;
      session.route_confidence = decision.confidence;
      session.route_by = decision.by;

      // Fire Make event (non-blocking-ish)
      const payloadBase = {
        ts: nowIso(),
        session_id: sid,
        caller,
        route: session.route,
        confidence: session.route_confidence || 0,
        by: session.route_by || "",
        user_text: userText,
        language: lang,
      };

      if (session.route === "sales") {
        const url = getMakeUrl(cfg, "MAKE_LEAD_URL");
        sendMakeEvent(url, { event: "lead", ...payloadBase }).then((r) => log("debug", "MAKE lead", r));
      } else if (session.route === "support") {
        const url = getMakeUrl(cfg, "MAKE_SUPPORT_URL");
        sendMakeEvent(url, { event: "support", ...payloadBase }).then((r) => log("debug", "MAKE support", r));
      }
    }

    // If still unknown: try website KB answer
    if (session.route === "unknown") {
      // ensure KB is crawled (cached)
      await crawlWebsite(false, cfg);
      const hits = kbSearch(userText, 2);

      if (hits.length) {
        const best = hits[0];
        const snippet = best.text.slice(0, 380);
        const say =
          `מָצָאתִי בָּאֲתָר מֵידָע שֶׁכְּנִרְאֶה קָשׁוּר:\n` +
          `${snippet}\n` +
          `רוצים שֶׁאֲנִי אֲדַיֵּק אֶת הַשְּׁאֵלָה אוֹ שֶׁאֲשַׁלַּח לָכֶם קִישּׁוּר בְּוָאטְסְאַפּ?`;

        // optional Make send WA suggestion event (not sending yet)
        return res.json({
          ok: true,
          session_id: sid,
          route: "unknown",
          step: session.step,
          language: lang,
          bot_say: say,
          expect: "user_text",
          event: null,
          block_id: "KB_FALLBACK",
          debug: { kb_url: best.url, kb_score: best.score, pages_cached: KB_CACHE.pages.length },
        });
      }

      // no KB hit -> generic ask
      return res.json({
        ok: true,
        session_id: sid,
        route: "unknown",
        step: session.step,
        language: lang,
        bot_say: "בְּמָה אֶפְשָׁר לַעֲזוֹר?",
        expect: "user_text",
        event: null,
        block_id: "UNKNOWN",
        debug: { script: "UNKNOWN", rows: 0 },
      });
    }

    // Route known: run script (sales/support)
    const rows = getScriptRows(cfg, session.route);
    if (!rows.length) {
      return res.json({
        ok: true,
        session_id: sid,
        route: session.route,
        step: session.step,
        language: lang,
        bot_say: "יֵשׁ לִי נִיתּוּב, אֲבָל אֵין עֲדַיִן תַּסְרִיט בַּשִּׁיטְס. רוֹצִים שֶׁאֲעָבִיר אֶתְכֶם לְנָצִיג?",
        expect: "user_text",
        event: null,
        block_id: "NO_SCRIPT",
        debug: { route: session.route, rows: 0 },
      });
    }

    // step handling:
    // step=0 reserved, step>=1 script steps
    const step = Math.max(0, Number(session.step || 0));
    const block = findBlockByStep(rows, step - 1); // step1 -> row0
    const blockId = String(block?.block_id || block?.id || "").trim() || `STEP_${step}`;

    // pick text
    let sayText = pickTextByLang(block || {}, lang, cfg.overview.DEFAULT_LANGUAGE || "he");

    // If block text empty, fallback
    if (!sayText) {
      sayText =
        session.route === "sales"
          ? "בְּהֶמְשֶׁךְ לָזֶה, אֵיזֶה מוּצָר מְעַנְיֵן אֶתְכֶם?"
          : "בְּהֶמְשֶׁךְ לָזֶה, אֵיזוֹ תַּקָּלָה יֵשׁ לָכֶם?";
    }

    // next step
    session.step = step + 1;

    // simple end condition (if script ended)
    const isEnd = (step - 1) >= rows.length - 1;
    if (isEnd) {
      sayText = `${sayText}\n${getClosingText(cfg)}`;
    }

    return res.json({
      ok: true,
      session_id: sid,
      route: session.route,
      step: session.step,
      language: lang,
      bot_say: sayText,
      expect: isEnd ? null : "user_text",
      event: null,
      block_id: blockId,
      debug: { script: session.route.toUpperCase(), rows: rows.length },
    });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
});
