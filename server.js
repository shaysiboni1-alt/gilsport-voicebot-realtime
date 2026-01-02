// server.js
//
// GilSport VoiceBot Realtime - Config + Router service
// - /health
// - /config-check
// - /route  (keywords first, Gemini fallback)
//
// Google Sheets access via Service Account (JWT)
// Node 18+ (Render uses Node 22.x)

import express from "express";
import { google } from "googleapis";
import fetch from "node-fetch";

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
  GEMINI_MODEL:
    process.env.GEMINI_MODEL || "models/gemini-2.0-flash-exp",

  LOG_LEVEL: (process.env.LOG_LEVEL || "info").toLowerCase(),
};

// ===================== Utils =====================
function safeJsonParse(maybeJson) {
  try {
    if (!maybeJson) return { ok: false };
    return { ok: true, value: JSON.parse(maybeJson) };
  } catch {
    return { ok: false };
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

// ===================== Google Sheets =====================
function getServiceAccountAuth() {
  const raw = ENV.GOOGLE_SERVICE_ACCOUNT_JSON;
  if (!raw) throw new Error("GOOGLE_SERVICE_ACCOUNT_JSON missing");

  const parsed = safeJsonParse(raw);
  const creds = parsed.ok
    ? parsed.value
    : JSON.parse(Buffer.from(raw, "base64").toString("utf8"));

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

// ===================== Config Cache =====================
let CACHE = { loaded_at: 0, data: null };

async function loadConfigFromSheet(force = false) {
  const ttlMs = Math.max(1, ENV.GSHEET_CACHE_TTL_SEC) * 1000;
  if (!force && CACHE.data && Date.now() - CACHE.loaded_at < ttlMs) {
    return { ok: true, from_cache: true, loaded_at: nowIso(), ...CACHE.data };
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
  for (const t of tabs) {
    results[t] = await fetchSheetTab(auth, ENV.GSHEET_ID, t);
  }

  const settings = {};
  for (const r of results.SETTINGS || []) {
    if (r.key) settings[r.key.trim()] = String(r.value ?? "").trim();
  }

  const cfg = {
    settings,
    routing_rules: results.ROUTING_RULES || [],
    overview: {
      BUSINESS_NAME: settings.BUSINESS_NAME || "",
      DEFAULT_LANGUAGE: settings.DEFAULT_LANGUAGE || "he",
      SUPPORTED_LANGUAGES: settings.SUPPORTED_LANGUAGES || "he",
      SITE_BASE_URL: settings.SITE_BASE_URL || "",
    },
  };

  CACHE = { loaded_at: Date.now(), data: cfg };
  return { ok: true, from_cache: false, loaded_at: nowIso(), ...cfg };
}

// ===================== Router: Keywords =====================
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

// ===================== Router: Gemini Fallback =====================
async function routeByGemini(text) {
  if (!ENV.GEMINI_API_KEY) throw new Error("GEMINI_API_KEY missing");

  const body = {
    contents: [
      {
        role: "user",
        parts: [
          {
            text: `
Classify the intent into one of:
- sales
- support
- ambiguous

Return JSON ONLY:
{ "route": "...", "confidence": 0-1 }

Text:
"${text}"
`,
          },
        ],
      },
    ],
  };

  const url = `https://generativelanguage.googleapis.com/v1beta/${ENV.GEMINI_MODEL}:generateContent?key=${ENV.GEMINI_API_KEY}`;

  const res = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });

  if (!res.ok) throw new Error(`Gemini HTTP ${res.status}`);

  const json = await res.json();
  const raw =
    json?.candidates?.[0]?.content?.parts?.[0]?.text || "";

  const parsed = safeJsonParse(raw);
  if (!parsed.ok || !parsed.value?.route) {
    throw new Error("Gemini invalid JSON");
  }

  return {
    route: parsed.value.route,
    confidence: Number(parsed.value.confidence || 0),
    by: "gemini",
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
        ROUTING_RULES: cfg.routing_rules.length,
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
    if (bySheet) {
      return res.json({ ok: true, decision: bySheet });
    }

    try {
      const byGemini = await routeByGemini(text);
      return res.json({ ok: true, decision: byGemini });
    } catch {
      return res.json({
        ok: true,
        decision: { route: "unknown", confidence: 0, by: "gemini_failed" },
      });
    }
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.listen(PORT, () => {
  console.log(`[BOOT] listening on ${PORT}`);
});
