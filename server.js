// server.js
// GilSport VoiceBot – Sheet-Driven, No FSM (field-driven), Realtime (Twilio + OpenAI)
// Single Source of Truth: Google Sheets
// - Text responses ONLY from Sheets (PROMPTS + SETTINGS); minimal fallbacks if missing prompt id.
// - DO_NOT_SAY enforced
// - Webhook only after required fields collected
// - Abandoned webhook on disconnect before completion
// - Voice/style/rate/VAD strictly via ENV (no changes required in code)

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const fetch = global.fetch || require("node-fetch");

// --------------------------------------------------
// ENV helpers
// --------------------------------------------------
const envNum = (k, d) => {
  const v = Number(process.env[k]);
  return Number.isFinite(v) ? v : d;
};
const envBool = (k, d = false) =>
  ["1", "true", "yes", "on"].includes(String(process.env[k] || "").toLowerCase()) || d;

// --------------------------------------------------
// Core ENV (voice/style strictly via ENV)
// --------------------------------------------------
const PORT = envNum("PORT", 10000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

const ALLOWED_VOICES = new Set([
  "alloy",
  "ash",
  "ballad",
  "coral",
  "echo",
  "sage",
  "shimmer",
  "verse",
  "marin",
  "cedar"
]);
function normalizeVoice(v) {
  const x = String(v || "").toLowerCase().trim();
  return ALLOWED_VOICES.has(x) ? x : "alloy";
}
const OPENAI_VOICE = normalizeVoice(process.env.OPENAI_VOICE || "alloy");
const OPENAI_VOICE_STYLE = process.env.OPENAI_VOICE_STYLE || "";
const OPENAI_SPEAKING_RATE = (() => {
  const r = parseFloat(process.env.OPENAI_SPEAKING_RATE);
  return Number.isFinite(r) && r > 0 ? r : 1.0;
})();

const MB_BASE_STYLE = process.env.MB_BASE_STYLE || ""; // make Neta happier/energetic here (ENV)

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const MB_WEBHOOK_URL = process.env.MB_WEBHOOK_URL || "";
const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

const MB_DEBUG = envBool("MB_DEBUG", false);

const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.75);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNum("MB_VAD_SUFFIX_MS", 150);

const MB_NO_BARGE_TAIL_MS = envNum("MB_NO_BARGE_TAIL_MS", 1600);

const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";

const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_LOG_RAW_OPENAI = envBool("MB_LOG_RAW_OPENAI", false);

const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || "";
const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const debug = (...a) => MB_DEBUG && console.log("[DEBUG]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);
const always = (...a) => console.log("[ALWAYS]", ...a);

const nowIso = () => new Date().toISOString();

const preview = (s, n = 240) => {
  const t = String(s || "").replace(/\s+/g, " ").trim();
  return t.length > n ? t.slice(0, n) + "..." : t;
};

// --------------------------------------------------
// Sheets – Single Source of Truth
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {},            // PROMPTS: prompt_id -> content_he
  settings: {},           // SETTINGS: key -> value
  kbFacts: [],            // KB_FACTS rows
  doNotSay: [],           // DO_NOT_SAY rows
  suppliersImporters: [], // SUPPLIERS_IMPORTERS rows
  deliveryContacts: []    // DELIVERY_CONTACTS rows
};

const rowsToObjects = (rows) => {
  const out = [];
  const headers = (rows.shift() || []).map((h) => String(h || "").trim());
  if (!headers.length) return out;
  for (const r of rows) {
    const o = {};
    headers.forEach((h, i) => (o[h] = r[i] || ""));
    const hasAny = Object.values(o).some((v) => String(v || "").trim() !== "");
    if (hasAny) out.push(o);
  }
  return out;
};

const parseTable = (rows, keyColName, valColName) => {
  const out = {};
  const headers = (rows.shift() || []).map((h) => String(h || "").trim());
  const keyIdx = headers.indexOf(keyColName);
  const valIdx = headers.indexOf(valColName);
  if (keyIdx === -1 || valIdx === -1) return out;

  for (const r of rows) {
    const k = String(r[keyIdx] || "").trim();
    const v = String(r[valIdx] || "");
    if (!k) continue;
    out[k] = v;
  }
  return out;
};

async function loadSheets() {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) return;

  try {
    const json = JSON.parse(
      Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8")
    );

    const auth = new google.auth.JWT({
      email: json.client_email,
      key: json.private_key,
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    });

    const sheets = google.sheets({ version: "v4", auth });

    const res = await sheets.spreadsheets.values.batchGet({
      spreadsheetId: GSHEET_ID,
      ranges: [
        "PROMPTS!A:Z",
        "SETTINGS!A:Z",
        "KB_FACTS!A:Z",
        "DO_NOT_SAY!A:Z",
        "SUPPLIERS_IMPORTERS!A:Z",
        "DELIVERY_CONTACTS!A:Z"
      ]
    });

    const vr = res.data.valueRanges || [];
    const get = (prefix) =>
      vr.find((v) => String(v.range || "").startsWith(prefix))?.values || [];

    // PROMPTS (expects prompt_id + content_he)
    const prompts = {};
    const pRows = get("PROMPTS!").slice();
    if (pRows.length) {
      const headers = pRows.shift() || [];
      for (const r of pRows) {
        const row = {};
        headers.forEach((h, i) => (row[h] = r[i] || ""));
        if (row.prompt_id && row.content_he) {
          prompts[String(row.prompt_id).trim()] = String(row.content_he);
        }
      }
    }

    // SETTINGS (expects key + value)
    const sRows = get("SETTINGS!").slice();
    const settings = sRows.length ? parseTable(sRows, "key", "value") : {};

    SHEETS = {
      loaded_at: new Date().toISOString(),
      prompts,
      settings,
      kbFacts: rowsToObjects(get("KB_FACTS!").slice()),
      doNotSay: rowsToObjects(get("DO_NOT_SAY!").slice()),
      suppliersImporters: rowsToObjects(get("SUPPLIERS_IMPORTERS!").slice()),
      deliveryContacts: rowsToObjects(get("DELIVERY_CONTACTS!").slice())
    };

    log("Sheets loaded", {
      prompts: Object.keys(prompts).length,
      settings: Object.keys(settings).length,
      kbFacts: SHEETS.kbFacts.length,
      doNotSay: SHEETS.doNotSay.length,
      suppliersImporters: SHEETS.suppliersImporters.length,
      deliveryContacts: SHEETS.deliveryContacts.length
    });
  } catch (e) {
    error("Sheets load failed", e?.message || e);
  }
}

const getPrompt = (id, fallback = "") => String(SHEETS.prompts[id] || fallback).trim();
const getSetting = (k, fallback = "") => String(SHEETS.settings[k] || fallback).trim();

// --------------------------------------------------
// Express
// --------------------------------------------------
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.get("/health", (_, res) => {
  res.json({
    ok: true,
    sheets_loaded_at: SHEETS.loaded_at,
    prompts: Object.keys(SHEETS.prompts || {}).length,
    settings: Object.keys(SHEETS.settings || {}).length
  });
});

app.post("/sheets/reload", async (_, res) => {
  await loadSheets();
  res.json({ ok: true, at: SHEETS.loaded_at });
});

// Twilio Voice → Media Stream
app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  const wsUrl = `wss://${host}/twilio-media-stream`;

  res.type("text/xml").send(
    `
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${req.body.From || ""}" />
      <Parameter name="called" value="${req.body.To || ""}" />
    </Stream>
  </Connect>
</Response>
`.trim()
  );
});

const server = http.createServer(app);

// --------------------------------------------------
// Helpers: text, yes/no, phone, after-hours
// --------------------------------------------------
const normalizeText = (s) =>
  String(s || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();

const isYes = (text) =>
  /(כן|כן כן|נכון|מאשר|אישור|yes|yep|yeah|ok|בסדר|סבבה|מוסכם)/i.test(String(text || "").trim());

const isNo = (text) =>
  /(לא|לא תודה|לא זה|לא נכון|no|nope|לא מעוניין|לא מסכים)/i.test(String(text || "").trim());

const extractPhoneDigits = (raw) => {
  let t = String(raw || "");
  const map = {
    "אפס": "0",
    "אחד": "1",
    "אחת": "1",
    "שתיים": "2",
    "שתים": "2",
    "שניים": "2",
    "שנים": "2",
    "שלוש": "3",
    "שלושה": "3",
    "ארבע": "4",
    "ארבעה": "4",
    "חמש": "5",
    "חמישה": "5",
    "שש": "6",
    "שישה": "6",
    "שבע": "7",
    "שבעה": "7",
    "שמונה": "8",
    "תשע": "9",
    "תשעה": "9"
  };
  for (const [w, d] of Object.entries(map)) {
    t = t.replace(new RegExp(`\\b${w}\\b`, "g"), d);
  }
  let digits = t.replace(/\D+/g, "");
  if (digits.startsWith("972") && digits.length > 3) digits = "0" + digits.slice(3);
  if (digits.startsWith("0") && digits.length > 10) digits = digits.slice(0, 10);
  return digits;
};
const isValidPhone = (d) => String(d || "").length === 10 && String(d || "").startsWith("0");
const spacedPhone = (d) => String(d || "").split("").join(" ");

const ensureCallerDigits = (callerRaw) => {
  const d = extractPhoneDigits(String(callerRaw || ""));
  return isValidPhone(d) ? d : "";
};

const parseHours = (s) => {
  const m = String(s || "").match(/(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})/);
  if (!m) return null;
  const aH = Number(m[1]), aM = Number(m[2]), bH = Number(m[3]), bM = Number(m[4]);
  if (![aH, aM, bH, bM].every((x) => Number.isFinite(x))) return null;
  return { start: aH * 60 + aM, end: bH * 60 + bM };
};

const isAfterHours = () => {
  const hoursStr =
    getSetting("BUSINESS_HOURS", "") ||
    getSetting("WORKING_HOURS", "") ||
    getSetting("HOURS", "") ||
    "";
  const parsed = parseHours(hoursStr);
  if (!parsed) return false;

  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: TIME_ZONE,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit"
  }).formatToParts(now);
  const hh = Number(parts.find((p) => p.type === "hour")?.value || 0);
  const mm = Number(parts.find((p) => p.type === "minute")?.value || 0);
  const cur = hh * 60 + mm;

  return cur < parsed.start || cur > parsed.end;
};

// --------------------------------------------------
// KB_FACTS routing helper (category=ROUTING)
// expects: category, keywords, topic
// topic should map to: sales/support/delivery/message
// --------------------------------------------------
const detectIntent = (text) => {
  const t = normalizeText(text);

  for (const row of SHEETS.kbFacts || []) {
    const cat = String(row.category || "").trim().toLowerCase();
    if (cat !== "routing") continue;
    const keys = String(row.keywords || "")
      .split(",")
      .map((k) => normalizeText(k));
    if (keys.some((k) => k && t.includes(k))) {
      const topic = String(row.topic || "").trim().toLowerCase();
      if (["sales", "support", "delivery", "message"].includes(topic)) return topic;
    }
  }

  // fallback keywords
  if (/אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול/.test(t)) return "support";
  if (/משלוח|אספקה|הספקה|שליח|הזמנה|הגיע|לא הגיע|מוביל/.test(t)) return "delivery";
  if (/מחיר|לקנות|רכישה|מוצר|דגם|מידה|צבע|מלאי|כמה עולה|מבצע/.test(t)) return "sales";
  if (/הודעה|מנהל|עובד|לחזור אלי|השארת הודעה/.test(t)) return "message";
  return "";
};

// --------------------------------------------------
// DO_NOT_SAY guard
// - if triggers match, respond with safe_response_he, but do not advance flow
// --------------------------------------------------
const doNotSayGuard = (text) => {
  const t = normalizeText(text);
  const rows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
  for (const r of rows) {
    const triggers = String(r.trigger_examples || "")
      .split(",")
      .map((x) => normalizeText(x))
      .filter(Boolean);
    if (!triggers.length) continue;
    if (triggers.some((tr) => tr && t.includes(tr))) {
      return String(r.safe_response_he || "").trim();
    }
  }
  return "";
};

// --------------------------------------------------
// SUPPLIERS_IMPORTERS exact match helper (brand_name)
// --------------------------------------------------
const findExactImporter = (brandName) => {
  const brand = String(brandName || "").trim();
  if (!brand) return null;
  const rows = Array.isArray(SHEETS.suppliersImporters) ? SHEETS.suppliersImporters : [];
  const match = rows.find((r) => String(r.brand_name || "").trim() === brand);
  if (!match) return null;
  return {
    brand,
    importer_name: String(match.importer_name || "").trim(),
    phone_e164: String(match.phone_e164 || match.phone || "").trim()
  };
};

// --------------------------------------------------
// DELIVERY_CONTACTS list -> "Name – 0 X X..." (spaced)
// --------------------------------------------------
const buildCarrierList = () => {
  const rows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
  const out = [];
  for (const r of rows) {
    let p = String(r.phone_e164 || r.phone || "").replace(/\D+/g, "");
    if (!p) continue;
    if (p.startsWith("972") && p.length > 3) p = "0" + p.slice(3);
    if (p.startsWith("0") && p.length > 10) p = p.slice(0, 10);
    const name = String(r.name || "").trim();
    const item = name ? `${name} – ${spacedPhone(p)}` : spacedPhone(p);
    out.push(item);
  }
  return out;
};

// --------------------------------------------------
// Webhook helper
// --------------------------------------------------
const publicRecordingUrl = (callSid) => {
  if (!PUBLIC_BASE_URL || !callSid) return "";
  const base = String(PUBLIC_BASE_URL).replace(/\/$/, "");
  return `${base}/recording/${callSid}`;
};

async function sendWebhookEvent(event, payload) {
  if (!MB_WEBHOOK_URL) return false;
  try {
    const resp = await fetch(MB_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ event, ...payload })
    });
    return resp.ok;
  } catch (e) {
    debug("Webhook failed", e?.message || e);
    return false;
  }
}

// --------------------------------------------------
// WebSocket: Twilio <-> OpenAI (single connection handler)
// --------------------------------------------------
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

wss.on("connection", (twilioWs, req) => {
  const connTag = `conn_${Date.now().toString(36)}_${Math.random().toString(16).slice(2, 6)}`;

  let openaiWs = null;
  let openaiReady = false;

  let streamSid = "";
  let callSid = "";
  let caller = "";
  let called = "";

  let startedAt = nowIso();
  let endedAt = null;

  let transcript = []; // {from,text,at}
  const pushTurn = (from, text) => {
    const t = String(text || "").trim();
    if (!t) return;
    transcript.push({ from, text: t, at: nowIso() });
    if (transcript.length > 400) transcript = transcript.slice(-400);
  };

  // anti-overlap (like your working code)
  let awaitingResponse = false;
  const pausedAudioBuffer = [];
  let isFlushingBufferedAudio = false;

  // For request dedupe
  let lastRequestedCaller = "";
  let lastCallerFinal = "";
  let lastBotFinal = "";

  // Flow data (field-driven, not FSM)
  const data = {
    // routing
    intent: "",

    // sales
    product_type: "",
    full_name: "",
    product_model: "",
    product_brand: "",
    sales_reinforced_sent: false,

    // support
    issue_desc: "",
    support_model: "", // map to product_model
    support_brand: "", // map to product_brand
    importer_offer_done: false,
    importer_found: null, // {brand, importer_name, phone}
    importer_phone_given: false,

    // delivery
    delivery_desc: "",
    after_hours: false,
    delivery_same_day: false,
    carrier_offer_done: false,
    carrier_info_given: false,

    // message
    message_target: "",
    message_body: "",

    // phones
    caller_id: "",
    callback_phone: "",
    extra_phone: "",
    phone_confirmed: false,

    // done
    final_event: "",
    completed: false
  };

  const requiredComplete = () => {
    // webhook sent only when all required fields collected per route
    if (data.intent === "sales") {
      return Boolean(data.product_type && data.full_name && data.product_model && data.product_brand && data.callback_phone);
    }
    if (data.intent === "support") {
      return Boolean(data.issue_desc && data.product_model && data.full_name && data.callback_phone);
    }
    if (data.intent === "delivery") {
      return Boolean(data.delivery_desc && data.full_name && data.callback_phone);
    }
    if (data.intent === "message") {
      return Boolean(data.message_target && data.message_body && data.full_name && data.callback_phone);
    }
    return false;
  };

  const getEventName = () => {
    if (data.intent === "sales") return "sales_lead";
    if (data.intent === "support") return "support_ticket";
    if (data.intent === "delivery") return "delivery_ticket";
    if (data.intent === "message") return "message_taken";
    return "call_ended";
  };

  const getStageName = () => {
    // for abandoned “שלב שבו נותקה”
    if (!data.intent) return "routing";
    if (data.intent === "sales") {
      if (!data.product_type) return "sales_product";
      if (!data.full_name) return "sales_name";
      if (!data.sales_reinforced_sent) return "sales_reinforce";
      if (!data.product_model) return "sales_model";
      if (!data.product_brand) return "sales_brand";
      if (!data.callback_phone) return "sales_phone";
      return "sales_done";
    }
    if (data.intent === "support") {
      if (!data.issue_desc) return "support_issue_desc";
      if (!data.product_model) return "support_model";
      if (!data.product_brand) return "support_brand";
      if (!data.importer_offer_done) return "support_importer_offer";
      if (!data.full_name) return "support_name";
      if (!data.callback_phone) return "support_phone";
      return "support_done";
    }
    if (data.intent === "delivery") {
      if (!data.delivery_desc) return "delivery_desc";
      if (!data.carrier_offer_done) return "delivery_carrier_offer";
      if (!data.full_name) return "delivery_name";
      if (!data.callback_phone) return "delivery_phone";
      return "delivery_done";
    }
    if (data.intent === "message") {
      if (!data.message_target) return "message_target";
      if (!data.message_body) return "message_body";
      if (!data.full_name) return "message_name";
      if (!data.callback_phone) return "message_phone";
      return "message_done";
    }
    return "other";
  };

  // Render template vars: {caller_id} {number} {brand} {carriers} {target}
  const render = (tpl, vars = {}) =>
    String(tpl || "").replace(/\{(\w+)\}/g, (_, k) =>
      Object.prototype.hasOwnProperty.call(vars, k) ? String(vars[k]) : `{${k}}`
    );

  // Build next bot SAY from sheets only (fallback minimal)
  const nextSayFromSheets = () => {
    // routing
    if (!data.intent) {
      return getPrompt(
        "FLOW_ROUTING",
        getSetting("NO_DATA_MESSAGE", "כדי לעזור במדויק—זה לגבי מכירה, שירות/תקלה, אספקה/משלוח, או הודעה?")
      );
    }

    // SALES (exact structure per your spec)
    if (data.intent === "sales") {
      if (!data.product_type) return getPrompt("FLOW_SALES_PRODUCT", "על איזה מוצר אתם מתעניינים?");
      if (!data.full_name) return getPrompt("FLOW_SALES_NAME", "מה השם המלא שלכם?");

      if (!data.sales_reinforced_sent) {
        data.sales_reinforced_sent = true;
        const claim = getSetting("PRICE_CLAIM_SENTENCE", "");
        const promo = getPrompt("SALES_PROMPT", "");
        // still sheets only; if empty, returns ""
        const merged = [claim, promo].filter(Boolean).join(" ");
        return merged || getSetting("NO_DATA_MESSAGE", "מעולה, תודה.");
      }

      // Model question (yes/no + collect) – driven by prompts if exist
      if (!data.product_model) {
        // Ask if specific model
        if (!data._salesAskedModelQ) {
          data._salesAskedModelQ = true;
          return getPrompt(
            "FLOW_SALES_MODEL_Q",
            "האם יש דגם ספציפי? אם כן—איזה דגם?"
          );
        }
        // Collect model
        return getPrompt(
          "FLOW_SALES_MODEL_COLLECT",
          "מה הדגם הספציפי, בבקשה?"
        );
      }

      // Brand question (yes/no + collect)
      if (!data.product_brand) {
        if (!data._salesAskedBrandQ) {
          data._salesAskedBrandQ = true;
          return getPrompt(
            "FLOW_SALES_BRAND_Q",
            "האם יש מותג ספציפי? אם כן—מה שם המותג?"
          );
        }
        return getPrompt(
          "FLOW_SALES_BRAND_COLLECT",
          "מה שם המותג, בבקשה?"
        );
      }

      // Phone confirm
      if (!data.callback_phone) {
        const cid = data.caller_id ? spacedPhone(data.caller_id) : "";
        if (cid) {
          return render(
            getPrompt(
              "FLOW_SALES_PHONE_CONFIRM",
              "האם לחזור אליכם למספר הזה: {caller_id} ?"
            ),
            { caller_id: cid }
          );
        }
        return getPrompt("FLOW_SALES_PHONE_COLLECT", "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
      }

      return getPrompt("FLOW_SALES_DONE", "מעולה. העברתי את הפרטים למכירות, ויחזרו אליכם בהקדם. תודה ויום טוב.");
    }

    // SUPPORT (exact structure per your spec)
    if (data.intent === "support") {
      if (!data.issue_desc) return getPrompt("FLOW_SUPPORT_ISSUE_DESC", "מה התקלה בכמה מילים?");
      if (!data.product_model) {
        if (!data._supportAskedModelQ) {
          data._supportAskedModelQ = true;
          return getPrompt("FLOW_SUPPORT_MODEL_Q", "מה הדגם?");
        }
        return getPrompt("FLOW_SUPPORT_MODEL_COLLECT", "מה הדגם, בבקשה?");
      }
      if (!data.product_brand) {
        if (!data._supportAskedBrandQ) {
          data._supportAskedBrandQ = true;
          return getPrompt("FLOW_SUPPORT_BRAND_Q", "מה המותג? ואם לא ידוע—תגידו 'לא ידוע'.");
        }
        return getPrompt("FLOW_SUPPORT_BRAND_COLLECT", "מה המותג, בבקשה? ואם לא ידוע—'לא ידוע'.");
      }

      // Importer offer (only if brand exists and found in sheet)
      if (!data.importer_offer_done) {
        data.importer_offer_done = true;

        const importer = findExactImporter(data.product_brand);
        if (importer && importer.phone_e164) {
          // normalize to 0xxxxxxxxx spaced
          let phone = String(importer.phone_e164 || "").replace(/\D+/g, "");
          if (phone.startsWith("972") && phone.length > 3) phone = "0" + phone.slice(3);
          if (phone.startsWith("0") && phone.length > 10) phone = phone.slice(0, 10);
          data.importer_found = { ...importer, phone };
          return render(
            getPrompt(
              "FLOW_SUPPORT_IMPORTER_OFFER",
              "מצאתי מספר ישיר ליבואן של {brand}. רוצים שאמסור אותו?"
            ),
            { brand: importer.brand }
          );
        }
        // no importer match -> move on silently
      }

      if (!data.full_name) return getPrompt("FLOW_SUPPORT_NAME", "מה השם המלא שלכם?");

      if (!data.callback_phone) {
        const cid = data.caller_id ? spacedPhone(data.caller_id) : "";
        if (cid) {
          return render(
            getPrompt(
              "FLOW_SUPPORT_PHONE_CONFIRM",
              "האם לחזור אליכם למספר הזה: {caller_id} ?"
            ),
            { caller_id: cid }
          );
        }
        return getPrompt("FLOW_SUPPORT_PHONE_COLLECT", "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
      }

      return getPrompt("FLOW_SUPPORT_DONE", "מעולה. שלחתי את הפרטים לשירות, ויחזרו אליכם בהקדם. תודה ויום טוב.");
    }

    // DELIVERY (after-hours same-day carrier offer)
    if (data.intent === "delivery") {
      if (!data.delivery_desc) return getPrompt("FLOW_DELIVERY_DESC", "מה הבקשה לגבי משלוח או אספקה?");
      if (!data.carrier_offer_done) {
        data.after_hours = isAfterHours();
        data.delivery_same_day = /היום|מהיום|עוד היום|לאותו יום/i.test(String(data.delivery_desc || ""));
        if (data.after_hours && data.delivery_same_day) {
          const carriers = buildCarrierList();
          if (carriers.length) {
            return getPrompt(
              "FLOW_DELIVERY_CARRIER_OFFER",
              "ציינתם אספקה להיום אחרי שעות פעילות. יש ברשותי מספרי מובילים—רוצים שאמסור?"
            );
          }
        }
        // if not applicable -> mark done and continue
        data.carrier_offer_done = true;
      }

      if (!data.full_name) return getPrompt("FLOW_DELIVERY_NAME", "מה השם המלא שלכם?");
      if (!data.callback_phone) {
        const cid = data.caller_id ? spacedPhone(data.caller_id) : "";
        if (cid) {
          return render(
            getPrompt(
              "FLOW_DELIVERY_PHONE_CONFIRM",
              "האם לחזור אליכם למספר הזה: {caller_id} ?"
            ),
            { caller_id: cid }
          );
        }
        return getPrompt("FLOW_DELIVERY_PHONE_COLLECT", "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
      }

      return getPrompt("FLOW_DELIVERY_DONE", "תודה. העברתי את הפרטים למחלקת אספקה, ויחזרו אליכם בהקדם. יום טוב.");
    }

    // MESSAGE (if target already captured early, don't ask again)
    if (data.intent === "message") {
      if (!data.message_target) return getPrompt("FLOW_MESSAGE_TARGET", "למי מיועדת ההודעה? (שם עובד/מנהל)");
      if (!data.message_body) return getPrompt("FLOW_MESSAGE_BODY", "מה תוכן ההודעה בקצרה?");
      if (!data.full_name) return getPrompt("FLOW_MESSAGE_NAME", "מה השם המלא שלכם?");
      if (!data.callback_phone) {
        const cid = data.caller_id ? spacedPhone(data.caller_id) : "";
        if (cid) {
          return render(
            getPrompt(
              "FLOW_MESSAGE_PHONE_CONFIRM",
              "האם לחזור אליכם למספר הזה: {caller_id} ?"
            ),
            { caller_id: cid }
          );
        }
        return getPrompt("FLOW_MESSAGE_PHONE_COLLECT", "אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
      }
      return render(
        getPrompt(
          "FLOW_MESSAGE_DONE",
          "תודה. העברתי את ההודעה ל־{target} ויחזרו אליכם בהקדם. יום טוב."
        ),
        { target: data.message_target || "הצוות" }
      );
    }

    return getSetting("NO_DATA_MESSAGE", "אפשר לחזור על זה?");
  };

  // Consume caller utterance and update fields (but do not “invent”)
  const consumeCaller = (utterance) => {
    const text = String(utterance || "").trim();
    if (!text) return { guardReply: "", didUpdate: false };

    // DO_NOT_SAY guard always first
    const guard = doNotSayGuard(text);
    if (guard) return { guardReply: guard, didUpdate: false };

    const low = normalizeText(text);

    // set intent if missing
    if (!data.intent) {
      const i = detectIntent(text);
      if (i) data.intent = i;
      return { guardReply: "", didUpdate: Boolean(i) };
    }

    // phone digits detection (always)
    const digits = extractPhoneDigits(text);
    if (isValidPhone(digits)) {
      // do not overwrite callback_phone if already confirmed
      if (!data.callback_phone) {
        data.callback_phone = digits;
      } else if (!data.extra_phone && digits !== data.callback_phone) {
        data.extra_phone = digits;
      }
    }

    // name candidate (only if asked / missing)
    if (!data.full_name) {
      // basic filter: avoid yes/no
      if (!isYes(low) && !isNo(low) && low.length >= 2) {
        // strip digits words already handled
        const name = String(text).replace(/[0-9]/g, "").trim();
        if (name.length >= 2 && name.length <= 40) data.full_name = name;
      }
    }

    // Route-specific captures (only fill missing)
    if (data.intent === "sales") {
      if (!data.product_type) data.product_type = text;
      else if (!data.product_model) {
        // If user said "לא" to model question, allow empty and move on
        if (isNo(low)) data.product_model = "לא צוין";
        else data.product_model = text;
      } else if (!data.product_brand) {
        if (isNo(low)) data.product_brand = "לא צוין";
        else data.product_brand = text;
      }

      // Phone confirm logic handled by prompt step; here we interpret yes/no when in that step
      // We'll detect that by checking "callback_phone" missing and caller_id exists and question is confirm
      // If user says NO on confirm, we clear callback_phone (if it was set from caller_id) and request collect.
      if (!data.callback_phone && data.caller_id) {
        // not yet answered; handled in response step
      }
    }

    if (data.intent === "support") {
      if (!data.issue_desc) data.issue_desc = text;
      else if (!data.product_model) data.product_model = text;
      else if (!data.product_brand) {
        if (/לא ידוע/.test(low)) data.product_brand = "לא ידוע";
        else data.product_brand = text;
      }

      // importer offer decision: if importer exists and we asked, interpret yes/no
      if (data.importer_found && !data.importer_phone_given && data.importer_offer_done) {
        if (isYes(low)) data.importer_phone_given = true;
        if (isNo(low)) data.importer_phone_given = false;
      }
    }

    if (data.intent === "delivery") {
      if (!data.delivery_desc) data.delivery_desc = text;

      // carrier offer decision: if applicable and not yet set
      if (data.after_hours && data.delivery_same_day && data.carrier_offer_done && !data.carrier_info_given) {
        if (isYes(low)) data.carrier_info_given = true;
        if (isNo(low)) data.carrier_info_given = false;
      }
    }

    if (data.intent === "message") {
      if (!data.message_target) data.message_target = text;
      else if (!data.message_body) data.message_body = text;
    }

    return { guardReply: "", didUpdate: true };
  };

  // Build strict instructions to avoid hallucinations
  const buildTurnInstructions = (sayText) => {
    const baseRules = [
      MB_BASE_STYLE ? MB_BASE_STYLE : "סגנון: נטע. שמחה, אנרגטית, קצרה ועניינית.",
      "עברית בלבד. תמיד בלשון רבים וללא פנייה מגדרית.",
      "את מקריאה רק את הטקסט שהוגדר ב-SAY. בלי להוסיף הסברים, בלי שאלות נוספות מעבר למה שכתוב.",
      "אסור להמציא פרטים שלא נאמרו או שלא מופיעים בשיטס.",
      "אם יש מספר טלפון – הקריאי ספרה-ספרה בלבד.",
      OPENAI_VOICE_STYLE ? `הנחיית סטייל: ${OPENAI_VOICE_STYLE}` : "",
      OPENAI_SPEAKING_RATE ? `קצב דיבור (אם נתמך): ${OPENAI_SPEAKING_RATE}` : ""
    ].filter(Boolean);

    const say = `SAY:\n${String(sayText || "").trim()}`;
    return [...baseRules, say].join("\n\n");
  };

  const requestAssistantResponse = (reason = "") => {
    if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;

    // prevent double responses on same utterance
    if (awaitingResponse) {
      try {
        openaiWs.send(JSON.stringify({ type: "response.cancel" }));
      } catch (_) {}
      awaitingResponse = false;
    }

    const say = nextSayFromSheets();
    const instructions = buildTurnInstructions(say);

    awaitingResponse = true;
    lastRequestedCaller = lastCallerFinal;

    debug(`[${connTag}] response.create reason=${reason} stage=${getStageName()} intent=${data.intent}`);
    openaiWs.send(
      JSON.stringify({
        type: "response.create",
        response: { modalities: ["audio", "text"], instructions }
      })
    );
  };

  // --------------------------------------------------
  // OpenAI WS connect
  // --------------------------------------------------
  if (!OPENAI_API_KEY) {
    error("OPENAI_API_KEY missing — closing Twilio WS");
    try { twilioWs.close(); } catch (_) {}
    return;
  }

  openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "OpenAI-Beta": "realtime=v1"
    }
  });

  const pendingAudio = [];

  openaiWs.on("open", async () => {
    openaiReady = true;

    if (!SHEETS.loaded_at) {
      await loadSheets();
    }

    const masterPrompt = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט."
    );

    // session update
    const session = {
      modalities: ["audio", "text"],
      voice: OPENAI_VOICE,
      input_audio_format: "g711_ulaw",
      output_audio_format: "g711_ulaw",
      turn_detection: {
        type: "server_vad",
        threshold: MB_VAD_THRESHOLD,
        silence_duration_ms: MB_VAD_SILENCE_MS,
        prefix_padding_ms: MB_VAD_PREFIX_MS,
        suffix_padding_ms: MB_VAD_SUFFIX_MS,
        create_response: false
      },
      instructions: [
        masterPrompt,
        MB_BASE_STYLE || "",
        OPENAI_VOICE_STYLE ? `סטייל דיבור: ${OPENAI_VOICE_STYLE}` : "",
        OPENAI_SPEAKING_RATE ? `קצב דיבור (אם נתמך): ${OPENAI_SPEAKING_RATE}` : ""
      ].filter(Boolean).join("\n")
    };

    if (MB_ENABLE_TRANSCRIPTION) {
      session.input_audio_transcription = { model: MB_TRANSCRIPTION_MODEL };
    }

    openaiWs.send(JSON.stringify({ type: "session.update", session }));

    // Opening from SETTINGS only
    const opening = getSetting("OPENING_SCRIPT", "");
    if (opening) {
      awaitingResponse = true;
      openaiWs.send(
        JSON.stringify({
          type: "response.create",
          response: {
            modalities: ["audio", "text"],
            instructions: `תגידי עכשיו בדיוק את המשפט הבא מילה במילה, ללא תוספות וללא שאלות:\n${opening}`
          }
        })
      );
    }

    // flush audio
    while (pendingAudio.length > 0 && openaiWs.readyState === WebSocket.OPEN) {
      const audio = pendingAudio.shift();
      openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio }));
    }
  });

  openaiWs.on("error", (e) => {
    error(`[${connTag}] OpenAI WS error`, e?.message || e);
    try { twilioWs.close(); } catch (_) {}
  });

  openaiWs.on("close", () => {
    debug(`[${connTag}] OpenAI closed`);
    try { twilioWs.close(); } catch (_) {}
  });

  // --------------------------------------------------
  // OpenAI inbound
  // --------------------------------------------------
  openaiWs.on("message", async (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      error(`[${connTag}] OpenAI JSON parse failed`, e?.message || e);
      return;
    }

    if (MB_LOG_RAW_OPENAI) {
      always(`[RAW_OPENAI][${connTag}]`, JSON.stringify({ type: msg.type, event_id: msg.event_id }));
    }

    // bot transcript final
    if (msg.type === "response.audio_transcript.done") {
      const t = String(msg.transcript || "").trim();
      if (t) {
        lastBotFinal = t;
        pushTurn("bot", t);
        always(`[BOT][${connTag}]`, t);
      }
      return;
    }

    // caller transcript final (robust done/completed)
    {
      const type = String(msg.type || "");
      const doneLike = type.includes("done") || type.includes("completed");
      const isInputTranscript =
        type.includes("input_audio_transcription") ||
        type.includes("input_audio_transcript") ||
        type.includes("conversation.item.input_audio_transcription");
      const possible =
        msg.transcript ||
        msg.text ||
        msg?.item?.content?.[0]?.transcript ||
        msg?.item?.content?.[0]?.text ||
        "";

      if (doneLike && isInputTranscript && possible) {
        if (isFlushingBufferedAudio) return;

        const utterance = String(possible).trim();
        if (!utterance) return;

        lastCallerFinal = utterance;
        pushTurn("caller", utterance);
        always(`[CALLER][${connTag}]`, utterance);

        // consume updates
        const { guardReply } = consumeCaller(utterance);

        // If in a YES/NO confirm stage, interpret properly:
        // - For phone confirm prompts: if yes -> set callback_phone=caller_id, else go collect
        // We detect by: callback_phone missing AND caller_id exists AND the prompt we are about to say is *PHONE_CONFIRM*
        const stage = getStageName();
        if (stage.endsWith("_phone") && !data.callback_phone) {
          const cid = data.caller_id;
          if (isYes(utterance) && cid) {
            data.callback_phone = cid;
            data.phone_confirmed = true;
          } else if (isNo(utterance) || !cid) {
            data.phone_confirmed = false;
            // move to collect: user will say digits next
          }
        }

        // Importer offer handling: if we asked offer, then if yes -> next response should say number (from sheet prompt)
        if (data.intent === "support" && data.importer_found && data.importer_offer_done && !data._importerSaidNumber) {
          if (isYes(utterance)) {
            data._importerSaidNumber = true;
            data.importer_phone_given = true;

            const spacedNum = spacedPhone(extractPhoneDigits(data.importer_found.phone));
            const sayImporter = render(
              getPrompt(
                "FLOW_SUPPORT_IMPORTER_SAY",
                "המספר הישיר ליבואן הוא: {number}."
              ),
              { number: spacedNum, brand: data.importer_found.brand }
            );
            awaitingResponse = false;
            requestAssistantResponse("importer_number");
            // Override the nextSayFromSheets briefly by injecting a one-off say:
            // We do it by temporarily stashing and reading in nextSayFromSheets? Not allowed.
            // Instead: send direct response now:
            awaitingResponse = true;
            openaiWs.send(
              JSON.stringify({
                type: "response.create",
                response: { modalities: ["audio", "text"], instructions: buildTurnInstructions(sayImporter) }
              })
            );
            return;
          }
        }

        // Delivery carriers offer handling: if yes -> say carriers list once, then continue
        if (data.intent === "delivery" && data.after_hours && data.delivery_same_day && data.carrier_offer_done && !data._carriersSaid) {
          if (isYes(utterance)) {
            data._carriersSaid = true;
            data.carrier_info_given = true;

            const carriers = buildCarrierList();
            const sayCarriers = render(
              getPrompt(
                "FLOW_DELIVERY_CARRIER_SAY",
                "אלו מספרי המובילים: {carriers}"
              ),
              { carriers: carriers.join(", ") }
            );

            awaitingResponse = true;
            openaiWs.send(
              JSON.stringify({
                type: "response.create",
                response: { modalities: ["audio", "text"], instructions: buildTurnInstructions(sayCarriers) }
              })
            );
            return;
          }
          if (isNo(utterance)) {
            data._carriersSaid = true;
            data.carrier_info_given = false;
          }
        }

        // DO_NOT_SAY reply takes precedence, but does not advance
        if (guardReply) {
          awaitingResponse = true;
          openaiWs.send(
            JSON.stringify({
              type: "response.create",
              response: { modalities: ["audio", "text"], instructions: buildTurnInstructions(guardReply) }
            })
          );
          return;
        }

        // Dedupe: do not respond twice to same utterance
        if (lastRequestedCaller && lastCallerFinal === lastRequestedCaller && awaitingResponse) return;

        // Decide if we should respond now:
        // - Always respond for meaningful input (avoid tiny noise)
        const norm = normalizeText(utterance);
        const wc = norm.split(/\s+/).filter(Boolean).length;
        const meaningful = wc >= 2 || isYes(norm) || isNo(norm) || isValidPhone(extractPhoneDigits(norm));

        if (meaningful) {
          // cancel overlap if needed
          if (awaitingResponse) {
            try { openaiWs.send(JSON.stringify({ type: "response.cancel" })); } catch (_) {}
            awaitingResponse = false;
          }
          requestAssistantResponse("caller_final");
        }

        return;
      }
    }

    // audio delta -> twilio
    if (msg.type === "response.audio.delta") {
      if (!streamSid) return;
      twilioWs.send(
        JSON.stringify({
          event: "media",
          streamSid,
          media: { payload: msg.delta || "" }
        })
      );
      return;
    }

    // response done: release audio + possibly finalize
    if (msg.type === "response.done") {
      awaitingResponse = false;

      // flush buffered audio that arrived while assistant spoke
      if (pausedAudioBuffer.length > 0) {
        isFlushingBufferedAudio = true;
        while (pausedAudioBuffer.length > 0) {
          const a = pausedAudioBuffer.shift();
          try { openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: a })); } catch (_) {}
        }
        setTimeout(() => (isFlushingBufferedAudio = false), 80);
      }

      // If complete -> send final webhook (only once)
      if (!data.completed && requiredComplete()) {
        data.completed = true;
        endedAt = endedAt || nowIso();

        const payload = {
          at: endedAt,
          started_at: startedAt,
          ended_at: endedAt,
          callSid,
          streamSid,
          caller,
          caller_id: data.caller_id,
          called,
          intent: data.intent,
          stage: getStageName(),
          full_name: data.full_name,
          // sales
          product_type: data.product_type,
          product_model: data.product_model,
          product_brand: data.product_brand,
          // support
          issue_desc: data.issue_desc,
          // delivery
          delivery_desc: data.delivery_desc,
          carrier_info_given: Boolean(data.carrier_info_given),
          after_hours: Boolean(data.after_hours),
          // message
          message_target: data.message_target,
          message_body: data.message_body,
          // phones
          callback_phone: data.callback_phone,
          extra_phone: data.extra_phone,
          recording_url_public: publicRecordingUrl(callSid),
          transcript
        };

        const event = getEventName();
        await sendWebhookEvent(event, payload);

        try { openaiWs.close(); } catch (_) {}
        try { twilioWs.close(); } catch (_) {}
      }

      return;
    }

    // OpenAI error event
    if (msg.type === "error") {
      error(`[${connTag}] OpenAI error`, msg?.error?.message || msg);
      return;
    }
  });

  // --------------------------------------------------
  // Twilio inbound
  // --------------------------------------------------
  twilioWs.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      error(`[${connTag}] Twilio JSON parse failed`, e?.message || e);
      return;
    }

    if (msg.event === "start" && msg.start?.streamSid) {
      streamSid = msg.start.streamSid;
      callSid = msg.start.callSid || callSid;
      startedAt = startedAt || nowIso();

      const params = msg.start?.customParameters || {};
      caller = params.caller || params.Caller || caller || "";
      called = params.called || params.Called || called || "";

      data.caller_id = ensureCallerDigits(caller);

      always(`[TWILIO_START][${connTag}]`, JSON.stringify({ streamSid, callSid, caller, called }));

      // call_started webhook (optional)
      if (!MB_FINAL_WEBHOOK_ONLY) {
        sendWebhookEvent("call_started", {
          at: nowIso(),
          callSid,
          streamSid,
          caller,
          caller_id: data.caller_id,
          called,
          intent: data.intent || "",
          stage: getStageName(),
          recording_url_public: publicRecordingUrl(callSid)
        });
      }
      return;
    }

    if (msg.event === "media" && msg.media?.payload) {
      const payload = msg.media.payload;

      if (!openaiReady || !openaiWs || openaiWs.readyState !== WebSocket.OPEN) {
        pendingAudio.push(payload);
        if (pendingAudio.length > 500) pendingAudio.splice(0, pendingAudio.length - 500);
        return;
      }

      // while assistant speaking, buffer
      if (awaitingResponse) {
        pausedAudioBuffer.push(payload);
        if (pausedAudioBuffer.length > 500) pausedAudioBuffer.splice(0, pausedAudioBuffer.length - 500);
        return;
      }

      try {
        openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: payload }));
      } catch (_) {}
      return;
    }

    if (msg.event === "stop") {
      endedAt = endedAt || nowIso();
      always(`[TWILIO_STOP][${connTag}]`, "stream stopped");
      return;
    }
  });

  twilioWs.on("close", async () => {
    endedAt = endedAt || nowIso();
    always(`[TWILIO_CLOSE][${connTag}]`, "socket closed");

    // If not completed -> abandoned
    if (!data.completed) {
      await sendWebhookEvent("call_abandoned", {
        at: endedAt,
        callSid,
        streamSid,
        caller,
        caller_id: data.caller_id,
        stage: getStageName(),
        intent: data.intent || "",
        caller_last_utterance: lastCallerFinal,
        bot_last_utterance: lastBotFinal,
        transcript,
        recording_url_public: publicRecordingUrl(callSid)
      });
    }

    try { if (openaiWs) openaiWs.close(); } catch (_) {}
  });

  twilioWs.on("error", (e) => {
    error(`[${connTag}] Twilio WS error`, e?.message || e);
    try { if (openaiWs) openaiWs.close(); } catch (_) {}
  });
});

// --------------------------------------------------
// Start
// --------------------------------------------------
server.listen(PORT, () => {
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
  always("BOOT", {
    at: nowIso(),
    port: PORT,
    MB_DEBUG,
    OPENAI_REALTIME_MODEL,
    OPENAI_VOICE,
    has_OPENAI_API_KEY: Boolean(OPENAI_API_KEY),
    has_GSHEET_ID: Boolean(GSHEET_ID),
    has_GOOGLE_SERVICE_ACCOUNT_JSON_B64: Boolean(GOOGLE_SERVICE_ACCOUNT_JSON_B64),
    PUBLIC_BASE_URL,
    TIME_ZONE,
    MB_ENABLE_TRANSCRIPTION,
    MB_TRANSCRIPTION_MODEL,
    MB_VAD_THRESHOLD,
    MB_VAD_SILENCE_MS
  });
});
