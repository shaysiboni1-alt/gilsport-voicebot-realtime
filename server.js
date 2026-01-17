/*
 gilsport_voicebot.js

 Custom voicebot for GilSport – realtime voice assistant without a rigid state machine.
 This implementation is inspired by the existing server.js file in the
 gilsport‑voicebot‑realtime repository but has been reorganised to
 reduce reliance on an explicit state machine while still following
 the required conversation flows. The bot reads prompts and settings
 from a Google Sheet (designated by GSHEET_ID) and uses OpenAI
 Realtime for speech recognition and synthesis. All critical
 behaviours (tone, voice, speaking rate, VAD settings, etc.) are
 controlled via environment variables so they can be adjusted in
 Render without code changes.
*/

require("dotenv").config();

const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { google } = require("googleapis");
const crypto = require("crypto");

// Helper functions to parse environment variables.
const envNum = (k, d) => {
  const v = Number(process.env[k]);
  return Number.isFinite(v) ? v : d;
};
const envBool = (k, d = false) =>
  ["1", "true", "yes", "on"].includes(String(process.env[k] || "").toLowerCase()) || d;

// Port for HTTP server. Defaults to 10000 if not specified.
const PORT = envNum("PORT", 10000);

// OpenAI credentials and realtime model configuration.
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

// Voice configuration. Allowed voices are defined to avoid arbitrary
// inputs. If an unsupported voice is provided, alloy is used.
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
  const raw = String(v || "").trim();
  const lower = raw.toLowerCase();
  return ALLOWED_VOICES.has(lower) ? lower : "alloy";
}
const OPENAI_VOICE = normalizeVoice(process.env.OPENAI_VOICE || "alloy");

// Voice style (e.g. "warm_professional"). Will be passed directly to the
// OpenAI API if provided. If empty, no style hint is sent.
const OPENAI_VOICE_STYLE = process.env.OPENAI_VOICE_STYLE || "";
// Speaking rate (1.0 = normal speed). Must be positive.
const OPENAI_SPEAKING_RATE = (() => {
  const rate = parseFloat(process.env.OPENAI_SPEAKING_RATE);
  return Number.isFinite(rate) && rate > 0 ? rate : 1.0;
})();

// Base style override for responses. Operators can specify
// MB_BASE_STYLE in the environment to control the overall tone.
// The default provided here encourages a cheerful and energetic tone.
const MB_BASE_STYLE =
  process.env.MB_BASE_STYLE ||
  "סגנון: נטע. תשובות קצרות, אנרגטיות ושמחות, לדבר בקול חיובי ונעים.";

// Google Sheets configuration. The sheet ID points to the
// VoiceBot Config (Client Controlled) spreadsheet. The service
// account JSON must be provided as a base64‑encoded string in
// GOOGLE_SERVICE_ACCOUNT_JSON_B64.
const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 =
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

// Webhook configuration. The bot only sends a webhook when all
// required fields have been collected. MB_WEBHOOK_URL is the
// endpoint, MB_FINAL_WEBHOOK_ONLY controls whether intermediate
// events are suppressed.
const MB_WEBHOOK_URL = process.env.MB_WEBHOOK_URL || "";
const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

// Debugging and logging flags.
const MB_DEBUG = envBool("MB_DEBUG", false);
const MB_LOG_TRANSCRIPTS = envBool("MB_LOG_TRANSCRIPTS", true);
const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_LOG_RAW_OPENAI = envBool("MB_LOG_RAW_OPENAI", false);

// Voice activity detection (VAD) configuration. These values
// determine how sensitive the bot is to background noise. Raising
// the threshold makes the bot less sensitive to small noises.
const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.75);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNum("MB_VAD_SUFFIX_MS", 150);

// Idle and hangup timers (ms).
const MB_IDLE_WARNING_MS = envNum("MB_IDLE_WARNING_MS", 40000);
const MB_IDLE_HANGUP_MS = envNum("MB_IDLE_HANGUP_MS", 90000);
const MB_MAX_CALL_MS = envNum("MB_MAX_CALL_MS", 5 * 60 * 1000);

// Twilio credentials for fetching recordings. If not set, the
// recording proxy endpoint will be disabled.
const TWILIO_ACCOUNT_SID = process.env.TWILIO_ACCOUNT_SID || "";
const TWILIO_AUTH_TOKEN = process.env.TWILIO_AUTH_TOKEN || "";

// Public URL of this service. Used to build public recording URLs.
const PUBLIC_BASE_URL = process.env.PUBLIC_BASE_URL || "";

// Time zone for after‑hours detection (defaults to Asia/Jerusalem).
const TIME_ZONE = process.env.TIME_ZONE || "Asia/Jerusalem";

// In‑memory store for sheet data. Populated by loadSheets().
let SHEETS = {
  loaded_at: null,
  prompts: {}, // prompt_id -> content_he
  settings: {}, // key -> value
  kbFacts: [],
  doNotSay: [],
  suppliersImporters: [],
  deliveryContacts: []
};

// Convenience functions for sheet lookups.
const getPrompt = (id, fallback = "") =>
  String(SHEETS.prompts[id] || fallback).trim();
const getSetting = (key, fallback = "") =>
  String(SHEETS.settings[key] || fallback).trim();

// Load prompts, settings, and other tables from the Google Sheet.
async function loadSheets() {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) return;
  try {
    const json = JSON.parse(
      Buffer.from(
        GOOGLE_SERVICE_ACCOUNT_JSON_B64,
        "base64"
      ).toString("utf8")
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
    const valueRanges = res.data.valueRanges || [];
    const toObj = (rows) => {
      const out = {};
      if (!rows.length) return out;
      const headers = rows.shift().map((h) => String(h || "").trim());
      rows.forEach((r) => {
        const obj = {};
        headers.forEach((h, i) => (obj[h] = r[i] || ""));
        if (obj.key && obj.value)
          out[String(obj.key).trim()] = String(obj.value);
      });
      return out;
    };
    // Convert an array of rows into an array of objects using the first row as headers.
    const toArr = (rows) => {
      const copy = rows.map((r) => r.slice());
      if (!copy.length) return [];
      const headers = copy
        .shift()
        .map((h) => String(h || "").trim());
      return copy
        .map((r) => {
          const obj = {};
          headers.forEach((h, i) => (obj[h] = r[i] || ""));
          return obj;
        })
        .filter((o) =>
          Object.values(o).some((v) => String(v || "").trim())
        );
    };
    const promptsRange = valueRanges.find((vr) =>
      vr.range.startsWith("PROMPTS!")
    );
    const settingsRange = valueRanges.find((vr) =>
      vr.range.startsWith("SETTINGS!")
    );
    const kbFactsRange = valueRanges.find((vr) =>
      vr.range.startsWith("KB_FACTS!")
    );
    const doNotSayRange = valueRanges.find((vr) =>
      vr.range.startsWith("DO_NOT_SAY!")
    );
    const suppliersRange = valueRanges.find((vr) =>
      vr.range.startsWith("SUPPLIERS_IMPORTERS!")
    );
    const deliveryRange = valueRanges.find((vr) =>
      vr.range.startsWith("DELIVERY_CONTACTS!")
    );
    // Build prompts dictionary
    const prompts = {};
    if (
      promptsRange &&
      promptsRange.values &&
      promptsRange.values.length
    ) {
      const headers = promptsRange.values[0];
      const idIdx = headers.indexOf("prompt_id");
      const contentIdx = headers.indexOf("content_he");
      promptsRange.values.slice(1).forEach((row) => {
        const pid = (row[idIdx] || "").trim();
        const text = (row[contentIdx] || "").trim();
        if (pid && text) prompts[pid] = text;
      });
    }
    // Build settings map
    const settings = toObj(
      (settingsRange?.values || []).map((r) => r.slice())
    );
    // Build other tables
    const rowsToObjects = (range) => {
      const vals = (range?.values || []).map((r) => r.slice());
      if (!vals.length) return [];
      const headers = vals.shift().map((h) =>
        String(h || "").trim()
      );
      return vals.map((r) => {
        const o = {};
        headers.forEach((h, i) => (o[h] = r[i] || ""));
        return o;
      });
    };
    SHEETS = {
      loaded_at: new Date().toISOString(),
      prompts,
      settings,
      kbFacts: rowsToObjects(kbFactsRange),
      doNotSay: rowsToObjects(doNotSayRange),
      suppliersImporters: rowsToObjects(suppliersRange),
      deliveryContacts: rowsToObjects(deliveryRange)
    };
    console.log(
      `[SHEETS] Loaded prompts=${Object.keys(prompts).length}, settings=${Object.keys(
        settings
      ).length}`
    );
  } catch (e) {
    console.error("[SHEETS] Failed to load", e.message);
  }
}

// DO_NOT_SAY text builder. Concatenate up to 20 forbidden topics.
function buildDoNotSayText() {
  const rows = SHEETS.doNotSay || [];
  return rows
    .slice(0, 20)
    .map((r) => {
      const parts = [];
      const topic = String(r.forbidden_topic || "").trim();
      const triggers = String(r.trigger_examples || "").trim();
      const response = String(r.safe_response_he || "").trim();
      if (topic) parts.push(`נושא: ${topic}`);
      if (triggers) parts.push(`טריגרים: ${triggers}`);
      if (response) parts.push(`תגובה בטוחה: ${response}`);
      return parts.join(" | ");
    })
    .filter(Boolean)
    .join("\n");
}

// Helpers for phone number processing.
function normalizePhoneDigits(raw) {
  let text = String(raw || "");
  const wordToDigit = {
    "אפס": "0",
    "אחת": "1",
    "אחד": "1",
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
  for (const [word, digit] of Object.entries(wordToDigit)) {
    text = text.replace(new RegExp(`\\b${word}\\b`, "g"), digit);
  }
  let digits = text.replace(/\D+/g, "");
  if (digits.startsWith("972") && digits.length > 3)
    digits = "0" + digits.slice(3);
  if (digits.startsWith("0") && digits.length > 10)
    digits = digits.slice(0, 10);
  return digits;
}
function isValidPhoneDigits(d) {
  const digits = String(d || "").replace(/\D+/g, "");
  return digits.length === 10 && digits.startsWith("0");
}
function formatSpacedDigits(d) {
  return String(d || "").split("").join(" ");
}

// Extract brand and model names from a free‑form description.
function extractBrandModel(text) {
  const t = String(text || "");
  const brandMatch = t.match(/מותג\s+([^,.\n\r]+)/);
  const modelMatch = t.match(/דגם\s+([^,.\n\r]+)/);
  return {
    brand: brandMatch ? brandMatch[1].trim() : "",
    model: modelMatch ? modelMatch[1].trim() : ""
  };
}

// Identify route by keywords. Returns one of 'sales', 'support', 'delivery', 'message', or ''.
function extractRoute(text) {
  const low = String(text || "").toLowerCase();
  if (
    /(אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול)/.test(low)
  )
    return "support";
  if (/(משלוח|אספקה|אספקת|שליח|הזמנה|הגיע|לא הגיע|מוביל)/.test(low))
    return "delivery";
  if (
    /(מחיר|קנ[יא]|רכישה|מוצר|דגם|מידה|צבע|מלאי|מבצע)/.test(low)
  )
    return "sales";
  if (
    /(הודעה|מנהל|עובד|לחזור אלי|השארת הודעה)/.test(low)
  )
    return "message";
  return "";
}

// Determine if current time is after business hours based on the sheet settings.
function isAfterHours() {
  const hoursStr =
    getSetting("BUSINESS_HOURS", "") ||
    getSetting("HOURS", "") ||
    getSetting("WORKING_HOURS", "") ||
    "";
  const m = String(hoursStr || "").match(
    /(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})/
  );
  if (!m) return false;
  const aH = Number(m[1]);
  const aM = Number(m[2]);
  const bH = Number(m[3]);
  const bM = Number(m[4]);
  const start = aH * 60 + aM;
  const end = bH * 60 + bM;
  const now = new Date();
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: TIME_ZONE,
    hour12: false,
    hour: "2-digit",
    minute: "2-digit"
  }).formatToParts(now);
  const hh = Number(
    parts.find((p) => p.type === "hour")?.value || 0
  );
  const mm = Number(
    parts.find((p) => p.type === "minute")?.value || 0
  );
  const cur = hh * 60 + mm;
  return cur < start || cur > end;
}

// Build dynamic instructions for a given SAY text and optional extra notes.
function buildInstructions(sayText, extra = []) {
  const doNotSayText = buildDoNotSayText();
  const rules = [
    MB_BASE_STYLE,
    "עברית בלבד. תמיד בלשון רבים וללא פנייה מגדרית.",
    "הבוט רק אוסף מידע – אין להמציא או ליזום נושאים חדשים.",
    "בכל שלב יש לשאול שאלה אחת בלבד ולהמתין לתשובה מלאה.",
    doNotSayText ? `DO_NOT_SAY (כללים מחייבים):\n${doNotSayText}` : ""
  ].filter(Boolean);
  if (!sayText) return rules.join("\n\n").trim();
  return [...rules, ...extra.filter(Boolean), `SAY:\n${sayText}`]
    .filter(Boolean)
    .join("\n\n");
}

// Twilio recording helper. Builds a public URL for a call recording if
// PUBLIC_BASE_URL is configured.
function makeRecordingPublicUrl(callSid) {
  if (!PUBLIC_BASE_URL) return "";
  const base = String(PUBLIC_BASE_URL).replace(/\/$/, "");
  return callSid ? `${base}/recording/${callSid}` : "";
}

// Send a webhook event with JSON payload. Respects MB_FINAL_WEBHOOK_ONLY.
async function sendWebhookEvent(event, payload, opts = {}) {
  if (!MB_WEBHOOK_URL) return false;
  try {
    const callSid =
      payload && payload.callSid ? String(payload.callSid) : "";
    // wait for recording if needed
    if (
      callSid &&
      (opts.wait_for_recording || opts.waitForRecording)
    ) {
      await waitForRecording(
        callSid,
        envNum("MB_RECORDING_WAIT_MS", 8000)
      );
    }
    const recording_url_public =
      payload.recording_url_public ||
      makeRecordingPublicUrl(callSid);
    const body = JSON.stringify({
      event,
      ...payload,
      recording_url_public
    });
    const resp = await fetch(MB_WEBHOOK_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body
    });
    if (!resp.ok) {
      console.error("[WEBHOOK] non-200", event, resp.status);
    }
    return true;
  } catch (e) {
    console.error("[WEBHOOK] failed", event, e.message);
    return false;
  }
}

// Wait for Twilio recording to become available.
async function waitForRecording(callSid, waitMs) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return false;
  const deadline = Date.now() + Math.max(0, Number(waitMs) || 0);
  if (await twilioHasRecording(callSid)) return true;
  while (Date.now() < deadline) {
    await new Promise((r) => setTimeout(r, 1000));
    if (await twilioHasRecording(callSid)) return true;
  }
  return false;
}
async function twilioHasRecording(callSid) {
  if (!TWILIO_ACCOUNT_SID || !TWILIO_AUTH_TOKEN) return false;
  if (!callSid) return false;
  try {
    const listUrl = `https://api.twilio.com/2010-04-01/Accounts/${TWILIO_ACCOUNT_SID}/Recordings.json?CallSid=${encodeURIComponent(
      callSid
    )}&PageSize=1`;
    const auth = Buffer.from(
      `${TWILIO_ACCOUNT_SID}:${TWILIO_AUTH_TOKEN}`
    ).toString("base64");
    const resp = await fetch(listUrl, {
      headers: { Authorization: `Basic ${auth}` }
    });
    if (!resp.ok) return false;
    const data = await resp.json();
    return (
      Array.isArray(data.recordings) && data.recordings.length > 0
    );
  } catch (_) {
    return false;
  }
}

// Express app and HTTP server
const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    sheets_loaded_at: SHEETS.loaded_at,
    prompts: Object.keys(SHEETS.prompts).length,
    settings: Object.keys(SHEETS.settings).length,
    kbFacts: (SHEETS.kbFacts || []).length,
    doNotSay: (SHEETS.doNotSay || []).length,
    suppliersImporters: (SHEETS.suppliersImporters || []).length,
    deliveryContacts: (SHEETS.deliveryContacts || []).length
  });
});

app.post("/sheets/reload", async (req, res) => {
  await loadSheets();
  res.json({ ok: true, reloaded: true, at: SHEETS.loaded_at });
});

// Twilio Voice → Media Stream entry point. Returns TwiML to start a WS session.
app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  const wsUrl = `wss://${host}/twilio-media-stream`;
  res
    .type("text/xml")
    .send(
      `\n<Response>\n  <Connect>\n    <Stream url="${wsUrl}">\n      <Parameter name="caller" value="${
        req.body.From || ""
      }" />\n      <Parameter name="called" value="${
        req.body.To || ""
      }" />\n    </Stream>\n  </Connect>\n</Response>`.trim()
    );
});

const server = http.createServer(app);

// WebSocket server for media streams.
const wss = new WebSocket.Server({
  server,
  path: "/twilio-media-stream"
});

// Main connection handler. Each new WS connection corresponds to a phone call.
wss.on("connection", (twilioWs, req) => {
  // Per‑call state variables
  let openaiWs = null;
  let callSid = null;
  let streamSid = null;
  let caller = "";
  let called = "";
  let callStart = new Date().toISOString();
  let callEnd = null;
  let afterHours = false;
  let route = "";
  let awaitingResponse = false;
  let pendingResponse = false;
  let lastCallerFinal = "";
  let lastBotFinal = "";
  let recognizedPhones = [];
  // Conversation data collected. Fields correspond to required info per route.
  const convo = {
    product_desc: "",
    product_brand: "",
    product_model: "",
    issue_desc: "",
    delivery_desc: "",
    message_target: "",
    message_body: "",
    full_name: "",
    callback_phone: ""
  };
  // Track whether we've given the user the importer phone number.
  let importerOffered = false;
  // Stage indicates which piece of information we are currently collecting. It is not
  // used as a rigid state machine but helps determine the next question.
  let stage = "routing";

  // Helper to log messages if MB_DEBUG is on.
  function debug(...args) {
    if (MB_DEBUG) console.log("[DEBUG]", ...args);
  }

  // Send instructions to OpenAI.
  function safeOpenAISend(obj) {
    try {
      if (openaiWs && openaiWs.readyState === WebSocket.OPEN) {
        openaiWs.send(JSON.stringify(obj));
        return true;
      }
    } catch (e) {
      console.error("[OpenAI] send failed", e.message);
    }
    return false;
  }

  // Send audio to Twilio.
  function safeTwilioSend(obj) {
    try {
      if (twilioWs && twilioWs.readyState === WebSocket.OPEN) {
        twilioWs.send(JSON.stringify(obj));
        return true;
      }
    } catch (e) {
      console.error("[Twilio] send failed", e.message);
    }
    return false;
  }

  // Determine the next question based on current route and collected data.
  function determineNextQuestion() {
    // If route is not set, ask routing question
    if (!route) {
      stage = "routing";
      return (
        getPrompt("FLOW_ROUTING") ||
        "איך אפשר לעזור לך היום?"
      );
    }
    if (route === "sales") {
      // Ask for product description
      if (!convo.product_desc) {
        stage = "sales_product";
        return (
          getPrompt("FLOW_SALES_PRODUCT") ||
          "איזה מוצר מעניין אותך?"
        );
      }
      // Ask for full name
      if (!convo.full_name) {
        stage = "sales_name";
        return (
          getPrompt("FLOW_SALES_NAME") ||
          "מה שמך המלא בבקשה?"
        );
      }
      // Ask brand/model details if not provided
      if (!convo.product_brand) {
        stage = "sales_brand";
        return "האם יש מותג מסוים? אם כן – אנא ציין אותו";
      }
      if (!convo.product_model) {
        stage = "sales_model";
        return "האם יש דגם מסוים? אם כן – אנא ציין את הדגם";
      }
      // Ask phone confirmation
      const callerDigits = normalizePhoneDigits(caller);
      if (callerDigits && !convo.callback_phone) {
        stage = "sales_phone_confirm";
        // Insert price claim sentence and coupon code before the question
        const priceClaim = getSetting("PRICE_CLAIM_SENTENCE", "");
        const salesCoupon = getSetting("SALES_COUPON_CODE", "");
        let prefix = "";
        if (priceClaim) prefix += `${priceClaim}\n`;
        if (salesCoupon)
          prefix += `קוד הקופון שלנו הוא ${salesCoupon}\n`;
        return `${prefix}${getPrompt(
          "FLOW_SALES_PHONE_CONFIRM"
        )}`;
      }
      // If caller phone not provided or declined, ask new phone
      if (!convo.callback_phone) {
        stage = "sales_phone_new";
        return (
          getPrompt("FLOW_SALES_PHONE_NEW") ||
          "מה מספר הטלפון שלך?"
        );
      }
      // Confirm new phone
      if (
        stage === "sales_phone_new" &&
        !convo.phone_confirmed
      ) {
        stage = "sales_phone_confirm_new";
        const spaced = formatSpacedDigits(
          convo.callback_phone
        );
        const tpl =
          getPrompt("FLOW_SALES_PHONE_CONFIRM_NEW") ||
          "${number}";
        return tpl.replace("{number}", spaced);
      }
      // Done
      stage = "sales_done";
      return (
        getPrompt("FLOW_SALES_DONE") ||
        "תודה, נציג יחזור אליך בהקדם."
      );
    }
    if (route === "support") {
      if (!convo.issue_desc) {
        stage = "support_issue";
        return (
          getPrompt("FLOW_SUPPORT_ISSUE") ||
          "ספר לי בבקשה מה התקלה"
        );
      }
      if (!convo.product_model) {
        stage = "support_model";
        return (
          getPrompt("FLOW_SUPPORT_PRODUCT") ||
          "מה הדגם של המוצר?"
        );
      }
      if (!convo.product_brand) {
        stage = "support_brand";
        return "האם אתה יודע מה המותג של המוצר?";
      }
      if (!convo.full_name) {
        stage = "support_name";
        return (
          getPrompt("FLOW_SUPPORT_NAME") ||
          "מה שמך המלא?"
        );
      }
      const callerDigits = normalizePhoneDigits(caller);
      if (callerDigits && !convo.callback_phone) {
        stage = "support_phone_confirm";
        return (
          getPrompt("FLOW_SUPPORT_PHONE_CONFIRM") ||
          "האם זה מספר הטלפון שלך?"
        );
      }
      if (!convo.callback_phone) {
        stage = "support_phone_new";
        return (
          getPrompt("FLOW_SUPPORT_PHONE_NEW") ||
          "מה מספר הטלפון שלך?"
        );
      }
      if (
        stage === "support_phone_new" &&
        !convo.phone_confirmed
      ) {
        stage = "support_phone_confirm_new";
        const spaced = formatSpacedDigits(
          convo.callback_phone
        );
        const tpl =
          getPrompt("FLOW_SUPPORT_PHONE_CONFIRM_NEW") ||
          "${number}";
        return tpl.replace("{number}", spaced);
      }
      stage = "support_done";
      return (
        getPrompt("FLOW_SUPPORT_DONE") ||
        "תודה, נציג יחזור אליך בקרוב."
      );
    }
    if (route === "delivery") {
      // special handling: if after hours and desc not yet given
      if (!convo.delivery_desc) {
        stage = "delivery_desc";
        return (
          getPrompt("FLOW_DELIVERY_DESC") ||
          "מהו נושא הבקשה למשלוח?"
        );
      }
      if (!convo.full_name) {
        stage = "delivery_name";
        return (
          getPrompt("FLOW_DELIVERY_NAME") ||
          "מה שמך המלא?"
        );
      }
      const callerDigits = normalizePhoneDigits(caller);
      if (callerDigits && !convo.callback_phone) {
        stage = "delivery_phone_confirm";
        return (
          getPrompt("FLOW_DELIVERY_PHONE_CONFIRM") ||
          "זה מספר הטלפון שלך?"
        );
      }
      if (!convo.callback_phone) {
        stage = "delivery_phone_new";
        return (
          getPrompt("FLOW_DELIVERY_PHONE_NEW") ||
          "מה מספר הטלפון שלך?"
        );
      }
      if (
        stage === "delivery_phone_new" &&
        !convo.phone_confirmed
      ) {
        stage = "delivery_phone_confirm_new";
        const spaced = formatSpacedDigits(
          convo.callback_phone
        );
        const tpl =
          getPrompt("FLOW_DELIVERY_PHONE_CONFIRM_NEW") ||
          "${number}";
        return tpl.replace("{number}", spaced);
      }
      stage = "delivery_done";
      return (
        getPrompt("FLOW_DELIVERY_DONE") ||
        "תודה, נציג יחזור אליך בקרוב."
      );
    }
    if (route === "message") {
      if (!convo.message_target) {
        stage = "message_target";
        return (
          getPrompt("FLOW_MESSAGE_TARGET") ||
          "למי ההודעה מיועדת?"
        );
      }
      if (!convo.message_body) {
        stage = "message_body";
        return (
          getPrompt("FLOW_MESSAGE_BODY") ||
          "מה תוכן ההודעה?"
        );
      }
      if (!convo.full_name) {
        stage = "message_name";
        return (
          getPrompt("FLOW_MESSAGE_NAME") ||
          "מה שמך המלא?"
        );
      }
      const callerDigits = normalizePhoneDigits(caller);
      if (callerDigits && !convo.callback_phone) {
        stage = "message_phone_confirm";
        return (
          getPrompt("FLOW_MESSAGE_PHONE_CONFIRM") ||
          "זה מספר הטלפון שלך?"
        );
      }
      if (!convo.callback_phone) {
        stage = "message_phone_new";
        return (
          getPrompt("FLOW_MESSAGE_PHONE_NEW") ||
          "מה מספר הטלפון שלך?"
        );
      }
      if (
        stage === "message_phone_new" &&
        !convo.phone_confirmed
      ) {
        stage = "message_phone_confirm_new";
        const spaced = formatSpacedDigits(
          convo.callback_phone
        );
        const tpl =
          getPrompt("FLOW_MESSAGE_PHONE_CONFIRM_NEW") ||
          "${number}";
        return tpl.replace("{number}", spaced);
      }
      stage = "message_done";
      return (
        getPrompt("FLOW_MESSAGE_DONE") ||
        "תודה, נציג יחזור אליך בקרוב."
      );
    }
    // fallback
    return (
      getPrompt("FLOW_FALLBACK") ||
      "אשמח לעזור בעוד משהו?"
    );
  }

  // Send next question to the assistant.
  function requestNext() {
    const sayText = determineNextQuestion();
    const instructions = buildInstructions(sayText);
    awaitingResponse = true;
    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions
      }
    });
  }

  // Process final utterance from caller.
  function handleCallerFinal(text) {
    const utterance = String(text || "").trim();
    if (!utterance) return;
    lastCallerFinal = utterance;
    // If route not set, try to detect route.
    if (!route) {
      const detected = extractRoute(utterance);
      route = detected || "message"; // default to message route
      afterHours = isAfterHours();
      return requestNext();
    }
    // Capture data based on stage
    if (route === "sales") {
      if (stage === "sales_product") {
        convo.product_desc = utterance;
        const { brand, model } = extractBrandModel(utterance);
        if (brand) convo.product_brand = brand;
        if (model) convo.product_model = model;
      } else if (stage === "sales_name") {
        // extract name ignoring numbers
        let t = utterance.replace(/\d/g, "").trim();
        if (t) convo.full_name = t;
      } else if (stage === "sales_brand") {
        convo.product_brand = utterance;
      } else if (stage === "sales_model") {
        convo.product_model = utterance;
      } else if (stage === "sales_phone_confirm") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          const digits = normalizePhoneDigits(caller);
          if (isValidPhoneDigits(digits)) {
            convo.callback_phone = digits;
            convo.phone_confirmed = true;
          }
        }
      } else if (stage === "sales_phone_new") {
        const digits = normalizePhoneDigits(utterance);
        if (isValidPhoneDigits(digits))
          convo.callback_phone = digits;
      } else if (stage === "sales_phone_confirm_new") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          convo.phone_confirmed = true;
        } else {
          convo.callback_phone = "";
        }
      }
    } else if (route === "support") {
      if (stage === "support_issue") {
        convo.issue_desc = utterance;
      } else if (stage === "support_model") {
        convo.product_model = utterance;
      } else if (stage === "support_brand") {
        convo.product_brand = utterance;
        // Check importer list for optional transfer
        if (!importerOffered) {
          const match = (
            SHEETS.suppliersImporters || []
          ).find(
            (row) =>
              String(row.brand_name || "").trim() ===
              convo.product_brand
          );
          if (match && match.phone) {
            importerOffered = true;
            const spaced = formatSpacedDigits(
              normalizePhoneDigits(match.phone)
            );
            const importerMsg =
              getPrompt("FLOW_SUPPORT_SUPPLIER_OPTIONAL") ||
              `ניתן לדבר עם היבואן של ${convo.product_brand} במספר ${spaced}. תרצה שאתן לך את המספר?`;
            const instructions = buildInstructions(
              importerMsg
            );
            awaitingResponse = true;
            safeOpenAISend({
              type: "response.create",
              response: {
                modalities: ["audio", "text"],
                instructions
              }
            });
            return;
          }
        }
      } else if (stage === "support_name") {
        let t = utterance.replace(/\d/g, "").trim();
        if (t) convo.full_name = t;
      } else if (stage === "support_phone_confirm") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          const digits = normalizePhoneDigits(caller);
          if (isValidPhoneDigits(digits)) {
            convo.callback_phone = digits;
            convo.phone_confirmed = true;
          }
        }
      } else if (stage === "support_phone_new") {
        const digits = normalizePhoneDigits(utterance);
        if (isValidPhoneDigits(digits))
          convo.callback_phone = digits;
      } else if (stage === "support_phone_confirm_new") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          convo.phone_confirmed = true;
        } else {
          convo.callback_phone = "";
        }
      }
    } else if (route === "delivery") {
      if (stage === "delivery_desc") {
        convo.delivery_desc = utterance;
      } else if (stage === "delivery_name") {
        let t = utterance.replace(/\d/g, "").trim();
        if (t) convo.full_name = t;
      } else if (stage === "delivery_phone_confirm") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          const digits = normalizePhoneDigits(caller);
          if (isValidPhoneDigits(digits)) {
            convo.callback_phone = digits;
            convo.phone_confirmed = true;
          }
        }
      } else if (stage === "delivery_phone_new") {
        const digits = normalizePhoneDigits(utterance);
        if (isValidPhoneDigits(digits))
          convo.callback_phone = digits;
      } else if (stage === "delivery_phone_confirm_new") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          convo.phone_confirmed = true;
        } else {
          convo.callback_phone = "";
        }
      }
    } else if (route === "message") {
      if (stage === "message_target") {
        convo.message_target = utterance;
      } else if (stage === "message_body") {
        convo.message_body = utterance;
      } else if (stage === "message_name") {
        let t = utterance.replace(/\d/g, "").trim();
        if (t) convo.full_name = t;
      } else if (stage === "message_phone_confirm") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          const digits = normalizePhoneDigits(caller);
          if (isValidPhoneDigits(digits)) {
            convo.callback_phone = digits;
            convo.phone_confirmed = true;
          }
        }
      } else if (stage === "message_phone_new") {
        const digits = normalizePhoneDigits(utterance);
        if (isValidPhoneDigits(digits))
          convo.callback_phone = digits;
      } else if (stage === "message_phone_confirm_new") {
        if (
          /(כן|נכון|מאשר|אישור|yes|ok|yep)/i.test(utterance)
        ) {
          convo.phone_confirmed = true;
        } else {
          convo.callback_phone = "";
        }
      }
    }
    // After capturing data, ask next question or complete.
    if (stage.endsWith("_done")) {
      finalizeCall();
    } else {
      requestNext();
    }
  }

  // Finalize the call: send webhook and send closing message.
  async function finalizeCall() {
    callEnd = new Date().toISOString();
    let event = "call_ended";
    let payload = {
      callSid,
      streamSid,
      caller,
      called,
      started_at: callStart,
      ended_at: callEnd,
      language: getSetting("DEFAULT_LANGUAGE", "he"),
      route,
      transcript: [],
      recording_url_public: makeRecordingPublicUrl(callSid)
    };
    // Build route‑specific payload
    if (route === "sales") {
      event = "מתעניין במכירות";
      payload = {
        ...payload,
        full_name: convo.full_name || "",
        product_type: convo.product_desc || "",
        product_brand: convo.product_brand || "",
        product_model: convo.product_model || "",
        callback_phone:
          convo.callback_phone ||
          normalizePhoneDigits(caller) ||
          "",
        summary: convo.product_desc
      };
    } else if (route === "support") {
      event = "שירות לקוחות תקלה";
      payload = {
        ...payload,
        full_name: convo.full_name || "",
        issue_desc: convo.issue_desc || "",
        product_brand: convo.product_brand || "",
        product_model: convo.product_model || "",
        callback_phone:
          convo.callback_phone ||
          normalizePhoneDigits(caller) ||
          "",
        summary: convo.issue_desc
      };
    } else if (route === "delivery") {
      event = "אספקה / משלוח";
      payload = {
        ...payload,
        full_name: convo.full_name || "",
        delivery_desc: convo.delivery_desc || "",
        callback_phone:
          convo.callback_phone ||
          normalizePhoneDigits(caller) ||
          "",
        summary: convo.delivery_desc,
        carriers_offered:
          afterHours &&
          SHEETS.deliveryContacts.length > 0
      };
    } else if (route === "message") {
      event = `הודעה עבור ${
        convo.message_target || ""
      }`;
      payload = {
        ...payload,
        target: convo.message_target || "",
        full_name: convo.full_name || "",
        message_body: convo.message_body || "",
        callback_phone:
          convo.callback_phone ||
          normalizePhoneDigits(caller) ||
          "",
        summary: convo.message_body
      };
    } else {
      event = "call_ended";
    }
    // Send webhook if final only or always for this implementation
    await sendWebhookEvent(event, payload, {
      wait_for_recording: true
    });
    // Send closing message to the caller
    const closingText = getSetting(
      "CLOSING_SCRIPT",
      "תודה שפניתם אלינו. שיהיה יום נעים!"
    );
    const instructions = buildInstructions(closingText);
    safeOpenAISend({
      type: "response.create",
      response: {
        modalities: ["audio", "text"],
        instructions
      }
    });
    awaitingResponse = true;
    // Hang up after closing message
    setTimeout(() => {
      try {
        if (openaiWs) openaiWs.close();
        if (twilioWs) twilioWs.close();
      } catch (_) {}
    }, 3000);
  }

  // Handle incoming messages from Twilio WebSocket
  twilioWs.on("message", async (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch (e) {
      console.error("[Twilio] invalid JSON", e.message);
      return;
    }
    if (msg.event === "start" && msg.start?.streamSid) {
      streamSid = msg.start.streamSid;
      callSid = msg.start.callSid || callSid;
      callStart = new Date().toISOString();
      caller = msg.start.customParameters?.caller || caller;
      called = msg.start.customParameters?.called || called;
      // lazy load sheets
      if (!SHEETS.loaded_at) await loadSheets();
      // connect to OpenAI
      openaiWs = new WebSocket(
        `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
        {
          headers: {
            Authorization: `Bearer ${OPENAI_API_KEY}`,
            "OpenAI-Beta": "realtime=v1"
          }
        }
      );
      openaiWs.on("open", () => {
        debug("OpenAI WS connected");
        // Start session
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
          instructions: getPrompt(
            "MASTER_PROMPT",
            "אתם עוזרת קולית בשם נטע עבור גיל ספורט."
          )
        };
        if (MB_ENABLE_TRANSCRIPTION)
          session.input_audio_transcription = {
            model:
              getSetting(
                "MB_TRANSCRIPTION_MODEL",
                "whisper-1"
              ) || "whisper-1"
          };
        if (OPENAI_VOICE_STYLE)
          session.voice_style = OPENAI_VOICE_STYLE;
        if (OPENAI_SPEAKING_RATE)
          session.speaking_rate = OPENAI_SPEAKING_RATE;
        safeOpenAISend({ type: "session.update", session });
        // Opening script
        const opening = getSetting(
          "OPENING_SCRIPT",
          "שלום, מדברת נטע מגיל ספורט."
        );
        const instr = buildInstructions(
          `תגידו עכשיו בדיוק את המשפט הבא מילה במילה ללא תוספות:\n${opening}`
        );
        awaitingResponse = true;
        safeOpenAISend({
          type: "response.create",
          response: {
            modalities: ["audio", "text"],
            instructions: instr
          }
        });
      });
      openaiWs.on("message", async (raw) => {
        let om;
        try {
          om = JSON.parse(raw.toString());
        } catch (e) {
          console.error("[OpenAI] parse error", e.message);
          return;
        }
        // Log transcripts if enabled
        if (
          MB_LOG_TRANSCRIPTS &&
          om.transcript &&
          om.type &&
          om.type.startsWith("response.audio_transcript")
        ) {
          console.log("[BOT_PART]", om.transcript.trim());
        }
        if (om.type === "response.audio.delta") {
          if (streamSid) {
            safeTwilioSend({
              event: "media",
              streamSid,
              media: { payload: om.delta }
            });
          }
          return;
        }
        if (om.type === "response.audio_transcript.done") {
          const t = String(om.transcript || "").trim();
          if (t) {
            lastBotFinal = t;
          }
          return;
        }
        // Caller final transcripts
        const doneLike = om.type && om.type.includes("done");
        const isInput =
          om.type &&
          (om.type.includes("input_audio_transcription") ||
            om.type.includes("input_audio_transcript") ||
            om.type.includes(
              "conversation.item.input_audio_transcription"
            ));
        const utterance =
          om.transcript ||
          om.text ||
          om.item?.content?.[0]?.transcript ||
          om.item?.content?.[0]?.text ||
          "";
        if (doneLike && isInput && utterance) {
          awaitingResponse = false;
          handleCallerFinal(utterance);
        }
        if (om.type === "response.done") {
          awaitingResponse = false;
          if (pendingResponse) {
            pendingResponse = false;
            requestNext();
          }
        }
        if (om.type === "error") {
          console.error("[OpenAI] error", om);
        }
      });
      openaiWs.on("close", () => {
        debug("OpenAI WS closed");
        try {
          twilioWs.close();
        } catch (_) {}
      });
      openaiWs.on("error", (e) => {
        console.error("[OpenAI] error", e.message);
      });
      return;
    }
    if (msg.event === "media" && msg.media?.payload) {
      // Forward audio to OpenAI
      if (
        openaiWs &&
        openaiWs.readyState === WebSocket.OPEN
      ) {
        if (awaitingResponse) {
          // Buffer audio while assistant speaking
          return;
        }
        safeOpenAISend({
          type: "input_audio_buffer.append",
          audio: msg.media.payload
        });
      }
      return;
    }
    if (msg.event === "stop") {
      callEnd = new Date().toISOString();
      // If call ends before completion, send abandoned webhook
      if (!stage.endsWith("_done")) {
        const payload = {
          callSid,
          streamSid,
          caller,
          called,
          started_at: callStart,
          ended_at: callEnd,
          language: getSetting("DEFAULT_LANGUAGE", "he"),
          route: route || "",
          stage,
          recording_url_public: makeRecordingPublicUrl(callSid)
        };
        await sendWebhookEvent(
          "call_abandoned",
          payload,
          { wait_for_recording: true }
        );
      }
      // Close sockets
      try {
        if (openaiWs) openaiWs.close();
        twilioWs.close();
      } catch (_) {}
      return;
    }
  });
  twilioWs.on("close", () => {
    debug("Twilio WS closed");
  });
  twilioWs.on("error", (e) => {
    console.error("[Twilio WS] error", e.message);
  });
});

server.listen(PORT, () => {
  console.log(
    `GilSport VoiceBot custom server running on port ${PORT}`
  );
  loadSheets();
});
