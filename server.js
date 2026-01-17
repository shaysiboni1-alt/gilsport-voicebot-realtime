// server.js
// GilSport VoiceBot – Sheet-Driven, No FSM, Realtime (Twilio + OpenAI)
// FIXED VERSION – PART 1 / 3
// Boot, ENV, Helpers, Sheets, Express, Base Runtime

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
// ENV – DO NOT TOUCH VOICE / STYLE LOGIC
// --------------------------------------------------
const PORT = envNum("PORT", 10000);

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";

const ALLOWED_VOICES = new Set([
  "alloy","ash","ballad","coral","echo","sage","shimmer","verse","marin","cedar"
]);
const normalizeVoice = (v) => {
  const x = String(v || "").toLowerCase().trim();
  return ALLOWED_VOICES.has(x) ? x : "alloy";
};
const OPENAI_VOICE = normalizeVoice(process.env.OPENAI_VOICE || "alloy");
const OPENAI_VOICE_STYLE = process.env.OPENAI_VOICE_STYLE || "";
const OPENAI_SPEAKING_RATE = (() => {
  const r = parseFloat(process.env.OPENAI_SPEAKING_RATE);
  return Number.isFinite(r) && r > 0 ? r : 1.0;
})();

const MB_BASE_STYLE = process.env.MB_BASE_STYLE || "";

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 =
  process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const MB_WEBHOOK_URL = process.env.MB_WEBHOOK_URL || "";
const MB_FINAL_WEBHOOK_ONLY = envBool("MB_FINAL_WEBHOOK_ONLY", true);

const MB_DEBUG = envBool("MB_DEBUG", false);

const MB_VAD_THRESHOLD = envNum("MB_VAD_THRESHOLD", 0.75);
const MB_VAD_SILENCE_MS = envNum("MB_VAD_SILENCE_MS", 900);
const MB_VAD_PREFIX_MS = envNum("MB_VAD_PREFIX_MS", 200);
const MB_VAD_SUFFIX_MS = envNum("MB_VAD_SUFFIX_MS", 150);

const MB_ENABLE_TRANSCRIPTION = envBool("MB_ENABLE_TRANSCRIPTION", true);
const MB_TRANSCRIPTION_MODEL = process.env.MB_TRANSCRIPTION_MODEL || "whisper-1";

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

// --------------------------------------------------
// Sheets – Single Source of Truth
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {},
  settings: {},
  kbFacts: [],
  doNotSay: [],
  suppliersImporters: [],
  deliveryContacts: []
};

const rowsToObjects = (rows) => {
  const out = [];
  const headers = (rows.shift() || []).map(h => String(h || "").trim());
  for (const r of rows) {
    const o = {};
    headers.forEach((h,i)=> o[h]=r[i]||"");
    if (Object.values(o).some(v=>String(v).trim())) out.push(o);
  }
  return out;
};

const parseTable = (rows, kCol, vCol) => {
  const out = {};
  const headers = (rows.shift()||[]).map(h=>String(h||"").trim());
  const ki = headers.indexOf(kCol);
  const vi = headers.indexOf(vCol);
  if (ki<0 || vi<0) return out;
  for (const r of rows) {
    const k = String(r[ki]||"").trim();
    if (!k) continue;
    out[k] = String(r[vi]||"");
  }
  return out;
};

async function loadSheets() {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) return;

  const json = JSON.parse(
    Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8")
  );
  const auth = new google.auth.JWT({
    email: json.client_email,
    key: json.private_key,
    scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
  });
  const sheets = google.sheets({ version:"v4", auth });

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
  const get = (n)=> vr.find(v=>String(v.range||"").startsWith(n))?.values || [];

  const prompts = {};
  const pRows = get("PROMPTS!").slice();
  if (pRows.length) {
    const h = pRows.shift();
    for (const r of pRows) {
      const row = {};
      h.forEach((x,i)=>row[x]=r[i]||"");
      if (row.prompt_id && row.content_he)
        prompts[String(row.prompt_id).trim()] = String(row.content_he);
    }
  }

  const settings = parseTable(get("SETTINGS!").slice(), "key", "value");

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
    settings: Object.keys(settings).length
  });
}

const getPrompt = (id, fb="") => String(SHEETS.prompts[id]||fb).trim();
const getSetting = (k, fb="") => String(SHEETS.settings[k]||fb).trim();

// --------------------------------------------------
// Express
// --------------------------------------------------
const app = express();
app.use(express.json());

app.get("/health", (_,res)=>{
  res.json({ ok:true, sheets_loaded_at:SHEETS.loaded_at });
});

app.post("/sheets/reload", async (_,res)=>{
  await loadSheets();
  res.json({ ok:true, at:SHEETS.loaded_at });
});

app.post("/twilio-voice",(req,res)=>{
  const host = req.headers.host;
  const wsUrl = `wss://${host}/twilio-media-stream`;
  res.type("text/xml").send(`
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${req.body.From||""}" />
      <Parameter name="called" value="${req.body.To||""}" />
    </Stream>
  </Connect>
</Response>`.trim());
});

const server = http.createServer(app);

// ==================================================
// PART 2 / 3
// WebSocket core – Intent, Flow by Missing Fields
// ==================================================

const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// --------------------------------------------------
// Text & Phone helpers
// --------------------------------------------------
const normalizeText = (s) =>
  String(s || "")
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s]/gu, " ")
    .replace(/\s+/g, " ")
    .trim();

const extractPhoneDigits = (raw) => {
  let t = String(raw || "");
  const map = {
    "אפס":"0","אחד":"1","אחת":"1","שתיים":"2","שתים":"2","שניים":"2",
    "שלוש":"3","שלושה":"3","ארבע":"4","ארבעה":"4","חמש":"5","חמישה":"5",
    "שש":"6","שישה":"6","שבע":"7","שבעה":"7","שמונה":"8","תשע":"9","תשעה":"9"
  };
  for (const [w,d] of Object.entries(map)) {
    t = t.replace(new RegExp(`\\b${w}\\b`,"g"), d);
  }
  let digits = t.replace(/\D+/g,"");
  if (digits.startsWith("972")) digits = "0"+digits.slice(3);
  if (digits.length > 10) digits = digits.slice(0,10);
  return digits;
};
const isValidPhone = (d)=> d.length===10 && d.startsWith("0");
const spacedPhone = (d)=> d.split("").join(" ");

const extractName = (text)=>{
  let t = String(text||"").trim();
  t = t.replace(/\d+/g,"").trim();
  if (t.length<2 || t.length>40) return "";
  return t;
};

// --------------------------------------------------
// Intent detection (KB_FACTS + fallback keywords)
// --------------------------------------------------
const detectIntent = (text)=>{
  const t = normalizeText(text);

  // First: KB_FACTS routing
  for (const row of SHEETS.kbFacts || []) {
    if (String(row.category||"").toLowerCase()==="routing") {
      const keys = String(row.keywords||"")
        .split(",")
        .map(k=>normalizeText(k));
      if (keys.some(k=>k && t.includes(k))) {
        return String(row.topic||"");
      }
    }
  }

  // Fallback
  if (/תקלה|בעיה|שירות|אחריות|לא עובד/.test(t)) return "support";
  if (/משלוח|אספקה|הגיע|לא הגיע|שליח/.test(t)) return "delivery";
  if (/לקנות|רכישה|מוצר|דגם|מחיר|מבצע/.test(t)) return "sales";
  if (/הודעה|למסור|מנהל|עובד/.test(t)) return "message";
  return "";
};

// --------------------------------------------------
// DO_NOT_SAY guard
// --------------------------------------------------
const violatesDoNotSay = (text)=>{
  const t = normalizeText(text);
  for (const r of SHEETS.doNotSay||[]) {
    const triggers = String(r.trigger_examples||"")
      .split(",")
      .map(x=>normalizeText(x));
    if (triggers.some(tr=>tr && t.includes(tr))) {
      return String(r.safe_response_he||"");
    }
  }
  return "";
};

// --------------------------------------------------
// WebSocket per call
// --------------------------------------------------
wss.on("connection",(twilioWs)=>{
  let streamSid = "";
  let callSid = "";
  let caller = "";
  let called = "";

  let transcript = [];
  let awaitingResponse = false;

  // -----------------------------
  // Collected data – NO FSM
  // -----------------------------
  const data = {
    intent: "",
    full_name: "",
    product_type: "",
    product_model: "",
    product_brand: "",
    issue_desc: "",
    delivery_desc: "",
    message_target: "",
    message_body: "",
    callback_phone: "",
    extra_phone: "",
    carrier_info_given: false,
    reinforced: false
  };

  const pushTurn = (from,text)=>{
    transcript.push({ from, text, at:new Date().toISOString() });
    if (transcript.length>300) transcript=transcript.slice(-300);
  };

  // -----------------------------
  // Decide next prompt by missing fields
  // -----------------------------
  const nextPrompt = ()=>{
    if (!data.intent) return getPrompt("FLOW_ROUTING");

    // SALES
    if (data.intent==="sales") {
      if (!data.product_type) return getPrompt("FLOW_SALES_PRODUCT");
      if (!data.full_name) return getPrompt("FLOW_SALES_NAME");

      if (!data.reinforced) {
        data.reinforced = true;
        return [
          getSetting("PRICE_CLAIM_SENTENCE",""),
          getPrompt("SALES_PROMPT","")
        ].filter(Boolean).join(" ");
      }

      if (!data.product_model) return getPrompt("FLOW_SALES_MODEL");
      if (!data.product_brand) return getPrompt("FLOW_SALES_BRAND");
      if (!data.callback_phone) return getPrompt("FLOW_SALES_PHONE_CONFIRM");
      return getPrompt("FLOW_SALES_DONE");
    }

    // SUPPORT
    if (data.intent==="support") {
      if (!data.issue_desc) return getPrompt("FLOW_SUPPORT_ISSUE_DESC");
      if (!data.product_model) return getPrompt("FLOW_SUPPORT_MODEL");
      if (!data.product_brand) return getPrompt("FLOW_SUPPORT_BRAND");
      if (!data.full_name) return getPrompt("FLOW_SUPPORT_NAME");
      if (!data.callback_phone) return getPrompt("FLOW_SUPPORT_PHONE_CONFIRM");
      return getPrompt("FLOW_SUPPORT_DONE");
    }

    // DELIVERY
    if (data.intent==="delivery") {
      if (!data.delivery_desc) return getPrompt("FLOW_DELIVERY_DESC");
      if (!data.full_name) return getPrompt("FLOW_DELIVERY_NAME");
      if (!data.callback_phone) return getPrompt("FLOW_DELIVERY_PHONE_CONFIRM");
      return getPrompt("FLOW_DELIVERY_DONE");
    }

    // MESSAGE
    if (data.intent==="message") {
      if (!data.message_target) return getPrompt("FLOW_MESSAGE_TARGET");
      if (!data.message_body) return getPrompt("FLOW_MESSAGE_BODY");
      if (!data.full_name) return getPrompt("FLOW_MESSAGE_NAME");
      if (!data.callback_phone) return getPrompt("FLOW_MESSAGE_PHONE_CONFIRM");
      return getPrompt("FLOW_MESSAGE_DONE");
    }

    return getSetting("NO_DATA_MESSAGE","אפשר לחזור על זה?");
  };

  // -----------------------------
  // Consume caller utterance
  // -----------------------------
  const consumeCaller = (text)=>{
    const guard = violatesDoNotSay(text);
    if (guard) return guard;

    if (!data.intent) {
      const i = detectIntent(text);
      if (i) data.intent = i;
      return "";
    }

    const digits = extractPhoneDigits(text);
    if (isValidPhone(digits)) {
      if (!data.callback_phone) data.callback_phone = digits;
      else if (!data.extra_phone) data.extra_phone = digits;
    }

    if (!data.full_name) {
      const n = extractName(text);
      if (n) data.full_name = n;
    }

    if (data.intent==="sales") {
      if (!data.product_type) data.product_type = text;
      else if (!data.product_model) data.product_model = text;
      else if (!data.product_brand) data.product_brand = text;
    }

    if (data.intent==="support") {
      if (!data.issue_desc) data.issue_desc = text;
      else if (!data.product_model) data.product_model = text;
      else if (!data.product_brand) data.product_brand = text;
    }

    if (data.intent==="delivery") {
      if (!data.delivery_desc) data.delivery_desc = text;
    }

    if (data.intent==="message") {
      if (!data.message_target) data.message_target = text;
      else if (!data.message_body) data.message_body = text;
    }

    return "";
  };

  // --------------------------------------------------
  // Twilio inbound
  // --------------------------------------------------
  twilioWs.on("message",(raw)=>{
    const msg = JSON.parse(raw.toString());

    if (msg.event==="start") {
      streamSid = msg.start.streamSid;
      callSid = msg.start.callSid;
      caller = msg.start.customParameters?.caller||"";
      called = msg.start.customParameters?.called||"";
      return;
    }

    if (msg.event==="media" && openaiWs && !awaitingResponse) {
      openaiWs.send(JSON.stringify({
        type:"input_audio_buffer.append",
        audio: msg.media.payload
      }));
      return;
    }

    if (msg.event==="stop") {
      twilioWs.__ENDED__ = true;
    }
  });

  // expose state for PART 3
  twilioWs.__STATE__ = {
    data,
    transcript,
    nextPrompt,
    consumeCaller,
    pushTurn,
    meta:()=>({ callSid, streamSid, caller, called }),
    setAwait:(v)=>{ awaitingResponse=v; },
    isAwait:()=>awaitingResponse
  };
});

// ==================================================
// PART 3 / 3
// OpenAI Realtime bridge + Webhooks + Abandoned
// ==================================================

wss.on("connection",(twilioWs)=>{
  const state = twilioWs.__STATE__;
  if (!state) return;

  const { data, transcript, nextPrompt, consumeCaller, pushTurn } = state;
  const { callSid, streamSid, caller, called } = state.meta();

  let openaiWs = null;
  let awaitingResponse = false;
  let ended = false;

  if (!OPENAI_API_KEY) {
    twilioWs.close();
    return;
  }

  // --------------------------------------------------
  // OpenAI WebSocket
  // --------------------------------------------------
  openaiWs = new WebSocket(
    `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
    {
      headers:{
        Authorization:`Bearer ${OPENAI_API_KEY}`,
        "OpenAI-Beta":"realtime=v1"
      }
    }
  );

  openaiWs.on("open",()=>{
    const master = getPrompt(
      "MASTER_PROMPT",
      "אתם עוזרת קולית בשם נטע עבור גיל ספורט."
    );

    openaiWs.send(JSON.stringify({
      type:"session.update",
      session:{
        modalities:["audio","text"],
        voice: OPENAI_VOICE,
        input_audio_format:"g711_ulaw",
        output_audio_format:"g711_ulaw",
        turn_detection:{
          type:"server_vad",
          threshold: MB_VAD_THRESHOLD,
          silence_duration_ms: MB_VAD_SILENCE_MS,
          prefix_padding_ms: MB_VAD_PREFIX_MS,
          suffix_padding_ms: MB_VAD_SUFFIX_MS,
          create_response:false
        },
        instructions:[
          master,
          MB_BASE_STYLE,
          OPENAI_VOICE_STYLE ? `סטייל דיבור: ${OPENAI_VOICE_STYLE}` : ""
        ].filter(Boolean).join("\n")
      }
    }));

    // Opening – from SETTINGS only
    const opening = getSetting("OPENING_SCRIPT","");
    if (opening) {
      awaitingResponse = true;
      openaiWs.send(JSON.stringify({
        type:"response.create",
        response:{
          modalities:["audio","text"],
          instructions:`תגידי מילה במילה בלבד:\n${opening}`
        }
      }));
    }
  });

  // --------------------------------------------------
  // OpenAI inbound
  // --------------------------------------------------
  openaiWs.on("message",async(raw)=>{
    const msg = JSON.parse(raw.toString());

    // Caller transcript (final)
    if (
      msg.type.includes("input_audio_transcription") &&
      (msg.type.includes("done") || msg.type.includes("completed")) &&
      msg.transcript
    ) {
      const text = String(msg.transcript).trim();
      if (!text) return;

      pushTurn("caller", text);

      const guardReply = consumeCaller(text);
      let instructions = guardReply || nextPrompt();

      if (instructions && !awaitingResponse) {
        awaitingResponse = true;
        openaiWs.send(JSON.stringify({
          type:"response.create",
          response:{ modalities:["audio","text"], instructions }
        }));
      }
      return;
    }

    // Bot transcript (final)
    if (msg.type==="response.audio_transcript.done") {
      if (msg.transcript) {
        pushTurn("bot", msg.transcript);
      }
      return;
    }

    // Audio to Twilio
    if (msg.type==="response.audio.delta" && streamSid) {
      twilioWs.send(JSON.stringify({
        event:"media",
        streamSid,
        media:{ payload: msg.delta }
      }));
      return;
    }

    // Response finished
    if (msg.type==="response.done") {
      awaitingResponse = false;

      const isComplete =
        (data.intent==="sales" &&
          data.product_type && data.full_name && data.callback_phone) ||
        (data.intent==="support" &&
          data.issue_desc && data.full_name && data.callback_phone) ||
        (data.intent==="delivery" &&
          data.delivery_desc && data.full_name && data.callback_phone) ||
        (data.intent==="message" &&
          data.message_body && data.full_name && data.callback_phone);

      if (isComplete && !ended) {
        ended = true;

        const payload = {
          callSid,
          streamSid,
          caller,
          called,
          at: new Date().toISOString(),
          intent: data.intent,
          full_name: data.full_name,
          product_type: data.product_type,
          product_model: data.product_model,
          product_brand: data.product_brand,
          issue_desc: data.issue_desc,
          delivery_desc: data.delivery_desc,
          message_target: data.message_target,
          message_body: data.message_body,
          callback_phone: data.callback_phone,
          extra_phone: data.extra_phone,
          transcript,
          recording_url_public:
            PUBLIC_BASE_URL && callSid
              ? `${PUBLIC_BASE_URL.replace(/\/$/,"")}/recording/${callSid}`
              : ""
        };

        const eventMap = {
          sales:"sales_lead",
          support:"support_ticket",
          delivery:"delivery_ticket",
          message:"message_taken"
        };

        await fetch(MB_WEBHOOK_URL,{
          method:"POST",
          headers:{ "Content-Type":"application/json" },
          body: JSON.stringify({
            event: eventMap[data.intent] || "call_ended",
            ...payload
          })
        });

        try { openaiWs.close(); } catch(_){}
        try { twilioWs.close(); } catch(_){}
      }
      return;
    }
  });

  openaiWs.on("close",()=>{
    try { twilioWs.close(); } catch(_){}
  });

  openaiWs.on("error",(e)=>{
    error("OpenAI WS error", e.message);
    try { twilioWs.close(); } catch(_){}
  });

  // --------------------------------------------------
  // Abandoned
  // --------------------------------------------------
  twilioWs.on("close",()=>{
    if (!ended) {
      fetch(MB_WEBHOOK_URL,{
        method:"POST",
        headers:{ "Content-Type":"application/json" },
        body: JSON.stringify({
          event:"call_abandoned",
          at:new Date().toISOString(),
          caller,
          callSid,
          stage: data.intent || "routing",
          recording_url_public:
            PUBLIC_BASE_URL && callSid
              ? `${PUBLIC_BASE_URL.replace(/\/$/,"")}/recording/${callSid}`
              : ""
        })
      }).catch(()=>{});
    }
  });
});

// --------------------------------------------------
// Start server
// --------------------------------------------------
server.listen(PORT,()=>{
  log(`GilSport VoiceBot running on port ${PORT}`);
  loadSheets();
});


