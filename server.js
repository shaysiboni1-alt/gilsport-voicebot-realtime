// server.js
// GilSport Realtime VoiceBot – Neta based
// Single Source of Truth: EXACT match with uploaded Google Sheets tabs

require("dotenv").config();

const express = require("express");
const http = require("http");
const https = require("https");
const WebSocket = require("ws");
const { google } = require("googleapis");

// --------------------------------------------------
// ENV & CONFIG
// --------------------------------------------------
const PORT = Number(process.env.PORT) || 10000;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_REALTIME_MODEL = process.env.OPENAI_REALTIME_MODEL || "gpt-4o-realtime-preview-2024-12-17";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "alloy";

const GSHEET_ID = process.env.GSHEET_ID || "";
const GOOGLE_SERVICE_ACCOUNT_JSON_B64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_B64 || "";

const MB_WEBHOOK_URL = process.env.MB_WEBHOOK_URL || "";
const MB_DEBUG = ["1", "true"].includes(String(process.env.MB_DEBUG).toLowerCase());

const MB_VAD_THRESHOLD = Number(process.env.MB_VAD_THRESHOLD) || 0.65;
const MB_VAD_SILENCE_MS = Number(process.env.MB_VAD_SILENCE_MS) || 900;

// --------------------------------------------------
// Logging
// --------------------------------------------------
const log = (...a) => console.log("[INFO]", ...a);
const error = (...a) => console.error("[ERROR]", ...a);
const always = (...a) => console.log("[ALWAYS]", ...a);

// --------------------------------------------------
// Webhook (Native HTTPS)
// --------------------------------------------------
function callWebhook(payload) {
  if (!MB_WEBHOOK_URL) return;
  const data = JSON.stringify({ ...payload, timestamp: new Date().toISOString() });
  try {
    const url = new URL(MB_WEBHOOK_URL);
    const options = {
      hostname: url.hostname,
      path: url.pathname + url.search,
      method: "POST",
      headers: { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(data) }
    };
    const req = https.request(options);
    req.on("error", (e) => error("Webhook failed", e.message));
    req.write(data);
    req.end();
  } catch (e) { error("Webhook URL error", e.message); }
}

// --------------------------------------------------
// Sheets (Matching uploaded files)
// --------------------------------------------------
let SHEETS = {
  loaded_at: null,
  prompts: {}, 
  settings: {},
  kb_facts: [],
  do_not_say: [],
  suppliers: [],
  delivery_contacts: []
};

function parseTable(rows, keyCol, valCol) {
  const out = {};
  if (!rows || rows.length < 2) return out;
  const headers = rows[0].map(h => String(h || "").trim());
  const kIdx = headers.indexOf(keyCol);
  const vIdx = headers.indexOf(valCol);
  if (kIdx === -1 || vIdx === -1) return out;
  for (let i = 1; i < rows.length; i++) {
    const k = String(rows[i][kIdx] || "").trim();
    if (k) out[k] = String(rows[i][vIdx] || "").trim();
  }
  return out;
}

function parseToObjects(rows) {
  if (!rows || rows.length < 2) return [];
  const headers = rows[0].map(h => String(h || "").trim());
  return rows.slice(1).map(r => {
    const obj = {};
    headers.forEach((h, i) => { if(h) obj[h] = r[i] || ""; });
    return obj;
  });
}

async function loadSheets() {
  if (!GSHEET_ID || !GOOGLE_SERVICE_ACCOUNT_JSON_B64) return;
  try {
    const json = JSON.parse(Buffer.from(GOOGLE_SERVICE_ACCOUNT_JSON_B64, "base64").toString("utf8"));
    const auth = new google.auth.JWT({
      email: json.client_email,
      key: json.private_key,
      scopes: ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    });
    const sheets = google.sheets({ version: "v4", auth });

    // שמות הטאבים כפי שמופיעים בקבצים שהעלית
    const tabs = ["PROMPTS", "SETTINGS", "KB_FACTS", "DO_NOT_SAY", "SUPPLIERS_IMPORTERS", "DELIVERY_CONTACTS"];
    
    const results = await Promise.all(tabs.map(async (tab) => {
      try {
        const res = await sheets.spreadsheets.values.get({ spreadsheetId: GSHEET_ID, range: `${tab}!A:Z` });
        return { tab, values: res.data.values || [] };
      } catch (e) {
        error(`Tab ${tab} load error`, e.message);
        return { tab, values: [] };
      }
    }));

    const get = (name) => results.find(r => r.tab === name)?.values || [];

    SHEETS.prompts = parseTable(get("PROMPTS"), "prompt_id", "content_he");
    SHEETS.settings = parseTable(get("SETTINGS"), "key", "value");
    SHEETS.kb_facts = parseToObjects(get("KB_FACTS"));
    SHEETS.do_not_say = parseToObjects(get("DO_NOT_SAY"));
    SHEETS.suppliers = parseToObjects(get("SUPPLIERS_IMPORTERS"));
    SHEETS.delivery_contacts = parseToObjects(get("DELIVERY_CONTACTS"));

    SHEETS.loaded_at = new Date().toISOString();
    log(`Sheets updated from correct tabs at ${SHEETS.loaded_at}`);
  } catch (e) { error("Critical Sheets error", e.message); }
}

// --------------------------------------------------
// Proxy Knowledge (Decision Layer)
// --------------------------------------------------
function getKnowledgeInjection(callerText) {
  const text = (callerText || "").toLowerCase();
  let injection = "";

  // Delivery Policy & Contacts
  if (text.includes("משלוח") || text.includes("איפה") || text.includes("הזמנה")) {
    const contacts = SHEETS.delivery_contacts.map(c => `${c.name}: ${c.phone}`).join(", ");
    injection += `\nמידע משלוחים: לא להבטיח בדיקת סטטוס חי. אם זה אחרי שעות הפעילות, תני את מספרי המובילים: ${contacts}.`;
  }

  // Do Not Say Enforcement
  if (SHEETS.do_not_say.length > 0) {
    const rules = SHEETS.do_not_say.map(r => r.rule || r.content).join(" | ");
    injection += `\nחוקי "אל תגידי": ${rules}`;
  }

  // KB Facts
  const relevantFact = SHEETS.kb_facts.find(f => text.includes(String(f.keyword || "").toLowerCase()));
  if (relevantFact) injection += `\nעובדה רלוונטית: ${relevantFact.content || relevantFact.fact}`;

  return injection;
}

// --------------------------------------------------
// Express & Twilio
// --------------------------------------------------
const app = express();
app.use(express.json());

app.post("/twilio-voice", (req, res) => {
  const host = req.headers.host;
  res.type("text/xml").send(`
<Response>
  <Connect>
    <Stream url="wss://${host}/twilio-media-stream">
      <Parameter name="caller" value="${req.body.From || ""}" />
    </Stream>
  </Connect>
</Response>`.trim());
});

const server = http.createServer(app);
const wss = new WebSocket.Server({ server, path: "/twilio-media-stream" });

// --------------------------------------------------
// Bridge Logic
// --------------------------------------------------
wss.on("connection", (twilioWs) => {
  let streamSid = null;
  let callerNum = "";
  let openaiReady = false;
  let hasTranscripts = false;
  let lastText = "";
  let fullTranscript = [];

  const openaiWs = new WebSocket(`wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`, {
    headers: { Authorization: `Bearer ${OPENAI_API_KEY}`, "OpenAI-Beta": "realtime=v1" }
  });

  openaiWs.on("open", async () => {
    openaiReady = true;
    if (!SHEETS.loaded_at) await loadSheets();
    
    const master = SHEETS.prompts["MASTER_PROMPT"] || "את נטע מגיל ספורט.";
    const opening = SHEETS.settings["OPENING_SCRIPT"] || "שלום, מדברת נטע.";

    openaiWs.send(JSON.stringify({
      type: "session.update",
      session: {
        voice: OPENAI_VOICE,
        instructions: master,
        turn_detection: { type: "server_vad", threshold: MB_VAD_THRESHOLD, silence_duration_ms: MB_VAD_SILENCE_MS }
      }
    }));

    openaiWs.send(JSON.stringify({
      type: "response.create",
      response: { modalities: ["audio", "text"], instructions: `תגידי בדיוק: ${opening}` }
    }));
    callWebhook({ event: "call_started", caller: callerNum });
  });

  openaiWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.type === "response.audio_transcript.done") {
      fullTranscript.push(`Bot: ${msg.transcript}`);
    }
    if (msg.type === "conversation.item.input_audio_transcription.completed") {
      const t = msg.transcript?.trim();
      if (t) {
        lastText = t;
        fullTranscript.push(`User: ${t}`);
        hasTranscripts = true;
        const inj = getKnowledgeInjection(t);
        if (inj) {
          openaiWs.send(JSON.stringify({
            type: "conversation.item.create",
            item: { type: "message", role: "system", content: [{ type: "input_text", text: inj }] }
          }));
        }
        openaiWs.send(JSON.stringify({ type: "response.create" }));
      }
    }
    if (msg.type === "response.audio.delta" && streamSid) {
      twilioWs.send(JSON.stringify({ event: "media", streamSid, media: { payload: msg.delta } }));
    }
  });

  twilioWs.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    if (msg.event === "start") {
      streamSid = msg.start.streamSid;
      callerNum = msg.start.customParameters?.caller || "";
    }
    if (msg.event === "media" && openaiReady) {
      openaiWs.send(JSON.stringify({ type: "input_audio_buffer.append", audio: msg.media.payload }));
    }
  });

  twilioWs.on("close", () => {
    callWebhook({
      event: hasTranscripts ? "call_ended" : "call_abandoned",
      callSid: streamSid,
      caller: callerNum,
      transcript: fullTranscript.join("\n"),
      recording_url_public: `https://api.twilio.com/2010-04-01/Accounts/${process.env.TWILIO_ACCOUNT_SID}/Recordings/${streamSid}`
    });
    if (openaiWs) openaiWs.close();
  });
});

server.listen(PORT, () => {
  log(`Server listening on port ${PORT}`);
  loadSheets();
});
