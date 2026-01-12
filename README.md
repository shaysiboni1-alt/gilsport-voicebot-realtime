# GilSport Realtime VoiceBot (Neta-based)

Twilio Media Streams <-> OpenAI Realtime API, driven by Google Sheets (single source of truth).

## Endpoints
- POST /twilio-voice  -> TwiML to start <Connect><Stream>
- wss://<domain>/twilio-media-stream -> Twilio Media Stream WS
- GET /health
- POST /sheets/reload

## ENV (Render)
- OPENAI_API_KEY
- GSHEET_ID
- GOOGLE_SERVICE_ACCOUNT_JSON_B64
- MB_WEBHOOK_URL (single Make endpoint, includes {event,...})

Optional:
- OPENAI_REALTIME_MODEL (default gpt-4o-realtime-preview-2024-12-17)
- OPENAI_VOICE (default alloy)
- MB_DEBUG=true
- MB_ALLOW_BARGE_IN=false
- TWILIO_ACCOUNT_SID / TWILIO_AUTH_TOKEN (optional)
