import express from "express";

const app = express();
const PORT = process.env.PORT || 3000;

// Health check – Render משתמש בזה
app.get("/health", (req, res) => {
  res.status(200).json({
    status: "ok",
    service: "gilsport-voicebot",
    timestamp: new Date().toISOString()
  });
});

// Root – בדיקה ידנית בדפדפן
app.get("/", (req, res) => {
  res.send("GilSport VoiceBot is running");
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
