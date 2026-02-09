"use strict";
require("dotenv").config();
const mode = String(process.env.PROVIDER_MODE || "openai").toLowerCase();
if (mode === "gemini") {
  require("./server.gemini");
} else {
  require("./server.openai");
}
