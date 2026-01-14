 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/server.js b/server.js
index 75268a7d05de568d24761dd195814e05bfc61e26..ca05fc3b38711a52725c8fd01ad0c72909b4fae4 100644
--- a/server.js
+++ b/server.js
@@ -498,439 +498,662 @@ wss.on("connection", (twilioWs, req) => {
   let lastBotFinal = "";
   // Tracks the last caller utterance for which a response was requested.
   // This prevents sending multiple assistant responses for the same caller final.
   let lastRequestedCallerFinal = "";
 
   // Internal flags for response management.  We no longer track speech
   // segments; instead, we queue responses based on new caller final
   // transcriptions (see message handlers below).
 
   // Call/session state for webhook + routing + abandoned
   let callSid = null;
   let startedAt = nowIso();
   let endedAt = null;
   let route = "other";
   let language = getSetting("DEFAULT_LANGUAGE", "he") || "he";
 
   let transcriptTurns = []; // {from, text, at}
 
   // Abandoned / ended dedupe guards
   let sentCallEnded = false;
   let sentCallAbandoned = false;
 
   // Proxy decision: dynamic response instructions (no FSM)
   let proxyInstructions = "";
 
+  // Keep track of all phone numbers provided by the caller during this call. When
+  // the caller mentions a phone number in their utterance, we extract the
+  // digits and store them here. These numbers will be sent in the final
+  // webhook payload under recognized_phones. The array is deduplicated.
+  let recognizedPhones = [];
+
   // Buffer audio frames when the assistant is speaking. When awaitingResponse is
   // true, we temporarily store incoming caller audio and send it only after
   // the assistant finishes speaking. This prevents the model from listening
   // and reacting to noise or speech while it's talking.
   let pausedAudioBuffer = [];
 
   const pushTurn = (from, text) => {
     const t = String(text || "").trim();
     if (!t) return;
     transcriptTurns.push({ from, text: t, at: nowIso() });
     // cap
     if (transcriptTurns.length > 400) transcriptTurns = transcriptTurns.slice(-400);
   };
 
   const extractPhoneCandidates = (text) => {
     const t = String(text || "");
     const digits = t.replace(/\D+/g, "");
     // Israeli 9-10 digits typical
     if (digits.length === 9 || digits.length === 10) return digits;
     if (digits.length > 10) return digits.slice(-10);
     return "";
   };
 
-  const getBizPhonesByHint = (hint) => {
-    const rows = Array.isArray(SHEETS.businessInfo) ? SHEETS.businessInfo : [];
-    const out = [];
-    const h = String(hint || "").toLowerCase();
-    for (const r of rows) {
-      const values = Object.values(r || {}).map((v) => String(v || ""));
-      const joined = values.join(" ").toLowerCase();
-      if (h && !joined.includes(h)) continue;
-      // try find any phone-like field
-      for (const [k, v] of Object.entries(r || {})) {
-        const key = String(k || "").toLowerCase();
-        const val = String(v || "").trim();
-        if (!val) continue;
-        if (key.includes("phone") || key.includes("טלפון") || key.includes("מספר")) {
-          out.push(val);
-        }
-      }
+  const formatSpacedDigits = (digits) => String(digits || "").split("").join(" ");
+
+  const normalizePhoneDigits = (raw) => {
+    let digits = String(raw || "").replace(/\D+/g, "");
+    if (digits.startsWith("972") && digits.length > 3) {
+      digits = "0" + digits.slice(3);
     }
-    return Array.from(new Set(out)).slice(0, 10);
+    return digits;
   };
 
-  const normalizeKeywords = (s) =>
-    String(s || "")
-      .split(/[,;\n\r\t]+/)
-      .map((x) => x.trim())
-      .filter(Boolean);
+  const isValidPhoneDigits = (digits) => {
+    const d = String(digits || "").replace(/\D+/g, "");
+    return d.length === 9 || d.length === 10;
+  };
 
-  const pickRouteFromRules = (text) => {
-    const t = String(text || "").toLowerCase();
-    const rules = Array.isArray(SHEETS.routingRules) ? SHEETS.routingRules : [];
-    const scored = [];
-    for (const r of rules) {
-      const routeVal = (r.route || r.intent || r.Route || r.ROUTE || "").toString().trim();
-      const pr = Number(r.priority || r.Priority || r.PRIORITY || 999);
-      // find any keyword field
-      let kws = [];
-      for (const [k, v] of Object.entries(r || {})) {
-        const key = String(k || "").toLowerCase();
-        if (key.includes("keyword") || key.includes("keywords") || key.includes("מילות")) {
-          kws = kws.concat(normalizeKeywords(v));
-        }
-      }
-      kws = kws.map((x) => x.toLowerCase());
-      if (!routeVal || !kws.length) continue;
-      const hit = kws.some((kw) => kw && t.includes(kw));
-      if (hit) scored.push({ route: routeVal, priority: Number.isFinite(pr) ? pr : 999 });
-    }
-    scored.sort((a, b) => a.priority - b.priority);
-    return scored[0]?.route || "";
+  const isYes = (text) =>
+    /(כן|כן כן|נכון|מאשר|אישור|yes|yep|yeah|ok|בסדר|סבבה|מוסכם)/i.test(
+      String(text || "").trim()
+    );
+
+  const isNo = (text) =>
+    /(לא|לא תודה|לא זה|לא נכון|no|nope|לא מעוניין|לא מסכים)/i.test(
+      String(text || "").trim()
+    );
+
+  const extractBrandModel = (text) => {
+    const t = String(text || "");
+    const brandMatch = t.match(/מותג\s+([^,.\n\r]+)/);
+    const modelMatch = t.match(/דגם\s+([^,.\n\r]+)/);
+    return {
+      brand: brandMatch ? brandMatch[1].trim() : "",
+      model: modelMatch ? modelMatch[1].trim() : ""
+    };
+  };
+
+  const extractRoute = (text) => {
+    const low = String(text || "").toLowerCase();
+    if (/(אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול)/.test(low)) return "support";
+    if (/(משלוח|אספקה|עסקה|הספקה|אספקת|שליח|הזמנה|הגיע|לא הגיע|מוביל)/.test(low))
+      return "delivery";
+    if (/(מחיר|לקנות|רכישה|מוצר|דגם|מידה|צבע|מלאי|כמה עולה|מבצע)/.test(low))
+      return "sales";
+    if (/(הודעה|מנהל|עובד|לחזור אלי|השארת הודעה)/.test(low)) return "message";
+    return "";
   };
 
   const parseHours = (s) => {
     // expects like "09:00-18:00" or "09:00–18:00"
     const m = String(s || "").match(/(\d{1,2}):(\d{2})\s*[-–]\s*(\d{1,2}):(\d{2})/);
     if (!m) return null;
     const aH = Number(m[1]),
       aM = Number(m[2]),
       bH = Number(m[3]),
       bM = Number(m[4]);
     if (![aH, aM, bH, bM].every((x) => Number.isFinite(x))) return null;
     return { start: aH * 60 + aM, end: bH * 60 + bM };
   };
 
   const isAfterHours = () => {
     const hoursStr =
       getSetting("BUSINESS_HOURS", "") ||
       getSetting("HOURS", "") ||
       getSetting("WORKING_HOURS", "") ||
       "";
     const parsed = parseHours(hoursStr);
     if (!parsed) return false; // if unknown, do not force after-hours
     // Use local time in TIME_ZONE
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
 
-  const buildProxyInstructions = (callerText) => {
-    const t = String(callerText || "").trim();
-    if (!t) return "";
-
-    const low = t.toLowerCase();
-
-    // Route heuristics (no FSM)
-    if (/(אחריות|תקלה|בעיה|שירות|החלפה|החזרה|לא עובד|תקול)/.test(low)) route = "support";
-    else if (/(משלוח|אספקה|עסקה|הספקה|אספקת|שליח|הזמנה|הגיע|לא הגיע|מוביל)/.test(low)) route = "delivery";
-    else if (/(מחיר|לקנות|רכישה|מוצר|דגם|מידה|צבע|מלאי|כמה עולה|מבצע)/.test(low)) route = "sales";
-    else route = route || "other";
-
-    // DO_NOT_SAY: rows in DO_NOT_SAY tab (enforce via instruction)
+  const buildFlowInstructions = (sayText, extra = []) => {
+    const baseStyle =
+      MB_BASE_STYLE && MB_BASE_STYLE.trim()
+        ? MB_BASE_STYLE.trim()
+        : "סגנון: נטע. תשובות קצרות, ענייניות, אנושיות. בלי חזרות מיותרות.";
     const dnsRows = Array.isArray(SHEETS.doNotSay) ? SHEETS.doNotSay : [];
     const doNotSayText = dnsRows
       .map((r) => {
         const a = String(r.forbidden_topic || "").trim();
         const b = String(r.trigger_examples || "").trim();
         const c = String(r.safe_response_he || "").trim();
         const parts = [a && `נושא: ${a}`, b && `טריגרים: ${b}`, c && `תגובה בטוחה: ${c}`].filter(
           Boolean
         );
         return parts.join(" | ");
       })
       .filter(Boolean)
       .slice(0, 20)
       .join("\n");
+    const rules = [
+      baseStyle,
+      "אין להמציא שמות, מספרים או פרטים שלא נאמרו או שלא קיימים בשיטס.",
+      "כאשר מציינים מספר טלפון, הקריאי ספרה־ספרה בלבד.",
+      doNotSayText ? `DO_NOT_SAY (כללים מחייבים):\n${doNotSayText}` : ""
+    ].filter(Boolean);
+    const say = sayText ? `תגידי בדיוק את המשפט הבא מילה במילה, ללא תוספות:\n${sayText}` : "";
+    return [...rules, ...extra.filter(Boolean), say].filter(Boolean).join("\n\n").trim();
+  };
 
-    const mustNotLieDelivery =
-      "אין לך גישה לסטטוס משלוח אמיתי. אסור להגיד 'בדקתי סטטוס' או להבטיח שראית מערכת משלוחים.";
-
-    // DELIVERY_CONTACTS: provide carrier contacts when needed
+  const buildCarrierList = () => {
     const deliveryRows = Array.isArray(SHEETS.deliveryContacts) ? SHEETS.deliveryContacts : [];
-    const carrierPhones = deliveryRows
-      .filter((r) => {
-        const ck = String(r.condition_keywords || "").toLowerCase();
-        return !ck || ck.split(/[,;\n\r\t]+/).some((kw) => kw.trim() && low.includes(kw.trim()));
+    const carrierDescriptions = deliveryRows
+      .map((r) => {
+        let p = String(r.phone_e164 || r.phone || "").replace(/\D+/g, "");
+        if (!p) return "";
+        if (p.startsWith("972") && p.length > 3) {
+          p = "0" + p.slice(3);
+        }
+        const spaced = formatSpacedDigits(p);
+        const name = String(r.name || "").trim();
+        return name ? `${name} – ${spaced}` : spaced;
       })
-      .map((r) => String(r.phone_e164 || r.phone || "").trim())
-      .filter(Boolean)
-      .slice(0, 10);
-
-    // KB_FACTS: soft facts injection (only a few)
-    const factsRows = Array.isArray(SHEETS.kbFacts) ? SHEETS.kbFacts : [];
-    const matchFacts = [];
-    for (const r of factsRows) {
-      const kw = String(r.keywords || "").toLowerCase();
-      if (!kw) continue;
-      const kws = kw.split(/[,;\n\r\t]+/).map((x) => x.trim()).filter(Boolean);
-      if (!kws.length) continue;
-      if (kws.some((k) => k && low.includes(k))) {
-        const ans = String(r.answer_he || "").trim();
-        if (ans) matchFacts.push(`• ${ans}`);
-      }
-      if (matchFacts.length >= 5) break;
-    }
-
-    // After-hours check (uses SETTINGS hours if present; if unknown -> false)
-    // We also treat phrases like "אחרי שעות" or "אחרי שעות הפעילות" in the caller text as hints
-    // that the caller expects information outside of business hours. This allows the assistant
-    // to offer after-hours delivery options even if the current time is within working hours.
-    let afterHours = false;
-    if (route === "delivery") {
-      // detect explicit mentions of being after hours in the caller's utterance
-      const afterHoursHint = /(אחרי\s+שעות|אחרי\s+שעות\s+הפעילות|לאחר\s+שעות\s+הפעילות|מחוץ\s+לשעות\s+הפעילות)/.test(
-        low
-      );
-      afterHours = isAfterHours() || afterHoursHint;
-    }
-
-    const baseStyle =
-      "סגנון: נטע. תשובות קצרות, ענייניות, אנושיות. משפט-שניים ואז שאלה מקדמת. לא לחפור, לא לחזור על עצמך.";
+      .filter(Boolean);
+    return carrierDescriptions;
+  };
 
-    const parts = [];
-    parts.push(baseStyle);
-    parts.push(
-      "תעדיפי מידע מהשיטס (KB_FACTS/DELIVERY_CONTACTS/DO_NOT_SAY/SUPPLIERS_IMPORTERS) על פני המצאות."
-    );
-    // Always instruct the assistant to read numbers digit by digit and repeat phone numbers exactly as given
-    parts.push(
-      "כאשר את מציינת מספר טלפון או קוד (כמו קוד קופון), הקריאי כל ספרה בנפרד – למשל: 5 5 5 5. " +
-        "אם את חוזרת על מספר טלפון שנאמר, הקריאי אותו בדיוק כפי שהלקוח אמר, ספרה־ספרה, ללא חזרות או השמטה. " +
-        "אל תקראי מספרים ברצף אחד. אם מתאים – ניתן גם להגיד שהטלפון נרשם בלי לחזור עליו, במקום לקרוא אותו שוב."
+  const findExactImporter = (brandName) => {
+    const brand = String(brandName || "").trim();
+    if (!brand) return null;
+    const importerRows = Array.isArray(SHEETS.suppliersImporters)
+      ? SHEETS.suppliersImporters
+      : [];
+    const match = importerRows.find(
+      (r) => String(r.brand_name || "").trim() === brand
     );
+    if (!match) return null;
+    return {
+      brand: brand,
+      importer: String(match.importer_name || "").trim(),
+      phone: String(match.phone_e164 || match.phone || "").trim()
+    };
+  };
 
-    // Never claim that information is unavailable for coupon queries. This prevents the phrase
-    // "אין לי מידע מדויק" from appearing. If you do not have specific info, tell the caller
-    // שהמידע לא נמצא בגיליון ותוכלי להשאיר פרטים לחזרה.
-    parts.push(
-      "אסור לומר 'אין לי מידע' או 'אין לי מידע מדויק'. במקום זאת, אם המידע לא נמצא בשיטס – אמרי שכרגע אין נתון זמין ותציעי להשאיר פרטים לחזרה."
-    );
+  const flowState = {
+    stage: "routing",
+    askedRouting: false,
+    route: "other",
+    afterHours: false,
+    data: {
+      product_interest: "",
+      product_model: "",
+      product_brand: "",
+      issue_type: "",
+      issue_desc: "",
+      message_target: "",
+      message_body: "",
+      delivery_desc: "",
+      full_name: "",
+      callback_phone: ""
+    },
+    finalEvent: "",
+    finalSummary: "",
+    finalPayload: null,
+    shouldHangup: false
+  };
 
-    // If caller asks about coupon codes, inject the coupon instruction early so the assistant does not claim missing info
-    let isCouponQuery = false;
-    try {
-      const couponKeywordsEarly = [
-        "קופון",
-        "קוד קופון",
-        "קוד הנחה",
-        "הנחה",
-        "קופון לאתר",
-        "קוד להנחה"
-      ];
-      isCouponQuery = couponKeywordsEarly.some((kw) => low.includes(kw));
-      if (isCouponQuery) {
-        const couponVal = String(getSetting("SALES_COUPON_CODE", "")).trim();
-        if (couponVal) {
-          const spacedCouponEarly = couponVal.replace(/\D/g, "").split("").join(" ");
-          parts.push(
-            `אם שואלים על קופון או על קוד קופון – תשיבי במשפט אחד ובאופן חד-משמעי: קוד הקופון לרכישה באתר הוא ${spacedCouponEarly}. ` +
-              `אל תגידי שאין לך מידע בנושא (המידע נמצא בשיטס), ואל תמציאי קוד אחר. לא להוסיף משפט מקדים לפני הקוד.`
-          );
-        }
-      }
-    } catch (_) {
-      /* ignore coupon lookup errors */
+  const ensureCallerDigits = () => {
+    const callerRaw = String(caller || "").trim();
+    if (!callerRaw) return "";
+    return normalizePhoneDigits(callerRaw);
+  };
+
+  const buildFinalPayload = () => {
+    const ended = endedAt || nowIso();
+    const fallbackCallerPhone = ensureCallerDigits();
+    const payload = {
+      callSid,
+      streamSid: twilioStreamSid,
+      caller,
+      called,
+      started_at: startedAt,
+      ended_at: ended,
+      language,
+      route: flowState.route,
+      caller_last_utterance: lastCallerFinal,
+      bot_last_utterance: lastBotFinal,
+      transcript: transcriptTurns,
+      recognized_phones: recognizedPhones,
+      call_reason: flowState.route,
+      call_subject: flowState.finalSummary || lastBotFinal || lastCallerFinal
+    };
+    if (flowState.route === "sales") {
+      payload.product_interest = flowState.data.product_interest;
+      payload.product_model = flowState.data.product_model;
+      payload.product_brand = flowState.data.product_brand;
+      payload.full_name = flowState.data.full_name;
+      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
+    } else if (flowState.route === "support") {
+      payload.issue_type = flowState.data.issue_type;
+      payload.issue_desc = flowState.data.issue_desc;
+      payload.product_model = flowState.data.product_model;
+      payload.product_brand = flowState.data.product_brand;
+      payload.full_name = flowState.data.full_name;
+      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
+    } else if (flowState.route === "delivery") {
+      payload.delivery_desc = flowState.data.delivery_desc;
+      payload.full_name = flowState.data.full_name;
+      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
+      payload.after_hours = flowState.afterHours ? "כן" : "לא";
+    } else if (flowState.route === "message") {
+      payload.message_target = flowState.data.message_target;
+      payload.message_body = flowState.data.message_body;
+      payload.full_name = flowState.data.full_name;
+      payload.callback_phone = flowState.data.callback_phone || fallbackCallerPhone;
     }
+    return payload;
+  };
 
-    // Always mention the caller's default phone number so the assistant suggests it first
-    try {
-      const callerRaw = String(caller || "").trim();
-      if (callerRaw) {
-        // extract digits and convert Israeli E.164 numbers (+972…) to 0X… local format
-        let digits = callerRaw.replace(/\D/g, "");
-        if (digits.startsWith("972") && digits.length > 3) {
-          digits = "0" + digits.slice(3);
-        }
-        // insert spaces to ensure the model reads each digit separately
-        const spacedCaller = digits.split("").join(" ");
-        parts.push(
-          `המספר ממנו התקשרתם הוא ${spacedCaller}. ברירת המחדל היא לחזור למספר זה. שאלי תמיד אם נוח לכם שנחזור למספר הזה או אם יש מספר אחר. אל תמציאי מספרים.`
-        );
-        // Also instruct to collect full name and confirm number for all routes
-        parts.push(
-          `בכל שיחה, קחי שם מלא של הלקוח ושאלי האם נוח לחזור למספר המזוהה (${spacedCaller}) או שמעדיף מספר אחר. אם נמסר מספר, הקראי אותו ספרה־ספרה ללא השמטה או תוספת, ובמידת הצורך הודיעי שהמספר נרשם בלי לקרוא אותו שוב.`
-        );
+  const handleRouting = (utterance) => {
+    const routeCandidate = extractRoute(utterance);
+    if (routeCandidate) {
+      flowState.route = routeCandidate;
+      flowState.stage =
+        routeCandidate === "sales"
+          ? "sales_product"
+          : routeCandidate === "support"
+          ? "support_issue"
+          : routeCandidate === "delivery"
+          ? "delivery_name"
+          : "message_target";
+      if (routeCandidate === "delivery") {
+        flowState.afterHours = isAfterHours();
+        flowState.data.delivery_desc = String(utterance || "").trim();
       }
-    } catch (_) {
-      /* ignore caller number errors */
+      if (routeCandidate === "sales") {
+        flowState.data.product_interest = String(utterance || "").trim();
+      }
+      if (routeCandidate === "support") {
+        flowState.data.issue_desc = String(utterance || "").trim();
+        flowState.data.issue_type = String(utterance || "").trim();
+      }
+    } else if (!flowState.askedRouting) {
+      flowState.askedRouting = true;
+      flowState.stage = "routing_clarify";
+    } else {
+      flowState.route = "message";
+      flowState.stage = "message_target";
     }
+    route = flowState.route;
+  };
 
-    // Include guardrails and routing prompts if available
-    try {
-      const ps = SHEETS.prompts || {};
-      const guardrailsPrompt = ps.GUARDRAILS_PROMPT || "";
-      const routingPrompt = ps.ROUTING_PROMPT || "";
-      if (guardrailsPrompt) parts.push(guardrailsPrompt.trim());
-      if (routingPrompt) parts.push(routingPrompt.trim());
-    } catch (_) {}
-
-    if (doNotSayText) {
-      parts.push("DO_NOT_SAY (כללים מחייבים):\n" + doNotSayText);
+  const buildNextInstructions = () => {
+    const callerDigits = ensureCallerDigits();
+    const spacedCaller = callerDigits ? formatSpacedDigits(callerDigits) : "";
+    if (flowState.stage === "routing_clarify") {
+      return buildFlowInstructions(
+        "כדי לעזור במדויק—זה לגבי התעניינות במוצר, שירות/תקלה/אחריות, משלוח/אספקה, או להשאיר הודעה למישהו מהצוות?"
+      );
     }
-
-    if (matchFacts.length && !isCouponQuery) {
-      parts.push(
-        "עובדות רלוונטיות מהשיטס (להשתמש רק אם מתאים לשאלה):\n" +
-          matchFacts.join("\n")
+    if (flowState.stage === "sales_product") {
+      return buildFlowInstructions(
+        "בשמחה. על איזה מוצר אתם מתעניינים? תגידו לי בבקשה: סוג מוצר, ואם יש—דגם ו־שם מותג."
       );
     }
-
-    // Sales coupon: if the caller mentions coupon-related words, include the coupon code
-    try {
-      const couponKeywords = ["קופון", "קוד קופון", "קוד הנחה", "הנחה"];
-      if (couponKeywords.some((kw) => low.includes(kw))) {
-        const coupon = String(getSetting("SALES_COUPON_CODE", "")).trim();
-      if (coupon) {
-          // Insert spaces between digits to ensure the assistant reads them one by one
-          const spacedCoupon = coupon.replace(/\D/g, "").split("").join(" ");
-          parts.push(
-            `לרכישה באתר, ניתן להשתמש בקוד קופון ${spacedCoupon}. אל תמציא קוד אחר.`
-          );
-        }
-      }
-    } catch (_) {}
-
-    if (route === "delivery") {
-      // Include the delivery prompt from the sheet when available
-      try {
-        const dp = (SHEETS.prompts || {}).DELIVERY_PROMPT || "";
-        if (dp) parts.push(String(dp).trim());
-      } catch (_) {}
-      parts.push(mustNotLieDelivery);
-      if (afterHours) {
-        // Use KB fact row for after-hours same-day delivery queries
-        parts.push(
-          "מבינה. אם האספקה תואמה להיום לאחר שעות הפעילות – אוכל למסור לכם את מספר המוביל."
-        );
-        if (carrierPhones.length) {
-          parts.push(
-            "מספרי מובילים: " +
-              carrierPhones.join(", ") +
-              ". אל תמציאי מספרים או שמות מובילים שלא קיימים."
-          );
-          // After giving the numbers, instruct to ask if the caller wants to leave a message
-          parts.push(
-            "לאחר מתן מספרי המובילים, שאלי אם תרצו שאעביר קריאה למשרד. אם כן, בקשי שם מלא ומספר טלפון כנדרש; אם לא – ניתן לסיים את השיחה, אך עדיין לשלוח webhook עם הערת סיכום על אספקה להיום."
-          );
-        } else {
-          // fallback when no carrier phones available
-          parts.push(
-            "אין לי מספר מוביל זמין כרגע, אוכל להעביר בקשה לחזרה. אל תמציאי מספרים."
-          );
-          parts.push(
-            "שאלי אם תרצו שאעביר הודעה למשרד. אם כן, קחי שם מלא ומספר טלפון כנדרש; אם לא – ניתן לסיים את השיחה, אך עדיין לשלוח webhook עם הערת סיכום על אספקה להיום."
-          );
-        }
-      } else {
-        parts.push(
-          "אם מבקשים סטטוס משלוח: להסביר שאין סטטוס בזמן אמת ולהציע להשאיר הודעה/פרטים לחזרה."
+    if (flowState.stage === "sales_name") {
+      return buildFlowInstructions("מעולה, תודה. כדי שנחזור אליכם—מה השם המלא שלכם?");
+    }
+    if (flowState.stage === "sales_phone_confirm") {
+      const text = spacedCaller
+        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
+        : "על איזה מספר טלפון נוח לחזור אליכם?";
+      return buildFlowInstructions(text);
+    }
+    if (flowState.stage === "sales_phone_collect") {
+      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
+    }
+    if (flowState.stage === "sales_phone_confirm_new") {
+      const spaced = formatSpacedDigits(flowState.data.callback_phone);
+      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
+    }
+    if (flowState.stage === "sales_done") {
+      flowState.finalEvent = "מתעניין";
+      flowState.finalSummary = "לקוח מתעניין ברכישת מוצר";
+      flowState.shouldHangup = true;
+      return buildFlowInstructions(
+        "מעולה. העברתי את הפרטים למחלקת המכירות, ויחזרו אליכם בהקדם. תודה רבה ויום טוב."
+      );
+    }
+    if (flowState.stage === "support_issue") {
+      return buildFlowInstructions(
+        "הבנתי. כדי שאעביר לשירות בצורה מדויקת—מה סוג התקלה ומה מהות התקלה בכמה מילים?"
+      );
+    }
+    if (flowState.stage === "support_product") {
+      return buildFlowInstructions("ועל איזה מוצר זה? תגידו לי בבקשה דגם ו־שם מותג.");
+    }
+    if (flowState.stage === "support_name") {
+      return buildFlowInstructions("תודה. מה השם המלא שלכם?");
+    }
+    if (flowState.stage === "support_phone_confirm") {
+      const text = spacedCaller
+        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
+        : "על איזה מספר טלפון נוח לחזור אליכם?";
+      return buildFlowInstructions(text);
+    }
+    if (flowState.stage === "support_phone_collect") {
+      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
+    }
+    if (flowState.stage === "support_phone_confirm_new") {
+      const spaced = formatSpacedDigits(flowState.data.callback_phone);
+      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
+    }
+    if (flowState.stage === "support_done") {
+      flowState.finalEvent = "שירות לקוחות \\ תמיכה";
+      flowState.finalSummary = "לקוח מבקש שירות/תמיכה";
+      flowState.shouldHangup = true;
+      const importer = findExactImporter(flowState.data.product_brand);
+      const extra = [];
+      if (importer && importer.phone) {
+        const spaced = formatSpacedDigits(normalizePhoneDigits(importer.phone));
+        extra.push(
+          `תגידי לפני הסיום: רק לידיעה—המספר של היבואן עבור המותג ${importer.brand} הוא: ${spaced}.`
         );
       }
-    } else if (route === "support") {
-      // Include the support prompt from the sheet when available
-      try {
-        const sp = (SHEETS.prompts || {}).SUPPORT_PROMPT || "";
-        if (sp) parts.push(String(sp).trim());
-      } catch (_) {}
-      parts.push(
-        "מטרה: להבין תקלה בקצרה, פרטי מוצר/מותג/הזמנה, ולסגור עם הבטחה לחזרה."
+      return buildFlowInstructions(
+        "מעולה. שלחתי את הפרטים למחלקת השירות, ויחזרו אליכם בהקדם. תודה רבה ויום טוב.",
+        extra
       );
-      // Detect brand keywords in the caller's utterance and provide importer phone numbers when appropriate.
-      try {
-        const importerRows = Array.isArray(SHEETS.suppliersImporters)
-          ? SHEETS.suppliersImporters
-          : [];
-        const brandPhones = [];
-        for (const r of importerRows) {
-          const kw = String(r.brand_keyword || "").toLowerCase().trim();
-          if (!kw) continue;
-          if (low.includes(kw)) {
-            const when = String(r.when_to_give || "").toLowerCase();
-            // Only give phone when the sheet instructs to do so (e.g. contains "fault" or "תקלה")
-            if (
-              (when && when.includes("fault")) ||
-              when.includes("fault_or_specific_request") ||
-              when.includes("תקלה")
-            ) {
-              let p = String(r.phone_e164 || r.phone || "").trim();
-              if (p) {
-                // Remove whitespace and avoid scientific notation
-                p = p.replace(/\s+/g, "");
-                brandPhones.push(p);
-              }
-            }
-          }
-        }
-        if (brandPhones.length) {
-          parts.push(
-            "מספרי יבואנים למותג התקלה: " +
-              brandPhones.join(", ") +
-              ". אל תמציא/י מספרים או שמות יבואנים."
-          );
-        }
-      } catch (_) {
-        /* ignore brand detection errors */
-      }
-    } else if (route === "sales") {
-      // Include the sales prompt from the sheet when available
-      try {
-        const sp = (SHEETS.prompts || {}).SALES_PROMPT || "";
-        if (sp) parts.push(String(sp).trim());
-      } catch (_) {}
-      parts.push(
-        "מטרה: להבין במה מתעניינים (סוג מוצר/דגם/מותג) ואז לקחת פרטי חזרה (אפשר להציע להשתמש במספר המזוהה)."
+    }
+    if (flowState.stage === "delivery_name") {
+      flowState.afterHours = isAfterHours();
+      const carriers = flowState.afterHours ? buildCarrierList() : [];
+      const afterHoursText =
+        flowState.afterHours && carriers.length
+          ? `כרגע אנחנו מחוץ לשעות הפעילות. כדי לעזור כבר עכשיו—אלו מספרי הטלפון של המובילים: ${carriers.join(
+              ", "
+            )}.`
+          : flowState.afterHours
+          ? "כרגע אנחנו מחוץ לשעות הפעילות. אין לי כרגע מספרי מובילים זמינים מהטבלה."
+          : "";
+      const intro = "הבנתי. זה לגבי משלוח/אספקה. אני אקח כמה פרטים ואעביר למחלקת אספקה.";
+      const askName = "מה השם המלא שלכם?";
+      return buildFlowInstructions(
+        [intro, afterHoursText, askName].filter(Boolean).join(" ")
       );
-    } else {
-      // Unknown route: include message-to-manager prompt if available and ask clarifying question
-      try {
-        const mp = (SHEETS.prompts || {}).MESSAGE_TO_MANAGER_PROMPT || "";
-        if (mp) parts.push(String(mp).trim());
-      } catch (_) {}
-      parts.push("אם לא ברור, תשאלי שאלה אחת להבהרה: מכירה / שירות / משלוח.");
     }
+    if (flowState.stage === "delivery_phone_confirm") {
+      const text = spacedCaller
+        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
+        : "על איזה מספר טלפון נוח לחזור אליכם?";
+      return buildFlowInstructions(text);
+    }
+    if (flowState.stage === "delivery_phone_collect") {
+      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
+    }
+    if (flowState.stage === "delivery_phone_confirm_new") {
+      const spaced = formatSpacedDigits(flowState.data.callback_phone);
+      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
+    }
+    if (flowState.stage === "delivery_done") {
+      flowState.finalEvent = "שירות לקוחות \\ אספקה";
+      flowState.finalSummary = "לקוח רוצה לברר לגבי משלוח/אספקה";
+      flowState.shouldHangup = true;
+      return buildFlowInstructions(
+        "תודה. העברתי את הפרטים למחלקת אספקה, ויחזרו אליכם בהקדם. יום טוב."
+      );
+    }
+    if (flowState.stage === "message_target") {
+      return buildFlowInstructions("בשמחה. למי מיועדת ההודעה? (שם עובד/מנהל)");
+    }
+    if (flowState.stage === "message_name") {
+      return buildFlowInstructions("מה השם המלא שלכם?");
+    }
+    if (flowState.stage === "message_body") {
+      return buildFlowInstructions("מה מהות ההודעה? תאמרו/תאמרי את זה בקצרה.");
+    }
+    if (flowState.stage === "message_phone_confirm") {
+      const text = spacedCaller
+        ? `האם לחזור אליכם למספר הזה: ${spacedCaller} ?`
+        : "על איזה מספר טלפון נוח לחזור אליכם?";
+      return buildFlowInstructions(text);
+    }
+    if (flowState.stage === "message_phone_collect") {
+      return buildFlowInstructions("אין בעיה, תגידו לי בבקשה את מספר הטלפון לחזרה.");
+    }
+    if (flowState.stage === "message_phone_confirm_new") {
+      const spaced = formatSpacedDigits(flowState.data.callback_phone);
+      return buildFlowInstructions(`רק לוודא—המספר לחזרה הוא: ${spaced}. נכון?`);
+    }
+    if (flowState.stage === "message_done") {
+      flowState.finalEvent = "הודעה כללית";
+      flowState.finalSummary = `הודעה עבור ${flowState.data.message_target || "הצוות"}`;
+      flowState.shouldHangup = true;
+      return buildFlowInstructions(
+        `תודה. העברתי את ההודעה ל־${flowState.data.message_target} ויחזרו אליכם בהקדם. יום טוב.`
+      );
+    }
+    return "";
+  };
 
-    let inst = parts.join("\n\n");
-
-    const phone = extractPhoneCandidates(t);
-    if (phone) inst += `זוהה מספר בטקסט: ${phone}. אל תחזרי עליו אם לא צריך.\n`;
-
-    return inst.trim();
+  const processCallerUtterance = (utterance) => {
+    const text = String(utterance || "").trim();
+    if (!text) return "";
+    if (flowState.stage === "routing" || flowState.stage === "routing_clarify") {
+      handleRouting(text);
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "sales_product") {
+      flowState.data.product_interest = text;
+      const { brand, model } = extractBrandModel(text);
+      if (brand) flowState.data.product_brand = brand;
+      if (model) flowState.data.product_model = model;
+      flowState.stage = "sales_name";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "sales_name") {
+      flowState.data.full_name = text;
+      flowState.stage = "sales_phone_confirm";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "sales_phone_confirm") {
+      if (isYes(text)) {
+        flowState.data.callback_phone = ensureCallerDigits();
+        flowState.stage = "sales_done";
+        return buildNextInstructions();
+      }
+      if (isNo(text) || !ensureCallerDigits()) {
+        flowState.stage = "sales_phone_collect";
+        return buildNextInstructions();
+      }
+      flowState.stage = "sales_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "sales_phone_collect") {
+      const digits = extractPhoneCandidates(text);
+      if (!isValidPhoneDigits(digits)) {
+        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
+      }
+      flowState.data.callback_phone = digits;
+      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
+      flowState.stage = "sales_phone_confirm_new";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "sales_phone_confirm_new") {
+      if (isYes(text)) {
+        flowState.stage = "sales_done";
+        return buildNextInstructions();
+      }
+      flowState.stage = "sales_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "support_issue") {
+      flowState.data.issue_type = text;
+      flowState.data.issue_desc = text;
+      flowState.stage = "support_product";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "support_product") {
+      const { brand, model } = extractBrandModel(text);
+      flowState.data.product_brand = brand || flowState.data.product_brand;
+      flowState.data.product_model = model || flowState.data.product_model;
+      flowState.stage = "support_name";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "support_name") {
+      flowState.data.full_name = text;
+      flowState.stage = "support_phone_confirm";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "support_phone_confirm") {
+      if (isYes(text)) {
+        flowState.data.callback_phone = ensureCallerDigits();
+        flowState.stage = "support_done";
+        return buildNextInstructions();
+      }
+      if (isNo(text) || !ensureCallerDigits()) {
+        flowState.stage = "support_phone_collect";
+        return buildNextInstructions();
+      }
+      flowState.stage = "support_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "support_phone_collect") {
+      const digits = extractPhoneCandidates(text);
+      if (!isValidPhoneDigits(digits)) {
+        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
+      }
+      flowState.data.callback_phone = digits;
+      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
+      flowState.stage = "support_phone_confirm_new";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "support_phone_confirm_new") {
+      if (isYes(text)) {
+        flowState.stage = "support_done";
+        return buildNextInstructions();
+      }
+      flowState.stage = "support_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "delivery_name") {
+      flowState.data.full_name = text;
+      flowState.stage = "delivery_phone_confirm";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "delivery_phone_confirm") {
+      if (isYes(text)) {
+        flowState.data.callback_phone = ensureCallerDigits();
+        flowState.stage = "delivery_done";
+        return buildNextInstructions();
+      }
+      if (isNo(text) || !ensureCallerDigits()) {
+        flowState.stage = "delivery_phone_collect";
+        return buildNextInstructions();
+      }
+      flowState.stage = "delivery_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "delivery_phone_collect") {
+      const digits = extractPhoneCandidates(text);
+      if (!isValidPhoneDigits(digits)) {
+        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
+      }
+      flowState.data.callback_phone = digits;
+      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
+      flowState.stage = "delivery_phone_confirm_new";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "delivery_phone_confirm_new") {
+      if (isYes(text)) {
+        flowState.stage = "delivery_done";
+        return buildNextInstructions();
+      }
+      flowState.stage = "delivery_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "message_target") {
+      flowState.data.message_target = text;
+      flowState.stage = "message_name";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "message_name") {
+      flowState.data.full_name = text;
+      flowState.stage = "message_body";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "message_body") {
+      flowState.data.message_body = text;
+      flowState.stage = "message_phone_confirm";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "message_phone_confirm") {
+      if (isYes(text)) {
+        flowState.data.callback_phone = ensureCallerDigits();
+        flowState.stage = "message_done";
+        return buildNextInstructions();
+      }
+      if (isNo(text) || !ensureCallerDigits()) {
+        flowState.stage = "message_phone_collect";
+        return buildNextInstructions();
+      }
+      flowState.stage = "message_phone_collect";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "message_phone_collect") {
+      const digits = extractPhoneCandidates(text);
+      if (!isValidPhoneDigits(digits)) {
+        return buildFlowInstructions("נראה שחסרה לי ספרה אחת, תוכלו להגיד שוב את המספר לאט?");
+      }
+      flowState.data.callback_phone = digits;
+      if (!recognizedPhones.includes(digits)) recognizedPhones.push(digits);
+      flowState.stage = "message_phone_confirm_new";
+      return buildNextInstructions();
+    }
+    if (flowState.stage === "message_phone_confirm_new") {
+      if (isYes(text)) {
+        flowState.stage = "message_done";
+        return buildNextInstructions();
+      }
+      flowState.stage = "message_phone_collect";
+      return buildNextInstructions();
+    }
+    return "";
   };
 
   /**
    * Normalize an input transcript for duplicate detection.
    * This helper removes common greetings and filler words,
    * strips punctuation and extra whitespace, and lowercases the result.
    * It allows us to compare two caller utterances for semantic equality
    * even if they differ slightly in casing or punctuation. We define
    * greetings that should not trigger a new response (e.g. "היי", "שלום", "ביי").
    */
   const normalizeTranscript = (s) => {
     try {
       let t = String(s || "").toLowerCase();
       // Remove punctuation
       t = t.replace(/[\.,!?\-–—;:'"\u05be]/g, " ");
       // Replace multiple spaces
       t = t.replace(/\s+/g, " ").trim();
       // Remove common greetings and filler words at start or end
       const greetings = [
         "היי",
         "הי",
         "שלום",
         "ביי",
         "היי שלום",
         "היי, שלום",
@@ -940,62 +1163,77 @@ wss.on("connection", (twilioWs, req) => {
         "ביי, שלום",
         "אה",
         "אה, שלום",
         "אה שלום",
         "אה, שלום לך",
         "אה שלום לך",
         // English greetings and farewells to ignore
         "hi",
         "hello",
         "bye",
         "bye-bye",
         "bye bye",
         "bye, bye"
       ];
       // Remove greeting phrases from beginning
       for (const g of greetings) {
         if (t.startsWith(g + " ")) t = t.slice(g.length).trim();
         if (t === g) return "";
       }
       return t;
     } catch (_) {
       return String(s || "").trim().toLowerCase();
     }
   };
 
+  const isFillerOnly = (normalized) => {
+    const fillerPhrases = [
+      "תודה",
+      "תודה רבה",
+      "כן",
+      "סבבה",
+      "בבקשה",
+      "בסדר",
+      "תודה על הקופון"
+    ];
+    return fillerPhrases.some(
+      (fp) => normalized === fp || normalized.startsWith(fp + " ") || normalized.endsWith(" " + fp)
+    );
+  };
+
   // Keep track of normalized caller utterances to prevent duplicate responses
   let lastCallerNormalized = "";
   let lastRequestedCallerNormalized = "";
 
   const printCallerFinal = (text) => {
     const t = String(text || "").trim();
     if (!t) return;
     if (t === lastCallerFinal) return;
     lastCallerFinal = t;
     pushTurn("caller", t);
-    // Update proxy instructions for next response (Option B)
-    proxyInstructions = buildProxyInstructions(t);
+    // Update proxy instructions for next response (flow-driven)
+    proxyInstructions = processCallerUtterance(t);
     always(`[CALLER][${connTag}]`, t);
   };
 
   const printBotFinal = (text) => {
     const t = String(text || "").trim();
     if (!t) return;
     if (t === lastBotFinal) return;
     lastBotFinal = t;
     pushTurn("bot", t);
     always(`[BOT][${connTag}]`, t);
   };
 
   // NOTE: declare openaiWs variable early so closures can reference safely
   let openaiWs = null;
 
   const safeOpenAISend = (obj) => {
     try {
       if (openaiWs && openaiWs.readyState === WebSocket.OPEN) {
         openaiWs.send(JSON.stringify(obj));
         return true;
       }
       return false;
     } catch (e) {
       error("OpenAI send failed", e.message);
       return false;
@@ -1040,51 +1278,51 @@ wss.on("connection", (twilioWs, req) => {
 
   const requestAssistantResponse = (reason = "") => {
     // Only send a response if the OpenAI WS is ready
     if (!openaiWs || openaiWs.readyState !== WebSocket.OPEN) return;
 
     // Don't start a new response while another is in flight
     if (awaitingResponse) {
       debug(`[${connTag}] response.request ignored (awaitingResponse=true) reason=${reason}`);
       return;
     }
 
     // Reset flags: we're starting a new response now
     awaitingResponse = true;
     pendingResponseRequest = false;
 
     // Mark that we've responded to the most recent caller utterance
     lastRequestedCallerFinal = lastCallerFinal;
     lastRequestedCallerNormalized = lastCallerNormalized;
 
     // Build dynamic instructions for this turn. If proxyInstructions is empty,
     // fall back to master prompt. We rebuild instructions here because the
     // caller may have asked a new question that requires updated context.
     let instructions = proxyInstructions;
     if (!instructions) {
       try {
-        instructions = buildProxyInstructions(lastCallerFinal);
+        instructions = buildNextInstructions();
       } catch (_) {
         instructions = getPrompt(
           "MASTER_PROMPT",
           "אתם עוזרת קולית בשם נטע עבור גיל ספורט. דברו קצר, קליל וברור."
         );
       }
     }
 
     debug(`[${connTag}] response.create (reason=${reason})`);
     safeOpenAISend({
       type: "response.create",
       response: {
         modalities: ["audio", "text"],
         instructions
       }
     });
   };
 
   debug(`[${connTag}] Creating OpenAI WS... model=${OPENAI_REALTIME_MODEL} voice=${OPENAI_VOICE}`);
 
   openaiWs = new WebSocket(
     `wss://api.openai.com/v1/realtime?model=${OPENAI_REALTIME_MODEL}`,
     {
       headers: {
         Authorization: `Bearer ${OPENAI_API_KEY}`,
@@ -1239,160 +1477,158 @@ wss.on("connection", (twilioWs, req) => {
         msg.text ||
         msg?.item?.content?.[0]?.transcript ||
         msg?.item?.content?.[0]?.text ||
         "";
       const isInputTranscript =
         type.includes("input_audio_transcription") ||
         type.includes("input_audio_transcript") ||
         type.includes("conversation.item.input_audio_transcription");
       if (doneLike && isInputTranscript && possible) {
         const utterance = String(possible).trim();
         // Normalize the utterance to remove greetings/punctuation for duplicate detection
         const normalized = normalizeTranscript(utterance);
         // Count meaningful words in the normalized text
         const wordCount = normalized.split(/\s+/).filter(Boolean).length;
         // If we are currently flushing buffered audio, ignore any transcripts generated
         // by the flush. These partial echoes of prior audio should not trigger new responses.
         if (isFlushingBufferedAudio) {
           // still update lastCallerNormalized for diagnostics, but do not queue a response
           lastCallerNormalized = normalized;
           lastCallerFinal = utterance;
           return;
         }
         printCallerFinal(utterance);
         // Update latest normalized utterance
         lastCallerNormalized = normalized;
-        // Only queue a response when the normalized utterance has at least two words,
-        // and we haven't responded to a semantically equivalent utterance yet
         // Determine if the utterance contains any meaningful keywords (for routing)
         const keywordList = [
           "קופון",
           "תקלה",
           "בעיה",
           "שירות",
           "החלפה",
           "החזרה",
           "לא עובד",
           "משלוח",
           "אספקה",
           "שליח",
           "הזמנה",
           "הגיע",
           "לא הגיע",
           "מוביל",
           "מחיר",
           "לקנות",
           "רכישה",
           "מוצר",
           "דגם",
           "מידה",
           "צבע",
           "מלאי",
           "מבצע",
           "קנייה",
           "קניה"
         ];
         const hasKeyword = keywordList.some((kw) => normalized.includes(kw));
         if (normalized) {
           let isDup = false;
           // Consider it a duplicate if the new normalized utterance is identical to the
           // last one we responded to, or one contains the other. This avoids
           // triggering multiple responses for slight transcription differences.
           if (lastRequestedCallerNormalized) {
             if (normalized === lastRequestedCallerNormalized) {
               isDup = true;
             } else if (normalized.startsWith(lastRequestedCallerNormalized)) {
               isDup = true;
             } else if (lastRequestedCallerNormalized.startsWith(normalized)) {
               isDup = true;
             }
           }
-          // Also treat as duplicate if the utterance contains only filler acknowledgements
-          const fillerPhrases = [
-            "תודה",
-            "תודה רבה",
-            "כן",
-            "סבבה",
-            "בבקשה",
-            "תודה על הקופון",
-            "בסדר"
-          ];
+          const allowShortReply =
+            flowState && flowState.stage && !String(flowState.stage).endsWith("done");
+          const hasPhone = Boolean(extractPhoneCandidates(normalized));
+          const meaningfulShort =
+            isYes(normalized) || isNo(normalized) || hasPhone || (normalized && !isFillerOnly(normalized));
           if (!isDup) {
-            for (const fp of fillerPhrases) {
-              if (normalized === fp || normalized.startsWith(fp + " ") || normalized.endsWith(" " + fp)) {
-                isDup = true;
-                break;
-              }
-            }
-          }
-          if (!isDup && (wordCount >= 4 || hasKeyword)) {
-            // require at least 5 meaningful words or a keyword to trigger a new response
-            if (wordCount >= 5 || hasKeyword) {
+            if (allowShortReply) {
+              if (meaningfulShort) pendingResponseRequest = true;
+            } else if (wordCount >= 5 || hasKeyword) {
               pendingResponseRequest = true;
             }
           }
         }
         return;
       }
     }
 
     // -----------------------------
     // Turn boundary events
     // -----------------------------
     if (msg.type === "input_audio_buffer.speech_stopped") {
       // Ignore speech_stopped events for response timing. We'll respond after
       // the assistant finishes speaking (response.done) based on pendingResponseRequest.
       return;
     }
 
     // response lifecycle
     if (msg.type === "response.done") {
       awaitingResponse = false;
       // Flush any buffered audio frames that arrived while assistant was speaking.
       // We mark that we are flushing so that any resulting transcriptions do not
       // trigger a new response inadvertently.
       if (Array.isArray(pausedAudioBuffer) && pausedAudioBuffer.length > 0) {
         isFlushingBufferedAudio = true;
         while (pausedAudioBuffer.length > 0) {
           const audioFrame = pausedAudioBuffer.shift();
           safeOpenAISend({ type: "input_audio_buffer.append", audio: audioFrame });
         }
         // give OpenAI some time to process the flush before accepting new transcripts
         // we don't await here, but we reset the flag on the next tick
         setTimeout(() => {
           isFlushingBufferedAudio = false;
         }, 50);
       }
       // If we have a pending caller utterance, and we haven't already
       // responded to it, send one response now. We rely on the check
       // lastCallerFinal !== lastRequestedCallerFinal to ensure we respond
       // exactly once per caller utterance.
       if (pendingResponseRequest && lastCallerFinal !== lastRequestedCallerFinal) {
         debug(`[${connTag}] response.done -> draining pendingResponseRequest`);
         pendingResponseRequest = false;
         requestAssistantResponse("pending_after_done");
       }
+      if (flowState.shouldHangup && !sentCallEnded && flowState.finalEvent) {
+        sentCallEnded = true;
+        endedAt = endedAt || nowIso();
+        const payload = buildFinalPayload();
+        sendWebhookEvent(flowState.finalEvent, payload, { wait_for_recording: true });
+        try {
+          if (openaiWs) openaiWs.close();
+        } catch (_) {}
+        try {
+          if (twilioWs) twilioWs.close();
+        } catch (_) {}
+      }
       return;
     }
 
     // AUDIO back to Twilio
     if (msg.type === "response.audio.delta") {
       if (!twilioStreamSid) return;
 
       safeTwilioSend({
         event: "media",
         streamSid: twilioStreamSid,
         media: { payload: msg.delta || "" }
       });
       return;
     }
   });
 
   twilioWs.on("message", async (data) => {
     let msg;
     try {
       msg = JSON.parse(data.toString());
     } catch (e) {
       error(`[${connTag}] Twilio message JSON parse failed`, e.message);
       return;
     }
 
@@ -1431,82 +1667,78 @@ wss.on("connection", (twilioWs, req) => {
         pendingAudio.push(payload);
         if (pendingAudio.length > 400) pendingAudio.splice(0, pendingAudio.length - 400);
         return;
       }
       // If the assistant is currently speaking, buffer the audio instead of
       // sending it immediately. This prevents the model from listening during
       // its own response.
       if (awaitingResponse) {
         pausedAudioBuffer.push(payload);
         // cap buffer to avoid unbounded growth
         if (pausedAudioBuffer.length > 400) {
           pausedAudioBuffer.splice(0, pausedAudioBuffer.length - 400);
         }
         return;
       }
       safeOpenAISend({
         type: "input_audio_buffer.append",
         audio: payload
       });
       return;
     }
 
     if (msg.event === "stop") {
       always(`[TWILIO_STOP][${connTag}]`, "stream stopped");
       endedAt = nowIso();
-      const recording_url_public = makeRecordingPublicUrl(callSid);
 
       if (!sentCallEnded) {
         sentCallEnded = true;
-        // ONE final webhook (by route when possible) - always wait for recording (best effort)
-        const finalEvent =
+        const fallbackEvent =
           route === "sales"
             ? "sales_lead"
             : route === "support"
             ? "support_ticket"
             : route === "delivery"
             ? "delivery_after_hours"
             : route === "message"
             ? "message_taken"
             : "call_ended";
-        await sendWebhookEvent(
-          finalEvent,
-          {
-            callSid,
-            streamSid: twilioStreamSid,
-            caller,
-            called,
-            started_at: startedAt,
-            ended_at: endedAt,
-            language,
-            route,
-            caller_last_utterance: lastCallerFinal,
-            bot_last_utterance: lastBotFinal,
-            transcript: transcriptTurns
-          },
-          { wait_for_recording: true }
-        );
+        const finalEvent = flowState.finalEvent || fallbackEvent;
+        const payload = flowState.finalEvent ? buildFinalPayload() : {
+          callSid,
+          streamSid: twilioStreamSid,
+          caller,
+          called,
+          started_at: startedAt,
+          ended_at: endedAt,
+          language,
+          route,
+          caller_last_utterance: lastCallerFinal,
+          bot_last_utterance: lastBotFinal,
+          transcript: transcriptTurns
+        };
+        await sendWebhookEvent(finalEvent, payload, { wait_for_recording: true });
       }
       try {
         if (openaiWs) openaiWs.close();
       } catch (_) {}
       return;
     }
   });
 
   twilioWs.on("error", (e) => {
     RUNTIME.ws_errors += 1;
     error(`[${connTag}] Twilio websocket error`, e?.message || e);
     try {
       if (openaiWs) openaiWs.close();
     } catch (_) {}
   });
 
   twilioWs.on("close", () => {
     RUNTIME.ws_closed += 1;
     RUNTIME.last_ws_close_at = new Date().toISOString();
     always(`[TWILIO_CLOSE][${connTag}]`, "socket closed");
     // If socket closed unexpectedly and we never sent call_ended -> abandoned
     if (!sentCallEnded && !sentCallAbandoned) {
       sentCallAbandoned = true;
       endedAt = endedAt || nowIso();
       const recording_url_public = makeRecordingPublicUrl(callSid);
 
EOF
)
