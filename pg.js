"use strict";

const CACHE = new Map();
const DEFAULT_TTL_MS = 10 * 60 * 1000;

function safeStr(x) {
  if (x === undefined || x === null) return "";
  return String(x).trim();
}

function nowMs() {
  return Date.now();
}

function applyTemplate(tpl, vars) {
  const s = safeStr(tpl);
  if (!s) return "";

  return s.replace(/\{([A-Za-z0-9_]+)\}/g, (_, rawKey) => {
    const key = String(rawKey || "");
    if (!key) return "";

    const direct = vars?.[key];
    if (direct !== undefined && direct !== null) return String(direct);

    const upper = vars?.[key.toUpperCase()];
    if (upper !== undefined && upper !== null) return String(upper);

    const lower = vars?.[key.toLowerCase()];
    if (lower !== undefined && lower !== null) return String(lower);

    return "";
  });
}

function computeGreetingHebrew(timeZone) {
  const tz = timeZone || "Asia/Jerusalem";
  const hourStr = new Intl.DateTimeFormat("en-US", {
    timeZone: tz,
    hour: "2-digit",
    hour12: false,
  }).format(new Date());

  const hour = Number(hourStr);
  if (Number.isNaN(hour)) return "שלום";
  if (hour >= 5 && hour < 11) return "בוקר טוב";
  if (hour >= 11 && hour < 17) return "צהריים טובים";
  if (hour >= 17 && hour < 22) return "ערב טוב";
  return "לילה טוב";
}

function buildOpeningKey({ settings, callerName, isReturning, greeting }) {
  return JSON.stringify({
    opening: safeStr(settings?.OPENING_SCRIPT),
    openingReturning: safeStr(settings?.OPENING_SCRIPT_RETURNING),
    businessName: safeStr(settings?.BUSINESS_NAME),
    botName: safeStr(settings?.BOT_NAME),
    mainPhone: safeStr(settings?.MAIN_PHONE),
    email: safeStr(settings?.BUSINESS_EMAIL),
    address: safeStr(settings?.BUSINESS_ADDRESS),
    hours: safeStr(settings?.WORKING_HOURS),
    website: safeStr(settings?.BUSINESS_WEBSITE_URL),
    callerName: safeStr(callerName),
    isReturning: !!isReturning,
    greeting: safeStr(greeting),
  });
}

function getOpeningScriptFromSSOT(ssot, vars) {
  const settings = ssot?.settings || {};
  const isReturning =
    Boolean(vars?.RETURNING_CALLER) || Boolean(vars?.returning_caller);

  const tpl =
    (isReturning && safeStr(settings.OPENING_SCRIPT_RETURNING)) ||
    safeStr(settings.OPENING_SCRIPT) ||
    "שלום! איך נוכל לעזור?";

  const merged = {
    BUSINESS_NAME: safeStr(settings.BUSINESS_NAME),
    BOT_NAME: safeStr(settings.BOT_NAME),
    CALLER_NAME: safeStr(vars?.CALLER_NAME),
    DISPLAY_NAME: safeStr(vars?.CALLER_NAME),
    display_name: safeStr(vars?.CALLER_NAME),
    MAIN_PHONE: safeStr(settings.MAIN_PHONE),
    BUSINESS_EMAIL: safeStr(settings.BUSINESS_EMAIL),
    BUSINESS_ADDRESS: safeStr(settings.BUSINESS_ADDRESS),
    WORKING_HOURS: safeStr(settings.WORKING_HOURS),
    BUSINESS_WEBSITE_URL: safeStr(settings.BUSINESS_WEBSITE_URL),
    VOICE_NAME: safeStr(settings.VOICE_NAME),
    GREETING: safeStr(vars?.GREETING),
    ...vars,
  };

  const filled = applyTemplate(tpl, merged)
    .replace(/\s{2,}/g, " ")
    .replace(/\s+,/g, ",")
    .replace(/,\s+,/g, ",")
    .trim();

  return filled || "שלום! איך נוכל לעזור?";
}

function warmOpeningCache({
  ssot,
  callerName,
  isReturning,
  timeZone,
  ttlMs = DEFAULT_TTL_MS,
}) {
  return getCachedOpening({
    ssot,
    callerName,
    isReturning,
    timeZone,
    ttlMs,
  });
}

function getCachedOpening({
  ssot,
  callerName,
  isReturning,
  timeZone,
  ttlMs = DEFAULT_TTL_MS,
}) {
  const greeting = computeGreetingHebrew(timeZone);
  const key = buildOpeningKey({
    settings: ssot?.settings,
    callerName,
    isReturning,
    greeting,
  });

  const cached = CACHE.get(key);
  const now = nowMs();

  if (cached && cached.expiresAt > now) {
    return { ...cached.value, cache_hit: true };
  }

  const opening = getOpeningScriptFromSSOT(ssot, {
    GREETING: greeting,
    CALLER_NAME: safeStr(callerName),
    DISPLAY_NAME: safeStr(callerName),
    display_name: safeStr(callerName),
    returning_caller: !!isReturning,
    RETURNING_CALLER: !!isReturning,
  });

  const value = {
    opening,
    greeting,
    cache_hit: false,
  };

  CACHE.set(key, {
    value,
    expiresAt: now + Math.max(1000, Number(ttlMs) || DEFAULT_TTL_MS),
  });

  return value;
}

module.exports = {
  computeGreetingHebrew,
  getOpeningScriptFromSSOT,
  getCachedOpening,
  warmOpeningCache,
};
