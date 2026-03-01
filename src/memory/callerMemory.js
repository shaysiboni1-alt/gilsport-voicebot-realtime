const { Pool } = require('pg');
const { logger } = require('../utils/logger');

// Caller Memory (Postgres)
// Goals:
// - Never break the runtime if DB is missing/unavailable.
// - Stable API: ensureCallerMemorySchema / getCallerProfile / upsertCallerProfile
// - Use short timeouts so DB work never blocks call flow.

const DEFAULT_TIMEOUT_MS = 1500;

let pool = null;

function hasDb() {
  return Boolean(process.env.DATABASE_URL && String(process.env.DATABASE_URL).trim());
}

function getPool() {
  if (!hasDb()) return null;
  if (pool) return pool;

  // Render Postgres usually requires SSL; local dev often doesn't.
  const ssl = process.env.PGSSLMODE === 'disable'
    ? false
    : { rejectUnauthorized: false };

  pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl,
    max: 3,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 2_000,
  });

  pool.on('error', (err) => {
    logger.warn('Caller memory pool error', { error: String(err?.message || err) });
  });

  return pool;
}

async function withTimeout(promise, ms = DEFAULT_TIMEOUT_MS) {
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(`timeout_after_${ms}ms`)), ms);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(t);
  }
}

async function ensureCallerMemorySchema() {
  const p = getPool();
  if (!p) return;

  // If a previous deploy created a different schema (common during iteration),
  // we'll detect it and reset the table. Caller memory is a cache, so this is safe.
  try {
    const { rows } = await withTimeout(
      p.query(
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'caller_profiles'`
      ),
      2_000
    );

    const cols = new Set((rows || []).map(r => String(r.column_name || '').toLowerCase()));
    if (cols.size > 0 && !cols.has('caller_id')) {
      logger.warn('Caller memory schema mismatch (missing caller_id); dropping caller_profiles', {
        columns: Array.from(cols).sort(),
      });
      await withTimeout(p.query('DROP TABLE IF EXISTS caller_profiles;'), 2_000);
    }
  } catch (e) {
    // If probing fails, don't block call flow.
    logger.warn('Caller memory schema probe failed', { error: String(e?.message || e) });
  }

  // Keep schema minimal, but also support in-place upgrades if an older
  // table exists (Render Postgres persists across deploys).
  const sql = `
    CREATE TABLE IF NOT EXISTS caller_profiles (
      caller_id TEXT PRIMARY KEY,
      display_name TEXT,
      total_calls INTEGER NOT NULL DEFAULT 0,
      first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_seen TIMESTAMPTZ,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS display_name TEXT;

    -- Ensure total_calls exists and is safe to use (older DBs may have NULLs)
    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS total_calls INTEGER;

    UPDATE caller_profiles
      SET total_calls = 0
      WHERE total_calls IS NULL;

    ALTER TABLE caller_profiles
      ALTER COLUMN total_calls SET DEFAULT 0;

    ALTER TABLE caller_profiles
      ALTER COLUMN total_calls SET NOT NULL;

    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS first_seen TIMESTAMPTZ;

    ALTER TABLE caller_profiles
      ALTER COLUMN first_seen SET DEFAULT NOW();

    UPDATE caller_profiles
      SET first_seen = COALESCE(first_seen, created_at, NOW())
      WHERE first_seen IS NULL;

    ALTER TABLE caller_profiles
      ALTER COLUMN first_seen SET NOT NULL;

    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS last_seen TIMESTAMPTZ;

    UPDATE caller_profiles
      SET last_seen = COALESCE(last_seen, updated_at, NOW())
      WHERE last_seen IS NULL;

    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS meta JSONB;

    ALTER TABLE caller_profiles
      ALTER COLUMN meta SET DEFAULT '{}'::jsonb;

    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;

    ALTER TABLE caller_profiles
      ALTER COLUMN created_at SET DEFAULT NOW();

    ALTER TABLE caller_profiles
      ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

    ALTER TABLE caller_profiles
      ALTER COLUMN updated_at SET DEFAULT NOW();

    CREATE INDEX IF NOT EXISTS caller_profiles_last_seen_idx
      ON caller_profiles(last_seen DESC);
  `;

  await withTimeout(p.query(sql), 3_000);
}

async function getCallerProfile(callerId) {
  const p = getPool();
  if (!p) return null;

  const cid = String(callerId || '').trim();
  if (!cid) return null;

  try {
    const { rows } = await withTimeout(
      p.query(
        `SELECT caller_id, display_name, total_calls, first_seen, last_seen, meta
         FROM caller_profiles
         WHERE caller_id = $1
         LIMIT 1`,
        [cid]
      )
    );

    if (!rows || rows.length === 0) return null;
    return rows[0];
  } catch (err) {
    logger.debug('Caller memory read failed', { error: String(err?.message || err) });
    return null;
  }
}

/**
 * Upsert profile.
 * @param {string} callerId
 * @param {{ display_name?: string|null, meta_patch?: object|null }} patch
 */
// Backward-compatible signature:
//   upsertCallerProfile(callerId: string, patch?: {display_name?, meta_patch?})
//   upsertCallerProfile(payload: { caller: string, display_name?, meta_patch?, ... })
// Older code paths (e.g., finalizePipeline) may call this with a single payload object.
async function upsertCallerProfile(callerId, patch = {}) {
  const p = getPool();
  if (!p) return false;

  // If called with a single object payload, extract caller + patch fields.
  let cidRaw = callerId;
  let patchObj = patch;
  if (callerId && typeof callerId === 'object' && !Array.isArray(callerId)) {
    const payload = callerId;
    cidRaw = payload.caller ?? payload.caller_id ?? payload.callerId;
    patchObj = {
      display_name: payload.display_name ?? payload.displayName ?? payload.full_name ?? payload.fullName ?? payload.name ?? null,
      meta_patch: (payload.meta_patch && typeof payload.meta_patch === 'object')
        ? payload.meta_patch
        : (payload.meta && typeof payload.meta === 'object')
          ? payload.meta
          : null,
    };
  }

  const cid = String(cidRaw || '').trim();
  if (!cid) return false;

  const displayNameRaw = (patchObj.display_name ?? patchObj.full_name ?? patchObj.fullName ?? patchObj.name ?? null);
  const displayName = sanitizeDisplayName(displayNameRaw);
  const metaPatch = (patchObj.meta_patch && typeof patchObj.meta_patch === 'object') ? patchObj.meta_patch : null;

  // jsonb merge: meta = meta || metaPatch
  const metaExpr = metaPatch ? 'caller_profiles.meta || $3::jsonb' : 'caller_profiles.meta';
  const params = metaPatch ? [cid, displayName, JSON.stringify(metaPatch)] : [cid, displayName];

  const sql = metaPatch
    ? `
      INSERT INTO caller_profiles (caller_id, display_name, total_calls, first_seen, last_seen, meta)
      VALUES ($1, $2, 1, NOW(), NOW(), $3::jsonb)
      ON CONFLICT (caller_id) DO UPDATE SET
        display_name = COALESCE(EXCLUDED.display_name, caller_profiles.display_name),
        total_calls = caller_profiles.total_calls + 1,
        last_seen = NOW(),
        meta = ${metaExpr},
        updated_at = NOW();
    `
    : `
      INSERT INTO caller_profiles (caller_id, display_name, total_calls, first_seen, last_seen)
      VALUES ($1, $2, 1, NOW(), NOW())
      ON CONFLICT (caller_id) DO UPDATE SET
        display_name = COALESCE(EXCLUDED.display_name, caller_profiles.display_name),
        total_calls = caller_profiles.total_calls + 1,
        last_seen = NOW(),
        updated_at = NOW();
    `;

  try {
    await withTimeout(p.query(sql, params));
    return true;
  } catch (err) {
    logger.debug('Caller memory write failed', { error: String(err?.message || err) });
    return false;
  }
}


async function updateCallerDisplayName(callerId, displayName, metaPatch = null) {
  const p = getPool();
  if (!p) return false;

  const cid = String(callerId || "").trim();
  let dn = String(displayName || "").trim();

  if (!cid) return false;
  if (!dn) return false;

  // Hard guardrails (anti-hallucination / safety)
  dn = dn.replace(/[\u200e\u200f\u202a-\u202e]/g, "").trim();
  if (dn.length < 2 || dn.length > 40) return false;
  if (/\d/.test(dn)) return false;
  if (!/[\p{Script=Hebrew}\p{Script=Latin}]/u.test(dn)) return false;
  if (!/^[\p{Script=Hebrew}\p{Script=Latin}\s'\-\.]{2,40}$/u.test(dn)) return false;

  const dnWords = dn.split(/\s+/).filter(Boolean);
  const dnIsFull = dnWords.length >= 2;

  const isClearlyInvalidStoredName = (name) => {
    const n = String(name || "").trim();
    if (!n) return true;
    const BAD = new Set(["לא", "כן", "אוקיי", "אוקי", "שלום", "היי", "הי", "תודה"]);
    if (BAD.has(n)) return true;
    // Common Hebrew preposition+"ה" prefix (e.g. "בהליכון")
    if (n.length >= 4 && n[0] === "ב" && n[1] === "ה") return true;
    return false;
  };

  try {
    // Read current state (including meta flags)
    const existingRes = await p.query(
      "SELECT display_name, meta FROM caller_profiles WHERE caller_id = $1 LIMIT 1",
      [cid]
    );
    const existing = existingRes.rows && existingRes.rows[0] ? existingRes.rows[0] : null;
    let existingName = existing && existing.display_name ? String(existing.display_name).trim() : "";
    let existingWords = existingName ? existingName.split(/\s+/).filter(Boolean) : [];
    let existingIsFull = existingWords.length >= 2;

    // If stored display_name is clearly garbage (e.g. "לא"), treat as missing so we can recover.
    const existingIsBad = isClearlyInvalidStoredName(existingName);
    if (existingIsBad) {
      existingName = "";
      existingWords = [];
      existingIsFull = false;
    }

    let existingMeta = {};
    try {
      if (existing && existing.meta && typeof existing.meta === "object") existingMeta = existing.meta;
    } catch { /* ignore */ }

    // If we had a bad name stored, we must ignore any existing lock flags.
    const nameLocked = !existingIsBad && existingMeta && (existingMeta.name_locked === true || String(existingMeta.name_locked).toLowerCase() === "true");

    // Rule 1: If locked, never change the name (unless identical).
    if (nameLocked) {
      if (existingName && existingName === dn) return true;
      return false;
    }

    // Rule 2: If we already have a full name stored, do not overwrite with anything else.
    if (existingIsFull) {
      if (existingName === dn) return true;
      return false;
    }

    // Rule 3: If we have a partial name (single token), do not overwrite with a different partial.
    // Only allow upgrading partial -> full (and lock it), preferably when it starts with the same token.
    if (existingName && !existingIsFull) {
      if (!dnIsFull) {
        if (existingName === dn) return true;
        return false;
      }
      const firstTokenMatches = existingWords[0] && dnWords[0] && existingWords[0] === dnWords[0];
      if (!firstTokenMatches) {
        // Allow upgrade only if the new full name contains the old token somewhere (still conservative)
        const contains = existingWords[0] && dnWords.includes(existingWords[0]);
        if (!contains) return false;
      }
      // Upgrade allowed -> lock
      const mergedMeta = { ...(existingMeta || {}), ...(metaPatch || {}), name_locked: true, name_verified: true, name_partial: false };
      await p.query(
        "UPDATE caller_profiles SET display_name=$1, updated_at=now(), meta = COALESCE(meta, '{}'::jsonb) || $2::jsonb WHERE caller_id=$3",
        [dn, JSON.stringify(mergedMeta), cid]
      );
      return true;
    }

    // Rule 4: No existing name. Save; if full -> lock, else keep partial flag.
    const baseMeta = { ...(existingMeta || {}), ...(metaPatch || {}) };
    baseMeta.name_locked = dnIsFull ? true : false;
    baseMeta.name_verified = dnIsFull ? true : false;
    baseMeta.name_partial = dnIsFull ? false : true;

    const res = await p.query(
      "UPDATE caller_profiles SET display_name=$1, updated_at=now(), meta = COALESCE(meta, '{}'::jsonb) || $2::jsonb WHERE caller_id=$3",
      [dn, JSON.stringify(baseMeta), cid]
    );
    return res.rowCount > 0;
  } catch (e) {
    logger.debug("updateCallerDisplayName failed", { callerId: cid, err: e?.message || e });
    return false;
  }
}


module.exports = {
  ensureCallerMemorySchema,
  getCallerProfile,
  upsertCallerProfile,
  updateCallerDisplayName,
  // exported for diagnostics
  hasDb,
  getPool,
  withTimeout,
};
