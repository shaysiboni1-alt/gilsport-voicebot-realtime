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

  const displayName = (patchObj.display_name ?? patchObj.full_name ?? patchObj.fullName ?? patchObj.name ?? null);
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

  const cid = String(callerId || '').trim();
  const dn = String(displayName || '').trim();

  if (!cid) return false;
  if (!dn) return false;

  // Hard guardrails (anti-hallucination / safety)
  if (dn.length < 2 || dn.length > 40) return false;
  if (/\d/.test(dn)) return false;

  const sql = `
    INSERT INTO caller_profiles (caller_id, display_name, total_calls, first_seen, last_seen, meta)
    VALUES ($1, $2, 0, NOW(), NOW(), COALESCE($3::jsonb, '{}'::jsonb))
    ON CONFLICT (caller_id) DO UPDATE SET
      display_name = EXCLUDED.display_name,
      last_seen = NOW(),
      meta = CASE
        WHEN $3::jsonb IS NULL THEN caller_profiles.meta
        ELSE caller_profiles.meta || $3::jsonb
      END
  `;

  try {
    await withTimeout(p.query(sql, [cid, dn, metaPatch ? JSON.stringify(metaPatch) : null]));
    return true;
  } catch (e) {
    logger.debug('Caller memory display_name update failed', { error: String(e?.message || e) });
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
