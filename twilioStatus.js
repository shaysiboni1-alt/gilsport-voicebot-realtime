const { Pool } = require('pg');

let _pool;

function parseBoolEnv(v) {
  if (v == null) return null;
  const s = String(v).trim().toLowerCase();
  if (['1', 'true', 'yes', 'y', 'on'].includes(s)) return true;
  if (['0', 'false', 'no', 'n', 'off'].includes(s)) return false;
  return null;
}

function shouldForceSslByHeuristic(connectionString) {
  try {
    const u = new URL(connectionString);
    const host = (u.hostname || '').toLowerCase();
    // Render Postgres hostnames commonly start with dpg-...
    if (host.startsWith('dpg-')) return true;
    // Some managed providers
    if (host.endsWith('.render.com')) return true;
    if (host.endsWith('.neon.tech')) return true;
    if (host.endsWith('.supabase.co')) return true;
    return false;
  } catch {
    return false;
  }
}

function buildSslOption(connectionString) {
  const envOverride = parseBoolEnv(process.env.PGSSL);
  if (envOverride === false) return undefined;
  if (envOverride === true) return { rejectUnauthorized: false };

  // If the URL explicitly demands sslmode=require, honor it.
  const urlWantsSsl = /sslmode=require/i.test(connectionString || '');
  if (urlWantsSsl) return { rejectUnauthorized: false };

  // Otherwise, use a heuristic for managed DBs.
  if (connectionString && shouldForceSslByHeuristic(connectionString)) {
    return { rejectUnauthorized: false };
  }

  return undefined;
}

function getPool() {
  if (_pool) return _pool;

  const connectionString = process.env.DATABASE_URL || process.env.DATABASE;
  // Caller-memory is optional. If DB is not configured, return null and let
  // the runtime continue without memory.
  if (!connectionString) return null;

  const ssl = buildSslOption(connectionString);

  _pool = new Pool({
    connectionString,
    ssl,
    // Keep these conservative; caller memory must never block call flow.
    max: Number(process.env.PGPOOL_MAX || 3),
    idleTimeoutMillis: Number(process.env.PG_IDLE_TIMEOUT_MS || 30_000),
    connectionTimeoutMillis: Number(process.env.PG_CONN_TIMEOUT_MS || 4_000),
  });

  return _pool;
}

function hasDb() {
  return Boolean(process.env.DATABASE_URL || process.env.DATABASE);
}

function withTimeout(promise, ms, label = 'db') {
  if (!ms || ms <= 0) return promise;
  let t;
  const timeout = new Promise((_, reject) => {
    t = setTimeout(() => reject(new Error(`${label} timeout after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(t));
}

module.exports = { getPool, hasDb, withTimeout };
