// trading-bot/src/db.js
//
// Postgres pool. Mirrors src/db/pg.ts (same DATABASE_URL, same Supabase
// instance) but stands alone so the bot can run as its own systemd unit
// without booting the Express app.

const { Pool } = require("pg");
const { requireDb } = require("./config");

const connectionString = requireDb();

// Supabase terminates TLS with a cert this client does not have in its trust
// store; the connection is still encrypted. Matches how the rest of the
// backend reaches the same instance.
const ssl = /supabase.(co|com)/.test(connectionString)
  ? { rejectUnauthorized: false }
  : undefined;

const pool = new Pool({
  connectionString,
  ssl,
  max: 6,
  idleTimeoutMillis: 30_000,
});

async function q(text, params) {
  return pool.query(text, params);
}

/** Insert many rows in one round trip. cols drive the $n placeholder grid. */
async function bulkInsert(table, cols, rows, { onConflict = "" } = {}) {
  if (!rows.length) return 0;
  const CHUNK = 500;
  let total = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const slice = rows.slice(i, i + CHUNK);
    const params = [];
    const tuples = slice.map((r) => {
      const ph = cols.map((c) => {
        params.push(r[c] === undefined ? null : r[c]);
        return `$${params.length}`;
      });
      return `(${ph.join(",")})`;
    });
    const sql = `insert into ${table} (${cols.join(",")}) values ${tuples.join(",")} ${onConflict}`;
    const res = await pool.query(sql, params);
    total += res.rowCount || 0;
  }
  return total;
}

/**
 * ON CONFLICT clause that updates a row ONLY when one of its data columns
 * actually changed. The daily re-ingests (nflverse, FTN) re-send whole seasons;
 * a plain "do update" rewrote every unchanged row every day -- 40k snap-count
 * rows a day, 2.2M row rewrites in a year -- which Postgres stores as a fresh
 * row version each time (bloat the size-capped database pays for). New and
 * corrected rows are written exactly as before; identical ones are skipped.
 *
 *   table   "sports.nfl_snap_counts" (schema optional)
 *   keys    conflict-target columns
 *   extra   SET fragment applied only when a row changes, e.g. "computed_at = now()"
 */
function upsertIfChanged(table, cols, keys, extra = "") {
  const rel = table.split(".").pop();
  const data = cols.filter((c) => !keys.includes(c));
  const set = data.map((c) => `${c} = excluded.${c}`).concat(extra ? [extra] : []).join(", ");
  const cmp = `(${data.map((c) => `${rel}.${c}`).join(", ")}) is distinct from (${data.map((c) => `excluded.${c}`).join(", ")})`;
  return `on conflict (${keys.join(", ")}) do update set ${set} where ${cmp}`;
}

async function close() {
  await pool.end();
}

module.exports = { pool, q, bulkInsert, upsertIfChanged, close };
