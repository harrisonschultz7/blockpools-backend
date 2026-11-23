// trading-bot/src/cache.js
//
// DISK CACHE FOR HISTORY READS (Supabase egress, 2026-11-23).
//
// Every tick is a fresh process (systemd timers, every 15 minutes), and each one
// re-read the same history from Supabase: full team-game stats for momentum,
// matchup and pace, one snap-count query per injured player, the scheme
// charting. That history only changes when run/daily.js ingests nflverse/FTN
// (once a day) -- yet it was ~all of the egress quota. Measured over a year:
// 14M team-stat rows, 11.5M injury-baseline rows, 4.6M scheme rows sent.
//
// RULE. A cached result is reused only when
//   1. the data VERSION is unchanged -- a tiny query over the source tables'
//      row counts and change stamps, run once per process; and
//   2. for "games before asOf" queries, the new asOf is not EARLIER than the
//      cached one. These tables only ever hold FINISHED games, loaded by the
//      daily ingest, so no row can fall between two later asOf values without
//      the ingest running -- which changes the version.
// A miss runs the query exactly as before and stores it. Anything wrong with
// the cache (missing dir, bad file) falls through to the database.
//
// Set TRADING_BOT_CACHE=off to bypass it entirely.

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { q } = require("./db");

const DIR = process.env.TRADING_BOT_CACHE_DIR || path.join(__dirname, "..", "cache");
const OFF = String(process.env.TRADING_BOT_CACHE || "").toLowerCase() === "off";

let versionPromise = null;

/**
 * One cheap row describing every history table the cache covers. Counts catch
 * inserts; change stamps and sums catch corrections. Memoised per process.
 */
function dataVersion() {
  if (!versionPromise) {
    versionPromise = q(
      `select
         (select count(*) || '@' || coalesce(max(computed_at)::text, '') from sports.nfl_team_game_stats) tgs,
         (select count(*) || '@' || coalesce(sum(coalesce(offense_snaps, 0) + coalesce(defense_snaps, 0)), 0)
            from sports.nfl_snap_counts) snaps,
         (select count(*) || '@' || coalesce(max(updated_at)::text, '') from sports.nfl_games) games,
         (select count(*) || '@' || coalesce(max(computed_at)::text, '') from sports.nfl_team_scheme_game) scheme`,
    ).then((r) => JSON.stringify(r.rows[0]));
  }
  return versionPromise;
}

const fileFor = (name, key) =>
  path.join(DIR, `${name}-${crypto.createHash("sha1").update(key).digest("hex").slice(0, 16)}.json`);

/**
 * Run `sql` through the cache.
 *   name    short label for the file
 *   keyParams  the params that identify the result (NOT asOf)
 *   asOf    the "games before" cut-off, or null when the query has none
 */
async function cachedRows(name, sql, params, { keyParams, asOf = null } = {}) {
  if (OFF) return (await q(sql, params)).rows;
  // LIVE READS ONLY. Rule 2 holds because a live tick's asOf is "now": a game
  // that finished after the cached read can only have arrived via a new ingest.
  // A back-test or fit passes past dates, where a later asOf really can add
  // games without any ingest -- those always go straight to the database.
  if (asOf != null && Math.abs(new Date(asOf).getTime() - Date.now()) > 2 * 3600 * 1000) {
    return (await q(sql, params)).rows;
  }
  let version;
  try { version = await dataVersion(); } catch { return (await q(sql, params)).rows; }
  const file = fileFor(name, JSON.stringify(keyParams ?? params));
  const at = asOf == null ? null : new Date(asOf).getTime();
  try {
    const c = JSON.parse(fs.readFileSync(file, "utf8"));
    if (c.version === version && (at == null || (c.asOf != null && at >= c.asOf))) return c.rows;
  } catch { /* miss */ }
  const { rows } = await q(sql, params);
  try {
    fs.mkdirSync(DIR, { recursive: true });
    const tmp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify({ version, asOf: at, rows }));
    fs.renameSync(tmp, file);
  } catch { /* cache is best-effort */ }
  return rows;
}

module.exports = { cachedRows, dataVersion };
