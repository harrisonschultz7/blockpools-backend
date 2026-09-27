#!/usr/bin/env node
// trading-bot/run/prune-books.js
//
// Reclaim sports.odds_history. DRY BY DEFAULT -- pass --apply to delete.
//
// WHAT A FINISHED GAME STILL NEEDS. Exactly one thing: the closing price, the
// last book at or before kickoff, which is what every trade is graded against
// (accounting/settle.js gradeClv). Everything else recorded for a game that has
// already been played is dead weight -- there is no backtest left to run on a
// result we already know.
//
// So for each finished game this keeps, per token:
//   - the newest row at or before kickoff   (the closing line, for CLV)
//   - the newest row overall                (the settle-time mark, for NAV)
// and deletes the rest.
//
// LIVE AND UPCOMING GAMES ARE NEVER TOUCHED. Historical book depth cannot be
// bought, scraped or reconstructed from any source at any price, so the only
// rows safe to drop are ones whose purpose has already been served.
//
//   node trading-bot/run/prune-books.js [--apply] [--older-than-days N]

const { q, close } = require("../src/db");
const log = require("../src/log");

const APPLY = process.argv.includes("--apply");
const DAYS = (() => {
  const i = process.argv.indexOf("--older-than-days");
  return i >= 0 ? Number(process.argv[i + 1]) : 2;
})();

const TARGET = `
  with finished as (
    select g.game_id, g.kickoff
      from sports.nfl_games g
     where g.home_score is not null
       and g.kickoff < now() - ($1 || ' days')::interval
  ),
  ranked as (
    select o.id, o.token_id,
           row_number() over (partition by o.token_id
                              order by (o.ts <= f.kickoff) desc, o.ts desc) AS closing_rank,
           row_number() over (partition by o.token_id order by o.ts desc) AS latest_rank
      from sports.odds_history o
      join finished f on f.game_id = o.game_id
  )
  select id from ranked where closing_rank > 1 and latest_rank > 1
`;

(async () => {
  const before = await q(
    `select count(*) n, pg_size_pretty(pg_total_relation_size('sports.odds_history')) sz
       from sports.odds_history`);
  log(`odds_history now: ${before.rows[0].n} rows, ${before.rows[0].sz}`);

  const { rows: doomed } = await q(`select count(*) n from (${TARGET}) t`, [String(DAYS)]);
  const n = Number(doomed[0].n);
  log(`prunable (finished games older than ${DAYS}d, keeping closing + latest per token): ${n} rows`);

  if (!n) { log("nothing to prune"); return; }
  if (!APPLY) {
    log("DRY RUN -- re-run with --apply to delete. Nothing was changed.");
    return;
  }

  // Deleted in batches: one statement over hundreds of thousands of rows holds a
  // long transaction and can trip the statement timeout on a small instance.
  let total = 0;
  for (;;) {
    const res = await q(
      `delete from sports.odds_history
        where id in (select id from (${TARGET}) t limit 20000)`,
      [String(DAYS)]);
    if (!res.rowCount) break;
    total += res.rowCount;
    log(`  deleted ${total}/${n}`);
  }

  // DELETE only marks rows dead; the file does not shrink until VACUUM, and
  // without FULL the space is returned to the table's free list rather than to
  // the filesystem. On a hosted instance that is the right trade -- VACUUM FULL
  // takes an exclusive lock -- and the space is reused by the next month of
  // recording instead of growing the database further.
  log("vacuuming (space is returned to the table, not the filesystem)...");
  await q(`vacuum (analyze) sports.odds_history`);

  const after = await q(
    `select count(*) n, pg_size_pretty(pg_total_relation_size('sports.odds_history')) sz
       from sports.odds_history`);
  log(`odds_history after: ${after.rows[0].n} rows, ${after.rows[0].sz}`);
})()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
