#!/usr/bin/env node
// trading-bot/run/thin-books.js
//
// Retroactively apply the heartbeat policy to history already recorded.
// DRY BY DEFAULT -- pass --apply to delete.
//
// READ THIS BEFORE RUNNING IT. Unlike the dedup in ingest/bookWriter.js, this is
// NOT lossless. Dedup drops rows that were identical to their predecessor and so
// carried no information. This drops rows that DID record a change, keeping only
// the last one in each interval. A replay of those minutes afterwards is coarser
// than what was captured, and historical book depth cannot be re-fetched from
// any source at any price.
//
// It exists because the recorder ran unthrottled for three days and put
// sports.odds_history at 448 MB of a 782 MB database on a 500 MB plan. Measured:
// thinning rows older than 6 hours to one per token per 10 minutes removes 89%
// of them and frees roughly 270 MB.
//
// The most recent hours are always left untouched, because that is the window a
// live decision actually reads.
//
//   node trading-bot/run/thin-books.js [--apply] [--keep-hours N] [--bucket-min N]

const { q, close } = require("../src/db");
const log = require("../src/log");

const APPLY = process.argv.includes("--apply");
const arg = (flag, dflt) => {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? Number(process.argv[i + 1]) : dflt;
};
const KEEP_HOURS = arg("--keep-hours", 6);
const BUCKET_MIN = arg("--bucket-min", 10);

const TARGET = `
  select id from (
    select id,
           row_number() over (
             partition by token_id,
                          date_trunc('hour', ts),
                          floor(extract(minute from ts) / $2)
             order by ts desc
           ) AS rn
      from sports.odds_history
     where ts < now() - ($1 || ' hours')::interval
  ) b
  where rn > 1
`;

(async () => {
  const before = await q(
    `select count(*) n, pg_size_pretty(pg_total_relation_size('sports.odds_history')) sz
       from sports.odds_history`);
  log(`odds_history now: ${before.rows[0].n} rows, ${before.rows[0].sz}`);
  log(`policy: keep everything newer than ${KEEP_HOURS}h; older than that, ` +
      `keep the newest row per token per ${BUCKET_MIN} minutes`);

  const { rows: cnt } = await q(
    `select count(*) n from (${TARGET}) t`, [String(KEEP_HOURS), String(BUCKET_MIN)]);
  const n = Number(cnt[0].n);
  log(`would delete ${n} rows (~${Math.round((n * 1.25) / 1024)} MB)`);

  if (!n) { log("nothing to thin"); return; }
  if (!APPLY) {
    log("DRY RUN -- this deletes REAL price changes, not duplicates. " +
        "Re-run with --apply only if the storage matters more than replay fidelity.");
    return;
  }

  let total = 0;
  for (;;) {
    const res = await q(
      `delete from sports.odds_history where id in (${TARGET} limit 20000)`,
      [String(KEEP_HOURS), String(BUCKET_MIN)]);
    if (!res.rowCount) break;
    total += res.rowCount;
    log(`  deleted ${total}/${n}`);
  }

  // VACUUM FULL, not plain VACUUM: plain returns the space to the table's own
  // free list, which keeps the database size exactly where it was as far as the
  // hosting plan is concerned. FULL rewrites the table and gives it back, at the
  // cost of an exclusive lock -- so stop the recorders before running this.
  log("vacuum full (takes an exclusive lock -- stop the recorders first)...");
  await q(`vacuum full sports.odds_history`);
  await q(`analyze sports.odds_history`);

  const after = await q(
    `select count(*) n, pg_size_pretty(pg_total_relation_size('sports.odds_history')) sz
       from sports.odds_history`);
  log(`odds_history after: ${after.rows[0].n} rows, ${after.rows[0].sz}`);
})()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
