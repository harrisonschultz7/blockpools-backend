#!/usr/bin/env node
// trading-bot/run/record-totals-books.js
//
// THE TOTALS DEPTH RECORDER. Long-running: polls every bookPollSeconds, faster
// inside nearKickoffHours.
//
// Runs continuously from day one because historical order-book depth cannot be
// backfilled from any source at any price. An unrecorded hour is gone.
//
//   node trading-bot/run/record-totals-books.js

const { selectConfig, cfg } = require("../src/config");
selectConfig("config.argo-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const { recordTotalsBooks } = require("../src/ingest/polymarketTotals");

let stopping = false;
process.on("SIGTERM", () => { stopping = true; });
process.on("SIGINT", () => { stopping = true; });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Faster cadence when any tracked game is close to kickoff -- OR in play with an
 * open position against it.
 *
 * The in-play half matters more than the pre-kickoff half. `kickoff between now()
 * and now() + nearKickoffHours` goes false the instant a game starts, so the old
 * version dropped to the slow poll exactly when the exits were live and the price
 * was moving fastest. A resting sell is worth what the book was when we last
 * looked at it, so during a game we look often.
 */
async function intervalMs() {
  const c = cfg().ingest;
  const { rows } = await q(
    `select 1 from sports.nfl_games g
      where g.kickoff between now() and now() + ($1 || ' hours')::interval
         or (g.home_score is null
             and g.kickoff between now() - interval '6 hours' and now()
             and exists (select 1 from bots.positions p
                          where p.game_id = g.game_id and p.shares > 0))
      limit 1`,
    [String(c.nearKickoffHours)],
  );
  return (rows.length ? c.bookPollSecondsNearKickoff : c.bookPollSeconds) * 1000;
}

(async () => {
  log("totals recorder: started");
  while (!stopping) {
    try { await recordTotalsBooks(); }
    catch (e) { log.err(`totals recorder tick: ${e.message}`); }
    const ms = await intervalMs().catch(() => 60000);
    for (let waited = 0; waited < ms && !stopping; waited += 1000) await sleep(1000);
  }
  log("totals recorder: stopped");
  await close();
})().catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
