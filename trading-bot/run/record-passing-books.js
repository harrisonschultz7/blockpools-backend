#!/usr/bin/env node
// trading-bot/run/record-passing-books.js
//
// THE PASSING-YARDS DEPTH RECORDER + Bruno-7 FILL SWEEP. Long-running: polls
// every bookPollSeconds, faster near kickoff and while a held game is in play.
//
// Started life as the liquidity probe (records only). Since Bruno-7 paper trades
// (2026-10-26) it also sweeps the bot's resting exits and stop-losses against
// each book it just wrote, exactly like record-totals-books.js does for Argo-7 --
// a resting sell is lifted the moment the book crosses it, not when the
// 15-minute tick happens to look.
//
// Runs continuously because historical order-book depth cannot be backfilled from
// any source at any price; an unrecorded hour is gone.
//
//   node trading-bot/run/record-passing-books.js

const { selectConfig, cfg } = require("../src/config");
// Bruno-7's config, not Argo-7's: the sweep reads policy (stop-loss floor) and
// botId from it. The ingest block is inherited, so recording is unchanged.
selectConfig("config.bruno-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const { recordPassingBooks } = require("../src/ingest/polymarketPassing");
const { sweepFills } = require("../src/exec/limits");

let stopping = false;
process.on("SIGTERM", () => { stopping = true; });
process.on("SIGINT", () => { stopping = true; });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Faster cadence when any tracked game is close to kickoff -- OR in play with an
 * open Bruno-7 position, because that is when its exits and stops are live.
 */
async function intervalMs() {
  const c = cfg().ingest;
  const { rows } = await q(
    `select 1 from sports.nfl_games g
      where g.kickoff between now() and now() + ($1 || ' hours')::interval
         or (g.home_score is null
             and g.kickoff between now() - interval '6 hours' and now()
             and exists (select 1 from bots.positions p
                          where p.bot_id = $2 and p.game_id = g.game_id and p.shares > 0))
      limit 1`,
    [String(c.nearKickoffHours), cfg().botId],
  );
  return (rows.length ? c.bookPollSecondsNearKickoff : c.bookPollSeconds) * 1000;
}

(async () => {
  log("passing recorder: started");
  while (!stopping) {
    try { await recordPassingBooks(); }
    catch (e) { log.err(`passing recorder tick: ${e.message}`); }

    // Fill against the book just written. Failures are logged and swallowed: a
    // fill problem must never stop the recorder (lost depth is permanent, a
    // missed fill is retried next poll).
    try {
      const res = await sweepFills(cfg().botId);
      if (res && res.filled) log(`  sweep: ${res.filled} exit(s) filled`);
    } catch (e) { log.err(`exit sweep: ${e.message}`); }

    const ms = await intervalMs().catch(() => 60000);
    for (let waited = 0; waited < ms && !stopping; waited += 1000) await sleep(1000);
  }
  log("passing recorder: stopped");
  await close();
})().catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
