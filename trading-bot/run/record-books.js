#!/usr/bin/env node
// trading-bot/run/record-books.js
//
// The depth recorder daemon. This is the job that must never stop: Polymarket
// serves current book state only, so an hour not recorded is an hour of
// backtest fidelity that cannot be bought back later.
//
// Cadence adapts -- config.ingest.bookPollSeconds normally, dropping to
// bookPollSecondsNearKickoff once any game is inside nearKickoffHours, because
// that is when the line actually moves and when the closing price is set.
//
//   node trading-bot/run/record-books.js

const { cfg } = require("../src/config");
const pm = require("../src/ingest/polymarket");
const { q } = require("../src/db");
const { sleep } = require("../src/http");
const { sweepFills } = require("../src/exec/limits");
const log = require("../src/log");

let stopping = false;
process.on("SIGTERM", () => { log("SIGTERM -- finishing tick then exiting"); stopping = true; });
process.on("SIGINT",  () => { log("SIGINT -- finishing tick then exiting");  stopping = true; });

/** Refresh the market registry occasionally; books every tick. */
async function main() {
  let lastDiscovery = 0;
  while (!stopping) {
    const c = cfg();
    const t0 = Date.now();
    try {
      if (Date.now() - lastDiscovery > 30 * 60 * 1000) {
        const all = await pm.fetchNflMarkets();
        await pm.mapMarketsToGames(pm.selectMoneylines(all));
        lastDiscovery = Date.now();
      }
      await pm.recordBooks();
    } catch (e) {
      // Never let one bad tick kill the daemon -- a missed hour is unrecoverable.
      log.err(`tick failed: ${e.message}`);
    }

    // FILL AGAINST THE BOOK JUST WRITTEN -- every poll, as the totals recorder
    // does for Argo-7. Left to the 15-minute tick, an in-play spike through the
    // limit (BUF @ LV, 2026-10-18: 0.46 printed for a minute) is never seen.
    try {
      const res = await sweepFills(c.botId);
      if (res && res.filled) log(`  sweep: ${res.filled} exit(s) filled`);
    } catch (e) { log.err(`exit sweep: ${e.message}`); }

    // Fast while a game is near kickoff OR in play with a position on it: the
    // old test went false the instant a game started -- exactly when the price
    // moves most and a resting exit is most likely to be lifted.
    const { rows } = await q(
      `select count(*)::int n from sports.nfl_games g
        join sports.pm_markets m on m.game_id = g.game_id and m.closed = false
       where g.kickoff between now() and now() + ($1 || ' hours')::interval
          or (g.home_score is null
              and g.kickoff between now() - interval '6 hours' and now()
              and exists (select 1 from bots.positions p
                           where p.game_id = g.game_id and p.shares > 0))`,
      [String(c.ingest.nearKickoffHours)],
    ).catch(() => ({ rows: [{ n: 0 }] }));

    const period = (rows[0].n > 0
      ? c.ingest.bookPollSecondsNearKickoff
      : c.ingest.bookPollSeconds) * 1000;
    const wait = Math.max(1000, period - (Date.now() - t0));
    if (!stopping) await sleep(wait);
  }
  process.exit(0);
}

main().catch((e) => { log.err(e.stack || e.message); process.exit(1); });
