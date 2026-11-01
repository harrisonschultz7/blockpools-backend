#!/usr/bin/env node
// trading-bot/run/record-books-nba.js
//
// Axis-3's depth recorder daemon: NBA moneyline books, plus the exit/stop
// sweep against each book just written (an in-play spike through a resting
// sell is only seen if something is watching every poll -- the 5-minute tick
// would miss it). Same shape as run/record-books.js for Adam-7.
//
// Market discovery needs the schedule first (markets map onto nba_games), so
// every discovery pass refreshes the next few days of ESPN's scoreboard too.
//
//   node trading-bot/run/record-books-nba.js

const { selectConfig, cfg } = require("../src/config");
selectConfig(process.env.TRADING_BOT_CONFIG || "config.axis-3.json");

const { q } = require("../src/db");
const { sleep } = require("../src/http");
const { sweepFills } = require("../src/exec/limits");
const espn = require("../src/ingest/espnNba");
const pm = require("../src/ingest/polymarketNba");
const log = require("../src/log");

let stopping = false;
process.on("SIGTERM", () => { log("SIGTERM -- finishing tick then exiting"); stopping = true; });
process.on("SIGINT",  () => { log("SIGINT -- finishing tick then exiting");  stopping = true; });

async function main() {
  let lastDiscovery = 0;
  while (!stopping) {
    const c = cfg();
    const t0 = Date.now();
    try {
      if (Date.now() - lastDiscovery > 30 * 60 * 1000) {
        const now = new Date();
        await espn.syncSchedule(new Date(now.getTime() - 86_400_000),
                                new Date(now.getTime() + c.ingest.scheduleDaysAhead * 86_400_000));
        await pm.mapNbaMarkets(await pm.fetchNbaMarkets());
        lastDiscovery = Date.now();
      }
      await pm.recordNbaBooks(c.botId);
    } catch (e) {
      log.err(`nba recorder tick failed: ${e.message}`);
    }

    try {
      const res = await sweepFills(c.botId);
      if (res && res.filled) log(`  sweep: ${res.filled} exit(s) filled`);
    } catch (e) { log.err(`nba exit sweep: ${e.message}`); }

    // Fast near tip, or while Axis-3 holds a position in a live game.
    const { rows } = await q(
      `select count(*)::int n from sports.nba_games g
         join sports.pm_markets m on m.game_id = g.game_id and m.closed = false
        where g.kickoff between now() and now() + ($1 || ' hours')::interval
           or (g.home_score is null and g.kickoff between now() - interval '4 hours' and now()
               and exists (select 1 from bots.positions p where p.game_id = g.game_id and p.shares > 0))`,
      [String(c.ingest.nearKickoffHours)],
    ).catch(() => ({ rows: [{ n: 0 }] }));
    const period = (rows[0].n > 0 ? c.ingest.bookPollSecondsNearKickoff : c.ingest.bookPollSeconds) * 1000;
    const wait = Math.max(1000, period - (Date.now() - t0));
    if (!stopping) await sleep(wait);
  }
  process.exit(0);
}

main().catch((e) => { log.err(e.stack || e.message); process.exit(1); });
