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

    const { rows } = await q(
      `select count(*)::int n from sports.nfl_games g
        join sports.pm_markets m on m.game_id = g.game_id and m.closed = false
       where g.kickoff between now() and now() + ($1 || ' hours')::interval`,
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
