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

/** Faster cadence when any tracked game is close to kickoff. */
async function intervalMs() {
  const c = cfg().ingest;
  const { rows } = await q(
    `select 1 from sports.nfl_games
      where kickoff between now() and now() + ($1 || ' hours')::interval limit 1`,
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
