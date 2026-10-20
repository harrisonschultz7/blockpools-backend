#!/usr/bin/env node
// trading-bot/run/record-passing-books.js
//
// THE PASSING-YARDS DEPTH RECORDER (Bruno-7 liquidity probe). Long-running: polls
// every bookPollSeconds, faster inside nearKickoffHours. Records only -- no fills
// -- because the question this answers is whether the central-line books ever get
// real two-sided depth, not whether an order fills against them.
//
// Runs continuously because historical order-book depth cannot be backfilled from
// any source at any price; an unrecorded hour is gone. Props seed near kickoff,
// so most of what this writes before game day is empty placeholder books -- which
// is itself the baseline the probe measures the depth transition against.
//
//   node trading-bot/run/record-passing-books.js

const { selectConfig, cfg } = require("../src/config");
selectConfig("config.argo-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const { recordPassingBooks } = require("../src/ingest/polymarketPassing");

let stopping = false;
process.on("SIGTERM", () => { stopping = true; });
process.on("SIGINT", () => { stopping = true; });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Faster cadence when any tracked game is within nearKickoffHours of kickoff. */
async function intervalMs() {
  const c = cfg().ingest;
  const { rows } = await q(
    `select 1 from sports.nfl_games g
      where g.kickoff between now() and now() + ($1 || ' hours')::interval
      limit 1`,
    [String(c.nearKickoffHours)],
  );
  return (rows.length ? c.bookPollSecondsNearKickoff : c.bookPollSeconds) * 1000;
}

(async () => {
  log("passing recorder: started");
  while (!stopping) {
    try { await recordPassingBooks(); }
    catch (e) { log.err(`passing recorder tick: ${e.message}`); }
    const ms = await intervalMs().catch(() => 60000);
    for (let waited = 0; waited < ms && !stopping; waited += 1000) await sleep(1000);
  }
  log("passing recorder: stopped");
  await close();
})().catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
