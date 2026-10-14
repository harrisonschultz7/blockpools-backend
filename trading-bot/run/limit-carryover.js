#!/usr/bin/env node
// trading-bot/run/limit-carryover.js
//
// THE CARRY-OVER WORKER. Long-running: re-places "Keep after kickoff" limit
// orders shortly after their game starts (Polymarket clears every resting
// sports order at the official start). See src/carry/carryover.js.
//
// MUST RUN WHERE THE CLOB ACCEPTS ORDERS. The main VPS IP is geoblocked by
// clob.polymarket.com (see the copy fan-out), so this worker lives on its own
// allowed-region host. Only users whose own browser placed the original order
// (i.e. Polymarket already accepted them from their location) are ever carried.
//
// Env (.env via src/config.js), beyond DATABASE_URL:
//   PRIVY_APP_ID, PRIVY_APP_SECRET, PRIVY_AUTHORIZATION_PRIVATE_KEY
//   PM_SIGNING_URL, POLYMARKET_BUILDER_CODE
//   LIMIT_CARRY_DRY_RUN=1   log what would be placed; orders are left pending
//   LIMIT_CARRY_DELAY_S     seconds after the start before re-placing (90)
//   LIMIT_CARRY_POLL_MS     poll interval (10000)
//
//   node trading-bot/run/limit-carryover.js

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { runCarryOver, reportStuck, DRY_RUN } = require("../src/carry/carryover");

const POLL_MS = Number(process.env.LIMIT_CARRY_POLL_MS || 10000);

for (const v of ["PRIVY_APP_ID", "PRIVY_APP_SECRET", "PRIVY_AUTHORIZATION_PRIVATE_KEY",
                 "PM_SIGNING_URL", "POLYMARKET_BUILDER_CODE"]) {
  if (!String(process.env[v] || "").trim()) {
    log.err(`limit carry-over: ${v} is not set -- refusing to start`);
    process.exit(1);
  }
}
if (DRY_RUN) log("  ## LIMIT_CARRY_DRY_RUN=1 -- nothing will be placed; orders stay pending ##");

let stopping = false;
process.on("SIGTERM", () => { stopping = true; });
process.on("SIGINT", () => { stopping = true; });
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  log(`limit carry-over: started (poll ${POLL_MS}ms)`);
  let ticks = 0;
  while (!stopping) {
    try {
      const n = await runCarryOver();
      if (n) log(`limit carry-over: handled ${n}`);
      if (++ticks % 30 === 0) await reportStuck();
    } catch (e) {
      log.err(`limit carry-over tick: ${e.message}`);
    }
    for (let w = 0; w < POLL_MS && !stopping; w += 250) await sleep(250);
  }
  log("limit carry-over: stopped");
  await close();
})().catch(async (e) => {
  log.err(e.stack || e.message);
  await close();
  process.exit(1);
});
