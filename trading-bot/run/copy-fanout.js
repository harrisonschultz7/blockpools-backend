#!/usr/bin/env node
// trading-bot/run/copy-fanout.js
//
// THE COPY WORKER. Long-running: polls bots.trade_intents and places the same
// trade for every active subscriber.
//
// Polls rather than listens. LISTEN needs a durable session and this database
// is reached through a transaction pooler -- the same property that makes
// session advisory locks silently fail here. A 3s poll is one indexed query
// against a cursor and cannot quietly stop working the way a dropped listener
// does.
//
// SINGLE INSTANCE. Two workers would both claim; the unique index on
// (subscription_id, intent_id) stops a double order, but the loser still burns
// a client init. systemd keeps one.
//
// Env comes from .env via src/config.js, which calls dotenv -- the same path
// every other bot process uses. Needs, beyond DATABASE_URL:
//   PRIVY_APP_ID, PRIVY_APP_SECRET   identify this app to Privy
//   PRIVY_AUTHORIZATION_PRIVATE_KEY  proves we hold the key quorum the
//                                    subscriber added as a session signer
//   PM_SIGNING_URL                   absolute URL of the deployed /pm/sign
//   POLYMARKET_BUILDER_CODE          attribution on every order
//
//   node trading-bot/run/copy-fanout.js

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { runFanout } = require("../src/copy/fanout");

const POLL_MS = Number(process.env.COPY_FANOUT_POLL_MS || 3000);

// Fail loudly at boot rather than at the first order. A worker that starts
// happily and then cannot sign is worse than one that refuses to start: the
// first symptom would otherwise be a rejected trade on someone's money.
for (const v of ["PRIVY_APP_ID", "PRIVY_APP_SECRET",
                 "PRIVY_AUTHORIZATION_PRIVATE_KEY", "PM_SIGNING_URL",
                 "POLYMARKET_BUILDER_CODE"]) {
  if (!String(process.env[v] || "").trim()) {
    log.err(`copy fan-out: ${v} is not set -- refusing to start`);
    process.exit(1);
  }
}

let stopping = false;
process.on("SIGTERM", () => { stopping = true; });
process.on("SIGINT", () => { stopping = true; });

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

(async () => {
  log(`copy fan-out: started (poll ${POLL_MS}ms)`);
  while (!stopping) {
    try {
      await runFanout();
    } catch (e) {
      // Never exit on a bad pass. The cursor only advances on a completed pass,
      // so a failed tick retries the same intents rather than skipping them.
      log.err(`copy fan-out tick: ${e.message}`);
    }
    for (let w = 0; w < POLL_MS && !stopping; w += 250) await sleep(250);
  }
  log("copy fan-out: stopped");
  await close();
})().catch(async (e) => {
  log.err(e.stack || e.message);
  await close();
  process.exit(1);
});
