#!/usr/bin/env node
// trading-bot/run/daily.js
//
// Daily maintenance: refresh source data, grade and settle finished games,
// then mark NAV. Order matters -- settle before NAV, or the NAV row counts a
// finished position at its last book mid instead of its payout.
//
//   node trading-bot/run/daily.js

const { cfg } = require("../src/config");
const { ingestAll } = require("../src/ingest/nflverse");
const { ingestWeather } = require("../src/ingest/weather");
const { gradeClv, settleTrades, botSummary } = require("../src/accounting/settle");
const { snapshotNav } = require("../src/accounting/nav");
const { close } = require("../src/db");
const log = require("../src/log");

async function main() {
  const c = cfg();
  await ingestAll();
  await ingestWeather();
  await gradeClv();       // freeze closing prices for kicked-off games
  await settleTrades();   // pay out finished games
  await snapshotNav(c.botId);

  const s = await botSummary(c.botId);
  log(`summary: ${s.wins}W-${s.losses}L  nav $${Number(s.nav).toFixed(2)} ` +
      `(${Number(s.roi_pct).toFixed(2)}%)  mean CLV ` +
      `${s.mean_clv_bps === null ? "n/a" : Number(s.mean_clv_bps).toFixed(0) + "bps"} ` +
      `over ${s.clv_graded} graded`);
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
