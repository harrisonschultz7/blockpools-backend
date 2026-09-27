#!/usr/bin/env node
// trading-bot/run/risk-tier.js
//
// Recomputes every enabled bot's risk tier from its sizing policy and the prices
// it trades, and writes it to bots.bot.risk_tier.
//
// The tier used to be a string typed into each config, which could drift from
// what the bot actually does and could not be compared across bots. It is now
// derived -- see src/accounting/riskTier.js for the formula and the simulations
// the price and correlation terms were checked against.
//
//   node trading-bot/run/risk-tier.js [--dry]

const { close } = require("../src/db");
const log = require("../src/log");
const { applyRiskTiers } = require("../src/accounting/riskTier");

const DRY = process.argv.includes("--dry");

applyRiskTiers({ write: !DRY })
  .then((rows) => {
    log(`risk tiers${DRY ? " (DRY RUN)" : ""}:`);
    for (const r of rows) {
      log(`  ${r.name.padEnd(8)} ${r.tier.toUpperCase().padEnd(7)} ` +
          `risk-adj capital at risk ${(100 * r.riskAdjustedCapitalAtRisk).toFixed(1)}%` +
          `  = ${(100 * r.basisPctNav).toFixed(1)}%` +
          ` x price ${r.meanSqrtOdds.toFixed(2)}` +
          ` x corr ${r.correlationFactor.toFixed(2)}` +
          (r.changed ? `   [was ${r.previousTier}]` : ""));
      log(`           basis: ${r.basis}`);
      log(`           cap ${(100 * r.weeklyCapPctNav).toFixed(0)}% | observed p90 ` +
          `${r.observedP90PctNav === null ? "n/a" : (100 * r.observedP90PctNav).toFixed(1) + "%"}` +
          ` | open right now ${(100 * r.observedOpenPctNav).toFixed(1)}%` +
          ` | prices from ${r.priceSource}`);
    }
    return close();
  })
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
