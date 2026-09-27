#!/usr/bin/env node
// trading-bot/run/risk-tier.js
//
// Recomputes every enabled bot's risk tier and writes bots.bot.risk_tier.
//
// The tier used to be a string typed into each config, which could drift from
// what the bot actually does and could not be compared across bots. It is now
// derived from two primary drivers -- how much of the portfolio is put at risk
// per week, and how far a position must travel before it is realised. See
// src/accounting/riskTier.js for the formula and the simulations each term was
// checked against.
//
//   node trading-bot/run/risk-tier.js [--dry]

const { close } = require("../src/db");
const log = require("../src/log");
const { applyRiskTiers } = require("../src/accounting/riskTier");

const DRY = process.argv.includes("--dry");

applyRiskTiers({ write: !DRY })
  .then((rows) => {
    log(`risk tiers${DRY ? " (DRY RUN)" : ""}   [low <10% | medium 10-20% | high 20%+]`);
    for (const r of rows) {
      log(`  ${r.name.padEnd(8)} ${r.tier.toUpperCase().padEnd(7)} score ` +
          `${(100 * r.riskScore).toFixed(1)}%  =  turnover ${(100 * r.basisPctNav).toFixed(1)}%` +
          ` x price ${r.priceFactor.toFixed(2)}` +
          ` x exit ${r.exitFactor.toFixed(2)}` +
          ` x corr ${r.correlationFactor.toFixed(2)}` +
          (r.changed ? `   [was ${r.previousTier}]` : ""));
      log(`           turnover basis: ${r.basis}`);
      log(`           exit factor from ${r.exitSource}` +
          ` (1.00 would mean holding to settlement)`);
      log(`           prices from ${r.priceSource}`);
    }
    return close();
  })
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
