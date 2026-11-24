#!/usr/bin/env node
// trading-bot/run/risk-tier.js
//
// Recomputes every enabled bot's risk tier and writes bots.bot.risk_tier.
//
// The tier is the share of the portfolio a model stakes on ONE trade (user,
// 2026-11-24): Low < 5%, Medium 5-15%, High 15%+. See
// src/accounting/riskTier.js. --score also prints the older turnover-based
// risk score as a diagnostic; it no longer sets the tier.
//
//   node trading-bot/run/risk-tier.js [--dry] [--score]

const { close } = require("../src/db");
const log = require("../src/log");
const { applyRiskTiers, computeRiskTier } = require("../src/accounting/riskTier");

const DRY = process.argv.includes("--dry");
const SCORE = process.argv.includes("--score");

(async () => {
  const rows = await applyRiskTiers({ write: !DRY });
  log(`risk tiers${DRY ? " (DRY RUN)" : ""}   [stake per trade: low <5% | medium 5-15% | high 15%+]`);
  for (const r of rows) {
    log(`  ${r.name.padEnd(8)} ${r.tier.toUpperCase().padEnd(7)} stakes ${(100 * r.stakePct).toFixed(1)}% per trade` +
        (r.changed ? `   [was ${r.previousTier}]` : ""));
    if (SCORE) {
      const s = await computeRiskTier(r.botId);
      log(`           (diagnostic) turnover score ${(100 * s.riskScore).toFixed(1)}% = turnover ${(100 * s.basisPctNav).toFixed(1)}%` +
          ` x price ${s.priceFactor.toFixed(2)} x exit ${s.exitFactor.toFixed(2)} x corr ${s.correlationFactor.toFixed(2)}`);
    }
  }
  await close();
})().catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
