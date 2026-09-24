#!/usr/bin/env node
// trading-bot/run/ingest-markets.js
//
// Discovers NFL game moneylines on Polymarket, maps them to nflverse game_ids
// and records one book snapshot. Run standalone to refresh the registry, or let
// run/record-books.js call it on a loop.
//
//   node trading-bot/run/ingest-markets.js [--no-book]

const pm = require("../src/ingest/polymarket");
const { close } = require("../src/db");
const log = require("../src/log");

async function main() {
  const all = await pm.fetchNflMarkets();
  const ml = pm.selectMoneylines(all);
  log(`gamma: ${all.length} NFL-tagged markets -> ${ml.length} game moneylines`);
  await pm.mapMarketsToGames(ml);
  if (!process.argv.includes("--no-book")) await pm.recordBooks();
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
