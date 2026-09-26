#!/usr/bin/env node
// trading-bot/run/ingest-totals-markets.js
//
// Discovers NFL totals markets and maps them to nflverse game_ids.
//
//   node trading-bot/run/ingest-totals-markets.js

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { fetchNflTotals, selectGameTotals, mapTotalsToGames } = require("../src/ingest/polymarketTotals");

(async () => {
  const all = await fetchNflTotals();
  const games = selectGameTotals(all);
  log(`fetched ${all.length} totals markets, ${games.length} are single-game`);
  await mapTotalsToGames(games);
})()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
