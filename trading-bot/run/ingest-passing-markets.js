#!/usr/bin/env node
// trading-bot/run/ingest-passing-markets.js
//
// Discovers NFL passing-yards prop markets and maps them to nflverse game_ids
// and gsis player_ids. Props seed near kickoff, so this is meant to run often
// (hourly) through the game-day window, not once a week.
//
//   node trading-bot/run/ingest-passing-markets.js
//
// Runs under Argo-7's config for now: it reuses that config's ingest.* recorder
// params (plus the two passing* keys added there). The standalone Bruno-7 config
// is created when the trading half of the bot is built.

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { fetchNflPassing, selectGamePassing, mapPassingToGames } = require("../src/ingest/polymarketPassing");

(async () => {
  const all = await fetchNflPassing();
  const games = selectGamePassing(all);
  log(`fetched ${all.length} passing markets, ${games.length} are single-game`);
  await mapPassingToGames(games);
})()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
