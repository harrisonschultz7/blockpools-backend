#!/usr/bin/env node
// trading-bot/run/ingest-nflverse-players.js
//
// Pulls weekly player box scores (stats_player_week_<season>.csv) for every
// season from config.data.startSeason to the current one, into
// sports.nfl_player_week. Safe to re-run: the table upserts on (game_id,
// player_id), so a finalised week overwrites its in-progress numbers.
//
//   node trading-bot/run/ingest-nflverse-players.js
//   node trading-bot/run/ingest-nflverse-players.js 2026   # one season only

const { ingestPlayerWeekAll, ingestPlayerWeekSeason } = require("../src/ingest/nflversePlayers");
const { close } = require("../src/db");
const log = require("../src/log");

const only = Number(process.argv[2]);

(Number.isFinite(only) ? ingestPlayerWeekSeason(only) : ingestPlayerWeekAll())
  .then((n) => close().then(() => log(`player_week ingest complete: ${n} rows`)))
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
