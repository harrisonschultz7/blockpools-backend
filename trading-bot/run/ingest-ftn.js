#!/usr/bin/env node
// trading-bot/run/ingest-ftn.js
//
// Pulls nflverse ftn_charting into sports.nfl_team_scheme_game.
//
// Argo-7 only, so the config is selected explicitly: Adam-7's config has no
// model.scheme block and the neutral-band lookup would be undefined.
//
// Runs on a timer rather than on demand because FTN LAGS the schedule -- on
// 2026-09-26 the 2026 file held weeks 1-2 complete and one week-3 game -- so
// the useful behaviour is to keep re-pulling and pick up games as they are
// charted.
//
//   node trading-bot/run/ingest-ftn.js [season]

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { ingestFtnSeason, ingestFtnAll } = require("../src/ingest/ftn");

const arg = process.argv[2] ? Number(process.argv[2]) : null;

(arg ? ingestFtnSeason(arg) : ingestFtnAll())
  .then((n) => { log(`ftn ingest: ${n} team-game rows`); return close(); })
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
