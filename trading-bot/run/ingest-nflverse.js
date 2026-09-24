#!/usr/bin/env node
// trading-bot/run/ingest-nflverse.js
//
// Pulls schedules + play-by-play + snap counts + injuries for every season from
// config.data.startSeason to the current one. Safe to re-run: the mirror tables
// upsert, and injuries only append rows whose status actually changed.
//
//   node trading-bot/run/ingest-nflverse.js

const { ingestAll } = require("../src/ingest/nflverse");
const { close } = require("../src/db");
const log = require("../src/log");

ingestAll()
  .then(() => close())
  .then(() => log("nflverse ingest complete"))
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
