#!/usr/bin/env node
// trading-bot/run/record-official-injuries.js
//
// One pass of the NBA official injury report logger (see
// src/ingest/nbaOfficialInjuries.js). Run every 5 minutes by
// systemd/blockpools-nba-official-injuries.timer.
//
//   node trading-bot/run/record-official-injuries.js

const { recordOfficialInjuries } = require("../src/ingest/nbaOfficialInjuries");
const { close } = require("../src/db");
const log = require("../src/log");

recordOfficialInjuries()
  .then(({ read, changes }) => { if (read) log(`official injuries: ${read} report(s) read, ${changes} change(s) logged`); })
  .catch((e) => { log.err(`official injuries: ${e.message}`); process.exitCode = 1; })
  .finally(() => close());
