#!/usr/bin/env node
// trading-bot/run/calibrate-totals.js
//
// Fits per-factor scales and sigma, writing model/calibration-totals.json.
// Argo-7 refuses to produce a forecast at all until this has succeeded -- see the
// header of model/forecastTotals.js for why there is no safe default.
//
//   node trading-bot/run/calibrate-totals.js [--dry]

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { calibrateTotals } = require("../src/model/calibrateTotals");

calibrateTotals({ write: !process.argv.includes("--dry") })
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
