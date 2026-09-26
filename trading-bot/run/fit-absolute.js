#!/usr/bin/env node
// trading-bot/run/fit-absolute.js
//
// Fits the independent mode's base total -> model/absolute-fit.json.
// Required before priceMode "independent" will produce a forecast.
//
//   node trading-bot/run/fit-absolute.js [--dry]

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { fitAbsolute } = require("../src/model/fitAbsolute");

fitAbsolute({ write: !process.argv.includes("--dry") })
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
