#!/usr/bin/env node
// trading-bot/run/fit-scheme.js
//
// Fits the scheme interaction coefficients and writes model/scheme-fit.json.
// Argo-7 only, so the config is selected explicitly rather than inherited from
// the environment -- running this against Adam-7's config would read a
// schemePriorSeason that does not exist there.
//
//   node trading-bot/run/fit-scheme.js [--dry]

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { fitScheme } = require("../src/model/fitScheme");

fitScheme({ write: !process.argv.includes("--dry") })
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
