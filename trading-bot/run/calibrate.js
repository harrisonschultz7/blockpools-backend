#!/usr/bin/env node
// trading-bot/run/calibrate.js -- fit residualisation betas against closing lines.
//   node trading-bot/run/calibrate.js [--dry]
const { calibrate } = require("../src/model/calibrate");
const { close } = require("../src/db");
const log = require("../src/log");

calibrate({ write: !process.argv.includes("--dry"), useCache: process.argv.includes("--cache") })
  .then((r) => {
    log(`fitted on ${r.samples} games`);
    console.log(JSON.stringify(r.diagnostics, null, 2));
    return close();
  })
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
