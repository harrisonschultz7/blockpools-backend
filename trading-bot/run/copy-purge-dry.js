#!/usr/bin/env node
// trading-bot/run/copy-purge-dry.js
//
// Delete everything a dry run produced.
//
// Dry-run rows exist to be thrown away, and the longer they sit the more they
// look like history. This removes them and nothing else: the where clause is
// dry_run, so a bug here cannot touch a real position.
//
//   node trading-bot/run/copy-purge-dry.js          # count only
//   node trading-bot/run/copy-purge-dry.js --delete # actually delete

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { pool, close } = require("../src/db");

(async () => {
  const doIt = process.argv.includes("--delete");
  try {
    const o = await pool.query("select count(*)::int n from copy.orders where dry_run");
    const p = await pool.query("select count(*)::int n from copy.positions where dry_run");
    console.log(`dry-run rows: ${o.rows[0].n} orders, ${p.rows[0].n} positions`);

    if (!doIt) {
      console.log("dry run (ha). pass --delete to remove them.");
      return;
    }
    // Positions first: orders are what they were created from, and a position
    // left behind with no order is harder to explain than the reverse.
    const dp = await pool.query("delete from copy.positions where dry_run");
    const do_ = await pool.query("delete from copy.orders where dry_run");
    console.log(`deleted ${do_.rowCount} orders, ${dp.rowCount} positions`);
  } catch (e) {
    console.error("purge failed:", e.message);
    process.exitCode = 1;
  } finally {
    await close();
  }
})();
