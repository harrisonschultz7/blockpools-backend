#!/usr/bin/env node
// trading-bot/run/migrate.js
//
// Applies trading-bot/sql/*.sql in filename order. Every statement is
// idempotent (create ... if not exists), so re-running is safe.
//
//   node trading-bot/run/migrate.js

const fs = require("fs");
const path = require("path");
const { pool } = require("../src/db");
const log = require("../src/log");

async function main() {
  const dir = path.join(__dirname, "../sql");
  // --only 010 applies just the files whose name starts with that prefix: a new
  // migration on a live database, without re-running the ones already applied.
  const i = process.argv.indexOf("--only");
  const only = i >= 0 ? process.argv[i + 1] : null;
  const files = fs.readdirSync(dir).filter((f) => f.endsWith(".sql") && (!only || f.startsWith(only))).sort();
  if (only && !files.length) throw new Error(`no migration matches --only ${only}`);
  for (const f of files) {
    const sql = fs.readFileSync(path.join(dir, f), "utf8");
    process.stdout.write(`applying ${f} ... `);
    await pool.query(sql);
    console.log("ok");
  }
  const { rows } = await pool.query(`
    select table_schema, count(*)::int n
      from information_schema.tables
     where table_schema in ('sports','bots')
     group by 1 order by 1`);
  rows.forEach((r) => log(`${r.table_schema}: ${r.n} tables`));
  await pool.end();
}

main().catch((e) => { log.err(e.message); process.exit(1); });
