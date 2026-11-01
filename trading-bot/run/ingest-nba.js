#!/usr/bin/env node
// trading-bot/run/ingest-nba.js
//
// NBA history for Axis-3: schedule + results + box scores from ESPN.
//
//   node trading-bot/run/ingest-nba.js                 # backfill from ingest.historyStart to today
//   node trading-bot/run/ingest-nba.js --from 2026-10-20 --to 2026-11-01
//   node trading-bot/run/ingest-nba.js --check         # parse today's live payloads, write NOTHING
//
// The full backfill (last season + this one) is ~400 scoreboard calls and
// ~1,400 box scores: a one-off of 15-25 minutes. Re-running is safe -- games
// upsert, and box scores are only fetched for finals that lack one.

const { selectConfig, cfg } = require("../src/config");
selectConfig(process.env.TRADING_BOT_CONFIG || "config.axis-3.json");

const espn = require("../src/ingest/espnNba");
const log = require("../src/log");

const arg = (k) => { const i = process.argv.indexOf(k); return i >= 0 ? process.argv[i + 1] : null; };

async function check() {
  const today = new Date();
  const games = espn.parseScoreboard(await espn.fetchScoreboard(today));
  log(`scoreboard ${today.toISOString().slice(0, 10)}: ${games.length} games`);
  for (const g of games) log(`  ${g.away_team} @ ${g.home_team} ${g.kickoff} ${g.status} tv=${g.national_tv || "-"}`);
  const yday = espn.parseScoreboard(await espn.fetchScoreboard(new Date(today.getTime() - 86_400_000)));
  const fin = yday.find((g) => g.home_score !== null);
  if (fin) {
    const box = espn.parseBoxscore(fin.game_id, await espn.fetchSummary(fin.espn_id));
    log(`box ${fin.away_team} @ ${fin.home_team}: ${box.length} player rows, ${box.filter((r) => r.dnp).length} DNP`);
  }
  const inj = espn.parseInjuries(await espn.fetchInjuries());
  const unknown = inj.filter((r) => !espn.NBA_TEAMS.has(r.team));
  log(`injury report: ${inj.length} players, statuses ${[...new Set(inj.map((r) => r.status))].join("/")}` +
      (unknown.length ? `, ${unknown.length} with an unknown team code (${unknown[0].team})` : ""));
}

async function main() {
  if (process.argv.includes("--check")) return check();
  const { close } = require("../src/db");
  try {
    const from = new Date(arg("--from") || cfg().ingest.historyStart);
    const to = new Date(arg("--to") || new Date().toISOString().slice(0, 10));
    log(`nba backfill: schedule ${from.toISOString().slice(0, 10)} .. ${to.toISOString().slice(0, 10)}`);
    const n = await espn.syncSchedule(from, to);
    log(`nba backfill: ${n} game rows upserted; fetching box scores`);
    let total = 0;
    for (;;) {
      const got = await espn.syncBoxscores(50);
      total += got;
      if (!got) break;
    }
    log(`nba backfill: ${total} box scores stored`);
    const r = await espn.syncInjuries();
    log(`nba backfill: injury report ${r.listed} listed`);
  } finally {
    await close();
  }
}

main().catch((e) => { log.err(e.stack || e.message); process.exit(1); });
