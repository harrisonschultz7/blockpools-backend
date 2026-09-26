#!/usr/bin/env node
// trading-bot/run/backfill-history.js
//
// Widens the CALIBRATION sample by pulling games, play-by-play, snap counts and
// injuries back to model.calibration.startSeason.
//
// WHY THIS IS A SEPARATE WINDOW from data.startSeason (2025). The 2025 cutoff
// exists because game-to-game TEAM matchups go stale. The calibration measures
// something different: how much of each factor the closing line already contains.
// That is a property of the MARKET, not of this year's rosters, and it does not
// go stale the way a team's form does. Same split already accepted for
// model.scheme.fitStartSeason.
//
// It matters because the standard errors are what is blocking every conclusion.
// At 318 games the scheme beta was -1.26 with t -1.24, meaning a 95% range of
// roughly -3.3 to +0.7 -- we cannot tell help from harm from nothing. Quadrupling
// the sample roughly halves that interval.
//
// NOTE: forecasting is unaffected. Nothing here widens what the live model reads;
// features still gate on data.startSeason.
//
//   node trading-bot/run/backfill-history.js

const { selectConfig, cfg } = require("../src/config");
selectConfig("config.argo-7.json");

const { close } = require("../src/db");
const log = require("../src/log");
const { ingestGames, ingestPbpSeason, ingestSnapCounts, ingestInjuries, currentSeason } =
  require("../src/ingest/nflverse");

(async () => {
  const c = cfg();
  const first = c.model.calibration.startSeason;
  const last = currentSeason();
  if (!Number.isFinite(first)) throw new Error("model.calibration.startSeason is not set");

  // ingestGames() and ingestPbpSeason() both read cfg().data.startSeason, so it
  // is widened IN MEMORY for this process only. config.json on disk is untouched,
  // and the live tick keeps its own narrower window.
  const original = c.data.startSeason;
  c.data.startSeason = first;
  log(`backfill: widening ingest window ${original} -> ${first} (in memory only)`);

  await ingestGames();
  for (let s = first; s <= last; s++) {
    log(`backfill: season ${s}`);
    await ingestPbpSeason(s);
    await ingestSnapCounts(s);
    await ingestInjuries(s);
  }
  c.data.startSeason = original;
  log("backfill: done -- now re-run run/calibrate-totals.js");
})()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
