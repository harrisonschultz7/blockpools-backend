#!/usr/bin/env node
// trading-bot/run/daily.js
//
// Daily maintenance: refresh source data, grade and settle finished games, then
// mark NAV. Order matters -- settle before NAV, or the NAV row counts a finished
// position at its last book mid instead of at its payout.
//
// COVERS EVERY ENABLED BOT. The data refresh, CLV grading and settlement are
// global (they operate on all trades), while NAV and the summary are per-bot. So
// this runs the global steps once and then loops the bots, rather than being run
// once per config -- which would re-ingest nflverse and re-grade every trade for
// each bot, and make the logs read as though settlement had happened twice.
//
//   node trading-bot/run/daily.js

const { selectConfig } = require("../src/config");
const { ingestAll } = require("../src/ingest/nflverse");
const { ingestWeather } = require("../src/ingest/weather");
const { ingestFtnAll } = require("../src/ingest/ftn");
const { gradeClv, settleTrades, settleFromMarkets, botSummary } = require("../src/accounting/settle");
const { snapshotNav, pruneNavIntraday } = require("../src/accounting/nav");
const { applyRiskTiers } = require("../src/accounting/riskTier");
const { q, close } = require("../src/db");
const log = require("../src/log");

async function main() {
  await ingestAll();
  await ingestWeather();
  // FTN charting for the scheme factor. Re-pulled daily because it LAGS the
  // schedule -- games get charted days after they are played -- so the useful
  // behaviour is to keep asking rather than to fetch once per week.
  //
  // Run under ARGO'S config on purpose. ingestFtnAll() reads
  // data.schemePriorSeason and model.scheme.neutral, neither of which exists in
  // Adam-7's config, and a missing key here does not throw -- the season filter
  // silently matches nothing and the ingest reports 0 rows as though FTN had no
  // data. Same failure shape as the missing replacementScaleEpa key that made
  // injury points NaN, so it is made explicit rather than left to whichever
  // config happens to be active.
  try {
    selectConfig("config.argo-7.json");
    await ingestFtnAll();
  } catch (e) {
    log.warn(`ftn refresh failed (scheme factor will use stale traits): ${e.message}`);
  } finally {
    selectConfig("config.json");
  }

  await gradeClv();       // freeze closing prices for kicked-off games
  await settleTrades();   // pay out finished games
  await settleFromMarkets();  // and any the exchange resolved before the box score landed

  const { rows: bots } = await q(`select id, name from bots.bot where enabled order by id`);
  if (!bots.length) { log("daily: no enabled bots registered yet"); return; }

  // Intraday points older than the retention window; the daily series below
  // already covers that history at the grain the track record needs.
  try { await pruneNavIntraday(14); }
  catch (e) { log.warn(`nav_intraday prune failed: ${e.message}`); }

  for (const b of bots) {
    await snapshotNav(b.id);
    const s = await botSummary(b.id);
    log(`${b.name}: ${s.wins}W-${s.losses}L  nav ${Number(s.nav).toFixed(2)} ` +
        `(${Number(s.roi_pct).toFixed(2)}%)  mean CLV ` +
        `${s.mean_clv_bps === null ? "n/a" : Number(s.mean_clv_bps).toFixed(0) + "bps"} ` +
        `over ${s.clv_graded} graded`);
  }

  // Risk tiers LAST, after NAV is marked -- the turnover basis divides by NAV, so
  // running it first would price today's trades against yesterday's portfolio.
  //
  // Here rather than on its own timer because the tier is derived: a bot that
  // starts trading more often, or widens its exit targets, has genuinely become
  // riskier, and a label left to go stale is worse than no label at all.
  try {
    const tiers = await applyRiskTiers({ write: true });
    for (const t of tiers) {
      if (t.changed) {
        log(`risk tier: ${t.name} ${t.previousTier} -> ${t.tier} ` +
            `(score ${(100 * t.riskScore).toFixed(1)}%)`);
      }
    }
  } catch (e) {
    log.warn(`risk tier refresh failed (tiers left as they were): ${e.message}`);
  }
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
