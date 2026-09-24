#!/usr/bin/env node
// trading-bot/run/tick.js
//
// One decision pass over every upcoming NFL game.
//
//   forecast -> policy -> paper fill -> decision log
//
// Every game looked at produces a bots.decisions row whether or not it trades,
// which is what makes the bot's selectivity auditable. A week where it trades
// 2 of 16 games and a week where it trades 14 should be distinguishable at a
// glance, not inferred from the trade table.
//
//   node trading-bot/run/tick.js [--dry]

const { cfg } = require("../src/config");
const { q, close } = require("../src/db");
const log = require("../src/log");
const { solveRatings } = require("../src/features/teamStrength");
const { loadProfiles } = require("../src/features/matchup");
const { forecastGame, saveForecast } = require("../src/model/forecast");
const { decide } = require("../src/policy/medium");
const { executePaper } = require("../src/exec/paper");
const { manageExits, createExitOrder } = require("../src/exec/limits");
const { currentNav } = require("../src/accounting/nav");

const DRY = process.argv.includes("--dry");
// --window-hours widens the trading window for a dry run only. Useful to see
// how the policy behaves across a full slate before the slate is inside the
// real window; refused outside --dry so it can never loosen live trading.
const WINDOW_ARG = (() => {
  const i = process.argv.indexOf("--window-hours");
  if (i < 0) return null;
  if (!DRY) throw new Error("--window-hours is only allowed with --dry");
  return Number(process.argv[i + 1]);
})();

/** Register the bot on first run so trades always have a parent row. */
async function ensureBot(c) {
  await q(
    `insert into bots.bot (id, name, league, risk_tier, mode, config, starting_nav)
     values ($1,$2,$3,$4,$5,$6,$7)
     on conflict (id) do update set
       name = excluded.name, mode = excluded.mode, config = excluded.config`,
    [c.botId, c.botName, c.league, c.riskTier, c.mode,
     JSON.stringify(c), c.paper.startingNavUsd],
  );
}

async function main() {
  const c = cfg();
  await ensureBot(c);

  const now = new Date();
  const asOf = now.toISOString();

  // Candidate games: the whole upcoming NFL WEEK as one slate.
  //
  // A rolling hours-window was the original approach and it was wrong: with a
  // 72h window only 1 of Week 3's 16 games was visible at a time, so the bot
  // judged each game in isolation and the weekly exposure cap could not ration
  // capital across a slate it could not see. The week is the natural unit --
  // NFL scheduling, injury reports and the bot's own budget all move on it.
  const weekRows = await q(
    `select season, week from sports.nfl_games
      where kickoff > now() order by kickoff limit 1`,
  );
  if (!weekRows.rows.length) { log("tick: no upcoming games"); return; }
  const { season, week } = weekRows.rows[0];

  const { rows: games } = await q(
    `select g.*, m.condition_id
       from sports.nfl_games g
       join sports.pm_markets m on m.game_id = g.game_id and m.closed = false
      where g.season = $1 and g.week = $2 and g.kickoff > now()
      order by g.kickoff`,
    [season, week],
  );
  log(`tick: evaluating ${season} week ${week} slate (${games.length} games)`);

  if (!games.length) { log("tick: no mapped markets for this week yet"); return; }

  // Ratings and profiles are expensive and identical across games -- solve once.
  const [ratings, profiles] = await Promise.all([solveRatings(asOf), loadProfiles(asOf)]);
  const ctx = { ratings, profiles };

  const nav = await currentNav(c.botId);
  const weekRow = await q(
    `select coalesce(sum(notional_usd), 0) v from bots.trades
      where bot_id = $1 and settled = false`, [c.botId]);
  let weekExposure = Number(weekRow.rows[0].v);

  let traded = 0;
  const forecasts = new Map();
  for (const game of games) {
    // Latest recorded book for each side.
    const { rows: books } = await q(
      `select distinct on (side) * from sports.odds_history
        where game_id = $1 order by side, ts desc`,
      [game.game_id],
    );
    const bySide = { home: books.find((b) => b.side === "home"),
                     away: books.find((b) => b.side === "away") };

    const forecast = await forecastGame(game, bySide, asOf, ctx);
    if (forecast) forecasts.set(game.game_id, forecast);
    if (!forecast) {
      await logDecision(c.botId, game, null, { acted: false, skip_reason: "no_book" }, null);
      continue;
    }

    const existing = await q(
      `select 1 from bots.positions where bot_id = $1 and game_id = $2 and shares > 0`,
      [c.botId, game.game_id]);

    const decision = decide({
      forecast, game, books: bySide, nav,
      windowOverrideHours: WINDOW_ARG,
      existingPosition: existing.rows.length > 0,
      weekExposureUsd: weekExposure, now,
    });

    const label = `${game.away_team} @ ${game.home_team}`;
    if (!decision.acted) {
      await logDecision(c.botId, game, forecast, decision, null);
      log(`  SKIP ${label.padEnd(12)} ${decision.skip_reason}` +
          (decision.edge !== undefined ? ` (edge ${(decision.edge * 100).toFixed(1)}c)` : ""));
      continue;
    }

    const featureId = DRY ? null : await saveForecast(forecast);
    log(`  TRADE ${label.padEnd(12)} ${decision.side.toUpperCase()} @ ${decision.price.toFixed(2)} ` +
        `edge ${(decision.edge * 100).toFixed(1)}c size $${decision.notionalUsd.toFixed(2)} ` +
        `${decision.betOnDog ? "[DOG]" : "[FAV]"}`);

    if (DRY) continue;

    const fill = await executePaper({
      botId: c.botId, game, decision, forecast,
      book: bySide[decision.side], featureId,
    });
    if (!fill.filled) {
      await logDecision(c.botId, game, forecast, { acted: false, skip_reason: fill.reason }, null);
      continue;
    }
    await logDecision(c.botId, game, forecast, decision, fill.tradeId);
    if (c.policy.exitAtFairValue) {
      await createExitOrder({
        botId: c.botId, tradeId: fill.tradeId, game, decision, forecast,
        fill: { ...fill, tokenId: bySide[decision.side].token_id },
      });
    }
    weekExposure += Number(fill.costUsd);
    traded++;
  }

  if (!DRY) await manageExits(c.botId, forecasts, now);

  log(`tick: looked at ${games.length} games, traded ${traded}` + (DRY ? " (DRY RUN)" : ""));
}

async function logDecision(botId, game, forecast, decision, tradeId) {
  if (DRY) return;
  await q(
    `insert into bots.decisions
       (bot_id, game_id, p_market, p_fair, edge, acted, skip_reason,
        bet_side, fav_side, bet_on_dog, trade_id)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
    [botId, game.game_id,
     forecast ? forecast.p_market : null,
     forecast ? forecast.p_fair : null,
     decision.edge ?? null,
     !!decision.acted, decision.skip_reason || null,
     decision.side || null, decision.favSide || null,
     decision.betOnDog ?? null, tradeId],
  );
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
