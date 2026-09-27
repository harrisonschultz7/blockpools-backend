#!/usr/bin/env node
// trading-bot/run/tick-totals.js
//
// One decision pass over every upcoming NFL game total.
//
//   pick the deepest line -> forecast -> policy -> paper fill -> decision log
//
// Every game looked at produces a bots.decisions row whether or not it trades,
// which is what makes selectivity auditable. A week where Argo-7 trades 2 of 16
// games and a week where it trades 14 should be distinguishable at a glance
// rather than inferred from the trade table.
//
//   node trading-bot/run/tick-totals.js [--dry] [--window-hours N]

const { selectConfig, cfg } = require("../src/config");
selectConfig("config.argo-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const { loadSchemeContext } = require("../src/features/scheme");
const { loadPace, paceSignal } = require("../src/features/paceTotals");
const { solveRatings } = require("../src/features/teamStrength");
const { loadAbsoluteFit } = require("../src/model/fitAbsolute");
const { forecastTotal, saveForecastTotal, loadCalibration } = require("../src/model/forecastTotals");
const { decideTotals } = require("../src/policy/totals");
const { executePaper } = require("../src/exec/paper");
const { manageExits, createExitOrder } = require("../src/exec/limits");
const { currentNav, snapshotNav } = require("../src/accounting/nav");
const { gradeClv, settleTrades } = require("../src/accounting/settle");

const DRY = process.argv.includes("--dry");
// --window-hours widens the window for a DRY run only, so a full slate can be
// inspected before it is inside the real one. Refused otherwise, so it can never
// loosen live trading.
const WINDOW_ARG = (() => {
  const i = process.argv.indexOf("--window-hours");
  if (i < 0) return null;
  if (!DRY) throw new Error("--window-hours is only allowed with --dry");
  return Number(process.argv[i + 1]);
})();

/** Register the bot on first run so trades always have a parent row. */
async function ensureBot(c) {
  await q(
    // market_scope and description are written here, not by hand. Adam-7's were
    // set with a manual UPDATE against the live database, which is why Argo-7
    // first appeared on the leaderboard with a blank subtitle and no description.
    `insert into bots.bot
       (id, name, league, risk_tier, mode, config, starting_nav, market_scope, description)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     on conflict (id) do update set
       name = excluded.name, mode = excluded.mode, config = excluded.config,
       market_scope = coalesce(excluded.market_scope, bots.bot.market_scope),
       description  = coalesce(excluded.description,  bots.bot.description)`,
    [c.botId, c.botName, c.league, c.riskTier, c.mode,
     JSON.stringify(c), c.paper.startingNavUsd,
     c.marketScopeLabel || null, c.description || null],
  );
}

async function main() {
  const c = cfg();
  await ensureBot(c);

  const calibration = loadCalibration();
  if (!calibration.calibrated) {
    log.err("tick-totals: model/calibration-totals.json is missing. " +
            "Run run/calibrate-totals.js -- the model will not forecast without it.");
    return;
  }

  const now = new Date();
  const asOf = now.toISOString();

  // The whole upcoming NFL WEEK as one slate. A rolling hours-window judges each
  // game in isolation and makes the weekly exposure cap meaningless, because it
  // cannot ration capital across a slate it cannot see.
  const weekRows = await q(
    `select season, week from sports.nfl_games where kickoff > now() order by kickoff limit 1`);
  if (!weekRows.rows.length) { log("tick-totals: no upcoming games"); return; }
  const { season, week } = weekRows.rows[0];

  // The DEEPEST line per game, chosen in SQL so the bot cannot accidentally look
  // at several lines on one game. Verified live: the deepest line is always the
  // one nearest 50/50, so this is also the closest-to-a-coin-flip rule.
  const { rows: games } = await q(
    `select g.*, t.condition_id, t.line, t.over_token_id, t.under_token_id,
            t.liquidity_num, t.slug, t.fee_type, t.fee_rate, t.tick_size, t.min_order_usd
       from sports.nfl_games g
       join lateral (
         select * from sports.pm_totals_markets m
          where m.game_id = g.game_id and m.closed = false
            and m.over_token_id is not null
          order by m.liquidity_num desc nulls last limit 1
       ) t on true
      where g.season = $1 and g.week = $2 and g.kickoff > now()
      order by g.kickoff`,
    [season, week],
  );
  log(`tick-totals: evaluating ${season} week ${week} slate (${games.length} games with a totals book)`);
  if (!games.length) { log("tick-totals: no mapped totals markets for this week yet"); return; }

  // Contexts are expensive and identical across games -- build once.
  //
  // Ratings use model.absolute.fitStartSeason, the SAME window the base model was
  // fitted on. Mismatching them would apply coefficients to inputs on a different
  // scale. Recency weighting means the wider window barely touches a current
  // rating anyway -- a game 40 back carries ~0.001 of last week's weight.
  const independent = c.model.priceMode === "independent";
  const ratingsFrom = c.model.absolute.fitStartSeason || c.data.startSeason;
  const [scheme, pace, ratings] = await Promise.all([
    loadSchemeContext(season, week),
    loadPace(asOf, c.data.startSeason),
    independent ? solveRatings(asOf, { fromSeason: ratingsFrom }) : Promise.resolve(new Map()),
  ]);

  const absoluteFit = loadAbsoluteFit();
  if (independent && !absoluteFit.fitted) {
    log.err("tick-totals: priceMode is 'independent' but model/absolute-fit.json is missing. " +
            "Run run/fit-absolute.js -- there is no base total to price from.");
    return;
  }
  if (independent) {
    log(`tick-totals: INDEPENDENT price mode. base model r2 ${absoluteFit.r2OutOfSample} ` +
        `out of sample, game sigma ${absoluteFit.gameSigmaOutOfSample} pts ` +
        `(closing line's own error: ${calibration.sigma} pts)`);
  }
  const ctx = { scheme, pace, ratings, absoluteFit, calibration };

  const nav = await currentNav(c.botId);
  const weekRow = await q(
    `select coalesce(sum(notional_usd), 0) v from bots.trades
      where bot_id = $1 and settled = false`, [c.botId]);
  let weekExposure = Number(weekRow.rows[0].v);

  let traded = 0;
  let dryNotional = 0;
  const forecasts = new Map();
  const candidates = [];

  for (const game of games) {
    const market = {
      condition_id: game.condition_id, line: game.line, slug: game.slug,
      liquidity_num: game.liquidity_num, fee_type: game.fee_type,
      fee_rate: game.fee_rate, tick_size: game.tick_size,
      min_order_usd: game.min_order_usd,
    };

    // Latest recorded book per side, for THIS market -- keyed on condition_id,
    // not game_id, because a game has dozens of totals markets.
    const { rows: books } = await q(
      `select distinct on (side) * from sports.odds_history
        where condition_id = $1 and side in ('over','under')
        order by side, ts desc`,
      [game.condition_id],
    );
    const bySide = { over: books.find((b) => b.side === "over"),
                     under: books.find((b) => b.side === "under") };

    // The base model needs this game's expected play count, which is per-matchup
    // rather than per-slate, so it is computed here and handed down.
    const gameCtx = { ...ctx, paceDetail: paceSignal(game, pace).detail };
    const forecast = await forecastTotal(game, market, bySide, asOf, gameCtx);
    const label = `${game.away_team}@${game.home_team} O/U${game.line}`;

    if (!forecast || forecast.skip) {
      const why = (forecast && forecast.skip) || "no_forecast";
      await logDecision(c.botId, game, market, null, { acted: false, skip_reason: why }, null);
      log(`  SKIP  ${label.padEnd(20)} ${why}`);
      continue;
    }
    // Keyed by the MARKET this forecast is for. A forecast for Over-41.5 must
    // never be used to reprice a resting order on Over-42.5.
    forecasts.set(game.condition_id, forecast);

    const existing = await q(
      `select 1 from bots.positions
        where bot_id = $1 and game_id = $2 and market_type = 'totals' and shares > 0`,
      [c.botId, game.game_id]);

    const decision = decideTotals({
      forecast, game, market, books: bySide, nav,
      windowOverrideHours: WINDOW_ARG,
      existingPosition: existing.rows.length > 0,
      weekExposureUsd: weekExposure, now,
    });

    if (!decision.acted) {
      await logDecision(c.botId, game, market, forecast, decision, null);
      log(`  SKIP  ${label.padEnd(20)} ${decision.skip_reason}` +
          (decision.edge !== undefined ? ` (edge ${(decision.edge * 100).toFixed(1)}c)` : "") +
          `  [book ${forecast.implied_mean_total.toFixed(1)} model ${forecast.model_mean_total.toFixed(1)}]`);
      continue;
    }

    // COLLECTED, not funded yet. See the allocation pass below.
    candidates.push({ game, market, bySide, decision, forecast });
  }

  // ---- allocation: best edge first ---------------------------------------
  // The weekly cap is a real constraint -- on the 2026 week-3 slate the slate
  // wanted 37% of NAV against a 25% cap. Funding in kickoff order means an early
  // 4-cent edge crowds out a late 16-cent one, which is allocation by accident.
  // evaluationMode "week" exists precisely so capital can be rationed across a
  // slate the bot can see all of; this is the part that actually does it.
  candidates.sort((x, y) => y.decision.edge - x.decision.edge);

  const weekBudget = nav * c.policy.maxWeeklyExposurePctNav;
  for (const cand of candidates) {
    const room = Math.max(0, weekBudget - weekExposure);
    const minOrder = Number(cand.market.min_order_usd) || 0;
    if (room < Math.max(minOrder, 1)) {
      await logDecision(c.botId, cand.game, cand.market, cand.forecast,
                        { acted: false, skip_reason: "exposure_cap", edge: cand.decision.edge }, null);
      log(`  CUT   ${(cand.game.away_team + "@" + cand.game.home_team).padEnd(20)} ` +
          `edge ${(cand.decision.edge * 100).toFixed(1)}c -- weekly budget exhausted`);
      continue;
    }
    // Trim the last funded position to the remaining room rather than dropping it.
    const notional = Math.min(cand.decision.notionalUsd, room);
    const decision = notional < cand.decision.notionalUsd
      ? { ...cand.decision, notionalUsd: notional, shares: notional / cand.decision.price }
      : cand.decision;

    const label = `${cand.game.away_team}@${cand.game.home_team} O/U${cand.game.line}`;
    log(`  TRADE ${label.padEnd(20)} ${decision.side.toUpperCase()} @ ${decision.price.toFixed(2)} ` +
        `edge ${(decision.edge * 100).toFixed(1)}c size ${decision.notionalUsd.toFixed(2)}` +
        (notional < cand.decision.notionalUsd ? " (trimmed to budget)" : "") +
        ` [${decision.pointsVsMarket > 0 ? "+" : ""}${decision.pointsVsMarket} pts vs book]`);

    traded++;
    dryNotional += decision.notionalUsd;
    weekExposure += decision.notionalUsd;
    if (DRY) continue;
    await openPosition(c, cand.game, cand.market, cand.bySide, decision, cand.forecast,
                       () => weekExposure, (v) => { weekExposure = v; }, () => {});
  }

  if (!DRY) await manageExits(c.botId, forecasts, now);

  // RECONCILE EVERY TICK, not once a day.
  //
  // Settlement used to live only in run/daily.js, so a game finishing at 16:00
  // was not marked won or lost until the 05:00 run: the record, the realised
  // P&L and the NAV curve all sat eighteen hours behind the scoreboard. Adam-7's
  // page showed +2.17% ROI beside +$303.53 of realised P&L with nothing open,
  // which is not a rounding disagreement -- it is two numbers from different
  // days on the same card.
  //
  // All three calls are idempotent and only touch rows that have changed, so
  // running them every 15 minutes costs a few cheap queries and keeps the page
  // within a tick of the truth.
  if (!DRY) {
    try {
      await gradeClv();
      await settleTrades();
      await snapshotNav(c.botId);
    } catch (e) {
      // A reconcile failure must not lose the trading work already done above.
      log.warn(`reconcile failed (will retry next tick): ${e.message}`);
    }
  }

  // Exposure is reported alongside the count because the count alone hides the
  // thing that matters: nine trades at the 5%-of-NAV cap is most of the weekly
  // budget, and a bot that is meant to be selective should not be quietly
  // spending it.
  const pctNav = nav > 0 ? (100 * dryNotional / nav).toFixed(1) : "n/a";
  log(`tick-totals: looked at ${games.length} games, traded ${traded}` +
      ` (${dryNotional.toFixed(0)} = ${pctNav}% of ${nav.toFixed(0)} NAV,` +
      ` weekly cap ${(100 * c.policy.maxWeeklyExposurePctNav).toFixed(0)}%)` +
      (DRY ? " (DRY RUN)" : ""));
}

/**
 * Save the forecast, take the paper fill, stamp the totals-specific columns and
 * rest the exit.
 *
 * Split out of main() because it is the only part that writes, and the write path
 * is where a market-type mix-up would do real damage -- executePaper() is shared
 * with Adam-7, so the totals columns have to be applied here rather than inside it.
 */
async function openPosition(c, game, market, bySide, decision, forecast,
                            getExposure, setExposure, onTraded) {
  const featureId = await saveForecastTotal(forecast);
  const book = bySide[decision.side];

  const fill = await executePaper({
    botId: c.botId, game, decision, forecast,
    // feature_id points at sports.features (the moneyline table) and must stay
    // null for a totals trade; feature_totals_id carries the reference instead.
    book: { ...book, condition_id: game.condition_id }, featureId: null,
  });
  if (!fill.filled) {
    await logDecision(c.botId, game, market, forecast,
                      { acted: false, skip_reason: fill.reason }, null, featureId);
    return;
  }

  await q(
    `update bots.trades set market_type = 'totals', line = $2, feature_totals_id = $3
      where id = $1`,
    [fill.tradeId, decision.line, featureId]);
  await q(
    `update bots.positions set market_type = 'totals', line = $3
      where bot_id = $1 and token_id = $2`,
    [c.botId, book.token_id, decision.line]);

  await logDecision(c.botId, game, market, forecast, decision, fill.tradeId, featureId);

  if (c.policy.exitAtFairValue) {
    await createExitOrder({
      botId: c.botId, tradeId: fill.tradeId, game, decision, forecast,
      fill: { ...fill, tokenId: book.token_id, conditionId: game.condition_id },
    });
  }
  setExposure(getExposure() + Number(fill.costUsd));
  onTraded();
}

async function logDecision(botId, game, market, forecast, decision, tradeId, featureTotalsId) {
  if (DRY) return;
  // For a total the market's "favourite" is whichever side of the line it leans
  // to. Reused rather than renamed so one bias audit covers both bots: if Argo-7
  // only ever scores on unders, that is a bias, not an edge.
  const favSide = forecast ? (forecast.p_market >= 0.5 ? "over" : "under") : null;
  await q(
    `insert into bots.decisions
       (bot_id, game_id, p_market, p_fair, edge, acted, skip_reason,
        bet_side, fav_side, bet_on_dog, trade_id, market_type, line, feature_totals_id)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'totals',$12,$13)`,
    [botId, game.game_id,
     forecast ? forecast.p_market : null,
     forecast ? forecast.p_fair : null,
     decision.edge ?? null,
     !!decision.acted, decision.skip_reason || null,
     decision.side || null, favSide,
     decision.side && favSide ? favSide !== decision.side : null,
     tradeId, market ? Number(market.line) : null, featureTotalsId ?? null],
  );
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
