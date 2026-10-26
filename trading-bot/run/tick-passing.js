#!/usr/bin/env node
// trading-bot/run/tick-passing.js
//
// One decision pass over every QB passing-yards prop in the window (Bruno-7).
//
//   central sticky line per QB -> forecast -> starter gate -> policy
//     -> paper fill -> resting exit -> decision log
//
// The Argo-7 tick (run/tick-totals.js) with the passing model swapped in. It
// replaces run/forecast-passing.js on the 15-minute timer: every QB looked at
// still gets a sports.features_passing row whether or not it trades, so the
// line-residual calibration keeps its raw material.
//
//   node trading-bot/run/tick-passing.js [--dry] [--window-hours N]
//
// --dry writes NOTHING -- not the bot row, not forecasts, not decisions -- so it
// can be run against production while Bruno-7 is still unregistered.

const { selectConfig, cfg } = require("../src/config");
selectConfig("config.bruno-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const {
  buildPassingContext, forecastPassing, saveForecastPassing,
  selectCentralLines, starterCheck,
} = require("../src/model/forecastPassing");
const { decidePassing } = require("../src/policy/passing");
const { executePaper } = require("../src/exec/paper");
const { manageExits, createExitOrder } = require("../src/exec/limits");
const { currentNav, snapshotNav, snapshotNavIntraday } = require("../src/accounting/nav");
const { gradeClv, settleTrades, settleFromMarkets } = require("../src/accounting/settle");

const DRY = process.argv.includes("--dry");
const WINDOW_ARG = (() => {
  const i = process.argv.indexOf("--window-hours");
  if (i < 0) return null;
  if (!DRY) throw new Error("--window-hours is only allowed with --dry");
  return Number(process.argv[i + 1]);
})();

/** Register the bot on first run so trades always have a parent row. */
async function ensureBot(c) {
  await q(
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

async function latestBooksByToken(tokenIds) {
  if (!tokenIds.length) return new Map();
  const { rows } = await q(
    `select o.*
       from unnest($1::text[]) t(token_id)
       join lateral (
         select * from sports.odds_history o
          where o.token_id = t.token_id order by o.ts desc limit 1
       ) o on true`,
    [tokenIds],
  );
  return new Map(rows.map((r) => [r.token_id, r]));
}

async function main() {
  const c = cfg();
  if (!DRY) await ensureBot(c);

  const now = new Date();
  const asOf = now.toISOString();
  const hours = WINDOW_ARG || c.passingModel.forecastWindowHours
             || c.ingest.passingRecordWindowHours || 30;

  const { rows: markets } = await q(
    `select m.condition_id, m.slug, m.player_id, m.player_name, m.line,
            m.over_token_id, m.under_token_id, m.fee_type, m.fee_rate, m.tick_size,
            m.min_order_usd,
            g.game_id, g.season, g.week, g.home_team, g.away_team, g.roof, g.wind,
            g.spread_line, g.total_line, g.kickoff
       from sports.pm_passing_markets m
       join sports.nfl_games g on g.game_id = m.game_id
      where m.closed = false and m.over_token_id is not null
        and g.home_score is null
        and g.kickoff between now() and now() + ($1 || ' hours')::interval`,
    [String(hours)],
  );

  const forecasts = new Map();   // condition_id -> forecast, for exit repricing
  let traded = 0;
  let dryNotional = 0;
  let looked = 0;
  const nav = await currentNav(c.botId);

  if (!markets.length) {
    log("tick-passing: no open passing props in window");
  } else {
    const tokens = [];
    for (const m of markets) { tokens.push(m.over_token_id); if (m.under_token_id) tokens.push(m.under_token_id); }
    const books = await latestBooksByToken(tokens);

    const { rows: prior } = await q(
      `select distinct on (f.player_id, f.game_id) f.player_id, f.game_id, f.condition_id
         from sports.features_passing f
        where f.game_id = any($1::text[])
        order by f.player_id, f.game_id, f.asof_ts asc`,
      [Array.from(new Set(markets.map((m) => m.game_id)))],
    );
    const sticky = new Map(prior.map((r) => [`${r.player_id}|${r.game_id}`, r.condition_id]));
    const byQb = selectCentralLines(markets, books, sticky);

    const ctxCache = new Map();
    const getCtx = async (season, week) => {
      const k = `${season}:${week}`;
      if (!ctxCache.has(k)) ctxCache.set(k, await buildPassingContext(season, week));
      return ctxCache.get(k);
    };

    const { rows: expRows } = await q(
      `select coalesce(sum(notional_usd), 0) v from bots.trades
        where bot_id = $1 and settled = false`, [c.botId]);
    let openExposure = Number(expRows[0].v);

    log(`tick-passing: ${byQb.size} QB(s) with a real central book, ${markets.length} prop lines in ${hours}h`);
    const candidates = [];

    for (const { m, over, under } of byQb.values()) {
      looked++;
      const game = {
        game_id: m.game_id, home_team: m.home_team, away_team: m.away_team,
        roof: m.roof, wind: m.wind, spread_line: m.spread_line, total_line: m.total_line,
        kickoff: m.kickoff,
      };
      const bySide = { over, under };
      const label = `${m.player_name} ${m.line}`.padEnd(26);
      const ctx = await getCtx(m.season, m.week);
      const forecast = forecastPassing(game, m, bySide, m.player_id, asOf, ctx);

      if (forecast.skip) {
        await logDecision(c.botId, game, m, null, { acted: false, skip_reason: forecast.skip }, null, null);
        log(`  SKIP  ${label} ${forecast.skip}`);
        continue;
      }
      const featureId = DRY ? null : await saveForecastPassing(forecast);
      forecasts.set(m.condition_id, forecast);

      const verdict = starterCheck(m.player_id, forecast.inputs.team, ctx,
                                   { skipQuestionable: c.policy.skipQuestionableQb !== false });
      const existing = await q(
        `select 1 from bots.trades t
           join sports.features_passing f on f.id = t.feature_passing_id
          where t.bot_id = $1 and t.game_id = $2 and f.player_id = $3
            and t.settled = false limit 1`,
        [c.botId, m.game_id, m.player_id]);

      const decision = decidePassing({
        forecast, game, market: m, books: bySide, nav,
        windowOverrideHours: WINDOW_ARG, existingPosition: existing.rows.length > 0,
        openExposureUsd: openExposure, now, starterVerdict: verdict,
      });

      if (!decision.acted) {
        await logDecision(c.botId, game, m, forecast, decision, null, featureId);
        log(`  SKIP  ${label} ${decision.skip_reason}` +
            (decision.edge !== undefined ? ` (edge ${(decision.edge * 100).toFixed(1)}c)` : "") +
            `  [book ${forecast.implied_mean_yards.toFixed(0)}yd model ${forecast.model_mean_yards.toFixed(0)}yd]`);
        continue;
      }
      candidates.push({ game, market: m, bySide, decision, forecast, featureId });
    }

    // ---- allocation: best edge first (see tick-totals.js) ------------------
    candidates.sort((x, y) => y.decision.edge - x.decision.edge);
    const openBudget = nav * c.policy.maxOpenExposurePctNav;
    for (const cand of candidates) {
      const label = `${cand.market.player_name} ${cand.market.line}`.padEnd(26);
      const room = Math.max(0, openBudget - openExposure);
      const minOrder = Number(cand.market.min_order_usd) || 0;
      if (room < Math.max(minOrder, 1)) {
        await logDecision(c.botId, cand.game, cand.market, cand.forecast,
                          { acted: false, skip_reason: "exposure_cap", edge: cand.decision.edge },
                          null, cand.featureId);
        log(`  CUT   ${label} edge ${(cand.decision.edge * 100).toFixed(1)}c -- open-exposure budget exhausted`);
        continue;
      }
      const notional = Math.min(cand.decision.notionalUsd, room);
      const decision = notional < cand.decision.notionalUsd
        ? { ...cand.decision, notionalUsd: notional, shares: notional / cand.decision.price }
        : cand.decision;

      log(`  TRADE ${label} ${decision.side.toUpperCase()} @ ${decision.price.toFixed(3)} ` +
          `edge ${(decision.edge * 100).toFixed(1)}c net of ${(decision.fee * 100).toFixed(1)}c fee, ` +
          `size ${decision.notionalUsd.toFixed(2)} [${decision.yardsVsMarket > 0 ? "+" : ""}${decision.yardsVsMarket} yds vs book]`);
      traded++;
      dryNotional += decision.notionalUsd;
      if (DRY) { openExposure += decision.notionalUsd; continue; }
      const cost = await openPosition(c, cand, decision, nav);
      openExposure += cost;
    }
  }

  if (!DRY) await manageExits(c.botId, forecasts, now);

  // Reconcile every tick, exactly as Argo-7 does -- idempotent and cheap.
  if (!DRY) {
    try {
      await gradeClv();
      await settleTrades();
      await settleFromMarkets();
      await snapshotNav(c.botId);
      await snapshotNavIntraday(c.botId);
    } catch (e) {
      log.warn(`reconcile failed (will retry next tick): ${e.message}`);
    }
  }

  const pctNav = nav > 0 ? (100 * dryNotional / nav).toFixed(1) : "n/a";
  log(`tick-passing: looked at ${looked} QBs, traded ${traded}` +
      ` (${dryNotional.toFixed(0)} = ${pctNav}% of ${nav.toFixed(0)} NAV,` +
      ` open cap ${(100 * c.policy.maxOpenExposurePctNav).toFixed(0)}%)` +
      (DRY ? " (DRY RUN)" : ""));
}

/**
 * Paper fill, stamp the passing columns, rest the exit. Returns the cost filled.
 * executePaper() is shared with Adam-7 and Argo-7, so market_type / line /
 * feature_passing_id are applied here rather than inside it -- the same split
 * tick-totals.js uses, and the place a market-type mix-up would do real damage.
 */
async function openPosition(c, cand, decision, nav) {
  const { game, market, bySide, forecast, featureId } = cand;
  const book = bySide[decision.side];
  const fill = await executePaper({
    botId: c.botId, game, decision, forecast,
    book: { ...book, condition_id: market.condition_id }, featureId: null, nav,
  });
  if (!fill.filled) {
    await logDecision(c.botId, game, market, forecast,
                      { acted: false, skip_reason: fill.reason }, null, featureId);
    return 0;
  }

  await q(
    `update bots.trades set market_type = 'passing', line = $2, feature_passing_id = $3
      where id = $1`,
    [fill.tradeId, decision.line, featureId]);
  await q(
    `update bots.positions set market_type = 'passing', line = $3
      where bot_id = $1 and token_id = $2`,
    [c.botId, book.token_id, decision.line]);

  await logDecision(c.botId, game, market, forecast, decision, fill.tradeId, featureId);

  if (c.policy.exitAtFairValue) {
    await createExitOrder({
      botId: c.botId, tradeId: fill.tradeId, game, decision, forecast,
      fill: { ...fill, tokenId: book.token_id, conditionId: market.condition_id },
    });
  }
  if (fill.unfilledShares > 0) {
    log(`    partial fill: ${fill.filledShares.toFixed(0)} of ${decision.shares.toFixed(0)} shares (book depth)`);
  }
  return Number(fill.costUsd);
}

async function logDecision(botId, game, market, forecast, decision, tradeId, featurePassingId) {
  if (DRY) return;
  // "Favourite" for a prop = the side the market leans to, as for totals, so the
  // same bias audit (does it only ever bet unders?) covers all three bots.
  const favSide = forecast ? (forecast.p_market >= 0.5 ? "over" : "under") : null;
  await q(
    `insert into bots.decisions
       (bot_id, game_id, p_market, p_fair, edge, acted, skip_reason,
        bet_side, fav_side, bet_on_dog, trade_id, market_type, line, feature_passing_id)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'passing',$12,$13)`,
    [botId, game.game_id,
     forecast ? forecast.p_market : null,
     forecast ? forecast.p_fair : null,
     decision.edge ?? null,
     !!decision.acted, decision.skip_reason || null,
     decision.side || null, favSide,
     decision.side && favSide ? favSide !== decision.side : null,
     tradeId, market ? Number(market.line) : null, featurePassingId ?? null],
  );
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
