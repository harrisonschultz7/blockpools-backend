#!/usr/bin/env node
// trading-bot/run/tick-nba.js
//
// Axis-3 / Bain-3 -- one decision pass over NBA games about to tip. The config
// picks the bot (TRADING_BOT_CONFIG, set per systemd unit).
//
//   refresh ESPN (today's board, injury report, new box scores)
//   -> forecast every game inside the 2h PRICING window
//   -> trade only inside the 45-minute TRADING window
//   -> exits, closing prices, settlement, NAV
//
// The two windows are the design (user, 2026-11-01): price early so the record
// shows the build-up, trade late because starting lineups and late scratches land
// ~30 minutes before tip, and the injury factor is the heaviest one. Runs every
// 5 minutes (systemd timer); a game sits in the trading window for ~8 ticks.
//
//   node trading-bot/run/tick-nba.js [--dry]

const { selectConfig, cfg } = require("../src/config");
selectConfig(process.env.TRADING_BOT_CONFIG || "config.axis-3.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const espn = require("../src/ingest/espnNba");
const { loadNbaContext } = require("../src/features/nba/context");
const { loadRatings } = require("../src/features/nba/ratings");
const { forecastNbaGame } = require("../src/model/forecastNba");
const { saveForecast } = require("../src/model/forecast");
const { decide } = require("../src/policy/medium");
const { executePaper } = require("../src/exec/paper");
const { manageExits, createExitOrder } = require("../src/exec/limits");
const { currentNav, snapshotNav, snapshotNavIntraday } = require("../src/accounting/nav");
const { gradeClv, settleTrades, settleFromMarkets } = require("../src/accounting/settle");

const DRY = process.argv.includes("--dry");

async function ensureBot(c) {
  await q(
    `insert into bots.bot
       (id, name, league, risk_tier, mode, config, starting_nav, market_scope, description)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9)
     on conflict (id) do update set
       name = excluded.name, mode = excluded.mode, config = excluded.config,
       market_scope = coalesce(excluded.market_scope, bots.bot.market_scope),
       description  = coalesce(excluded.description,  bots.bot.description)`,
    [c.botId, c.botName, c.league, c.riskTier, c.mode, JSON.stringify(c),
     c.paper.startingNavUsd, c.marketScopeLabel || null, c.description || null],
  );
}

/** Pull today's ESPN state. A failure here degrades the tick, never stops it. */
async function refreshData(c, now) {
  const from = new Date(now.getTime() - 86_400_000);
  const to = new Date(now.getTime() + c.ingest.scheduleDaysAhead * 86_400_000);
  try { await espn.syncSchedule(from, to); } catch (e) { log.warn(`schedule: ${e.message}`); }
  try {
    const r = await espn.syncInjuries();
    log(`injuries: ${r.listed} listed, ${r.cleared} cleared`);
  } catch (e) { log.warn(`injuries: ${e.message} -- the injury factor's confidence drops`); }
  try { await espn.syncBoxscores(20); } catch (e) { log.warn(`box scores: ${e.message}`); }
}

/**
 * The market's home probability at tip, kept on the game row for calibration
 * (the books themselves are pruned after the final -- run/daily.js).
 */
async function captureClosing() {
  await q(
    `update sports.nba_games g
        set closing_home_mid = (
          select o.mid from sports.odds_history o
           where o.game_id = g.game_id and o.side = 'home' and o.mid is not null
             and o.ts <= g.kickoff
           order by o.ts desc limit 1)
      where g.closing_home_mid is null
        and g.kickoff <= now() and g.kickoff > now() - interval '2 days'`,
  );
}

async function main() {
  const c = cfg();
  const now = new Date();
  const asOf = now.toISOString();
  if (!DRY) await ensureBot(c);
  // One bot refreshes ESPN; a second would only double the calls. Bain-3 reads what
  // Axis-3's tick wrote, and a stale feed lowers its injury confidence as usual.
  if (!c.ingest.skipRefresh) await refreshData(c, now);

  // Games inside the PRICING window with a mapped, open market.
  const { rows: games } = await q(
    `select g.game_id, g.kickoff, g.game_date::text as game_date, g.home_team, g.away_team,
            g.national_tv, g.neutral_site, m.condition_id
       from sports.nba_games g
       join sports.pm_markets m on m.game_id = g.game_id and m.closed = false
      where g.kickoff > now()
        and g.kickoff <= now() + ($1 || ' hours')::interval
      order by g.kickoff`,
    [String(c.policy.priceWindowHoursBeforeTip)]);
  log(`tick-nba: ${games.length} game(s) inside the ${c.policy.priceWindowHoursBeforeTip}h pricing window`);

  const forecasts = new Map();
  let traded = 0;
  if (games.length) {
    const teams = [...new Set(games.flatMap((g) => [g.home_team, g.away_team]))];
    const ctx = await loadNbaContext(teams, asOf);
    if ((c.model.weights.matchup ?? 0) > 0) ctx.ratings = await loadRatings(asOf);
    const nav = await currentNav(c.botId);
    const { rows: ex } = await q(
      `select coalesce(sum(notional_usd), 0) v from bots.trades where bot_id = $1 and settled = false`, [c.botId]);
    let openExposure = Number(ex[0].v);

    for (const game of games) {
      const { rows: books } = await q(
        `select distinct on (side) * from sports.odds_history
          where game_id = $1 order by side, ts desc`, [game.game_id]);
      const bySide = { home: books.find((b) => b.side === "home"), away: books.find((b) => b.side === "away") };
      const label = `${game.away_team} @ ${game.home_team}`;

      const forecast = await forecastNbaGame(game, bySide, asOf, ctx);
      if (!forecast) {
        await logDecision(c.botId, game, null, { acted: false, skip_reason: "no_book" });
        log(`  SKIP  ${label.padEnd(10)} no_book`);
        continue;
      }
      forecasts.set(game.game_id, forecast);

      const { rows: held } = await q(
        `select 1 from bots.positions where bot_id = $1 and game_id = $2 and shares > 0`, [c.botId, game.game_id]);
      let decision = decide({
        forecast, game, books: bySide, nav, existingPosition: held.length > 0,
        openExposureUsd: openExposure, now,
      });
      // SAFETY CHECK (Bain-3). Team ratings cannot see a scratch, so a big "edge" on
      // a team that just lost a real player is usually the model being late, not
      // right. Never buy that side while the news is fresh.
      const vetoMin = Number(c.policy.freshStatusVetoMinutes) || 0;
      if (vetoMin > 0 && decision.acted) {
        const hit = (forecast.inputs.detail.injury[decision.side] || []).find((x) => x.pts < 0
          && x.ageHours !== null && x.ageHours * 60 <= vetoMin
          && x.impact >= c.policy.freshStatusVetoMinImpactPts);
        if (hit) decision = { acted: false, skip_reason: "fresh_injury_veto", edge: decision.edge, side: decision.side };
      }
      const inp = forecast.inputs;
      const line = `mkt ${(forecast.p_market * 100).toFixed(1)}% fair ${(forecast.p_fair * 100).toFixed(1)}% ` +
        `(${inp.impliedMarginPts >= 0 ? "+" : ""}${inp.impliedMarginPts} -> ${inp.modelMarginPts >= 0 ? "+" : ""}${inp.modelMarginPts} pts, conf ${forecast.confidence.toFixed(2)})`;

      if (!decision.acted) {
        await logDecision(c.botId, game, forecast, decision);
        log(`  SKIP  ${label.padEnd(10)} ${decision.skip_reason.padEnd(20)} ${line}`);
        continue;
      }
      log(`  TRADE ${label.padEnd(10)} ${decision.side.toUpperCase()} @ ${decision.price.toFixed(2)} ` +
          `edge ${(decision.edge * 100).toFixed(1)}c $${decision.notionalUsd.toFixed(2)} ${line}`);
      if (DRY) continue;

      const featureId = await saveForecast(forecast);
      const fill = await executePaper({
        botId: c.botId, game, decision, forecast, book: bySide[decision.side], featureId, nav,
      });
      if (!fill.filled) {
        await logDecision(c.botId, game, forecast, { acted: false, skip_reason: fill.reason });
        continue;
      }
      await logDecision(c.botId, game, forecast, decision, fill.tradeId);
      if (c.policy.exitAtFairValue) {
        await createExitOrder({
          botId: c.botId, tradeId: fill.tradeId, game, decision, forecast,
          fill: { ...fill, tokenId: bySide[decision.side].token_id },
        });
      }
      openExposure += Number(fill.costUsd);
      traded++;
    }
  }

  if (!DRY) {
    await manageExits(c.botId, forecasts, now);
    try {
      await captureClosing();
      await gradeClv();
      await settleTrades();
      await settleFromMarkets();
      await snapshotNav(c.botId);
      await snapshotNavIntraday(c.botId);
    } catch (e) {
      log.warn(`reconcile failed (will retry next tick): ${e.message}`);
    }
  }
  log(`tick-nba: priced ${forecasts.size}, traded ${traded}${DRY ? " (DRY RUN)" : ""}`);
}

async function logDecision(botId, game, forecast, decision, tradeId = null) {
  if (DRY) return;
  await q(
    `insert into bots.decisions
       (bot_id, game_id, p_market, p_fair, edge, acted, skip_reason,
        bet_side, fav_side, bet_on_dog, trade_id)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
    [botId, game.game_id, forecast ? forecast.p_market : null, forecast ? forecast.p_fair : null,
     decision.edge ?? null, !!decision.acted, decision.skip_reason || null,
     decision.side || null, decision.favSide || null, decision.betOnDog ?? null, tradeId],
  );
}

main()
  .then(() => close())
  .catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
