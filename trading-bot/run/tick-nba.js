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
const { openEntryOrder, openBidsUsd, cancelEntry, postOrKeepBid, reviewIdleBid, sweepEntryFills,
        closeFilledBidsForCopiers } = require("../src/exec/entryOrders");
const { currentNav, snapshotNav, snapshotNavIntraday } = require("../src/accounting/nav");
const { gradeClv, settleTrades, settleFromMarkets } = require("../src/accounting/settle");
const { normaliseBook, fetchBook } = require("../src/ingest/polymarket");
const { writeBooks } = require("../src/ingest/bookWriter");

const DRY = process.argv.includes("--dry");
// --preview: the MORNING LOOK (user, 2026-11-17). Prices every game 2-30h out
// once, so the ops monitor shows a preliminary fair price and any data gap hours
// before the 2h pricing window -- like the NFL models' daily look. It cannot
// trade: every game it sees is outside the 45-minute trading window, so the
// policy returns outside_window. Books are fetched live (the recorder only runs
// from 3h out) and stored once so the monitor can project an edge.
const PREVIEW = process.argv.includes("--preview");
const PREVIEW_HOURS = 30;

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

/** The morning look: price every game 2-30h out once, record the decision, never trade. */
async function preview(c, now) {
  const asOf = now.toISOString();
  const { rows: games } = await q(
    `select g.game_id, g.kickoff, g.game_date::text as game_date, g.home_team, g.away_team,
            g.national_tv, g.neutral_site, g.season_type, m.condition_id, m.home_token_id, m.away_token_id
       from sports.nba_games g
       join sports.pm_markets m on m.game_id = g.game_id and m.closed = false
      where m.home_token_id is not null and m.away_token_id is not null
        and g.kickoff > now() + ($1 || ' hours')::interval
        and g.kickoff <= now() + ($2 || ' hours')::interval
      order by g.kickoff`,
    [String(c.policy.priceWindowHoursBeforeTip), String(PREVIEW_HOURS)]);
  log(`tick-nba --preview: ${games.length} game(s) ${c.policy.priceWindowHoursBeforeTip}-${PREVIEW_HOURS}h out`);
  if (!games.length) return;

  const teams = [...new Set(games.flatMap((g) => [g.home_team, g.away_team]))];
  const ctx = await loadNbaContext(teams, asOf);
  if ((c.model.weights.matchup ?? 0) > 0) ctx.ratings = await loadRatings(asOf);
  const nav = await currentNav(c.botId);

  const snaps = [];
  for (const game of games) {
    const label = `${game.away_team} @ ${game.home_team}`;
    const bySide = {};
    for (const side of ["home", "away"]) {
      const tokenId = side === "home" ? game.home_token_id : game.away_token_id;
      try {
        const snap = normaliseBook(await fetchBook(tokenId), c.ingest.bookLevelsStored);
        if (snap.mid === null) continue;
        const row = { condition_id: game.condition_id, token_id: tokenId, game_id: game.game_id, side, ...snap };
        snaps.push(row);
        bySide[side] = { ...row, ts: asOf };
      } catch (e) { log.warn(`  preview book ${label}/${side}: ${e.message}`); }
    }
    const forecast = await forecastNbaGame(game, bySide, asOf, ctx);
    if (!forecast) {
      await logDecision(c.botId, game, null, { acted: false, skip_reason: "no_book" });
      log(`  PREVIEW ${label.padEnd(10)} no_book`);
      continue;
    }
    // decide() returns outside_window for every game here -- the record of the
    // look, with its fair price, is the point.
    const decision = decide({ forecast, game, books: bySide, nav, existingPosition: false, openExposureUsd: 0, now });
    if (decision.acted) { log.warn(`  PREVIEW ${label}: policy would act outside its window -- not trading`); continue; }
    if (DRY) { log(`  PREVIEW ${label.padEnd(10)} fair ${(forecast.p_fair * 100).toFixed(1)}% mkt ${(forecast.p_market * 100).toFixed(1)}% (dry)`); continue; }
    await logDecision(c.botId, game, forecast, decision);
    await saveForecast(forecast); // factor breakdown for the monitor
    log(`  PREVIEW ${label.padEnd(10)} fair ${(forecast.p_fair * 100).toFixed(1)}% mkt ${(forecast.p_market * 100).toFixed(1)}%`);
  }
  if (!DRY && snaps.length) await writeBooks(snaps, `nba preview books (${games.length} games)`);
}

async function main() {
  const c = cfg();
  const now = new Date();
  const asOf = now.toISOString();
  if (!DRY) await ensureBot(c);
  // One bot refreshes ESPN; a second would only double the calls. Bain-3 reads what
  // Axis-3's tick wrote, and a stale feed lowers its injury confidence as usual.
  if (!c.ingest.skipRefresh) await refreshData(c, now);
  if (PREVIEW) { await preview(c, now); return; }

  // Games inside the PRICING window with a mapped, open market.
  const { rows: games } = await q(
    `select g.game_id, g.kickoff, g.game_date::text as game_date, g.home_team, g.away_team,
            g.national_tv, g.neutral_site, g.season_type, m.condition_id
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
    // MAKER ENTRIES (policy.makerEntry): book any resting bid the market traded
    // through since the last tick BEFORE deciding, so a filled game reads as held.
    const mk = c.policy.makerEntry || {};
    let makerOn = !!mk.enabled && !DRY;
    // Until sql/014 + 015 are applied: trade as a taker.
    if (makerOn) await q(`select copy_closed_at from bots.entry_orders limit 1`).catch(() => {
      makerOn = false;
      log.warn("makerEntry is on but bots.entry_orders is missing or pre-015 (run migrate --only 014 and --only 015) -- taking the ask");
    });
    if (makerOn) {
      await sweepEntryFills(c.botId).catch((e) => log.warn(`entry fill sweep failed: ${e.message}`));
      await closeFilledBidsForCopiers(c.botId, Number(mk.fallbackMinutesBeforeTip ?? 10))
        .catch((e) => log.warn(`copier bid close failed: ${e.message}`));
    }
    const nav = await currentNav(c.botId);
    const { rows: ex } = await q(
      `select coalesce(sum(notional_usd), 0) v from bots.trades where bot_id = $1 and settled = false`, [c.botId]);
    // Resting bids count against the open-exposure cap: each is a trade waiting to happen.
    let openExposure = Number(ex[0].v) + (makerOn ? await openBidsUsd(c.botId) : 0);

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
      const ownBid = makerOn ? await openEntryOrder(c.botId, game.game_id) : null;
      let decision = decide({
        forecast, game, books: bySide, nav, existingPosition: held.length > 0,
        // this game's own resting bid is not "other" exposure
        openExposureUsd: openExposure - (ownBid ? Number(ownBid.notional_usd) : 0), now,
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
        if (ownBid) await reviewIdleBid(ownBid, forecast, decision);
        await logDecision(c.botId, game, forecast, decision);
        await snapshotSkip(forecast, decision);
        log(`  SKIP  ${label.padEnd(10)} ${decision.skip_reason.padEnd(20)} ${line}`);
        continue;
      }
      let fallbackBidId = null;
      if (makerOn) {
        const minsToTip = (new Date(game.kickoff).getTime() - now.getTime()) / 60000;
        if (minsToTip > Number(mk.fallbackMinutesBeforeTip ?? 10)) {
          const r = await postOrKeepBid({ botId: c.botId, game, decision, forecast, book: bySide[decision.side],
            featureIdFn: () => saveForecast(forecast), nav });
          if (r.order) {
            if (r.action !== "kept") openExposure += Number(r.order.notional_usd) - (ownBid ? Number(ownBid.notional_usd) : 0);
            await logDecision(c.botId, game, forecast,
              { acted: false, skip_reason: `maker_bid_${r.action}`, edge: decision.edge, side: decision.side });
            log(`  BID   ${label.padEnd(10)} ${decision.side.toUpperCase()} @ ${Number(r.order.limit_price).toFixed(3)} ` +
                `(ask ${decision.price.toFixed(3)}, edge ${(decision.edge * 100).toFixed(1)}c) ${r.action} ${line}`);
            continue;
          }
          // no usable mid: fall through and take the ask as before
        } else if (ownBid) {
          // FALLBACK: the bid never filled -- cancel it and take the ask now.
          await cancelEntry(ownBid, "fallback_taker");
          fallbackBidId = ownBid.id; // the taker buy below tells copiers to close their bid and top up
          openExposure -= Number(ownBid.notional_usd);
          log(`  BID   ${label.padEnd(10)} unfilled ${minsToTip.toFixed(0)} min before tip -- taking the ask`);
        }
      }
      log(`  TRADE ${label.padEnd(10)} ${decision.side.toUpperCase()} @ ${decision.price.toFixed(2)} ` +
          `edge ${(decision.edge * 100).toFixed(1)}c $${decision.notionalUsd.toFixed(2)} ${line}`);
      if (DRY) continue;

      const featureId = await saveForecast(forecast);
      const fill = await executePaper({
        botId: c.botId, game, decision, forecast, book: bySide[decision.side], featureId, nav,
        entryOrderId: fallbackBidId,
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

/**
 * The factors behind a game the model priced and passed on. Only traded games were
 * saved, so a skip could not be reviewed. One row when it first prices the game
 * inside the trading window, then only when its number moves: about two rows per
 * game per model. Never allowed to stop a tick.
 */
const NOT_PRICED_TO_TRADE = new Set(["outside_window", "already_positioned"]);
async function snapshotSkip(forecast, decision) {
  if (DRY || NOT_PRICED_TO_TRADE.has(decision.skip_reason)) return;
  try {
    const { rows } = await q(
      `select p_fair from sports.features where game_id = $1 and model_version = $2
        order by asof_ts desc limit 1`, [forecast.game_id, forecast.model_version]);
    if (rows[0] && Math.abs(Number(rows[0].p_fair) - forecast.p_fair) < 0.001) return;
    await saveForecast(forecast);
  } catch (e) {
    log.warn(`skip snapshot failed: ${e.message}`);
  }
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
