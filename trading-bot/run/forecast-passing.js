#!/usr/bin/env node
// trading-bot/run/forecast-passing.js
//
// Cash-7 SHADOW forecaster. For each starting QB with an open prop in the window,
// pick the central line (latest recorded book nearest P(over)=0.5), build the
// residual forecast, and write it to sports.features_passing. Trades nothing.
//
// Run on a timer alongside the recorder. Every row it logs pairs the factor
// signals with the live line, which is the raw material for the line-residual
// calibration that will eventually replace the provisional backtest weights.
//
//   node trading-bot/run/forecast-passing.js

const { selectConfig, cfg } = require("../src/config");
selectConfig("config.cash-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const {
  buildPassingContext, forecastPassing, saveForecastPassing, marketProbOverProp,
} = require("../src/model/forecastPassing");

async function latestBooksByToken(tokenIds) {
  if (!tokenIds.length) return new Map();
  const { rows } = await q(
    `select o.token_id, o.condition_id, o.side, o.ts, o.mid, o.best_bid, o.best_ask
       from unnest($1::text[]) t(token_id)
       join lateral (
         select * from sports.odds_history o
          where o.token_id = t.token_id order by o.ts desc limit 1
       ) o on true`,
    [tokenIds],
  );
  return new Map(rows.map((r) => [r.token_id, r]));
}

(async () => {
  const hours = cfg().passingModel.forecastWindowHours
             || cfg().ingest.passingRecordWindowHours || 30;

  const { rows: markets } = await q(
    `select m.condition_id, m.slug, m.player_id, m.player_name, m.line,
            m.over_token_id, m.under_token_id, m.fee_type, m.fee_rate, m.tick_size,
            g.game_id, g.season, g.week, g.home_team, g.away_team, g.roof, g.wind,
            g.spread_line, g.total_line, g.kickoff
       from sports.pm_passing_markets m
       join sports.nfl_games g on g.game_id = m.game_id
      where m.closed = false and m.over_token_id is not null
        and g.home_score is null
        and g.kickoff between now() - interval '6 hours'
                          and now() + ($1 || ' hours')::interval`,
    [String(hours)],
  );
  if (!markets.length) { log("forecast-passing: no open markets in window"); await close(); return; }

  // Latest book for every over/under token in one round trip.
  const tokens = [];
  for (const m of markets) { tokens.push(m.over_token_id); if (m.under_token_id) tokens.push(m.under_token_id); }
  const books = await latestBooksByToken(tokens);

  // Per QB (player_id, else player_name), choose the central line: a real book
  // (an inner quote exists) whose P(over) is nearest 0.5.
  const byQb = new Map();
  for (const m of markets) {
    const over = books.get(m.over_token_id);
    const under = m.under_token_id ? books.get(m.under_token_id) : null;
    const p = marketProbOverProp(over, under);
    if (p === null) continue;
    const key = m.player_id || m.player_name;
    const cur = byQb.get(key);
    const dist = Math.abs(p - 0.5);
    if (!cur || dist < cur.dist) byQb.set(key, { m, over, under, p, dist });
  }
  if (!byQb.size) { log("forecast-passing: no central line with a real book yet"); await close(); return; }

  // Context per distinct (season, week).
  const ctxCache = new Map();
  const getCtx = async (season, week) => {
    const k = `${season}:${week}`;
    if (!ctxCache.has(k)) ctxCache.set(k, await buildPassingContext(season, week));
    return ctxCache.get(k);
  };

  const asOf = new Date().toISOString();
  let saved = 0;
  const lines = [];
  for (const { m, over, under } of byQb.values()) {
    const game = {
      game_id: m.game_id, home_team: m.home_team, away_team: m.away_team,
      roof: m.roof, wind: m.wind, spread_line: m.spread_line, total_line: m.total_line,
    };
    const ctx = await getCtx(m.season, m.week);
    const f = forecastPassing(game, m, { over, under }, m.player_id, asOf, ctx);
    if (f.skip) { lines.push(`  ${m.player_name} @${m.line}: skip ${f.skip}`); continue; }
    await saveForecastPassing(f);
    saved++;
    lines.push(`  ${m.player_name} @${m.line}: p_mkt ${(f.p_market * 100).toFixed(0)}% ` +
      `model ${f.model_mean_yards.toFixed(0)}yd (impl ${f.implied_mean_yards.toFixed(0)}, ` +
      `Δ${f.yards_delta >= 0 ? "+" : ""}${f.yards_delta.toFixed(1)}) ` +
      `p_fair ${(f.p_fair * 100).toFixed(0)}% edge ${(f.delta * 100).toFixed(1)}pts`);
  }
  log(`forecast-passing: ${saved} shadow forecast(s) written`);
  for (const l of lines) log(l);
  await close();
})().catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
