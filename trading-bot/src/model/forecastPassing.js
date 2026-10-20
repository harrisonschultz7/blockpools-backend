// trading-bot/src/model/forecastPassing.js
//
// THE RESIDUAL MODEL, IN YARDS (Bruno-7, QB passing yards). Same shape as
// forecastTotals.js:
//
//   implied_mean = line + sigma * Phi^-1(p_market)       <- invert the price
//   model_mean   = implied_mean + scale * sum(w_k * points_k)
//   p_fair       = 1 - Phi((line - model_mean) / sigma)  <- back to P(over)
//
// SHADOW ONLY. It writes sports.features_passing and trades nothing. Two reasons
// it cannot trade yet: (1) the trading half (policy/exec for props) is unbuilt,
// and (2) the factor weights are PROVISIONAL -- fitted vs a naive baseline in the
// 2025-26 backtest, NOT vs the market line, so they overstate tradeable edge. A
// global provisionalScale haircuts them, and the real per-factor scales come from
// line-residual calibration once ~3-4 weeks of recorded prop closing lines exist.
// Logging f_* now is exactly what makes that calibration possible later.
//
// Weights (config.bruno-7.json passingModel.weights) are the backtest coefficients:
//   form 0.238 (t=4.19, the only factor clearing 2sigma), matchup 0.102 (t=1.5),
//   weather 0.62 yds/mph (t=-1.5, sign baked in via -wind). injury/script are
//   priors with low confidence -- untested in the backtest.

const { cfg } = require("../config");
const { q } = require("../db");
const { normCdf, normInv } = require("./forecastTotals");

const FACTORS = ["form", "matchup", "weather", "injury", "script"];
const DEFAULT_ATT = 33;   // league-ish attempts when a QB has no trailing history

/**
 * Market P(over) for a prop book, or null when there is no real market. An
 * unseeded prop quotes bid 0.01 / ask 0.99, so its mid is a fake coin flip; a
 * single resting order is one person's price, not the market's. P(over) is the
 * midpoint of the combined over-side book, and only when it is two-sided within
 * MAX_PROP_SPREAD.
 */
function marketProbOverProp(over, under) {
  // A REAL TWO-SIDED BOOK OR NOTHING. Combine both tokens into one over-side
  // book (an under bid at b is an over ask at 1-b) and require the best ask and
  // best bid within MAX_PROP_SPREAD. Without this the fallback below read a
  // 0.01/0.99 placeholder as a 50% market, and a lone resting order (ask 0.46,
  // no bid within 45c) as the price: of 768 pre-game week-6 forecasts only 93
  // had a spread <= 0.12, and the bot's largest "edges" were those fake prices
  // (Geno Smith 199.5 AND 249.5 both "50%"; edge >= 3c traded -38%).
  const num = (x) => (x != null && Number(x) > 0 && Number(x) < 1 ? Number(x) : null);
  const asks = [num(over?.best_ask), under?.best_bid != null && num(under.best_bid) != null ? 1 - Number(under.best_bid) : null].filter((x) => x != null);
  const bids = [num(over?.best_bid), under?.best_ask != null && num(under.best_ask) != null ? 1 - Number(under.best_ask) : null].filter((x) => x != null);
  if (!asks.length || !bids.length) return null;
  const ask = Math.min(...asks), bid = Math.max(...bids);
  if (!(ask - bid <= MAX_PROP_SPREAD + 1e-9) || ask <= bid - 1e-9) return null;
  return (ask + bid) / 2;
}
const MAX_PROP_SPREAD = 0.12;

/**
 * Per-tick context shared by every game in one week: league Y/A, each defense's
 * trailing pass-yards-ALLOWED (opposing QB rows only -- pass-only, no run-D
 * leakage), each QB's trailing form, team WR target shares, and this week's OUT
 * receivers. Everything is measured STRICTLY before `week`, so a forecast never
 * sees the game it is forecasting.
 */
async function buildPassingContext(season, week) {
  const { rows } = await q(
    `select player_id, player_display_name, position, team, opponent_team, week,
            attempts::float as att, passing_yards::float as yds,
            target_share::float as ts
       from sports.nfl_player_week
      where season = $1 and week < $2`,
    [season, week],
  );

  let lYds = 0, lAtt = 0;
  const def = new Map();     // defense -> {yds, att}
  const qb = new Map();      // player_id -> {yds, att, games, last3:[{week,yds,att}]}
  const wr = new Map();      // team -> Map(player_id -> {name, ts, week})
  for (const r of rows) {
    if (r.position === "QB" && r.att >= 1) {
      lYds += r.yds; lAtt += r.att;
      const d = def.get(r.opponent_team) || { yds: 0, att: 0 };
      d.yds += r.yds; d.att += r.att; def.set(r.opponent_team, d);
      const g = qb.get(r.player_id) || { yds: 0, att: 0, games: 0, last3: [], team: r.team, teamWeek: r.week };
      g.yds += r.yds; g.att += r.att; g.games += 1;
      g.last3.push({ week: r.week, yds: r.yds, att: r.att });
      if (r.week >= (g.teamWeek || 0)) { g.team = r.team; g.teamWeek = r.week; }  // latest team
      qb.set(r.player_id, g);
    }
    if ((r.position === "WR" || r.position === "TE") && r.ts != null) {
      const t = wr.get(r.team) || new Map();
      const prev = t.get(r.player_id);
      if (!prev || r.week > prev.week) t.set(r.player_id, { name: r.player_display_name, ts: r.ts, week: r.week });
      wr.set(r.team, t);
    }
  }
  for (const g of qb.values()) g.last3 = g.last3.sort((a, b) => b.week - a.week).slice(0, 3);
  const leagueYA = lAtt > 0 ? lYds / lAtt : 7.0;

  // This week's OUT / Doubtful players, latest report per player.
  const inj = new Map();     // team -> Set(gsis_id)
  const { rows: ir } = await q(
    `select distinct on (gsis_id) gsis_id, team, report_status
       from sports.nfl_injuries
      where season = $1 and week = $2 and gsis_id is not null
      order by gsis_id, observed_at desc`,
    [season, week],
  );
  for (const r of ir) {
    const st = String(r.report_status || "").toLowerCase();
    if (st.includes("out") || st.includes("doubtful")) {
      const s = inj.get(r.team) || new Set(); s.add(r.gsis_id); inj.set(r.team, s);
    }
  }
  return { leagueYA, def, qb, wr, inj, season, week };
}

/** Compute the five factor signals in yards for one QB on one game. */
function computeFactors(game, playerId, team, opp, ctx) {
  const pm = cfg().passingModel;
  const leagueYA = ctx.leagueYA;
  const minAtt = pm.minTrailingAtt || 20;

  const form = ctx.qb.get(playerId);
  const expAtt = form && form.games > 0 ? form.att / form.games : DEFAULT_ATT;

  // FORM -- QB trailing Y/A vs league, in yards over expected volume.
  let formPts = 0, formConf = 0;
  if (form && form.att >= minAtt) {
    formPts = (form.yds / form.att - leagueYA) * expAtt;
    formConf = Math.min(1, form.att / 100);
  }

  // MATCHUP -- opponent trailing pass-yards-ALLOWED per attempt vs league.
  const d = ctx.def.get(opp);
  let matchupPts = 0, matchupConf = 0;
  if (d && d.att >= minAtt) {
    matchupPts = (d.yds / d.att - leagueYA) * expAtt;
    matchupConf = Math.min(1, d.att / 120);
  }

  // WEATHER -- wind only (dome = 0). Points carry the sign (-wind) so a positive
  // weight means "wind costs yards"; the flat per-mph fit understates the tail.
  const dome = game.roof === "dome" || game.roof === "closed";
  const wind = dome ? 0 : (Number.isFinite(Number(game.wind)) ? Number(game.wind) : 0);
  const weatherPts = -wind;
  const weatherConf = dome ? 1 : (Number.isFinite(Number(game.wind)) ? 0.8 : 0.2);

  // INJURY -- target share of this team's OUT/Doubtful receivers -> yards lost.
  // Prior only (untested in the backtest), so low confidence.
  const out = ctx.inj.get(team);
  const roster = ctx.wr.get(team);
  let lostTs = 0;
  if (out && roster) for (const [pid, v] of roster) if (out.has(pid)) lostTs += (v.ts || 0);
  const injuryPts = -(lostTs * expAtt * leagueYA * (pm.injuryPassThrough || 0.5));
  const injuryConf = lostTs > 0 ? 0.3 : 0;

  // SCRIPT -- underdogs throw more. nflverse spread_line is the HOME line; flip for
  // the away team. Positive team-spread = underdog = more passing. Prior, low conf.
  const spread = Number(game.spread_line);
  let scriptPts = 0, scriptConf = 0;
  if (Number.isFinite(spread)) {
    const teamSpread = team === game.home_team ? spread : -spread;   // + = underdog
    scriptPts = teamSpread * (pm.scriptYardsPerSpread || 2.0);
    scriptConf = 0.3;
  }

  return {
    form: { points: formPts, confidence: formConf, detail: { expAtt, trailingAtt: form ? form.att : 0, trailingYA: form && form.att ? +(form.yds / form.att).toFixed(2) : null, leagueYA: +leagueYA.toFixed(2) } },
    matchup: { points: matchupPts, confidence: matchupConf, detail: { opp, oppAtt: d ? d.att : 0, oppYAallowed: d && d.att ? +(d.yds / d.att).toFixed(2) : null } },
    weather: { points: weatherPts, confidence: weatherConf, detail: { wind, dome } },
    injury: { points: injuryPts, confidence: injuryConf, detail: { lostTargetShare: +lostTs.toFixed(3) } },
    script: { points: scriptPts, confidence: scriptConf, detail: { teamSpread: Number.isFinite(spread) ? (team === game.home_team ? spread : -spread) : null } },
    expAtt,
  };
}

/** Full passing forecast for one QB on one prop line. Returns a row or {skip}. */
function forecastPassing(game, market, books, playerId, asOf, ctx) {
  const pm = cfg().passingModel;
  const p_market = marketProbOverProp(books.over, books.under);
  if (p_market === null) return { skip: "no_book" };

  const line = Number(market.line);
  const sigma = Number(pm.sigmaPassingYards) || 54;
  const pClamped = Math.max(0.02, Math.min(0.98, p_market));
  const implied_mean = line + sigma * normInv(pClamped);

  // The QB's team (latest observed) decides the opponent (for matchup) and which
  // side of the spread they are on (for script); fall back to the game's home side.
  const qbrec = ctx.qb.get(market.player_id);
  const team = qbrec && qbrec.team ? qbrec.team : game.home_team;
  const opp = team === game.home_team ? game.away_team : game.home_team;
  const f = computeFactors(game, market.player_id, team, opp, ctx);
  const w = pm.weights;
  const scale = Number(pm.provisionalScale);
  const weighted = {};
  let rawDelta = 0;
  for (const k of FACTORS) { weighted[k] = (w[k] || 0) * f[k].points; rawDelta += weighted[k]; }

  // Global provisional haircut + a hard yards cap, so a hot-form QB cannot run the
  // projection away from the market while the weights are still uncalibrated.
  const maxYards = Number(pm.maxYardsDelta) || 30;
  let yardsDelta = scale * rawDelta;
  yardsDelta = Math.max(-maxYards, Math.min(maxYards, yardsDelta));

  const model_mean = implied_mean + yardsDelta;
  const p_fair_raw = 1 - normCdf((line - model_mean) / sigma);
  const p_fair = Math.max(0.01, Math.min(0.99, p_fair_raw));
  const delta = p_fair - p_market;

  const weightTotal = FACTORS.reduce((s, k) => s + (w[k] || 0), 0);
  const confidence = weightTotal > 0
    ? FACTORS.reduce((s, k) => s + (w[k] || 0) * f[k].confidence, 0) / weightTotal : 0;

  const out = {
    game_id: game.game_id,
    condition_id: market.condition_id,
    player_id: market.player_id || null,
    asof_ts: asOf,
    line, p_market, sigma,
    implied_mean_yards: implied_mean,
    f_form: weighted.form,
    f_matchup: weighted.matchup,
    f_weather: weighted.weather,
    f_injury: weighted.injury,
    f_script: weighted.script,
    yards_delta: yardsDelta,
    model_mean_yards: model_mean,
    delta, p_fair, confidence,
    inputs: {
      mode: "shadow",
      provisionalScale: scale,
      weights: w,
      rawDelta: +rawDelta.toFixed(3),
      expAtt: +f.expAtt.toFixed(1),
      factors: Object.fromEntries(FACTORS.map((k) => [k, { points: +f[k].points.toFixed(3), weighted: +weighted[k].toFixed(3), confidence: +f[k].confidence.toFixed(3), detail: f[k].detail }])),
      book: {
        over_best_ask: books.over ? Number(books.over.best_ask) : null,
        under_best_bid: books.under ? Number(books.under.best_bid) : null,
        over_best_bid: books.over ? Number(books.over.best_bid) : null,
        under_best_ask: books.under ? Number(books.under.best_ask) : null,
        ts: books.over ? books.over.ts : (books.under ? books.under.ts : null),
      },
      market: { slug: market.slug, player_name: market.player_name, fee_type: market.fee_type, fee_rate: market.fee_rate, tick_size: market.tick_size },
    },
    model_version: pm.modelVersion || "bruno-7-shadow",
  };

  const bad = [];
  for (const k of ["p_market", "sigma", "implied_mean_yards", "model_mean_yards", "yards_delta", "delta", "p_fair", "confidence", "line"])
    if (!Number.isFinite(out[k])) bad.push(k);
  if (bad.length) return { skip: "non_finite_forecast", nonFinite: bad, detail: { game_id: game.game_id, line, fields: bad } };
  return out;
}

async function saveForecastPassing(f) {
  const { rows } = await q(
    `insert into sports.features_passing
       (game_id, condition_id, player_id, asof_ts, line, p_market, sigma, implied_mean_yards,
        f_matchup, f_injury, f_weather, f_form, f_script,
        yards_delta, model_mean_yards, delta, p_fair, confidence, inputs, model_version)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
     returning id`,
    [f.game_id, f.condition_id, f.player_id, f.asof_ts, f.line, f.p_market, f.sigma,
     f.implied_mean_yards, f.f_matchup, f.f_injury, f.f_weather, f.f_form, f.f_script,
     f.yards_delta, f.model_mean_yards, f.delta, f.p_fair, f.confidence,
     JSON.stringify(f.inputs), f.model_version],
  );
  return rows[0].id;
}

module.exports = {
  forecastPassing, saveForecastPassing, buildPassingContext,
  computeFactors, marketProbOverProp, FACTORS,
};
