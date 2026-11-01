// trading-bot/src/model/forecastNba.js
//
// AXIS-3: NBA game winners. Adam-7's structure -- the market price is the
// ANCHOR and the model forecasts only the deviation -- worked in POINTS, the
// unit NBA games are actually priced in:
//
//   implied_margin = sigma x Phi^-1(p_market)          what the price says
//   model_margin   = implied_margin + sum(w_k x pts_k)  plus what it may not know
//   p_raw          = Phi(model_margin / sigma)
//   p_fair         = p_market + clamp(p_raw - p_market, +-deltaCap) x confidence
//
// sigma is the spread of NBA final margins around expectation (~12.5 points).
// Points add across factors and convert to probability once, so a 3-point
// injury swing is worth more in a 50/50 game than in a 90/10 one -- which is
// how a margin actually maps to a win chance, and what adding probabilities
// factor by factor gets wrong.
//
// UNCALIBRATED. The weights are the user's ordering (2026-11-01), not fitted:
// there is no NBA closing-line history yet. `calibrated: false` rides on every
// forecast so nothing downstream mistakes this for a validated model; the bot
// paper trades, hidden, until its record says otherwise.

const { cfg } = require("../config");
const { marketProb } = require("./forecast");
const { normCdf, normInv } = require("./forecastTotals");
const {
  injurySignal, momentumSignal, restSignal, primetimeSignal, styleSignal,
} = require("../features/nba/factors");

const FACTORS = ["injury", "momentum", "rest", "primetime", "style"];

async function forecastNbaGame(game, books, asOf, ctx) {
  const m = cfg().model;
  const p_market = marketProb(books.home, books.away);
  if (p_market === null) return null;
  const now = new Date(asOf);
  const sigma = m.sigmaMarginPts;
  const p = Math.min(0.995, Math.max(0.005, p_market));
  const implied = sigma * normInv(p);

  const raw = {
    injury: injurySignal(game, ctx, now),
    momentum: momentumSignal(game, ctx),
    rest: restSignal(game, ctx),
    primetime: primetimeSignal(game, ctx),
    style: styleSignal(game, ctx, implied),
  };

  let pts = 0;
  const contrib = {};
  for (const k of FACTORS) {
    const w = m.weights[k] ?? 0;
    const c = w * raw[k].pts;
    pts += c;
    contrib[k] = { pts: +raw[k].pts.toFixed(3), weighted: +c.toFixed(3), confidence: +raw[k].confidence.toFixed(3) };
  }
  // Weight-weighted confidence, as Adam-7 does it.
  const wSum = FACTORS.reduce((s, k) => s + (m.weights[k] ?? 0), 0) || 1;
  const confidence = FACTORS.reduce((s, k) => s + (m.weights[k] ?? 0) * raw[k].confidence, 0) / wSum;

  const p_raw = normCdf((implied + pts) / sigma);
  const delta = Math.max(-m.deltaCap, Math.min(m.deltaCap, p_raw - p_market)) * confidence;
  const p_fair = Math.max(0.01, Math.min(0.99, p_market + delta));

  return {
    game_id: game.game_id,
    asof_ts: asOf,
    p_market, p_fair, delta, confidence,
    calibrated: false,
    // sports.features has four factor columns (Adam-7's). The NBA factors map
    // onto them by kind; all five, unmapped, are in inputs.contrib.
    f_injury: contrib.injury.weighted,
    f_momentum: contrib.momentum.weighted,
    f_situational: +(contrib.rest.weighted + contrib.primetime.weighted).toFixed(3),
    f_matchup: contrib.style.weighted,
    inputs: {
      model: "axis-nba-v1",
      impliedMarginPts: +implied.toFixed(2),
      modelMarginPts: +(implied + pts).toFixed(2),
      factorPts: +pts.toFixed(3),
      sigma,
      contrib,
      detail: Object.fromEntries(FACTORS.map((k) => [k, raw[k].detail])),
      game: { national_tv: game.national_tv || null },
      book: {
        home_mid: books.home ? Number(books.home.mid) : null,
        away_mid: books.away ? Number(books.away.mid) : null,
        home_best_ask: books.home ? Number(books.home.best_ask) : null,
        away_best_ask: books.away ? Number(books.away.best_ask) : null,
        ts: books.home ? books.home.ts : null,
      },
    },
    model_version: cfg().modelVersion,
  };
}

module.exports = { forecastNbaGame, FACTORS };
