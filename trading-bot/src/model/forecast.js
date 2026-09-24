// trading-bot/src/model/forecast.js
//
// THE RESIDUAL MODEL.
//
//   p_fair = p_market + delta
//
// The market price is the ANCHOR, not a weighted factor. This matters more
// than any single weight: the Polymarket midpoint is already an output of team
// strength, injuries, rest and matchup. Blending p_market as one factor
// alongside those same inputs double-counts every one of them, and lowering
// the market's blend weight does not fix that -- it just adds noise on top of
// the double-count. So the model forecasts only the DEVIATION.
//
// Two things keep the deviation honest:
//
//  1. RESIDUALISATION. Each factor has a beta describing how much of it the
//     market already prices. The model uses signal - beta * (p_market - 0.5),
//     i.e. only the part the price does not already explain. Betas are fitted
//     by model/calibrate.js against closing lines; until that has run they are
//     zero, which is the conservative choice in the wrong direction -- flagged
//     in the output as `calibrated: false` so nothing downstream mistakes an
//     uncalibrated run for a validated one.
//
//  2. THE CAP. delta is squashed through tanh and limited to config deltaCap
//     (0.12 for medium). tanh rather than a hard clamp so that an extreme
//     factor saturates smoothly instead of hitting a wall -- a hard clamp makes
//     every blowout signal produce an identical bet size.
//
// Confidence multiplies delta down when inputs are thin, and a game below
// confidence.minToTrade is skipped rather than traded small.

const fs = require("fs");
const path = require("path");
const { cfg } = require("../config");
const { q } = require("../db");
const { solveRatings, momentumSignal } = require("../features/teamStrength");
const { loadProfiles, matchupSignal } = require("../features/matchup");
const { injurySignal } = require("../features/injuries");
const { situationalSignal } = require("../features/situational");

const CALIB_PATH = path.join(__dirname, "calibration.json");

/** Fitted betas/scales, or safe defaults when calibrate.js has not run. */
function loadCalibration() {
  try {
    const c = JSON.parse(fs.readFileSync(CALIB_PATH, "utf8"));
    return { ...c, calibrated: true };
  } catch {
    return {
      calibrated: false,
      // scale converts a factor's natural unit into probability points.
      // Momentum and matchup are in EPA/play; injury and situational already
      // arrive in probability points, hence scale 1.
      scale: { momentum: 0.55, matchup: 0.55, injury: 1, situational: 1 },
      beta:  { momentum: 0,    matchup: 0,    injury: 0, situational: 0 },
    };
  }
}

/** Market-implied home win probability from the recorded book. */
function marketProb(homeBook, awayBook) {
  const h = homeBook && homeBook.mid !== null ? Number(homeBook.mid) : null;
  const a = awayBook && awayBook.mid !== null ? Number(awayBook.mid) : null;
  if (h === null && a === null) return null;
  if (h === null) return 1 - a;
  if (a === null) return h;
  // Both sides quoted: normalise away the (small) overround so the pair sums
  // to 1. Polymarket is usually within a cent, but a stale side can drift.
  const sum = h + a;
  return sum > 0 ? h / sum : h;
}

/**
 * Full forecast for one game.
 * `books` is { home, away } rows from sports.odds_history.
 */
async function forecastGame(game, books, asOf, ctx) {
  const m = cfg().model;
  const cal = loadCalibration();

  const ratings = ctx.ratings || await solveRatings(asOf);
  const profiles = ctx.profiles || await loadProfiles(asOf);

  const p_market = marketProb(books.home, books.away);
  if (p_market === null) return null;

  const mo = momentumSignal(ratings, game.home_team, game.away_team);
  const ma = matchupSignal(profiles, ratings, game.home_team, game.away_team);
  const inj = await injurySignal(game, asOf);
  const sit = await situationalSignal({ ...game, marketFavoursHome: p_market >= 0.5 }, asOf);

  const raw = { momentum: mo, matchup: ma, injury: inj, situational: sit };

  // Scale into probability points, then strip the part the price already knows.
  const contrib = {};
  let weighted = 0;
  for (const k of ["momentum", "matchup", "injury", "situational"]) {
    const scaled = raw[k].signal * (cal.scale[k] ?? 1);
    const residual = scaled - (cal.beta[k] ?? 0) * (p_market - 0.5);
    const c = m.weights[k] * residual;
    contrib[k] = { signal: +raw[k].signal.toFixed(5), scaled: +scaled.toFixed(5),
                   residual: +residual.toFixed(5), weighted: +c.toFixed(5),
                   confidence: +raw[k].confidence.toFixed(3) };
    weighted += c;
  }

  // Confidence is the weight-weighted average of the factors' own confidence.
  const confidence = ["momentum", "matchup", "injury", "situational"]
    .reduce((s, k) => s + m.weights[k] * raw[k].confidence, 0);

  // tanh saturation, then the hard cap. Dividing by the cap first means the
  // curve is roughly linear for small signals and only bends near the limit.
  const delta = m.deltaCap * Math.tanh(weighted / m.deltaCap) * confidence;
  const p_fair = Math.max(0.01, Math.min(0.99, p_market + delta));

  return {
    game_id: game.game_id,
    asof_ts: asOf,
    p_market,
    p_fair,
    delta,
    confidence,
    calibrated: cal.calibrated,
    f_momentum: contrib.momentum.weighted,
    f_matchup: contrib.matchup.weighted,
    f_injury: contrib.injury.weighted,
    f_situational: contrib.situational.weighted,
    inputs: {
      calibration: { calibrated: cal.calibrated, scale: cal.scale, beta: cal.beta },
      contrib,
      detail: {
        momentum: mo.detail, matchup: ma.detail,
        injury: inj.detail, situational: sit.detail,
      },
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

/** Persist a forecast and return its id, so a trade can reference it. */
async function saveForecast(f) {
  const { rows } = await q(
    `insert into sports.features
       (game_id, asof_ts, p_market, f_momentum, f_matchup, f_injury,
        f_situational, delta, p_fair, confidence, inputs, model_version)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) returning id`,
    [f.game_id, f.asof_ts, f.p_market, f.f_momentum, f.f_matchup, f.f_injury,
     f.f_situational, f.delta, f.p_fair, f.confidence,
     JSON.stringify(f.inputs), f.model_version],
  );
  return rows[0].id;
}

module.exports = { forecastGame, saveForecast, marketProb, loadCalibration, CALIB_PATH };
