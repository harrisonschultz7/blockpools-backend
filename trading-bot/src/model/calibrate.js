// trading-bot/src/model/calibrate.js
//
// Fits the residualisation betas and the factor scales against closing lines.
//
// WHY THIS IS NOT OPTIONAL. Run uncalibrated (all betas zero), the bot backs
// the market favourite in every single trade -- observed on the first live
// slate: 4 trades, 4 favourites. That is the double-counting problem made
// visible. Team strength, injuries and rest are already inside the price, so
// adding them again pushes p_fair further in the direction the price already
// leans, and the "edge" it finds is largest exactly where the market is most
// confident. The bot ends up an expensive way to agree with the public.
//
// The fix is to measure how much of each factor the price already contains:
//
//   beta_i = slope of ( scaled_signal_i ) regressed on ( p_market - 0.5 )
//
// and then feed the model only signal_i - beta_i * (p_market - 0.5) -- the part
// the market has NOT priced. A factor that the market prices perfectly
// contributes nothing, which is correct.
//
// The scales are then fitted so the surviving residual maps into probability
// points: regress the actual outcome on each residual.
//
// POINT-IN-TIME: ratings are solved once per (season, week) using the FIRST
// kickoff of that week, so no game is ever scored with information from a game
// played later that same week.

const fs = require("fs");
const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");
const { solveRatings, momentumSignal } = require("../features/teamStrength");
const { loadProfiles, matchupSignal } = require("../features/matchup");
const { injurySignal } = require("../features/injuries");
const { situationalSignal } = require("../features/situational");
const { CALIB_PATH } = require("./forecast");

const FACTORS = ["momentum", "matchup", "injury", "situational"];

/** American moneyline -> implied probability, with the vig split out. */
function devig(homeMl, awayMl) {
  const imp = (ml) => (ml > 0 ? 100 / (ml + 100) : -ml / (-ml + 100));
  const h = imp(Number(homeMl));
  const a = imp(Number(awayMl));
  const sum = h + a;
  return sum > 0 ? h / sum : null;
}

const mean = (xs) => xs.reduce((s, x) => s + x, 0) / (xs.length || 1);

/** OLS slope of y on x. */
function slope(xs, ys) {
  const mx = mean(xs), my = mean(ys);
  let num = 0, den = 0;
  for (let i = 0; i < xs.length; i++) {
    num += (xs[i] - mx) * (ys[i] - my);
    den += (xs[i] - mx) ** 2;
  }
  return den === 0 ? 0 : num / den;
}

function corr(xs, ys) {
  const mx = mean(xs), my = mean(ys);
  let sxy = 0, sxx = 0, syy = 0;
  for (let i = 0; i < xs.length; i++) {
    sxy += (xs[i] - mx) * (ys[i] - my);
    sxx += (xs[i] - mx) ** 2;
    syy += (ys[i] - my) ** 2;
  }
  return sxx > 0 && syy > 0 ? sxy / Math.sqrt(sxx * syy) : 0;
}

async function buildSamples() {
  const { startSeason } = cfg().data;
  const { rows: games } = await q(
    `select * from sports.nfl_games
      where season >= $1
        and home_score is not null and away_score is not null
        and home_score <> away_score
        and home_moneyline is not null and away_moneyline is not null
        and kickoff is not null
      order by kickoff`,
    [startSeason],
  );
  log(`calibrate: ${games.length} completed games with closing lines`);

  // Group by (season, week); solve ratings ONCE per week at its first kickoff.
  const weeks = new Map();
  for (const g of games) {
    const k = `${g.season}-${g.week}`;
    if (!weeks.has(k)) weeks.set(k, []);
    weeks.get(k).push(g);
  }

  const samples = [];
  let done = 0;
  for (const [key, wkGames] of weeks) {
    const asOf = new Date(Math.min(...wkGames.map((g) => new Date(g.kickoff).getTime()))).toISOString();
    const [ratings, profiles] = await Promise.all([solveRatings(asOf), loadProfiles(asOf)]);

    for (const g of wkGames) {
      const p_market = devig(g.home_moneyline, g.away_moneyline);
      if (p_market === null) continue;

      const mo = momentumSignal(ratings, g.home_team, g.away_team);
      const ma = matchupSignal(profiles, ratings, g.home_team, g.away_team);
      const inj = await injurySignal(g, asOf);
      const sit = await situationalSignal({ ...g, marketFavoursHome: p_market >= 0.5 }, asOf);
      if (!mo.confidence) continue;              // no ratings yet (season openers)

      samples.push({
        game_id: g.game_id,
        p_market,
        outcome: g.home_score > g.away_score ? 1 : 0,
        signals: {
          momentum: mo.signal, matchup: ma.signal,
          injury: inj.signal, situational: sit.signal,
        },
      });
    }
    done++;
    if (done % 5 === 0) log(`  ...${done}/${weeks.size} weeks, ${samples.length} samples`);
  }
  return samples;
}

const SAMPLE_CACHE = CALIB_PATH.replace(/calibration.json$/, "calibration-samples.json");

async function calibrate({ write = true, useCache = false } = {}) {
  let samples;
  if (useCache && fs.existsSync(SAMPLE_CACHE)) {
    samples = JSON.parse(fs.readFileSync(SAMPLE_CACHE, "utf8"));
    log(`calibrate: reusing ${samples.length} cached samples`);
  } else {
    samples = await buildSamples();
    fs.writeFileSync(SAMPLE_CACHE, JSON.stringify(samples));
  }
  if (samples.length < 50) {
    throw new Error(`only ${samples.length} samples -- too few to fit anything meaningful`);
  }

  const pm = samples.map((s) => s.p_market - 0.5);
  const y = samples.map((s) => s.outcome);

  // Provisional scales convert each factor into probability points. Momentum
  // and matchup arrive in EPA/play; injury and situational already in prob.
  const scale0 = { momentum: 0.55, matchup: 0.55, injury: 1, situational: 1 };

  const beta = {};
  const diag = {};
  for (const f of FACTORS) {
    const raw = samples.map((s) => s.signals[f] * scale0[f]);
    beta[f] = slope(pm, raw);
    diag[f] = {
      corrWithMarket: +corr(pm, raw).toFixed(4),
      beta: +beta[f].toFixed(4),
    };
  }

  // Residualise, then see whether what is left predicts the OUTCOME at all.
  // This is the honest test: a residual uncorrelated with the result is a
  // factor with no edge left in it once the market has had its say.
  const scale = {};
  for (const f of FACTORS) {
    const resid = samples.map((s, i) => s.signals[f] * scale0[f] - beta[f] * pm[i]);
    const c = corr(resid, y);
    const b = slope(resid, y);
    diag[f].residCorrWithOutcome = +c.toFixed(4);
    diag[f].residSlopeToOutcome = +b.toFixed(4);
    // Damp a factor whose residual does not predict outcomes. Signed, NOT
    // absolute: a NEGATIVE residual correlation means the factor is
    // anti-predictive on this sample, and |c| would reward it with a high
    // weight for being wrong. Negative damps to zero. We deliberately do not
    // flip its sign -- at n=300 that would be fitting noise.
    const keep = Math.max(0, Math.min(1, c / 0.10));
    scale[f] = +(scale0[f] * keep).toFixed(4);
    diag[f].scaleKept = +keep.toFixed(3);
  }

  // Standard error of a correlation at this n, so the caller can see whether
  // any of this is distinguishable from zero.
  const se = 1 / Math.sqrt(samples.length);
  for (const f of FACTORS) {
    diag[f].seOfCorr = +se.toFixed(4);
    diag[f].sigmas = +(diag[f].residCorrWithOutcome / se).toFixed(2);
  }

  const out = {
    fittedAt: new Date().toISOString(),
    samples: samples.length,
    startSeason: cfg().data.startSeason,
    scale,
    beta,
    diagnostics: diag,
    note: "beta = the part of each factor the closing line already prices. " +
          "scale is damped toward 0 when the residual does not predict outcomes.",
  };

  if (write) {
    fs.writeFileSync(CALIB_PATH, JSON.stringify(out, null, 2));
    log(`calibration written to ${CALIB_PATH}`);
  }
  return out;
}

module.exports = { calibrate, buildSamples, devig, slope, corr, SAMPLE_CACHE };
