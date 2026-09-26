// trading-bot/src/model/calibrateTotals.js
//
// Fits how much of each factor the CLOSING TOTAL already contains, and the SD of
// the total around that line. Writes model/calibration-totals.json.
//
// THE REGRESSION:
//
//   (actual_total - closing_total_line)  ~  sum_k beta_k * points_k
//
// The left side is what the market got wrong. So beta_k reads directly:
//
//   beta_k near 0  -- the line already prices this factor; there is nothing here
//   beta_k near 1  -- the market ignores it and our points estimate is about right
//   beta_k above 1 -- the market under-reacts and the factor should be amplified
//   beta_k below 0 -- the factor points the WRONG WAY; it must not be traded
//
// That last case is the one this exists to catch. Weather is the obvious
// candidate: bad weather pushing a total down is the single adjustment
// recreational money reliably does make, so it is entirely possible the line
// over-corrects and a weather-driven under is a losing bet. A model without this
// step would have no way to find that out except by losing money.
//
// SIGN CONVENTION. Each factor's points are already signed toward the over, and
// the target is signed the same way, so no flipping happens anywhere here.
//
// THE WEATHER SUBSTITUTION, stated plainly because it is the one methodological
// compromise in this file: there is no archive of what a forecast said three days
// before a 2025 game -- sports.nfl_weather only holds what this bot has recorded
// since it started running. So calibration feeds features/weatherTotals.js the
// OBSERVED temp and wind from nflverse instead. That is sound for asking how much
// of the weather effect the line contained, and it would be plain look-ahead
// leakage in a forecast, which is why the live path cannot reach these columns.
// The consequence is that beta_weather is fitted on slightly better information
// than the bot will ever have, so it is biased OPTIMISTIC and should be read with
// that in mind.

const fs = require("fs");
const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");
const { wls } = require("./fitScheme");
const { weatherPoints } = require("../features/weatherTotals");
const { loadSchemeContext, schemeSignal } = require("../features/scheme");
const { loadPace, paceSignal, restSignal, situationalTotalsSignal } = require("../features/paceTotals");
const { injuryTotalsSignal } = require("../features/injuryTotals");
const { CALIB_PATH, FACTORS } = require("./forecastTotals");

/**
 * One observation per finished game with a closing total.
 *
 * asOf is the KICKOFF, which is the moment the closing line was struck, so every
 * feature is read as of then and nothing downstream can see the result.
 */
/**
 * Flatten one factor's signal into named regressors.
 *
 * A factor that reports a parts object is fitted PER SUB-TERM; one that does not is
 * fitted whole. The distinction is the point of this file's second revision: as a
 * single lumped number, rest fitted with the wrong sign, which is exactly what two
 * sub-terms pulling against each other looks like.
 */
function flatten(name, sig, into) {
  if (sig.parts && Object.keys(sig.parts).length) {
    for (const [k, v] of Object.entries(sig.parts)) into[`${name}.${k}`] = Number(v) || 0;
  } else {
    into[name] = Number(sig.points) || 0;
  }
  return into;
}

async function buildSamples() {
  const c = cfg();
  // The CALIBRATION window, wider than data.startSeason on purpose -- see
  // run/backfill-history.js. Standard errors were the binding constraint: at 318
  // games every beta had an SE about its own size.
  const first = c.model.calibration.startSeason || c.data.startSeason;
  const { rows: games } = await q(
    `select * from sports.nfl_games
      where season >= $1
        and total_line is not null
        and home_score is not null and away_score is not null
        and kickoff is not null
      order by kickoff`,
    [first],
  );
  log(`calibrate-totals: ${games.length} finished games with a closing total`);

  // Scheme and pace contexts are keyed by when they are read, so they are rebuilt
  // per week rather than per game -- same point-in-time discipline as the scheme
  // fit, and about 45 queries instead of 600.
  const samples = [];
  let ctxKey = null;
  let schemeCtx = null;
  let paceCtx = null;

  for (const g of games) {
    const key = `${g.season}-${g.week}`;
    if (key !== ctxKey) {
      schemeCtx = await loadSchemeContext(g.season, g.week);
      paceCtx = await loadPace(g.kickoff, first);
      ctxKey = key;
    }
    const asOf = new Date(g.kickoff).toISOString();
    const actual = Number(g.home_score) + Number(g.away_score);
    const line = Number(g.total_line);

    const wx = weatherPoints({ roof: g.roof, wind: g.wind, temp: g.temp, precip: null });
    const injury = await injuryTotalsSignal(g, asOf);

    samples.push({
      game_id: g.game_id, season: g.season, week: g.week,
      y: actual - line,
      line, actual,
      x: [
        ["weather", wx],
        ["scheme", schemeSignal(g, schemeCtx)],
        ["rest", restSignal(g)],
        ["pace", paceSignal(g, paceCtx)],
        ["injury", injury],
        ["situational", situationalTotalsSignal(g)],
      ].reduce((acc, [n, sig]) => flatten(n, sig, acc), {}),
    });
  }
  return samples;
}

/** Fit and write model/calibration-totals.json. */
async function calibrateTotals({ write = true } = {}) {
  const cal = cfg().model.calibration;
  const samples = await buildSamples();
  if (samples.length < cal.minGames) {
    log.warn(`calibrate-totals: ${samples.length} games is below minGames (${cal.minGames}). ` +
             `Writing nothing -- the bot will refuse to trade until this succeeds.`);
    return { calibrated: false, n: samples.length };
  }

  // SIGMA first, and independently of the regression. This is the number that
  // converts points into a probability, so it is measured directly as the SD of
  // the closing line's own error rather than inferred from a model fit.
  const ys = samples.map((s) => s.y);
  const ymean = ys.reduce((a, b) => a + b, 0) / ys.length;
  const sigma = Math.sqrt(ys.reduce((a, b) => a + (b - ymean) ** 2, 0) / (ys.length - 1));

  // Only regressors that actually VARY across the sample can be fitted. Weather
  // sub-terms are the live risk: each hinge is zero in most games by design, so a
  // sample with few cold games gives nothing to regress on, and a beta fitted on
  // four games would be noise handed a share of the heaviest weight.
  const allKeys = [...new Set(samples.flatMap((sm) => Object.keys(sm.x)))].sort();
  const nonZero = (k) => samples.filter((sm) => Math.abs(sm.x[k] || 0) > 1e-6).length;
  const active = allKeys.filter((k) => {
    const nz = nonZero(k);
    if (nz < cal.minNonZeroGamesPerFactor) {
      log.warn(`calibrate-totals: ${k} is non-zero in only ${nz} games ` +
               `(needs ${cal.minNonZeroGamesPerFactor}) -- scale forced to 0`);
      return false;
    }
    return true;
  });

  const scale = Object.fromEntries(allKeys.map((k) => [k, 0]));
  let diagnostics = {};
  let r2 = 0;

  if (active.length) {
    const X = samples.map((sm) => [1, ...active.map((k) => sm.x[k] || 0)]);
    const res = wls(X, samples.map((sm) => sm.y), samples.map(() => 1), cal.ridge);
    r2 = res.r2;
    active.forEach((k, i) => {
      const beta = res.beta[i + 1];
      const t = res.t[i + 1];
      // Clamped to [0, maxScale]. A NEGATIVE beta means the regressor points the
      // wrong way, and the only safe response is to stop trading it -- amplifying
      // it in reverse would be fitting a sign off one sample.
      const clamped = Math.max(0, Math.min(cal.maxScale, beta));
      // Damped by reliability, t^2/(t^2+1), the same shrinkage the scheme fit
      // uses. Most of these betas will not be distinguishable from zero, and the
      // honest response is to trade a fraction rather than all or nothing.
      const keep = (t * t) / (t * t + 1);
      let sc = clamped * keep;
      // THE FLOOR. User decision, 2026-09-26: a factor they believe in should still
      // move the price when the history cannot confirm it -- the bot is supposed to
      // take risk, not wait for statistical permission. minScale therefore lifts a
      // weak-but-correctly-signed factor off zero.
      //
      // It is deliberately NOT applied to a wrong-signed factor. Flooring one of
      // those would mean betting against the only evidence available, which is a
      // different thing from taking risk where there is no evidence. Note this
      // only affects RESIDUAL mode; independent mode ignores scales entirely and
      // every factor already speaks at full weight.
      const floor = cal.minScale || 0;
      if (floor > 0 && beta >= 0 && sc < floor) sc = floor;
      scale[k] = +sc.toFixed(4);
      diagnostics[k] = {
        beta: +beta.toFixed(4), t: +t.toFixed(2), se: +res.se[i + 1].toFixed(4),
        clamped: +clamped.toFixed(4), reliability: +keep.toFixed(4),
        scale: scale[k], nonZeroGames: nonZero(k), wrongSign: beta < 0,
      };
    });
  }

  const out = {
    fittedAt: new Date().toISOString(),
    n: samples.length,
    seasons: [...new Set(samples.map((s) => s.season))],
    sigma: +sigma.toFixed(3),
    meanLineError: +ymean.toFixed(3),
    r2: +r2.toFixed(4),
    scale,
    diagnostics,
    regressors: allKeys,
    disabledRegressors: allKeys.filter((k) => !active.includes(k)),
  };

  if (write) fs.writeFileSync(CALIB_PATH, JSON.stringify(out, null, 2) + "\n");
  log(`calibrate-totals: n=${out.n} sigma=${out.sigma} r2=${out.r2} ` +
      `meanLineError=${out.meanLineError}`);
  for (const k of allKeys) {
    const d = diagnostics[k];
    if (!d) { log(`  ${k.padEnd(20)} DISABLED (insufficient variation)`); continue; }
    log(`  ${k.padEnd(20)} beta ${String(d.beta).padStart(8)}  t ${String(d.t).padStart(6)}  ` +
        `scale ${String(d.scale).padStart(7)}  nz ${String(d.nonZeroGames).padStart(4)}` +
        (d.wrongSign ? "   <-- WRONG SIGN, zeroed" : ""));
  }
  return out;
}

module.exports = { calibrateTotals, buildSamples };
