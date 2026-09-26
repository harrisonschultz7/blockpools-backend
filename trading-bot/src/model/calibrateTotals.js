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
async function buildSamples() {
  const c = cfg();
  const { rows: games } = await q(
    `select * from sports.nfl_games
      where season >= $1
        and total_line is not null
        and home_score is not null and away_score is not null
        and kickoff is not null
      order by kickoff`,
    [c.data.startSeason],
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
      paceCtx = await loadPace(g.kickoff, c.data.startSeason);
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
      x: {
        weather: wx.points,
        scheme: schemeSignal(g, schemeCtx).points,
        rest: restSignal(g).points,
        pace: paceSignal(g, paceCtx).points,
        injury: injury.points,
        situational: situationalTotalsSignal(g).points,
      },
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

  // Only factors that actually VARY across the sample can be fitted. Weather is
  // the live risk here: the hinge is zero in most games by design, so if the
  // sample happens to contain few windy or cold games there is nothing to
  // regress, and a beta fitted on four games would be noise handed the heaviest
  // weight in the model.
  const active = FACTORS.filter((k) => {
    const nz = samples.filter((s) => Math.abs(s.x[k]) > 1e-6).length;
    if (nz < cal.minNonZeroGamesPerFactor) {
      log.warn(`calibrate-totals: ${k} is non-zero in only ${nz} games ` +
               `(needs ${cal.minNonZeroGamesPerFactor}) -- scale forced to 0, factor disabled`);
      return false;
    }
    return true;
  });

  const scale = Object.fromEntries(FACTORS.map((k) => [k, 0]));
  let diagnostics = {};
  let r2 = 0;

  if (active.length) {
    const X = samples.map((s) => [1, ...active.map((k) => s.x[k])]);
    const res = wls(X, samples.map((s) => s.y), samples.map(() => 1), cal.ridge);
    r2 = res.r2;
    active.forEach((k, i) => {
      const beta = res.beta[i + 1];
      const t = res.t[i + 1];
      // Clamped to [0, maxScale]. A NEGATIVE beta means the factor points the
      // wrong way, and the only safe response is to stop trading it -- amplifying
      // it in reverse would be fitting the sign off one season.
      const clamped = Math.max(0, Math.min(cal.maxScale, beta));
      // Damped by reliability, the same t^2/(t^2+1) shrinkage the scheme fit uses.
      // With ~300 games and a factor that fires rarely, most of these betas will
      // be statistically indistinguishable from zero, and the honest response is
      // to trade a fraction of them rather than all or none.
      const keep = (t * t) / (t * t + 1);
      scale[k] = +(clamped * keep).toFixed(4);
      diagnostics[k] = {
        beta: +beta.toFixed(4), t: +t.toFixed(2), se: +res.se[i + 1].toFixed(4),
        clamped: +clamped.toFixed(4), reliability: +keep.toFixed(4),
        scale: scale[k],
        nonZeroGames: samples.filter((s) => Math.abs(s.x[k]) > 1e-6).length,
        wrongSign: beta < 0,
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
    disabledFactors: FACTORS.filter((k) => !active.includes(k)),
  };

  if (write) fs.writeFileSync(CALIB_PATH, JSON.stringify(out, null, 2) + "\n");
  log(`calibrate-totals: n=${out.n} sigma=${out.sigma} r2=${out.r2} ` +
      `meanLineError=${out.meanLineError}`);
  for (const k of FACTORS) {
    const d = diagnostics[k];
    if (!d) { log(`  ${k.padEnd(12)} DISABLED (insufficient variation)`); continue; }
    log(`  ${k.padEnd(12)} beta ${String(d.beta).padStart(8)}  t ${String(d.t).padStart(6)}  ` +
        `scale ${String(d.scale).padStart(7)}  nz ${d.nonZeroGames}` +
        (d.wrongSign ? "   <-- WRONG SIGN, zeroed" : ""));
  }
  return out;
}

module.exports = { calibrateTotals, buildSamples };
