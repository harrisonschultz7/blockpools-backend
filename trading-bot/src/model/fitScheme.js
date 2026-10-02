// trading-bot/src/model/fitScheme.js
//
// Fits the league-wide scheme INTERACTION coefficients that features/scheme.js
// applies. Writes model/scheme-fit.json.
//
// Design matrix per observation (one offence in one game):
//
//   [1, A, T, P, B, A*P, A*B, T*P, T*B]
//
// where A = offence aggression, T = offence tempo, P = defence pressure,
// B = defence front weight, all z-scored as of BEFORE that game.
//
// MAIN EFFECTS ARE CONTROLS ONLY. A, T, P and B are in the matrix so the
// interaction coefficients are not left absorbing main-effect variance, and
// then they are discarded: the forecast uses the four product terms alone. An
// interaction fitted WITHOUT its main effects is a well-known way to produce a
// large, confident and meaningless coefficient.
//
// WEIGHTED least squares, by neutral play count TIMES season recency. An
// 18-snap target is a much noisier measurement of an offence's EPA than a
// 53-snap one, and unweighted OLS treats them as equals.
//
// The recency half is a judgement, not a measurement: how a team plays is a
// property of this year's staff and personnel, so an old season is evidence
// about a team that no longer exists. seasonRecencyDecay applies decay^(latest
// season - this season), so at 0.5 a 2025 observation counts half of a 2026
// one. Default 1 leaves the old unweighted-by-season behaviour in place.
//
// POINT-IN-TIME. Traits for a week-N game are pooled from weeks before N only,
// which is why this walks the calendar week by week instead of building one
// context and reusing it. Building the context once from the full season would
// leak the outcome into its own predictors and produce a fit that backtests
// beautifully and earns nothing.

const fs = require("fs");
const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");
const { loadSchemeContext, interactionTerms, FIT_PATH } = require("../features/scheme");

const TERMS = ["aggr_pressure", "aggr_box", "tempo_pressure", "tempo_box"];
const COLS = ["intercept", "aggression", "tempo", "pressure", "frontWeight", ...TERMS];

/** Solve (A + ridge*I) x = b by Gauss-Jordan, returning x and the inverse. */
function solveWithInverse(A, b, ridge) {
  const n = A.length;
  // [A | I | b]
  const m = A.map((row, i) => [
    ...row.map((v, j) => v + (i === j ? ridge : 0)),
    ...Array.from({ length: n }, (_, j) => (i === j ? 1 : 0)),
    b[i],
  ]);
  for (let col = 0; col < n; col++) {
    let piv = col;
    for (let r = col + 1; r < n; r++) if (Math.abs(m[r][col]) > Math.abs(m[piv][col])) piv = r;
    if (Math.abs(m[piv][col]) < 1e-12) throw new Error(`singular design matrix at column ${col}`);
    [m[col], m[piv]] = [m[piv], m[col]];
    const d = m[col][col];
    for (let j = col; j < 2 * n + 1; j++) m[col][j] /= d;
    for (let r = 0; r < n; r++) {
      if (r === col) continue;
      const f = m[r][col];
      if (f === 0) continue;
      for (let j = col; j < 2 * n + 1; j++) m[r][j] -= f * m[col][j];
    }
  }
  return {
    x: m.map((row) => row[2 * n]),
    inv: m.map((row) => row.slice(n, 2 * n)),
  };
}

/** Weighted least squares with t-statistics. */
function wls(X, y, w, ridge) {
  const n = X.length;
  const p = X[0].length;
  const XtWX = Array.from({ length: p }, () => Array(p).fill(0));
  const XtWy = Array(p).fill(0);
  for (let i = 0; i < n; i++) {
    for (let a = 0; a < p; a++) {
      XtWy[a] += w[i] * X[i][a] * y[i];
      for (let b2 = a; b2 < p; b2++) XtWX[a][b2] += w[i] * X[i][a] * X[i][b2];
    }
  }
  for (let a = 0; a < p; a++) for (let b2 = 0; b2 < a; b2++) XtWX[a][b2] = XtWX[b2][a];

  const { x: beta, inv } = solveWithInverse(XtWX, XtWy, ridge);

  let rss = 0, tss = 0, wsum = 0, ymean = 0;
  for (let i = 0; i < n; i++) { ymean += w[i] * y[i]; wsum += w[i]; }
  ymean /= wsum;
  for (let i = 0; i < n; i++) {
    const pred = X[i].reduce((s, v, a) => s + v * beta[a], 0);
    rss += w[i] * (y[i] - pred) ** 2;
    tss += w[i] * (y[i] - ymean) ** 2;
  }
  const dof = Math.max(1, n - p);
  const sigma2 = rss / dof;
  const se = inv.map((row, a) => Math.sqrt(Math.max(0, sigma2 * row[a])));
  return {
    beta,
    se,
    t: beta.map((b2, a) => (se[a] > 0 ? b2 / se[a] : 0)),
    r2: tss > 0 ? 1 - rss / tss : 0,
    n,
    dof,
  };
}

/**
 * Walk the calendar and build one observation per (game, offence).
 *
 * The context is rebuilt for every (season, week) so that a week-N observation
 * only ever sees traits pooled from earlier weeks. That is one query per week --
 * about 45 of them across two seasons, which is cheap for the guarantee.
 */
async function buildSamples() {
  const c = cfg();
  // The FIT window, not the trait window. See model.scheme.fitStartSeason.
  const first = c.model.scheme.fitStartSeason || c.data.schemePriorSeason;
  const { rows: weeks } = await q(
    `select distinct season, week from sports.nfl_team_scheme_game
      where season >= $1 order by season, week`,
    [first],
  );

  const samples = [];
  for (const { season, week } of weeks) {
    const ctx = await loadSchemeContext(season, week, { fromSeason: first });
    if (!ctx.traits.size) continue;
    const { rows } = await q(
      `select team, opponent, game_id, off_epa_neutral, off_neutral_plays
         from sports.nfl_team_scheme_game
        where season = $1 and week = $2
          and off_epa_neutral is not null and off_neutral_plays > 0`,
      [season, week],
    );
    for (const r of rows) {
      const off = ctx.traits.get(r.team);
      const def = ctx.traits.get(r.opponent);
      if (!off || !def) continue;          // no history yet -- week 1 of the first season
      const terms = interactionTerms(off.axes, def.axes);
      samples.push({
        season, week, game_id: r.game_id, team: r.team, opponent: r.opponent,
        y: Number(r.off_epa_neutral),
        // Season recency is applied after the loop, once the latest season in
        // the sample is known -- it cannot be computed here without assuming
        // which season is newest.
        w: Number(r.off_neutral_plays),
        x: [1, off.axes.aggression, off.axes.tempo, def.axes.pressure,
            def.axes.frontWeight, ...TERMS.map((t) => terms[t])],
      });
    }
  }
  // Scale by season recency now that the newest season in the sample is known.
  const decay = Number(c.model.scheme.seasonRecencyDecay ?? 1);
  if (samples.length && decay > 0 && decay !== 1) {
    const latest = Math.max(...samples.map((s) => s.season));
    for (const s of samples) s.w *= Math.pow(decay, latest - s.season);
  }
  return samples;
}

/** Fit and write model/scheme-fit.json. */
async function fitScheme({ write = true } = {}) {
  const sc = cfg().model.scheme;
  const samples = await buildSamples();
  log(`scheme fit: ${samples.length} team-game observations`);

  if (samples.length < sc.minFitGames) {
    log.warn(`scheme fit: ${samples.length} observations is below minFitGames ` +
             `(${sc.minFitGames}). Refusing to write a fit -- features/scheme.js ` +
             `will contribute 0 until there is enough sample.`);
    return { fitted: false, n: samples.length };
  }

  const res = wls(samples.map((s) => s.x), samples.map((s) => s.y),
                  samples.map((s) => s.w), sc.fitRidge);

  const named = {};
  COLS.forEach((k, i) => {
    named[k] = { beta: +res.beta[i].toFixed(6), se: +res.se[i].toFixed(6), t: +res.t[i].toFixed(2) };
  });

  // Interactions only. Main effects were controls and are recorded for audit but
  // deliberately NOT carried into the forecast -- see the header.
  //
  // Each beta is then DAMPED BY ITS OWN RELIABILITY, t^2 / (t^2 + 1). On the
  // 2022+ fit only tempo_pressure reaches t 2.28; the other three sit between
  // 0.11 and 1.58 and are, on the evidence, noise -- but noise with real
  // magnitude, since 0.011 EPA/play across 64 plays is 0.7 points. The
  // alternatives are both worse: keeping raw betas trades that noise at full
  // size, and hard-dropping terms below a t cutoff is its own selection step
  // that biases whatever survives. Damping shrinks the unreliable terms toward
  // zero in proportion to how unreliable they are and leaves a strong one nearly
  // intact (t 2.28 keeps 84%, t 0.11 keeps 1%). Same shape as the correlation
  // damping in model/calibrate.js, which exists for the same reason.
  const coef = {};
  const shrink = {};
  TERMS.forEach((t) => {
    const tv = named[t].t;
    const keep = sc.betaShrinkByT === false ? 1 : (tv * tv) / (tv * tv + 1);
    shrink[t] = +keep.toFixed(4);
    coef[t] = +(named[t].beta * keep).toFixed(6);
  });

  const maxAbsT = Math.max(...TERMS.map((t) => Math.abs(named[t].t)));
  const out = {
    fittedAt: new Date().toISOString(),
    n: res.n,
    r2: +res.r2.toFixed(4),
    seasons: [...new Set(samples.map((s) => s.season))],
    fitStartSeason: cfg().model.scheme.fitStartSeason || null,
    traitStartSeason: cfg().data.schemePriorSeason,
    coef,
    rawCoef: Object.fromEntries(TERMS.map((t) => [t, named[t].beta])),
    shrinkFactor: shrink,
    mainEffects: Object.fromEntries(
      ["aggression", "tempo", "pressure", "frontWeight"].map((k) => [k, named[k]])),
    diagnostics: named,
    maxAbsTInteraction: +maxAbsT.toFixed(2),
    // Read this before trusting the factor. Below 2 the interaction is not
    // distinguishable from noise at this sample, whatever the betas look like.
    significant: maxAbsT >= 2,
  };

  if (write) fs.writeFileSync(FIT_PATH, JSON.stringify(out, null, 2) + "\n");
  log(`scheme fit: r2 ${out.r2}, max |t| on an interaction ${out.maxAbsTInteraction}` +
      (out.significant ? "" : "  <-- NOT significant at this sample"));
  for (const t of TERMS) {
    log(`  ${t.padEnd(16)} beta ${named[t].beta.toFixed(5).padStart(9)}  t ${named[t].t.toFixed(2).padStart(6)}`);
  }
  return out;
}

module.exports = { fitScheme, buildSamples, wls, solveWithInverse, TERMS, COLS };
