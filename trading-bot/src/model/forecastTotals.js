// trading-bot/src/model/forecastTotals.js
//
// THE RESIDUAL MODEL, IN POINTS.
//
//   implied_mean = line + sigma * Phi^-1(p_market)      <- invert the price
//   model_mean   = implied_mean + sum(w_k * scale_k * points_k)
//   p_fair       = Phi((model_mean - line) / sigma)     <- back to a price
//   delta        = capped(p_fair - p_market)
//
// The market price is the ANCHOR, exactly as in Adam-7. The difference is that
// the adjustment happens in points, because that is the unit a total is actually
// argued about: "the book says 44.5 and I think 47" is a statement a human can
// check, while "the book says 0.50 and I think 0.61" hides how big a claim it is.
//
// WHY INVERT THE PRICE INSTEAD OF PROJECTING FROM SCRATCH. A from-scratch total
// projection would be compared against the book's number, and most of the gap
// would be the model's own error rather than information -- both are built from
// largely the same public data. Reading the market's implied mean first and then
// moving it means the only thing that can move the forecast is a factor, not
// model noise.
//
// WHAT deltaCap MEANS HERE. 0.16 at sigma 10.5 is about +/-4.3 points of
// permitted disagreement at a coin-flip line, narrowing toward the extremes.
// That translation is the most useful sanity check on this whole file: if the
// model wants to move a total by 8 points, it is broken, and the cap will say so
// rather than sizing into it.
//
// CALIBRATION IS MANDATORY, not optional. Every factor here is something the
// market can see, and weather especially is something recreational money does
// price. Without a fitted estimate of how much of each factor is ALREADY in the
// closing line, the model would re-bet the book's own view back at it and call
// the difference edge. An uncalibrated run therefore produces no forecast at all.

const fs = require("fs");
const path = require("path");
const { cfg } = require("../config");
const { q } = require("../db");
const { weatherTotalsSignal } = require("../features/weatherTotals");
const { schemeSignal } = require("../features/scheme");
const { paceSignal, restSignal, situationalTotalsSignal } = require("../features/paceTotals");
const { injuryTotalsSignal } = require("../features/injuryTotals");

const CALIB_PATH = path.join(__dirname, "calibration-totals.json");
const FACTORS = ["weather", "scheme", "rest", "pace", "injury", "situational"];

// == Normal distribution ====================================================
// Abramowitz & Stegun 7.1.26 for the CDF, Acklam's rational approximation for
// the inverse. Both are accurate to well under a tenth of a probability point,
// which is far finer than a 1-cent tick.
function normCdf(z) {
  const s = z < 0 ? -1 : 1;
  const x = Math.abs(z) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * x);
  const y = 1 - ((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t
                - 0.284496736) * t + 0.254829592) * t * Math.exp(-x * x);
  return 0.5 * (1 + s * y);
}

function normInv(p) {
  if (p <= 0) return -Infinity;
  if (p >= 1) return Infinity;
  const a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
             1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00];
  const b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
             6.680131188771972e+01, -1.328068155288572e+01];
  const c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
             -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00];
  const d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
             3.754408661907416e+00];
  const pl = 0.02425;
  let x;
  if (p < pl) {
    const qq = Math.sqrt(-2 * Math.log(p));
    x = (((((c[0] * qq + c[1]) * qq + c[2]) * qq + c[3]) * qq + c[4]) * qq + c[5]) /
        ((((d[0] * qq + d[1]) * qq + d[2]) * qq + d[3]) * qq + 1);
  } else if (p <= 1 - pl) {
    const qq = p - 0.5;
    const r = qq * qq;
    x = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * qq /
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
  } else {
    const qq = Math.sqrt(-2 * Math.log(1 - p));
    x = -(((((c[0] * qq + c[1]) * qq + c[2]) * qq + c[3]) * qq + c[4]) * qq + c[5]) /
         ((((d[0] * qq + d[1]) * qq + d[2]) * qq + d[3]) * qq + 1);
  }
  return x;
}

/**
 * Fitted per-factor scales, or an explicit refusal.
 *
 * scale_k is the fraction of factor k that the closing line does NOT already
 * contain, fitted by model/calibrateTotals.js. A scale near 0 means the market
 * prices that factor as well as we do and there is nothing to trade; near 1 means
 * it is being ignored. There is no safe default value, which is why the absent
 * case refuses rather than guessing -- 1.0 would trade the market's own opinion
 * back at it and 0.0 would silently disable the bot while looking operational.
 */
function loadCalibration() {
  try {
    const c = JSON.parse(fs.readFileSync(CALIB_PATH, "utf8"));
    return { ...c, calibrated: true };
  } catch {
    return { calibrated: false, scale: null, sigma: null };
  }
}

/** Market P(over) from the recorded book, de-overrounded when both sides quote. */
function marketProbOver(overBook, underBook) {
  const o = overBook && overBook.mid !== null ? Number(overBook.mid) : null;
  const u = underBook && underBook.mid !== null ? Number(underBook.mid) : null;
  if (o === null && u === null) return null;
  if (o === null) return 1 - u;
  if (u === null) return o;
  const sum = o + u;
  return sum > 0 ? o / sum : o;
}

/**
 * Full totals forecast for one game on one line.
 *
 * `market` is a sports.pm_totals_markets row (carrying the line), `books` is
 * { over, under } from sports.odds_history, and `ctx` holds the per-tick context
 * that is identical across games (scheme traits, pace history).
 */
async function forecastTotal(game, market, books, asOf, ctx) {
  const m = cfg().model;
  const cal = ctx.calibration || loadCalibration();
  if (!cal.calibrated) {
    return { skip: "not_calibrated",
             detail: "model/calibration-totals.json missing -- run run/calibrate-totals.js" };
  }

  const p_market = marketProbOver(books.over, books.under);
  if (p_market === null) return { skip: "no_book" };

  const line = Number(market.line);
  const sigma = Number(cal.sigma) || m.sigmaTotalPoints;

  // Invert the price into the market's own expected total. Clamped away from the
  // tails first: at p 0.995 the inverse normal runs away and a one-cent quoting
  // artefact would become a 27-point implied total.
  const pClamped = Math.max(0.02, Math.min(0.98, p_market));
  const implied_mean = line + sigma * normInv(pClamped);

  const [weather, injury] = await Promise.all([
    weatherTotalsSignal(game, asOf),
    injuryTotalsSignal(game, asOf),
  ]);
  const raw = {
    weather,
    injury,
    scheme: schemeSignal(game, ctx.scheme),
    pace: paceSignal(game, ctx.pace),
    rest: restSignal(game),
    situational: situationalTotalsSignal(game),
  };

  // Weighted, scaled sum. scale strips the part the closing line already has.
  //
  // SUB-TERM SCALES WHERE THEY EXIST. Weather, rest and situational report named
  // parts (wind/cold/precip, restSum/shortWeek, division/primetime) and each gets
  // its own fitted scale. This is not cosmetic: a single lumped scale can only
  // answer "is weather, as a bundle, mispriced", so a real wind effect and a
  // useless cold effect cancel and the factor reads as noise. Rest fitted with the
  // WRONG SIGN as one number, which is exactly the shape of two sub-terms pulling
  // against each other. Factors without parts (scheme, pace, injury) fall back to
  // a single scale keyed on the factor name.
  const contrib = {};
  let pointsDelta = 0;
  for (const k of FACTORS) {
    const parts = raw[k].parts;
    let scaled = 0;
    const partDetail = {};
    if (parts && Object.keys(parts).length) {
      for (const [name, pts] of Object.entries(parts)) {
        const key = `${k}.${name}`;
        const sc = cal.scale[key] ?? 0;
        const v = (Number(pts) || 0) * sc;
        partDetail[name] = { points: +(Number(pts) || 0).toFixed(3), scale: +sc.toFixed(3),
                             scaled: +v.toFixed(3) };
        scaled += v;
      }
    } else {
      const sc = cal.scale[k] ?? 0;
      scaled = raw[k].points * sc;
      partDetail.whole = { points: +raw[k].points.toFixed(3), scale: +sc.toFixed(3),
                           scaled: +scaled.toFixed(3) };
    }
    const weighted = m.weights[k] * scaled;
    contrib[k] = {
      points: +raw[k].points.toFixed(3),
      scaled: +scaled.toFixed(3),
      weighted: +weighted.toFixed(3),
      confidence: +raw[k].confidence.toFixed(3),
      parts: partDetail,
    };
    pointsDelta += weighted;
  }

  // Weight-weighted average of the factors' own confidence.
  const confidence = FACTORS.reduce((s, k) => s + m.weights[k] * raw[k].confidence, 0);

  const model_mean = implied_mean + pointsDelta;
  const p_fair_raw = normCdf((model_mean - line) / sigma);

  // tanh saturation then the hard cap, so an extreme factor bends toward the
  // limit instead of hitting a wall -- a hard clamp would make every large
  // signal produce an identical bet.
  const rawDelta = p_fair_raw - p_market;
  const delta = m.deltaCap * Math.tanh(rawDelta / m.deltaCap) * confidence;
  const p_fair = Math.max(0.01, Math.min(0.99, p_market + delta));

  const out = {
    game_id: game.game_id,
    condition_id: market.condition_id,
    asof_ts: asOf,
    line,
    p_market,
    sigma,
    implied_mean_total: implied_mean,
    model_mean_total: model_mean,
    points_delta: pointsDelta,
    delta,
    p_fair,
    confidence,
    calibrated: true,
    f_weather: contrib.weather.weighted,
    f_scheme: contrib.scheme.weighted,
    f_rest: contrib.rest.weighted,
    f_pace: contrib.pace.weighted,
    f_injury: contrib.injury.weighted,
    f_situational: contrib.situational.weighted,
    inputs: {
      calibration: { sigma: cal.sigma, scale: cal.scale, fittedAt: cal.fittedAt, n: cal.n },
      contrib,
      // The whole reason a decision can be explained a month later without
      // recomputing it -- which would silently use today's data.
      detail: Object.fromEntries(FACTORS.map((k) => [k, raw[k].detail])),
      book: {
        over_mid: books.over ? Number(books.over.mid) : null,
        under_mid: books.under ? Number(books.under.mid) : null,
        over_best_ask: books.over ? Number(books.over.best_ask) : null,
        under_best_ask: books.under ? Number(books.under.best_ask) : null,
        over_ask_depth_usd: books.over ? Number(books.over.ask_depth_usd) : null,
        under_ask_depth_usd: books.under ? Number(books.under.ask_depth_usd) : null,
        ts: books.over ? books.over.ts : (books.under ? books.under.ts : null),
      },
      market: {
        slug: market.slug, liquidity_num: market.liquidity_num,
        fee_type: market.fee_type, fee_rate: market.fee_rate,
        tick_size: market.tick_size, min_order_usd: market.min_order_usd,
      },
      rawDeltaBeforeCap: +rawDelta.toFixed(5),
    },
    model_version: cfg().modelVersion,
  };

  const bad = nonFiniteFields(out);
  if (bad.length) {
    return { skip: "non_finite_forecast", nonFinite: bad,
             detail: { game_id: game.game_id, line, fields: bad } };
  }
  return out;
}

/**
 * Refuse to emit a forecast containing a non-finite number.
 *
 * NOT defensive padding -- this catches a whole class of silent failure. Every
 * gate in policy/totals.js is a comparison, and EVERY comparison against NaN is
 * false, so `if (edge < minEdge) skip` does not skip on a NaN edge. It passes.
 * On the first live dry run a single missing config key made injury points NaN,
 * which made p_fair NaN, and the policy proposed trades on 14 of 15 games with
 * "edge NaNc size $NaN". Nothing in the gate chain objected.
 *
 * So the forecast is validated at the boundary, once, and the offending factor is
 * named rather than leaving a NaN to be traced back from a nonsense trade log.
 */
function nonFiniteFields(f) {
  const bad = [];
  for (const k of ["p_market", "sigma", "implied_mean_total", "model_mean_total",
                   "points_delta", "delta", "p_fair", "confidence", "line"]) {
    if (!Number.isFinite(f[k])) bad.push(k);
  }
  for (const k of FACTORS) {
    const v = f.inputs.contrib[k];
    if (!Number.isFinite(v.points)) bad.push(`${k}.points`);
    if (!Number.isFinite(v.confidence)) bad.push(`${k}.confidence`);
  }
  return bad;
}

/** Persist a totals forecast and return its id, so a trade can reference it. */
async function saveForecastTotal(f) {
  const { rows } = await q(
    `insert into sports.features_totals
       (game_id, condition_id, asof_ts, line, p_market, sigma, implied_mean_total,
        f_weather, f_scheme, f_rest, f_pace, f_injury, f_situational,
        points_delta, model_mean_total, delta, p_fair, confidence, inputs, model_version)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
     returning id`,
    [f.game_id, f.condition_id, f.asof_ts, f.line, f.p_market, f.sigma,
     f.implied_mean_total, f.f_weather, f.f_scheme, f.f_rest, f.f_pace,
     f.f_injury, f.f_situational, f.points_delta, f.model_mean_total,
     f.delta, f.p_fair, f.confidence, JSON.stringify(f.inputs), f.model_version],
  );
  return rows[0].id;
}

module.exports = {
  forecastTotal, saveForecastTotal, marketProbOver, loadCalibration,
  normCdf, normInv, CALIB_PATH, FACTORS,
};
