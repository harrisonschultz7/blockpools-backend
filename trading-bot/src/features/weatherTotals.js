// trading-bot/src/features/weatherTotals.js
//
// WEATHER -- Argo-7's heaviest factor (0.30). Output is in POINTS on the game
// total, negative meaning fewer points.
//
// A HINGE, NOT A SLOPE. This is the whole reason weather gets top weight and
// still behaves. A linear wind term is wrong on its own terms: 8 mph and 3 mph
// are both "no effect", so a coefficient fitted across the full range gets
// dragged toward zero by the hundreds of calm games and then under-reacts to the
// handful of genuinely windy ones. Flat below the threshold, steepening above,
// means the factor contributes EXACTLY ZERO in most games -- so its 0.30 weight
// only bites where the user intended it to.
//
// ONE-DIRECTIONAL, which is worth stating because it shapes the whole bot's
// behaviour. Wind, cold and rain all push the same way: fewer points. Argo-7 will
// therefore be a systematic UNDER buyer in bad weather. That is also the one
// adjustment recreational money reliably does make, so the edge here is probably
// thinner than the weight implies. A run of losing weather unders is a known
// property of this design, not a surprise.
//
// FORECASTS ONLY, from sports.nfl_weather, recorded by ingest/weather.js at the
// time we saw them. The nflverse temp/wind columns are observed after the game
// and using them would be a look-ahead leak that makes the backtest fiction.
//
// This is also why Argo-7's trading window is 72h rather than Adam-7's 192h: a
// wind forecast eight days out carries almost no information, and opening a
// window before the model's largest input exists would mean entering on a thesis
// the model cannot yet evaluate.

const { cfg } = require("../config");
const { q } = require("../db");

/** Most recent forecast recorded at or before asOf. Never a later one. */
async function latestForecast(gameId, asOf) {
  const { rows } = await q(
    `select forecast_temp_f, forecast_wind_mph, precip_prob, asof_ts
       from sports.nfl_weather
      where game_id = $1 and asof_ts <= $2
      order by asof_ts desc limit 1`,
    [gameId, asOf],
  );
  return rows[0] || null;
}

const hinge = (v, threshold, perUnit) =>
  Number.isFinite(v) && v > threshold ? (v - threshold) * perUnit : 0;

/**
 * The hinge arithmetic, with no I/O. Shared deliberately.
 *
 * The LIVE path feeds this recorded forecasts. The CALIBRATOR feeds it the
 * OBSERVED temp and wind from nflverse, because there is no archive of what the
 * forecast said three days before a 2025 game -- nfl_weather only holds what this
 * bot has recorded since it started running. That substitution is legitimate for
 * calibration, which is asking how much of the weather effect the closing line
 * already contained, and it would be straightforward look-ahead leakage in a
 * forecast. Keeping the shared piece pure is what makes the difference between
 * the two callers visible instead of buried.
 */
function weatherPoints({ roof, wind, temp, precip }) {
  const w = cfg().model.weather;
  const r = String(roof || "").toLowerCase();
  // UNKNOWN IS NOT INDOORS. nfl_games.roof is null on 37 of the 2025+ games
  // (7.5%), and the old test treated every one of them as a dome -- returning
  // zero weather with FULL confidence. That is right by luck for a retractable
  // like Lucas Oil and flatly wrong for Maracana in Rio, an open-air stadium
  // that was on the 2026 week-3 slate. An unknown roof has to read as unknown so
  // the policy can decline the game instead of silently discarding the
  // heaviest-weighted factor.
  const known = ["outdoors", "open", "dome", "closed"].includes(r);
  if (!known) {
    return { points: 0, unknownRoof: true, indoors: false,
             parts: { wind: 0, cold: 0, precip: 0 } };
  }
  const outdoors = ["outdoors", "open"].includes(r);
  if (w.requireOutdoors && !outdoors) {
    return { points: 0, indoors: true, parts: { wind: 0, cold: 0, precip: 0 } };
  }
  const parts = {};
  // Wind is the dominant term: it degrades the deep passing game and field-goal
  // range, both of which are scoring.
  parts.wind = -hinge(Number(wind), w.windThresholdMph, w.pointsPerMphOverThreshold);
  // Cold, measured as degrees BELOW the threshold, so the hinge runs downward.
  parts.cold = -hinge(-Number(temp), -w.coldThresholdF, w.pointsPerDegUnderThreshold);
  // Precipitation enters as a probability above its own threshold, scaled so a
  // certain soaking costs pointsAtCertainPrecip.
  // PERCENT, 0-100 -- see config _comment_precip_units. Open-Meteo returns
  // precipitation_probability as a percentage and ingest/weather.js stores it
  // raw, so both the threshold and the span below are in percent.
  const pp = Number(precip);
  parts.precip = Number.isFinite(pp) && pp > w.precipProbThreshold
    ? -((pp - w.precipProbThreshold) / (100 - w.precipProbThreshold)) * w.pointsAtCertainPrecip
    : 0;
  const raw = parts.wind + parts.cold + parts.precip;
  return {
    points: Math.max(-w.maxPoints, Math.min(w.maxPoints, raw)),
    raw,
    indoors: false,
    parts,
  };
}

async function weatherTotalsSignal(game, asOf) {
  const w = cfg().model.weather;

  const roof = String(game.roof || "").toLowerCase();
  if (!["outdoors", "open", "dome", "closed"].includes(roof)) {
    // Confidence 0, not 1. The difference matters: a confident zero lets the
    // game trade with no weather input, which for a weather-led model is the
    // worst of both worlds.
    return { points: 0, confidence: 0,
             detail: { roof: game.roof, reason: "unknown roof", stadium: game.stadium } };
  }
  const outdoors = ["outdoors", "open"].includes(roof);
  if (w.requireOutdoors && !outdoors) {
    return { points: 0, confidence: 1, detail: { roof, indoors: true } };
  }

  const wx = await latestForecast(game.game_id, asOf);
  if (!wx) {
    // Not a zero with confidence -- the largest input is MISSING. Reporting
    // confidence 0 lets the caller skip the game rather than trade it as though
    // the weather were known to be fine.
    return { points: 0, confidence: 0, detail: { roof, reason: "no forecast recorded" } };
  }

  const wind = Number(wx.forecast_wind_mph);
  const temp = Number(wx.forecast_temp_f);
  const precip = Number(wx.precip_prob);
  const { points, raw, parts } = weatherPoints({ roof, wind, temp, precip });

  // A forecast goes stale. Inside a 72h window it should be hours old, and an
  // old one is downweighted rather than trusted or discarded.
  const ageH = (new Date(asOf).getTime() - new Date(wx.asof_ts).getTime()) / 3600000;
  const confidence = ageH <= w.forecastMaxAgeHours ? 1
    : ageH <= w.forecastMaxAgeHours * 3 ? 0.6 : 0.3;

  return {
    points,
    confidence,
    // NAMED SUB-TERMS, calibrated independently. Wind, cold and rain are three
    // different physical claims, and lumping them into one number means the
    // regression can only ever answer "is weather, as a bundle, priced?" -- so a
    // strong wind effect and a useless cold effect cancel and the whole factor
    // reads as noise. Fitting a scale per sub-term is what lets the model keep
    // the part that works.
    parts: { wind: parts.wind, cold: parts.cold, precip: parts.precip },
    detail: {
      roof, wind, temp, precip,
      windPoints: +parts.wind.toFixed(3),
      coldPoints: +parts.cold.toFixed(3),
      precipPoints: +parts.precip.toFixed(3),
      uncapped: +raw.toFixed(3),
      forecastAgeHours: +ageH.toFixed(1),
      // True in the overwhelming majority of games, and the point of the hinge.
      belowAllThresholds: raw === 0,
    },
  };
}

module.exports = { weatherTotalsSignal, weatherPoints, latestForecast, hinge };
