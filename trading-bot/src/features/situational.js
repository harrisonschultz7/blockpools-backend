// trading-bot/src/features/situational.js
//
// SITUATIONAL (weight 0.20): rest, travel, primetime, division, weather.
//
// These get real weight because they are the slowest things for casual money
// to price. A recreational bettor picks a team; they do not discount it for a
// short week after a cross-country trip into 18 mph wind.
//
// Everything here is available BEFORE kickoff, which is the point --
// away_rest/home_rest are populated for all 240 future games, unlike temp/wind
// which nflverse only fills in afterwards. Forecast weather therefore comes
// from sports.nfl_weather (recorded by ingest/weather.js), never from the
// nflverse columns, which would be a look-ahead leak.
//
// Sign convention: positive favours the HOME team.
//
// No home-field term. The market price already contains it; this is a residual
// model and pricing it twice would be the single easiest way to lose money.

const { cfg } = require("../config");
const { q } = require("../db");

async function latestForecast(gameId, asOf) {
  const { rows } = await q(
    `select forecast_temp_f, forecast_wind_mph, precip_prob
       from sports.nfl_weather
      where game_id = $1 and asof_ts <= $2
      order by asof_ts desc limit 1`,
    [gameId, asOf],
  );
  return rows[0] || null;
}

async function situationalSignal(game, asOf) {
  const c = cfg().model.situational;
  const parts = {};
  let signal = 0;

  // Rest differential. Positive when the home side is the fresher one.
  const homeRest = Number(game.home_rest);
  const awayRest = Number(game.away_rest);
  if (Number.isFinite(homeRest) && Number.isFinite(awayRest)) {
    const restEdge = (homeRest - awayRest) * c.restDayProbPerDay;
    parts.rest = +restEdge.toFixed(5);
    signal += restEdge;

    // A short week is worse than the day count alone implies -- less practice,
    // less recovery, no travel buffer. Applied to whichever side is on one.
    if (homeRest <= 4 && awayRest > 4) { parts.shortWeek = -c.shortWeekPenalty; signal -= c.shortWeekPenalty; }
    if (awayRest <= 4 && homeRest > 4) { parts.shortWeek = c.shortWeekPenalty; signal += c.shortWeekPenalty; }
  }

  // Primetime road games: travel plus a short turnaround into a loud building.
  const isPrimetime = /^(19|20|21|22):/.test(String(game.gametime || "")) ||
                      ["Thursday", "Monday", "Sunday"].includes(game.weekday) &&
                      /^(20|21):/.test(String(game.gametime || ""));
  if (isPrimetime) {
    parts.primetimeRoad = c.primetimeRoadPenalty;
    signal += c.primetimeRoadPenalty;
  }

  // Weather, outdoor games only, from the FORECAST we recorded -- never from
  // nflverse temp/wind, which are observed after the fact.
  const outdoors = ["outdoors", "open"].includes(String(game.roof || "").toLowerCase());
  if (outdoors) {
    const wx = await latestForecast(game.game_id, asOf);
    if (wx) {
      const wind = Number(wx.forecast_wind_mph);
      const temp = Number(wx.forecast_temp_f);
      // Bad conditions compress scoring, which helps the weaker side by
      // shrinking the number of possessions the better team can exploit. The
      // home team is usually (not always) the favourite, so this is applied
      // against whichever side the market favours -- resolved by the caller
      // passing marketFavoursHome.
      let envPenalty = 0;
      if (Number.isFinite(wind) && wind > c.windThresholdMph) {
        envPenalty += (wind - c.windThresholdMph) * c.windProbPerMphOver;
      }
      if (Number.isFinite(temp) && temp < c.coldThresholdF) {
        envPenalty += (c.coldThresholdF - temp) * c.coldProbPerDegUnder;
      }
      if (envPenalty > 0) {
        const towardUnderdog = game.marketFavoursHome ? -envPenalty : envPenalty;
        parts.weather = +towardUnderdog.toFixed(5);
        parts.wind = wind;
        parts.temp = temp;
        signal += towardUnderdog;
      }
    } else {
      parts.weather = "no forecast recorded";
    }
  }

  // Division games are famously closer than the ratings imply -- familiarity
  // compresses the spread. Damp the whole signal rather than adding a term.
  if (game.div_game) {
    signal *= (1 - c.divisionGameDamp);
    parts.divisionDamp = c.divisionGameDamp;
  }

  const capped = Math.max(-c.maxProbSwing, Math.min(c.maxProbSwing, signal));
  return {
    signal: capped,
    confidence: Number.isFinite(homeRest) && Number.isFinite(awayRest) ? 1 : 0.5,
    detail: { ...parts, uncapped: +signal.toFixed(5), outdoors, isPrimetime },
  };
}

module.exports = { situationalSignal, latestForecast };
