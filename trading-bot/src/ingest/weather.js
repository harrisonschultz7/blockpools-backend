// trading-bot/src/ingest/weather.js
//
// Weather FORECASTS from Open-Meteo (free, no key), recorded append-only.
//
// Why this exists at all: nflverse `temp`/`wind` are observed after the game --
// 0 of 240 future games have them populated. Using those columns to score an
// upcoming game would be a look-ahead leak; using them to score a PAST game in
// a backtest would be a worse one, because the model would "know" the weather
// that actually occurred rather than the forecast that was available.
//
// So every forecast is stamped with the moment we saw it, and the feature layer
// reads the newest forecast at or before its asOf.
//
// LIMITATION: venue is keyed off the home team, which is wrong for
// international games (London/Munich/Sao Paulo). Those are flagged rather than
// silently given the home team's domestic weather -- see `isNeutralSite`.

const { ENV } = require("../config");
const { getJson } = require("../http");
const { q, bulkInsert } = require("../db");
const log = require("../log");

// Home venue coordinates by team. Shared venues (NYG/NYJ, LA/LAC) repeat.
const VENUE = {
  ARI: [33.5277, -112.2626], ATL: [33.7554, -84.4009], BAL: [39.2780, -76.6227],
  BUF: [42.7738, -78.7870],  CAR: [35.2258, -80.8528], CHI: [41.8623, -87.6167],
  CIN: [39.0955, -84.5161],  CLE: [41.5061, -81.6995], DAL: [32.7473, -97.0945],
  DEN: [39.7439, -105.0201], DET: [42.3400, -83.0456], GB:  [44.5013, -88.0622],
  HOU: [29.6847, -95.4107],  IND: [39.7601, -86.1639], JAX: [30.3239, -81.6373],
  KC:  [39.0489, -94.4839],  LA:  [33.9535, -118.3392], LAC: [33.9535, -118.3392],
  LV:  [36.0909, -115.1833], MIA: [25.9580, -80.2389], MIN: [44.9738, -93.2578],
  NE:  [42.0909, -71.2643],  NO:  [29.9511, -90.0812], NYG: [40.8135, -74.0745],
  NYJ: [40.8135, -74.0745],  PHI: [39.9008, -75.1675], PIT: [40.4468, -80.0158],
  SEA: [47.5952, -122.3316], SF:  [37.4033, -121.9694], TB:  [27.9759, -82.5033],
  TEN: [36.1665, -86.7713],  WAS: [38.9076, -76.8645],
};

/** nflverse marks international games with a non-Home location. */
const isNeutralSite = (g) => String(g.location || "").toLowerCase() === "neutral";

async function ingestWeather() {
  // Only outdoor games inside Open-Meteo's ~16-day forecast horizon are worth
  // asking about. Domes are skipped entirely -- the feature ignores them.
  const { rows: games } = await q(
    `select game_id, home_team, kickoff, roof, stadium
       from sports.nfl_games
      where kickoff between now() and now() + interval '15 days'
        and lower(coalesce(roof,'')) in ('outdoors','open')
      order by kickoff`,
  );
  if (!games.length) { log("weather: no outdoor games in forecast range"); return 0; }

  // One API call per venue, not per game -- a Sunday slate shares few venues
  // but the dedupe still halves the calls on divisional weekends.
  const byVenue = new Map();
  for (const g of games) {
    const coords = VENUE[g.home_team];
    if (!coords) { log.warn(`weather: no venue coords for ${g.home_team}`); continue; }
    const key = coords.join(",");
    if (!byVenue.has(key)) byVenue.set(key, { coords, games: [] });
    byVenue.get(key).games.push(g);
  }

  const out = [];
  for (const { coords, games: vg } of byVenue.values()) {
    const [lat, lon] = coords;
    const u = `${ENV.OPEN_METEO_URL}?latitude=${lat}&longitude=${lon}` +
              `&hourly=temperature_2m,wind_speed_10m,precipitation_probability` +
              `&temperature_unit=fahrenheit&wind_speed_unit=mph&forecast_days=16&timezone=UTC`;
    let data;
    try { data = await getJson(u); }
    catch (e) { log.warn(`weather fetch failed ${lat},${lon}: ${e.message}`); continue; }

    const times = (data.hourly && data.hourly.time) || [];
    if (!times.length) continue;
    const stamps = times.map((t) => Date.parse(t + "Z"));

    for (const g of vg) {
      const kick = new Date(g.kickoff).getTime();
      // Nearest forecast hour to kickoff.
      let best = 0, bestDiff = Infinity;
      for (let i = 0; i < stamps.length; i++) {
        const d = Math.abs(stamps[i] - kick);
        if (d < bestDiff) { bestDiff = d; best = i; }
      }
      if (bestDiff > 6 * 3600 * 1000) continue;    // kickoff outside the horizon
      out.push({
        game_id: g.game_id,
        forecast_temp_f: data.hourly.temperature_2m[best],
        forecast_wind_mph: data.hourly.wind_speed_10m[best],
        precip_prob: data.hourly.precipitation_probability[best],
        source: "open-meteo",
      });
    }
  }

  if (out.length) {
    await bulkInsert("sports.nfl_weather",
      ["game_id", "forecast_temp_f", "forecast_wind_mph", "precip_prob", "source"], out);
  }
  log(`weather: ${out.length} forecasts across ${byVenue.size} venues`);
  return out.length;
}

module.exports = { ingestWeather, VENUE, isNeutralSite };
