// trading-bot/src/features/nba/arenas.js
//
// Home arena of each franchise: coordinates for travel distance, and the UTC
// offset (standard time) for time zones crossed. Keyed by Polymarket code, the
// code sports.nba_games stores. A neutral-site game (Mexico City, Paris, the NBA
// Cup final in Las Vegas) is NOT here -- travel for those is skipped rather
// than computed from the wrong city.

const ARENAS = {
  ATL: { lat: 33.757, lon: -84.396, tz: -5 },
  BOS: { lat: 42.366, lon: -71.062, tz: -5 },
  BKN: { lat: 40.683, lon: -73.975, tz: -5 },
  CHA: { lat: 35.225, lon: -80.839, tz: -5 },
  CHI: { lat: 41.881, lon: -87.674, tz: -6 },
  CLE: { lat: 41.497, lon: -81.688, tz: -5 },
  DAL: { lat: 32.790, lon: -96.810, tz: -6 },
  DEN: { lat: 39.749, lon: -105.008, tz: -7 },
  DET: { lat: 42.341, lon: -83.055, tz: -5 },
  GSW: { lat: 37.768, lon: -122.388, tz: -8 },
  HOU: { lat: 29.751, lon: -95.362, tz: -6 },
  IND: { lat: 39.764, lon: -86.156, tz: -5 },
  LAC: { lat: 33.945, lon: -118.343, tz: -8 },  // Intuit Dome, Inglewood
  LAL: { lat: 34.043, lon: -118.267, tz: -8 },
  MEM: { lat: 35.138, lon: -90.051, tz: -6 },
  MIA: { lat: 25.781, lon: -80.188, tz: -5 },
  MIL: { lat: 43.045, lon: -87.917, tz: -6 },
  MIN: { lat: 44.979, lon: -93.276, tz: -6 },
  NOP: { lat: 29.949, lon: -90.082, tz: -6 },
  NYK: { lat: 40.751, lon: -73.993, tz: -5 },
  OKC: { lat: 35.463, lon: -97.515, tz: -6 },
  ORL: { lat: 28.539, lon: -81.384, tz: -5 },
  PHI: { lat: 39.901, lon: -75.172, tz: -5 },
  PHX: { lat: 33.446, lon: -112.071, tz: -7 },  // Arizona: no DST, -7 all season
  POR: { lat: 45.532, lon: -122.667, tz: -8 },
  SAC: { lat: 38.580, lon: -121.500, tz: -8 },
  SAS: { lat: 29.427, lon: -98.438, tz: -6 },
  TOR: { lat: 43.643, lon: -79.379, tz: -5 },
  UTA: { lat: 40.768, lon: -111.901, tz: -7 },
  WAS: { lat: 38.898, lon: -77.021, tz: -5 },
};

/** Great-circle miles between two arenas, null when either is unknown. */
function milesBetween(a, b) {
  const A = ARENAS[a], B = ARENAS[b];
  if (!A || !B) return null;
  const rad = (d) => (d * Math.PI) / 180;
  const dLat = rad(B.lat - A.lat), dLon = rad(B.lon - A.lon);
  const h = Math.sin(dLat / 2) ** 2 + Math.cos(rad(A.lat)) * Math.cos(rad(B.lat)) * Math.sin(dLon / 2) ** 2;
  return 3958.8 * 2 * Math.asin(Math.sqrt(h));
}

/** Hours of clock lost flying from a to b (positive = eastward, the hard way). */
function hoursEast(a, b) {
  const A = ARENAS[a], B = ARENAS[b];
  return A && B ? B.tz - A.tz : 0;
}

module.exports = { ARENAS, milesBetween, hoursEast };
