// trading-bot/src/policy/totals.js
//
// MEDIUM-RISK TOTALS POLICY. The forecast says what a line is worth; this decides
// whether to act, on which side, and how big.
//
// Same shape as policy/medium.js, with three differences that matter:
//
//  1. SIDE IS over / under, chosen by the sign of delta -- never by whether the
//     price is above or below 50 cents. A market at 72c with a positive delta is
//     an over buy at 72c; the coin-flip level has no special status. Worth
//     spelling out because "buy yes if above 50" is the intuitive-but-wrong rule.
//
//  2. ONE LINE PER GAME. The caller has already picked the deepest book. Taking
//     three lines on the same game would be the same bet three times -- they are
//     correlated near perfectly -- while consuming three times the exposure cap
//     and looking like diversification in the trade log.
//
//  3. THE 72-HOUR WINDOW. Weather carries the heaviest weight and a wind forecast
//     eight days out is close to worthless, so the window opens where the model's
//     largest input starts to exist rather than as early as a market is quoted.
//
// Edge is measured against the price actually PAYABLE (best ask), never the
// midpoint. Using the mid manufactures half the spread as free edge on every
// trade, which is the most common reason a paper record collapses on going live.

const { cfg } = require("../config");

const SKIP = {
  NO_BOOK: "no_book",
  NOT_CALIBRATED: "not_calibrated",
  NO_WEATHER_FORECAST: "no_weather_forecast",
  STALE_BOOK: "stale_book",
  IN_PLAY: "in_play",
  OUTSIDE_WINDOW: "outside_window",
  TOO_CLOSE_TO_KICKOFF: "too_close_to_kickoff",
  LOW_CONFIDENCE: "low_confidence",
  EDGE_BELOW_THRESHOLD: "edge_below_threshold",
  PRICE_OUT_OF_RANGE: "price_out_of_range",
  THIN_BOOK: "thin_book",
  BELOW_MIN_ORDER: "below_min_order",
  ALREADY_POSITIONED: "already_positioned",
  EXPOSURE_CAP: "exposure_cap",
};

const skip = (reason, extra) => ({ acted: false, skip_reason: reason, ...(extra || {}) });

function decideTotals(args) {
  const { forecast, game, market, books, nav, existingPosition, weekExposureUsd,
          now, windowOverrideHours } = args;
  const p = cfg().policy;
  const mc = cfg().model.confidence;

  if (forecast && forecast.skip) return skip(forecast.skip, { nonFinite: forecast.nonFinite });

  // Belt as well as braces. forecastTotal() already refuses to emit a non-finite
  // forecast, but this gate is the one that matters if that ever regresses: every
  // check below is a comparison, and a NaN makes all of them false, so a bad
  // forecast does not fail the gates -- it sails through them.
  if (!Number.isFinite(forecast.p_fair) || !Number.isFinite(forecast.delta)) {
    return skip("non_finite_forecast");
  }

  const kickoff = new Date(game.kickoff).getTime();
  const t = now.getTime();

  // -- gates independent of price -----------------------------------------
  // Entry is pre-game only. The resting EXIT rides through kickoff, which is a
  // different thing and lives in exec/limits.js: the bot will hold and try to
  // get paid on an in-play swing, but it will not open a new position blind.
  if (t >= kickoff) return skip(SKIP.IN_PLAY);
  const hoursOut = (kickoff - t) / 3600000;
  const windowHours = windowOverrideHours || p.openWindowHoursBeforeKickoff;
  if (hoursOut > windowHours) return skip(SKIP.OUTSIDE_WINDOW, { hoursOut });
  if ((kickoff - t) / 60000 < p.closeWindowMinutesBeforeKickoff) {
    return skip(SKIP.TOO_CLOSE_TO_KICKOFF);
  }
  if (forecast.confidence < mc.minToTrade) {
    return skip(SKIP.LOW_CONFIDENCE, { confidence: forecast.confidence });
  }
  if (existingPosition) return skip(SKIP.ALREADY_POSITIONED);

  // A weather-led model should not trade with the weather unknown. Inside a 72h
  // window an outdoor game's forecast SHOULD exist, so its absence means the
  // weather ingest is broken rather than that conditions are fine -- and the
  // heaviest-weighted factor would silently contribute zero while the other five
  // still cleared the confidence bar. Indoor games are unaffected: the factor
  // legitimately reports confidence 1 and no points for them.
  if (p.requireWeatherForecast) {
    const wx = forecast.inputs && forecast.inputs.detail && forecast.inputs.detail.weather;
    // Two distinct ways the weather input can be absent, and both must skip.
    // "unknown roof" used to be invisible here because the old weather code
    // returned a CONFIDENT zero for a null roof, so the game looked fully priced.
    if (wx && (wx.reason === "no forecast recorded" || wx.reason === "unknown roof")) {
      return skip(SKIP.NO_WEATHER_FORECAST, { roof: wx.roof, reason: wx.reason });
    }
  }

  // -- side and payable price ---------------------------------------------
  // delta > 0 means the model wants MORE points than the line implies.
  const side = forecast.delta > 0 ? "over" : "under";
  const book = books[side];
  if (!book || book.best_ask === null || book.best_ask === undefined) return skip(SKIP.NO_BOOK);

  const bookAgeSec = (t - new Date(book.ts).getTime()) / 1000;
  if (bookAgeSec > mc.staleBookSeconds) return skip(SKIP.STALE_BOOK, { bookAgeSec });

  const price = Number(book.best_ask);
  if (price < p.minPrice || price > p.maxPrice) return skip(SKIP.PRICE_OUT_OF_RANGE, { price });

  // p_fair is the probability of the OVER; the under's fair value is its complement.
  const fairForSide = side === "over" ? forecast.p_fair : 1 - forecast.p_fair;
  const edge = fairForSide - price;
  if (edge < p.minEdge) return skip(SKIP.EDGE_BELOW_THRESHOLD, { edge, price, side });

  const depth = Number(book.ask_depth_usd || 0);
  if (depth < p.minBookDepthUsd) return skip(SKIP.THIN_BOOK, { depth });

  // -- sizing --------------------------------------------------------------
  // Kelly for a binary at price c paying 1: f* = (p - c) / (1 - c).
  const kellyFull = edge / (1 - price);
  const kelly = Math.max(0, kellyFull * p.kellyFraction);

  const capPerMarket = nav * p.maxPositionPctNav;
  const weekRoom = Math.max(0, nav * p.maxWeeklyExposurePctNav - weekExposureUsd);
  const notional = Math.min(kelly * nav, capPerMarket, weekRoom);
  if (notional <= 0) return skip(SKIP.EXPOSURE_CAP, { weekRoom, capPerMarket });

  // Polymarket enforces a per-market minimum order (5 on these books, read from
  // Gamma rather than assumed). Refuse rather than round up: rounding would size
  // past what Kelly asked for, which is the one direction a sizing bug must not go.
  const minOrder = Number(market.min_order_usd) || 0;
  if (minOrder > 0 && notional < minOrder) {
    return skip(SKIP.BELOW_MIN_ORDER, { notional, minOrder });
  }

  return {
    acted: true,
    side,
    marketType: "totals",
    line: Number(market.line),
    conditionId: market.condition_id,
    price,
    edge,
    kellyFraction: kelly,
    conviction: Math.max(0, Math.min(1, kellyFull / 0.25)),
    notionalUsd: notional,
    shares: notional / price,
    hoursOut,
    // The model's disagreement with the book, in the unit a total is argued in.
    // Far more legible in a log than a delta in probability points.
    pointsVsMarket: +(forecast.model_mean_total - forecast.implied_mean_total).toFixed(2),
  };
}

module.exports = { decideTotals, SKIP };
