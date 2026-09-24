// trading-bot/src/policy/medium.js
//
// MEDIUM-RISK POLICY. The forecast says what a game is worth; this decides
// whether to act, and how big.
//
// The bot is meant to SKIP most games. Selectivity is not a side effect of the
// edge threshold -- it is the product. Every look is written to bots.decisions
// with a skip_reason, so "traded 3 of 16 games this week" is provable rather
// than asserted, and a week where it suddenly trades everything is visible
// immediately.
//
// Edge is measured against the price actually PAYABLE (best ask), never the
// midpoint. Using the mid manufactures half the spread as free edge on every
// trade -- the most common reason a paper record collapses when it goes live.

const { cfg } = require("../config");

const SKIP = {
  NO_BOOK: "no_book",
  STALE_BOOK: "stale_book",
  IN_PLAY: "in_play",
  OUTSIDE_WINDOW: "outside_window",
  TOO_CLOSE_TO_KICKOFF: "too_close_to_kickoff",
  LOW_CONFIDENCE: "low_confidence",
  EDGE_BELOW_THRESHOLD: "edge_below_threshold",
  PRICE_OUT_OF_RANGE: "price_out_of_range",
  THIN_BOOK: "thin_book",
  ALREADY_POSITIONED: "already_positioned",
  EXPOSURE_CAP: "exposure_cap",
};

const skip = (reason, extra) => ({ acted: false, skip_reason: reason, ...(extra || {}) });

function decide(args) {
  const { forecast, game, books, nav, existingPosition, weekExposureUsd, now,
          windowOverrideHours } = args;
  const p = cfg().policy;
  const mc = cfg().model.confidence;

  const kickoff = new Date(game.kickoff).getTime();
  const t = now.getTime();

  // -- gates independent of price -----------------------------------------
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

  // -- side and payable price ---------------------------------------------
  // delta > 0 means the model likes the home side.
  const side = forecast.delta > 0 ? "home" : "away";
  const book = books[side];
  if (!book || book.best_ask === null || book.best_ask === undefined) return skip(SKIP.NO_BOOK);

  const bookAgeSec = (t - new Date(book.ts).getTime()) / 1000;
  if (bookAgeSec > mc.staleBookSeconds) return skip(SKIP.STALE_BOOK, { bookAgeSec });

  const price = Number(book.best_ask);
  if (price < p.minPrice || price > p.maxPrice) return skip(SKIP.PRICE_OUT_OF_RANGE, { price });

  // p_fair is a HOME probability; the away side's fair value is its complement.
  const fairForSide = side === "home" ? forecast.p_fair : 1 - forecast.p_fair;
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

  const favSide = forecast.p_market >= 0.5 ? "home" : "away";
  return {
    acted: true,
    side,
    price,
    edge,
    kellyFraction: kelly,
    conviction: Math.max(0, Math.min(1, kellyFull / 0.25)),
    notionalUsd: notional,
    shares: notional / price,
    hoursOut,
    favSide,
    betOnDog: favSide !== side,
  };
}

module.exports = { decide, SKIP };
