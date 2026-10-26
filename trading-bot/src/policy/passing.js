// trading-bot/src/policy/passing.js
//
// PASSING-YARDS PROP POLICY (Bruno-7). The forecast says what a prop line is
// worth; this decides whether to act, on which side, and how big.
//
// Same shape as policy/totals.js -- side from the sign of delta, edge against the
// payable ASK, flat sizing -- with four differences, all from the 2026-10-26
// week-7 health check:
//
//  1. STARTER GATE. A prop on a QB who does not take the field is a bet on
//     nothing (week 6 forecast Jalen Hurts while he was listed Out). The caller
//     passes starterCheck()'s verdict; anything but null skips.
//
//  2. FEE-NET EDGE. Props carry a 3% TAKER fee (sports_fees_nfl_cfb_oct26), paid
//     on entry; the resting exit is a maker order and pays none. The edge gate
//     subtracts fee_rate * min(p, 1-p) per share -- the shape of Polymarket's
//     sports fee -- so a 4c edge means 4c after paying to get in.
//
//  3. SMALL SIZE. Central prop books held ~$29 bid / ~$221 ask in the last hours
//     of DAL@PHI. A 7%-of-NAV order would be mostly unfilled paper; 2% is about
//     what one side of the book actually holds.
//
//  4. NO WEATHER GATE. Wind is one input among five here, not the top weight, and
//     the passing model already zeroes it when no wind is known.
//
// Edge is measured against the price actually PAYABLE (best ask), never the
// midpoint, for the same reason as totals: the mid hands the bot half the spread,
// and prop spreads are 5-10c wide.

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
  BELOW_MIN_ORDER: "below_min_order",
  ALREADY_POSITIONED: "already_positioned",
  EXPOSURE_CAP: "exposure_cap",
};

const skip = (reason, extra) => ({ acted: false, skip_reason: reason, ...(extra || {}) });

/** Taker fee per share at `price`, as a fraction of $1. */
function takerFeePerShare(market, price) {
  const rate = Number(market.fee_rate);
  if (!(rate > 0)) return 0;
  return rate * Math.min(price, 1 - price);
}

function decidePassing(args) {
  const { forecast, game, market, books, nav, existingPosition, openExposureUsd,
          now, windowOverrideHours, starterVerdict } = args;
  const p = cfg().policy;

  if (forecast && forecast.skip) return skip(forecast.skip, { nonFinite: forecast.nonFinite });
  // NaN passes every comparison below, so refuse it explicitly.
  if (!Number.isFinite(forecast.p_fair) || !Number.isFinite(forecast.delta)) {
    return skip("non_finite_forecast");
  }

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
  if (p.requireConfirmedStarter && starterVerdict) return skip(starterVerdict);
  if (forecast.confidence < (p.minConfidence ?? 0)) {
    return skip(SKIP.LOW_CONFIDENCE, { confidence: forecast.confidence });
  }
  if (existingPosition) return skip(SKIP.ALREADY_POSITIONED);

  // -- side and payable price ---------------------------------------------
  // p_fair is P(over); delta > 0 means the model expects MORE yards than the line.
  const side = forecast.delta > 0 ? "over" : "under";
  const book = books[side];
  if (!book || book.best_ask === null || book.best_ask === undefined) return skip(SKIP.NO_BOOK);

  const bookAgeSec = (t - new Date(book.ts).getTime()) / 1000;
  if (bookAgeSec > (p.staleBookSeconds || 900)) return skip(SKIP.STALE_BOOK, { bookAgeSec });

  const price = Number(book.best_ask);
  if (!(price >= p.minPrice && price <= p.maxPrice)) return skip(SKIP.PRICE_OUT_OF_RANGE, { price });

  const fairForSide = side === "over" ? forecast.p_fair : 1 - forecast.p_fair;
  const fee = p.deductTakerFee ? takerFeePerShare(market, price) : 0;
  const edge = fairForSide - price - fee;
  if (edge < p.minEdge) return skip(SKIP.EDGE_BELOW_THRESHOLD, { edge, price, side, fee });

  const depth = Number(book.ask_depth_usd || 0);
  if (depth < p.minBookDepthUsd) return skip(SKIP.THIN_BOOK, { depth, edge });

  // -- sizing (flat; see the totals policy for why not Kelly) --------------
  const kellyFull = edge / (1 - price);
  const kelly = Math.max(0, kellyFull * p.kellyFraction);
  const sized = p.sizingMode === "flat" ? nav * p.flatPositionPctNav : kelly * nav;
  const capPerMarket = nav * p.maxPositionPctNav;
  const openRoom = Math.max(0, nav * p.maxOpenExposurePctNav - openExposureUsd);
  const notional = Math.min(sized, capPerMarket, openRoom);
  if (notional <= 0) return skip(SKIP.EXPOSURE_CAP, { openRoom, capPerMarket, edge });

  const minOrder = Number(market.min_order_usd) || 0;
  if (minOrder > 0 && notional < minOrder) {
    return skip(SKIP.BELOW_MIN_ORDER, { notional, minOrder, edge });
  }

  return {
    acted: true,
    side,
    marketType: "passing",
    line: Number(market.line),
    conditionId: market.condition_id,
    price,
    edge,
    fee,
    kellyFraction: kelly,
    conviction: Math.max(0, Math.min(1, kellyFull / 0.25)),
    notionalUsd: notional,
    shares: notional / price,
    hoursOut,
    // The disagreement in the unit a prop is argued in.
    yardsVsMarket: +(forecast.model_mean_yards - forecast.implied_mean_yards).toFixed(1),
  };
}

module.exports = { decidePassing, takerFeePerShare, SKIP };
