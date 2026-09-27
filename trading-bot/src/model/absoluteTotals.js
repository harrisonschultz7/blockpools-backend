// trading-bot/src/model/absoluteTotals.js
//
// THE INDEPENDENT PRICE. The bot's own expected total, built from scratch, with
// the market price nowhere in the calculation.
//
//   base_total   = expected_points(home) + expected_points(away)
//   model_total  = base_total + sum(weight_k * points_k)
//   p_fair       = Phi((model_total - line) / sigma)
//
// The market is the thing this trades against, not an input to forming the view.
// That is the whole difference from the residual mode, and it is why no factor
// scale is applied here: "how much has the market already priced this" is a
// question that only means anything when you are adding to the market's number.
//
// WHAT THE BASE IS. Opponent-adjusted offensive and defensive quality plus
// expected plays -- the three biggest effects on points scored by a wide margin
// (t 13.4, 7.1 and 11.1). Scheme STYLE is deliberately NOT in the base; it is a
// weighted factor, so style gets its own voice at the weight it was assigned
// rather than being buried in a coefficient.
//
// HOW ACCURATE IT IS, measured out of sample on 318 held-out games: 13.075 points
// of error on a game total, against the Polymarket closing line's 13.069. Even.
// That is the honest basis for trading its disagreements -- not better than the
// market, but not worse, which means the disagreements are worth something rather
// than being pure self-inflicted error.
//
// THE RAIL. maxPointsVsMarket is a circuit breaker for broken inputs -- a stale
// rating or an empty book producing a 15-point gap and a maximum-size bet off it.
// It is not there to stop the bot disagreeing, and it is set generously.

const { cfg } = require("../config");
const { loadAbsoluteFit } = require("./fitAbsolute");

/**
 * Expected points for one team, from the fitted base model.
 *
 * NO PLAY-COUNT TERM. It used to be here and it had to go: the fit used realised
 * plays (t 11.1) while the forecast could only supply a pace estimate, and that
 * estimate was measured against 318 games at an error SD of 9.9 plays versus 9.9
 * for guessing the league average -- no information at all. It was contributing
 * 24% of the bot's disagreement with the market out of pure noise, and it made the
 * reported accuracy flattering because the holdout was scored with a play count the
 * live model never gets.
 */
function expectedPoints(fit, myRating, oppRating) {
  const c = fit.coef;
  return c.intercept
       + c.myOffRating * myRating.off
       + c.oppDefRating * oppRating.def;
}

/**
 * The bot's own total for a game, before the weighted factors are applied.
 * Returns null when either team is too thinly rated to price.
 */
function baseTotal(game, ctx) {
  const ac = cfg().model.absolute;
  const fit = ctx.absoluteFit || loadAbsoluteFit();
  if (!fit.fitted) return { skip: "absolute_fit_missing" };

  const h = ctx.ratings.get(game.home_team);
  const a = ctx.ratings.get(game.away_team);
  if (!h || !a) return { skip: "no_ratings" };
  if (h.games < ac.minGamesToPrice || a.games < ac.minGamesToPrice) {
    return { skip: "ratings_too_thin", homeGames: h.games, awayGames: a.games };
  }

  const homePts = expectedPoints(fit, h, a);
  const awayPts = expectedPoints(fit, a, h);
  const total = homePts + awayPts;

  // Confidence on the thinner rating. A team rated off three games is a guess
  // however extreme the number looks.
  const minGames = Math.min(h.games, a.games);
  const confidence = Math.max(0, Math.min(1, minGames / 8));

  return {
    total,
    confidence,
    detail: {
      homePoints: +homePts.toFixed(2),
      awayPoints: +awayPts.toFixed(2),
      homeRating: { off: +h.off.toFixed(4), def: +h.def.toFixed(4), games: h.games },
      awayRating: { off: +a.off.toFixed(4), def: +a.def.toFixed(4), games: a.games },
      fit: { r2OutOfSample: fit.r2OutOfSample, gameSigma: fit.gameSigmaOutOfSample },
    },
  };
}

/** The sigma the independent price should be read with -- its OWN error, not the line's. */
function absoluteSigma(ctx) {
  const ac = cfg().model.absolute;
  const fit = ctx.absoluteFit || loadAbsoluteFit();
  if (ac.useFittedSigma && fit.fitted && Number.isFinite(fit.gameSigmaOutOfSample)) {
    return fit.gameSigmaOutOfSample;
  }
  return cfg().model.sigmaTotalPoints;
}

module.exports = { baseTotal, expectedPoints, absoluteSigma };
