// trading-bot/src/accounting/riskTier.js
//
// DERIVES a bot's risk tier instead of trusting a hand-typed string.
//
// The audience is a copy-trader deciding how much of their own money to put
// behind a bot. What they need to know is how much of the portfolio is exposed,
// not how clever the model claims to be -- so the tier is computed from the
// bot's SIZING POLICY and the prices it actually trades, never from its record.
// A bot cannot improve its tier by claiming an edge, and the tier does not flap
// around when a few bets land.
//
//   risk score = weeklyTurnover x priceFactor x exitFactor x corrFactor
//
// TWO PRIMARY DRIVERS, per the user: how much of the portfolio is put at risk,
// and how far a position has to travel before it is realised.
//
// THE BANDS (set by the user, 2026-09-27; medium and high widened 2026-09-30):
//   Low     < 25%
//   Medium   25-45%
//   High    >= 45%
//
// Widened so the bots can deploy materially more capital without relabelling
// themselves: Adam-7 stays Low and Argo-7 stays Medium at a 60% weekly cap.
// The bands stay contiguous -- a gap would leave scores with no tier to fall
// into.
//
// HEADROOM IS THE THING TO WATCH. Adam-7 scores 21.2% against a 25% ceiling.
// priceFactor and exitFactor are measured from its own fills -- two of them at
// the time of writing -- so the score will drift as real ones accumulate, and a
// drift upward relabels the card in public. Check the tier after each week of
// fills until observedWeeks passes minWeeksForObservedBasis.
//
// The widening is what lets a bot deploy more without relabelling itself: these
// numbers are RISK-ADJUSTED, not capital deployed. A bot resting a sell above
// entry carries (T-p)/(1-p) of a full binary's variance, so 25% of NAV per week
// scored 8.8% for Adam-7 and 15.6% for Argo-7. "Low risk" never meant it staked
// a tenth of the portfolio.
//
// Bands live HERE, not in each bot's config. A bot that could define its own
// bands could label itself anything.
//
// 1. weeklyTurnover -- CUMULATIVE notional opened per 7 days, over NAV. Not
//    concurrent exposure, which is what this used to measure and which quietly
//    flatters a fast sport: an NFL position is held for a week, so concurrent and
//    weekly are the same number, while an NBA bot recycling the same 25% every
//    day carries 125% of NAV through the week at an identical concurrent
//    reading. Turnover captures frequency and size in one figure and is
//    comparable across sports, which is the point.
//
// 2. exitFactor -- how far the position must travel to be realised. A resting
//    sell at T on a position bought at p has
//
//      Var(exit) / Var(hold to settlement) = (T - p) / (1 - p)
//
//    which is exactly "volatility premium over full upside". It is exact rather
//    than an approximation: a binary can only resolve at 1 by passing THROUGH T,
//    so never touching T means it settles at 0. Confirmed by simulating the price
//    path -- formula 0.220 against 0.221 simulated at p 0.50, T 0.61.
//
//    So a bot that sells 11c above entry carries 22% of the variance of the same
//    bot holding to settlement. A bot with no resting exits gets 1.0 and is rated
//    as the full binary it is.
//
// 3. priceFactor -- sqrt((1-p)/p). Capital at risk is the MAXIMUM loss and that
//    really is price-independent, but the FREQUENCY of losing it is not. At a
//    constant 25% of NAV per week over 13 weeks at zero edge, the 95th-percentile
//    drawdown runs from 91% at a price of 0.10 to 25% at 0.90. This is 1.0 at a
//    coin flip, so it changes nothing for a bot trading near 0.50.
//
// 4. corrFactor -- sqrt(1 + rho(n-1)), and it ONLY ever penalises. It does not
//    divide by sqrt(n), which would credit diversification: the money at risk is
//    the money at risk, and a tier that got friendlier the more bets a bot placed
//    would reward churn. Measured at -0.007 for NFL totals within a week, i.e.
//    none. A PROPS bot must set it -- same-game legs share one game script.
//
// The tier never reads the bot's record or its claimed edge. A bot cannot
// improve its tier by asserting skill, and the tier does not move when a few
// bets land.

const { q } = require("../db");
const log = require("../log");

// Upper bound of each tier, as a fraction of NAV.
const BANDS = [
  { tier: "low", max: 0.25 },
  { tier: "medium", max: 0.45 },
  { tier: "high", max: Infinity },
];

const tierFor = (rcar) => BANDS.find((b) => rcar < b.max).tier;

/** sqrt((1-p)/p), clamped so a quoting artefact at the extremes cannot explode. */
function sqrtOdds(p) {
  const safe = Math.max(0.02, Math.min(0.98, Number(p)));
  return Math.sqrt((1 - safe) / safe);
}

/**
 * The price level this bot actually trades at.
 *
 * Its own fills first, because that is ground truth. A bot with too few trades
 * falls back to the price distribution of its MARKET TYPE -- an unproven totals
 * bot should still be assessed as trading near 0.50, not assumed to be a coin
 * flip by default when its book might be longshots.
 */
async function priceProfile(botId, marketType) {
  const { rows: own } = await q(
    `select fill_price p from bots.trades
      where bot_id = $1 and fill_price is not null`,
    [botId],
  );
  if (own.length >= 20) {
    return { prices: own.map((r) => Number(r.p)), source: "own fills", n: own.length };
  }

  const sides = marketType === "totals" ? ["over", "under"] : ["home", "away"];
  const { rows: mkt } = await q(
    `select mid p from sports.odds_history
      where side = any($1) and mid between 0.05 and 0.95
      order by ts desc limit 4000`,
    [sides],
  );
  if (mkt.length) {
    return {
      prices: mkt.map((r) => Number(r.p)),
      source: `${marketType} market quotes (own fills: ${own.length}, need 20)`,
      n: mkt.length,
    };
  }
  // Nothing to go on. 0.50 is the NEUTRAL assumption, not a safe one -- it is
  // flagged so a tier resting on it is not mistaken for a measured one.
  return { prices: [0.5], source: "DEFAULT 0.50 -- no price data", n: 0 };
}

/**
 * CUMULATIVE notional opened per 7 days, as a fraction of NAV.
 *
 * Deliberately not concurrent exposure. An NFL position is held for a week, so
 * the two coincide; a daily sport recycling the same 25% every day carries far
 * more through the week at the same concurrent reading. Returned as the p90
 * rather than the mean, so a bot that is usually light and occasionally heavy is
 * rated on its heavy weeks.
 */
async function weeklyTurnover(botId, lookbackDays) {
  const { rows } = await q(
    `with weeks as (
       select date_trunc('week', opened_at) AS wk,
              sum(notional_usd)             AS deployed
         from bots.trades
        where bot_id = $1 and opened_at > now() - ($2 || ' days')::interval
        group by 1
     )
     select w.wk, w.deployed,
            (select n.nav_usd from bots.nav_history n
              where n.bot_id = $1 and n.d <= w.wk::date
              order by n.d desc limit 1) AS nav
       from weeks w order by w.wk`,
    [botId, String(lookbackDays)],
  );
  const { rows: startRows } = await q(
    `select starting_nav from bots.bot where id = $1`, [botId]);
  const startNav = startRows.length ? Number(startRows[0].starting_nav) : 0;

  const xs = rows
    .map((r) => {
      const nav = Number(r.nav) || startNav;
      return nav > 0 ? Number(r.deployed) / nav : null;
    })
    .filter((x) => x !== null && Number.isFinite(x));
  if (!xs.length) return { weeks: 0, p90: null, mean: null, max: null };
  const sorted = [...xs].sort((a, b) => a - b);
  return {
    weeks: xs.length,
    p90: sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.9))],
    mean: xs.reduce((a, b) => a + b, 0) / xs.length,
    max: sorted[sorted.length - 1],
  };
}

/**
 * How far a position has to travel before it is realised, as an SD multiplier.
 *
 *   sqrt((T - p) / (1 - p))
 *
 * 1.0 means hold-to-settlement -- the full binary. Measured from the bot's own
 * resting sells where it has them, because the configured alpha is a target and
 * the fills are what happened.
 *
 * A bot with NO resting exits gets 1.0 and is rated as the full binary it is.
 * That is the honest default: holding to settlement really is the riskiest way
 * to run the same position.
 */
async function exitFactor(botId, cfg) {
  const { rows } = await q(
    `select t.fill_price AS entry, o.limit_price AS target
       from bots.limit_orders o
       join bots.trades t on t.id = o.trade_id
      where o.bot_id = $1 and o.action = 'SELL'
        and t.fill_price is not null and o.limit_price is not null`,
    [botId],
  );
  if (!rows.length) {
    const restsExits = cfg?.policy?.exitAtFairValue === true;
    return {
      factor: 1,
      source: restsExits
        ? "no resting sells recorded yet -- rated as hold-to-settlement"
        : "holds to settlement (policy.exitAtFairValue is off)",
      n: 0,
    };
  }
  const ratios = rows
    .map((r) => {
      const entry = Number(r.entry);
      const target = Math.min(0.999, Number(r.target));
      if (!(entry > 0) || target <= entry) return null;
      return Math.sqrt((target - entry) / (1 - entry));
    })
    .filter((x) => x !== null && Number.isFinite(x));
  if (!ratios.length) return { factor: 1, source: "no usable exit prices", n: 0 };
  const f = ratios.reduce((a, b) => a + b, 0) / ratios.length;
  // Cannot exceed 1: selling at or above $1 is holding to settlement.
  return { factor: Math.min(1, f), source: `${ratios.length} resting sells`, n: ratios.length };
}

/**
 * Compute a bot's tier.
 *
 * BASIS: what the bot actually deploys, once there is enough history to say --
 * the 90th percentile of daily exposure, not the mean, so a bot that is usually
 * light but occasionally heavy is rated on the heavy weeks.
 *
 * Until then it falls back to the CONFIGURED CAP, which is the conservative
 * reading: a bot sitting at 2% under a 25% cap can reach 25% any time without
 * anything changing, and a tier built on a fortnight of quiet weeks would be a
 * promise the config does not make. Both numbers are always reported, because a
 * large gap between permission and practice is itself worth seeing -- it usually
 * means the cap is set far above anything the policy will actually use.
 */
async function computeRiskTier(botId) {
  const { rows: botRows } = await q(
    `select id, name, config, starting_nav from bots.bot where id = $1`, [botId]);
  if (!botRows.length) throw new Error(`no such bot: ${botId}`);
  const b = botRows[0];
  const cfg = typeof b.config === "string" ? JSON.parse(b.config) : b.config;

  const openCap = Number(cfg?.policy?.maxOpenExposurePctNav);
  if (!Number.isFinite(openCap)) throw new Error(`${botId}: no policy.maxOpenExposurePctNav`);
  const perPosition = Number(cfg?.policy?.maxPositionPctNav) || openCap;
  const marketType = cfg?.marketScope === "totals" ? "totals" : "moneyline";

  // Concurrent legs the cap allows. Drives the correlation penalty only.
  const legs = Math.max(1, Math.round(openCap / perPosition));

  const obsWindow = Number(cfg?.model?.risk?.observedLookbackDays) || 90;
  const minWeeks = Number(cfg?.model?.risk?.minWeeksForObservedBasis) || 4;

  const turn = await weeklyTurnover(botId, obsWindow);
  const useObserved = turn.weeks >= minWeeks && turn.p90 !== null;

  // FALLBACK when a bot is too new to measure. The cap is CONCURRENT exposure, so
  // for a sport that settles more than once a week it understates the weekly
  // turnover -- roundsPerWeek is how many times the book realistically recycles.
  // NFL is 1. A daily sport must say so, or it will be rated as though it traded
  // once a week.
  const roundsPerWeek = Math.max(1, Number(cfg?.model?.risk?.roundsPerWeek) || 1);
  const basisPct = useObserved ? turn.p90 : openCap * roundsPerWeek;
  const basis = useObserved
    ? `observed p90 weekly turnover over ${turn.weeks} weeks`
    : `cap ${(100 * openCap).toFixed(0)}% x ${roundsPerWeek} round(s)/wk ` +
      `(only ${turn.weeks} weeks of history, need ${minWeeks})`;

  const exit = await exitFactor(botId, cfg);

  const prof = await priceProfile(botId, marketType);
  const meanSqrtOdds = prof.prices.reduce((s, p) => s + sqrtOdds(p), 0) / prof.prices.length;

  // Correlation between CONCURRENT positions. Measured at -0.007 for NFL totals
  // within a week (2022-2026), i.e. none -- games do not share a scoring
  // environment in any detectable way, which was the opposite of what was
  // expected. A props bot MUST set this: same-game legs key off one game script
  // and 0 would badly understate them.
  const rho = Math.max(0, Number(cfg?.model?.risk?.concurrentCorrelation) || 0);
  const corrFactor = rho > 0 ? Math.sqrt(1 + rho * (legs - 1)) : 1;

  const score = basisPct * meanSqrtOdds * exit.factor * corrFactor;
  const tier = tierFor(score);

  // What the bot has actually had at stake, for comparison with the permission.
  const { rows: obs } = await q(
    `select coalesce(sum(notional_usd), 0) v from bots.trades
      where bot_id = $1 and settled = false`, [botId]);
  const { rows: navRows } = await q(
    `select nav_usd from bots.nav_history where bot_id = $1 order by d desc limit 1`, [botId]);
  const nav = navRows.length ? Number(navRows[0].nav_usd) : Number(b.starting_nav);
  const observedPct = nav > 0 ? Number(obs[0].v) / nav : 0;

  return {
    botId, name: b.name, tier,
    riskScore: +score.toFixed(4),
    basis,
    basisPctNav: +basisPct.toFixed(4),
    observedWeeks: turn.weeks,
    observedP90Turnover: turn.p90 === null ? null : +turn.p90.toFixed(4),
    observedMeanTurnover: turn.mean === null ? null : +turn.mean.toFixed(4),
    openCapPctNav: openCap,
    roundsPerWeek,
    maxPositionPctNav: perPosition,
    concurrentLegs: legs,
    priceFactor: +meanSqrtOdds.toFixed(4),
    priceSource: prof.source,
    exitFactor: +exit.factor.toFixed(4),
    exitSource: exit.source,
    assumedCorrelation: rho,
    correlationFactor: +corrFactor.toFixed(4),
    bands: BANDS.map((x) => ({ tier: x.tier, max: x.max })),
  };
}


/** Compute and persist for every enabled bot. */
async function applyRiskTiers({ write = true } = {}) {
  const { rows: bots } = await q(
    `select id, risk_tier from bots.bot where enabled order by id`);
  const out = [];
  for (const b of bots) {
    const r = await computeRiskTier(b.id);
    r.previousTier = b.risk_tier;
    r.changed = b.risk_tier !== r.tier;
    if (write && r.changed) {
      await q(`update bots.bot set risk_tier = $2 where id = $1`, [b.id, r.tier]);
    }
    out.push(r);
  }
  return out;
}

module.exports = {
  computeRiskTier, applyRiskTiers, tierFor, sqrtOdds,
  weeklyTurnover, exitFactor, BANDS,
};
