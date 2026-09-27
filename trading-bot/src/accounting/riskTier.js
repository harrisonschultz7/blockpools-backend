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
//   risk-adjusted capital at risk = weeklyCap x meanSqrtOdds x corrFactor
//
// THE BANDS (set by the user, 2026-09-27):
//   Low     < 10%
//   Medium   10-20%
//   High    >= 20%
//
// Bands live HERE, not in each bot's config. A bot that could define its own
// bands could label itself anything.
//
// WHY THE PRICE TERM. Capital at risk is the MAXIMUM loss, and that part really
// is price-independent -- you can only lose the stake. But the FREQUENCY of
// losing it is not. Simulated at a constant 25% of NAV per week over 13 weeks,
// zero edge, the 95th-percentile drawdown runs from 91% at a price of 0.10 to
// 25% at 0.90: a 3.6x spread at identical "capital at risk". sqrt((1-p)/p)
// normalises that, and it is 1.0 at a coin flip, so it changes nothing for a bot
// trading near 0.50 and correctly penalises a longshot book.
//
// Checked against simulation rather than assumed:
//   p 0.25 at 25%/wk  ~  p 0.50 at 40%/wk   (formula 1.73x, observed 1.6x)
//   p 0.75 at 25%/wk  ~  p 0.50 at 15%/wk   (formula 0.58x, observed 0.60x)
//
// WHY THE CORRELATION TERM, and why it only ever penalises. Concurrent positions
// that share an outcome driver are one bet wearing several hats -- five props on
// one game key off a single game script. sqrt(1 + rho(n-1)) predicted 1.84x the
// volatility at rho 0.6 with 5 legs; simulation gave 1.86x.
//
// It does NOT divide by sqrt(n), which would credit a bot for diversification.
// That is deliberate and it is the user's call: spreading the same 25% over more
// games does reduce variance, but the 25% is still the money at risk, and a tier
// that got friendlier the more bets a bot placed would reward churn.

const { q } = require("../db");
const log = require("../log");

// Upper bound of each tier, as a fraction of NAV.
const BANDS = [
  { tier: "low", max: 0.10 },
  { tier: "medium", max: 0.20 },
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

/** Daily exposure the bot has actually carried, from bots.nav_history. */
async function observedExposure(botId, lookbackDays) {
  const { rows } = await q(
    `select (position_value_usd / nullif(nav_usd, 0))::float8 AS e
       from bots.nav_history
      where bot_id = $1 and d > current_date - $2::int
        and nav_usd > 0
      order by d`,
    [botId, lookbackDays],
  );
  const xs = rows.map((r) => Number(r.e)).filter(Number.isFinite);
  if (!xs.length) return { days: 0, p90: null, max: null, mean: null };
  const sorted = [...xs].sort((a, b) => a - b);
  return {
    days: xs.length,
    p90: sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * 0.9))],
    max: sorted[sorted.length - 1],
    mean: xs.reduce((a, b) => a + b, 0) / xs.length,
  };
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

  const weeklyCap = Number(cfg?.policy?.maxWeeklyExposurePctNav);
  if (!Number.isFinite(weeklyCap)) throw new Error(`${botId}: no policy.maxWeeklyExposurePctNav`);
  const perPosition = Number(cfg?.policy?.maxPositionPctNav) || weeklyCap;
  const marketType = cfg?.marketScope === "totals" ? "totals" : "moneyline";

  // Concurrent legs the cap allows. Drives the correlation penalty only.
  const legs = Math.max(1, Math.round(weeklyCap / perPosition));

  const obsWindow = Number(cfg?.model?.risk?.observedLookbackDays) || 90;
  const minDays = Number(cfg?.model?.risk?.minDaysForObservedBasis) || 28;
  const observed = await observedExposure(botId, obsWindow);
  const useObserved = observed.days >= minDays && observed.p90 !== null;
  const basisPct = useObserved ? observed.p90 : weeklyCap;
  const basis = useObserved
    ? `observed p90 over ${observed.days} days`
    : `configured cap (only ${observed.days} days of history, need ${minDays})`;

  const prof = await priceProfile(botId, marketType);
  const meanSqrtOdds = prof.prices.reduce((s, p) => s + sqrtOdds(p), 0) / prof.prices.length;

  // Correlation between CONCURRENT positions. Measured at -0.007 for NFL totals
  // within a week (2022-2026), i.e. none -- games do not share a scoring
  // environment in any detectable way, which was the opposite of what was
  // expected. A props bot MUST set this: same-game legs key off one game script
  // and 0 would badly understate them.
  const rho = Math.max(0, Number(cfg?.model?.risk?.concurrentCorrelation) || 0);
  const corrFactor = rho > 0 ? Math.sqrt(1 + rho * (legs - 1)) : 1;

  const rcar = basisPct * meanSqrtOdds * corrFactor;
  const tier = tierFor(rcar);

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
    riskAdjustedCapitalAtRisk: +rcar.toFixed(4),
    basis,
    basisPctNav: +basisPct.toFixed(4),
    observedDays: observed.days,
    observedP90PctNav: observed.p90 === null ? null : +observed.p90.toFixed(4),
    observedMeanPctNav: observed.mean === null ? null : +observed.mean.toFixed(4),
    weeklyCapPctNav: weeklyCap,
    maxPositionPctNav: perPosition,
    concurrentLegs: legs,
    meanSqrtOdds: +meanSqrtOdds.toFixed(4),
    priceSource: prof.source,
    priceSampleN: prof.n,
    assumedCorrelation: rho,
    correlationFactor: +corrFactor.toFixed(4),
    observedOpenPctNav: +observedPct.toFixed(4),
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

module.exports = { computeRiskTier, applyRiskTiers, tierFor, sqrtOdds, BANDS };
