// trading-bot/src/exec/paper.js
//
// PAPER EXECUTION -- no money moves, but the fill is not fictional.
//
// The order walks the RECORDED ask ladder level by level, consuming size until
// it is filled or the book runs out, then reports the size-weighted average
// price and any unfilled remainder. Filling at the midpoint would hand the bot
// the whole spread for free on every trade, which is precisely the gap that
// makes paper records collapse the day they go live.
//
// On paper vs tiny real orders: Polymarket enforces a minimum order size of 5
// shares (verified on the live book), so cent-sized real orders are rejected
// outright. Sizing here is full-scale notional against a simulated NAV, so the
// arithmetic is identical when mode flips to live -- only the fill source
// changes.

const { cfg } = require("../config");
const { q } = require("../db");

/**
 * Walk a recorded ask ladder.
 * `asks` is [[price, size], ...] best-first, as ingest/polymarket.js stores it.
 */
function walkDepth(asks, sharesWanted, maxSlippageCents, intendedPrice) {
  const levels = Array.isArray(asks) ? asks : JSON.parse(asks || "[]");
  let remaining = sharesWanted;
  let cost = 0;
  let filled = 0;
  const consumed = [];

  for (const lvl of levels) {
    if (remaining <= 0) break;
    const price = Number(lvl[0]);
    const size = Number(lvl[1]);
    // Refuse to walk past the slippage limit: a deep but badly priced book
    // should produce a partial fill, not a terrible one. Measured in CENTS,
    // because the book ticks in cents -- see config _comment_slippage.
    // 1e-9 tolerance: 0.32 - 0.30 is 0.020000000000000018 in binary floating
    // point, which would exclude a level that is exactly at the limit.
    if (price - intendedPrice > maxSlippageCents + 1e-9) break;

    const take = Math.min(remaining, size);
    cost += take * price;
    filled += take;
    remaining -= take;
    consumed.push([price, +take.toFixed(4)]);
  }

  return {
    filledShares: filled,
    unfilledShares: Math.max(0, sharesWanted - filled),
    avgPrice: filled > 0 ? cost / filled : null,
    costUsd: cost,
    consumed,
  };
}

/** Record a paper trade against the book snapshot the decision was made on. */
async function executePaper(args) {
  const { botId, game, decision, forecast, book, featureId } = args;
  const c = cfg();
  const fill = walkDepth(book.asks, decision.shares, c.policy.maxSlippageCents, decision.price);
  if (!fill.filledShares) return { filled: false, reason: "no_fillable_depth" };

  const slippageBps = ((fill.avgPrice - decision.price) / decision.price) * 10000;

  const { rows } = await q(
    `insert into bots.trades
       (bot_id, game_id, condition_id, token_id, side, action, mode,
        p_market, p_fair, edge, conviction, kelly_fraction, feature_id,
        intended_price, fill_price, shares, notional_usd, slippage_bps,
        fill_detail, unfilled_shares)
     values ($1,$2,$3,$4,$5,'BUY',$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
     returning id`,
    [botId, game.game_id, book.condition_id, book.token_id, decision.side,
     c.mode, forecast.p_market, forecast.p_fair, decision.edge,
     decision.conviction, decision.kellyFraction, featureId,
     decision.price, fill.avgPrice, fill.filledShares, fill.costUsd,
     slippageBps, JSON.stringify({ consumed: fill.consumed, bookTs: book.ts }),
     fill.unfilledShares],
  );

  await q(
    `insert into bots.positions (bot_id, game_id, token_id, side, shares, avg_price, cost_usd)
     values ($1,$2,$3,$4,$5,$6,$7)
     on conflict (bot_id, token_id) do update set
       shares   = bots.positions.shares + excluded.shares,
       cost_usd = bots.positions.cost_usd + excluded.cost_usd,
       avg_price = (bots.positions.cost_usd + excluded.cost_usd)
                   / nullif(bots.positions.shares + excluded.shares, 0),
       updated_at = now()`,
    [botId, game.game_id, book.token_id, decision.side,
     fill.filledShares, fill.avgPrice, fill.costUsd],
  );

  return { filled: true, tradeId: rows[0].id, slippageBps, ...fill };
}

module.exports = { executePaper, walkDepth };
