// trading-bot/src/exec/limits.js
//
// RESTING EXIT ORDERS.
//
// The bot buys because the price is below its fair value. The natural exit is
// the fair value itself: buy at 0.72 against a fair of 0.80, rest a sell at
// 0.80, and if the market comes to us we bank the markup.
//
// Why this is strictly better, not a trade-off:
//
//   hold to settlement : EV = 0.80 x $1 = $0.80 vs 0.72 cost -> +8c/share,
//                        but the outcome is 1.00 or 0.00. Huge variance.
//   sell at fair       : +8c/share locked, near-zero variance, and the
//                        capital is freed for the next game.
//
// Same expected value, far less variance. And if the market never reaches the
// limit, nothing happens and the position rides to settlement exactly as it
// would have. The limit is a free option with no downside branch.
//
// RE-PRICING is the part that is easy to get wrong. p_fair moves as injuries
// and weather update, so a limit parked at Tuesday's fair is stale by Sunday:
// if the opposing QB is ruled out and fair drifts to 0.86, a static 0.80 sells
// 6c too cheap. Every tick re-prices the resting order to current fair -- the
// same reason seed-bot.js re-prices its ladder instead of leaving stale rungs.
//
// PAPER FILLS: a sell fills when the recorded best BID reaches the limit --
// someone genuinely willing to buy from us. Walking the bid ladder rather than
// assuming the full size fills at the touch keeps the exit as honest as entry.

const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");

/** Exit target for a held side, from the current forecast. */
function exitPriceFor(forecast, side) {
  const p = cfg().policy;
  const fair = side === "home" ? forecast.p_fair : 1 - forecast.p_fair;
  // Sit a touch below fair so the order is marketable when the book arrives
  // there, rather than requiring the market to trade through us.
  const target = fair - (p.exitBufferCents || 0);
  return Math.max(0.02, Math.min(0.98, target));
}

/** Walk the recorded BID ladder -- the mirror of the entry's ask walk. */
function walkBids(bids, sharesWanted, limitPrice) {
  const levels = Array.isArray(bids) ? bids : JSON.parse(bids || "[]");
  let remaining = sharesWanted;
  let proceeds = 0;
  let filled = 0;
  const consumed = [];
  for (const lvl of levels) {
    if (remaining <= 0) break;
    const price = Number(lvl[0]);
    const size = Number(lvl[1]);
    // Only bids at or above our limit can fill a sell.
    if (price < limitPrice - 1e-9) break;
    const take = Math.min(remaining, size);
    proceeds += take * price;
    filled += take;
    remaining -= take;
    consumed.push([price, +take.toFixed(4)]);
  }
  return {
    filledShares: filled,
    unfilledShares: Math.max(0, sharesWanted - filled),
    avgPrice: filled > 0 ? proceeds / filled : null,
    proceedsUsd: proceeds,
    consumed,
  };
}

/** Post the resting exit for a freshly opened position. */
async function createExitOrder(args) {
  const { botId, tradeId, game, decision, forecast, fill } = args;
  const price = exitPriceFor(forecast, decision.side);
  // Nothing to capture if fair is already at or below what we paid.
  if (price <= fill.avgPrice) return null;

  const { rows } = await q(
    `insert into bots.limit_orders
       (bot_id, trade_id, game_id, token_id, side, action,
        limit_price, original_price, shares)
     values ($1,$2,$3,$4,$5,'SELL',$6,$6,$7) returning id`,
    [botId, tradeId, game.game_id, fill.tokenId, decision.side,
     price, fill.filledShares],
  );
  log(`    exit: rest SELL ${fill.filledShares.toFixed(0)} @ ${price.toFixed(3)} (fair)`);
  return rows[0].id;
}

/** Book the sale: close the order, credit the trade, reduce the position. */
async function settleExit(botId, order, hit, bookTs) {
  const partial = hit.unfilledShares > 0;

  await q(
    `update bots.limit_orders
        set status = $2, filled_at = now(), fill_price = $3,
            fill_detail = $4, closed_reason = $5, shares = $6, updated_at = now()
      where id = $1`,
    [order.id, partial ? "open" : "filled", hit.avgPrice,
     JSON.stringify({ consumed: hit.consumed, bookTs }),
     partial ? null : "filled_at_fair",
     partial ? hit.unfilledShares : order.shares],
  );

  // P&L on the sold shares only. `exited` is what stops settleTrades() ALSO
  // paying out a position that was already closed -- double-counting it would
  // be exactly the realised-only inflation this project exists to avoid.
  const { rows } = await q(
    `update bots.trades t
        set exit_price  = $2,
            exit_at     = now(),
            exit_shares = coalesce(t.exit_shares, 0) + $3,
            exit_reason = 'limit_fill_at_fair',
            exited      = (coalesce(t.exit_shares,0) + $3) >= t.shares - 1e-6,
            settled     = (coalesce(t.exit_shares,0) + $3) >= t.shares - 1e-6,
            closed_at   = now(),
            pnl_usd     = coalesce(t.pnl_usd,0) + ($3 * ($2 - t.fill_price))
      where t.id = $1
      returning t.game_id, t.pnl_usd`,
    [order.trade_id, hit.avgPrice, hit.filledShares],
  );

  await q(
    `update bots.positions
        set shares   = greatest(0, shares - $3),
            cost_usd = greatest(0, cost_usd - ($3 * avg_price)),
            updated_at = now()
      where bot_id = $1 and token_id = $2`,
    [botId, order.token_id, hit.filledShares],
  );
  await q(`delete from bots.positions where bot_id = $1 and shares <= 1e-6`, [botId]);

  const pnl = rows[0] ? Number(rows[0].pnl_usd) : 0;
  log(`    EXIT ${rows[0] ? rows[0].game_id : order.game_id} sold ` +
      `${hit.filledShares.toFixed(0)} @ ${hit.avgPrice.toFixed(3)} ` +
      `-> pnl $${pnl.toFixed(2)}${partial ? " (partial)" : ""}`);
}

/**
 * Re-price every open exit to current fair, then try to fill it against the
 * latest recorded book. `forecasts` is Map<game_id, forecast>.
 */
async function manageExits(botId, forecasts, now) {
  const { rows: orders } = await q(
    `select o.*, g.kickoff
       from bots.limit_orders o
       join sports.nfl_games g on g.game_id = o.game_id
      where o.bot_id = $1 and o.status = 'open'`,
    [botId],
  );
  let filled = 0;
  let repriced = 0;

  for (const o of orders) {
    // Kickoff cancels the exit: the bot does not trade in play, so from here
    // the position simply rides to settlement.
    if (new Date(o.kickoff).getTime() <= now.getTime()) {
      await q(`update bots.limit_orders set status='cancelled',
                 closed_reason='kickoff', updated_at=now() where id=$1`, [o.id]);
      continue;
    }

    let limit = Number(o.limit_price);
    const f = forecasts.get(o.game_id);
    if (f) {
      const target = exitPriceFor(f, o.side);
      // 1c deadband, so a stable fair causes no churn.
      if (Math.abs(target - limit) >= 0.01) {
        limit = target;
        await q(`update bots.limit_orders set limit_price=$2,
                   reprice_count=reprice_count+1, updated_at=now() where id=$1`,
                [o.id, limit]);
        repriced++;
      }
    }

    const { rows: books } = await q(
      `select bids, ts from sports.odds_history
        where token_id = $1 order by ts desc limit 1`, [o.token_id]);
    if (!books[0]) continue;

    const hit = walkBids(books[0].bids, Number(o.shares), limit);
    if (!hit.filledShares) continue;
    await settleExit(botId, o, hit, books[0].ts);
    filled++;
  }

  if (repriced || filled) log(`  exits: ${repriced} repriced, ${filled} filled`);
  return { filled, repriced };
}

module.exports = { manageExits, createExitOrder, walkBids, exitPriceFor, settleExit };
