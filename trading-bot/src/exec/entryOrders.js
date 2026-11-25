// trading-bot/src/exec/entryOrders.js
//
// MAKER ENTRIES (user, 2026-11-25; policy.makerEntry). When the model decides to
// buy, rest a BID at mid - offsetCents instead of taking the ask. The market comes
// to it or it doesn't:
//
//   filled   -- a seller traded BELOW the bid (a print under it, or an ask posted
//               under it). Strictly below, not at: orders already resting at that
//               price are ahead of ours in the queue. Booked at the bid, no fee.
//   fallback -- still open fallbackMinutesBeforeTip before tip: cancelled, and the
//               tick takes the ask exactly as it did before makerEntry existed.
//   cancelled-- the model's side flips, or its fair price falls so the bid no
//               longer clears minEdge, or the game starts.
//
// Replay on Polymarket minute prices (2025-26 on, 662 Axis picks, bid 0.5c under
// the mid, fallback to the ask): +1.13 points of return per trade (t 6.2); the
// bids that filled won 65% of the time against an average price of 60c, so fills
// are not concentrated on losers. Script: scratchpad maker_test.js.
//
// Paper only. Copiers still receive an 'enter' intent when the bot's bid fills and
// buy as before (FAK up to 3% over the bot's price); resting copier bids are a
// separate, real-money build.

const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");
const { recordFill } = require("./paper");
const { createExitOrder } = require("./limits");
const { emitBidEvent } = require("./intents");

const DATA_API = "https://data-api.polymarket.com";

/** The open bid for a game, or null. */
async function openEntryOrder(botId, gameId) {
  const { rows } = await q(
    `select * from bots.entry_orders where bot_id = $1 and game_id = $2 and status = 'open'`,
    [botId, gameId]);
  return rows[0] || null;
}

/** Dollars resting in open bids: counted against the open-exposure cap. */
async function openBidsUsd(botId) {
  const { rows } = await q(
    `select coalesce(sum(notional_usd), 0) v from bots.entry_orders where bot_id = $1 and status = 'open'`,
    [botId]);
  return Number(rows[0].v);
}

/**
 * Withdraw a bid. Copiers cancel theirs too ('bid_cancel', no top-up) -- except on
 * the fallback, where the bot's taker 'enter' (carrying this entry_order_id) tells
 * them to close and top up instead.
 */
async function cancelEntry(order, reason) {
  const { rowCount } = await q(
    `update bots.entry_orders set status = 'cancelled', closed_reason = $2, updated_at = now(),
            copy_closed_at = coalesce(copy_closed_at, now())
      where id = $1 and status = 'open'`,
    [order.id, reason]);
  if (rowCount && reason !== "fallback_taker") await emitBidEvent({ botId: order.bot_id, kind: "bid_cancel", order });
}

/**
 * FALLBACK TIME on bids the BOT filled as a maker: tell copiers to cancel what is
 * left of theirs and buy the rest at market, so they hold what the bot holds. Once
 * per bid; run every tick.
 */
async function closeFilledBidsForCopiers(botId, fallbackMinutes) {
  const { rows } = await q(
    `update bots.entry_orders o set copy_closed_at = now()
       from sports.nba_games g
      where g.game_id = o.game_id and o.bot_id = $1 and o.status = 'filled' and o.copy_closed_at is null
        and g.kickoff <= now() + ($2 || ' minutes')::interval
      returning o.*`,
    [botId, String(fallbackMinutes)]);
  for (const o of rows) await emitBidEvent({ botId, kind: "bid_close", order: o });
  return rows.length;
}

/** The bid price: mid less the offset, floored to the 0.001 grid, never under a cent. */
function bidPrice(book, offset) {
  let mid = Number(book.mid);
  if (!Number.isFinite(mid) && Number.isFinite(Number(book.best_bid)) && Number.isFinite(Number(book.best_ask))) {
    mid = (Number(book.best_bid) + Number(book.best_ask)) / 2;
  }
  if (!Number.isFinite(mid)) return null;
  return Math.max(0.01, Math.floor((mid - offset) * 1000 + 1e-6) / 1000);
}

/**
 * Post a bid for this decision, or keep the one already resting on the same side.
 * Returns { action: 'posted' | 'kept' | 'replaced' | 'no_mid', order }.
 */
async function postOrKeepBid({ botId, game, decision, forecast, book, featureIdFn, nav }) {
  const mk = cfg().policy.makerEntry || {};
  const existing = await openEntryOrder(botId, game.game_id);
  if (existing && existing.side === decision.side) return { action: "kept", order: existing };
  if (existing) await cancelEntry(existing, "side_flip");

  const limit = bidPrice(book, Number(mk.offsetCents ?? 0.005));
  if (limit === null || limit >= Number(decision.price)) return { action: "no_mid", order: null };
  const featureId = await featureIdFn();
  const shares = decision.notionalUsd / limit;
  const { rows } = await q(
    `insert into bots.entry_orders
       (bot_id, game_id, condition_id, token_id, side, limit_price, shares, notional_usd,
        p_market, p_fair, edge, feature_id, nav_usd, decision)
     values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
     on conflict do nothing
     returning *`,
    [botId, game.game_id, book.condition_id, book.token_id, decision.side, limit, shares,
     decision.notionalUsd, forecast.p_market, forecast.p_fair, decision.edge, featureId, nav,
     JSON.stringify({ edge: decision.edge, conviction: decision.conviction, kellyFraction: decision.kellyFraction,
                      favSide: decision.favSide, betOnDog: decision.betOnDog, askAtPost: decision.price })]);
  if (rows[0]) await emitBidEvent({ botId, kind: "bid", order: rows[0] });
  return { action: existing ? "replaced" : "posted", order: rows[0] || null };
}

/**
 * Keep or cancel a resting bid when this tick's model would NOT trade the game.
 * Kept while the bid still clears minEdge against the model's current fair price
 * (the policy measured edge against the ask; the bid is cheaper than the ask).
 */
async function reviewIdleBid(order, forecast, decision) {
  const p = cfg().policy;
  if (["in_play", "too_close_to_kickoff"].includes(decision.skip_reason)) return cancelEntry(order, "game_started");
  if (!forecast) return null;
  const fair = order.side === "home" ? forecast.p_fair : 1 - forecast.p_fair;
  if (fair - Number(order.limit_price) < p.minEdge) return cancelEntry(order, "edge_gone");
  return null;
}

/** Trades printed on this token BELOW the bid since it was posted (strict). */
async function printedBelow(order) {
  if (!order.condition_id) return false;
  const since = Math.floor(new Date(order.created_at).getTime() / 1000);
  const limit = Number(order.limit_price);
  let cursor = null;
  for (let n = 0; n < 6; n++) {
    const r = await fetch(
      `${DATA_API}/v2/trades?condition=${order.condition_id}&taker_only=false&limit=500`
        + (cursor ? `&cursor=${encodeURIComponent(cursor)}` : ""),
      { headers: { accept: "application/json" } });
    if (!r.ok) throw new Error(`trades ${r.status}`);
    const page = await r.json();
    for (const t of page?.data || []) {
      if (String(t.token_id) === String(order.token_id) && Number(t.timestamp) >= since
          && Number(t.price) < limit - 1e-9) return true;
    }
    cursor = page?.pagination?.next_cursor;
    if (!cursor) break;
  }
  return false;
}

/**
 * Fill every open bid the market has traded through. Run at the start of each
 * tick, before decisions, so a filled game reads as already positioned.
 */
async function sweepEntryFills(botId) {
  const { rows: orders } = await q(
    `select o.*, g.kickoff, bk.best_ask, bk.ts as book_ts
       from bots.entry_orders o
       join sports.nba_games g on g.game_id = o.game_id
       left join lateral (
         select h.best_ask, h.ts from sports.odds_history h
          where h.token_id = o.token_id and h.best_ask is not null
          order by h.ts desc limit 1) bk on true
      where o.bot_id = $1 and o.status = 'open'`,
    [botId]);
  let filled = 0;
  for (const o of orders) {
    if (new Date(o.kickoff).getTime() <= Date.now()) { await cancelEntry(o, "game_started"); continue; }
    const limit = Number(o.limit_price);
    let how = null;
    if (o.best_ask !== null && Number(o.best_ask) < limit - 1e-9) how = "ask_below_bid";
    else {
      try { if (await printedBelow(o)) how = "print_below_bid"; }
      catch (e) { log.warn(`  entry print check ${o.game_id}: ${e.message}`); }
    }
    if (!how) continue;
    // Claim the order first so two runs cannot book it twice.
    const { rowCount } = await q(
      `update bots.entry_orders set status = 'filled', filled_at = now(), updated_at = now(),
              fill_detail = $2 where id = $1 and status = 'open'`,
      [o.id, JSON.stringify({ how, bookTs: o.book_ts, bestAsk: o.best_ask })]);
    if (!rowCount) continue;
    const d = o.decision || {};
    const decision = { side: o.side, edge: Number(o.edge), conviction: d.conviction ?? null,
      kellyFraction: d.kellyFraction ?? null, price: limit, shares: Number(o.shares) };
    const forecast = { p_market: Number(o.p_market), p_fair: Number(o.p_fair) };
    const book = { condition_id: o.condition_id, token_id: o.token_id, ts: o.book_ts };
    const fill = { filledShares: Number(o.shares), unfilledShares: 0, avgPrice: limit,
      costUsd: Number(o.shares) * limit, consumed: [["maker_bid", Number(o.shares)]] };
    const rec = await recordFill({ botId, game: { game_id: o.game_id }, decision, forecast, book,
      featureId: o.feature_id, nav: Number(o.nav_usd), fill, detail: { maker: true, entryOrderId: o.id, how },
      // copiers' own bids are still resting: they act at bid_close, not now
      entryOrderId: o.id, intentKind: "enter_maker" });
    await q(`update bots.entry_orders set trade_id = $2 where id = $1`, [o.id, rec.tradeId]);
    if (cfg().policy.exitAtFairValue) {
      await createExitOrder({ botId, tradeId: rec.tradeId, game: { game_id: o.game_id, kickoff: o.kickoff },
        decision, forecast, fill: { ...fill, tokenId: o.token_id } }).catch((e) => log.warn(`  exit after maker fill: ${e.message}`));
    }
    filled++;
    log(`  MAKER FILL ${o.game_id} ${o.side} ${Number(o.shares).toFixed(0)} sh @ ${limit.toFixed(3)} (${how})`);
  }
  return filled;
}

module.exports = { openEntryOrder, openBidsUsd, cancelEntry, postOrKeepBid, reviewIdleBid, sweepEntryFills,
  closeFilledBidsForCopiers, bidPrice };
