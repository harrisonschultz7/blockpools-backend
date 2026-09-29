// trading-bot/src/exec/intents.js
//
// Emitting the copy-trading signal.
//
// One job: record what the bot decided, in a form a subscriber's wallet can act
// on, at the moment it decides it. Nothing reads this yet.
//
// EVERY EMIT IS BEST-EFFORT. A failure here must never abort a trade, cancel an
// exit or crash a tick -- the bot's own book is the source of truth and has to
// keep working whether or not anyone is copying it. So every function swallows
// its errors after logging them. The cost of a dropped intent is one subscriber
// missing one trade; the cost of a thrown one is the bot stopping.
const { q } = require("../db");
const log = require("../log");

/**
 * The bot bought. `navFraction` is the fraction of ITS portfolio the position
 * took, which is the only number a subscriber needs to size the same exposure
 * against a different balance.
 */
async function emitEnter({ botId, tradeId, game, decision, book, fill, nav }) {
  const navFraction = nav > 0 ? fill.costUsd / nav : null;
  try {
    await q(
      `insert into bots.trade_intents
         (bot_id, kind, game_id, condition_id, token_id, side, market_type, line,
          limit_price, nav_fraction, nav_usd, bot_trade_id)
       values ($1,'enter',$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
      [botId, game.game_id, book.condition_id, book.token_id, decision.side,
       decision.marketType || "moneyline",
       decision.line === undefined ? null : decision.line,
       fill.avgPrice, navFraction, nav, tradeId],
    );
  } catch (e) {
    log.warn(`intent enter failed (${game.game_id}): ${e.message}`);
  }
}

/**
 * The bot posted, or moved, a resting sell.
 *
 * `kind` separates the two because they are different instructions downstream: a
 * subscriber with no fill has nothing to place an exit against, and a reprice is
 * a cancel-and-replace rather than a new order.
 */
async function emitExit({ botId, kind, order, limitPrice }) {
  try {
    await q(
      `insert into bots.trade_intents
         (bot_id, kind, game_id, condition_id, token_id, side, market_type, line,
          limit_price, bot_trade_id)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
      [botId, kind, order.game_id, order.condition_id, order.token_id,
       order.side, order.market_type || "moneyline",
       order.line === undefined ? null : order.line,
       limitPrice, order.trade_id],
    );
  } catch (e) {
    log.warn(`intent ${kind} failed (${order.game_id}): ${e.message}`);
  }
}

module.exports = { emitEnter, emitExit };
