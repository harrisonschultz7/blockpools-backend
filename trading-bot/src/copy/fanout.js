// trading-bot/src/copy/fanout.js
//
// THE FAN-OUT. Reads what the bot decided and places the same trade for every
// subscriber, sized against their sleeve.
//
// This is the first code in the project that spends someone else's money, so it
// is built to fail closed at every step:
//
//   - one copy.orders row per (subscription, intent), enforced by a unique
//     index, so a restart mid-fan-out cannot place a second order
//   - a skip is RECORDED, not silent: "no funds" and "the worker never ran" are
//     indistinguishable otherwise
//   - delegation is not cached or trusted; Privy is asked at signing time and a
//     revoked wallet fails there
//   - an error on one subscriber never touches another
//
// Sizing: sleeve NAV x the bot's nav_fraction x the subscriber's multiplier,
// then clamped by their per-trade cap and by what the wallet actually holds.
const { q, pool } = require("../db");
const log = require("../log");
const { getPmClient } = require("./pmClient");

/** Nothing below this is worth a fill; Polymarket's own floor is $5. */
const MIN_ORDER_USD = 5;

/**
 * The sleeve's current value: what they allocated, plus what this sleeve has
 * banked. Unrealised is deliberately excluded -- marking open copied positions
 * every tick would make the next trade's size depend on an unrealised swing,
 * which is how a losing week quietly shrinks every subsequent position.
 */
function sleeveNav(sub) {
  return Number(sub.basis_usd) + Number(sub.realized_pnl);
}

/**
 * The price grid a market accepts. Polymarket rejects anything off it outright
 * ("maxPrice has unsupported precision"), so every price we send has to be
 * snapped to it first.
 *
 * Cached per market: it is a property of the market, not of the moment, and the
 * fan-out would otherwise re-read it for every subscriber on every intent.
 *
 * Falls back to 0.01, the coarser of the two grids in use. A 0.01 multiple is
 * also a 0.001 multiple, so the fallback is valid on either market rather than
 * merely likely to be.
 */
const tickCache = new Map();
async function tickSize(conditionId) {
  if (!conditionId) return 0.01;
  if (tickCache.has(conditionId)) return tickCache.get(conditionId);
  let tick = 0.01;
  try {
    const { rows } = await pool.query(
      `select tick_size from sports.pm_totals_markets where condition_id = $1`,
      [conditionId],
    );
    const t = Number(rows[0]?.tick_size);
    if (Number.isFinite(t) && t > 0) tick = t;
  } catch (e) {
    log.warn(`copy: tick lookup failed for ${conditionId}: ${e.message}`);
  }
  tickCache.set(conditionId, tick);
  return tick;
}

/**
 * Snap a price DOWN to the grid. Down, because the only price we send is a
 * ceiling on what we will pay, and rounding a ceiling up spends money the
 * caller did not authorise.
 *
 * toFixed before parsing kills the float dust that makes 0.53 arrive as
 * 0.5300000000000001 -- which the exchange rejects for the same reason.
 */
function floorToTick(price, tick) {
  const decimals = Math.max(0, Math.round(-Math.log10(tick)));
  return Number((Math.floor(price / tick) * tick).toFixed(decimals));
}

/** Active, delegated subscriptions for a bot. */
async function subscribersOf(botId) {
  const { rows } = await q(
    `select id, privy_did, wallet_address, basis_usd, multiplier, max_trade_usd,
            realized_pnl
       from copy.subscriptions
      where bot_id = $1 and status = 'active' and delegated_at is not null`,
    [botId],
  );
  return rows;
}

/** Claim this (subscription, intent) or discover someone already has. */
async function claim(subId, intent) {
  try {
    const { rows } = await q(
      `insert into copy.orders
         (subscription_id, intent_id, kind, token_id, side, status)
       values ($1,$2,$3,$4,$5,'skipped')
       on conflict (subscription_id, intent_id) do nothing
       returning id`,
      [subId, intent.id, intent.kind, intent.token_id, intent.side],
    );
    return rows[0] ? rows[0].id : null;
  } catch (e) {
    log.warn(`copy: claim failed sub ${subId} intent ${intent.id}: ${e.message}`);
    return null;
  }
}

async function finish(orderId, patch) {
  const sets = [];
  const vals = [orderId];
  for (const [k, v] of Object.entries(patch)) {
    vals.push(v);
    sets.push(`${k} = $${vals.length}`);
  }
  await q(
    `update copy.orders set ${sets.join(", ")}, updated_at = now() where id = $1`,
    vals,
  );
}

/**
 * The bot bought; buy the same market for this subscriber.
 *
 * A MARKET order, not a limit at the bot's fill. The bot's price is already
 * seconds old by the time this runs and a limit at it would simply not fill on
 * a book that moved -- which reads to a subscriber as the copy silently not
 * working. maxPrice caps how far past the bot's fill we will chase.
 */
async function copyEnter(sub, intent, orderId) {
  const nav = sleeveNav(sub);
  const frac = Number(intent.nav_fraction);
  if (!Number.isFinite(frac) || frac <= 0) {
    await finish(orderId, { status: "skipped", skip_reason: "no_nav_fraction" });
    return false;
  }

  let usd = nav * frac * Number(sub.multiplier);
  if (sub.max_trade_usd !== null) usd = Math.min(usd, Number(sub.max_trade_usd));

  if (usd < MIN_ORDER_USD) {
    await finish(orderId, {
      status: "skipped", skip_reason: "below_min_order", requested_usd: usd,
    });
    return false;
  }

  const client = await getPmClient(sub.wallet_address);

  // The wallet is the real constraint, not the sleeve. Someone can allocate
  // $500 and later withdraw to $50; the sleeve does not know, and only the
  // balance at THIS moment can say what is spendable.
  const balance = await walletUsd(client);
  if (balance !== null && usd > balance) {
    if (balance < MIN_ORDER_USD) {
      await finish(orderId, {
        status: "skipped", skip_reason: "insufficient_funds", requested_usd: usd,
      });
      return false;
    }
    usd = balance;
  }

  const { OrderSide, OrderType } = await import("@polymarket/client");
  const tick = await tickSize(intent.condition_id);
  const limit = Number(intent.limit_price);
  // Chase up to 3% past the bot's price, snapped to the grid. Flooring can land
  // below the bot's own price when 3% is thinner than one tick (cheap
  // outcomes), so the bot's price, rounded up to the grid, is the floor: never
  // pay less than it was willing to, never send a price the exchange refuses.
  const chased = floorToTick(Math.min(0.99, limit * 1.03), tick);
  const atLeastBot = floorToTick(Math.min(0.99, limit + tick), tick);
  const maxPrice = Math.max(chased, atLeastBot);

  let res;
  try {
    res = await client.placeMarketOrder({
      assetId: intent.token_id,
      side: OrderSide.BUY,
      amount: usd,
      maxSpend: usd,
      maxPrice,
      orderType: OrderType.FAK,
      builderCode: process.env.POLYMARKET_BUILDER_CODE,
    });
  } catch (e) {
    // Record what we TRIED before rethrowing. The outer handler only knows the
    // message, and a rejected row with no size or price is near useless when
    // the thing that failed is the size or the price.
    await finish(orderId, {
      status: "rejected",
      requested_usd: usd,
      limit_price: maxPrice,
      error: String(e.message || e).slice(0, 400),
    }).catch(() => {});
    throw e;
  }

  const shares = Number(res?.makingAmount || 0);
  await finish(orderId, {
    status: shares > 0 ? "filled" : "rejected",
    requested_usd: usd,
    limit_price: maxPrice,
    clob_order_id: res?.orderId || null,
    filled_shares: shares || null,
    avg_price: shares > 0 ? usd / shares : null,
    error: shares > 0 ? null : (res?.message || "no fill"),
  });

  if (shares > 0) {
    await q(
      `insert into copy.positions
         (subscription_id, token_id, bot_trade_id, shares, cost_usd, avg_price)
       values ($1,$2,$3,$4,$5,$6)
       on conflict (subscription_id, token_id) do update set
         shares    = copy.positions.shares + excluded.shares,
         cost_usd  = copy.positions.cost_usd + excluded.cost_usd,
         avg_price = (copy.positions.cost_usd + excluded.cost_usd)
                     / nullif(copy.positions.shares + excluded.shares, 0),
         updated_at = now()`,
      [sub.id, intent.token_id, intent.bot_trade_id, shares, usd, usd / shares],
    );
  }
  return shares > 0;
}

/** USDC the Deposit Wallet can actually spend, or null if it cannot be read. */
async function walletUsd(client) {
  try {
    const v = await client.fetchPortfolioValue?.();
    const n = Number(v?.cash ?? v?.balance ?? v);
    return Number.isFinite(n) ? n : null;
  } catch {
    // Unreadable balance must not block the trade -- the exchange rejects an
    // unfunded order anyway, and that rejection is recorded.
    return null;
  }
}

/**
 * The bot posted or moved a resting sell; mirror it on whatever this subscriber
 * actually holds.
 *
 * Sized from THEIR position, never from the bot's. A subscriber who got a
 * partial fill, or none, must not have a sell resting for shares they do not
 * own -- that is an order that can never fill and looks like a stuck exit.
 *
 * A reprice cancels the previous order first. Cancel-then-place leaves a gap
 * with no exit, which is the safer direction: the alternative is two live sells
 * for one position, and a double fill sells shares the subscriber does not have.
 */
async function copyExit(sub, intent, orderId) {
  const { rows } = await q(
    `select shares, detached from copy.positions
      where subscription_id = $1 and token_id = $2`,
    [sub.id, intent.token_id],
  );
  const pos = rows[0];
  if (!pos || Number(pos.shares) <= 0) {
    await finish(orderId, { status: "skipped", skip_reason: "no_position" });
    return false;
  }
  if (pos.detached) {
    await finish(orderId, { status: "skipped", skip_reason: "detached" });
    return false;
  }

  const client = await getPmClient(sub.wallet_address);
  const price = Number(intent.limit_price);
  const shares = Number(pos.shares);

  if (intent.kind === "reprice") {
    const prev = await q(
      `select clob_order_id from copy.orders
        where subscription_id = $1 and token_id = $2
          and kind in ('exit','reprice') and status = 'placed'
          and clob_order_id is not null
        order by id desc limit 1`,
      [sub.id, intent.token_id],
    );
    const old = prev.rows[0]?.clob_order_id;
    if (old) {
      try { await client.cancelOrder({ orderId: old }); }
      catch (e) { log.warn(`copy: cancel ${old} failed: ${e.message}`); }
      await q(
        `update copy.orders set status = 'skipped', skip_reason = 'repriced',
                updated_at = now()
          where subscription_id = $1 and clob_order_id = $2`,
        [sub.id, old],
      );
    }
  }

  const { OrderSide } = await import("@polymarket/client");
  const res = await client.placeLimitOrder({
    assetId: intent.token_id,
    side: OrderSide.SELL,
    price,
    size: shares,
    builderCode: process.env.POLYMARKET_BUILDER_CODE,
  });

  await finish(orderId, {
    status: res?.orderId ? "placed" : "rejected",
    limit_price: price,
    clob_order_id: res?.orderId || null,
    error: res?.orderId ? null : (res?.message || "not placed"),
  });
  return Boolean(res?.orderId);
}

/**
 * One pass: every intent newer than the cursor, fanned out to every subscriber.
 *
 * The cursor advances even when a subscriber errored. Their failure is recorded
 * on their own row; holding the cursor back would replay the intent for
 * everyone else on the next tick, and the unique index would then make those
 * look like duplicates rather than retries.
 */
async function runFanout() {
  const cur = await q(`select last_intent_id from copy.fanout_cursor where id = 1`);
  const from = Number(cur.rows[0]?.last_intent_id || 0);

  const { rows: intents } = await q(
    `select * from bots.trade_intents where id > $1 order by id asc limit 200`,
    [from],
  );
  if (!intents.length) return { intents: 0, placed: 0 };

  let placed = 0;
  let lastId = from;
  for (const intent of intents) {
    const subs = await subscribersOf(intent.bot_id);
    for (const sub of subs) {
      const orderId = await claim(sub.id, intent);
      if (!orderId) continue; // already handled
      try {
        const ok = intent.kind === "enter"
          ? await copyEnter(sub, intent, orderId)
          : await copyExit(sub, intent, orderId);
        if (ok) placed++;
      } catch (e) {
        log.err(`copy: sub ${sub.id} intent ${intent.id}: ${e.message}`);
        await finish(orderId, { status: "rejected", error: String(e.message).slice(0, 400) })
          .catch(() => {});
      }
    }
    lastId = intent.id;
  }

  await q(
    `update copy.fanout_cursor set last_intent_id = $1, updated_at = now() where id = 1`,
    [lastId],
  );
  if (placed) log(`  copy: ${placed} order(s) placed across ${intents.length} intent(s)`);
  return { intents: intents.length, placed };
}

module.exports = { runFanout, sleeveNav };
