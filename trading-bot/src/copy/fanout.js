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
const mkt = require("./market");

/** Nothing below this is worth a fill; Polymarket's own floor is $5. */
const MIN_ORDER_USD = 5;

/**
 * DRY RUN: do everything except hand the order to the exchange.
 *
 * The point is the bookkeeping, not the trade. Sizing, position accounting,
 * exit sizing, reprice cancel-and-replace and every surface built on top of
 * them are the parts that have never run end to end, and they do not need a
 * real fill to be exercised -- they need a fill-shaped result.
 *
 * Off unless explicitly asked for, and everything it writes is flagged
 * dry_run so a simulated position can never be mistaken for one somebody
 * actually holds.
 */
const DRY_RUN = /^(1|true|yes|on)$/i.test(String(process.env.COPY_DRY_RUN || ""));

/**
 * What the order would have filled at.
 *
 * Reads the live book -- a public GET, not an order -- so a simulated fill
 * carries a real price and the position maths is exercised on a real number
 * rather than on the ceiling, which would never slip and never partial.
 * Falls back to the ceiling when the book cannot be read: a dry run must not
 * fail for want of a nicety.
 */
async function simulatedFillPrice(tokenId, maxPrice) {
  try {
    const r = await fetch(`https://clob.polymarket.com/book?token_id=${tokenId}`);
    if (!r.ok) return maxPrice;
    const b = await r.json();
    const asks = (b?.asks || [])
      .map((a) => Number(a.price))
      .filter((p) => Number.isFinite(p) && p > 0)
      .sort((x, y) => x - y);
    const best = asks[0];
    // Past our ceiling is a miss, which is a real outcome worth simulating.
    if (!Number.isFinite(best)) return maxPrice;
    return best > maxPrice ? null : best;
  } catch {
    return maxPrice;
  }
}

/** Distinguishable from a real exchange id at a glance and in a query. */
function dryOrderId(kind) {
  return `dry-${kind}-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

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
      where bot_id = $1 and status = 'active' and delegated_at is not null
        -- never trade for someone with no recorded agreement to the terms
        and terms_accepted_at is not null`,
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
  // Stamped here rather than at each call site: the early skips -- no funds,
  // below minimum, no position -- outnumber the paths that reach an exchange,
  // and one of them forgetting the flag is a simulated row that looks real.
  // Postgres refuses two assignments to one column, so only when absent.
  const full = "dry_run" in patch ? patch : { ...patch, dry_run: DRY_RUN };
  for (const [k, v] of Object.entries(full)) {
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
    // Never silent: the sleeve is too small for the venue minimum, so this
    // model has stopped trading for them until they top up.
    if (!DRY_RUN) await notifyBelowMin(sub, intent.bot_id, nav);
    return false;
  }

  const client = await getPmClient(sub.wallet_address);
  const wallets = await mkt.userWallets(sub.privy_did);

  // The wallet is the real constraint, not the sleeve. Someone can allocate
  // $500 and later withdraw to $50; the sleeve does not know, and only the
  // balance at THIS moment can say what is spendable.
  const balance = await mkt.spendableUsd(wallets.trading);
  if (balance !== null && usd > balance) {
    // The wallet cannot cover the model's size -- the user's own trades or a
    // withdrawal spent what the allocation assumed. Never silent: tell them.
    if (!DRY_RUN) await notifyUnderfunded(sub, intent.bot_id, usd, balance);
    if (balance < MIN_ORDER_USD) {
      await finish(orderId, {
        status: "skipped", skip_reason: "insufficient_funds", requested_usd: usd,
      });
      return false;
    }
    usd = balance;
  }

  const { OrderSide, OrderType } = await import("@polymarket/client");
  const tick = await mkt.tickSize(intent.token_id);
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
    if (DRY_RUN) {
      // Everything above this line ran for real: the sleeve maths, the wallet
      // read, the signing client. Only the submission is withheld.
      const fill = await simulatedFillPrice(intent.token_id, maxPrice);
      res = fill === null
        ? { orderId: null, makingAmount: 0, message: "dry run: book above maxPrice" }
        : { orderId: dryOrderId("enter"), makingAmount: usd, takingAmount: usd / fill };
      log(`copy[dry] enter sub=${sub.id} ${usd.toFixed(2)} ` +
               `max=${maxPrice} fill=${fill ?? "none"}`);
    } else {
      res = await client.placeMarketOrder({
        assetId: intent.token_id,
        side: OrderSide.BUY,
        amount: usd,
        maxSpend: usd,
        maxPrice,
        orderType: OrderType.FAK,
        builderCode: process.env.POLYMARKET_BUILDER_CODE,
      });
    }
  } catch (e) {
    // Record what we TRIED before rethrowing. The outer handler only knows the
    // message, and a rejected row with no size or price is near useless when
    // the thing that failed is the size or the price.
    await finish(orderId, {
      status: "rejected",
      requested_usd: usd,
      limit_price: maxPrice,
      dry_run: DRY_RUN,
      error: String(e.message || e).slice(0, 400),
    }).catch(() => {});
    throw e;
  }

  // A BUY gives dollars (making) for shares (taking). The dry run's simulated
  // result follows the same shape.
  const shares = Number(res?.takingAmount || 0);
  const spent = Number(res?.makingAmount || 0) > 0 ? Number(res.makingAmount) : usd;
  await finish(orderId, {
    status: shares > 0 ? "filled" : "rejected",
    requested_usd: usd,
    limit_price: maxPrice,
    clob_order_id: res?.orderId || null,
    filled_shares: shares || null,
    avg_price: shares > 0 ? spent / shares : null,
    dry_run: DRY_RUN,
    error: shares > 0 ? null : (res?.message || "no fill"),
  });

  if (shares > 0) {
    await q(
      `insert into copy.positions
         (subscription_id, token_id, bot_trade_id, shares, cost_usd, avg_price, dry_run)
       values ($1,$2,$3,$4,$5,$6,$7)
       on conflict (subscription_id, token_id, dry_run) do update set
         -- A closed row (exited or settled) starts over rather than adding to
         -- shares that no longer exist.
         shares    = case when copy.positions.closed_at is null
                          then copy.positions.shares + excluded.shares else excluded.shares end,
         cost_usd  = case when copy.positions.closed_at is null
                          then copy.positions.cost_usd + excluded.cost_usd else excluded.cost_usd end,
         avg_price = case when copy.positions.closed_at is null
                          then (copy.positions.cost_usd + excluded.cost_usd)
                               / nullif(copy.positions.shares + excluded.shares, 0)
                          else excluded.avg_price end,
         bot_trade_id = excluded.bot_trade_id,
         opened_at  = case when copy.positions.closed_at is null then copy.positions.opened_at else now() end,
         closed_at  = null,
         realized_usd = null,
         updated_at = now()`,
      [sub.id, intent.token_id, intent.bot_trade_id, shares, spent, spent / shares, DRY_RUN],
    );
    if (!DRY_RUN && res?.orderId) {
      // Into the shared order record (closed at once: a FAK does not rest), so
      // the fill sync writes the buy to the subscriber's History with its fee.
      await recordPmOrder(sub, intent, wallets, {
        id: res.orderId, side: "buy", price: maxPrice, shares, live: false,
      });
      await notifyCopy(sub, intent, "buy", spent / shares, res.orderId);
    }
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
 * Shares this subscriber can sell now: the copied position, capped at what the
 * wallet holds so an exit that part-filled, or shares sold by hand, never
 * leaves a sell for shares that are gone. Floored to 2dp.
 *
 * The exit intent lands seconds after the entry, often before the buy has
 * settled on-chain. A fresh position that reads short is therefore waited on
 * (up to ~45s) instead of skipped -- skipping it is how copied positions ended
 * up with no resting sell at all.
 */
async function sellableShares(pos, wallets, intent) {
  const want = Number(pos.shares);
  const fresh = Date.now() - new Date(pos.updated_at || 0).getTime() < 10 * 60 * 1000;
  let held = null;
  for (let attempt = 0; attempt < (fresh ? 10 : 1); attempt++) {
    if (attempt) await new Promise((r) => setTimeout(r, 5000));
    held = await mkt.heldShares(wallets.trading, intent.condition_id, intent.token_id);
    if (held === null || held >= want * 0.99) break;
  }
  const shares = held === null ? want : Math.min(want, held);
  return Math.floor(shares * 100) / 100;
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
    `select shares, detached, updated_at from copy.positions
      where subscription_id = $1 and token_id = $2 and dry_run = $3
        and closed_at is null`,
    [sub.id, intent.token_id, DRY_RUN],
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
  const wallets = await mkt.userWallets(sub.privy_did);
  // Snapped UP for a sell: the price is a floor on what we will accept, so
  // rounding it down would sell cheaper than the bot intended -- the mirror of
  // the ceiling rule on the way in.
  const tick = await mkt.tickSize(intent.token_id);
  const raw = Number(intent.limit_price);
  const decimals = Math.max(0, Math.round(-Math.log10(tick)));
  const price = Number((Math.ceil(raw / tick) * tick).toFixed(decimals));
  // Sized from what the wallet HOLDS, capped at the copied position: an exit
  // that already part-filled, or shares sold by hand, must not leave a sell for
  // shares that are gone. Floor to 2dp -- never more than held.
  const shares = await sellableShares(pos, wallets, intent);
  if (!(shares > 0)) {
    await finish(orderId, { status: "skipped", skip_reason: "no_shares_held" });
    return false;
  }

  if (intent.kind === "reprice" && !DRY_RUN) {
    // The live exit may no longer be the id copy.orders remembers: the
    // carry-over worker re-places it after kickoff under a new id. The shared
    // order record knows the current one.
    await cancelLiveCopySells(client, sub, intent.token_id, "edited");
  }

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
      // A dry-run order was never on the book, so there is nothing to cancel --
      // but the row still has to move, or the next reprice finds two live.
      if (!DRY_RUN) {
        try { await client.cancelOrder({ orderId: old }); }
        catch (e) { log.warn(`copy: cancel ${old} failed: ${e.message}`); }
      }
      await q(
        `update copy.orders set status = 'skipped', skip_reason = 'repriced',
                updated_at = now()
          where subscription_id = $1 and clob_order_id = $2`,
        [sub.id, old],
      );
    }
  }

  const { OrderSide } = await import("@polymarket/client");
  let res;
  if (DRY_RUN) {
    // A resting sell has no fill to simulate -- it either rests or it does not,
    // and what matters here is that it was sized from the subscriber's OWN
    // position rather than the bot's.
    res = { orderId: dryOrderId(intent.kind) };
    log(`copy[dry] ${intent.kind} sub=${sub.id} ${shares.toFixed(2)} sh @ ${price}`);
  } else {
    res = await client.placeLimitOrder({
      assetId: intent.token_id,
      side: OrderSide.SELL,
      price,
      size: shares,
      builderCode: process.env.POLYMARKET_BUILDER_CODE,
    });
    if (res?.orderId) {
      // Shared order record: the betslip shows it, the carry-over worker keeps
      // it through kickoff (the model's exit rides the game), and the fill sync
      // logs its fills to History.
      await recordPmOrder(sub, intent, wallets, {
        id: res.orderId, side: "sell", price, shares, live: true,
      });
      if (intent.kind === "exit") await notifyCopy(sub, intent, "limit", price, res.orderId);
    }
  }

  await finish(orderId, {
    status: res?.orderId ? "placed" : "rejected",
    limit_price: price,
    clob_order_id: res?.orderId || null,
    dry_run: DRY_RUN,
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

  if (!DRY_RUN && Date.now() - lastReconcile > RECONCILE_MS) {
    lastReconcile = Date.now();
    await reconcileSleeves().catch((e) => log.warn(`copy: reconcile failed: ${e.message}`));
  }

  const { rows: intents } = await q(
    `select * from bots.trade_intents where id > $1 order by id asc limit 200`,
    [from],
  );
  if (!intents.length) return { intents: 0, placed: 0 };

  let placed = 0;
  let lastId = from;
  for (const intent of intents) {
    // Moneyline exits and reprices are written without a condition id (the
    // game id is unambiguous to the bot). The copy side needs it to record the
    // sell -- unrecorded, the carry-over worker never sees it and Polymarket's
    // kickoff wipe leaves the position with no sell -- so take it from the
    // entry on the same token.
    if (!intent.condition_id) {
      const e = await q(
        `select condition_id, line from bots.trade_intents
          where token_id = $1 and condition_id is not null order by id desc limit 1`,
        [intent.token_id]);
      if (e.rows[0]) {
        intent.condition_id = e.rows[0].condition_id;
        if (intent.line == null) intent.line = e.rows[0].line;
      }
    }
    const subs = await subscribersOf(intent.bot_id);
    for (const sub of subs) {
      const orderId = await claim(sub.id, intent);
      if (!orderId) continue; // already handled
      try {
        const ok = intent.kind === "enter"
          ? await copyEnter(sub, intent, orderId)
          : intent.kind === "stop"
            ? await copyStop(sub, intent, orderId)
            : await copyExit(sub, intent, orderId);
        if (ok) placed++;
      } catch (e) {
        log.err(`copy: sub ${sub.id} intent ${intent.id}: ${e.message}`);
        await finish(orderId, { status: "rejected", error: String(e.message).slice(0, 400) })
          .catch(() => {});
      }
    }
    if (intent.kind === "stop") {
      try { placed += await ticketStops(intent); }
      catch (e) { log.err(`copy: ticket stops for intent ${intent.id}: ${e.message}`); }
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

/** Cancel every live copied sell on this token (venue + shared record). */
async function cancelLiveCopySells(client, sub, tokenId, reason) {
  const live = await q(
    `select id from public.pm_limit_orders
      where user_id = $1 and token_id = $2 and side = 'sell' and status = 'live'
        and meta->>'source' = 'copy'`,
    [sub.privy_did, tokenId],
  );
  for (const r of live.rows) {
    try { await client.cancelOrder({ orderId: r.id }); }
    catch (e) { log.warn(`copy: cancel ${r.id} failed: ${e.message}`); }
    await q(
      `update public.pm_limit_orders set status = 'closed', closed_reason = $2,
              closed_at = now(), updated_at = now() where id = $1 and status = 'live'`,
      [r.id, reason],
    );
  }
}

/** Accept this far below the bot's stop price: a stop fires into a falling
 *  book, and a FAK that refuses the next tick down leaves the position open. */
const STOP_SLIPPAGE = 0.05;

/**
 * The bot stopped out: sell everything this subscriber holds, now.
 *
 * Cancel the resting exit first (its shares are locked by the venue), then a
 * FAK market sell of what the wallet holds, floored at the bot's stop price
 * less STOP_SLIPPAGE. Logged to the shared order record so the fill sync puts
 * it in History and the sleeve banks the result.
 */
async function copyStop(sub, intent, orderId) {
  const { rows } = await q(
    `select shares, detached, updated_at from copy.positions
      where subscription_id = $1 and token_id = $2 and dry_run = $3 and closed_at is null`,
    [sub.id, intent.token_id, DRY_RUN],
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
  const wallets = await mkt.userWallets(sub.privy_did);
  if (!DRY_RUN) await cancelLiveCopySells(client, sub, intent.token_id, "stop_loss");

  const shares = await sellableShares(pos, wallets, intent);
  if (!(shares > 0)) {
    await finish(orderId, { status: "skipped", skip_reason: "no_shares_held" });
    return false;
  }

  const tick = await mkt.tickSize(intent.token_id);
  const minPrice = Math.max(tick, floorToTick(Number(intent.limit_price) * (1 - STOP_SLIPPAGE), tick));

  let res;
  if (DRY_RUN) {
    res = { orderId: dryOrderId("stop"), makingAmount: shares, takingAmount: shares * Number(intent.limit_price) };
    log(`copy[dry] stop sub=${sub.id} ${shares.toFixed(2)} sh min=${minPrice}`);
  } else {
    const { OrderSide, OrderType } = await import("@polymarket/client");
    try {
      res = await client.placeMarketOrder({
        assetId: intent.token_id,
        side: OrderSide.SELL,
        shares,
        minPrice,
        orderType: OrderType.FAK,
        builderCode: process.env.POLYMARKET_BUILDER_CODE,
      });
    } catch (e) {
      await finish(orderId, {
        status: "rejected", limit_price: minPrice, error: String(e.message || e).slice(0, 400),
      }).catch(() => {});
      throw e;
    }
  }

  // A SELL gives shares (making) for dollars (taking).
  const sold = Number(res?.makingAmount || 0);
  const got = Number(res?.takingAmount || 0);
  if (!DRY_RUN && res?.orderId && sold > 0) {
    // Recorded at the floor: every fill at or above it is this order's.
    await recordPmOrder(sub, intent, wallets, {
      id: res.orderId, side: "sell", price: minPrice, shares: sold, live: false,
    });
    await notifyCopy(sub, intent, "sell", got > 0 ? got / sold : minPrice, res.orderId);
  }
  await finish(orderId, {
    status: sold > 0 ? "filled" : "rejected",
    limit_price: minPrice,
    clob_order_id: res?.orderId || null,
    filled_shares: sold || null,
    avg_price: sold > 0 && got > 0 ? got / sold : null,
    error: sold > 0 ? null : (res?.message || "no fill"),
  });
  return sold > 0;
}

/**
 * QUEUE TICKETS FOLLOW THE MODEL'S STOP TOO.
 *
 * A user who approved a model's alert ticket (the Queue) bought the position and
 * rested the model's exit themselves. Polymarket has no stop order, so the
 * model's stop-loss can only be executed by a server holding the user's session
 * signer -- the same thing copyStop does for subscribers.
 *
 * Who qualifies, and why that is the consent: the ticket's exit is still LIVE
 * and was carried over (or flagged to carry), which only happens when the user
 * ticked "keep the exit after kickoff" on the ticket AND granted the signer --
 * the backend verifies the grant with Privy before setting carry_over. A user
 * who cancelled the exit, or never granted the signer, is left alone.
 *
 * Returns the number of positions sold.
 */
async function ticketStops(intent) {
  const { rows } = await q(
    `select distinct on (user_id) user_id, signer_address
       from public.pm_limit_orders
      where token_id = $1 and side = 'sell' and status = 'live'
        and (carry_over or carried_from is not null)
        and signer_address is not null
        and meta->>'source' = 'queue' and meta->>'botId' = $2
      order by user_id, created_at desc`,
    [intent.token_id, intent.bot_id],
  );
  let sold = 0;
  for (const r of rows) {
    const who = { privy_did: r.user_id, wallet_address: r.signer_address };
    try {
      const client = await getPmClient(r.signer_address);
      const wallets = await mkt.userWallets(r.user_id);

      // Release the resting exit first -- the venue locks its shares.
      const live = await q(
        `select id from public.pm_limit_orders
          where user_id = $1 and token_id = $2 and side = 'sell' and status = 'live'
            and meta->>'source' = 'queue'`,
        [r.user_id, intent.token_id]);
      for (const o of live.rows) {
        if (!DRY_RUN) {
          try { await client.cancelOrder({ orderId: o.id }); }
          catch (e) { log.warn(`ticket stop: cancel ${o.id} failed: ${e.message}`); }
        }
        await q(
          `update public.pm_limit_orders set status = 'closed', closed_reason = 'stop_loss',
                  closed_at = now(), updated_at = now() where id = $1 and status = 'live'`,
          [o.id]);
      }

      const held = await mkt.heldShares(wallets.trading, intent.condition_id, intent.token_id);
      const shares = held === null ? 0 : Math.floor(held * 100) / 100;
      if (!(shares > 0)) { log(`ticket stop: ${r.user_id} holds nothing on ${intent.game_id}`); continue; }

      const tick = await mkt.tickSize(intent.token_id);
      const minPrice = Math.max(tick, floorToTick(Number(intent.limit_price) * (1 - STOP_SLIPPAGE), tick));
      if (DRY_RUN) {
        log(`copy[dry] ticket stop ${r.user_id} ${shares.toFixed(2)} sh min=${minPrice}`);
        continue;
      }
      const { OrderSide, OrderType } = await import("@polymarket/client");
      const res = await client.placeMarketOrder({
        assetId: intent.token_id, side: OrderSide.SELL, shares, minPrice,
        orderType: OrderType.FAK, builderCode: process.env.POLYMARKET_BUILDER_CODE,
      });
      const out = Number(res?.makingAmount || 0);
      const got = Number(res?.takingAmount || 0);
      if (res?.orderId && out > 0) {
        await recordPmOrder(who, intent, wallets, {
          id: res.orderId, side: "sell", price: minPrice, shares: out, live: false, source: "queue",
        });
        await notifyCopy(who, intent, "sell", got > 0 ? got / out : minPrice, res.orderId);
        sold++;
        log(`ticket stop: sold ${out.toFixed(2)} sh for ${r.user_id} on ${intent.game_id}`);
      } else {
        log.warn(`ticket stop: no fill for ${r.user_id} on ${intent.game_id} (${res?.message || "no fill"})`);
      }
    } catch (e) {
      log.err(`ticket stop ${r.user_id} intent ${intent.id}: ${e.message}`);
    }
  }
  return sold;
}

// ── Shared order record, notifications, sleeve P&L ──────────────────────────

/**
 * Write a copied order into public.pm_limit_orders (meta.source = 'copy'), the
 * record every other piece already reads:
 *   - the betslip shows a live exit as the position's limit row
 *   - the carry-over worker re-places a pre-kickoff exit after Polymarket clears
 *     the book at kickoff (the bot keeps its exit through the game)
 *   - the fill sync writes each fill to user_trade_events (History, fees, ROI)
 * A buy is a FAK that fills at once: recorded closed, with a short window for
 * its fill to land in the venue's feed.
 */
async function recordPmOrder(sub, intent, wallets, o) {
  try {
    const info = await mkt.marketInfo(intent.condition_id, intent.line);
    const idx = mkt.outcomeIndexOf(info, intent.token_id);
    if (!info || idx === null || !info.gameId) {
      log.warn(`copy: no market record for ${intent.condition_id} -- order ${o.id} not recorded`);
      return;
    }
    const start = info.gameStartSec;
    const carry = o.live && o.side === "sell" && start && start > Date.now() / 1000;
    await q(
      `insert into public.pm_limit_orders
         (id, user_id, owner_address, market_id, outcome_index, side, price, shares,
          token_id, signer_address, game_start_at, carry_over, carry_status,
          game_id, meta, status, closed_reason, closed_at)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, to_timestamp($11), $12,
               case when $12 then 'pending' end, $13, $14::jsonb,
               case when $15 then 'live' else 'closed' end,
               case when $15 then null else 'copy_fak' end,
               case when $15 then null else now() + interval '2 minutes' end)
       on conflict (id) do nothing`,
      [o.id, sub.privy_did, wallets.owner, info.conditionId, idx, o.side,
       Number(o.price), Number(o.shares), String(intent.token_id),
       String(sub.wallet_address).toLowerCase(), start, !!carry, info.gameId,
       // serverPlaced: no app logged this order, so the fill sync must (see
       // src/workers/pmFillSyncWorker.ts). A queue-ticket stop is placed here too.
       JSON.stringify({ ...(info.meta || {}), source: o.source || "copy", botId: intent.bot_id,
                        serverPlaced: true }),
       !!o.live],
    );
  } catch (e) {
    log.warn(`copy: recording order ${o.id} failed: ${e.message}`);
  }
}

const botNames = new Map();
async function botName(botId) {
  if (!botNames.has(botId)) {
    const { rows } = await q(`select name from bots.bot where id = $1`, [botId]);
    botNames.set(botId, rows[0]?.name || botId);
  }
  return botNames.get(botId);
}

/** "Copied: Adam-7 bought Broncos at 44%" in the subscriber's notifications. */
async function notifyCopy(sub, intent, event, price, orderId) {
  try {
    const info = await mkt.marketInfo(intent.condition_id, intent.line);
    await q(
      `insert into public.notifications (recipient_id, type, actor_id, payload, dedupe_key)
       values ($1, 'model_trade', null, $2::jsonb, $3)
       on conflict do nothing`,
      [sub.privy_did, JSON.stringify({
        copied: true,
        event,
        botId: intent.bot_id,
        botName: await botName(intent.bot_id),
        position: mkt.positionLabel(info, intent.token_id),
        // The game it is on: the notification names it and links its page.
        gameId: info?.gameId || null,
        pricePct: Number.isFinite(price) ? Math.round(price * 100) : null,
      }), `copy:${orderId}`],
    );
  } catch (e) {
    log.warn(`copy: notify failed: ${e.message}`);
  }
}

/** Smallest sleeve whose ~7% trades clear the $5 venue minimum (the Copy flow
 *  asks for at least this much). Below it every entry is skipped. */
const MIN_SLEEVE_USD = Math.ceil(MIN_ORDER_USD / 0.07);

/**
 * "Argo-7 paused: portfolio $70, below the $72 minimum." One per sleeve per
 * day (dedupe key), so a run of skipped signals does not spam the bell.
 */
async function notifyBelowMin(sub, botId, nav) {
  try {
    const day = new Date().toISOString().slice(0, 10);
    await q(
      `insert into public.notifications (recipient_id, type, actor_id, payload, dedupe_key)
       values ($1, 'model_trade', null, $2::jsonb, $3)
       on conflict do nothing`,
      [sub.privy_did, JSON.stringify({
        copied: true, event: 'paused', botId, botName: await botName(botId),
        sleeveUsd: Math.round(Number(nav) * 100) / 100, minUsd: MIN_SLEEVE_USD,
      }), `copy-min:${sub.id}:${day}`],
    );
  } catch (e) {
    log.warn(`copy: below-min notify failed: ${e.message}`);
  }
}

/**
 * "Argo-7 couldn't place a full trade: your wallet has $3 for a $7 trade."
 * Once per sleeve per day, like the below-minimum notice.
 */
async function notifyUnderfunded(sub, botId, needUsd, haveUsd) {
  try {
    const day = new Date().toISOString().slice(0, 10);
    await q(
      `insert into public.notifications (recipient_id, type, actor_id, payload, dedupe_key)
       values ($1, 'model_trade', null, $2::jsonb, $3)
       on conflict do nothing`,
      [sub.privy_did, JSON.stringify({
        copied: true, event: 'underfunded', botId, botName: await botName(botId),
        needUsd: Math.round(Number(needUsd) * 100) / 100,
        haveUsd: Math.round(Number(haveUsd) * 100) / 100,
      }), `copy-funds:${sub.id}:${day}`],
    );
  } catch (e) {
    log.warn(`copy: underfunded notify failed: ${e.message}`);
  }
}

const RECONCILE_MS = 60_000;
let lastReconcile = 0;

/**
 * Close finished sleeve positions and bank their P&L into the subscription, so
 * the sleeve's NAV -- and with it every later trade size -- follows results.
 *
 * A position is finished when its copied exits have sold it (filled shares from
 * the shared order record, at the exit price), or when its market has resolved
 * (what was left pays $1 or $0). Settlement itself is Polymarket's auto-redeem;
 * this only does the sleeve's books.
 */
async function reconcileSleeves() {
  const { rows } = await q(
    `select p.id, p.subscription_id, p.token_id, p.shares, p.cost_usd, p.opened_at, s.privy_did,
            i.condition_id, i.line
       from copy.positions p
       join copy.subscriptions s on s.id = p.subscription_id
       left join lateral (
         select condition_id, line from bots.trade_intents
          where token_id = p.token_id and condition_id is not null
          order by id desc limit 1
       ) i on true
      where not p.dry_run and p.closed_at is null and p.shares > 0`);
  for (const p of rows) {
    if (!p.condition_id) continue;
    // Proceeds are what the fill sync recorded for each copied sell (its rows'
    // ids end in the order's id prefix), net of fees; the order's own price is
    // the fallback for a fill the sync has not written yet.
    const sold = await q(
      `select coalesce(sum(o.filled_shares), 0) as sh,
              coalesce(sum(coalesce(
                (select sum(e.net_out_dec) from public.user_trade_events e
                  where e.id like 'pmfill-%-' || lower(substr(o.id, 3, 12))),
                o.filled_shares * o.price)), 0) as usd
         from public.pm_limit_orders o
        where o.user_id = $1 and o.token_id = $2 and o.side = 'sell'
          and o.meta->>'source' = 'copy' and o.created_at >= $3`,
      [p.privy_did, p.token_id, p.opened_at]);
    const soldSh = Number(sold.rows[0].sh), soldUsd = Number(sold.rows[0].usd);
    const shares = Number(p.shares), cost = Number(p.cost_usd);

    let pnl = null;
    const info = await mkt.marketInfo(p.condition_id, p.line);
    const idx = mkt.outcomeIndexOf(info, p.token_id);
    const px = idx === null ? NaN : info.outcomePrices[idx];
    if (info?.closed && (px >= 0.99 || px <= 0.01)) {
      pnl = soldUsd + Math.max(shares - soldSh, 0) * (px >= 0.99 ? 1 : 0) - cost;
    } else if (soldSh >= shares - Math.max(0.01, shares * 0.001)) {
      pnl = soldUsd - cost;
    }
    if (pnl === null) continue;

    const c = await pool.connect();
    try {
      await c.query("begin");
      const upd = await c.query(
        `update copy.positions set closed_at = now(), realized_usd = $2, updated_at = now()
          where id = $1 and closed_at is null`, [p.id, pnl.toFixed(6)]);
      if (upd.rowCount) {
        await c.query(
          `update copy.subscriptions set realized_pnl = realized_pnl + $2, updated_at = now()
            where id = $1`, [p.subscription_id, pnl.toFixed(6)]);
      }
      await c.query("commit");
      if (upd.rowCount) log(`  copy: sleeve ${p.subscription_id} closed position ${p.id}: ${pnl >= 0 ? "+" : ""}${pnl.toFixed(2)}`);
      if (upd.rowCount) {
        const { rows: subRows } = await q(
          `select id, privy_did, bot_id, basis_usd, realized_pnl from copy.subscriptions where id = $1 and status = 'active'`,
          [p.subscription_id]);
        const sub = subRows[0];
        if (sub && sleeveNav(sub) < MIN_SLEEVE_USD) await notifyBelowMin(sub, sub.bot_id, sleeveNav(sub));
      }
    } catch (e) {
      await c.query("rollback").catch(() => {});
      throw e;
    } finally {
      c.release();
    }
  }
}

module.exports = { runFanout, sleeveNav, reconcileSleeves, copyExit };
