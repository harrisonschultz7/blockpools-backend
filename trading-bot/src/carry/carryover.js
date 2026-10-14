// trading-bot/src/carry/carryover.js
//
// KEEP AFTER KICKOFF. Polymarket clears every resting order on a sports market
// at the official game start. A user who ticked "Keep after kickoff" on a
// pre-kickoff limit order (public.pm_limit_orders.carry_over, set by
// src/routes/pmOrders.ts only after Privy confirmed our session signer) has it
// re-placed here shortly after the start: same side, same price, same size --
// or, for a sell, no more than they still hold.
//
// Spends from a user's wallet unattended, so it fails closed like the copy
// fan-out:
//   - one order at a time is CLAIMED (pending -> placing) with SKIP LOCKED, so
//     two workers or a restart can never place the same carry twice
//   - delegation is asked of Privy at placement time, never trusted from the row
//   - a user who cancels while we are placing gets the new order cancelled too
//   - every outcome is written to the row (placed / failed / no_permission /
//     no_shares), never silent
//
// Lifecycle of the original row: live+pending -> (placed) closed 'carried', and a
// NEW live row is inserted under the re-placed order id with carried_from set,
// so the app keeps showing one order and its Edit/Cancel act on the real one.

const { PrivyClient } = require("@privy-io/server-auth");
const { q, pool } = require("../db");
const log = require("../log");
const { getPmClient } = require("../copy/pmClient");

/** Polymarket clears AT the start; give the clear time to finish first. */
const DELAY_S = Number(process.env.LIMIT_CARRY_DELAY_S || 90);
/** Placement retries before giving up (backoff 30s, 60s, 120s, ...). */
const MAX_ATTEMPTS = 5;
/** Past this the order is not worth placing (game long over / market closing). */
const GIVE_UP_S = 6 * 3600;
const DRY_RUN = /^(1|true|yes|on)$/i.test(String(process.env.LIMIT_CARRY_DRY_RUN || ""));

let privy = null;
function privyClient() {
  if (!privy) {
    privy = new PrivyClient(
      String(process.env.PRIVY_APP_ID || "").trim(),
      String(process.env.PRIVY_APP_SECRET || "").trim(),
    );
  }
  return privy;
}

/** Does the user still have our session signer on this wallet? Errors = no. */
async function stillDelegated(did, address) {
  try {
    const user = await privyClient().getUserById(did);
    return (user?.linkedAccounts || []).some((a) =>
      a.type === "wallet" && a.delegated === true &&
      String(a.address || "").toLowerCase() === String(address).toLowerCase());
  } catch (e) {
    log.warn(`carry: delegation check failed for ${did}: ${e.message}`);
    return false;
  }
}

async function getJson(url) {
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`${r.status} ${url}`);
  return r.json();
}

/** Market tick size from the CLOB; 0.01 (valid on either grid) on failure. */
async function tickSize(tokenId) {
  try {
    const j = await getJson(`https://clob.polymarket.com/tick-size?token_id=${tokenId}`);
    const t = Number(j?.minimum_tick_size);
    return Number.isFinite(t) && t > 0 ? t : 0.01;
  } catch { return 0.01; }
}

/**
 * Snap to the grid. A SELL price is a floor on what we accept, so it rounds UP;
 * a BUY price is a ceiling on what we pay, so it rounds DOWN. Either way the
 * user never gets a worse price than they set.
 */
function snap(price, tick, side) {
  const d = Math.max(0, Math.round(-Math.log10(tick)));
  const f = side === "sell" ? Math.ceil : Math.floor;
  return Number((f(Number((price / tick).toFixed(6))) * tick).toFixed(d));
}

/** Shares of this outcome token the user's Polymarket wallet holds now. */
async function heldShares(userId, marketId, tokenId) {
  const { rows } = await q(`select trading_address from public.users where id = $1`, [userId]);
  const wallet = rows[0]?.trading_address;
  if (!wallet) return null; // unknown -> let the exchange decide
  const list = await getJson(
    `https://data-api.polymarket.com/positions?user=${wallet}&market=${marketId}&sizeThreshold=0`);
  const p = (Array.isArray(list) ? list : []).find((x) => String(x.asset) === String(tokenId));
  return p ? Number(p.size) || 0 : 0;
}

/** Claim the next due carry, or null. pending -> placing, atomically. */
async function claimNext() {
  const { rows } = await q(
    `update public.pm_limit_orders
        set carry_status = 'placing', updated_at = now()
      where id = (
        select id from public.pm_limit_orders
         where status = 'live' and carry_over and carry_status = 'pending'
           and game_start_at <= now() - make_interval(secs => $1)
           and (carry_next_at is null or carry_next_at <= now())
         order by game_start_at
         limit 1
         for update skip locked)
      returning *`,
    [DELAY_S],
  );
  return rows[0] || null;
}

/** End a carry without a new order: the original closes, with why. */
async function giveUp(row, carryStatus, error) {
  await q(
    `update public.pm_limit_orders
        set status = 'closed', closed_reason = 'game_start', closed_at = now(), updated_at = now(),
            carry_status = $2, carry_error = $3
      where id = $1`,
    [row.id, carryStatus, error ? String(error).slice(0, 500) : null],
  );
  log(`carry ${row.id.slice(0, 12)}…: ${carryStatus}${error ? ` (${String(error).slice(0, 120)})` : ""}`);
}

/** A failed attempt: back to pending with backoff, or give up. */
async function retryLater(row, error) {
  const attempts = Number(row.carry_attempts || 0) + 1;
  if (attempts >= MAX_ATTEMPTS) return giveUp(row, "failed", error);
  const backoff = 30 * 2 ** (attempts - 1);
  await q(
    `update public.pm_limit_orders
        set carry_status = 'pending', carry_attempts = $2, carry_error = $3,
            carry_next_at = now() + make_interval(secs => $4), updated_at = now()
      where id = $1`,
    [row.id, attempts, String(error).slice(0, 500), backoff],
  );
  log.warn(`carry ${row.id.slice(0, 12)}…: attempt ${attempts} failed, retry in ${backoff}s: ${String(error).slice(0, 160)}`);
}

/**
 * Swap the original for the re-placed order -- unless the user closed the
 * original while we were placing, in which case the new order must not stand.
 * Returns false when the user had cancelled.
 */
async function commitPlaced(row, newId, price, size) {
  const c = await pool.connect();
  try {
    await c.query("begin");
    const cur = await c.query(
      `select status from public.pm_limit_orders where id = $1 for update`, [row.id]);
    if (cur.rows[0]?.status !== "live") { await c.query("rollback"); return false; }
    await c.query(
      `update public.pm_limit_orders
          set status = 'closed', closed_reason = 'carried', closed_at = now(), updated_at = now(),
              carry_status = 'placed', carry_error = null
        where id = $1`,
      [row.id]);
    await c.query(
      `insert into public.pm_limit_orders
         (id, user_id, owner_address, market_id, outcome_index, side, price, shares,
          token_id, signer_address, game_start_at, carry_over, carry_status, carried_from)
       values ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,false,'placed',$12)
       on conflict (id) do nothing`,
      [newId, row.user_id, row.owner_address, row.market_id, row.outcome_index, row.side,
       price, size, row.token_id, row.signer_address, row.game_start_at, row.id]);
    await c.query("commit");
    return true;
  } catch (e) {
    await c.query("rollback").catch(() => {});
    throw e;
  } finally {
    c.release();
  }
}

async function carryOne(row) {
  const tag = `carry ${row.id.slice(0, 12)}…`;
  if (Date.now() / 1000 - new Date(row.game_start_at).getTime() / 1000 > GIVE_UP_S) {
    return giveUp(row, "failed", "too long after game start");
  }
  if (!row.token_id || !row.signer_address) return giveUp(row, "failed", "missing token/signer");
  if (!(await stillDelegated(row.user_id, row.signer_address))) {
    return giveUp(row, "no_permission", "session signer not granted");
  }

  // A sell can only be for what is still held (they may have sold some, or the
  // order part-filled before kickoff). Floor to 2dp: never more than held.
  let size = Number(row.shares);
  if (row.side === "sell") {
    let held = null;
    try { held = await heldShares(row.user_id, row.market_id, row.token_id); }
    catch (e) { return retryLater(row, `position read: ${e.message}`); }
    if (held != null) size = Math.min(size, Math.floor(held * 100) / 100);
    if (!(size > 0)) return giveUp(row, "no_shares", `holds ${held}`);
  }
  const price = snap(Number(row.price), await tickSize(row.token_id), row.side);

  if (DRY_RUN) {
    log(`${tag} [dry] would place ${row.side} ${size} @ ${price} token ${row.token_id.slice(0, 10)}…`);
    // Parked, not done: re-checked in 10 min so a dry run never closes orders.
    await q(`update public.pm_limit_orders set carry_status = 'pending', carry_error = 'dry_run',
               carry_next_at = now() + interval '10 minutes' where id = $1`, [row.id]);
    return;
  }

  let res;
  try {
    const client = await getPmClient(row.signer_address);
    const { OrderSide } = await import("@polymarket/client");
    res = await client.placeLimitOrder({
      assetId: row.token_id,
      side: row.side === "sell" ? OrderSide.SELL : OrderSide.BUY,
      price,
      size,
      builderCode: process.env.POLYMARKET_BUILDER_CODE,
    });
  } catch (e) {
    return retryLater(row, e.message || String(e));
  }
  const newId = res?.orderId || res?.orderID || null;
  if (!newId) return retryLater(row, res?.message || res?.errorMsg || "not placed");

  let kept;
  try { kept = await commitPlaced(row, newId, price, size); }
  catch (e) {
    // Placed on the venue but not recorded: leave the original 'placing' so it
    // is not placed twice, and say so loudly -- this needs a human.
    log.err(`${tag}: placed ${newId} but could not record it: ${e.message}`);
    return;
  }
  if (!kept) {
    // The user cancelled the original mid-placement. Honour it.
    try { const client = await getPmClient(row.signer_address); await client.cancelOrder({ orderId: newId }); }
    catch (e) { log.err(`${tag}: user cancelled but cancelling ${newId} failed: ${e.message}`); }
    log(`${tag}: user cancelled during placement; ${newId} cancelled`);
    return;
  }
  log(`${tag}: re-placed as ${newId.slice(0, 12)}… ${row.side} ${size} @ ${price}`);
}

/** One pass: every due carry, one at a time. Returns how many were handled. */
async function runCarryOver({ max = 25 } = {}) {
  let n = 0;
  for (; n < max; n++) {
    const row = await claimNext();
    if (!row) break;
    try { await carryOne(row); }
    catch (e) {
      log.err(`carry ${row.id}: ${e.stack || e.message}`);
      await retryLater(row, e.message || String(e)).catch(() => {});
    }
  }
  return n;
}

/**
 * Rows stuck in 'placing' (the worker died mid-order). Not re-placed
 * automatically -- the order may be on the book -- just reported.
 */
async function reportStuck() {
  const { rows } = await q(
    `select id from public.pm_limit_orders
      where status = 'live' and carry_status = 'placing' and updated_at < now() - interval '10 minutes'`);
  for (const r of rows) log.err(`carry ${r.id}: stuck in 'placing' >10m -- check the venue by hand`);
}

module.exports = { runCarryOver, reportStuck, snap, DRY_RUN };
