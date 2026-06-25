// src/services/notifications/notify.ts
//
// In-app notification emitters. Every function here is FIRE-AND-FORGET and
// NON-THROWING: notifications are a side effect of follows/trades/promos/
// invites and must never block or fail those flows. Call them without await
// (or with a `.catch`) from the existing write-points.
//
// Storage: public.notifications (see migration create_notifications_table).
// recipient_id / actor_id are users.id (Privy DID). Trades and promos arrive
// keyed by wallet address, so we resolve address -> users row here and
// denormalize the display bits (username, avatar, address) into `payload` so
// the list endpoint needs no joins and links survive later username changes.

import { pool } from "../../db";

type NotificationType = "follow" | "trade" | "promo_credit" | "referral";

interface NotificationRow {
  recipientId: string;
  type: NotificationType;
  actorId?: string | null;
  payload: Record<string, unknown>;
  dedupeKey?: string | null;
}

interface ResolvedUser {
  id: string;
  address: string | null;
  username: string; // always a display-ready label
  avatarUrl: string | null;
}

function shortAddr(addr?: string | null): string {
  if (!addr) return "Someone";
  return `${addr.slice(0, 6)}…${addr.slice(-4)}`;
}

function displayLabel(
  username: string | null,
  displayName: string | null,
  address: string | null
): string {
  return (
    (username && username.trim()) ||
    (displayName && displayName.trim()) ||
    shortAddr(address)
  );
}

async function resolveUserById(id: string): Promise<ResolvedUser | null> {
  if (!id) return null;
  try {
    const { rows } = await pool.query(
      `SELECT id, lower(primary_address) AS address, username, display_name, avatar_url
         FROM public.users WHERE id = $1 LIMIT 1`,
      [id]
    );
    const u = rows[0];
    if (!u) return null;
    return {
      id: u.id,
      address: u.address ?? null,
      username: displayLabel(u.username, u.display_name, u.address),
      avatarUrl: u.avatar_url ?? null,
    };
  } catch (err) {
    console.error("[notify] resolveUserById failed", err);
    return null;
  }
}

async function resolveUserByAddress(addr: string): Promise<ResolvedUser | null> {
  const a = String(addr || "").toLowerCase();
  if (!a) return null;
  try {
    const { rows } = await pool.query(
      `SELECT id, lower(primary_address) AS address, username, display_name, avatar_url
         FROM public.users
        WHERE lower(primary_address) = $1 OR lower(eoa_address) = $1
        LIMIT 1`,
      [a]
    );
    const u = rows[0];
    if (!u) return null;
    return {
      id: u.id,
      address: u.address ?? a,
      username: displayLabel(u.username, u.display_name, u.address ?? a),
      avatarUrl: u.avatar_url ?? null,
    };
  } catch (err) {
    console.error("[notify] resolveUserByAddress failed", err);
    return null;
  }
}

/**
 * Bulk insert. Relies on the partial unique index
 * (recipient_id, type, dedupe_key) WHERE dedupe_key IS NOT NULL for
 * idempotency — re-emitting the same event is a no-op.
 */
async function insertNotifications(rows: NotificationRow[]): Promise<void> {
  if (!rows.length) return;
  try {
    const values: any[] = [];
    const chunks: string[] = [];
    rows.forEach((r, i) => {
      const b = i * 5;
      chunks.push(`($${b + 1}, $${b + 2}, $${b + 3}, $${b + 4}::jsonb, $${b + 5})`);
      values.push(
        r.recipientId,
        r.type,
        r.actorId ?? null,
        JSON.stringify(r.payload ?? {}),
        r.dedupeKey ?? null
      );
    });
    await pool.query(
      `INSERT INTO public.notifications
         (recipient_id, type, actor_id, payload, dedupe_key)
       VALUES ${chunks.join(",")}
       ON CONFLICT (recipient_id, type, dedupe_key)
         WHERE dedupe_key IS NOT NULL
         DO NOTHING`,
      values
    );
  } catch (err) {
    console.error("[notify] insertNotifications failed (non-blocking)", err);
  }
}

/* ============================================================================
   Emitters — call these from the write-points.
============================================================================ */

/**
 * "X followed you" — recipient is the followed user, actor is the follower.
 * Both ids are users.id. Caller should only invoke this when a NEW follow row
 * was actually inserted (gate on rowCount) to avoid re-firing on duplicates.
 */
export async function notifyFollow(opts: {
  followerId: string;
  followingId: string;
}): Promise<void> {
  try {
    if (!opts.followerId || !opts.followingId) return;
    if (opts.followerId === opts.followingId) return;
    const actor = await resolveUserById(opts.followerId);
    if (!actor) return;
    await insertNotifications([
      {
        recipientId: opts.followingId,
        type: "follow",
        actorId: actor.id,
        dedupeKey: `follow:${opts.followerId}`,
        payload: {
          actorUsername: actor.username,
          actorAvatarUrl: actor.avatarUrl,
          actorAddress: actor.address,
        },
      },
    ]);
  } catch (err) {
    console.error("[notify] notifyFollow failed (non-blocking)", err);
  }
}

/**
 * "X bought/sold (Outcome) at Y%" — fans out to the followers of the trader.
 * `traderAddress` is the wallet that traded. One notification per follower,
 * deduped by the trade event id so re-persisting a trade is a no-op.
 */
export async function notifyTradeToFollowers(opts: {
  tradeId: string;
  txHash?: string | null;
  traderAddress: string;
  type: "BUY" | "SELL";
  outcomeCode: string | null;
  outcomeIndex: number | null;
  avgPriceBps: number | null;
  gameId: string; // == pool / contract address
  league: string | null;
}): Promise<void> {
  try {
    if (!opts.tradeId || !opts.traderAddress || !opts.gameId) return;

    const trader = await resolveUserByAddress(opts.traderAddress);
    // No user row -> nobody can be following them anyway.
    if (!trader) return;

    const { rows: followers } = await pool.query(
      `SELECT follower_id FROM public.user_follows WHERE following_id = $1`,
      [trader.id]
    );
    if (!followers.length) return;

    const outcome =
      (opts.outcomeCode && opts.outcomeCode.trim()) ||
      (opts.outcomeIndex != null ? `Outcome ${opts.outcomeIndex + 1}` : "a market");
    const pricePct =
      opts.avgPriceBps != null && Number.isFinite(opts.avgPriceBps)
        ? Math.round(opts.avgPriceBps / 100)
        : null;

    const payload = {
      actorUsername: trader.username,
      actorAvatarUrl: trader.avatarUrl,
      actorAddress: trader.address,
      side: opts.type, // BUY | SELL
      outcome,
      pricePct,
      poolAddress: String(opts.gameId).toLowerCase(),
      league: opts.league,
    };

    // Dedupe on the STABLE on-chain identity (tx hash + outcome index), not the
    // synthetic row id — the indexer's id is unstable (e.g. a "trade-" vs
    // "trade-trade-" prefix flip), which would otherwise re-fire the same trade
    // under a new key on re-ingestion. Fall back to the row id only if no hash.
    const stableKey =
      opts.txHash && opts.txHash.trim()
        ? `${opts.txHash.toLowerCase()}:${opts.outcomeIndex ?? "x"}`
        : opts.tradeId;

    await insertNotifications(
      followers.map((f: any) => ({
        recipientId: f.follower_id as string,
        type: "trade" as const,
        actorId: trader.id,
        dedupeKey: `trade:${stableKey}`,
        payload,
      }))
    );
  } catch (err) {
    console.error("[notify] notifyTradeToFollowers failed (non-blocking)", err);
  }
}

/**
 * "You have been credited with $X promo" — fires when a redemption becomes
 * spendable (status -> 'eligible'). Deduped by redemption id.
 */
export async function notifyPromoCredit(opts: {
  redemptionId: string;
  userAddress: string;
  creditUsdc: number | string;
  promotionName?: string | null;
}): Promise<void> {
  try {
    if (!opts.redemptionId || !opts.userAddress) return;
    const user = await resolveUserByAddress(opts.userAddress);
    if (!user) return;
    const credit = Number(opts.creditUsdc);
    await insertNotifications([
      {
        recipientId: user.id,
        type: "promo_credit",
        actorId: null,
        dedupeKey: `promo:${opts.redemptionId}`,
        payload: {
          creditUsdc: Number.isFinite(credit) ? credit : 0,
          promotionName: opts.promotionName ?? null,
        },
      },
    ]);
  } catch (err) {
    console.error("[notify] notifyPromoCredit failed (non-blocking)", err);
  }
}

/**
 * Convenience wrapper for the promo path: look up the redemption's user_address,
 * credit and campaign name from its id, then emit. Keeps the call sites in
 * evaluatePromoEligibility trivial (just pass the redemption id after the flip).
 */
export async function notifyPromoCreditById(redemptionId: string): Promise<void> {
  try {
    if (!redemptionId) return;
    const { rows } = await pool.query(
      `SELECT r.user_address, r.credit_usdc, p.name AS promotion_name
         FROM public.promo_redemptions r
         LEFT JOIN public.promotions p ON p.id = r.promotion_id
        WHERE r.id = $1
        LIMIT 1`,
      [redemptionId]
    );
    const r = rows[0];
    if (!r || !r.user_address) return;
    await notifyPromoCredit({
      redemptionId,
      userAddress: r.user_address,
      creditUsdc: r.credit_usdc,
      promotionName: r.promotion_name,
    });
  } catch (err) {
    console.error("[notify] notifyPromoCreditById failed (non-blocking)", err);
  }
}

/**
 * "New Referred Friend: X" — recipient is the inviter, actor is the friend who
 * joined. Deduped by invite id so accept + redeem only notify once.
 */
export async function notifyReferral(opts: {
  inviteId: number | string;
  inviterUserId: string;
  refereeUserId: string;
}): Promise<void> {
  try {
    if (!opts.inviterUserId || !opts.refereeUserId) return;
    if (opts.inviterUserId === opts.refereeUserId) return;
    const friend = await resolveUserById(opts.refereeUserId);
    if (!friend) return;
    await insertNotifications([
      {
        recipientId: opts.inviterUserId,
        type: "referral",
        actorId: friend.id,
        dedupeKey: `referral:${opts.inviteId}`,
        payload: {
          actorUsername: friend.username,
          actorAvatarUrl: friend.avatarUrl,
          actorAddress: friend.address,
        },
      },
    ]);
  } catch (err) {
    console.error("[notify] notifyReferral failed (non-blocking)", err);
  }
}
