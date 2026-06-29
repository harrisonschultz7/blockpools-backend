// src/services/promotions/evaluatePromoEligibility.ts
//
// Decides whether a 'pending_qualification' redemption can be transitioned to
// 'eligible'. Called from handlePromoTradeAttribution after a real-money BUY
// trade is persisted for a user.
//
// Trade-driven unlock conditions handled here:
//   - 'first_trade'          → user themselves places real-money BUYs
//   - 'referee_first_trade'  → the referee's wallet (stored on
//                              referrer_address) places real-money BUYs
//
// 'referee_signup' and 'none' are resolved at claim time, not here.
//
// Qualifier model: CUMULATIVE held-to-settlement. For every (game, outcome)
// the user touched, we compute (sum of post-claim BUY gross) − (sum of SELL
// cost_basis_closed). That's what the user was still holding when the game
// went final. Across every SETTLED game, we sum those positives. When the
// running total reaches `unlock_min_trade_usdc`, the redemption flips to
// eligible. "Trade $10 to unlock $10" reads as: $10 worth of positions held
// across one or more games until those games settled — no win required, but
// sells before settlement subtract from the count. Buy-then-immediately-
// sell farming therefore can't unlock the bonus.
//
// Free-bet trades MUST NOT count toward unlocking another free bet, so every
// query includes `beneficiary_address IS NULL`.

import { pool } from "../../db";
import { notifyPromoCreditById } from "../notifications/notify";

export type EvaluateResult =
  | { unlocked: false; reason: string }
  | { unlocked: true; redemptionId: string };

export async function evaluatePromoEligibility(
  redemptionId: string
): Promise<EvaluateResult> {
  const q = await pool.query(
    `
    SELECT
      r.id,
      r.user_address,
      r.referrer_address,
      r.status,
      r.claimed_at,
      r.qualify_by,
      r.referral_invite_id,
      p.id            AS promotion_id,
      p.type          AS promotion_type,
      p.unlock_condition,
      p.unlock_min_trade_usdc,
      p.placement_window_hours,
      p.starts_at     AS promotion_starts_at
    FROM public.promo_redemptions r
    JOIN public.promotions p ON p.id = r.promotion_id
    WHERE r.id = $1
    `,
    [redemptionId]
  );
  const row = q.rows[0];
  if (!row) return { unlocked: false, reason: "redemption_not_found" };
  if (row.status !== "pending_qualification") {
    return { unlocked: false, reason: `status_${row.status}` };
  }

  const unlockCondition = String(row.unlock_condition || "").toLowerCase();
  const minTrade = Number(row.unlock_min_trade_usdc ?? 0);

  // ── mutual_referral_trade ────────────────────────────────────────────────
  // DECOUPLED, per-side. Each side of the pair unlocks on its OWN gate and is
  // flipped on its own — the two sides no longer wait for each other.
  //   - Friend (referee): one fresh qualifying trade of minTrade (held to
  //     settlement), counted from the pair's claimed_at (their join). Flips
  //     only the referee row.
  //   - Referrer:         must RE-TRADE minTrade of their own money AFTER each
  //     referral is accepted, and each minTrade of volume can fund only ONE
  //     friend (FIFO, no reuse). So bringing in N friends needs N×minTrade of
  //     post-referral volume. Flips only that referrer row. This closes the
  //     "trade $20 once, then collect for every friend" loop.
  if (unlockCondition === "mutual_referral_trade") {
    return evaluateMutualReferral(row, minTrade);
  }

  let watchAddress: string;
  if (
    unlockCondition === "first_trade" ||
    unlockCondition === "new_user_first_trade"
  ) {
    // Both conditions watch the user's own wallet for a qualifying BUY.
    // The new-user constraint was already enforced at claim time in
    // redeemPromoCode.ts — at this point the pending redemption just needs
    // the trade-volume gate to flip it to eligible.
    watchAddress = String(row.user_address).toLowerCase();
  } else if (unlockCondition === "referee_first_trade") {
    if (!row.referrer_address) {
      return { unlocked: false, reason: "referee_address_missing" };
    }
    watchAddress = String(row.referrer_address).toLowerCase();
  } else {
    // 'none' / 'referee_signup' / unknown — not a trade-driven path.
    return { unlocked: false, reason: `unsupported_condition_${unlockCondition}` };
  }

  // Cumulative-held-to-settlement qualifier. Rules:
  //   1. Per (game_id, outcome_index): compute bought − sold using
  //      gross_in_dec for BUYs and cost_basis_closed_dec for SELLs. That's
  //      the user's "still held when the game went final" position in
  //      gross-USDC terms.
  //   2. gross_in_dec (NOT net_stake_dec) is the right column for BUYs
  //      because the user thinks of their trade as the dollar amount they
  //      typed. net_stake subtracts the protocol fee, which would mean a
  //      literal "$10" trade only counts as ~$9.93 — a single $10 trade
  //      could never satisfy `unlock_min_trade_usdc = 10`.
  //   3. The pool must be is_final = true. Sells don't satisfy
  //      settlement — only the game settling does. This prevents the
  //      buy-then-immediately-sell farm. (Settled-aware re-evaluation
  //      lives elsewhere; see settleFreeBet / the games settle hook.)
  //   4. Sum (bought − sold), clamped to ≥ 0, across every settled
  //      (game, outcome) the user touched. THAT running total is what
  //      gets compared to unlock_min_trade_usdc.
  //   5. Exclude trades where beneficiary_address IS NOT NULL — those are
  //      free-bet placements paid by the funding wallet, which must never
  //      count toward unlocking ANOTHER free bet (structural guard).
  //   6. Only count trades made AFTER claimed_at — no back-claiming with
  //      pre-existing trade volume.
  const tradeQ = await pool.query(
    `
    WITH per_outcome AS (
      SELECT
        e.game_id,
        e.outcome_index,
        SUM(CASE WHEN e.type = 'BUY'
                 THEN COALESCE(e.gross_in_dec, 0)
                 ELSE 0 END)::numeric AS bought,
        SUM(CASE WHEN e.type = 'SELL'
                 THEN COALESCE(e.cost_basis_closed_dec, 0)
                 ELSE 0 END)::numeric AS sold,
        MIN(e.id) FILTER (WHERE e.type = 'BUY') AS first_buy_id
      FROM public.user_trade_events e
      JOIN public.games g ON lower(g.game_id) = lower(e.game_id)
      WHERE lower(e.user_address) = $1
        AND e.beneficiary_address IS NULL
        AND e.inserted_at        >= $3
        AND g.is_final            = true
      GROUP BY e.game_id, e.outcome_index
    )
    SELECT
      COALESCE(SUM(GREATEST(bought - sold, 0)), 0)::numeric AS cumulative_held,
      MIN(first_buy_id) AS first_buy_id
      FROM per_outcome
    HAVING COALESCE(SUM(GREATEST(bought - sold, 0)), 0) >= $2::numeric
    `,
    [watchAddress, String(minTrade), row.claimed_at]
  );
  const qualifyingTrade = tradeQ.rows[0];
  if (!qualifyingTrade) {
    return { unlocked: false, reason: "cumulative_held_to_settlement_below_threshold" };
  }

  // Promote to eligible and arm the placement window.
  // NB: schema's qualifying_trade_id is uuid but user_trade_events.id is text
  // (e.g. "bet-bet-0x...-3"), so we record the trade ref in event_data
  // instead of the typed column. qualifying_trade_amount_usdc is numeric and
  // does work.
  const upd = await pool.query(
    `
    UPDATE public.promo_redemptions
       SET status                       = 'eligible',
           qualified_at                 = now(),
           expires_at                   = now() + ($1 || ' hours')::interval,
           qualifying_trade_amount_usdc = $2::numeric
     WHERE id = $3
       AND status = 'pending_qualification'
     RETURNING id
    `,
    [
      String(row.placement_window_hours ?? 168),
      qualifyingTrade.cumulative_held,
      redemptionId,
    ]
  );

  if (upd.rowCount === 0) {
    return { unlocked: false, reason: "already_unlocked_concurrently" };
  }

  await pool.query(
    `INSERT INTO public.promo_eligibility_events
       (redemption_id, event_type, event_data)
     VALUES ($1, 'qualified', $2::jsonb)`,
    [
      redemptionId,
      JSON.stringify({
        watchAddress,
        unlockCondition,
        // first_buy_id: the lowest-id BUY trade that contributed to the
        // sum. Kept for audit traceability — useful when debugging which
        // trade window tipped a user over the cumulative threshold.
        firstQualifyingTradeId: qualifyingTrade.first_buy_id,
        cumulativeHeldToSettlementUsdc: String(qualifyingTrade.cumulative_held),
      }),
    ]
  );

  // "You have been credited with $X promo" — the credit is now spendable.
  notifyPromoCreditById(redemptionId).catch(() => {});

  return { unlocked: true, redemptionId };
}

// ── Shared qualifier ─────────────────────────────────────────────────────────
//
// Cumulative held-to-settlement USDC for one wallet since `since`:
//   per (settled game, outcome): GREATEST(sum BUY gross_in_dec − sum SELL
//   cost_basis_closed_dec, 0), summed across all settled buckets. Own-money
//   only (beneficiary_address IS NULL) so free bets never count toward
//   unlocking another free bet. This matches the single-address qualifier used
//   above for first_trade / referee_first_trade.
export async function cumulativeHeldToSettlement(
  address: string,
  since: string | Date
): Promise<number> {
  const q = await pool.query(
    `
    WITH per_outcome AS (
      SELECT
        e.game_id,
        e.outcome_index,
        SUM(CASE WHEN e.type = 'BUY'
                 THEN COALESCE(e.gross_in_dec, 0)
                 ELSE 0 END)::numeric AS bought,
        SUM(CASE WHEN e.type = 'SELL'
                 THEN COALESCE(e.cost_basis_closed_dec, 0)
                 ELSE 0 END)::numeric AS sold
      FROM public.user_trade_events e
      JOIN public.games g ON lower(g.game_id) = lower(e.game_id)
      WHERE lower(e.user_address) = $1
        AND e.beneficiary_address IS NULL
        AND e.inserted_at        >= $2
        AND g.is_final            = true
      GROUP BY e.game_id, e.outcome_index
    )
    SELECT COALESCE(SUM(GREATEST(bought - sold, 0)), 0)::numeric AS held
    FROM per_outcome
    `,
    [String(address).toLowerCase(), since]
  );
  return Number(q.rows[0]?.held ?? 0);
}

// FIFO rank of one referrer-side redemption among THIS referrer's non-void
// referral pairs, ordered by (claimed_at, id). The k-th friend a referrer
// brings in therefore requires k×minTrade of cumulative own-money volume — i.e.
// a fresh minTrade per friend with no reuse. The ordering comparison is done
// entirely in SQL (row-value `<=` against the row's own stored claimed_at/id)
// so we never round-trip a timestamp through JS and miss an exact-tie match.
export async function countReferrerSlotsUpTo(
  inviterAddr: string,
  redemptionId: string
): Promise<number> {
  const q = await pool.query(
    `
    SELECT count(*)::int AS k
    FROM public.promo_redemptions r
    JOIN public.invites i ON i.id = r.referral_invite_id
    JOIN public.users   ua ON ua.id = i.inviter_user_id
    WHERE r.referral_invite_id IS NOT NULL
      AND lower(ua.primary_address) = $1      -- the invite's inviter is this referrer
      AND lower(r.user_address)     = $1      -- and this is the referrer-side row
      AND r.status NOT IN ('expired', 'voided')
      AND (r.claimed_at, r.id) <= (
            SELECT r0.claimed_at, r0.id
              FROM public.promo_redemptions r0
             WHERE r0.id = $2::uuid
          )
    `,
    [String(inviterAddr).toLowerCase(), redemptionId]
  );
  return Number(q.rows[0]?.k ?? 1) || 1;
}

// Promote a single referral redemption row to 'eligible' and arm its placement
// window. Decoupled: flips ONLY this row (by id), never the pair.
async function flipReferralSide(
  row: any,
  side: "referrer" | "referee",
  meta: {
    inviteId: number;
    inviterAddr: string;
    refereeAddr: string;
    heldUsdc: number;
    qualifyingAmount: number;
    rank?: number;
  }
): Promise<EvaluateResult> {
  const upd = await pool.query(
    `
    UPDATE public.promo_redemptions
       SET status                       = 'eligible',
           qualified_at                 = now(),
           expires_at                   = now() + ($1 || ' hours')::interval,
           qualifying_trade_amount_usdc = $2::numeric
     WHERE id = $3
       AND status = 'pending_qualification'
     RETURNING id
    `,
    [String(row.placement_window_hours ?? 168), String(meta.qualifyingAmount), row.id]
  );

  if (upd.rowCount === 0) {
    return { unlocked: false, reason: "already_unlocked_concurrently" };
  }

  await pool.query(
    `INSERT INTO public.promo_eligibility_events
       (redemption_id, event_type, event_data)
     VALUES ($1, 'qualified', $2::jsonb)`,
    [
      row.id,
      JSON.stringify({
        unlockCondition: "mutual_referral_trade",
        side,
        inviteId: meta.inviteId,
        inviterAddress: meta.inviterAddr,
        refereeAddress: meta.refereeAddr,
        heldUsdc: String(meta.heldUsdc),
        ...(meta.rank != null ? { referrerRank: meta.rank } : {}),
      }),
    ]
  );

  // Notify this side that their referral credit is now spendable.
  notifyPromoCreditById(String(row.id)).catch(() => {});

  return { unlocked: true, redemptionId: String(row.id) };
}

// ── mutual_referral_trade evaluator (decoupled, per-side) ────────────────────
async function evaluateMutualReferral(
  row: any,
  minTrade: number
): Promise<EvaluateResult> {
  const inviteId = row.referral_invite_id;
  if (inviteId == null) {
    return { unlocked: false, reason: "referral_invite_missing" };
  }

  // Hard stop on the 30-day mutual-trade deadline. (expirePromoRedemptions
  // also sweeps these, but guard here so a late trade can't sneak an unlock.)
  if (row.qualify_by && new Date(row.qualify_by).getTime() < Date.now()) {
    return { unlocked: false, reason: "qualify_window_passed" };
  }

  // Resolve the canonical roles from the invite: inviter = referrer (A),
  // accepted/redeemed = friend (B). referrer_address on the redemption can be
  // either party depending on which side this row is, so we don't trust it for
  // role assignment — we key off user_address (the beneficiary of THIS row).
  const pairQ = await pool.query(
    `
    SELECT
      lower(ua.primary_address) AS inviter_address,
      lower(ub.primary_address) AS referee_address
    FROM public.invites i
    LEFT JOIN public.users ua ON ua.id = i.inviter_user_id
    LEFT JOIN public.users ub
      ON ub.id = COALESCE(i.redeemed_by_user_id, i.accepted_by_user_id)
    WHERE i.id = $1
    `,
    [Number(inviteId)]
  );
  const pair = pairQ.rows[0];
  const inviterAddr = pair?.inviter_address as string | null;
  const refereeAddr = pair?.referee_address as string | null;
  if (!inviterAddr || !refereeAddr) {
    return { unlocked: false, reason: "referral_pair_unresolved" };
  }

  const thisAddr = String(row.user_address).toLowerCase();

  // ── Referee side: one fresh minTrade since they joined (claimed_at). ──
  if (thisAddr === refereeAddr) {
    const refereeHeld = await cumulativeHeldToSettlement(
      refereeAddr,
      row.claimed_at
    );
    if (refereeHeld < minTrade) {
      return {
        unlocked: false,
        reason: `referee_below_threshold(held=${refereeHeld},min=${minTrade})`,
      };
    }
    return flipReferralSide(row, "referee", {
      inviteId: Number(inviteId),
      inviterAddr,
      refereeAddr,
      heldUsdc: refereeHeld,
      qualifyingAmount: refereeHeld,
    });
  }

  // ── Referrer side: RE-TRADE minTrade per friend, FIFO non-reusable. ──
  if (thisAddr === inviterAddr) {
    // `sinceClaimed`: fresh own-money volume traded AFTER this referral was
    //   accepted — so historical/pre-referral volume can never unlock a friend.
    // `sinceStart`:   total own-money volume since the campaign began — used
    //   only for the non-reuse multiplier so one trade can't unlock many.
    const sinceClaimed = await cumulativeHeldToSettlement(
      inviterAddr,
      row.claimed_at
    );
    const sinceStart = await cumulativeHeldToSettlement(
      inviterAddr,
      row.promotion_starts_at
    );

    // FIFO rank among this referrer's pairs: the k-th friend needs k×minTrade
    // of cumulative volume, so each minTrade funds exactly one friend.
    const k = await countReferrerSlotsUpTo(inviterAddr, String(row.id));
    const needCumulative = k * minTrade;

    // Must have (a) a fresh minTrade since THIS referral AND (b) enough total
    // volume that this friend's $20 isn't reused from an earlier friend's.
    if (sinceClaimed < minTrade || sinceStart < needCumulative) {
      return {
        unlocked: false,
        reason: `referrer_retrade_below_threshold(sinceClaimed=${sinceClaimed},sinceStart=${sinceStart},rank=${k},need=${needCumulative},min=${minTrade})`,
      };
    }

    // The slice of volume attributable to this friend (for display/audit).
    const allocated = Math.max(
      0,
      Math.min(minTrade, sinceStart - (k - 1) * minTrade, sinceClaimed)
    );
    return flipReferralSide(row, "referrer", {
      inviteId: Number(inviteId),
      inviterAddr,
      refereeAddr,
      heldUsdc: sinceClaimed,
      rank: k,
      qualifyingAmount: allocated,
    });
  }

  return { unlocked: false, reason: "redemption_address_not_in_pair" };
}
