// src/services/promotions/evaluatePromoEligibility.ts
//
// Decides whether a 'pending_qualification' redemption can be transitioned to
// 'eligible'. Called from handlePromoTradeAttribution after a real-money BUY
// trade is persisted for a user.
//
// Trade-driven unlock conditions handled here:
//   - 'first_trade'          → user themselves places a real-money BUY
//   - 'referee_first_trade'  → the referee's wallet (stored on
//                              referrer_address) places a real-money BUY
//
// 'referee_signup' and 'none' are resolved at claim time, not here.
//
// Free-bet trades MUST NOT count toward unlocking another free bet, so every
// query includes `beneficiary_address IS NULL`.

import { pool } from "../../db";

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
      p.id            AS promotion_id,
      p.type          AS promotion_type,
      p.unlock_condition,
      p.unlock_min_trade_usdc,
      p.placement_window_hours
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

  let watchAddress: string;
  if (unlockCondition === "first_trade") {
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

  // Look for a qualifying real-money BUY since the claim. The
  // beneficiary_address IS NULL filter is the structural guard against using
  // a free-bet trade to unlock another free bet.
  const tradeQ = await pool.query(
    `
    SELECT id, net_stake_dec
    FROM public.user_trade_events
    WHERE lower(user_address) = $1
      AND type = 'BUY'
      AND beneficiary_address IS NULL
      AND COALESCE(net_stake_dec, 0) >= $2::numeric
      AND inserted_at >= $3
    ORDER BY inserted_at ASC
    LIMIT 1
    `,
    [watchAddress, String(minTrade), row.claimed_at]
  );
  const qualifyingTrade = tradeQ.rows[0];
  if (!qualifyingTrade) {
    return { unlocked: false, reason: "no_qualifying_trade_yet" };
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
      qualifyingTrade.net_stake_dec,
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
        qualifyingTradeId: qualifyingTrade.id,
        qualifyingTradeAmountUsdc: String(qualifyingTrade.net_stake_dec),
      }),
    ]
  );

  return { unlocked: true, redemptionId };
}
