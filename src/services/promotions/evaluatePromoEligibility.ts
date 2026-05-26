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

  return { unlocked: true, redemptionId };
}
