// src/utils/updatePromoProgress.ts
import { pool } from "../db";

/**
 * Recomputes promo_trade_accumulated for a promo-locked user from net
 * principal still at risk:
 *   SUM(BUY gross_in_dec) - SUM(SELL cost_basis_closed_dec)
 *
 * This prevents buy-then-sell round trips from counting toward promo unlock.
 *
 * Safe to call for any user — the WHERE guard makes it a no-op for users
 * who are not promo-locked or who have already unlocked.
 */
export async function updatePromoProgress(userAddress: string): Promise<void> {
  if (!userAddress) return;

  await pool.query(
    `
    WITH latest_redemption AS (
      SELECT COALESCE(MAX(redeemed_at), MAX(inserted_at)) AS redeemed_at
      FROM public.promo_redemptions
      WHERE user_address = $1
    ),
    net_open AS (
      SELECT GREATEST(
        COALESCE(
          SUM(
            CASE
              WHEN type = 'BUY' THEN COALESCE(gross_in_dec, 0)
              WHEN type = 'SELL' THEN -COALESCE(cost_basis_closed_dec, 0)
              ELSE 0
            END
          ),
          0
        ),
        0
      )::numeric AS principal_at_risk
      FROM public.user_trade_events
      WHERE user_address = $1
        AND (
          (SELECT redeemed_at FROM latest_redemption) IS NULL
          OR inserted_at >= (SELECT redeemed_at FROM latest_redemption)
        )
    )
    UPDATE public.users
    SET
      promo_trade_accumulated = LEAST(
        promo_trade_required,
        (SELECT principal_at_risk FROM net_open)
      ),
      promo_locked = CASE
        WHEN (SELECT principal_at_risk FROM net_open) >= promo_trade_required THEN false
        ELSE true
      END
    WHERE
      primary_address = $1
      AND promo_trade_required > 0
    `,
    [userAddress.toLowerCase()]
  );
}