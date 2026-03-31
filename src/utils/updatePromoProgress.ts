// src/utils/updatePromoProgress.ts
import { pool } from "../db";

/**
 * Increments promo_trade_accumulated for a promo-locked user by the given
 * BUY volume, and atomically unlocks withdrawals when the threshold is met.
 *
 * Safe to call for any user — the WHERE guard makes it a no-op for users
 * who are not promo-locked or who have already unlocked.
 */
export async function updatePromoProgress(
  userAddress: string,
  buyVolume: number
): Promise<void> {
  if (!userAddress || buyVolume <= 0) return;

  await pool.query(
    `
    UPDATE public.users
    SET
      promo_trade_accumulated = promo_trade_accumulated + $2,
      promo_locked = CASE
        WHEN (promo_trade_accumulated + $2) >= promo_trade_required THEN false
        ELSE promo_locked
      END
    WHERE
      primary_address = $1
      AND promo_locked = true
      AND promo_trade_accumulated < promo_trade_required
    `,
    [userAddress.toLowerCase(), buyVolume]
  );
}