// src/services/promotions/redeemPromoCode.ts
//
// Claim flow for the new promo framework. Race-safe via SELECT ... FOR UPDATE
// on the promotion row, so simultaneous claims at the cap behave correctly.
//
// Returns the redemption id and the resulting status:
//   - 'eligible'              → no unlock condition; user can place immediately
//   - 'pending_qualification' → unlock requires a future event (trade or signup)
//
// Errors are thrown with stable .code values so the router can map to HTTP.

import { pool } from "../../db";

export type RedeemError =
  | "PROMO_NOT_FOUND"
  | "PROMO_INACTIVE"
  | "PROMO_EXPIRED"
  | "PROMO_NOT_STARTED"
  | "PROMO_EXHAUSTED"
  | "ALREADY_REDEEMED"
  | "INVALID_ADDRESS"
  | "REFERRER_REQUIRED"
  | "NOT_NEW_USER"
  | "ALREADY_HAS_REFERRAL_BONUS";

export class RedeemPromoError extends Error {
  code: RedeemError;
  constructor(code: RedeemError, message?: string) {
    super(message || code);
    this.code = code;
  }
}

export type RedeemPromoCodeInput = {
  code: string;
  userAddress: string;
  // Optional referrer / referee wallet — only meaningful for referral promos.
  referrerAddress?: string | null;
};

export type RedeemPromoCodeResult = {
  redemptionId: string;
  promotionId: string;
  promotionType: string;
  status: "eligible" | "pending_qualification";
  expiresAt: string | null;
  creditUsdc: string;
};

const ADDR_RE = /^0x[a-f0-9]{40}$/;

export async function redeemPromoCode(
  input: RedeemPromoCodeInput
): Promise<RedeemPromoCodeResult> {
  const code = String(input.code || "").trim().toUpperCase();
  const userAddress = String(input.userAddress || "").trim().toLowerCase();
  const referrerAddress = input.referrerAddress
    ? String(input.referrerAddress).trim().toLowerCase()
    : null;

  if (!ADDR_RE.test(userAddress)) {
    throw new RedeemPromoError("INVALID_ADDRESS");
  }
  if (!code) {
    throw new RedeemPromoError("PROMO_NOT_FOUND");
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Lock the promotion row for the duration of this transaction so the
    // counter check, increment, and insert are atomic with respect to other
    // concurrent claimers.
    const promoQ = await client.query(
      `SELECT
         id,
         type,
         active,
         credit_usdc,
         max_claims_total,
         max_claims_per_user,
         is_repeatable,
         starts_at,
         expires_at,
         placement_window_hours,
         unlock_condition,
         total_claimed,
         funding_wallet_address
       FROM public.promotions
       WHERE upper(code) = $1
       FOR UPDATE`,
      [code]
    );

    const promo = promoQ.rows[0];
    if (!promo) {
      await client.query("ROLLBACK");
      throw new RedeemPromoError("PROMO_NOT_FOUND");
    }
    if (!promo.active) {
      await client.query("ROLLBACK");
      throw new RedeemPromoError("PROMO_INACTIVE");
    }

    const now = new Date();
    if (promo.starts_at && new Date(promo.starts_at) > now) {
      await client.query("ROLLBACK");
      throw new RedeemPromoError("PROMO_NOT_STARTED");
    }
    if (promo.expires_at && new Date(promo.expires_at) < now) {
      await client.query("ROLLBACK");
      throw new RedeemPromoError("PROMO_EXPIRED");
    }

    // Total-claims cap. Use the promotion's own counter (incremented below).
    if (
      promo.max_claims_total != null &&
      Number(promo.total_claimed) >= Number(promo.max_claims_total)
    ) {
      await client.query("ROLLBACK");
      throw new RedeemPromoError("PROMO_EXHAUSTED");
    }

    // Per-user cap. The promo_redemptions_unique_per_user_idx partial unique
    // index is the structural defense; this check just turns a 23505 into a
    // clean 409. is_repeatable=true ignores the per-user cap.
    if (!promo.is_repeatable) {
      const perUserCap = Number(promo.max_claims_per_user ?? 1);
      const userQ = await client.query(
        `SELECT count(*)::int AS n
           FROM public.promo_redemptions
           WHERE promotion_id = $1
             AND lower(user_address) = $2
             AND status NOT IN ('expired','voided')`,
        [promo.id, userAddress]
      );
      if ((userQ.rows[0]?.n ?? 0) >= perUserCap) {
        await client.query("ROLLBACK");
        throw new RedeemPromoError("ALREADY_REDEEMED");
      }
    }

    const unlockCondition = String(promo.unlock_condition || "none").toLowerCase();

    // ── 'new_user' / 'new_user_first_trade' gate ───────────────────────────
    // Only allow this redemption if the user's profile was created AFTER the
    // promo's starts_at. Applies to both:
    //   - 'new_user'             → immediate eligibility once gate passes
    //   - 'new_user_first_trade' → defers to 'first_trade' qualification
    //                              (bet-to-unlock for new users)
    if (
      unlockCondition === "new_user" ||
      unlockCondition === "new_user_first_trade"
    ) {
      // No-stacking guard: a user who already holds a referral redemption
      // cannot also claim the one-time signup bonus. First bonus wins.
      const refQ = await client.query(
        `SELECT 1
           FROM public.promo_redemptions
          WHERE lower(user_address) = $1
            AND referral_invite_id IS NOT NULL
            AND status NOT IN ('expired', 'voided')
          LIMIT 1`,
        [userAddress]
      );
      if ((refQ.rowCount ?? 0) > 0) {
        await client.query("ROLLBACK");
        throw new RedeemPromoError("ALREADY_HAS_REFERRAL_BONUS");
      }

      const userQ = await client.query(
        `SELECT created_at
           FROM public.users
          WHERE LOWER(primary_address) = $1
          LIMIT 1`,
        [userAddress]
      );
      const userRow = userQ.rows[0];
      const userCreatedAt = userRow?.created_at
        ? new Date(userRow.created_at)
        : null;
      const promoStartsAt = promo.starts_at ? new Date(promo.starts_at) : null;

      // Reject if:
      //   - no profile exists for this wallet (must sign up first), OR
      //   - profile predates the promo's launch
      const promoStartMs = promoStartsAt ? promoStartsAt.getTime() : 0;
      const userMs = userCreatedAt ? userCreatedAt.getTime() : 0;
      if (!userCreatedAt || userMs < promoStartMs) {
        await client.query("ROLLBACK");
        throw new RedeemPromoError("NOT_NEW_USER");
      }
    }

    // Decide whether the redemption should be eligible immediately or wait for
    // a qualifying event. Four supported conditions:
    //   - 'none'                 → eligible immediately
    //   - 'first_trade'          → wait for the user's own real-money BUY
    //   - 'referee_first_trade'  → wait for the referee (referrer_address)
    //                              to do a real-money BUY
    //   - 'referee_signup'       → eligible if the referee already has a user
    //                              record; else pending until they sign up
    //   - 'new_user'             → handled above; falls through to immediate
    //                              eligibility (same path as 'none').
    let goesEligibleImmediately = false;

    if (unlockCondition === "none" || unlockCondition === "new_user") {
      goesEligibleImmediately = true;
    } else if (unlockCondition === "new_user_first_trade") {
      // Hybrid: new-user check already passed above. Now defer to the
      // first_trade path so the credit only unlocks after the user makes a
      // qualifying real-money BUY (≥ unlock_min_trade_usdc).
      goesEligibleImmediately = false;
    } else if (unlockCondition === "referee_signup") {
      if (!referrerAddress) {
        await client.query("ROLLBACK");
        throw new RedeemPromoError("REFERRER_REQUIRED");
      }
      const refereeQ = await client.query(
        `SELECT 1
           FROM public.users
           WHERE lower(primary_address) = $1
              OR lower(eoa_address)     = $1
           LIMIT 1`,
        [referrerAddress]
      );
      goesEligibleImmediately = (refereeQ.rowCount ?? 0) > 0;
    } else if (unlockCondition === "first_trade") {
      // Defer until the user's first qualifying real-money BUY.
    } else if (unlockCondition === "referee_first_trade") {
      if (!referrerAddress) {
        await client.query("ROLLBACK");
        throw new RedeemPromoError("REFERRER_REQUIRED");
      }
    } else {
      // Defensive: unknown condition → pending. Won't auto-unlock.
      console.warn("[redeemPromoCode] unknown unlock_condition:", unlockCondition);
    }

    // Build the insert dynamically because expires_at is computed inline only
    // on the eligible path.
    let insertSql: string;
    let insertArgs: any[];
    if (goesEligibleImmediately) {
      insertSql = `
        INSERT INTO public.promo_redemptions
          (promotion_id, user_address, referrer_address,
           status, credit_usdc, claimed_at, qualified_at, expires_at,
           is_repeatable)
        VALUES
          ($1, $2, $3, 'eligible', $4,
           now(), now(), now() + ($5 || ' hours')::interval,
           $6)
        RETURNING id, expires_at
      `;
      insertArgs = [
        promo.id,
        userAddress,
        referrerAddress,
        String(promo.credit_usdc),
        String(promo.placement_window_hours ?? 168),
        Boolean(promo.is_repeatable),
      ];
    } else {
      insertSql = `
        INSERT INTO public.promo_redemptions
          (promotion_id, user_address, referrer_address,
           status, credit_usdc, claimed_at,
           is_repeatable)
        VALUES
          ($1, $2, $3, 'pending_qualification', $4, now(),
           $5)
        RETURNING id, expires_at
      `;
      insertArgs = [
        promo.id,
        userAddress,
        referrerAddress,
        String(promo.credit_usdc),
        Boolean(promo.is_repeatable),
      ];
    }

    let redemptionId: string;
    let expiresAtIso: string | null;
    try {
      const ins = await client.query(insertSql, insertArgs);
      redemptionId = ins.rows[0].id;
      expiresAtIso = ins.rows[0].expires_at
        ? new Date(ins.rows[0].expires_at).toISOString()
        : null;
    } catch (err: any) {
      // 23505 = unique violation on promo_redemptions_unique_per_user_idx.
      if (String(err?.code) === "23505") {
        await client.query("ROLLBACK");
        throw new RedeemPromoError("ALREADY_REDEEMED");
      }
      throw err;
    }

    // Increment the campaign's total_claimed counter (lock held above).
    await client.query(
      `UPDATE public.promotions
          SET total_claimed = total_claimed + 1,
              updated_at    = now()
        WHERE id = $1`,
      [promo.id]
    );

    // Audit log entry — append-only.
    await client.query(
      `INSERT INTO public.promo_eligibility_events
         (redemption_id, event_type, event_data)
       VALUES ($1, 'claim', $2::jsonb)`,
      [
        redemptionId,
        JSON.stringify({
          code,
          userAddress,
          referrerAddress,
          unlockCondition,
          status: goesEligibleImmediately ? "eligible" : "pending_qualification",
        }),
      ]
    );

    await client.query("COMMIT");

    return {
      redemptionId,
      promotionId: promo.id,
      promotionType: promo.type,
      status: goesEligibleImmediately ? "eligible" : "pending_qualification",
      expiresAt: expiresAtIso,
      creditUsdc: String(promo.credit_usdc),
    };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw err;
  } finally {
    client.release();
  }
}
