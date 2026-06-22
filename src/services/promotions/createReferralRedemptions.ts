// src/services/promotions/createReferralRedemptions.ts
//
// Creates the two pending redemptions for a referral pair (referrer A +
// invited friend B) once the friend has joined. Idempotent and non-throwing:
// it's called fire-and-forget from the invite accept/redeem endpoints and from
// the backfill cron, so a failure here must never block those flows.
//
// What it enforces (all of the product rules live here, NOT in SQL):
//   - Both parties must resolve to a real wallet (users.primary_address).
//   - No self-referral (same user id OR same wallet address).
//   - The referral campaign must be active and inside its window — i.e. the
//     friend must have joined while the campaign is live.
//   - No bonus stacking: if the friend already holds a signup/new-user
//     redemption, the pair is skipped (first bonus wins).
//   - Per-referrer cap of 10 qualifying friends. Past the cap, the friend (B)
//     still gets their redemption; only the referrer's (A) is skipped.
//   - 30-day mutual-trade deadline stamped on qualify_by.
//
// The two redemptions go in as 'pending_qualification'. They flip to
// 'eligible' later via evaluatePromoEligibility's mutual_referral_trade branch,
// which is driven by the existing persistTrades hook.

import { pool } from "../../db";
import { PROMO_FRAMEWORK_ENABLED } from "../../config/promo";

// Per-referrer cap on how many qualifying friends earn the referrer a bonus.
export const REFERRAL_MAX_PER_REFERRER = 10;

// Days both sides have to each hit the trade threshold before the pair expires.
export const REFERRAL_QUALIFY_WINDOW_DAYS = 30;

export type CreateReferralResult = {
  created: number;
  reason?: string;
  refereeRedemptionId?: string;
  referrerRedemptionId?: string;
};

const ADDR_RE = /^0x[a-f0-9]{40}$/;

export async function createReferralRedemptions(
  inviteId: number | string
): Promise<CreateReferralResult> {
  if (!PROMO_FRAMEWORK_ENABLED) return { created: 0, reason: "framework_disabled" };

  const id = Number(inviteId);
  if (!Number.isFinite(id)) return { created: 0, reason: "bad_invite_id" };

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // 1. Invite + both wallet addresses in one shot. The friend ("referee") is
    //    whoever redeemed or accepted the invite.
    const inv = await client.query(
      `
      SELECT
        i.inviter_user_id,
        COALESCE(i.redeemed_by_user_id, i.accepted_by_user_id) AS referee_user_id,
        lower(ua.primary_address) AS inviter_address,
        lower(ub.primary_address) AS referee_address
      FROM public.invites i
      LEFT JOIN public.users ua ON ua.id = i.inviter_user_id
      LEFT JOIN public.users ub
        ON ub.id = COALESCE(i.redeemed_by_user_id, i.accepted_by_user_id)
      WHERE i.id = $1
      `,
      [id]
    );
    const row = inv.rows[0];
    if (!row) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "invite_not_found" };
    }

    const inviterUserId = row.inviter_user_id as string | null;
    const refereeUserId = row.referee_user_id as string | null;
    const inviterAddr = (row.inviter_address as string | null) || "";
    const refereeAddr = (row.referee_address as string | null) || "";

    // 2. Friend hasn't joined yet — nothing to do (backfill will retry).
    if (!refereeUserId) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "no_referee_yet" };
    }

    // 3. Self-referral guard (by user id and by wallet).
    if (inviterUserId && refereeUserId && inviterUserId === refereeUserId) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "self_referral_user" };
    }

    // 4. Both must resolve to a real wallet. If not, backfill retries later.
    if (!ADDR_RE.test(inviterAddr) || !ADDR_RE.test(refereeAddr)) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "missing_wallet" };
    }
    if (inviterAddr === refereeAddr) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "self_referral_wallet" };
    }

    // 5. Active referral campaign inside its window. If none, the friend joined
    //    outside the campaign — no bonus.
    const camp = await client.query(
      `
      SELECT id, credit_usdc, is_repeatable
        FROM public.promotions
       WHERE type = 'referral'
         AND unlock_condition = 'mutual_referral_trade'
         AND active = true
         AND starts_at <= now()
         AND (expires_at IS NULL OR expires_at > now())
       ORDER BY created_at DESC
       LIMIT 1
      `
    );
    const campaign = camp.rows[0];
    if (!campaign) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "no_active_referral_campaign" };
    }

    // 6. Anti-stacking: if the friend already holds a signup/new-user bonus,
    //    skip the whole pair (first bonus wins).
    const stack = await client.query(
      `
      SELECT 1
        FROM public.promo_redemptions r
        JOIN public.promotions p ON p.id = r.promotion_id
       WHERE lower(r.user_address) = $1
         AND p.unlock_condition IN ('new_user', 'new_user_first_trade')
         AND r.status NOT IN ('expired', 'voided')
       LIMIT 1
      `,
      [refereeAddr]
    );
    if ((stack.rowCount ?? 0) > 0) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "referee_has_signup_bonus" };
    }

    // 6b. Anti-circular: no DUPLICATE PAIR. A single invite already pays BOTH
    //     sides $20, so a second referral between the same two wallets (in
    //     either direction, via any other invite) is pure double-dipping — the
    //     literal signature of the closed-loop ring (A "refers" B, then B
    //     "refers" A, or the same two keep pairing). Genuine new referrals are
    //     between two wallets that haven't been paired before, so this never
    //     touches a legit first-time referral.
    const dupPair = await client.query(
      `
      SELECT 1
        FROM public.promo_redemptions r
       WHERE r.referral_invite_id IS NOT NULL
         AND r.referral_invite_id <> $1
         AND r.status NOT IN ('expired', 'voided')
         AND (
              (lower(r.user_address) = $2 AND lower(r.referrer_address) = $3)
           OR (lower(r.user_address) = $3 AND lower(r.referrer_address) = $2)
         )
       LIMIT 1
      `,
      [id, refereeAddr, inviterAddr]
    );
    if ((dupPair.rowCount ?? 0) > 0) {
      await client.query("ROLLBACK");
      return { created: 0, reason: "duplicate_referral_pair" };
    }

    // 6c. Anti-circular: REFEREE-ONCE. The "friend" side is for genuinely NEW
    //     users, and you're only new once — so a user may be the REDEEMER of at
    //     most one invite. We read this straight from the invites table (the
    //     authoritative role source: the redeemer IS the new friend), which
    //     avoids the referee/referrer ambiguity of the redemption rows. This
    //     blocks one account being "referred" by several ring members (exactly
    //     what let Matu66 redeem both his own invite AND lolita's). It does NOT
    //     cap how many DISTINCT friends a REFERRER brings in (inviter_user_id,
    //     not redeemed_by — the 10x stays intact); it only stops the same
    //     account from being the new-friend repeatedly.
    if (refereeUserId) {
      const redeemedBefore = await client.query(
        `
        SELECT 1
          FROM public.invites
         WHERE id <> $1
           AND COALESCE(redeemed_by_user_id, accepted_by_user_id) = $2
         LIMIT 1
        `,
        [id, refereeUserId]
      );
      if ((redeemedBefore.rowCount ?? 0) > 0) {
        await client.query("ROLLBACK");
        return { created: 0, reason: "referee_already_referred" };
      }
    }

    const creditUsdc = String(campaign.credit_usdc);
    const isRepeatable = Boolean(campaign.is_repeatable);

    // Helper: insert one side's redemption if it doesn't already exist for this
    // invite. The partial unique index is the structural backstop; the
    // pre-check keeps the happy path clean.
    async function insertSide(
      beneficiary: string,
      counterparty: string
    ): Promise<string | null> {
      const exists = await client.query(
        `SELECT id FROM public.promo_redemptions
          WHERE referral_invite_id = $1 AND lower(user_address) = $2
          LIMIT 1`,
        [id, beneficiary]
      );
      if (exists.rows[0]) return exists.rows[0].id; // already created

      try {
        const ins = await client.query(
          `
          INSERT INTO public.promo_redemptions
            (promotion_id, user_address, referrer_address, status, credit_usdc,
             claimed_at, qualify_by, referral_invite_id, is_repeatable)
          VALUES
            ($1, $2, $3, 'pending_qualification', $4,
             now(), now() + ($5 || ' days')::interval, $6, $7)
          RETURNING id
          `,
          [
            campaign.id,
            beneficiary,
            counterparty,
            creditUsdc,
            String(REFERRAL_QUALIFY_WINDOW_DAYS),
            id,
            isRepeatable,
          ]
        );
        const newId = ins.rows[0].id as string;
        await client.query(
          `INSERT INTO public.promo_eligibility_events
             (redemption_id, event_type, event_data)
           VALUES ($1, 'claim', $2::jsonb)`,
          [
            newId,
            JSON.stringify({
              source: "referral_pair",
              inviteId: id,
              beneficiary,
              counterparty,
              unlockCondition: "mutual_referral_trade",
            }),
          ]
        );
        return newId;
      } catch (err: any) {
        // 23505 = lost a race on the unique index. Treat as already-created.
        if (String(err?.code) === "23505") return null;
        throw err;
      }
    }

    // 7. Friend (B) — always gets a redemption (subject to anti-stacking above).
    const refereeRedemptionId = await insertSide(refereeAddr, inviterAddr);

    // 8. Referrer (A) — only under the cap. Count distinct invites A has been
    //    credited on (one redemption per invite via the unique index).
    let referrerRedemptionId: string | null = null;
    const capQ = await client.query(
      `
      SELECT count(*)::int AS n
        FROM public.promo_redemptions
       WHERE lower(user_address) = $1
         AND referral_invite_id IS NOT NULL
         AND status NOT IN ('expired', 'voided')
      `,
      [inviterAddr]
    );
    const referrerPairs = capQ.rows[0]?.n ?? 0;
    if (referrerPairs < REFERRAL_MAX_PER_REFERRER) {
      referrerRedemptionId = await insertSide(inviterAddr, refereeAddr);
    }

    await client.query("COMMIT");

    const created =
      (refereeRedemptionId ? 1 : 0) + (referrerRedemptionId ? 1 : 0);
    return {
      created,
      refereeRedemptionId: refereeRedemptionId ?? undefined,
      referrerRedemptionId: referrerRedemptionId ?? undefined,
      reason:
        referrerPairs >= REFERRAL_MAX_PER_REFERRER
          ? "referrer_at_cap_referee_only"
          : undefined,
    };
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("[createReferralRedemptions] failed (non-blocking)", err);
    return { created: 0, reason: "error" };
  } finally {
    client.release();
  }
}
