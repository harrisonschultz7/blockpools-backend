// src/scripts/backfillReferralRedemptions.ts
//
// Safety-net cron for the referral promo. createReferralRedemptions is called
// inline from the invite accept/redeem endpoints, but at that moment the
// friend's smart wallet (users.primary_address) may not be provisioned yet —
// in which case the inline call no-ops with reason "missing_wallet". This job
// re-runs creation for any accepted/redeemed invite that still has no
// redemption pair, now that the wallet has likely landed.
//
// Idempotent: createReferralRedemptions guards every rule (window, cap,
// stacking, self-referral) and the unique index prevents duplicates, so
// re-running is safe.
//
// Run hourly (or right after the user-sync job):
//   node dist/scripts/backfillReferralRedemptions.js

import "dotenv/config";

import { pool } from "../db";
import { PROMO_FRAMEWORK_ENABLED } from "../config/promo";
import { createReferralRedemptions } from "../services/promotions/createReferralRedemptions";

async function main() {
  if (!PROMO_FRAMEWORK_ENABLED) {
    console.log("[backfillReferralRedemptions] disabled — exiting");
    return;
  }

  // Accepted/redeemed invites from the recent past that have no redemption pair
  // yet. Bounded window keeps the scan cheap; anything older than the campaign
  // window can't qualify anyway.
  const { rows } = await pool.query(
    `
    SELECT i.id
      FROM public.invites i
     WHERE COALESCE(i.redeemed_by_user_id, i.accepted_by_user_id) IS NOT NULL
       AND i.created_at > now() - interval '45 days'
       AND NOT EXISTS (
         SELECT 1 FROM public.promo_redemptions r
          WHERE r.referral_invite_id = i.id
       )
     ORDER BY i.id
     LIMIT 500
    `
  );

  if (!rows.length) {
    console.log("[backfillReferralRedemptions] nothing to backfill");
    return;
  }

  let created = 0;
  let skipped = 0;
  for (const r of rows) {
    const result = await createReferralRedemptions(r.id);
    if (result.created > 0) {
      created += result.created;
      console.log(
        `[backfillReferralRedemptions] invite=${r.id} created=${result.created}`
      );
    } else {
      skipped++;
    }
  }

  console.log(
    `[backfillReferralRedemptions] done — ${created} redemption(s) created across ${rows.length} invite(s), ${skipped} skipped`
  );
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error("[backfillReferralRedemptions] unhandled", err);
    process.exit(1);
  });
