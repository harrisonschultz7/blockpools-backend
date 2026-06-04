// src/scripts/expirePromoRedemptions.ts
//
// Hourly cron. Marks any 'eligible' redemption whose placement window has
// passed as 'expired'. No on-chain action.
//
// Run:
//   node dist/scripts/expirePromoRedemptions.js
//
// systemd timer / cron should fire this once an hour.

import "dotenv/config";

import { pool } from "../db";
import { PROMO_FRAMEWORK_ENABLED } from "../config/promo";

async function main() {
  if (!PROMO_FRAMEWORK_ENABLED) {
    console.log("[expirePromoRedemptions] disabled — exiting");
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const upd = await client.query(
      `
      UPDATE public.promo_redemptions
         SET status = 'expired'
       WHERE status = 'eligible'
         AND expires_at IS NOT NULL
         AND expires_at < now()
       RETURNING id
      `
    );

    if (upd.rowCount && upd.rowCount > 0) {
      const ids = upd.rows.map((r: any) => r.id);
      await client.query(
        `
        INSERT INTO public.promo_eligibility_events
          (redemption_id, event_type, event_data)
        SELECT
          unnest($1::uuid[]),
          'expired',
          '{}'::jsonb
        `,
        [ids]
      );
      console.log(`[expirePromoRedemptions] expired ${ids.length} redemption(s)`);
    } else {
      console.log("[expirePromoRedemptions] nothing to expire");
    }

    // Referral pairs that never both-qualified within the 30-day window. These
    // sit in 'pending_qualification' with a qualify_by deadline; once it passes
    // the pair can no longer unlock, so void it.
    const qexp = await client.query(
      `
      UPDATE public.promo_redemptions
         SET status = 'expired'
       WHERE status = 'pending_qualification'
         AND qualify_by IS NOT NULL
         AND qualify_by < now()
       RETURNING id
      `
    );
    if (qexp.rowCount && qexp.rowCount > 0) {
      const qids = qexp.rows.map((r: any) => r.id);
      await client.query(
        `
        INSERT INTO public.promo_eligibility_events
          (redemption_id, event_type, event_data)
        SELECT unnest($1::uuid[]), 'expired',
               '{"reason":"qualify_window_passed"}'::jsonb
        `,
        [qids]
      );
      console.log(
        `[expirePromoRedemptions] expired ${qids.length} referral pending redemption(s) past qualify_by`
      );
    }

    await client.query("COMMIT");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error("[expirePromoRedemptions] failed", err);
    process.exitCode = 1;
  } finally {
    client.release();
  }
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error("[expirePromoRedemptions] unhandled", err);
    process.exit(1);
  });
