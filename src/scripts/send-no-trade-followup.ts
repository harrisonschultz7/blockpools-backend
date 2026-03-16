/**
 * send-no-trade-followup.ts
 *
 * One-time send of the re-engagement email to all users who signed up
 * but have never made a trade.
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-no-trade-followup.ts
 */
import { pool } from "../db";
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

async function run() {
  // Atomically claim all eligible rows so if the script is run
  // twice concurrently it won't double-send.
  const { rows } = await pool.query(
    `UPDATE users
       SET followup_email_sent = true
     WHERE has_traded = false
       AND (followup_email_sent IS NULL OR followup_email_sent = false)
       AND email IS NOT NULL
       AND email != ''
       AND welcome_email_sent = true
     RETURNING id, email`
  );

  console.log(`Found ${rows.length} users to send follow-up emails to`);

  if (rows.length === 0) {
    console.log("Nothing to do.");
    await pool.end();
    process.exit(0);
  }

  let sent = 0;
  let failed = 0;

  for (const user of rows) {
    try {
      const result = await resend.emails.send({
        from: process.env.RESEND_FROM_EMAIL || "Harrison at BlockPools <info@mail.blockpools.io>",
        to: user.email,
        subject: "Everyone's trading. You haven't yet.",
        template: {
          id: "f304fffd-a8c3-4bfe-b208-a0d3dde0a663",
        },
      } as any);

      const resultAny = result as any;

      if (resultAny?.error || !resultAny?.data?.id) {
        console.error(`[FAILED] ${user.email} — Resend error: ${JSON.stringify(resultAny?.error)}`);
        // Roll back the flag so a future run retries this user
        await pool.query(
          `UPDATE users SET followup_email_sent = false WHERE id = $1`,
          [user.id]
        );
        failed++;
      } else {
        console.log(`[SENT] ${user.email}`);
        sent++;
      }

      // Stay within Resend's rate limit
      await new Promise((r) => setTimeout(r, 700));
    } catch (err: any) {
      console.error(`[ERROR] ${user.email} — ${err?.message || err}`);
      // Roll back the flag so a future run retries this user
      await pool.query(
        `UPDATE users SET followup_email_sent = false WHERE id = $1`,
        [user.id]
      ).catch(() => {});
      failed++;
    }
  }

  console.log(`\nDone. Sent: ${sent}, Failed: ${failed}`);
  await pool.end();
  process.exit(0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});