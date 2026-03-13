/**
 * backfill-welcome-emails.ts
 *
 * Sends welcome emails to all users who have an email stored
 * but have never received a welcome email (welcome_email_sent = false).
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   env $(cat /etc/blockpools/backend.env | xargs) npx ts-node scripts/backfill-welcome-emails.ts
 */

import { pool } from "../src/db";
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

async function run() {
  // Atomically claim all unsent rows in one query so if the script is run
  // twice concurrently it won't double-send.
  const { rows } = await pool.query(
    `UPDATE users
       SET welcome_email_sent = true
     WHERE email IS NOT NULL
       AND email != ''
       AND (welcome_email_sent IS NULL OR welcome_email_sent = false)
     RETURNING id, email`
  );

  console.log(`Found ${rows.length} users to send welcome emails to`);

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
        from: process.env.RESEND_FROM_EMAIL || "BlockPools <welcome@mail.blockpools.io>",
        to: user.email,
        subject: "Welcome to BlockPools",
        template: {
          id: "2a86d254-f493-45d1-abda-706fd33f1479",
        },
      } as any);

      const resultAny = result as any;

      if (resultAny?.error || !resultAny?.data?.id) {
        console.error(`[FAILED] ${user.email} — Resend error: ${JSON.stringify(resultAny?.error)}`);
        // Roll back the flag so a future run retries this user
        await pool.query(
          `UPDATE users SET welcome_email_sent = false WHERE id = $1`,
          [user.id]
        );
        failed++;
      } else {
        console.log(`[SENT] ${user.email}`);
        sent++;
      }

      // Small delay to stay within Resend rate limits
      await new Promise((r) => setTimeout(r, 300));
    } catch (err: any) {
      console.error(`[ERROR] ${user.email} — ${err?.message || err}`);
      // Roll back the flag so a future run retries this user
      await pool.query(
        `UPDATE users SET welcome_email_sent = false WHERE id = $1`,
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