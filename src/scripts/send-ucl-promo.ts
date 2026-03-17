/**
 * send-ucl-promo.ts
 *
 * Sends the Champions League Trading Bonus promo email to all users
 * with an email address.
 * Subject and preview text are pulled directly from the Resend template.
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-ucl-promo.ts
 */
import { pool } from "../db";
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

// ── Resend template ID ───────────────────────────────────────────────────────
// Find it in the URL: resend.com/templates/YOUR_ID_HERE/editor
const TEMPLATE_ID = "4a929977-2a9c-4981-b14d-7bb7fcba6411";
// ─────────────────────────────────────────────────────────────────────────────

async function run() {
  const { rows } = await pool.query(
    `SELECT id, email
     FROM users
     WHERE email IS NOT NULL
       AND email != ''
       AND email = 'harrisonschultz1240@gmail.com'
     ORDER BY created_at ASC`
  );

  console.log(`Found ${rows.length} users to send UCL promo to`);

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
        from: process.env.RESEND_FROM_EMAIL || "Harrison at BlockPools <harrison@mail.blockpools.io>",
        to: user.email,
        template: {
          id: TEMPLATE_ID,
        },
      } as any);

      const resultAny = result as any;

      if (resultAny?.error || !resultAny?.data?.id) {
        console.error(`[FAILED] ${user.email} — ${JSON.stringify(resultAny?.error)}`);
        failed++;
      } else {
        console.log(`[SENT] ${user.email}`);
        sent++;
      }

      // Stay within Resend rate limit
      await new Promise((r) => setTimeout(r, 700));
    } catch (err: any) {
      console.error(`[ERROR] ${user.email} — ${err?.message || err}`);
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