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

import { pool } from "../db";
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

// Template ids per language (mirrors src/routes/profile.ts). Spanish keeps the
// existing hardcoded id as default; English comes from RESEND_WELCOME_TEMPLATE_EN.
const WELCOME_TEMPLATE_ES =
  (process.env.RESEND_WELCOME_TEMPLATE_ES || "").trim() ||
  "2a86d254-f493-45d1-abda-706fd33f1479";
const WELCOME_TEMPLATE_EN =
  (process.env.RESEND_WELCOME_TEMPLATE_EN || "").trim() ||
  "120a6317-8e49-4388-a8be-290ecb9abf8e";

function pickTemplate(preferredLocale: string | null): { id: string; subject: string } {
  const tag = (preferredLocale || "").trim().toLowerCase();
  const isEnglish = tag !== "" && !tag.startsWith("es");
  if (isEnglish && WELCOME_TEMPLATE_EN) {
    return { id: WELCOME_TEMPLATE_EN, subject: "Welcome to BlockPools" };
  }
  return { id: WELCOME_TEMPLATE_ES, subject: "Bienvenido a BlockPools" };
}

async function run() {
  // Atomically claim all unsent rows in one query so if the script is run
  // twice concurrently it won't double-send.
  const { rows } = await pool.query(
    `UPDATE users
       SET welcome_email_sent = true
     WHERE email IS NOT NULL
       AND email != ''
       AND (welcome_email_sent IS NULL OR welcome_email_sent = false)
     RETURNING id, email, preferred_locale`
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
      const tpl = pickTemplate(user.preferred_locale);
      const result = await resend.emails.send({
        from: process.env.RESEND_FROM_EMAIL || "BlockPools <welcome@mail.blockpools.io>",
        to: user.email,
        subject: tpl.subject,
        template: {
          id: tpl.id,
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

      // Stay within Resend's 2 req/sec rate limit
      await new Promise((r) => setTimeout(r, 700));
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