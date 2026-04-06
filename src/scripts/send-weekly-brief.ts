/**
 * send-weekly-brief.ts
 *
 * DRY RUN — sending to info@blockpools.io only.
 * Swap RECIPIENTS back to getRecipients() for full send.
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-weekly-brief.ts
 */
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

const TEMPLATE_ID = "93f918e5-06d4-4ec9-b29c-5c24e31a8425";

const RECIPIENTS = ["info@blockpools.io"];

const DELAY_MS = 700;

async function run() {
  console.log(`Sending to ${RECIPIENTS.length} recipient(s)...\n`);

  let sent = 0;
  let failed = 0;

  for (const email of RECIPIENTS) {
    try {
      const result = await resend.emails.send({
        from: "Harrison <harrison@mail.blockpools.io>",
        to: email,
        template: {
          id: TEMPLATE_ID,
        },
      } as any);

      const resultAny = result as any;

      if (resultAny?.error || !resultAny?.data?.id) {
        console.error(`[FAILED] ${email} — ${JSON.stringify(resultAny?.error)}`);
        failed++;
      } else {
        console.log(`[SENT] ${email}`);
        sent++;
      }
    } catch (err: any) {
      console.error(`[ERROR] ${email} — ${err?.message || err}`);
      failed++;
    }

    await new Promise((r) => setTimeout(r, DELAY_MS));
  }

  console.log(`\nDone. Sent: ${sent} | Failed: ${failed}`);
  process.exit(0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});