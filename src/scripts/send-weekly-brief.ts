/**
 * send-weekly-brief-retry.ts
 *
 * Resends the BlockPools Weekly Brief to the 12 addresses that failed
 * due to Resend daily quota on 2026-03-30.
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-weekly-brief-retry.ts
 */
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

const TEMPLATE_ID = "2fb7a3e0-c425-4221-8036-63887b64305b";

const FAILED_EMAILS = [
  "jc741899@gmail.com",
  "geduardogomes93@gmail.com",
  "silvestresilva864@gmail.com",
  "jonathancruzdemoraes300@gmail.com",
  "danieljunior0099@gmail.com",
  "ellyqueiros23@gmail.com",
  "silvahortegalj@gmail.com",
  "catrachoalex83@gmail.com",
  "yolanda120694@gmail.com",
  "agnaldolucas118@gmail.com",
  // From screenshot
  "miyabimty1217@gmail.com",
  "adeianasabinofurtunato123@gmail.com",
];

async function run() {
  console.log(`Retrying ${FAILED_EMAILS.length} failed recipients...`);

  let sent = 0;
  let failed = 0;

  for (const email of FAILED_EMAILS) {
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

      await new Promise((r) => setTimeout(r, 700));
    } catch (err: any) {
      console.error(`[ERROR] ${email} — ${err?.message || err}`);
      failed++;
    }
  }

  console.log(`\nDone. Sent: ${sent}, Failed: ${failed}`);
  process.exit(0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});