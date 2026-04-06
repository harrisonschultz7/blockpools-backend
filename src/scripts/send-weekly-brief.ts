/**
 * send-weekly-brief.ts
 *
 * Sends the BlockPools Weekly Brief to all users with an email on file,
 * excluding addresses that were already sent on this run.
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-weekly-brief.ts
 */
import { Resend } from "resend";
import { createClient } from "@supabase/supabase-js";

const resend = new Resend(process.env.RESEND_API_KEY);

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const TEMPLATE_ID = "93f918e5-06d4-4ec9-b29c-5c24e31a8425";

const DELAY_MS = 700;

const ALREADY_SENT = new Set([
  "catrachoalex83@gmail.com",
  "abrahammorga512@gmail.com",
  "mayflordomi@gmail.com",
  "francokpomercado30@gmail.com",
  "jordaniomouramatoss@gmail.com",
  "sandre640@gmail.com",
  "valentinobrawl2101@gmail.com",
  "elpolloramirez1@outlook.com",
  "polentaasesina@outlook.com",
  "gerardodaniel8592@gmail.com",
  "paypal780@yanemail.com",
]);

async function getRecipients(): Promise<string[]> {
  const { data, error } = await supabase
    .from("users")
    .select("email")
    .not("email", "is", null)
    .neq("email", "");

  if (error) {
    throw new Error(`Supabase fetch failed: ${error.message}`);
  }

  return (data ?? [])
    .map((row: { email: string }) => row.email)
    .filter((email: string) => !ALREADY_SENT.has(email));
}

async function run() {
  console.log("Fetching recipients from users table...");
  const emails = await getRecipients();
  console.log(`Found ${emails.length} remaining recipients. Starting send...\n`);

  let sent = 0;
  let failed = 0;

  for (const email of emails) {
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

  console.log(`\nDone. Sent: ${sent} | Failed: ${failed} | Total: ${emails.length}`);
  process.exit(0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});