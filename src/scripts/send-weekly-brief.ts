/**
 * send-weekly-brief.ts
 *
 * Sends the BlockPools Weekly Brief to all users with an email on file.
 * Each user receives the template matching their `preferred_locale`:
 *   - locale starts with "es" (or is empty/null) -> Spanish template
 *   - any other non-empty locale                 -> English template
 * (Mirrors the welcome-email routing in backfill-welcome-emails.ts.)
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-weekly-brief.ts
 */
import "dotenv/config";
import { Resend } from "resend";
import { createClient } from "@supabase/supabase-js";

const resend = new Resend(process.env.RESEND_API_KEY);

// ── TEMPLATES ─────────────────────────────────────────────────────────────────
const SPANISH_TEMPLATE_ID = "93f918e5-06d4-4ec9-b29c-5c24e31a8425";
const ENGLISH_TEMPLATE_ID = "241ae77f-15a8-4191-8aad-435c820a3c31";

function pickTemplateId(preferredLocale: string | null): string {
  const tag = (preferredLocale || "").trim().toLowerCase();
  const isEnglish = tag !== "" && !tag.startsWith("es");
  return isEnglish ? ENGLISH_TEMPLATE_ID : SPANISH_TEMPLATE_ID;
}
// ─────────────────────────────────────────────────────────────────────────────

const DELAY_MS = 700;

// ── TEST MODE ────────────────────────────────────────────────────────────────
// Set to true to send BOTH language versions only to TEST_EMAIL.
// Set to false when ready to blast everyone.
const TEST_MODE = false;
const TEST_EMAIL = "harrisonschultz1240@gmail.com";
// ─────────────────────────────────────────────────────────────────────────────

const ALREADY_SENT = new Set<string>([
  "bgee355@gmail.com",
  "adrianop1414@gmail.com",
  "max.r.mccullough@gmail.com",
  "rschacht25@gmail.com",
  "tommy.rice6@gmail.com",
]);

type Recipient = { email: string; preferredLocale: string | null };

async function getRecipients(): Promise<Recipient[]> {
  if (TEST_MODE) {
    console.log(`TEST MODE — sending both language versions only to ${TEST_EMAIL}`);
    return [
      { email: TEST_EMAIL, preferredLocale: "es" },
      { email: TEST_EMAIL, preferredLocale: "en" },
    ];
  }

  // Created here (not at module load) so TEST_MODE runs without Supabase env vars.
  const supabase = createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );

  const { data, error } = await supabase
    .from("users")
    .select("email, preferred_locale")
    .not("email", "is", null)
    .neq("email", "");

  if (error) {
    throw new Error(`Supabase fetch failed: ${error.message}`);
  }

  return (data ?? [])
    .map((row: { email: string; preferred_locale: string | null }) => ({
      email: row.email,
      preferredLocale: row.preferred_locale,
    }))
    .filter((r: Recipient) => !ALREADY_SENT.has(r.email.trim().toLowerCase()));
}

async function run() {
  console.log("Fetching recipients from users table...");
  const recipients = await getRecipients();
  console.log(`Found ${recipients.length} remaining recipients. Starting send...\n`);

  let sent = 0;
  let failed = 0;

  for (const { email, preferredLocale } of recipients) {
    const templateId = pickTemplateId(preferredLocale);
    const lang = templateId === ENGLISH_TEMPLATE_ID ? "EN" : "ES";
    try {
      const result = await resend.emails.send({
        from: "Harrison from BlockPools <harrison@mail.blockpools.io>",
        to: email,
        template: {
          id: templateId,
        },
      } as any);

      const resultAny = result as any;

      if (resultAny?.error || !resultAny?.data?.id) {
        console.error(`[FAILED] ${email} (${lang}) — ${JSON.stringify(resultAny?.error)}`);
        failed++;
      } else {
        console.log(`[SENT] ${email} (${lang})`);
        sent++;
      }
    } catch (err: any) {
      console.error(`[ERROR] ${email} (${lang}) — ${err?.message || err}`);
      failed++;
    }

    await new Promise((r) => setTimeout(r, DELAY_MS));
  }

  console.log(`\nDone. Sent: ${sent} | Failed: ${failed} | Total: ${recipients.length}`);
  process.exit(0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
