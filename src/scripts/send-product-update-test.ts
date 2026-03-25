/**
 * send-product-update-test.ts
 *
 * Sends the Product Update email to a single test address.
 *
 * Run on VPS:
 *   cd /opt/blockpools/backend
 *   set -a && source /etc/blockpools/backend.env && set +a
 *   npx ts-node src/scripts/send-product-update-test.ts
 */
import { Resend } from "resend";

const resend = new Resend(process.env.RESEND_API_KEY);

const TEMPLATE_ID = "4a929977-2a9c-4981-b14d-7bb7fcba6411";
const TEST_EMAIL = "harrisonschultz1240@gmail.com";

async function run() {
  console.log(`Sending test to ${TEST_EMAIL}...`);

  const result = await resend.emails.send({
from: "Harrison at BlockPools <harrison@mail.blockpools.io>",
    to: TEST_EMAIL,
    template: {
      id: TEMPLATE_ID,
    },
  } as any);

  const resultAny = result as any;
  if (resultAny?.error || !resultAny?.data?.id) {
    console.error(`[FAILED] ${JSON.stringify(resultAny?.error)}`);
    process.exit(1);
  } else {
    console.log(`[SENT] Email ID: ${resultAny.data.id}`);
  }

  process.exit(0);
}

run().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});