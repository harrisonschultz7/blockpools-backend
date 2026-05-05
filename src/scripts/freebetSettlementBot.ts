// src/scripts/freebetSettlementBot.ts
//
// Settlement cron for free bets. Designed to run AFTER the main
// settlement-bot.ts on the same schedule (or as a follow-on systemd unit).
//
// Picks up every 'placed' redemption whose pool is now is_final = true and
// has not yet been settled, and dispatches each one through settleFreeBet.
//
// Run:
//   node dist/scripts/freebetSettlementBot.js
//
// Idempotency lives in settleFreeBet (payout_tx_hash IS NULL is the lock), so
// re-running this script as often as you like is safe.

import "dotenv/config";

import { pool } from "../db";
import { PROMO_FRAMEWORK_ENABLED } from "../config/promo";
import { settleFreeBet } from "../services/promotions/settleFreeBet";

const MAX_PER_RUN = Number(process.env.PROMO_SETTLE_MAX_PER_RUN || 50);
const CONCURRENCY = Number(process.env.PROMO_SETTLE_CONCURRENCY || 1);

async function pickWork(): Promise<string[]> {
  const r = await pool.query(
    `
    SELECT r.id
      FROM public.promo_redemptions r
      JOIN public.games g ON lower(g.game_id) = lower(r.pool_address)
     WHERE r.status = 'placed'
       AND r.payout_tx_hash IS NULL
       AND g.is_final = true
     ORDER BY r.placed_at ASC NULLS LAST
     LIMIT $1
    `,
    [MAX_PER_RUN]
  );
  return r.rows.map((row: any) => String(row.id));
}

async function runOne(id: string): Promise<void> {
  try {
    const result = await settleFreeBet(id);
    if ((result as any).settled) {
      const r = result as any;
      console.log(
        `[freebetSettlementBot] ${id} → ${r.status} profit=${r.profitUsdc} tx=${r.payoutTxHash || "none"}`
      );
    } else {
      console.log(`[freebetSettlementBot] ${id} skipped (${(result as any).reason})`);
    }
  } catch (err) {
    console.error(`[freebetSettlementBot] ${id} threw`, err);
  }
}

async function runConcurrent(ids: string[], concurrency: number): Promise<void> {
  if (concurrency <= 1) {
    for (const id of ids) await runOne(id);
    return;
  }
  let cursor = 0;
  await Promise.all(
    Array.from({ length: concurrency }).map(async () => {
      while (cursor < ids.length) {
        const i = cursor++;
        await runOne(ids[i]);
      }
    })
  );
}

async function main() {
  if (!PROMO_FRAMEWORK_ENABLED) {
    console.log("[freebetSettlementBot] disabled — exiting");
    return;
  }

  const ids = await pickWork();
  if (!ids.length) {
    console.log("[freebetSettlementBot] no settleable redemptions");
    return;
  }
  console.log(`[freebetSettlementBot] settling ${ids.length} redemption(s)`);
  await runConcurrent(ids, CONCURRENCY);
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error("[freebetSettlementBot] unhandled", err);
    process.exit(1);
  });
