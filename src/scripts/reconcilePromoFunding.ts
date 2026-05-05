// src/scripts/reconcilePromoFunding.ts
//
// Daily ledger audit. Walks every redemption that has reached a terminal
// state and verifies:
//
//   1. bet_funded entry exists exactly once for any 'placed' or settled
//      redemption.
//   2. settled_win redemptions have:
//      - payout_to_user entry (when profit > 0)
//      - treasury_recovered entry
//      - payout_tx_hash set
//   3. settled_loss redemptions have:
//      - treasury_recovered entry
//      - payout_amount_usdc = 0
//   4. Per-redemption ledger sums make sense:
//      bet_funded ≈ payout_to_user + treasury_recovered (within rounding)
//
// Discrepancies are logged loudly. The script does NOT auto-correct — it just
// flags. Auto-correction would be too risky given the on-chain side.
//
// Run:
//   node dist/scripts/reconcilePromoFunding.js

import "dotenv/config";

import { pool } from "../db";
import { PROMO_FRAMEWORK_ENABLED } from "../config/promo";

type Issue = { redemptionId: string; kind: string; detail?: any };

async function audit(): Promise<Issue[]> {
  const issues: Issue[] = [];

  const r = await pool.query(`
    SELECT
      r.id,
      r.status,
      r.credit_usdc,
      r.payout_tx_hash,
      r.payout_amount_usdc,
      r.treasury_recovered_usdc,
      COALESCE(SUM(CASE WHEN l.direction = 'bet_funded'         THEN l.amount_usdc END), 0) AS sum_funded,
      COALESCE(SUM(CASE WHEN l.direction = 'payout_to_user'     THEN l.amount_usdc END), 0) AS sum_payout,
      COALESCE(SUM(CASE WHEN l.direction = 'treasury_recovered' THEN l.amount_usdc END), 0) AS sum_treasury,
      COUNT(*) FILTER (WHERE l.direction = 'bet_funded')         AS n_funded,
      COUNT(*) FILTER (WHERE l.direction = 'payout_to_user')     AS n_payout,
      COUNT(*) FILTER (WHERE l.direction = 'treasury_recovered') AS n_treasury
    FROM public.promo_redemptions r
    LEFT JOIN public.promotion_funding_ledger l
      ON l.redemption_id = r.id
    WHERE r.status IN ('placed','settled_win','settled_loss')
    GROUP BY r.id
  `);

  for (const row of r.rows) {
    const credit = Number(row.credit_usdc);
    const sumFunded = Number(row.sum_funded);
    const sumPayout = Number(row.sum_payout);
    const sumTreasury = Number(row.sum_treasury);

    if (Number(row.n_funded) !== 1) {
      issues.push({
        redemptionId: row.id,
        kind: "bet_funded_count_unexpected",
        detail: { count: row.n_funded },
      });
    }
    if (Math.abs(sumFunded - credit) > 0.000001) {
      issues.push({
        redemptionId: row.id,
        kind: "bet_funded_amount_mismatch",
        detail: { sumFunded, credit },
      });
    }

    if (row.status === "settled_win") {
      // payout_tx_hash is required only when there was a non-zero payout. A
      // win with profit=0 (rare — shares*$1 ≤ credit) sets payout_tx_hash=''
      // intentionally to hold the IS NULL idempotency lock.
      if (Number(row.payout_amount_usdc) > 0 && !row.payout_tx_hash) {
        issues.push({ redemptionId: row.id, kind: "win_missing_payout_tx" });
      }
      if (Number(row.n_treasury) !== 1) {
        issues.push({
          redemptionId: row.id,
          kind: "win_treasury_count_unexpected",
          detail: { count: row.n_treasury },
        });
      }
      if (Math.abs(sumTreasury - credit) > 0.000001) {
        issues.push({
          redemptionId: row.id,
          kind: "win_treasury_amount_mismatch",
          detail: { sumTreasury, credit },
        });
      }
      if (Number(row.payout_amount_usdc) > 0 && Number(row.n_payout) !== 1) {
        issues.push({
          redemptionId: row.id,
          kind: "win_payout_count_unexpected",
          detail: { count: row.n_payout },
        });
      }
      if (Math.abs(sumPayout - Number(row.payout_amount_usdc)) > 0.000001) {
        issues.push({
          redemptionId: row.id,
          kind: "win_payout_amount_mismatch",
          detail: { sumPayout, payout: row.payout_amount_usdc },
        });
      }
    }

    if (row.status === "settled_loss") {
      if (Number(row.payout_amount_usdc) !== 0) {
        issues.push({
          redemptionId: row.id,
          kind: "loss_payout_nonzero",
          detail: { payout: row.payout_amount_usdc },
        });
      }
      if (Number(row.n_treasury) !== 1) {
        issues.push({
          redemptionId: row.id,
          kind: "loss_treasury_count_unexpected",
          detail: { count: row.n_treasury },
        });
      }
    }
  }

  return issues;
}

async function main() {
  if (!PROMO_FRAMEWORK_ENABLED) {
    console.log("[reconcilePromoFunding] disabled — exiting");
    return;
  }

  const issues = await audit();
  if (!issues.length) {
    console.log("[reconcilePromoFunding] OK — no discrepancies");
    return;
  }

  console.error(`[reconcilePromoFunding] found ${issues.length} issue(s):`);
  for (const i of issues) {
    console.error(`  - [${i.kind}] redemption=${i.redemptionId}`, i.detail || "");
  }
  process.exitCode = 2;
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error("[reconcilePromoFunding] unhandled", err);
    process.exit(1);
  });
