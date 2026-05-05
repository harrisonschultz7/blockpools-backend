// src/services/promotions/promotionFunding.ts
//
// Single entry point for writing rows into promotion_funding_ledger. Centralizing
// here keeps the entry shape consistent across placement, settlement, and
// reconciliation, and gives us one place to add invariants later.
//
// Schema columns: promotion_id, redemption_id, direction (enum
// promo_funding_direction: 'bet_funded' | 'payout_to_user' |
// 'treasury_recovered'), amount_usdc, tx_hash, created_at.

import type { PoolClient } from "pg";
import { pool } from "../../db";

export type LedgerDirection =
  | "bet_funded"
  | "payout_to_user"
  | "treasury_recovered";

export type LedgerEntry = {
  promotionId: string;
  redemptionId: string;
  direction: LedgerDirection;
  amountUsdc: string; // numeric — pass as string to preserve precision
  txHash?: string | null;
};

// Use an existing transaction if the caller has one open, else open our own.
// Most call sites already wrap their work in BEGIN/COMMIT, so accepting a
// client lets the ledger write share that atomicity.
export async function writeLedgerEntry(
  entry: LedgerEntry,
  client?: PoolClient
): Promise<void> {
  const sql = `
    INSERT INTO public.promotion_funding_ledger
      (promotion_id, redemption_id, direction, amount_usdc, tx_hash)
    VALUES ($1, $2, $3, $4, $5)
  `;
  const args = [
    entry.promotionId,
    entry.redemptionId,
    entry.direction,
    entry.amountUsdc,
    entry.txHash ?? null,
  ];

  if (client) {
    await client.query(sql, args);
    return;
  }

  await pool.query(sql, args);
}

// Idempotency helper used by placeFreeBet — checks whether we've already
// recorded a bet_funded entry for this redemption so a retry can short-circuit
// before sending a duplicate on-chain tx.
export async function hasBetFundedEntry(redemptionId: string): Promise<boolean> {
  const r = await pool.query(
    `SELECT 1 FROM public.promotion_funding_ledger
       WHERE redemption_id = $1 AND direction = 'bet_funded' LIMIT 1`,
    [redemptionId]
  );
  return r.rowCount! > 0;
}
