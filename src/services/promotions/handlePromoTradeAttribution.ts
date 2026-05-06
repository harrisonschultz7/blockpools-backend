// src/services/promotions/handlePromoTradeAttribution.ts
//
// Two responsibilities, both invoked from the persistTrades hook:
//
//   1. Pre-insert mutation: when a BUY trade is being persisted whose
//      user_address is the promo funding wallet, look up the matching open
//      'placed' redemption by (pool_address, tx_hash) and stamp BOTH:
//        - beneficiary_address = redemption.user_address
//        - promo_redemption_id = redemption.id
//      onto the trade row before insert. Stats queries that opt into the
//      effective_user_address generated column then attribute the trade to
//      the user automatically.
//
//   2. Post-insert: when a real-money BUY (NOT from the funding wallet) lands
//      for a user who has any pending_qualification redemptions, evaluate
//      each one to see if the unlock condition is now met.
//
// Both functions are wrapped in try/catch and never throw. Failure here MUST
// NOT block trade ingestion.

import { pool } from "../../db";
import {
  isPromoFundingWallet,
  PROMO_FRAMEWORK_ENABLED,
  PROMO_FUNDING_WALLET_ADDRESS,
} from "../../config/promo";
import { evaluatePromoEligibility } from "./evaluatePromoEligibility";
import { refreshUserTradesPage } from "../cacheRefresh";

// Loose shape so the persistTrades file doesn't need to import our types.
export type AttributionTradeRow = {
  user_address?: string;
  user?: string;
  type?: string;
  tx_hash?: string | null;
  txHash?: string | null;
  game_id?: string;
  gameId?: string;
  beneficiary_address?: string | null;
  promo_redemption_id?: string | null;
};

// ── Pre-insert hook ──────────────────────────────────────────────────────────
//
// Mutates `trade` in place — adds beneficiary_address + promo_redemption_id
// if this is a funding-wallet BUY that maps to an open 'placed' redemption.
// The persistTrades hook reads those fields off the proxy and applies them in
// a follow-up UPDATE after the main batch insert commits.
export async function applyBeneficiaryToFundingWalletTrade(
  trade: AttributionTradeRow
): Promise<void> {
  if (!PROMO_FRAMEWORK_ENABLED) return;

  try {
    const user = String(trade.user_address || trade.user || "").toLowerCase();
    const type = String(trade.type || "").toUpperCase();
    if (!isPromoFundingWallet(user)) return;
    if (type !== "BUY") return;

    const txHash = String(trade.tx_hash || trade.txHash || "").toLowerCase();
    const gameId = String(trade.game_id || trade.gameId || "").toLowerCase();
    if (!txHash || !gameId) return;

    const r = await pool.query(
      `
      SELECT id, user_address
        FROM public.promo_redemptions
       WHERE lower(pool_address) = $1
         AND lower(tx_hash)      = $2
         AND status              = 'placed'
       LIMIT 1
      `,
      [gameId, txHash]
    );
    const row = r.rows[0];
    if (!row) return;

    trade.beneficiary_address = String(row.user_address).toLowerCase();
    trade.promo_redemption_id = String(row.id);
  } catch (err) {
    console.error(
      "[handlePromoTradeAttribution] pre-insert lookup failed (non-blocking)",
      err
    );
  }
}

// ── Post-insert hook ─────────────────────────────────────────────────────────
//
// Called once per persisted trade (after upsert). Two paths:
//   - Funding-wallet BUYs that we successfully attributed in the pre-insert
//     hook: nothing further to do here (the redemption already had its
//     pool_address / tx_hash stamped at placement time).
//   - Real-user BUYs with no beneficiary: evaluate any pending qualifications
//     for that user.
export async function handlePromoTradeAttribution(
  trade: AttributionTradeRow
): Promise<void> {
  if (!PROMO_FRAMEWORK_ENABLED) return;

  try {
    const user = String(trade.user_address || trade.user || "").toLowerCase();
    const type = String(trade.type || "").toUpperCase();
    if (!user) return;
    if (type !== "BUY") return;
    if (isPromoFundingWallet(user)) return;
    if (trade.beneficiary_address) return; // already a free-bet trade

    const r = await pool.query(
      `
      SELECT id
        FROM public.promo_redemptions
       WHERE status = 'pending_qualification'
         AND (
           lower(user_address)        = $1
           OR lower(referrer_address) = $1
         )
      `,
      [user]
    );
    if (r.rowCount === 0) return;

    for (const row of r.rows) {
      try {
        await evaluatePromoEligibility(row.id);
      } catch (err) {
        console.error(
          "[handlePromoTradeAttribution] evaluate failed (non-blocking)",
          { redemptionId: row.id, err }
        );
      }
    }
  } catch (err) {
    console.error(
      "[handlePromoTradeAttribution] post-insert hook failed (non-blocking)",
      err
    );
  }
}

// ── Funding-wallet attribution refresh ───────────────────────────────────────
//
// Called fire-and-forget from placeFreeBet immediately after a successful
// on-chain buy. Pulls the funding wallet's latest trades from the subgraph
// and persists them, which triggers the pre-insert hook to stamp
// beneficiary_address / promo_redemption_id on the new trade row.
//
// The subgraph has a small indexing lag, so we retry with backoff. Once the
// row appears with the attribution stamped, we stop. If after all attempts
// the trade still isn't indexed, future place-bets (or the next manual pull)
// will eventually catch it — the pre-insert hook keys off the redemption row,
// which doesn't go anywhere.
//
// Never throws — failures are non-blocking and logged.
export async function triggerFundingWalletAttributionRefresh(
  txHash: string,
  redemptionId: string
): Promise<void> {
  if (!PROMO_FRAMEWORK_ENABLED) return;
  if (!PROMO_FUNDING_WALLET_ADDRESS) return;
  if (!txHash) return;

  // Backoff schedule. Subgraph latency on Arbitrum is usually <30s.
  const delaysMs = [3_000, 10_000, 30_000, 60_000];

  const txHashLower = String(txHash).toLowerCase();

  for (let i = 0; i < delaysMs.length; i++) {
    await new Promise((r) => setTimeout(r, delaysMs[i]));

    try {
      await refreshUserTradesPage({
        user: PROMO_FUNDING_WALLET_ADDRESS,
        leagues: [], // empty = no league filter on the persist path
        range: "ALL",
        page: 1,
        pageSize: 50,
      });
    } catch (err: any) {
      console.warn(
        `[triggerFundingWalletAttributionRefresh] refresh attempt ${i + 1}/${
          delaysMs.length
        } failed (non-blocking):`,
        err?.message ?? err
      );
      continue;
    }

    // Did our trade land with the attribution stamped?
    try {
      const r = await pool.query(
        `SELECT 1
           FROM public.user_trade_events
          WHERE lower(tx_hash)         = $1
            AND beneficiary_address    IS NOT NULL
            AND promo_redemption_id    = $2
          LIMIT 1`,
        [txHashLower, redemptionId]
      );
      if ((r.rowCount ?? 0) > 0) {
        console.log(
          `[triggerFundingWalletAttributionRefresh] attributed redemption=${redemptionId} on attempt ${
            i + 1
          }`
        );
        return;
      }
    } catch (err: any) {
      console.warn(
        `[triggerFundingWalletAttributionRefresh] verification query failed (non-blocking):`,
        err?.message ?? err
      );
    }
  }

  console.warn(
    `[triggerFundingWalletAttributionRefresh] tx not attributed after ${delaysMs.length} attempts; ` +
      `redemption=${redemptionId} txHash=${txHashLower}. The next place-bet or manual /cache pull will pick it up.`
  );
}
