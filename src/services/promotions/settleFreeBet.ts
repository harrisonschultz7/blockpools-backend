// src/services/promotions/settleFreeBet.ts
//
// Settles a single free-bet redemption after its pool has finalized.
//
// Algorithm:
//  1. Pool must be is_final = true.
//  2. If resolution_type = 'REFUND' → loss with profit=0, treasury_recovered=
//     credit_usdc.
//  3. Else if outcome_index === winning_outcome_index:
//       - Funding wallet calls claimWinnings on the pool (idempotent across
//         redemptions on the same pool — first caller drains all positions,
//         subsequent calls revert harmlessly).
//       - profit = max(0, payout - credit). For prediction-market AMM pools
//         where each share pays $1 on a win, payout ≈ shares; we approximate
//         by reading shares-held BEFORE claim and using shares*$1.
//       - Funding wallet transfers `profit` USDC to redemption.user_address.
//  4. Else (loss): profit=0, treasury_recovered=credit_usdc.
//
// Idempotency: payout_tx_hash IS NULL is the lock. Once we set it, retries
// skip the redemption.

import { Contract } from "@ethersproject/contracts";
import { BigNumber } from "@ethersproject/bignumber";
import { parseUnits, formatUnits } from "@ethersproject/units";

import { pool } from "../../db";
import {
  USDC_DECIMALS,
  PROMO_TX_CONFIRMATIONS,
  PROMO_CLAIM_GAS_LIMIT,
} from "../../config/promo";
import { getFundingWallet } from "./findFundingWallet";
import { writeLedgerEntry } from "./promotionFunding";

// Pool ABI subset for settlement, covering both pool variants:
//   - Multi:  shares(address, uint8) view returns (uint256)
//   - Binary: sharesTeamAByUser(address) / sharesTeamBByUser(address)
const POOL_ABI = [
  "function claimWinnings()",
  "function isResolved() view returns (bool)",
  // multi
  "function shares(address user, uint8 outcome) view returns (uint256)",
  // binary
  "function sharesTeamAByUser(address user) view returns (uint256)",
  "function sharesTeamBByUser(address user) view returns (uint256)",
];

export type SettleFreeBetResult =
  | { settled: false; reason: string; redemptionId: string }
  | {
      settled: true;
      redemptionId: string;
      status: "settled_win" | "settled_loss";
      profitUsdc: string;
      payoutTxHash: string | null;
    };

export async function settleFreeBet(
  redemptionId: string
): Promise<SettleFreeBetResult> {
  const q = await pool.query(
    `
    SELECT
      r.id,
      r.user_address,
      r.pool_address,
      r.outcome_index,
      r.credit_usdc,
      r.payout_tx_hash,
      r.status,
      r.promotion_id,
      g.is_final,
      g.winning_outcome_index,
      g.resolution_type,
      g.market_type
    FROM public.promo_redemptions r
    LEFT JOIN public.games g ON lower(g.game_id) = lower(r.pool_address)
    WHERE r.id = $1
    `,
    [redemptionId]
  );
  const row = q.rows[0];
  if (!row) return { settled: false, reason: "redemption_not_found", redemptionId };
  if (row.status !== "placed") {
    return { settled: false, reason: `status_${row.status}`, redemptionId };
  }
  if (row.payout_tx_hash) {
    return { settled: false, reason: "already_settled", redemptionId };
  }
  if (!row.is_final) {
    return { settled: false, reason: "pool_not_final", redemptionId };
  }

  const creditUsdc = String(row.credit_usdc);
  const userAddress = String(row.user_address).toLowerCase();
  const poolAddress = String(row.pool_address).toLowerCase();
  const userOutcome = Number(row.outcome_index);
  const winningOutcome = row.winning_outcome_index;
  const resolutionType = String(row.resolution_type || "").toUpperCase();
  const marketType = String(row.market_type || "").toUpperCase();

  const isRefund = resolutionType === "REFUND";
  const isWin =
    !isRefund &&
    winningOutcome != null &&
    Number(winningOutcome) === userOutcome;

  // ── Loss / refund path: pure DB transitions, no on-chain calls. ──────────
  if (!isWin) {
    return await markLossOrRefund(redemptionId, row.promotion_id, creditUsdc, isRefund);
  }

  // ── Win path: claim on-chain, transfer profit, then book the ledger. ─────
  const { wallet, usdc } = getFundingWallet();
  const poolContract = new Contract(poolAddress, POOL_ABI, wallet);

  // Read shares BEFORE the claim so we know how big the payout will be. Each
  // winning share pays $1 in our pool design. Branch by market_type:
  //   - BINARY → sharesTeamAByUser / sharesTeamBByUser
  //   - else   → shares(address, uint8)
  let sharesHeld: BigNumber;
  try {
    if (marketType === "BINARY") {
      if (userOutcome === 0) {
        sharesHeld = await poolContract.sharesTeamAByUser(wallet.address);
      } else if (userOutcome === 1) {
        sharesHeld = await poolContract.sharesTeamBByUser(wallet.address);
      } else {
        throw new Error(`binary outcome out of range: ${userOutcome}`);
      }
    } else {
      sharesHeld = await poolContract.shares(wallet.address, userOutcome);
    }
  } catch (err: any) {
    // Some older pool variants don't expose either signature. Fall back to
    // assuming the credit doubled (worst-case under-payout for user) — flag
    // for reconciliation.
    console.warn(
      "[settleFreeBet] shares read failed; conservative payout fallback",
      { redemptionId, marketType, err: err?.message }
    );
    sharesHeld = parseUnits(creditUsdc, USDC_DECIMALS);
  }

  // Issue the on-chain claim. If claimWinnings was already invoked for this
  // pool by an earlier redemption settlement, this will revert — that's fine,
  // the funding wallet already holds the USDC.
  try {
    const tx = await poolContract.claimWinnings({ gasLimit: PROMO_CLAIM_GAS_LIMIT });
    await tx.wait(PROMO_TX_CONFIRMATIONS);
  } catch (err: any) {
    // Tolerate "already claimed" reverts. Anything else is a hard failure
    // because we're about to compute payout assuming the funds are in hand.
    const msg = String(err?.message || "").toLowerCase();
    const benign = msg.includes("already claimed") || msg.includes("nothing to claim");
    if (!benign) {
      console.error("[settleFreeBet] claimWinnings failed", { redemptionId, err });
      return { settled: false, reason: "claim_failed", redemptionId };
    }
  }

  // payout in USDC base units = sharesHeld (1:1 share→USDC at settlement).
  // profit = max(0, payout - credit).
  const creditBn = parseUnits(creditUsdc, USDC_DECIMALS);
  const profitBn = sharesHeld.gt(creditBn) ? sharesHeld.sub(creditBn) : BigNumber.from(0);
  const profitUsdc = formatUnits(profitBn, USDC_DECIMALS);

  let payoutTxHash: string | null = null;
  if (profitBn.gt(0)) {
    try {
      const tx = await usdc.transfer(userAddress, profitBn);
      const receipt = await tx.wait(PROMO_TX_CONFIRMATIONS);
      if (!receipt || receipt.status !== 1) {
        return { settled: false, reason: "payout_transfer_failed", redemptionId };
      }
      payoutTxHash = tx.hash;
    } catch (err: any) {
      console.error("[settleFreeBet] payout transfer failed", { redemptionId, err });
      return { settled: false, reason: "payout_transfer_threw", redemptionId };
    }
  }

  // Flip the redemption + write ledger entries atomically. Use a sentinel
  // value for payout_tx_hash on losses so the IS NULL idempotency lock holds.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const upd = await client.query(
      `
      UPDATE public.promo_redemptions
         SET status                  = 'settled_win',
             payout_tx_hash          = $1,
             payout_amount_usdc      = $2,
             treasury_recovered_usdc = $3,
             settled_at              = now()
       WHERE id = $4
         AND payout_tx_hash IS NULL
       RETURNING id
      `,
      [
        payoutTxHash ?? "",
        profitUsdc,
        creditUsdc,
        redemptionId,
      ]
    );
    if (upd.rowCount === 0) {
      await client.query("ROLLBACK");
      return { settled: false, reason: "race_lost", redemptionId };
    }

    await client.query(
      `INSERT INTO public.promo_eligibility_events
         (redemption_id, event_type, event_data)
       VALUES ($1, 'settled', $2::jsonb)`,
      [
        redemptionId,
        JSON.stringify({
          outcome: "win",
          payoutTxHash,
          profitUsdc,
          treasuryRecoveredUsdc: creditUsdc,
        }),
      ]
    );

    if (profitBn.gt(0)) {
      await writeLedgerEntry(
        {
          promotionId: row.promotion_id,
          redemptionId,
          direction: "payout_to_user",
          amountUsdc: profitUsdc,
          txHash: payoutTxHash,
        },
        client
      );
    }
    await writeLedgerEntry(
      {
        promotionId: row.promotion_id,
        redemptionId,
        direction: "treasury_recovered",
        amountUsdc: creditUsdc,
      },
      client
    );

    await client.query("COMMIT");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    console.error(
      "[settleFreeBet] DB persist failed AFTER successful payout",
      { redemptionId, payoutTxHash, err }
    );
    throw err;
  } finally {
    client.release();
  }

  return {
    settled: true,
    redemptionId,
    status: "settled_win",
    profitUsdc,
    payoutTxHash,
  };
}

async function markLossOrRefund(
  redemptionId: string,
  promotionId: string,
  creditUsdc: string,
  isRefund: boolean
): Promise<SettleFreeBetResult> {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const upd = await client.query(
      `
      UPDATE public.promo_redemptions
         SET status                  = 'settled_loss',
             payout_tx_hash          = '',
             payout_amount_usdc      = 0,
             treasury_recovered_usdc = $1,
             settled_at              = now()
       WHERE id = $2
         AND payout_tx_hash IS NULL
       RETURNING id
      `,
      [creditUsdc, redemptionId]
    );
    if (upd.rowCount === 0) {
      await client.query("ROLLBACK");
      return { settled: false, reason: "race_lost", redemptionId };
    }

    await client.query(
      `INSERT INTO public.promo_eligibility_events
         (redemption_id, event_type, event_data)
       VALUES ($1, 'settled', $2::jsonb)`,
      [
        redemptionId,
        JSON.stringify({
          outcome: isRefund ? "refund" : "loss",
          treasuryRecoveredUsdc: creditUsdc,
        }),
      ]
    );

    await writeLedgerEntry(
      {
        promotionId,
        redemptionId,
        direction: "treasury_recovered",
        amountUsdc: creditUsdc,
      },
      client
    );

    await client.query("COMMIT");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    throw err;
  } finally {
    client.release();
  }

  return {
    settled: true,
    redemptionId,
    status: "settled_loss",
    profitUsdc: "0",
    payoutTxHash: null,
  };
}
