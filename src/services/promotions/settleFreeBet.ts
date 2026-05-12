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
      g.market_type,
      g.league,
      g.team_a_code,
      g.team_b_code
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

  // ── Per-redemption share count ───────────────────────────────────────────
  // Reading the funding wallet's TOTAL shares for this outcome from the pool
  // is the wrong measure when multiple redemptions sit on the same pool — the
  // first one to settle would consume the entire wallet balance and the rest
  // would get $0. Instead, compute shares from THIS redemption's own BUY row
  // in user_trade_events: shares = net_stake / avg_price (each winning share
  // pays $1, AMM-style).
  let sharesHeld: BigNumber;
  const tradeQ = await pool.query(
    `SELECT net_stake_dec, avg_price_bps
       FROM public.user_trade_events
      WHERE promo_redemption_id = $1
        AND type = 'BUY'
      LIMIT 1`,
    [redemptionId]
  );
  const tradeRow = tradeQ.rows[0];
  if (tradeRow && tradeRow.net_stake_dec != null && tradeRow.avg_price_bps != null) {
    const netStake = Number(tradeRow.net_stake_dec);
    const avgPriceBps = Number(tradeRow.avg_price_bps);
    // shares = (net_stake * 10000) / avg_price_bps. Each share is worth $1 on
    // a win, so shares-as-USDC equals payout-as-USDC. Compute in floating
    // point then convert to base units — net_stake is already in USDC, so
    // the result is in USDC too.
    const sharesUsdc =
      avgPriceBps > 0 ? (netStake * 10_000) / avgPriceBps : 0;
    sharesHeld = parseUnits(sharesUsdc.toFixed(USDC_DECIMALS), USDC_DECIMALS);
  } else {
    // No BUY row yet — fall back to on-chain read (legacy behavior). This
    // path is conservative for single-redemption pools but unsafe for
    // multi-redemption pools, so we log a warning.
    console.warn(
      "[settleFreeBet] no BUY row for redemption; falling back to on-chain shares read",
      { redemptionId, poolAddress }
    );
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
      console.warn(
        "[settleFreeBet] shares read failed; conservative payout fallback",
        { redemptionId, marketType, err: err?.message }
      );
      sharesHeld = parseUnits(creditUsdc, USDC_DECIMALS);
    }
  }

  // Issue the on-chain claim. ANY revert here is treated as benign — the
  // funding wallet either drained the pool earlier (during a sibling
  // redemption's settlement) or has nothing to claim. Either way we proceed
  // with the per-redemption share count computed above.
  try {
    const tx = await poolContract.claimWinnings({ gasLimit: PROMO_CLAIM_GAS_LIMIT });
    await tx.wait(PROMO_TX_CONFIRMATIONS);
  } catch (err: any) {
    // Don't fail the redemption — the share count came from the trade row,
    // not the wallet's live balance. Log for ops visibility.
    console.warn("[settleFreeBet] claimWinnings reverted (likely already drained)", {
      redemptionId,
      msg: String(err?.message || "").slice(0, 200),
    });
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

    // ── Synthetic CLAIM row in user_trade_events ──────────────────────────
    // The on-chain Claim event is emitted under the funding wallet's
    // address and aggregates payouts across ALL free bets on this pool, so
    // it can't be cleanly attributed to one user. We insert our own row
    // keyed to the user instead — gives the frontend's existing trade
    // history queries a clean CLAIM to render alongside the BUY.
    //
    // Shape matches the existing CLAIM convention used by persistTrades
    // when the subgraph indexes a normal Claim event:
    //   - gross_out_dec = net_out_dec = full contract payout
    //   - cost_basis_closed_dec = 0, realized_pnl_dec = 0
    //     (PnL is computed by trade-agg from BUY.gross_in vs CLAIM.gross_out)
    //   - outcome_index = NULL, outcome_code = NULL
    //     (matches binary AND multi/three-way payouts identically — no
    //      hardcoded DRAW or team-code mapping needed)
    //
    // Idempotent via deterministic id + ON CONFLICT DO NOTHING.
    const fullPayoutUsdc = formatUnits(sharesHeld, USDC_DECIMALS);

    await client.query(
      `
      INSERT INTO public.user_trade_events
        (id, user_address, game_id, league, type, side,
         outcome_index, outcome_code,
         timestamp, tx_hash,
         spot_price_bps, avg_price_bps,
         gross_in_dec, gross_out_dec, fee_dec, net_stake_dec, net_out_dec,
         cost_basis_closed_dec, realized_pnl_dec,
         beneficiary_address, promo_redemption_id)
      VALUES
        ($1, $2, $3, $4, 'CLAIM', 'C',
         null, null,
         extract(epoch from now())::bigint, $5,
         null, null,
         0, $6::numeric, 0, 0, $6::numeric,
         0, 0,
         $2, $7::uuid)
      ON CONFLICT (id) DO NOTHING
      `,
      [
        `claim-promo-${redemptionId}`,
        userAddress,                  // $2 — user_address AND beneficiary
        poolAddress,
        row.league || null,
        payoutTxHash || "",           // $5 — keep for Arbiscan traceability
        fullPayoutUsdc,               // $6 — gross_out AND net_out (full contract payout)
        redemptionId,                 // $7
      ]
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
