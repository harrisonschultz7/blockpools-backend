// src/services/promotions/placeFreeBet.ts
//
// Funding wallet places a single free bet on behalf of a redemption-holding
// user. The user never owns the shares — the funding wallet does. That's what
// makes the bet structurally non-sellable and non-withdrawable for the user
// pre-settlement.
//
// Idempotent on retry: if a bet_funded ledger entry already exists for this
// redemption, we return the redemption's current state instead of placing
// again.

import { Contract } from "@ethersproject/contracts";
import { BigNumber } from "@ethersproject/bignumber";
import { parseUnits, formatUnits } from "@ethersproject/units";

import { pool } from "../../db";
import {
  USDC_DECIMALS,
  PROMO_TX_CONFIRMATIONS,
  PROMO_BUY_GAS_LIMIT,
} from "../../config/promo";
import { getFundingWallet } from "./findFundingWallet";
import { hasBetFundedEntry, writeLedgerEntry } from "./promotionFunding";
import { triggerFundingWalletAttributionRefresh } from "./handlePromoTradeAttribution";

// Minimal pool ABI covering both pool variants in this codebase:
//   - Multi-outcome (gamePoolMulti): buy(uint8 outcome, uint256, uint256)
//   - Binary (gamePool):              buyTeamA(uint256, uint256) / buyTeamB(...)
// We branch on games.market_type to pick the right method.
const POOL_ABI = [
  // multi
  "function buy(uint8 outcome, uint256 grossAmount, uint256 minSharesOut)",
  "function currentPriceBps(uint8 outcome) view returns (uint256)",
  "function isResolved() view returns (bool)",
  // binary
  "function buyTeamA(uint256 grossAmount, uint256 minSharesOut)",
  "function buyTeamB(uint256 grossAmount, uint256 minSharesOut)",
  // shared
  "function isLocked() view returns (bool)",
];

const ERC20_APPROVE_ABI = [
  "function allowance(address owner, address spender) view returns (uint256)",
  "function approve(address spender, uint256 amount) returns (bool)",
];

export type PlaceFreeBetError =
  | "REDEMPTION_NOT_FOUND"
  | "REDEMPTION_NOT_ELIGIBLE"
  | "REDEMPTION_EXPIRED"
  | "ADDRESS_MISMATCH"
  | "POOL_INELIGIBLE"
  | "POOL_LOCKED_OR_FINAL"
  | "PRICE_OUT_OF_BAND"
  | "FUNDING_WALLET_MISMATCH"
  | "INSUFFICIENT_FUNDING_BALANCE"
  | "ON_CHAIN_TX_FAILED";

export class PlaceFreeBetException extends Error {
  code: PlaceFreeBetError;
  detail?: any;
  constructor(code: PlaceFreeBetError, detail?: any) {
    super(code);
    this.code = code;
    this.detail = detail;
  }
}

export type PlaceFreeBetInput = {
  redemptionId: string;
  poolAddress: string;
  outcomeIndex: number;
  userAddress: string;
};

export type PlaceFreeBetResult = {
  redemptionId: string;
  txHash: string;
  poolAddress: string;
  outcomeIndex: number;
  creditUsdc: string;
  status: "placed";
  alreadyPlaced?: boolean;
};

export async function placeFreeBet(
  input: PlaceFreeBetInput
): Promise<PlaceFreeBetResult> {
  const redemptionId = String(input.redemptionId);
  const poolAddress = String(input.poolAddress).toLowerCase();
  const outcomeIndex = Number(input.outcomeIndex);
  const userAddress = String(input.userAddress).toLowerCase();

  // Idempotency short-circuit: if the ledger already has a bet_funded row for
  // this redemption, this is a retry — don't re-place.
  if (await hasBetFundedEntry(redemptionId)) {
    const r = await pool.query(
      `SELECT pool_address, outcome_index, tx_hash, credit_usdc
         FROM public.promo_redemptions WHERE id = $1`,
      [redemptionId]
    );
    const row = r.rows[0];
    if (row?.tx_hash) {
      return {
        redemptionId,
        txHash: row.tx_hash,
        poolAddress: String(row.pool_address || "").toLowerCase(),
        outcomeIndex: Number(row.outcome_index ?? outcomeIndex),
        creditUsdc: String(row.credit_usdc),
        status: "placed",
        alreadyPlaced: true,
      };
    }
  }

  // Load redemption + promotion guardrails.
  const q = await pool.query(
    `
    SELECT
      r.id,
      r.user_address,
      r.status,
      r.expires_at,
      r.credit_usdc,
      p.id                           AS promotion_id,
      p.eligible_leagues,
      p.eligible_pool_addresses,
      p.min_odds_bps,
      p.max_odds_bps,
      p.credit_usdc                  AS promo_credit_usdc,
      p.funding_wallet_address       AS promo_funding_wallet
    FROM public.promo_redemptions r
    JOIN public.promotions p ON p.id = r.promotion_id
    WHERE r.id = $1
    `,
    [redemptionId]
  );
  const red = q.rows[0];
  if (!red) throw new PlaceFreeBetException("REDEMPTION_NOT_FOUND");
  if (red.status !== "eligible") {
    throw new PlaceFreeBetException("REDEMPTION_NOT_ELIGIBLE", { current: red.status });
  }
  if (red.expires_at && new Date(red.expires_at).getTime() < Date.now()) {
    throw new PlaceFreeBetException("REDEMPTION_EXPIRED");
  }
  if (String(red.user_address).toLowerCase() !== userAddress) {
    throw new PlaceFreeBetException("ADDRESS_MISMATCH");
  }

  // Pool must be in the promo's allow-list (either by league or by explicit
  // pool address). If both eligibility lists are null the campaign accepts
  // any pool — we still require the pool to exist in `games`.
  const gameQ = await pool.query(
    `SELECT game_id, league, is_final, lock_time, market_type
       FROM public.games WHERE lower(game_id) = $1`,
    [poolAddress]
  );
  const game = gameQ.rows[0];
  if (!game) {
    throw new PlaceFreeBetException("POOL_INELIGIBLE", { reason: "game_not_found" });
  }
  const marketType = String(game.market_type || "").toUpperCase();

  const eligibleLeagues: string[] | null = red.eligible_leagues;
  const eligiblePools: string[] | null = red.eligible_pool_addresses;
  if (eligibleLeagues && eligibleLeagues.length) {
    const leagueOk = eligibleLeagues
      .map((s) => String(s).toUpperCase())
      .includes(String(game.league || "").toUpperCase());
    if (!leagueOk) {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", { reason: "league_not_allowed" });
    }
  } else if (eligiblePools && eligiblePools.length) {
    const poolOk = eligiblePools
      .map((s) => String(s).toLowerCase())
      .includes(poolAddress);
    if (!poolOk) {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", { reason: "pool_not_allowed" });
    }
  }

  if (game.is_final) {
    throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", { reason: "is_final" });
  }
  if (game.lock_time != null) {
    const lockMs = Number(game.lock_time) * 1000;
    if (lockMs <= Date.now()) {
      throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", { reason: "lock_time_passed" });
    }
  }

  // Wire up funding wallet + on-chain reads.
  const { wallet, usdc } = getFundingWallet();

  // Defense in depth: refuse to place if the env-configured wallet doesn't
  // match the campaign's funding_wallet_address. Catches cross-environment
  // misconfig (test wallet env in prod, etc.).
  const promoFundingWallet = String(red.promo_funding_wallet || "").toLowerCase();
  if (promoFundingWallet && promoFundingWallet !== wallet.address.toLowerCase()) {
    throw new PlaceFreeBetException("FUNDING_WALLET_MISMATCH", {
      promoFundingWallet,
      configured: wallet.address.toLowerCase(),
    });
  }

  const poolContract = new Contract(poolAddress, POOL_ABI, wallet);

  // Defense in depth: also confirm the contract itself isn't locked/resolved.
  try {
    const [locked, resolved] = await Promise.all([
      poolContract.isLocked(),
      poolContract.isResolved(),
    ]);
    if (locked || resolved) {
      throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", {
        reason: "contract_state",
        locked,
        resolved,
      });
    }
  } catch (err: any) {
    if (err instanceof PlaceFreeBetException) throw err;
    // Best-effort — old pools may not implement these getters.
    console.warn("[placeFreeBet] pool state read failed; relying on DB gate", err?.message);
  }

  // Odds guardrail (bps). Reject if the live price is outside the allowed
  // band. min/max may be null, in which case that side is unbounded.
  // Binary pools don't expose currentPriceBps — skip the read entirely if
  // we're on a binary pool, or if no bounds are set on the campaign.
  let priceBps: number | null = null;
  const hasOddsBand = red.min_odds_bps != null || red.max_odds_bps != null;
  const supportsPriceRead = marketType !== "BINARY";

  if (supportsPriceRead) {
    try {
      const raw = await poolContract.currentPriceBps(outcomeIndex);
      priceBps = Number(raw.toString());
    } catch (err: any) {
      if (hasOddsBand) {
        throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", {
          reason: "price_read_failed",
          detail: err?.message,
        });
      }
      console.warn(
        "[placeFreeBet] currentPriceBps unavailable; skipping band check (no bounds on campaign)",
        err?.message
      );
    }
  } else if (hasOddsBand) {
    // Binary pool with odds band on the campaign — we can't enforce it.
    // Refuse rather than silently bypass.
    throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", {
      reason: "binary_pool_does_not_support_odds_band",
      marketType,
    });
  }

  if (priceBps != null) {
    if (red.min_odds_bps != null && priceBps < Number(red.min_odds_bps)) {
      throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", {
        priceBps,
        min: red.min_odds_bps,
      });
    }
    if (red.max_odds_bps != null && priceBps > Number(red.max_odds_bps)) {
      throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", {
        priceBps,
        max: red.max_odds_bps,
      });
    }
  }

  const creditUsdc = String(red.credit_usdc ?? red.promo_credit_usdc);
  const grossAmount = parseUnits(creditUsdc, USDC_DECIMALS);

  // Pre-check funding wallet balance so we fail loudly before sending any tx.
  const balance: BigNumber = await usdc.balanceOf(wallet.address);
  if (balance.lt(grossAmount)) {
    throw new PlaceFreeBetException("INSUFFICIENT_FUNDING_BALANCE", {
      have: formatUnits(balance, USDC_DECIMALS),
      need: creditUsdc,
    });
  }

  // Ensure USDC allowance for the pool. Approve generously (max uint256) once
  // per pool to avoid one-extra-tx-per-bet overhead. If allowance already
  // covers grossAmount, skip.
  const usdcWithApprove = new Contract(usdc.address, ERC20_APPROVE_ABI, wallet);
  const currentAllowance: BigNumber = await usdcWithApprove.allowance(
    wallet.address,
    poolAddress
  );
  if (currentAllowance.lt(grossAmount)) {
    const MAX_UINT256 = BigNumber.from(
      "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    );
    const approveTx = await usdcWithApprove.approve(poolAddress, MAX_UINT256);
    await approveTx.wait(PROMO_TX_CONFIRMATIONS);
  }

  // Place the buy. minSharesOut = 0 for now — we already gated on price band
  // (when applicable) above. Tighten later via a quoteBuyTeam{A,B}/quoteBuy
  // call + slippage if MEV/front-running is observed.
  //
  // Branch by market_type:
  //   - BINARY  → buyTeamA(amount, minSharesOut) for outcome 0,
  //               buyTeamB(amount, minSharesOut) for outcome 1.
  //   - else    → buy(uint8 outcome, amount, minSharesOut) (multi).
  let txHash: string;
  try {
    let tx;
    if (marketType === "BINARY") {
      if (outcomeIndex === 0) {
        tx = await poolContract.buyTeamA(grossAmount, 0, {
          gasLimit: PROMO_BUY_GAS_LIMIT,
        });
      } else if (outcomeIndex === 1) {
        tx = await poolContract.buyTeamB(grossAmount, 0, {
          gasLimit: PROMO_BUY_GAS_LIMIT,
        });
      } else {
        throw new PlaceFreeBetException("POOL_INELIGIBLE", {
          reason: "binary_pool_only_supports_outcome_0_or_1",
          outcomeIndex,
        });
      }
    } else {
      tx = await poolContract.buy(outcomeIndex, grossAmount, 0, {
        gasLimit: PROMO_BUY_GAS_LIMIT,
      });
    }
    const receipt = await tx.wait(PROMO_TX_CONFIRMATIONS);
    if (!receipt || receipt.status !== 1) {
      throw new PlaceFreeBetException("ON_CHAIN_TX_FAILED", { txHash: tx.hash });
    }
    txHash = tx.hash;
  } catch (err: any) {
    if (err instanceof PlaceFreeBetException) throw err;
    throw new PlaceFreeBetException("ON_CHAIN_TX_FAILED", {
      detail: err?.message ?? String(err),
    });
  }

  // Persist DB transitions in one transaction so a redemption can't be marked
  // 'placed' without the matching ledger entry.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(
      `
      UPDATE public.promo_redemptions
         SET status        = 'placed',
             pool_address  = $1,
             outcome_index = $2,
             placed_at     = now(),
             tx_hash       = $3
       WHERE id = $4
         AND status = 'eligible'
      `,
      [poolAddress, outcomeIndex, txHash, redemptionId]
    );

    await client.query(
      `INSERT INTO public.promo_eligibility_events
         (redemption_id, event_type, event_data)
       VALUES ($1, 'placed', $2::jsonb)`,
      [
        redemptionId,
        JSON.stringify({
          poolAddress,
          outcomeIndex,
          txHash,
          priceBps,
          creditUsdc,
        }),
      ]
    );

    await writeLedgerEntry(
      {
        promotionId: red.promotion_id,
        redemptionId,
        direction: "bet_funded",
        amountUsdc: creditUsdc,
        txHash,
      },
      client
    );

    await client.query("COMMIT");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    // The on-chain bet succeeded but the DB write failed. Reconciliation
    // (reconcilePromoFunding) will surface this. Don't throw a 5xx that hides
    // the tx hash from the caller — log loudly and rethrow.
    console.error(
      "[placeFreeBet] DB persist failed AFTER successful on-chain buy",
      { redemptionId, txHash, err }
    );
    throw err;
  } finally {
    client.release();
  }

  // Fire-and-forget: pull the funding wallet's freshly-confirmed BUY from the
  // subgraph and persist it. Stamps beneficiary_address + promo_redemption_id
  // on the trade row via persistTrades' pre-insert hook so the bet lands in
  // user-facing stats immediately. Retries with backoff to absorb subgraph
  // indexing lag. Never blocks placeFreeBet's response — the tx hash is
  // already returned to the caller.
  triggerFundingWalletAttributionRefresh(txHash, redemptionId).catch((err) => {
    console.error(
      "[placeFreeBet] background attribution refresh threw (non-blocking)",
      { redemptionId, txHash, err }
    );
  });

  return {
    redemptionId,
    txHash,
    poolAddress,
    outcomeIndex,
    creditUsdc,
    status: "placed",
  };
}
