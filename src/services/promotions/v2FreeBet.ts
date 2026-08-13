// src/services/promotions/v2FreeBet.ts
//
// v2 (order-book) primitives for the promo free-bet system. A free bet on a v2
// market is placed by the funding wallet exactly the way the seed bot places its
// own orders: sign an EIP-712 Order (maker = funding wallet) and POST it to the
// off-chain matcher, which crosses it and settles on-chain via Exchange. The
// funding wallet ends up HOLDING the outcome shares — identical custody to the
// AMM promo path (the user never owns the stake). Settlement redeems those shares
// at the MarketVault.
//
// This module is intentionally standalone (no DB) so placeFreeBet/settleFreeBet
// keep their existing DB state-machine and just swap the on-chain mechanism.

import { Contract } from "@ethersproject/contracts";
import { BigNumber } from "@ethersproject/bignumber";
import { randomBytes } from "@ethersproject/random";

import { getFundingWallet } from "./findFundingWallet";
import {
  assertPromoV2Config,
  PROMO_EXCHANGE_ADDRESS,
  PROMO_VAULT_ADDRESS,
  PROMO_MATCHER_URL,
  PROMO_V2_CHAIN_ID,
  PROMO_V2_FEE_RATE_BPS,
  PROMO_V2_MAX_PRICE,
  USDC_ADDRESS,
  PROMO_TX_CONFIRMATIONS,
} from "../../config/promo";

// EIP-712 order typing — MUST mirror Exchange.sol (name "BlockPoolsExchange",
// version "1"). Identical to seed-bot.js / mm-ladder.js.
const ORDER_TYPES = {
  Order: [
    { name: "maker", type: "address" },
    { name: "marketId", type: "bytes32" },
    { name: "outcome", type: "uint8" },
    { name: "side", type: "uint8" },
    { name: "shares", type: "uint256" },
    { name: "price", type: "uint256" },
    { name: "feeRateBps", type: "uint256" },
    { name: "expiry", type: "uint256" },
    { name: "salt", type: "uint256" },
  ],
};
const SIDE_BUY = 0;

const ERC20_APPROVE_ABI = [
  "function allowance(address owner, address spender) view returns (uint256)",
  "function approve(address spender, uint256 amount) returns (bool)",
];

// MarketVault surface we need: read holdings + redeem winnings. Matches the ABI
// the seed bot uses.
const VAULT_ABI = [
  "function sharesOf(bytes32 marketId, uint8 outcome, address user) view returns (uint256)",
  "function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)",
  "function redeem(bytes32 marketId)",
];

export type V2QuoteResult = {
  shares: string; // 6dp share units
  spent: string; // USDC micro-units actually consumed
  avgPrice: string; // 1e6 scale ($1 == 1e6)
  worstPrice: string; // 1e6 scale
  insufficient: boolean;
  availableAmount: string; // USDC micro-units fillable across the whole book
};

async function matcherGet(path: string): Promise<any> {
  const res = await fetch(`${PROMO_MATCHER_URL}${path}`);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json?.error || `${path} → ${res.status}`);
  return json;
}
async function matcherPost(path: string, body: any): Promise<any> {
  const res = await fetch(`${PROMO_MATCHER_URL}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json?.error || `${path} → ${res.status}`);
  return json;
}

/** Simulate a market BUY of `amountMicro` USDC on (marketId, outcome). */
export async function quoteV2MarketBuy(
  marketId: string,
  outcome: number,
  amountMicro: string
): Promise<V2QuoteResult> {
  const q = new URLSearchParams({
    marketId: String(marketId),
    outcome: String(outcome),
    amount: String(amountMicro),
  });
  const j = await matcherGet(`/quote?${q.toString()}`);
  return {
    shares: String(j.shares ?? "0"),
    spent: String(j.spent ?? "0"),
    avgPrice: String(j.avgPrice ?? "0"),
    worstPrice: String(j.worstPrice ?? "0"),
    insufficient: !!j.insufficient,
    availableAmount: String(j.availableAmount ?? "0"),
  };
}

/** One-time (idempotent) USDC approval so matched fills can pull the funding
 *  wallet's USDC into the Exchange. Mirrors seed-bot `--approve`. */
export async function ensureExchangeApproval(needMicro: BigNumber): Promise<void> {
  assertPromoV2Config();
  const { wallet } = getFundingWallet();
  const usdc = new Contract(USDC_ADDRESS, ERC20_APPROVE_ABI, wallet);
  const current: BigNumber = await usdc.allowance(wallet.address, PROMO_EXCHANGE_ADDRESS);
  if (current.gte(needMicro)) return;
  const MAX_UINT256 = BigNumber.from(
    "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
  );
  const tx = await usdc.approve(PROMO_EXCHANGE_ADDRESS, MAX_UINT256);
  await tx.wait(PROMO_TX_CONFIRMATIONS);
}

export type V2PlaceResult = {
  orderHash: string; // matcher order hash — used as the redemption's tx identifier
  filledShares: string; // 6dp share units actually taken
  restedShares: string; // 6dp share units left unfilled (cancelled by caller flow)
  avgPriceScaled: string; // 1e6 scale
};

/**
 * Sign + submit a crossing BUY for the funding wallet and return what filled.
 * Prices the order at `min(worstPrice, PROMO_V2_MAX_PRICE)` so it crosses the
 * exact levels the quote consumed while bounding worst-case overpay. Cancels any
 * unfilled remainder so no stray resting order is left on the book.
 */
export async function placeV2Order(opts: {
  marketId: string;
  outcome: number;
  shares: string; // 6dp share units (from the quote)
  worstPriceScaled: string; // 1e6 scale (from the quote)
}): Promise<V2PlaceResult> {
  assertPromoV2Config();
  const { wallet } = getFundingWallet();

  // Limit price: cross everything the quote consumed, capped so a moving book
  // can't make us pay more than PROMO_V2_MAX_PRICE per share.
  let priceScaled = BigNumber.from(opts.worstPriceScaled || "0");
  const cap = BigNumber.from(PROMO_V2_MAX_PRICE);
  if (priceScaled.lte(0) || priceScaled.gt(cap)) priceScaled = cap;

  const order = {
    maker: wallet.address,
    marketId: opts.marketId,
    outcome: Number(opts.outcome),
    side: SIDE_BUY,
    shares: String(opts.shares),
    price: priceScaled.toString(),
    feeRateBps: String(PROMO_V2_FEE_RATE_BPS),
    expiry: "0",
    salt: BigNumber.from(randomBytes(8)).toString(),
  };

  const domain = {
    name: "BlockPoolsExchange",
    version: "1",
    chainId: PROMO_V2_CHAIN_ID,
    verifyingContract: PROMO_EXCHANGE_ADDRESS,
  };

  // ethers v5 wallet (from findFundingWallet) → _signTypedData.
  const sig: string = await (wallet as any)._signTypedData(domain, ORDER_TYPES, order);

  const result = await matcherPost(`/orders`, { order, sig });
  const orderHash = String(result?.hash || "");
  const rested = BigNumber.from(String(result?.rested ?? "0"));
  const requested = BigNumber.from(order.shares);
  const filled = requested.gt(rested) ? requested.sub(rested) : BigNumber.from(0);

  // Pull any unfilled remainder so we don't leave a resting promo order that
  // could fill later (which would spend funding USDC outside a redemption).
  if (rested.gt(0) && orderHash) {
    try {
      await matcherPost(`/cancel`, { hash: orderHash });
    } catch {
      /* best-effort — the remainder expires/settles harmlessly if this fails */
    }
  }

  return {
    orderHash,
    filledShares: filled.toString(),
    restedShares: rested.toString(),
    avgPriceScaled: priceScaled.toString(),
  };
}

/** On-chain market state from the vault — the authoritative lock/resolution
 *  gate for a v2 free bet (v2 markets have no per-game DB row to gate on). */
export async function v2MarketState(
  marketId: string
): Promise<{ exists: boolean; resolved: boolean; lockTime: number }> {
  const { wallet } = getFundingWallet();
  const vault = new Contract(PROMO_VAULT_ADDRESS, VAULT_ABI, wallet);
  const m = await vault.markets(marketId);
  return { exists: !!m.exists, resolved: !!m.resolved, lockTime: Number(m.lockTime) };
}

/** Funding wallet's current share holdings for a market outcome (6dp units). */
export async function v2SharesOf(
  marketId: string,
  outcome: number
): Promise<BigNumber> {
  const { wallet } = getFundingWallet();
  const vault = new Contract(PROMO_VAULT_ADDRESS, VAULT_ABI, wallet);
  return vault.sharesOf(marketId, outcome, wallet.address);
}

/**
 * Redeem the funding wallet's winning shares for a resolved v2 market. Like the
 * AMM `claimWinnings`, this is idempotent across sibling redemptions on the same
 * market (first caller drains the wallet's position; later calls revert or
 * return nothing) — callers treat any revert as benign and rely on the
 * per-redemption share count computed from stored placement data.
 */
export async function redeemV2(marketId: string): Promise<string | null> {
  assertPromoV2Config();
  const { wallet } = getFundingWallet();
  const vault = new Contract(PROMO_VAULT_ADDRESS, VAULT_ABI, wallet);
  const tx = await vault.redeem(marketId);
  const receipt = await tx.wait(PROMO_TX_CONFIRMATIONS);
  return receipt?.transactionHash || tx.hash || null;
}
