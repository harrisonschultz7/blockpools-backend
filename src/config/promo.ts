// src/config/promo.ts
//
// Promo framework configuration. Sidecar — entire system is dormant when
// PROMO_FRAMEWORK_ENABLED is false. Read at module-load time so behavior is
// consistent for the lifetime of a process.

import "dotenv/config";
import { Wallet } from "@ethersproject/wallet";

function envBool(name: string, dflt = false): boolean {
  const v = (process.env[name] || "").toLowerCase().trim();
  if (!v) return dflt;
  return v === "1" || v === "true" || v === "yes";
}

function envStr(name: string, dflt = ""): string {
  return (process.env[name] || dflt).trim();
}

export const PROMO_FRAMEWORK_ENABLED = envBool("PROMO_FRAMEWORK_ENABLED", false);

// Single hot wallet that places all free-bet trades and receives all settlements
// for those trades. The user never holds these shares, which is what enforces
// "winnings only — stake is never withdrawable USDC" structurally.
//
// Single source of truth for the promo wallet: we reuse the SAME wallet the
// legacy /api/promo route already uses, so there's only one private key in the
// env. Resolution order for the key:
//   1. PROMO_FUNDING_WALLET_PRIVATE_KEY  (explicit, if you ever want a separate wallet)
//   2. PROMO_HOT_WALLET_PRIVATE_KEY      (the existing/regular-promotions wallet)
// The address is derived from that key unless PROMO_FUNDING_WALLET_ADDRESS is
// explicitly set — so you don't have to maintain the address separately.
export const PROMO_FUNDING_WALLET_PRIVATE_KEY =
  envStr("PROMO_FUNDING_WALLET_PRIVATE_KEY") ||
  envStr("PROMO_HOT_WALLET_PRIVATE_KEY");

function deriveAddressFromKey(pk: string): string {
  if (!pk) return "";
  try {
    const k = pk.startsWith("0x") ? pk : `0x${pk}`;
    return new Wallet(k).address.toLowerCase();
  } catch {
    // Malformed key → leave empty; assertPromoConfig() will surface the problem
    // loudly at the first on-chain action rather than crashing module load.
    return "";
  }
}

export const PROMO_FUNDING_WALLET_ADDRESS = (
  envStr("PROMO_FUNDING_WALLET_ADDRESS") ||
  deriveAddressFromKey(PROMO_FUNDING_WALLET_PRIVATE_KEY)
).toLowerCase();

// Same RPC the settlement bot uses. Defaults defer to the bot's RPC_URL so we
// don't need a second key in the env file.
export const PROMO_RPC_URL =
  envStr("PROMO_RPC_URL") ||
  envStr("RPC_URL") ||
  envStr("ARBITRUM_RPC_URL") ||
  "https://arb1.arbitrum.io/rpc";

// USDC on Arbitrum One (matches the existing legacy /api/promo route).
export const USDC_ADDRESS = envStr("USDC_ADDRESS", "0xaf88d065e77c8cC2239327C5EDb3A432268e5831");
export const USDC_DECIMALS = 6;

// How many confirmations we wait for on every funding-wallet tx.
export const PROMO_TX_CONFIRMATIONS = Number(process.env.PROMO_TX_CONFIRMATIONS || 1);

// Generous default — pool buys typically run in the few-hundred-k gas range.
export const PROMO_BUY_GAS_LIMIT = Number(process.env.PROMO_BUY_GAS_LIMIT || 600_000);
export const PROMO_CLAIM_GAS_LIMIT = Number(process.env.PROMO_CLAIM_GAS_LIMIT || 400_000);

// ── v2 order-book (Exchange + MarketVault + off-chain matcher) ────────────────
// Reuse the SAME env the seed bot / settler already read on the VPS so there's a
// single source of truth. A free bet on a v2 market is placed as a funding-wallet
// EIP-712 order to the matcher (see placeFreeBet's v2 branch) and settled by
// redeeming the winning shares at the vault (settleFreeBet's v2 branch). When
// these are unset the promo system still works for AMM pools; only v2 free bets
// are unavailable (they fail loudly via assertPromoV2Config()).
export const PROMO_EXCHANGE_ADDRESS = envStr("EXCHANGE_ADDRESS").toLowerCase();
// Same default the seed bot ships with; override via VAULT_ADDRESS.
export const PROMO_VAULT_ADDRESS = (
  envStr("VAULT_ADDRESS") || "0x14BD1fd3911C22173FeaEcBfD670D09c1143A594"
).toLowerCase();
// Local matcher on the VPS (same default as seed-bot.js / mm-ladder.js).
export const PROMO_MATCHER_URL = (
  envStr("MATCHER_URL") || "http://127.0.0.1:8090"
).replace(/\/+$/, "");
// Arbitrum One. Only override if the deployment ever moves chains.
export const PROMO_V2_CHAIN_ID = Number(process.env.CHAIN_ID || 42161);
// Max fee (bps) a promo order will accept — mirrors the seed bot's "1000" (10%).
// The live protocol fee is 0/taker-only; this is just the cap the maker signs.
export const PROMO_V2_FEE_RATE_BPS = Number(process.env.PROMO_V2_FEE_RATE_BPS || 1000);
// Highest per-share price (1e6 scale, i.e. 990000 == 99¢) a free-bet BUY will
// cross to. Bounds worst-case overpay if the book moves between quote and submit.
export const PROMO_V2_MAX_PRICE = Number(process.env.PROMO_V2_MAX_PRICE || 990_000);
// EIP-712 order price/share scale (1e6 == $1.00 / share). Matches Exchange.sol.
export const PROMO_V2_PRICE_SCALE = 1_000_000;

// Cheap helper used in the persistTrades hook. Kept here so there's a single
// canonical comparison everywhere.
export function isPromoFundingWallet(address: string | null | undefined): boolean {
  if (!PROMO_FRAMEWORK_ENABLED) return false;
  if (!address) return false;
  if (!PROMO_FUNDING_WALLET_ADDRESS) return false;
  return String(address).toLowerCase() === PROMO_FUNDING_WALLET_ADDRESS;
}

// Throws if the framework is enabled but env is incomplete. Call this at the
// top of any flow that performs an on-chain action so misconfig fails loudly
// instead of half-running.
export function assertPromoConfig(): void {
  if (!PROMO_FRAMEWORK_ENABLED) {
    throw new Error("Promo framework is disabled (PROMO_FRAMEWORK_ENABLED=false)");
  }
  if (!PROMO_FUNDING_WALLET_ADDRESS) {
    throw new Error("PROMO_FUNDING_WALLET_ADDRESS is not set");
  }
  if (!PROMO_FUNDING_WALLET_PRIVATE_KEY) {
    throw new Error("PROMO_FUNDING_WALLET_PRIVATE_KEY is not set");
  }
}

// Additional guard for v2 (order-book) free bets. Call before placing/settling a
// v2 bet so a missing Exchange address fails loudly instead of signing an order
// against the zero address.
export function assertPromoV2Config(): void {
  assertPromoConfig();
  if (!PROMO_EXCHANGE_ADDRESS) {
    throw new Error("EXCHANGE_ADDRESS is not set (required for v2 order-book free bets)");
  }
  if (!PROMO_VAULT_ADDRESS) {
    throw new Error("VAULT_ADDRESS is not set (required for v2 order-book free bets)");
  }
}
