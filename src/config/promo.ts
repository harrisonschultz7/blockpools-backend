// src/config/promo.ts
//
// Promo framework configuration. Sidecar — entire system is dormant when
// PROMO_FRAMEWORK_ENABLED is false. Read at module-load time so behavior is
// consistent for the lifetime of a process.

import "dotenv/config";

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
export const PROMO_FUNDING_WALLET_ADDRESS = envStr("PROMO_FUNDING_WALLET_ADDRESS").toLowerCase();
export const PROMO_FUNDING_WALLET_PRIVATE_KEY = envStr("PROMO_FUNDING_WALLET_PRIVATE_KEY");

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
