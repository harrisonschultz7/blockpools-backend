// src/services/promotions/findFundingWallet.ts
//
// Returns ethers v5 Wallet + Provider + a USDC contract handle backed by the
// promo funding wallet. Single source of truth so every flow that touches the
// chain on behalf of the promo system uses the same signer.

import { JsonRpcProvider } from "@ethersproject/providers";
import { Wallet } from "@ethersproject/wallet";
import { Contract } from "@ethersproject/contracts";

import {
  assertPromoConfig,
  PROMO_FUNDING_WALLET_PRIVATE_KEY,
  PROMO_RPC_URL,
  USDC_ADDRESS,
} from "../../config/promo";

// Minimal ERC20 surface — balance + transfer is all we need.
const ERC20_ABI = [
  "function balanceOf(address account) view returns (uint256)",
  "function transfer(address to, uint256 amount) returns (bool)",
  "function decimals() view returns (uint8)",
];

export type FundingWalletHandle = {
  provider: JsonRpcProvider;
  wallet: Wallet;
  usdc: Contract;
};

let cached: FundingWalletHandle | null = null;

export function getFundingWallet(): FundingWalletHandle {
  if (cached) return cached;
  assertPromoConfig();

  const provider = new JsonRpcProvider(PROMO_RPC_URL);
  const pkey = PROMO_FUNDING_WALLET_PRIVATE_KEY.startsWith("0x")
    ? PROMO_FUNDING_WALLET_PRIVATE_KEY
    : `0x${PROMO_FUNDING_WALLET_PRIVATE_KEY}`;

  const wallet = new Wallet(pkey, provider);
  const usdc = new Contract(USDC_ADDRESS, ERC20_ABI, wallet);

  cached = { provider, wallet, usdc };
  return cached;
}

// For tests / hot-reload. Not used in production.
export function _resetFundingWalletCache(): void {
  cached = null;
}
