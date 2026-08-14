// src/services/marketSnapshot.ts
//
// Server-cached snapshot of AMM (v1) game-pool state so the homepage market
// cards render with ZERO browser eth_calls.
//
// Before: every GameInfoHomeBinary card fired its own 8-call multicall every
// ~30s, PER USER — the bulk of our Alchemy eth_call bill (grew with cards ×
// concurrent visitors). Now: ONE Multicall3 `aggregate3` over all active pools,
// at most once per SNAPSHOT_TTL, cached in memory and served to every visitor.
// So the browser makes 0 eth_calls and the backend makes ~1 read / 30s total.

import { ethers } from "ethers";
import { pool } from "../db";

const RPC_URL = (
  process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc"
).trim();
const MULTICALL3 = process.env.MULTICALL3_ADDRESS || "0xcA11bde05977b3631167028862bE2a173976CA11";
const SNAPSHOT_TTL_MS = Number(process.env.MARKET_SNAPSHOT_TTL_MS || 30_000);
const CHUNK = Number(process.env.MARKET_SNAPSHOT_CHUNK || 40); // pools per aggregate3 (gas-bounded)

// The 8 methods each homepage card used to read on-chain, in order.
const POOL_ABI = [
  "function isLocked() view returns (bool)",
  "function spotAllInPriceBpsTeamA() view returns (uint256)",
  "function spotAllInPriceBpsTeamB() view returns (uint256)",
  "function teamACode() view returns (string)",
  "function teamBCode() view returns (string)",
  "function currentBuyFeeBpsTeamA() view returns (uint256)",
  "function currentBuyFeeBpsTeamB() view returns (uint256)",
  "function maxBetPerTx() view returns (uint256)",
];
const METHODS = [
  "isLocked",
  "spotAllInPriceBpsTeamA",
  "spotAllInPriceBpsTeamB",
  "teamACode",
  "teamBCode",
  "currentBuyFeeBpsTeamA",
  "currentBuyFeeBpsTeamB",
  "maxBetPerTx",
] as const;

const MULTICALL_ABI = [
  "function aggregate3(tuple(address target, bool allowFailure, bytes callData)[] calls) view returns (tuple(bool success, bytes returnData)[])",
];

export type MarketSnapshot = {
  isLocked: boolean | null;
  spotABps: number | null;
  spotBBps: number | null;
  teamACode: string | null;
  teamBCode: string | null;
  feeABps: number | null;
  feeBBps: number | null;
  maxBet: string | null; // raw base-unit (6dp USDC) uint string
};

let cache: Record<string, MarketSnapshot> = {};
let updatedAt = 0;
let inFlight: Promise<void> | null = null;

let _provider: ethers.JsonRpcProvider | null = null;
function provider(): ethers.JsonRpcProvider {
  if (!_provider) _provider = new ethers.JsonRpcProvider(RPC_URL);
  return _provider;
}

/** Active v1 AMM pools = not finalized, game_id is a pool contract address.
 *  (v2 games use a string game_id like "MLB-…", so `0x%` selects v1 only.) */
async function activePoolAddresses(): Promise<string[]> {
  const { rows } = await pool.query(
    `SELECT DISTINCT lower(game_id) AS addr
       FROM public.games
      WHERE is_final IS NOT TRUE
        AND game_id LIKE '0x%'`
  );
  return rows
    .map((r: any) => String(r.addr))
    .filter((a: string) => /^0x[0-9a-f]{40}$/.test(a));
}

async function refresh(): Promise<void> {
  const addrs = await activePoolAddresses();
  if (!addrs.length) {
    cache = {};
    updatedAt = Date.now();
    return;
  }
  const iface = new ethers.Interface(POOL_ABI);
  const mc = new ethers.Contract(MULTICALL3, MULTICALL_ABI, provider());
  const next: Record<string, MarketSnapshot> = { ...cache };

  for (let i = 0; i < addrs.length; i += CHUNK) {
    const batch = addrs.slice(i, i + CHUNK);
    const calls = batch.flatMap((addr) =>
      METHODS.map((m) => ({
        target: addr,
        allowFailure: true,
        callData: iface.encodeFunctionData(m),
      }))
    );
    let res: any[];
    try {
      res = await mc.aggregate3(calls); // ← ONE eth_call for the whole batch
    } catch (e: any) {
      console.warn("[marketSnapshot] aggregate3 chunk failed; keeping prior cache", e?.message || e);
      continue;
    }
    for (let p = 0; p < batch.length; p++) {
      const addr = batch[p];
      const dec = (j: number): any => {
        const r = res[p * METHODS.length + j];
        if (!r || !r.success || r.returnData === "0x") return null;
        try {
          return iface.decodeFunctionResult(METHODS[j], r.returnData)[0];
        } catch {
          return null;
        }
      };
      const vals = METHODS.map((_, j) => dec(j));
      const num = (v: any) => (v == null ? null : Number(v));
      next[addr] = {
        isLocked: vals[0] == null ? null : Boolean(vals[0]),
        spotABps: num(vals[1]),
        spotBBps: num(vals[2]),
        teamACode: vals[3] == null ? null : String(vals[3]),
        teamBCode: vals[4] == null ? null : String(vals[4]),
        feeABps: num(vals[5]),
        feeBBps: num(vals[6]),
        maxBet: vals[7] == null ? null : String(vals[7]),
      };
    }
  }

  // Drop pools that finalized out of the active set so the map doesn't grow.
  const activeSet = new Set(addrs);
  cache = Object.fromEntries(Object.entries(next).filter(([a]) => activeSet.has(a)));
  updatedAt = Date.now();
}

/** Cached AMM pool state for all active v1 markets. Triggers a background
 *  refresh when stale (guarded so concurrent requests don't stampede); the very
 *  first call (cold cache) awaits the refresh so it never returns {}. */
export async function getMarketSnapshots(): Promise<{
  snapshots: Record<string, MarketSnapshot>;
  updatedAt: number;
}> {
  const stale = Date.now() - updatedAt > SNAPSHOT_TTL_MS;
  if (stale && !inFlight) {
    inFlight = refresh()
      .catch((e: any) => console.warn("[marketSnapshot] refresh failed", e?.message || e))
      .finally(() => {
        inFlight = null;
      });
  }
  if (!updatedAt && inFlight) {
    try {
      await inFlight;
    } catch {
      /* served empty; next call retries */
    }
  }
  return { snapshots: cache, updatedAt };
}
