// src/scripts/backfillOnchainBuys.ts
//
// ONE-TIME backfill that reads BUY events directly from chain (NOT the subgraph),
// decodes them, and upserts them into public.user_trade_events via the normal
// persist path — which fires updatePromoProgress() so a stuck promo unlocks.
//
// Why this exists: the published subgraph stalled, so recent buys never reached
// Supabase. The CLAIM direct-write path still ran, leaving CLAIMs with no BUY
// ("orphan claims") and promos that never unlocked. This script reconstructs the
// missing BUY rows straight from Arbitrum logs.
//
// Reconciliation: rows are written with id `buy-direct-<txHash>` — the SAME id
// the frontend direct-write uses — so when the subgraph recovers,
// refreshUserTradesPage's reconcile DELETEs them by txHash and replaces them with
// the authoritative `trade-...` rows. Exposure / cost-basis are never double-counted.
//
// Run (after `npm run build`):
//   BACKFILL_POOLS=0x439c2744bc96fbb9980d5102180e5915e86f5607 \
//   BACKFILL_USERS=0x17d9a1a8be6cbc8f79ba9458b25532528ce00188 \
//   BACKFILL_FROM_BLOCK=476000000 \
//   node dist/scripts/backfillOnchainBuys.js
//
// Env:
//   BACKFILL_POOLS       comma-sep pool (game) addresses to scan. REQUIRED.
//   BACKFILL_USERS       comma-sep user addresses to limit to (optional; default = all buyers on those pools)
//   BACKFILL_FROM_BLOCK  start block (default 407204265 = subgraph startBlock)
//   BACKFILL_TO_BLOCK    end block (default = latest)
//   BACKFILL_CHUNK       getLogs block-range chunk size (default 9000)
//   RPC_URL / ARBITRUM_RPC_URL / PROMO_RPC_URL   Arbitrum RPC (default public arb1)

import { JsonRpcProvider, Interface, Contract, id as keccakId, zeroPadValue, getAddress, formatUnits } from "ethers";
import { upsertUserTradesAndGames } from "../services/persistTrades";
import { pool as dbPool } from "../db";

const RPC_URL =
  process.env.RPC_URL ||
  process.env.ARBITRUM_RPC_URL ||
  process.env.PROMO_RPC_URL ||
  "https://arb1.arbitrum.io/rpc";

// MULTI pools (gamePoolMulti.buy) — indexed uint8 outcome + string outcomeCode.
const BUY_FRAGMENT_MULTI =
  "event Buy(address indexed user, bytes32 indexed marketId, string league, uint8 indexed outcome, string outcomeCode, uint256 grossAmount, uint256 netStake, uint256 fee, uint256 sharesOut, uint256 spotPriceBps, uint256 avgPriceBps)";
const BUY_TOPIC0_MULTI = keccakId(
  "Buy(address,bytes32,string,uint8,string,uint256,uint256,uint256,uint256,uint256,uint256)"
).toLowerCase();
const ifaceMulti = new Interface([BUY_FRAGMENT_MULTI]);

// BINARY pools (gamePool.buyTeamA/B) — NO indexed outcome; a non-indexed
// `string teamCode` instead. Different signature → different topic0, so it must
// be matched and decoded separately. Outcome index is derived from teamCode via
// the pool's teamACode()/teamBCode() getters (A=0, B=1).
const BUY_FRAGMENT_BINARY =
  "event Buy(address indexed user, bytes32 indexed gameId, string league, string teamCode, uint256 grossAmount, uint256 netStake, uint256 fee, uint256 sharesOut, uint256 spotPriceBps, uint256 avgPriceBps)";
const BUY_TOPIC0_BINARY = keccakId(
  "Buy(address,bytes32,string,string,uint256,uint256,uint256,uint256,uint256,uint256)"
).toLowerCase();
const ifaceBinary = new Interface([BUY_FRAGMENT_BINARY]);

const TEAM_ABI = [
  "function teamACode() view returns (string)",
  "function teamBCode() view returns (string)",
];

const USDC_DECIMALS = 6;

function csv(name: string): string[] {
  return String(process.env[name] || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function run() {
  const pools = csv("BACKFILL_POOLS");
  const usersFilter = new Set(csv("BACKFILL_USERS"));
  if (!pools.length) {
    console.log("[backfill-onchain] BACKFILL_POOLS is required (comma-sep pool addresses). Aborting.");
    process.exit(1);
  }

  const provider = new JsonRpcProvider(RPC_URL);
  const latest = await provider.getBlockNumber();
  const fromBlock = Number(process.env.BACKFILL_FROM_BLOCK || 407204265);
  const toBlock = Number(process.env.BACKFILL_TO_BLOCK || latest);
  const chunk = Math.max(1000, Number(process.env.BACKFILL_CHUNK || 9000));

  const userTopic = usersFilter.size === 1
    ? zeroPadValue(getAddress([...usersFilter][0]), 32)
    : null; // single-user fast filter; otherwise scan all and filter in code

  console.log(
    `[backfill-onchain] rpc=${RPC_URL} pools=${pools.length} users=${usersFilter.size || "ALL"} blocks=${fromBlock}..${toBlock} chunk=${chunk}`
  );

  const blockTsCache = new Map<number, number>();
  async function blockTs(bn: number): Promise<number> {
    if (blockTsCache.has(bn)) return blockTsCache.get(bn)!;
    const b = await provider.getBlock(bn);
    const ts = b ? Number(b.timestamp) : Math.floor(Date.now() / 1000);
    blockTsCache.set(bn, ts);
    return ts;
  }

  // Match BOTH the multi and binary Buy signatures in a single getLogs (topic0
  // OR-filter). user is topic1 (indexed) in both, so the single-user fast filter
  // still applies on topic2.
  const topic0Or = [BUY_TOPIC0_MULTI, BUY_TOPIC0_BINARY];

  // Per-pool teamACode/teamBCode, read once, to map a binary event's teamCode
  // string to an outcome index (A=0, B=1). Best-effort; outcome index doesn't
  // affect the promo's held-to-settlement sum for buy-only positions, and the
  // subgraph reconcile overwrites these rows with authoritative data anyway.
  const teamCodeCache = new Map<string, { a: string; b: string }>();
  async function resolveBinaryOutcome(
    addr: string,
    teamCode: string
  ): Promise<{ index: number; code: string }> {
    const key = addr.toLowerCase();
    let codes = teamCodeCache.get(key);
    if (!codes) {
      codes = { a: "", b: "" };
      try {
        const c = new Contract(addr, TEAM_ABI, provider);
        const [a, b] = await Promise.all([c.teamACode(), c.teamBCode()]);
        codes = {
          a: String(a || "").toUpperCase().trim(),
          b: String(b || "").toUpperCase().trim(),
        };
      } catch {
        /* leave blank; fall through to index 0 (A) like the subgraph default */
      }
      teamCodeCache.set(key, codes);
    }
    const code = String(teamCode || "").toUpperCase().trim();
    if (codes.b && code === codes.b) return { index: 1, code };
    return { index: 0, code: code || codes.a }; // default A
  }

  // Collect rows grouped by user so each user's promo is evaluated once.
  const rowsByUser = new Map<string, any[]>();
  let totalLogs = 0;

  for (const poolAddr of pools) {
    let address: string;
    try {
      address = getAddress(poolAddr);
    } catch {
      console.log(`[backfill-onchain] skip invalid pool ${poolAddr}`);
      continue;
    }

    for (let start = fromBlock; start <= toBlock; start += chunk) {
      const end = Math.min(start + chunk - 1, toBlock);
      let logs;
      try {
        logs = await provider.getLogs({
          address,
          fromBlock: start,
          toBlock: end,
          topics: userTopic ? [topic0Or, userTopic] : [topic0Or],
        });
      } catch (e: any) {
        console.log(`[backfill-onchain] getLogs ${address} ${start}-${end} err: ${String(e?.message || e)} (retrying smaller)`);
        // Shrink-and-retry once for RPC range limits.
        const mid = Math.floor((start + end) / 2);
        try {
          const a = await provider.getLogs({ address, fromBlock: start, toBlock: mid, topics: userTopic ? [topic0Or, userTopic] : [topic0Or] });
          const b = await provider.getLogs({ address, fromBlock: mid + 1, toBlock: end, topics: userTopic ? [topic0Or, userTopic] : [topic0Or] });
          logs = a.concat(b);
        } catch (e2: any) {
          console.log(`[backfill-onchain] retry failed ${start}-${end}: ${String(e2?.message || e2)}`);
          continue;
        }
      }

      for (const log of logs) {
        const t0 = String((log.topics as string[])[0] || "").toLowerCase();
        const isMulti = t0 === BUY_TOPIC0_MULTI;
        const isBinary = t0 === BUY_TOPIC0_BINARY;
        if (!isMulti && !isBinary) continue;

        let parsed;
        try {
          parsed = (isMulti ? ifaceMulti : ifaceBinary).parseLog({
            topics: log.topics as string[],
            data: log.data,
          });
        } catch {
          continue;
        }
        if (!parsed) continue;

        const user = String(parsed.args.user).toLowerCase();
        if (usersFilter.size && !usersFilter.has(user)) continue;

        const txHash = String(log.transactionHash);
        const ts = await blockTs(Number(log.blockNumber));
        const league = String(parsed.args.league || "");

        // Outcome: multi carries it in the event; binary derives it from teamCode.
        let outcomeIndex: number;
        let outcomeCode: string;
        if (isMulti) {
          outcomeIndex = Number(parsed.args.outcome);
          outcomeCode = String(parsed.args.outcomeCode || "");
        } else {
          const r = await resolveBinaryOutcome(address, String(parsed.args.teamCode || ""));
          outcomeIndex = r.index;
          outcomeCode = r.code;
        }

        const row = {
          // SAME id convention as the frontend direct-write so the subgraph
          // reconcile (DELETE … id LIKE 'buy-direct-%' … tx_hash = ANY) collapses it.
          id: `buy-direct-${txHash.toLowerCase()}`,
          type: "BUY",
          side: null,
          outcomeIndex,
          outcomeCode,
          timestamp: ts,
          txHash,
          spotPriceBps: Number(parsed.args.spotPriceBps),
          avgPriceBps: Number(parsed.args.avgPriceBps),
          grossInDec: formatUnits(parsed.args.grossAmount, USDC_DECIMALS),
          grossOutDec: "0",
          feeDec: formatUnits(parsed.args.fee, USDC_DECIMALS),
          netStakeDec: formatUnits(parsed.args.netStake, USDC_DECIMALS),
          netOutDec: "0",
          costBasisClosedDec: "0",
          realizedPnlDec: "0",
          game: { id: address.toLowerCase(), league: league || null },
          __source: "buy-onchain",
        };

        if (!rowsByUser.has(user)) rowsByUser.set(user, []);
        rowsByUser.get(user)!.push(row);
        totalLogs++;
        console.log(
          `[backfill-onchain] BUY(${isMulti ? "multi" : "binary"}) user=${user} outcome=${outcomeIndex} net=${row.netStakeDec} tx=${txHash} blk=${log.blockNumber}`
        );
      }
      await sleep(60);
    }
  }

  console.log(`[backfill-onchain] decoded ${totalLogs} BUY logs across ${rowsByUser.size} users. Upserting…`);

  let okUsers = 0;
  for (const [user, rows] of rowsByUser) {
    try {
      const res = await upsertUserTradesAndGames({ user, tradeRows: rows });
      okUsers++;
      console.log(`[backfill-onchain] upserted user=${user} rows=${rows.length} -> tradesUpserted=${res.tradesUpserted}`);
    } catch (e: any) {
      console.log(`[backfill-onchain] upsert user=${user} ERR: ${String(e?.message || e)}`);
    }
  }

  console.log(`[backfill-onchain] done. users=${rowsByUser.size} okUsers=${okUsers} totalBuyRows=${totalLogs}`);

  // upsertUserTradesAndGames fires markUserHasTraded / updatePromoProgress /
  // handlePromoTradeAttribution as fire-and-forget (NOT awaited) — they run on
  // the shared pool AFTER the upsert returns. Give them time to finish before
  // we end the pool, otherwise they throw "Cannot use a pool after calling end".
  console.log("[backfill-onchain] waiting 10s for promo/stat hooks to settle…");
  await new Promise((r) => setTimeout(r, 10_000));

  await dbPool.end().catch(() => {});
  process.exit(0);
}

run().catch((e) => {
  console.log(`[backfill-onchain] fatal: ${String((e as any)?.message || e)}`);
  process.exit(1);
});
