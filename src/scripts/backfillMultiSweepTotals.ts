// src/scripts/backfillMultiSweepTotals.ts
//
// One-time backfill. Multi / N-way pools (league-winner, three-way) were swept
// before the sweeper learned to record their lifetime LP funding + fees, so
// their sweeps rows have a null lp_funded_total. The game_accounting view then
// treats LP as $0 and overstates net P&L for those games. This script
// chain-scans each already-swept multi pool's LPFunded / Buy / ExitedAtMarketPrice
// events and fills the lifetime accounting columns on the row:
//   lp_funded_total, lp_funded_count, total_fees_1pct,
//   withdraw_count, withdraw_net_payout_total, withdraw_fees_total
//
// Only pools exposing outcomesCount() (i.e. GamePoolMulti) are touched; binary
// rows already carry these fields and are skipped. Idempotent — re-running
// recomputes from chain and overwrites with the same values.
//
// Run (from blockpools-backend):
//   npm run build && node dist/scripts/backfillMultiSweepTotals.js
// Dry run (scan + log, no DB writes):
//   DRY_RUN=1 node dist/scripts/backfillMultiSweepTotals.js

import "dotenv/config";
import { JsonRpcProvider } from "@ethersproject/providers";
import { Contract } from "@ethersproject/contracts";
import { BigNumber } from "@ethersproject/bignumber";
import { Interface } from "@ethersproject/abi";

import { pool } from "../db";
import { PROMO_RPC_URL } from "../config/promo";

const DRY_RUN = !!process.env.DRY_RUN;

// Probe: only GamePoolMulti exposes outcomesCount().
const DETECT_ABI = ["function outcomesCount() view returns (uint256)"];

// Event surface for the lifetime scan (mirrors getLifetimeEventTotalsMulti in
// the sweeper). LPFunded has the same signature on binary and multi pools.
const MULTI_EVENTS_ABI = [
  "event Buy(address indexed user, bytes32 indexed marketId, string league, uint8 indexed outcome, string outcomeCode, uint256 grossAmount, uint256 netStake, uint256 fee, uint256 sharesOut, uint256 spotPriceBps, uint256 avgPriceBps)",
  "event ExitedAtMarketPrice(address indexed user, uint8 indexed outcome, uint256 sharesIn, uint256 spotPriceBps, uint256 avgPriceBps, uint256 postPriceBps, uint256 grossValue, uint256 fee, uint256 netPayout)",
  "event LPFunded(uint256 amount)",
];

async function scanMulti(provider: JsonRpcProvider, address: string) {
  const iface = new Interface(MULTI_EVENTS_ABI);
  const buyTopic = iface.getEventTopic("Buy");
  const lpTopic = iface.getEventTopic("LPFunded");
  const exitTopic = iface.getEventTopic("ExitedAtMarketPrice");

  const latest = await provider.getBlockNumber();
  const logs = await provider.getLogs({
    address,
    fromBlock: 0,
    toBlock: latest,
    topics: [[buyTopic, lpTopic, exitTopic]],
  });

  let fees = BigNumber.from(0);
  let lpFundedTotal = BigNumber.from(0);
  let lpFundedCount = 0;
  let withdrawCount = 0;
  let withdrawNetPayout = BigNumber.from(0);
  let withdrawFees = BigNumber.from(0);

  for (const log of logs) {
    let parsed;
    try {
      parsed = iface.parseLog(log);
    } catch {
      continue; // unrelated event (Claimed/ResultPosted/etc.)
    }
    if (parsed.name === "Buy") {
      fees = fees.add(parsed.args.fee);
    } else if (parsed.name === "LPFunded") {
      lpFundedTotal = lpFundedTotal.add(parsed.args.amount);
      lpFundedCount += 1;
    } else if (parsed.name === "ExitedAtMarketPrice") {
      withdrawCount += 1;
      withdrawNetPayout = withdrawNetPayout.add(parsed.args.netPayout);
      withdrawFees = withdrawFees.add(parsed.args.fee);
    }
  }

  return { fees, lpFundedTotal, lpFundedCount, withdrawCount, withdrawNetPayout, withdrawFees };
}

async function main() {
  const provider = new JsonRpcProvider(PROMO_RPC_URL);

  // Candidates: swept rows missing lp_funded_total. Binary pools always record
  // it, so these are the multi pools (plus the outcomesCount() gate below).
  const { rows } = await pool.query(
    `SELECT chain_id, contract_address, tx_hash
       FROM public.sweeps
      WHERE lp_funded_total IS NULL OR lp_funded_total = ''`
  );
  console.log(`[backfill] ${rows.length} candidate sweep row(s) missing lp_funded_total`);

  let updated = 0;
  let skipped = 0;
  let errors = 0;

  for (const r of rows) {
    const addr = String(r.contract_address);
    try {
      // outcomesCount() exists only on GamePoolMulti — gate on it so we never
      // touch a binary row that happens to be missing lp_funded_total.
      let isMulti = true;
      try {
        await new Contract(addr, DETECT_ABI, provider).outcomesCount();
      } catch {
        isMulti = false;
      }
      if (!isMulti) {
        skipped += 1;
        continue;
      }

      const t = await scanMulti(provider, addr);
      console.log(
        `[backfill] ${addr} lp=${t.lpFundedTotal.toString()} fees=${t.fees.toString()} ` +
          `lpCount=${t.lpFundedCount} withdraws=${t.withdrawCount}`
      );

      if (DRY_RUN) {
        updated += 1;
        continue;
      }

      await pool.query(
        `UPDATE public.sweeps
            SET lp_funded_total           = $1,
                lp_funded_count           = $2,
                total_fees_1pct           = $3,
                withdraw_count            = $4,
                withdraw_net_payout_total = $5,
                withdraw_fees_total       = $6
          WHERE chain_id = $7 AND contract_address = $8 AND tx_hash = $9`,
        [
          t.lpFundedTotal.toString(),
          String(t.lpFundedCount),
          t.fees.toString(),
          String(t.withdrawCount),
          t.withdrawNetPayout.toString(),
          t.withdrawFees.toString(),
          r.chain_id,
          addr,
          r.tx_hash,
        ]
      );
      updated += 1;
    } catch (e: any) {
      errors += 1;
      console.error(`[backfill] error for ${addr}: ${e?.message ?? e}`);
    }
  }

  console.log(
    `[backfill] done — updated=${updated} skipped(non-multi)=${skipped} errors=${errors}` +
      (DRY_RUN ? " (DRY_RUN — no writes)" : "")
  );
}

main()
  .then(() => pool.end())
  .catch((err) => {
    console.error("[backfill] unhandled", err);
    process.exit(1);
  });
