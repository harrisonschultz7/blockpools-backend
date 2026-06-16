// scripts/resolve-stuck-multi.mjs
//
// One-shot resolver for GamePoolMulti pools that the Chainlink Functions path
// abandoned (coordinator retryCount == maxRetries). It calls the owner-only
// pool.manualSetWinner(outcomeIndex) directly, using results already confirmed
// off-chain by the settlement bot. Use this to unstick pools whose retries are
// exhausted; the source.js fix prevents NEW pools from getting here.
//
// Required env (already in settler.env):
//   RPC_URL
//   PRIVATE_KEY        (must be the pool owner — your settler wallet)
// Optional env:
//   DRY_RUN=1          (simulate; no tx sent)
//   POOLS=0xaddr:idx,0xaddr:idx   (override the default list; idx = outcome index)
//
// Usage:
//   cd /opt/blockpools/backend
//   set -a; source /etc/blockpools/settler.env; set +a
//   DRY_RUN=1 node scripts/resolve-stuck-multi.mjs      # preview
//   node scripts/resolve-stuck-multi.mjs                # execute

import { ethers } from "ethers";
try { (await import("dotenv")).config(); } catch {}

const RPC_URL = process.env.RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const DRY_RUN = /^(1|true)$/i.test(String(process.env.DRY_RUN || ""));

// outcome index = position in the pool's outcomeCodes array.
// SUI/QAT [SUI,DRAW,QAT] 1-1 -> DRAW (1)
// MAR/BRA [MAR,DRAW,BRA] 1-1 -> DRAW (1)
// SCO/HAI [SCO,DRAW,HAI] Scotland 1-0 -> SCO (0)
// TUR/AUS [TUR,DRAW,AUS] Australia 2-0 -> AUS (2)
const DEFAULT = [
  { addr: "0xAd57788eac106300AD723E8D581De03010F5c719", idx: 1, label: "SUI/QAT -> DRAW" },
  { addr: "0x71F2bD38f56d4a4C5dbE287C80Edd4e07ed57911", idx: 1, label: "MAR/BRA -> DRAW" },
  { addr: "0x2eCA5B975f8fd90f71E27A4E1BA17226e1b5C527", idx: 0, label: "SCO/HAI -> SCO" },
  { addr: "0x36831e647Fb95253Bf210BD35e602149D4fA2b1d", idx: 2, label: "TUR/AUS -> AUS" },
];

const POOL_ABI = [
  "function owner() view returns (address)",
  "function isLocked() view returns (bool)",
  "function isResolved() view returns (bool)",
  "function outcomesCount() view returns (uint8)",
  "function outcomeCode(uint8) view returns (string)",
  "function manualSetWinner(uint8 winnerOutcome) external",
];

function parsePools() {
  const raw = String(process.env.POOLS || "").trim();
  if (!raw) return DEFAULT;
  return raw.split(",").map((s) => {
    const [addr, idx] = s.split(":");
    return { addr: addr.trim(), idx: Number(idx), label: `${addr.trim()} -> idx ${idx}` };
  });
}

async function main() {
  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);
  console.log(`[resolve-stuck-multi] signer=${wallet.address} dryRun=${DRY_RUN}\n`);

  for (const { addr, idx, label } of parsePools()) {
    const pool = new ethers.Contract(addr, POOL_ABI, wallet);
    try {
      const [owner, locked, resolved, nOut] = await Promise.all([
        pool.owner(), pool.isLocked(), pool.isResolved(), pool.outcomesCount(),
      ]);

      if (resolved) { console.log(`[skip] ${label} (${addr}) already resolved`); continue; }
      if (!locked)  { console.log(`[skip] ${label} (${addr}) not locked`); continue; }
      if (owner.toLowerCase() !== wallet.address.toLowerCase()) {
        console.log(`[skip] ${label} (${addr}) owner=${owner} != signer — cannot manualSetWinner`);
        continue;
      }
      if (idx >= Number(nOut)) { console.log(`[skip] ${label} idx ${idx} >= outcomesCount ${nOut}`); continue; }

      const code = await pool.outcomeCode(idx);
      console.log(`[set]  ${label} (${addr}) -> outcome[${idx}]="${code}"`);
      if (DRY_RUN) continue;

      const tx = await pool.manualSetWinner(idx);
      console.log(`       tx ${tx.hash}`);
      const r = await tx.wait(1);
      console.log(`       ${r.status === 1 ? "OK" : "FAILED"} in block ${r.blockNumber}`);
    } catch (e) {
      console.error(`[err]  ${label} (${addr}): ${e?.reason || e?.shortMessage || e?.message || e}`);
    }
  }
}

main().catch((e) => { console.error(e?.message || e); process.exit(1); });
