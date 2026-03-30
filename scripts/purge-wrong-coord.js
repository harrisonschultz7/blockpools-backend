// scripts/purge-wrong-coord.js
// One-shot: purgePool() on the 12 pools that have the old coordinator hardcoded.
// These are confirmed isKnownPool=true on the new coordinator but will never settle.
//
// Usage:
//   set -a && source /etc/blockpools/settler.env && set +a
//   node scripts/purge-wrong-coord.js
//
//   DRY_RUN=1 node scripts/purge-wrong-coord.js

import { ethers } from "ethers";

const RPC_URL = process.env.RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const SETTLEMENT_COORDINATOR_ADDRESS = (process.env.SETTLEMENT_COORDINATOR_ADDRESS || "").trim();
const DRY_RUN = /^(1|true)$/i.test(String(process.env.DRY_RUN || ""));
const TX_PACE_MS = Number(process.env.TX_PACE_MS || 2000);

if (!RPC_URL) throw new Error("Missing RPC_URL");
if (!PRIVATE_KEY) throw new Error("Missing PRIVATE_KEY");
if (!ethers.isAddress(SETTLEMENT_COORDINATOR_ADDRESS)) throw new Error("Missing/invalid SETTLEMENT_COORDINATOR_ADDRESS");

// The 12 pools confirmed wrong-coordinator from settlement-bot log 2026-03-29
const WRONG_COORD_POOLS = [
  { addr: "0x46Ea2A387bf7C91bA548d2DA213A6B17F8d5EC9D", label: "MLB Washington Nationals vs Chicago Cubs" },
  { addr: "0x41FfD5cE969e071f036c3Ed8758E26f6375F2b79", label: "MLB Minnesota Twins vs Baltimore Orioles" },
  { addr: "0x65b00d0467f95a8FF0510c992748e34981201aD1", label: "MLB Chicago White Sox vs Milwaukee Brewers" },
  { addr: "0xde621236FeD6DEcC77e167A3a2DA9EbA96c15263", label: "MLB New York Yankees vs San Francisco Giants" },
  { addr: "0x46Ceae457E8136952dF153D80C7459d55d7cC096", label: "MLB Cleveland Guardians vs Seattle Mariners" },
  { addr: "0x881e15106D1704853f51acdDBB6570c3D36006aC", label: "NBA Sacramento Kings vs Atlanta Hawks" },
  { addr: "0xE31cd1b13865eC14e399784A0E3811c5653A6931", label: "NHL Pittsburgh Penguins vs Ottawa Senators" },
  { addr: "0xaBd330D173DD1B01984e6A9c4E8E1c8a0f3B7a86", label: "NHL Ottawa Senators vs Tampa Bay Lightning" },
  { addr: "0xF0509905458226f6aEc55eeA55aec2c95C343413", label: "NHL Dallas Stars vs Pittsburgh Penguins" },
  { addr: "0xA1535233b3ba37781DB222cc848A440E41558D89", label: "NHL New Jersey Devils vs Carolina Hurricanes" },
  { addr: "0xaAD7fDa3a756924db4171750FD1bF66976c9700C", label: "NHL Seattle Kraken vs Buffalo Sabres" },
  { addr: "0x286A814EAAD7b18dC6Bd16A6A225716c6909218B", label: "NHL Philadelphia Flyers vs Detroit Red Wings" },
];

const COORDINATOR_ABI = [
  "function purgePool(address pool) external",
  "function isKnownPool(address pool) view returns (bool)",
  "function pending(address pool) view returns (bool)",
];

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  console.log(`[purge-wrong-coord] DRY_RUN=${DRY_RUN}`);
  console.log(`[purge-wrong-coord] Coordinator: ${SETTLEMENT_COORDINATOR_ADDRESS}`);
  console.log(`[purge-wrong-coord] Targets: ${WRONG_COORD_POOLS.length} pools\n`);

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);
  const coordinator = new ethers.Contract(SETTLEMENT_COORDINATOR_ADDRESS, COORDINATOR_ABI, wallet);

  let purged = 0, skipped = 0, errors = 0;

  for (const { addr, label } of WRONG_COORD_POOLS) {
    console.log(`[pool] ${addr}  ${label}`);

    if (DRY_RUN) {
      console.log(`  → DRY_RUN: would call purgePool`);
      purged++;
      continue;
    }

    // Verify still known and not mid-flight before sending
    let isKnown, isPending;
    try {
      [isKnown, isPending] = await Promise.all([
        coordinator.isKnownPool(addr),
        coordinator.pending(addr),
      ]);
    } catch (e) {
      console.warn(`  [warn] pre-check read failed: ${e?.message} — skipping`);
      skipped++;
      continue;
    }

    if (!isKnown) {
      console.log(`  [skip] not known in coordinator (already purged?)`);
      skipped++;
      continue;
    }
    if (isPending) {
      console.log(`  [skip] currently pending — wait for request to resolve then retry`);
      skipped++;
      continue;
    }

    try {
      await sleep(TX_PACE_MS);
      const tx = await coordinator.purgePool(addr);
      console.log(`  [tx] ${tx.hash}`);
      const receipt = await tx.wait(1);
      if (receipt.status !== 1) throw new Error("tx reverted");
      console.log(`  [ok] purged ✓`);
      purged++;
    } catch (e) {
      console.error(`  [err] ${e?.reason || e?.message || e}`);
      errors++;
    }
  }

  console.log(`\n[done] purged=${purged} skipped=${skipped} errors=${errors}`);
  if (!DRY_RUN && purged > 0) {
    console.log(`\nNext: remove these addresses from games.json, commit, and redeploy.`);
    console.log(`  cd /opt/blockpools/backend`);
    console.log(`  # Edit src/data/games.json to remove the 12 addresses above`);
    console.log(`  git add src/data/games.json && git commit -m "chore: remove ${purged} wrong-coord pools"`);
    console.log(`  git push && git pull && npm run build && sudo systemctl restart blockpools-backend`);
  }
}

main().catch(e => { console.error(e?.stack || e); process.exit(1); });