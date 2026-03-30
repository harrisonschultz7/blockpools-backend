// purge-settled-pools.mjs
// Purges 19 pools that are isKnownPool=true in the coordinator but winningTeam != 0.
// These were manually settled — the coordinator never got the fulfillRequest callback
// that would have cleaned them up, so they're dead weight in the coordinator state.
//
// Usage:
//   set -a && source /etc/blockpools/settler.env && set +a
//   DRY_RUN=1 node purge-settled-pools.mjs   # preview
//   node purge-settled-pools.mjs              # execute

import { ethers } from "/opt/blockpools/backend/node_modules/ethers/dist/ethers.js";

const DRY_RUN = /^(1|true)$/i.test(String(process.env.DRY_RUN || ""));
const TX_PACE_MS = Number(process.env.TX_PACE_MS || 2000);
const sleep = ms => new Promise(r => setTimeout(r, ms));

const RPC_URL = process.env.RPC_URL;
const PRIVATE_KEY = process.env.PRIVATE_KEY;
const COORD = process.env.SETTLEMENT_COORDINATOR_ADDRESS;

if (!RPC_URL) throw new Error("Missing RPC_URL");
if (!PRIVATE_KEY) throw new Error("Missing PRIVATE_KEY");
if (!ethers.isAddress(COORD)) throw new Error("Missing/invalid SETTLEMENT_COORDINATOR_ADDRESS");

// 19 pools confirmed: isKnownPool=true, winningTeam != 0 (audit 2026-03-30)
const TARGETS = [
  { addr: "0xDc1A2CB301D863e482B057Cca05C36f028B7Fd31", label: "MLB Kansas City Royals vs Atlanta Braves" },
  { addr: "0x83606C5f95Ee8297501836BD81dd31BA52bFcBD7", label: "MLB Minnesota Twins vs Baltimore Orioles" },
  { addr: "0xC71C28E940a1A56573B6b9eEFf273833d42375a3", label: "MLB Texas Rangers vs Philadelphia Phillies" },
  { addr: "0xC069a99163225B73da9a8755Ba69B4F00D59550c", label: "MLB Athletics vs Toronto Blue Jays" },
  { addr: "0xDa3D413C1B6b0b3B47a71BdEe1b3EcfcdB905f7F", label: "MLB Boston Red Sox vs Cincinnati Reds" },
  { addr: "0x3676B9ED8f66aa112e08289b0e77b73D7E888Aa2", label: "MLB Colorado Rockies vs Miami Marlins" },
  { addr: "0x6c69c7528DbCdA10d122eb1E774ad0aDBeeac978", label: "MLB Pittsburgh Pirates vs New York Mets" },
  { addr: "0x7ba3D6306c05B73b03445dc91D6F5345bf174B0D", label: "MLB Los Angeles Angels vs Houston Astros" },
  { addr: "0x411dAd3932B5B06c491106a97a06BcC9C79636d6", label: "MLB Chicago White Sox vs Milwaukee Brewers" },
  { addr: "0xE5Bb07396Da29f2E58A4ADB3D8AaEBbadAAe4249", label: "MLB Tampa Bay Rays vs St. Louis Cardinals" },
  { addr: "0xaf4ab402BD11774D534FfE192AE205D0a142308e", label: "MLB Washington Nationals vs Chicago Cubs" },
  { addr: "0xBC4E4CF9C2a797C8AD1Cc9AbbdBa0cdbb0599F82", label: "NHL Chicago Blackhawks vs New Jersey Devils" },
  { addr: "0x6118F4754203186976B4FEf137760945Ae3BfACa", label: "NBA Los Angeles Clippers vs Milwaukee Bucks" },
  { addr: "0xF0cCBa8BB3FAA60934fDd8DE80A8cBd1bAf8BE84", label: "NBA Miami Heat vs Indiana Pacers" },
  { addr: "0xc6F250F9e28095ebC6ea8FE9E71a2a84fFa74146", label: "NBA Sacramento Kings vs Brooklyn Nets" },
  { addr: "0x22C44d6867602bd016761C289a367b67A52831d3", label: "NBA Boston Celtics vs Charlotte Hornets" },
  { addr: "0xAa72CF6EAE3d8eb508e9f2b2a9F334ddCff179Af", label: "NBA New York Knicks vs Oklahoma City Thunder" },
  { addr: "0x77bd12fae35d51ef15fC810A3b02b8a1164C8A6e", label: "NBA Golden State Warriors vs Denver Nuggets" },
  { addr: "0xe6d804115bceAC78Dc41d07c616Ef61d917bF0c8", label: "NHL Montreal Canadiens vs Carolina Hurricanes" },
];

const COORD_ABI = [
  "function purgePool(address pool) external",
  "function isKnownPool(address pool) view returns (bool)",
  "function pending(address pool) view returns (bool)",
];

async function main() {
  console.log(`[purge-settled-pools] DRY_RUN=${DRY_RUN}`);
  console.log(`[purge-settled-pools] Coordinator: ${COORD}`);
  console.log(`[purge-settled-pools] Targets: ${TARGETS.length}\n`);

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);
  const coord = new ethers.Contract(COORD, COORD_ABI, wallet);

  let purged = 0, skipped = 0, errors = 0;

  for (const { addr, label } of TARGETS) {
    console.log(`[pool] ${addr}  ${label}`);

    if (DRY_RUN) {
      console.log(`  → DRY_RUN: would call purgePool`);
      purged++;
      continue;
    }

    let isKnown, isPending;
    try {
      [isKnown, isPending] = await Promise.all([
        coord.isKnownPool(addr),
        coord.pending(addr),
      ]);
    } catch (e) {
      console.warn(`  [warn] pre-check failed: ${e.message} — skipping`);
      skipped++;
      continue;
    }

    if (!isKnown) {
      console.log(`  [skip] already not known in coordinator`);
      skipped++;
      continue;
    }
    if (isPending) {
      console.log(`  [skip] currently pending — wait for it to resolve then retry`);
      skipped++;
      continue;
    }

    try {
      await sleep(TX_PACE_MS);
      const tx = await coord.purgePool(addr);
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
    console.log(`\nNext steps:`);
    console.log(`  1. Remove these ${purged} addresses from games.json`);
    console.log(`  2. Also handle the retry-exhausted pool separately:`);
    console.log(`     0xe55710A5C597d052675CF569E07344f0E14EA86f  MLB Cleveland Guardians vs Seattle Mariners`);
    console.log(`     (retries=2, ready=true — this game may still be unresolved, check manually)`);
  }
}

main().catch(e => { console.error(e?.stack || e); process.exit(1); });
