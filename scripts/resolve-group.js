// Manual resolver for GROUP / LEAGUE-WINNER v2 markets (>3 outcomes).
//
// settle-v2.js deliberately skips league-winner groups (Goalserve can't resolve
// "who wins the league / where does a player sign"). This is the operator's
// manual path: pick the winning outcome and it reports payouts to EVERY
// sub-market — winner → [1,0] (Yes), all others → [0,1] (No).
//
// Each outcome in a group is its own binary Yes/No market in the vault, so a
// full resolution = one reportPayouts per outcome. Winners can then redeem $1/sh.
//
//   node scripts/resolve-group.js --market <gameId|slug> --winner <CODE|index> --dry
//   node scripts/resolve-group.js --market EPL-SPECIAL-VINICIUS-2027-1819836000 --winner RMA --dry
//   node scripts/resolve-group.js --market EPL-SPECIAL-VINICIUS-2027-1819836000 --winner RMA
//   node scripts/resolve-group.js --market <gameId> --void        # even refund, every outcome [1,1]
//
// Env (backend .env): PRIVATE_KEY (vault resolver), VAULT_ADDRESS,
//   GAMES_JSON_PATH, ARBITRUM_RPC_URL, MATCHER_URL, BACKEND_URL.

const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { ethers } = require("ethers");

const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const MATCHER_URL = (process.env.MATCHER_URL || "http://127.0.0.1:8090").replace(/\/+$/, "");
const BACKEND_URL = (process.env.BACKEND_URL || "").replace(/\/+$/, "");

const VAULT_ABI = [
  "function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)",
  "function reportPayouts(bytes32 marketId, uint256[] nums)",
];

function argValue(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}
const DRY = process.argv.includes("--dry");
const VOID = process.argv.includes("--void");
const marketFilter = argValue("--market");
const winnerArg = argValue("--winner");

async function postJson(url, body) {
  try {
    const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    return res.ok;
  } catch { return false; }
}

function loadGame(id) {
  const raw = JSON.parse(fs.readFileSync(GAMES_JSON, "utf8"));
  for (const list of Object.values(raw)) {
    if (Array.isArray(list)) for (const g of list) if (g && g.v2 && (g.gameId === id || g.slug === id)) return g;
  }
  return null;
}

async function main() {
  if (!marketFilter) throw new Error("--market <gameId|slug> is required");
  if (!VOID && winnerArg == null) throw new Error("provide --winner <CODE|index> or --void");

  const RPC = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc";
  const VAULT = process.env.VAULT_ADDRESS || process.env.V2_VAULT_ADDRESS;
  const KEY = process.env.PRIVATE_KEY || process.env.OPERATOR_PRIVATE_KEY;
  if (!VAULT || !KEY) throw new Error("Set VAULT_ADDRESS and PRIVATE_KEY in the backend .env");

  const provider = new ethers.JsonRpcProvider(RPC);
  const wallet = new ethers.Wallet(KEY, provider);
  const vault = new ethers.Contract(VAULT, VAULT_ABI, wallet);

  const g = loadGame(marketFilter);
  if (!g) throw new Error(`no v2 game matching "${marketFilter}" in ${GAMES_JSON}`);
  if (!Array.isArray(g.outcomes) || g.outcomes.length < 2) throw new Error(`"${g.gameId}" is not a group market (needs an outcomes[] array)`);

  // Resolve the winning outcome index from either a code (RMA) or a numeric index.
  let winnerIdx = null;
  if (!VOID) {
    const byCode = g.outcomes.find((o) => String(o.code).toUpperCase() === String(winnerArg).toUpperCase());
    if (byCode) winnerIdx = Number(byCode.outcomeIndex);
    else if (/^\d+$/.test(String(winnerArg))) winnerIdx = Number(winnerArg);
    if (winnerIdx == null || !g.outcomes.some((o) => Number(o.outcomeIndex) === winnerIdx)) {
      throw new Error(`--winner "${winnerArg}" matched no outcome. Valid: ${g.outcomes.map((o) => `${o.code}=${o.outcomeIndex}`).join(", ")}`);
    }
  }

  const winLabel = VOID ? "VOID (even refund)" : `${g.outcomes.find((o) => Number(o.outcomeIndex) === winnerIdx).label} [idx ${winnerIdx}]`;
  console.log(`resolve-group: ${g.gameId} — "${g.marketQuestion}"`);
  console.log(`  vault=${VAULT} resolver=${wallet.address}${DRY ? "  (DRY RUN)" : ""}`);
  console.log(`  winner → ${winLabel}\n`);

  const calls = g.outcomes
    .filter((o) => o.marketId)
    .map((o) => ({
      marketId: o.marketId,
      code: o.code,
      vector: VOID ? [1, 1] : Number(o.outcomeIndex) === winnerIdx ? [1, 0] : [0, 1],
    }));

  for (const call of calls) {
    try {
      const m = await vault.markets(call.marketId);
      if (!m.exists) { console.log(`  ${call.code}: market not found on-chain — skip`); continue; }
      if (m.resolved) { console.log(`  ${call.code}: already resolved — skip`); continue; }

      const nums = call.vector.map((n) => BigInt(n));
      await vault.reportPayouts.staticCall(call.marketId, nums); // simulate first
      if (DRY) { console.log(`  ${call.code}: WOULD reportPayouts([${call.vector}]) ✓`); continue; }

      const tx = await vault.reportPayouts(call.marketId, nums);
      await tx.wait();
      console.log(`  ${call.code}: resolved [${call.vector}] tx=${tx.hash}`);

      await postJson(`${MATCHER_URL}/clear-market`, { marketId: call.marketId });
      if (BACKEND_URL) {
        await postJson(`${BACKEND_URL}/api/v2/resolve`, {
          marketId: call.marketId,
          winningOutcome: call.vector[0] === call.vector[1] ? null : call.vector.indexOf(1),
          gameId: g.gameId,
        });
      }
    } catch (e) {
      console.log(`  ${call.code}: ERROR ${e.shortMessage || e.message}`);
    }
  }
  console.log(`\n${DRY ? "Dry run complete — re-run without --dry to settle on-chain." : "Done."}`);
}

main().catch((e) => { console.error(e); process.exit(1); });
