// Manual v2 market resolution — the order-book analog of the AMM
// manual-set-winner script. Use for markets Goalserve can't settle (props,
// league-winners) or to override/force a result.
//
//   node scripts/resolve-v2-manual.js --market <gameId|slug|0xMarketId> --winner <W> [--dry]
//
// --winner accepts (case-insensitive):
//   binary  : a team CODE (e.g. MIL), or A / B / 0 / 1, or VOID (even refund)
//   prop    : YES / NO (or A / B / 0 / 1), or VOID
//   group   : the winning outcome CODE (e.g. ARS) — that sub-market pays Yes,
//             every other sub-market pays No; or VOID to refund the whole group
//   a single sub-market 0xMarketId + YES/NO resolves just that sub-market
//
// Reuses the same on-chain path as the auto settler: reportPayouts (simulated
// first) → clear the matcher book → POST /api/v2/resolve for realized-PnL rows.
// Env (backend .env): PRIVATE_KEY, VAULT_ADDRESS, GAMES_JSON_PATH, BACKEND_URL, MATCHER_URL, ARBITRUM_RPC_URL.

const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { ethers } = require("ethers");
const { binaryVector, groupSubVector } = require("./settle-v2");

const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const MATCHER_URL = (process.env.MATCHER_URL || "http://127.0.0.1:8090").replace(/\/+$/, "");
const BACKEND_URL = (process.env.BACKEND_URL || "").replace(/\/+$/, "");

const VAULT_ABI = [
  "function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)",
  "function reportPayouts(bytes32 marketId, uint256[] nums)",
];

const DRY = process.argv.includes("--dry");
const arg = (flag) => {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
};

function loadGames() {
  const raw = JSON.parse(fs.readFileSync(GAMES_JSON, "utf8"));
  const out = [];
  for (const list of Object.values(raw)) if (Array.isArray(list)) for (const g of list) if (g && g.v2) out.push(g);
  return out;
}

function kindOf(g) {
  if (Array.isArray(g.outcomes) && g.outcomes.length >= 2) return "group";
  if (String(g.marketType || "").toUpperCase() === "PROP") return "prop";
  return "binary";
}

/** Find the game + (optionally) the specific sub-market matching the selector. */
function findTarget(games, sel) {
  const s = String(sel || "").toLowerCase();
  for (const g of games) {
    if (String(g.gameId || "").toLowerCase() === s || String(g.slug || "").toLowerCase() === s) {
      return { g, subMarketId: null };
    }
    if (String(g.marketId || "").toLowerCase() === s) return { g, subMarketId: null };
    if (Array.isArray(g.outcomes)) {
      const o = g.outcomes.find((x) => String(x.marketId || "").toLowerCase() === s);
      if (o) return { g, subMarketId: o.marketId };
    }
  }
  return null;
}

/** Build the reportPayouts calls for a manual winner selection. */
function planManual(g, winnerRaw, subMarketId) {
  const W = String(winnerRaw || "").trim().toUpperCase();
  const kind = kindOf(g);
  const isVoid = W === "VOID" || W === "TIE";

  // Resolving one explicit sub-market of a group (Yes/No on that leg).
  if (subMarketId) {
    if (isVoid) return { calls: [{ marketId: subMarketId, vector: [1, 1], label: "VOID" }] };
    const yes = W === "YES" || W === "A" || W === "0";
    return { calls: [{ marketId: subMarketId, vector: yes ? [1, 0] : [0, 1], label: yes ? "YES" : "NO" }] };
  }

  if (kind === "binary" || kind === "prop") {
    if (!g.marketId) return { error: "no marketId on game" };
    if (isVoid) return { calls: [{ marketId: g.marketId, vector: [1, 1], label: "VOID" }] };
    const aCodes = new Set(["A", "0", "YES", String(g.teamACode || "").toUpperCase(), String(g.teamA || "").toUpperCase()].filter(Boolean));
    const bCodes = new Set(["B", "1", "NO", String(g.teamBCode || "").toUpperCase(), String(g.teamB || "").toUpperCase()].filter(Boolean));
    if (aCodes.has(W)) return { calls: [{ marketId: g.marketId, vector: binaryVector(0), label: g.teamACode || "A" }] };
    if (bCodes.has(W)) return { calls: [{ marketId: g.marketId, vector: binaryVector(1), label: g.teamBCode || "B" }] };
    const aLbl = g.teamACode || g.teamA || (kind === "prop" ? "YES" : "?");
    const bLbl = g.teamBCode || g.teamB || (kind === "prop" ? "NO" : "?");
    return { error: `winner "${winnerRaw}" not one of A/${aLbl} or B/${bLbl} (or VOID)` };
  }

  // group: winner = a CODE → that sub-market Yes, others No (or VOID → all refund)
  const subs = g.outcomes.filter((o) => o.marketId);
  if (isVoid) return { calls: subs.map((o) => ({ marketId: o.marketId, vector: [1, 1], label: `${o.code} VOID` })) };
  const winIdx = subs.find((o) => String(o.code || "").toUpperCase() === W)?.outcomeIndex;
  if (winIdx == null) {
    return { error: `winner "${winnerRaw}" not one of: ${subs.map((o) => o.code).join(", ")}, VOID` };
  }
  return {
    calls: subs.map((o) => ({ marketId: o.marketId, vector: groupSubVector(o.outcomeIndex, winIdx), label: o.code })),
  };
}

async function postJson(url, body) {
  try {
    const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    return res.ok;
  } catch {
    return false;
  }
}

async function main() {
  const sel = arg("--market");
  const winner = arg("--winner");
  if (!sel || !winner) {
    console.error("usage: node scripts/resolve-v2-manual.js --market <gameId|slug|0xMarketId> --winner <CODE|A|B|YES|NO|VOID> [--dry]");
    process.exit(2);
  }

  const RPC = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc";
  const VAULT = process.env.VAULT_ADDRESS || process.env.V2_VAULT_ADDRESS;
  const KEY = process.env.PRIVATE_KEY || process.env.OPERATOR_PRIVATE_KEY;
  if (!VAULT || !KEY) throw new Error("Set VAULT_ADDRESS (or V2_VAULT_ADDRESS) and PRIVATE_KEY in the backend .env");

  const provider = new ethers.JsonRpcProvider(RPC);
  const wallet = new ethers.Wallet(KEY, provider);
  const vault = new ethers.Contract(VAULT, VAULT_ABI, wallet);

  const games = loadGames();
  const target = findTarget(games, sel);
  if (!target) throw new Error(`no v2 market found for "${sel}"`);

  const plan = planManual(target.g, winner, target.subMarketId);
  if (plan.error) throw new Error(plan.error);

  console.log(`resolve-v2-manual: ${target.g.gameId} winner="${winner}"${DRY ? " (DRY RUN)" : ""}`);
  for (const call of plan.calls) {
    const m = await vault.markets(call.marketId);
    if (!m.exists) {
      console.log(`  ${call.label}: market ${call.marketId} not on-chain — skip`);
      continue;
    }
    if (m.resolved) {
      console.log(`  ${call.label}: already resolved — skip`);
      continue;
    }
    const nums = call.vector.map((n) => BigInt(n));
    await vault.reportPayouts.staticCall(call.marketId, nums); // simulate — never mis-settle
    if (DRY) {
      console.log(`  ${call.label}: WOULD reportPayouts([${call.vector}]) ✓ (staticCall ok) ${call.marketId}`);
      continue;
    }
    const tx = await vault.reportPayouts(call.marketId, nums);
    await tx.wait();
    console.log(`  ${call.label}: resolved [${call.vector}] tx=${tx.hash}`);
    await postJson(`${MATCHER_URL}/clear-market`, { marketId: call.marketId });
    if (BACKEND_URL) {
      await postJson(`${BACKEND_URL}/api/v2/resolve`, {
        marketId: call.marketId,
        winningOutcome: call.vector[0] === call.vector[1] ? null : call.vector.indexOf(1),
        gameId: target.g.gameId,
      });
    }
  }
  console.log(DRY ? "dry run complete (nothing sent)" : "done");
}

main().catch((e) => {
  console.error("ERROR:", e.shortMessage || e.message);
  process.exit(1);
});
