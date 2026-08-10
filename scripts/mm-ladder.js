// Capital-light market-maker ladder for v2 order-book markets.
//
// For every market in mm-ladder.config.json it posts a bounded BID ladder on
// BOTH outcomes of each binary market, around the MID price you set per game.
// Because they're resting BUY orders, NOTHING leaves the MM wallet until a
// user trade fills one — so you can seed 10-30 markets without locking LP, and
// re-quote or pull instantly. Buy-side liquidity comes from mint-matching (a
// user buying Yes mints against the MM's No bid); sell-side from complementary
// fills (a user selling Yes hits the MM's Yes bid).
//
// Lives in the backend repo (blockpools-backend/scripts) so it deploys with the
// backend and runs on the VPS beside the matcher. Run from the backend root:
//
//   Setup (one-time): fund the MM wallet with USDC, then approve the Exchange:
//     node scripts/mm-ladder.js --approve
//
//   Seed only NEW markets — never re-seeds a market it has seeded before, and
//   skips resolved / already-started games:
//     node scripts/mm-ladder.js
//
//   Force a full re-quote (cancel this MM's ladder + re-post, still skipping
//   resolved/started games) — use when you want to MOVE your prices:
//     node scripts/mm-ladder.js --reseed
//
//   Keep seeding newly-added markets every 30s:
//     node scripts/mm-ladder.js --loop 30
//
//   Pull all MM orders (cancel, post nothing):
//     node scripts/mm-ladder.js --cancel
//
// Env (backend .env): ARBITRUM_RPC_URL, EXCHANGE_ADDRESS, GAMES_JSON_PATH,
// USDC_ADDRESS (defaults to native USDC), MM_PRIVATE_KEY (falls back to
// PRIVATE_KEY). MATCHER_URL defaults to http://127.0.0.1:8090 (the local matcher).

const fs = require("fs");
const path = require("path");
// Load the backend .env (ARBITRUM_RPC_URL, EXCHANGE_ADDRESS, GAMES_JSON_PATH,
// MATCHER_URL, MM_PRIVATE_KEY/PRIVATE_KEY, USDC_ADDRESS…) — the same file the
// other backend scripts use, regardless of the working directory.
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { ethers } = require("ethers");

// EIP-712 order typing — inlined (must mirror Exchange.sol's ORDER_TYPEHASH +
// domain: name "BlockPoolsExchange", version "1").
const ORDER_TYPES = {
  Order: [
    { name: "maker", type: "address" },
    { name: "marketId", type: "bytes32" },
    { name: "outcome", type: "uint8" },
    { name: "side", type: "uint8" },
    { name: "shares", type: "uint256" },
    { name: "price", type: "uint256" },
    { name: "feeRateBps", type: "uint256" },
    { name: "expiry", type: "uint256" },
    { name: "salt", type: "uint256" },
  ],
};
const buildDomain = (chainId, verifyingContract) => ({ name: "BlockPoolsExchange", version: "1", chainId, verifyingContract });

const PRICE_SCALE = 1_000_000n; // 1e6 == $1.00 / share
const SHARE_SCALE = 1_000_000n; // 6-decimal shares
const BUY = 0;

const MATCHER_URL = (process.env.MATCHER_URL || "http://127.0.0.1:8090").replace(/\/+$/, "");
const USDC_ADDRESS = process.env.USDC_ADDRESS || "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
// Use the same games.json the backend resolver uses (GAMES_JSON_PATH); the
// relative fallback only applies if that env isn't set.
const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const CONFIG_PATH = path.join(__dirname, "mm-ladder.config.json");
const STATE_PATH = path.join(__dirname, ".mm-ladder-state.json");

const ERC20_ABI = [
  "function approve(address,uint256) returns (bool)",
  "function allowance(address,address) view returns (uint256)",
  "function balanceOf(address) view returns (uint256)",
];

const isProp = (g) => String(g?.marketType || "").toUpperCase() === "PROP";

/** Resolve config markets → a flat list of { marketId, mid0Cents } binary markets. */
function resolveTargets(config, games) {
  const all = [];
  for (const list of Object.values(games)) {
    if (Array.isArray(list)) for (const g of list) if (g?.v2) all.push(g);
  }
  const find = (m) =>
    all.find((g) => (m.gameId && g.gameId === m.gameId) || (m.slug && g.slug === m.slug) || (m.marketId && g.marketId === m.marketId));

  const targets = [];
  for (const m of config.markets || []) {
    const g = find(m);
    if (!g) { console.warn(`  ⚠️  no v2 market found for ${m.gameId || m.slug || m.marketId} — skipping`); continue; }
    const mids = m.mids || {};
    const params = { ...config.defaults, ...(m.params || {}) };

    const gameBudget = Number(params.budgetUsd) || 0;

    if (Array.isArray(g.outcomes) && g.outcomes.length >= 2 && !isProp(g)) {
      // group: each outcome is its own binary sub-market. The game's budget is
      // split evenly across the outcomes we're seeding.
      const seeded = g.outcomes.filter((oc) => mids[oc.code] != null);
      const perBudget = gameBudget / (seeded.length || 1);
      for (const oc of seeded) {
        targets.push({ label: `${g.gameId || g.slug} · ${oc.code}`, marketId: oc.marketId, mid0: mids[oc.code], params, budgetUsd: perBudget, group: true });
      }
    } else if (g.marketId) {
      // binary matchup or prop: single market, outcome 0 = teamA/YES.
      const codeA = String(g.teamACode || g.teamA || "YES");
      const codeB = String(g.teamBCode || g.teamB || "NO");
      let mid0 = mids[codeA];
      if (mid0 == null && mids[codeB] != null) mid0 = 100 - mids[codeB];
      if (mid0 == null && isProp(g)) mid0 = mids.YES;
      if (mid0 == null) { console.warn(`  ⚠️  no mid for ${g.gameId || g.slug} — skipping`); continue; }
      targets.push({ label: `${g.gameId || g.slug} · ${codeA}/${codeB}`, marketId: g.marketId, mid0, params, budgetUsd: gameBudget, group: false });
    }
  }
  return targets;
}

/** Build the BID ladder orders for one binary market (both outcomes).
 *  Shares are sized so the ladder's total notional == budgetUsd (max exposure). */
function buildOrders(maker, { marketId, mid0, params, budgetUsd }) {
  const { rungs, topSpreadCents, stepCents } = params;
  // 1) Collect the bid price levels for both outcomes.
  const levels = [];
  const sideMids = [mid0, 100 - mid0]; // outcome 0 mid, outcome 1 mid
  for (let outcome = 0; outcome < 2; outcome++) {
    const mid = sideMids[outcome];
    for (let k = 0; k < rungs; k++) {
      const cents = mid - topSpreadCents - k * stepCents;
      if (cents < 1) break; // never bid below 1¢
      levels.push({ outcome, cents });
    }
  }
  if (!levels.length) return [];
  // 2) Size shares so Σ(price × shares) == budgetUsd.
  const priceSumDollars = levels.reduce((s, l) => s + l.cents / 100, 0) || 1;
  const shares = Math.max(1, Math.floor((Number(budgetUsd) || 0) / priceSumDollars));
  // 3) Build the orders.
  return levels.map((l) => ({
    maker,
    marketId,
    outcome: l.outcome,
    side: BUY,
    shares: (BigInt(shares) * SHARE_SCALE).toString(),
    price: (BigInt(l.cents) * (PRICE_SCALE / 100n)).toString(), // cents → 1e6 scale
    feeRateBps: "1000",
    expiry: "0",
    salt: BigInt(`0x${ethers.hexlify(ethers.randomBytes(8)).slice(2)}`).toString(),
  }));
}

async function post(url, body) {
  const res = await fetch(url, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `matcher ${res.status}`);
  return json;
}

async function getJson(url) {
  const res = await fetch(url);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `matcher ${res.status}`);
  return json;
}

/** True if the MM wallet already has ≥1 resting order in this market's book. */
async function marketSeededByMM(marketId, maker) {
  const book = await getJson(`${MATCHER_URL}/book?marketId=${marketId}`);
  const mk = String(maker).toLowerCase();
  const inSide = (side) => [0, 1].some((o) => (book?.[side]?.[o] || []).some((ord) => String(ord.maker).toLowerCase() === mk));
  return inSide("bids") || inSide("asks");
}

// Persistent state: `hashes` = posted order hashes (for --cancel); `seeded` =
// a permanent record of every marketId this script has EVER seeded, so a market
// is seeded exactly ONCE and never re-seeded on a later run (this script is for
// NEW games/markets only). Keyed by lowercased marketId -> label.
function loadState() {
  try { const s = JSON.parse(fs.readFileSync(STATE_PATH, "utf8")); return { hashes: s.hashes || [], seeded: s.seeded || {} }; }
  catch { return { hashes: [], seeded: {} }; }
}
function saveState(s) {
  fs.writeFileSync(STATE_PATH, JSON.stringify({ hashes: s.hashes || [], seeded: s.seeded || {} }, null, 2));
}

/** True if the matcher is up (so we can read the book / post orders). */
async function matcherReachable() {
  try { await getJson(`${MATCHER_URL}/book?marketId=0x${"0".repeat(64)}`); return true; }
  catch { return false; }
}

async function cancelPrevious() {
  const state = loadState();
  if (!state.hashes.length) return 0;
  let n = 0;
  for (const h of state.hashes) { try { await post(`${MATCHER_URL}/cancel`, { hash: h }); n++; } catch {} }
  saveState({ ...state, hashes: [] }); // keep the permanent `seeded` record
  return n;
}

async function main() {
  const args = process.argv.slice(2);
  const RPC = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL;
  const EXCHANGE = process.env.EXCHANGE_ADDRESS;
  const KEY = process.env.MM_PRIVATE_KEY || process.env.PRIVATE_KEY;
  if (!RPC || !EXCHANGE || !KEY) throw new Error("Set ARBITRUM_RPC_URL, EXCHANGE_ADDRESS, and MM_PRIVATE_KEY (or PRIVATE_KEY) in the backend .env");

  const provider = new ethers.JsonRpcProvider(RPC);
  const wallet = new ethers.Wallet(KEY, provider);
  const chainId = Number((await provider.getNetwork()).chainId);
  const domain = buildDomain(chainId, EXCHANGE);
  // Vault: read `resolved` so we never seed a finished game.
  const VAULT_ADDRESS = process.env.VAULT_ADDRESS || "0x14BD1fd3911C22173FeaEcBfD670D09c1143A594";
  const vault = new ethers.Contract(
    VAULT_ADDRESS,
    ["function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)"],
    provider
  );
  console.log(`MM wallet ${wallet.address}  exchange ${EXCHANGE}  matcher ${MATCHER_URL}`);

  // --approve: one-time USDC approval so fills can pull the MM's USDC.
  if (args.includes("--approve")) {
    const usdc = new ethers.Contract(USDC_ADDRESS, ERC20_ABI, wallet);
    const tx = await usdc.approve(EXCHANGE, ethers.MaxUint256);
    console.log(`approve tx ${tx.hash} …`);
    await tx.wait();
    console.log("✅ USDC approved to the Exchange.");
    return;
  }

  // Warn if the MM can't actually fund fills.
  try {
    const usdc = new ethers.Contract(USDC_ADDRESS, ERC20_ABI, provider);
    const [bal, alw] = await Promise.all([usdc.balanceOf(wallet.address), usdc.allowance(wallet.address, EXCHANGE)]);
    console.log(`MM USDC balance ${ethers.formatUnits(bal, 6)}  allowance ${alw >= ethers.parseUnits("1", 30) ? "max" : ethers.formatUnits(alw, 6)}`);
    if (alw === 0n) console.warn("  ⚠️  allowance is 0 — run `--approve` first, or fills will revert.");
  } catch {}

  const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
  const games = JSON.parse(fs.readFileSync(GAMES_JSON, "utf8"));

  const dry = args.includes("--dry");

  const forceReseed = args.includes("--reseed");

  const quoteOnce = async () => {
    // --cancel: pull every tracked MM order, post nothing.
    if (args.includes("--cancel")) {
      const cancelled = await cancelPrevious();
      console.log(`--cancel: pulled ${cancelled} MM order(s), posted nothing.`);
      return;
    }
    // --reseed: cancel this MM's ladder first so every market re-quotes from
    // scratch (moves your prices). Default run leaves existing ladders in place.
    if (forceReseed && !dry) {
      const cancelled = await cancelPrevious();
      if (cancelled) console.log(`--reseed: cancelled ${cancelled} previous MM order(s)`);
    }

    // The matcher must be up to read books + post orders. Abort early instead of
    // spraying failed posts across every market (and falsely treating a market as
    // "unseeded" because its book check errored) when it's unreachable.
    if (!dry && !(await matcherReachable())) {
      console.error(`✗ matcher unreachable at ${MATCHER_URL} — start it (or set MATCHER_URL). Nothing seeded.`);
      return;
    }

    const state = loadState();
    const seeded = state.seeded || {}; // permanent "ever seeded" record (marketId -> label)
    // --groups-only: seed ONLY multi-outcome group/futures markets (champions,
    // division winners, specials) and leave binary games to the Polymarket seed
    // bot. Use it to re-seed the futures after a matcher restart without the
    // ladder colliding with the seed bot on the binary MLB markets.
    const groupsOnly = args.includes("--groups-only");
    const targets = resolveTargets(config, games).filter((t) => !groupsOnly || t.group);
    if (groupsOnly) console.log(`--groups-only: ${targets.length} group sub-market(s) to consider (binary games left to the seed bot).`);

    // Decide which markets to seed. This script is for NEW games/markets only.
    // Skip a market if ANY of these hold (all but the first apply even to --reseed):
    //   • we've EVER seeded it before (persistent record) — the core rule,
    //   • the game is resolved,
    //   • the game has already STARTED (now >= vault lockTime),
    //   • (lost-state recovery) the live book already holds this MM's orders.
    const toSeed = [];
    const nowSec = Math.floor(Date.now() / 1000);
    let skipResolved = 0, skipStarted = 0, skipSeeded = 0;
    for (const tgt of targets) {
      const mid = String(tgt.marketId).toLowerCase();

      // #1 permanent record: never re-seed a market we've already seeded.
      if (!forceReseed && seeded[mid]) { skipSeeded++; console.log(`  ⏭  ${tgt.label.padEnd(28)} already seeded before — skipping`); continue; }

      let m = null;
      try { m = await vault.markets(tgt.marketId); }
      catch { /* transient RPC — treat as open; seeding is harmless (orders just rest) */ }
      if (m?.resolved) { skipResolved++; console.log(`  ⏭  ${tgt.label.padEnd(28)} resolved — skipping`); continue; }
      const lockTime = m ? Number(m.lockTime) : 0;
      if (lockTime > 0 && nowSec >= lockTime) { skipStarted++; console.log(`  ⏭  ${tgt.label.padEnd(28)} already started — skipping`); continue; }

      // Lost/reset state-file recovery: if the live book already has this MM's
      // orders, it WAS seeded — record it so it's remembered, and skip.
      if (!forceReseed && !dry) {
        let onBook = false;
        try { onBook = await marketSeededByMM(tgt.marketId, wallet.address); } catch { /* matcher confirmed up above; treat as not-on-book */ }
        if (onBook) { seeded[mid] = tgt.label; skipSeeded++; console.log(`  ⏭  ${tgt.label.padEnd(28)} already on book — skipping`); continue; }
      }
      toSeed.push(tgt);
    }

    if (dry) {
      console.log(`\n--dry: would seed ${toSeed.length} market(s); skipping ${skipResolved} resolved + ${skipStarted} started + ${skipSeeded} already-seeded.\n`);
      for (const tgt of toSeed) {
        const orders = buildOrders(wallet.address, tgt);
        const fmt = (o) => `o${o.outcome}@${Number(o.price) / 1e4}¢×${Number(o.shares) / 1e6}`;
        console.log(`  ${tgt.label.padEnd(28)} mid ${String(tgt.mid0).padStart(3)}¢ | ${orders.map(fmt).join("  ")}`);
      }
      return;
    }

    // Append new hashes to the saved state so --cancel can still pull everything
    // across runs (deduped; --reseed already emptied it above).
    const hashes = new Set(state.hashes || []);
    let posted = 0;
    for (const tgt of toSeed) {
      const orders = buildOrders(wallet.address, tgt);
      let ok = 0;
      for (const order of orders) {
        const sig = await wallet.signTypedData(domain, ORDER_TYPES, {
          ...order, outcome: order.outcome, side: order.side,
          shares: BigInt(order.shares), price: BigInt(order.price),
          feeRateBps: BigInt(order.feeRateBps), expiry: BigInt(order.expiry), salt: BigInt(order.salt),
        });
        try {
          const r = await post(`${MATCHER_URL}/orders`, { order, sig });
          if (r.hash) hashes.add(r.hash);
          posted++; ok++;
        } catch (e) { console.warn(`  post failed (${tgt.label} o${order.outcome} @${Number(order.price) / 1e4}¢): ${e.message}`); }
      }
      // Record as permanently seeded once at least one order landed, so it's
      // never re-seeded on a future run.
      if (ok > 0) seeded[String(tgt.marketId).toLowerCase()] = tgt.label;
      console.log(`  ✅ ${tgt.label.padEnd(28)} mid ${tgt.mid0}¢ → ${ok}/${orders.length} bids`);
    }
    saveState({ hashes: [...hashes], seeded });
    console.log(`✅ posted ${posted} bids across ${toSeed.length} market(s); skipped ${skipResolved} resolved + ${skipStarted} started + ${skipSeeded} already-seeded.`);
  };

  await quoteOnce();

  const loopIdx = args.indexOf("--loop");
  if (loopIdx >= 0) {
    const secs = Number(args[loopIdx + 1]) || 30;
    console.log(`looping every ${secs}s … (Ctrl+C to stop)`);
    setInterval(() => quoteOnce().catch((e) => console.error(e.message)), secs * 1000);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
