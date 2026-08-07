#!/usr/bin/env node
// scripts/seed-bot.js
//
// LIVE price-following order-book seeding bot for v2 (order-book) markets.
//
// For each configured market it reads a FAIR price from Polymarket (public CLOB
// midpoint), then seeds a capital-light BID ladder on BOTH outcomes around that
// fair — sitting one tick BEHIND any user order so real user liquidity always
// fills first and the house only takes the overflow. As the live line moves it
// re-prices (cancel the stale rungs, post the new ones) with a deadband so a
// stable line causes no churn. Bounded exposure + inventory skew + a dead-man's
// switch keep risk contained on a live game.
//
// Unlike mm-ladder.js this bot is MEANT to run DURING the game (started games
// are seeded, not skipped) — that's why its wallet must NOT be in SWEEP_MAKERS
// (the kickoff+30m sweeper would fight it). Its safety is its own live
// re-pricing + dead-man's switch, not the sweeper.
//
// Because they're resting BUY orders, NOTHING leaves the wallet until a user
// trade fills one, so exposure is bounded by maxExposureUsd per market.
//
// ── Run (from the backend root, on the VPS beside the matcher) ────────────────
//   Setup (one-time): fund the bot wallet with USDC, then approve the Exchange:
//     node scripts/seed-bot.js --approve
//   Preview what it WOULD post/cancel (signs & sends nothing):
//     node scripts/seed-bot.js --dry
//   Run the live loop (systemd uses this; --loop is the tick floor in seconds):
//     node scripts/seed-bot.js --loop 1
//   Pull every order this bot has resting and idle:
//     node scripts/seed-bot.js --cancel
//
// ── On/off ────────────────────────────────────────────────────────────────────
//   The daemon keeps running but only SEEDS while the backend bot_control flag
//   is enabled (GET /api/v2/seed-bot/control?bot=<BOT_NAME>). Flip it from the
//   Supabase dashboard, the admin endpoint, or an admin button. When it flips
//   off (or can't be read — fail-safe) the bot cancels its orders and idles.
//   `systemctl stop blockpools-seed-bot` is the hard kill.
//
// ── Env (backend .env) ────────────────────────────────────────────────────────
//   ARBITRUM_RPC_URL / RPC_URL, EXCHANGE_ADDRESS, VAULT_ADDRESS,
//   SEED_BOT_PRIVATE_KEY (falls back to MM_PRIVATE_KEY, then PRIVATE_KEY),
//   MATCHER_URL (default http://127.0.0.1:8090),
//   BACKEND_URL (default http://127.0.0.1:8080), V2_MATCHER_SECRET (for control read),
//   GAMES_JSON_PATH, USDC_ADDRESS (defaults to native USDC),
//   SEED_BOT_NAME (default "seed-bot"),
//   HOUSE_MAKERS (extra comma-sep wallets to treat as "house" when reading the book).

const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "../.env") });
const { ethers } = require("ethers");

// ── EIP-712 order typing — must mirror Exchange.sol (name "BlockPoolsExchange",
//    version "1"). Identical to mm-ladder.js. ─────────────────────────────────
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
const BACKEND_URL = (process.env.BACKEND_URL || "http://127.0.0.1:8080").replace(/\/+$/, "");
const V2_SECRET = (process.env.V2_MATCHER_SECRET || "").trim();
const USDC_ADDRESS = process.env.USDC_ADDRESS || "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const CONFIG_PATH = path.join(__dirname, "seed-bot.config.json");
const BOT_NAME = process.env.SEED_BOT_NAME || "seed-bot";

// Polymarket public read endpoints (no auth). Gamma → market meta (token ids +
// outcome names); CLOB → live per-token midpoint.
const POLY_GAMMA = "https://gamma-api.polymarket.com";
const POLY_CLOB = "https://clob.polymarket.com";

const ERC20_ABI = [
  "function approve(address,uint256) returns (bool)",
  "function allowance(address,address) view returns (uint256)",
  "function balanceOf(address) view returns (uint256)",
];

// ── Small utils ───────────────────────────────────────────────────────────────
const clampCents = (c) => Math.max(1, Math.min(99, Math.round(c)));

/** Canonicalize a team name for matching Polymarket outcome ↔ BlockPools team. */
function canonName(s) {
  return String(s || "")
    .normalize("NFD").replace(/[̀-ͯ]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim().toLowerCase();
}
/** True if two team names refer to the same team (full-name or last-word/mascot match). */
function sameTeam(a, b) {
  const x = canonName(a), y = canonName(b);
  if (!x || !y) return false;
  if (x === y || x.includes(y) || y.includes(x)) return true;
  const xp = x.split(" "), yp = y.split(" ");
  return xp[xp.length - 1] === yp[yp.length - 1]; // mascot (Red Sox / Sox handled loosely)
}

async function getJson(url, opts) {
  const res = await fetch(url, opts);
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `${url} → ${res.status}`);
  return json;
}
async function postJson(url, body, headers) {
  const res = await fetch(url, {
    method: "POST",
    headers: { "content-type": "application/json", ...(headers || {}) },
    body: JSON.stringify(body),
  });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error || `${url} → ${res.status}`);
  return json;
}

// ── Polymarket fair price ─────────────────────────────────────────────────────
const _polyMetaCache = new Map(); // slug -> { tokenIds:[id0,id1], outcomes:[n0,n1] }

/** Resolve a Polymarket market's token ids + outcome names (cached; static). */
async function polyMeta(slug) {
  if (_polyMetaCache.has(slug)) return _polyMetaCache.get(slug);
  const arr = await getJson(`${POLY_GAMMA}/markets?slug=${encodeURIComponent(slug)}`);
  const m = Array.isArray(arr) ? arr[0] : arr;
  if (!m) throw new Error(`no Polymarket market for slug ${slug}`);
  const outcomes = JSON.parse(m.outcomes || "[]");
  const tokenIds = JSON.parse(m.clobTokenIds || "[]");
  if (outcomes.length < 2 || tokenIds.length < 2) throw new Error(`Polymarket ${slug} is not a 2-outcome market`);
  const meta = { tokenIds, outcomes, closed: !!m.closed };
  _polyMetaCache.set(slug, meta);
  return meta;
}

/** CLOB midpoint (0..1) for one token; falls back to Gamma best bid/ask mid. */
async function polyMidpoint(tokenId, slug, polyIdx) {
  try {
    const j = await getJson(`${POLY_CLOB}/midpoint?token_id=${tokenId}`);
    const mid = Number(j.mid);
    if (Number.isFinite(mid) && mid > 0 && mid < 1) return mid;
  } catch { /* fall through */ }
  // Fallback: Gamma bestBid/bestAsk on the market (outcome-0 oriented).
  const arr = await getJson(`${POLY_GAMMA}/markets?slug=${encodeURIComponent(slug)}`);
  const m = Array.isArray(arr) ? arr[0] : arr;
  const bb = Number(m?.bestBid), ba = Number(m?.bestAsk);
  if (Number.isFinite(bb) && Number.isFinite(ba)) {
    const mid0 = (bb + ba) / 2; // this is outcome-0 (first token) oriented
    return polyIdx === 0 ? mid0 : 1 - mid0;
  }
  throw new Error(`no Polymarket price for ${slug}`);
}

/**
 * Fair price (cents) for BlockPools outcome 0 (teamA), read live from Polymarket.
 * Aligns which Polymarket token is teamA by team name, so home/away can't flip.
 * Returns { fair0Cents, closed } or throws (→ dead-man's switch upstream).
 */
async function polyFair(slug, teamAName, teamBName) {
  const meta = await polyMeta(slug);
  let idxA = meta.outcomes.findIndex((o) => sameTeam(o, teamAName));
  if (idxA < 0) {
    const idxB = meta.outcomes.findIndex((o) => sameTeam(o, teamBName));
    if (idxB >= 0) idxA = idxB === 0 ? 1 : 0; // infer A as the other token
  }
  if (idxA < 0) throw new Error(`cannot align Polymarket outcomes [${meta.outcomes}] to ${teamAName}/${teamBName}`);
  const midA = await polyMidpoint(meta.tokenIds[idxA], slug, idxA);
  return { fair0Cents: clampCents(midA * 100), closed: meta.closed };
}

// ── Matcher book ──────────────────────────────────────────────────────────────
async function fetchBook(marketId) {
  return getJson(`${MATCHER_URL}/book?marketId=${encodeURIComponent(marketId)}`);
}
/** True if the matcher is reachable (empty-market book read never errors). */
async function matcherReachable() {
  try { await fetchBook("0x" + "0".repeat(64)); return true; } catch { return false; }
}
const priceToCents = (p1e6) => Number(p1e6) / 1e4; // 1e6 scale → cents

/** The bot's own resting bids on a market: [{hash, outcome, cents}]. */
function houseBids(book, houseSet) {
  const out = [];
  for (const o of [0, 1]) {
    for (const e of book.bids?.[o] || []) {
      if (houseSet.has(String(e.maker || "").toLowerCase())) {
        out.push({ hash: e.hash, outcome: o, cents: priceToCents(e.price) });
      }
    }
  }
  return out;
}
/** Best NON-house user bid (cents) on an outcome, or null. Users we sit behind. */
function bestUserBidCents(book, outcome, houseSet) {
  const bids = (book.bids?.[outcome] || []).filter((e) => !houseSet.has(String(e.maker || "").toLowerCase()));
  if (!bids.length) return null;
  return Math.max(...bids.map((e) => priceToCents(e.price)));
}

async function cancelHash(hash) { try { await postJson(`${MATCHER_URL}/cancel`, { hash }); return true; } catch { return false; } }

// ── Ladder construction ───────────────────────────────────────────────────────
/**
 * Desired bid levels for one market given the live fair and current book.
 * Sits behind users (never at/above the best user bid) and honors inventory skew.
 * Returns { levels:[{outcome,cents}], top:{0,1} }.
 */
function desiredLevels({ fair0Cents, params, book, houseSet, shares0, shares1 }) {
  const { rungs, topSpreadCents, stepCents, inventoryCapShares } = params;
  const levels = [];
  const top = { 0: null, 1: null };
  const sideFair = { 0: fair0Cents, 1: 100 - fair0Cents };
  const sideShares = { 0: shares0, 1: shares1 };
  for (const outcome of [0, 1]) {
    // Inventory skew: if we're already long this outcome past the cap, stop
    // buying more of it (don't add to a one-sided position on a live game).
    if (sideShares[outcome] >= inventoryCapShares) continue;
    let topCents = sideFair[outcome] - topSpreadCents;
    const userBest = bestUserBidCents(book, outcome, houseSet);
    if (userBest != null) topCents = Math.min(topCents, userBest - 1); // one tick behind users
    topCents = clampCents(topCents);
    top[outcome] = topCents;
    for (let k = 0; k < rungs; k++) {
      const cents = topCents - k * stepCents;
      if (cents < 1) break;
      levels.push({ outcome, cents });
    }
  }
  return { levels, top };
}

/** Size shares so Σ(price × shares) == maxExposureUsd across all levels. */
function sizeShares(levels, maxExposureUsd) {
  if (!levels.length) return 0;
  const priceSumDollars = levels.reduce((s, l) => s + l.cents / 100, 0) || 1;
  return Math.max(1, Math.floor((Number(maxExposureUsd) || 0) / priceSumDollars));
}

function buildOrder(maker, marketId, outcome, cents, shares) {
  return {
    maker, marketId, outcome, side: BUY,
    shares: (BigInt(shares) * SHARE_SCALE).toString(),
    price: (BigInt(cents) * (PRICE_SCALE / 100n)).toString(),
    feeRateBps: "1000",
    expiry: "0",
    salt: BigInt(`0x${ethers.hexlify(ethers.randomBytes(8)).slice(2)}`).toString(),
  };
}

// ── Config / games ────────────────────────────────────────────────────────────
function loadV2Games() {
  const raw = JSON.parse(fs.readFileSync(GAMES_JSON, "utf8"));
  const out = [];
  for (const list of Object.values(raw)) if (Array.isArray(list)) for (const g of list) if (g?.v2) out.push(g);
  return out;
}
/** Auto-derive a Polymarket slug from a game when the config doesn't set one. */
function deriveSlug(g) {
  const lg = String(g.league || "").toLowerCase();
  const a = String(g.teamACode || g.teamA || "").toLowerCase();
  const b = String(g.teamBCode || g.teamB || "").toLowerCase();
  const date = String(g.date || (String(g.gameId || "").match(/(\d{4}-\d{2}-\d{2})/) || [])[1] || "");
  if (!lg || !a || !b || !date) return null;
  return `${lg}-${a}-${b}-${date}`; // NOTE: Polymarket slug is away-home; verify per game.
}
/** kickoff epoch: explicit lockTime, else trailing -<epoch> of the gameId. */
function lockTimeOf(g) {
  if (g.lockTime != null && Number(g.lockTime) > 0) return Number(g.lockTime);
  const m = String(g.gameId || "").match(/(\d{9,})$/);
  return m ? Number(m[1]) : 0;
}

/** Resolve config.markets → [{ label, game, marketId, slug, params }]. */
function resolveTargets(config, games) {
  const targets = [];
  for (const m of config.markets || []) {
    const g = games.find((x) =>
      (m.gameId && x.gameId === m.gameId) || (m.slug && x.slug === m.slug) || (m.marketId && x.marketId === m.marketId));
    if (!g) { console.warn(`  ⚠️  no v2 game found for ${m.gameId || m.slug || m.marketId} — skipping`); continue; }
    if (!g.marketId) { console.warn(`  ⚠️  ${g.gameId} has no marketId (group markets not supported yet) — skipping`); continue; }
    const started = lockTimeOf(g) > 0 && Math.floor(Date.now() / 1000) >= lockTimeOf(g);
    const params = { ...config.defaults[started ? "live" : "pre"], ...(m.params || {}) };
    const slug = m.polymarketSlug || deriveSlug(g);
    if (!slug) { console.warn(`  ⚠️  no Polymarket slug for ${g.gameId} — set polymarketSlug in config — skipping`); continue; }
    targets.push({ label: `${g.gameId} · ${g.teamACode}/${g.teamBCode}`, game: g, marketId: g.marketId, slug, params });
  }
  return targets;
}

// ── Control flag (backend bot_control) ────────────────────────────────────────
async function readEnabled() {
  try {
    const j = await getJson(`${BACKEND_URL}/api/v2/seed-bot/control?bot=${encodeURIComponent(BOT_NAME)}`);
    return { ok: true, enabled: !!j.enabled };
  } catch (e) {
    return { ok: false, enabled: false, error: e.message }; // fail-safe: paused
  }
}

// ── Per-market reprice state (in-memory; deadband memory) ─────────────────────
const _lastTop = new Map(); // marketId -> { 0: cents, 1: cents }

/**
 * Reconcile one market: read live fair + book, decide the desired ladder, and
 * (unless within the reprice deadband) cancel our stale bids and post the new
 * ones. Returns a short status string.
 */
async function reprice(tgt, ctx) {
  const { wallet, domain, vault, houseSet, dry } = ctx;
  const { marketId, slug, params, game, label } = tgt;

  // 1) Never seed a resolved market.
  let vm = null;
  try { vm = await vault.markets(marketId); } catch { /* transient RPC → treat as open */ }
  if (vm?.resolved) { await flatten(marketId, ctx); _lastTop.delete(marketId); return `${label} resolved → flat`; }

  // 2) Live fair from Polymarket. Any failure = dead-man's switch (go flat).
  let fair;
  try { fair = await polyFair(slug, game.teamAName || game.teamACode, game.teamBName || game.teamBCode); }
  catch (e) { await flatten(marketId, ctx); _lastTop.delete(marketId); return `${label} feed error (${e.message}) → flat`; }
  if (fair.closed) { await flatten(marketId, ctx); _lastTop.delete(marketId); return `${label} Polymarket closed → flat`; }

  // 3) Book + inventory.
  const book = await fetchBook(marketId);
  const [sh0, sh1] = await Promise.all([
    vault.sharesOf(marketId, 0, wallet.address).then((x) => Number(x) / 1e6).catch(() => 0),
    vault.sharesOf(marketId, 1, wallet.address).then((x) => Number(x) / 1e6).catch(() => 0),
  ]);

  // 4) Desired ladder.
  const { levels, top } = desiredLevels({ fair0Cents: fair.fair0Cents, params, book, houseSet, shares0: sh0, shares1: sh1 });

  // 5) Deadband: if our top on each side hasn't moved beyond repriceDeadbandCents
  //    and we still have live bids, do nothing (no churn on a stable line).
  const prev = _lastTop.get(marketId);
  const current = houseBids(book, houseSet);
  const deadband = Number(params.repriceDeadbandCents ?? 1);
  const withinDeadband = prev && [0, 1].every((o) =>
    (top[o] == null && prev[o] == null) ||
    (top[o] != null && prev[o] != null && Math.abs(top[o] - prev[o]) < deadband));
  if (withinDeadband && current.length) {
    return `${label} fair ${fair.fair0Cents}¢ | inv ${sh0.toFixed(0)}/${sh1.toFixed(0)} | steady`;
  }

  if (dry) {
    const fmt = levels.map((l) => `o${l.outcome}@${l.cents}¢`).join(" ");
    return `${label} DRY fair ${fair.fair0Cents}¢ top ${top[0]}/${top[1]} | would rest: ${fmt || "(none — inv capped)"}`;
  }

  // 6) Cancel our stale bids, post the new ladder.
  let cancelled = 0;
  for (const b of current) if (await cancelHash(b.hash)) cancelled++;

  const shares = sizeShares(levels, params.maxExposureUsd);
  let posted = 0;
  for (const l of levels) {
    const order = buildOrder(wallet.address, marketId, l.outcome, l.cents, shares);
    try {
      const sig = await wallet.signTypedData(domain, ORDER_TYPES, {
        ...order, shares: BigInt(order.shares), price: BigInt(order.price),
        feeRateBps: BigInt(order.feeRateBps), expiry: BigInt(order.expiry), salt: BigInt(order.salt),
      });
      await postJson(`${MATCHER_URL}/orders`, { order, sig });
      posted++;
    } catch (e) { console.warn(`    post failed (${label} o${l.outcome}@${l.cents}¢): ${e.message}`); }
  }
  _lastTop.set(marketId, top);
  return `${label} fair ${fair.fair0Cents}¢ top ${top[0]}/${top[1]} | inv ${sh0.toFixed(0)}/${sh1.toFixed(0)} | -${cancelled} +${posted}`;
}

/** Cancel every house bid on a market (dead-man / disabled / resolved). */
async function flatten(marketId, ctx) {
  const { houseSet } = ctx;
  let book;
  try { book = await fetchBook(marketId); } catch { return 0; }
  const mine = houseBids(book, houseSet);
  let n = 0;
  for (const b of mine) if (await cancelHash(b.hash)) n++;
  return n;
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const args = process.argv.slice(2);
  const RPC = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL;
  const EXCHANGE = process.env.EXCHANGE_ADDRESS;
  const KEY = process.env.SEED_BOT_PRIVATE_KEY || process.env.MM_PRIVATE_KEY || process.env.PRIVATE_KEY;
  if (!RPC || !EXCHANGE || !KEY) throw new Error("Set ARBITRUM_RPC_URL, EXCHANGE_ADDRESS, and SEED_BOT_PRIVATE_KEY (or MM_PRIVATE_KEY/PRIVATE_KEY) in the backend .env");

  const provider = new ethers.JsonRpcProvider(RPC);
  const wallet = new ethers.Wallet(KEY, provider);
  const chainId = Number((await provider.getNetwork()).chainId);
  const domain = buildDomain(chainId, EXCHANGE);
  const VAULT_ADDRESS = process.env.VAULT_ADDRESS || "0x14BD1fd3911C22173FeaEcBfD670D09c1143A594";
  const vault = new ethers.Contract(
    VAULT_ADDRESS,
    [
      "function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)",
      "function sharesOf(bytes32 marketId, uint8 outcome, address user) view returns (uint256)",
    ],
    provider
  );

  // "House" makers we sit BEHIND-of and never treat as user liquidity: this bot
  // + any operator/MM wallets listed in HOUSE_MAKERS.
  const houseSet = new Set([wallet.address.toLowerCase()]);
  for (const w of (process.env.HOUSE_MAKERS || "").split(",").map((s) => s.trim().toLowerCase()).filter(Boolean)) houseSet.add(w);

  console.log(`seed-bot[${BOT_NAME}] wallet ${wallet.address}  exchange ${EXCHANGE}  matcher ${MATCHER_URL}`);

  // --approve: one-time USDC approval so fills can pull the bot's USDC.
  if (args.includes("--approve")) {
    const usdc = new ethers.Contract(USDC_ADDRESS, ERC20_ABI, wallet);
    const tx = await usdc.approve(EXCHANGE, ethers.MaxUint256);
    console.log(`approve tx ${tx.hash} …`); await tx.wait();
    console.log("✅ USDC approved to the Exchange."); return;
  }

  // Balance / allowance sanity.
  try {
    const usdc = new ethers.Contract(USDC_ADDRESS, ERC20_ABI, provider);
    const [bal, alw] = await Promise.all([usdc.balanceOf(wallet.address), usdc.allowance(wallet.address, EXCHANGE)]);
    console.log(`USDC balance ${ethers.formatUnits(bal, 6)}  allowance ${alw >= ethers.parseUnits("1", 30) ? "max" : ethers.formatUnits(alw, 6)}`);
    if (alw === 0n) console.warn("  ⚠️  allowance is 0 — run `--approve` first, or fills will revert.");
  } catch {}

  const dry = args.includes("--dry");
  const ctx = { wallet, domain, vault, houseSet, dry };

  const config = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));

  // --cancel: pull every house order on every configured market, idle.
  if (args.includes("--cancel")) {
    const targets = resolveTargets(config, loadV2Games());
    let n = 0; for (const t of targets) n += await flatten(t.marketId, ctx);
    console.log(`--cancel: pulled ${n} order(s), posted nothing.`); return;
  }

  const tick = async () => {
    // Re-read config + games each tick so edits (and games.json updates) apply
    // live without a restart.
    let cfg, games;
    try { cfg = JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8")); games = loadV2Games(); }
    catch (e) { return console.error(`config/games load error: ${e.message}`); }
    const targets = resolveTargets(cfg, games);
    if (!targets.length) { if (dry) console.log("(no targets resolved — check config.markets gameId vs games.json, and that the entry has v2:true + marketId)"); return; }

    // On/off switch (fail-safe: unreadable ⇒ paused ⇒ go flat). Skipped for
    // --dry so you can preview the ladder while the bot is still switched OFF.
    if (!dry) {
      const ctl = await readEnabled();
      if (!ctl.enabled) {
        let n = 0; for (const t of targets) n += await flatten(t.marketId, ctx);
        if (n) console.log(`⏸  ${BOT_NAME} disabled${ctl.ok ? "" : ` (control unreadable: ${ctl.error})`} — flattened ${n} order(s)`);
        return;
      }
    }

    // Matcher must be up to read books / post.
    if (!dry && !(await matcherReachable())) return console.error(`✗ matcher unreachable at ${MATCHER_URL} — skipping tick`);

    // Per-market cadence: live games every tick, pre-game on the pre cadence.
    const nowMs = Date.now();
    for (const t of targets) {
      const cadenceMs = 1000 * Number(t.params.cadenceSec || 1);
      if (t._nextAt && nowMs < t._nextAt && _lastTop.has(t.marketId)) continue; // not due yet
      try { console.log(`  ${await reprice(t, ctx)}`); }
      catch (e) { console.warn(`  ${t.label}: ${e.message}`); }
      t._nextAt = nowMs + cadenceMs;
    }
  };

  await tick();
  const loopIdx = args.indexOf("--loop");
  if (loopIdx >= 0) {
    const secs = Number(args[loopIdx + 1]) || 1;
    console.log(`looping every ${secs}s … (Ctrl+C to stop)`);
    let running = false;
    setInterval(async () => { if (running) return; running = true; try { await tick(); } catch (e) { console.error(e.message); } finally { running = false; } }, secs * 1000);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });
