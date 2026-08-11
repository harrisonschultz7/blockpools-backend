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
//   SEED_BOT_RPC_URL (dedicated bot RPC; falls back to ARBITRUM_RPC_URL / RPC_URL),
//   EXCHANGE_ADDRESS, VAULT_ADDRESS,
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

// Make stdout synchronous so logs flush to journald live. Node buffers piped
// stdout (non-TTY) and would otherwise only flush on exit/crash — which is why
// a healthy run showed no logs until it errored.
try { process.stdout._handle && process.stdout._handle.setBlocking && process.stdout._handle.setBlocking(true); } catch {}

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

/**
 * STRICT team match for props/futures: exact, or one full name is a whole-word
 * prefix/suffix of the other ("Tottenham" ⊂ "Tottenham Hotspur"). NO last-word
 * fallback and NO 3-letter code matching — those cross-match teams that merely
 * share a suffix. Without this, "Coventry City" / "Hull City" both collapsed
 * onto "Manchester City" (all end in "City") and were seeded at ~27% instead of
 * ~0%. Verified to map all 146 configured prop outcomes 1:1 with no collisions.
 */
function sameTeamStrict(a, b) {
  const x = canonName(a), y = canonName(b);
  if (!x || !y) return false;
  if (x === y) return true;
  // One full name must appear inside the other as a COMPLETE word-sequence.
  // Space-padding both sides forces whole-word boundaries, so "chi" can't match
  // "chiefs" and "City" can't bridge "Coventry City" ↔ "Manchester City".
  const wx = ` ${x} `, wy = ` ${y} `;
  return wx.includes(wy) || wy.includes(wx);
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

// ── Polymarket EVENT fair (multi-outcome futures / props) ───────────────────────
// A champion/division future is a Polymarket EVENT (/event/<slug>) containing one
// binary "Will <team> win?" sub-market per team (groupItemTitle = team, outcomes
// ["Yes","No"]). Each BlockPools prop OUTCOME is its own binary market whose
// side-0 ("<team> wins") fair == that team's Yes midpoint. We cache the per-team
// token map (static-ish) but read the midpoint live each call.
const _polyEventCache = new Map(); // eventSlug -> { at, closed, teams:[{title, yesToken, closed}] }

async function polyEventMeta(eventSlug) {
  const c = _polyEventCache.get(eventSlug);
  if (c && Date.now() - c.at < 10 * 60_000) return c; // refresh token map every 10m
  const arr = await getJson(`${POLY_GAMMA}/events?slug=${encodeURIComponent(eventSlug)}`);
  const e = Array.isArray(arr) ? arr[0] : arr;
  if (!e || !Array.isArray(e.markets)) throw new Error(`no Polymarket event for slug ${eventSlug}`);
  const teams = [];
  for (const m of e.markets) {
    let oc = [], tk = [];
    try { oc = JSON.parse(m.outcomes || "[]"); } catch { /* skip */ }
    try { tk = JSON.parse(m.clobTokenIds || "[]"); } catch { /* skip */ }
    const yi = oc.findIndex((o) => String(o).trim().toLowerCase() === "yes");
    if (yi < 0 || !tk[yi]) continue;
    teams.push({ title: m.groupItemTitle || m.question || "", yesToken: tk[yi], closed: !!m.closed });
  }
  if (!teams.length) throw new Error(`Polymarket event ${eventSlug} has no Yes/No sub-markets`);
  const meta = { at: Date.now(), closed: !!e.closed, teams };
  _polyEventCache.set(eventSlug, meta);
  return meta;
}

/** CLOB midpoint (0..1) for one token; throws on failure (no market-slug fallback). */
async function clobMid(tokenId) {
  const j = await getJson(`${POLY_CLOB}/midpoint?token_id=${tokenId}`);
  const mid = Number(j.mid);
  if (Number.isFinite(mid) && mid > 0 && mid < 1) return mid;
  throw new Error(`no CLOB midpoint for token ${tokenId}`);
}

/**
 * Fair (cents) for a prop OUTCOME's side 0 ("<team> wins") = that team's Yes
 * midpoint in the Polymarket event. Returns { fair0Cents, closed } or throws
 * (→ dead-man's switch flattens just that one outcome).
 */
async function polyPropFair(eventSlug, teamName, teamCode) {
  const meta = await polyEventMeta(eventSlug);
  if (meta.closed) return { fair0Cents: 50, closed: true };
  // STRICT, name-only, and require EXACTLY one hit. 0 or >1 → throw (dead-man's
  // switch flattens just this outcome) rather than seed a wrong price. teamCode
  // is intentionally NOT used to match — a 3-letter code substring-matches the
  // wrong team (e.g. "CHI" ⊂ "Chiefs").
  const hits = meta.teams.filter((t) => sameTeamStrict(t.title, teamName));
  if (hits.length !== 1) {
    throw new Error(
      `${hits.length === 0 ? "no" : hits.length + " ambiguous"} Polymarket sub-market(s) for "${teamName}" in ${eventSlug}`
    );
  }
  const mid = await clobMid(hits[0].yesToken);
  return { fair0Cents: clampCents(mid * 100), closed: !!hits[0].closed };
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
 * Desired bid ladder for one market, given the live fair, current book, and our
 * inventory. Two risk controls fold into ONE knob (inventoryCapShares):
 *
 *   • Skew: each side's budget weight = max(0, 1 - shares[side]/cap). As we get
 *     long a side its bids shrink smoothly; the OFFSETTING (short) side is
 *     emphasized in proportion to how lopsided we are, pushing liquidity to the
 *     side that nets us back toward flat.
 *   • Hard cap: that same weight hits 0 exactly at the cap, so we simply stop
 *     bidding a side once we hold `cap` shares of it — no separate cap needed.
 *
 * Also sits one tick BEHIND the best user bid so real user liquidity fills first.
 * Returns { perSide: {0:{levels:[cents…],budget}, 1:{…}}, top:{0,1} }.
 */
function desiredLadder({ fair0Cents, params, book, houseSet, shares0, shares1 }) {
  const { rungs, topSpreadCents, stepCents, inventoryCapShares, maxExposureUsd } = params;
  const cap = Number(inventoryCapShares) > 0 ? Number(inventoryCapShares) : Infinity;
  const sideFair = { 0: fair0Cents, 1: 100 - fair0Cents };
  const sideShares = { 0: shares0, 1: shares1 };
  const top = { 0: null, 1: null };
  const perSide = { 0: { levels: [], budget: 0 }, 1: { levels: [], budget: 0 } };

  // Skew weights (0 at the cap = hard stop) + offsetting-side emphasis.
  const w = { 0: Math.max(0, 1 - sideShares[0] / cap), 1: Math.max(0, 1 - sideShares[1] / cap) };
  const imbalance = cap === Infinity ? 0 : Math.max(0, Math.min(1, Math.abs(sideShares[0] - sideShares[1]) / cap));
  const shortSide = sideShares[0] > sideShares[1] ? 1 : 0;
  w[shortSide] *= 1 + imbalance; // push extra liquidity to the side that flattens us
  const wSum = w[0] + w[1];
  if (wSum <= 0) return { perSide, top }; // both sides capped → seed nothing

  for (const o of [0, 1]) {
    if (w[o] <= 0) continue;
    let topCents = sideFair[o] - topSpreadCents;
    const userBest = bestUserBidCents(book, o, houseSet);
    if (userBest != null) topCents = Math.min(topCents, userBest - 1); // one tick behind users
    topCents = clampCents(topCents);
    top[o] = topCents;
    const levels = [];
    for (let k = 0; k < rungs; k++) {
      const cents = topCents - k * stepCents;
      if (cents < 1) break;
      levels.push(cents);
    }
    perSide[o].levels = levels;
    perSide[o].budget = (Number(maxExposureUsd) || 0) * (w[o] / wSum);
  }
  return { perSide, top };
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
/** YYYY-MM-DD ± days (UTC). */
function addDaysISO(iso, days) {
  const [y, m, d] = String(iso).split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
}

// gameId -> resolved slug (positive, permanent); and a negative-result backoff.
const _slugCache = new Map();
const _slugNextProbe = new Map();
// BlockPools ↔ Polymarket team-CODE differences (extend as found). The team-NAME
// check in resolvePolySlug still guards against a wrong match.
const POLY_CODE_ALIAS = { chw: "cws", cws: "chw" };

/**
 * Discover a game's Polymarket slug by probing variants — team order (away-home
 * vs home-away) × kickoff date ±1 (Polymarket dates by UTC, which can be a day
 * ahead of the local game date) — and verifying BOTH team names appear in the
 * market's outcomes so we never bind to the wrong game. Positive results are
 * cached permanently; misses back off 30 min so a not-yet-posted market is
 * retried, not hammered. Returns a slug or null.
 */
async function resolvePolySlug(game) {
  const gid = String(game.gameId || "");
  if (_slugCache.has(gid)) return _slugCache.get(gid);
  if (Date.now() < (_slugNextProbe.get(gid) || 0)) return null;
  const lg = String(game.league || "").toLowerCase();
  const a = String(game.teamACode || game.teamA || "").toLowerCase();
  const b = String(game.teamBCode || game.teamB || "").toLowerCase();
  const base = String(game.date || (gid.match(/(\d{4}-\d{2}-\d{2})/) || [])[1] || "");
  if (!lg || !a || !b || !base) { _slugNextProbe.set(gid, Date.now() + 30 * 60_000); return null; }
  const av = POLY_CODE_ALIAS[a] ? [a, POLY_CODE_ALIAS[a]] : [a];
  const bv = POLY_CODE_ALIAS[b] ? [b, POLY_CODE_ALIAS[b]] : [b];
  const cands = [];
  for (const d of [base, addDaysISO(base, 1), addDaysISO(base, -1)])
    for (const x of av) for (const y of bv) cands.push(`${lg}-${x}-${y}-${d}`, `${lg}-${y}-${x}-${d}`);
  for (const slug of cands) {
    try {
      const arr = await getJson(`${POLY_GAMMA}/markets?slug=${encodeURIComponent(slug)}`);
      const m = Array.isArray(arr) ? arr[0] : arr;
      if (!m) continue;
      const outcomes = JSON.parse(m.outcomes || "[]");
      if (outcomes.length < 2) continue;
      const okA = outcomes.some((o) => sameTeam(o, game.teamAName || a));
      const okB = outcomes.some((o) => sameTeam(o, game.teamBName || b));
      if (okA && okB) { _slugCache.set(gid, slug); console.log(`  ↳ ${gid} → Polymarket ${slug}`); return slug; }
    } catch { /* try next variant */ }
  }
  _slugNextProbe.set(gid, Date.now() + 30 * 60_000);
  return null;
}
/** kickoff epoch: explicit lockTime, else trailing -<epoch> of the gameId. */
function lockTimeOf(g) {
  if (g.lockTime != null && Number(g.lockTime) > 0) return Number(g.lockTime);
  const m = String(g.gameId || "").match(/(\d{9,})$/);
  return m ? Number(m[1]) : 0;
}

/**
 * Resolve targets = explicit config.markets (always) + every v2 game whose
 * league is in config.autoLeagues AND whose kickoff is inside the active window
 * [kickoff - preSeedLeadHours, kickoff + maxGameHours]. Deduped by marketId,
 * sorted soonest-kickoff-first, capped at config.maxMarkets. Phase (far/pre/
 * live) is recomputed each call so a game transitions far→pre→live on its own;
 * auto games get their slug resolved lazily in reprice (slug: null here).
 * Returns [{ label, game, marketId, slug, params, lockTime }].
 */
function resolveTargets(config, games) {
  const now = Math.floor(Date.now() / 1000);
  const leadSec = Number(config.preSeedLeadHours ?? 24) * 3600;
  // Boundary between the far (early, hourly) and pre (near, 60s) tiers. Defaults
  // to the whole lead window so that, if unset, every pre-game uses `pre` exactly
  // as before.
  const nearSec = Number(config.preSeedNearHours ?? config.preSeedLeadHours ?? 24) * 3600;
  const maxGameSec = Number(config.maxGameHours ?? 5) * 3600;
  const seen = new Set();
  const out = [];

  // Three tiers by time-to-kickoff, recomputed each call so a game walks
  // far → pre → live on its own:
  //   live: started (now >= kickoff)              — unchanged
  //   pre:  within preSeedNearHours of kickoff    — unchanged
  //   far:  preSeedNearHours..preSeedLeadHours out — hourly seeding (falls back
  //         to `pre` params if no `far` phase is configured).
  const phaseParams = (g, override) => {
    const lt = lockTimeOf(g);
    const started = lt > 0 && now >= lt;
    let phase = "pre";
    if (started) phase = "live";
    else if (lt > 0 && now < lt - nearSec) phase = "far";
    const base = config.defaults[phase] || config.defaults.pre;
    return { ...base, ...(override || {}) };
  };
  const add = (g, explicitSlug, override) => {
    const key = String(g.marketId).toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    const codeA = g.teamACode || g.teamA || "A";
    const codeB = g.teamBCode || g.teamB || "B";
    out.push({
      label: `${g.gameId} · ${codeA}/${codeB}`, game: g, marketId: g.marketId,
      slug: explicitSlug || null, params: phaseParams(g, override), lockTime: lockTimeOf(g),
    });
  };

  // Explicit markets — always included (a pinned game / slug override), no window filter.
  for (const m of config.markets || []) {
    const g = games.find((x) =>
      (m.gameId && x.gameId === m.gameId) || (m.slug && x.slug === m.slug) || (m.marketId && x.marketId === m.marketId));
    if (!g) { console.warn(`  ⚠️  no v2 game found for ${m.gameId || m.slug || m.marketId} — skipping`); continue; }
    if (!g.marketId) { console.warn(`  ⚠️  ${g.gameId} has no marketId — skipping`); continue; }
    add(g, m.polymarketSlug || null, m.params);
  }

  // Auto leagues — all in-window v2 games.
  const autoLeagues = (config.autoLeagues || []).map((s) => String(s).toUpperCase());
  if (autoLeagues.length) {
    const inWindow = [];
    for (const g of games) {
      if (!g.marketId || seen.has(String(g.marketId).toLowerCase())) continue;
      if (!autoLeagues.includes(String(g.league || "").toUpperCase())) continue;
      const lt = lockTimeOf(g);
      if (lt <= 0) continue;                 // can't phase an unknown kickoff
      if (now < lt - leadSec) continue;      // too far ahead
      if (now > lt + maxGameSec) continue;   // ended / too old
      inWindow.push(g);
    }
    inWindow.sort((a, b) => lockTimeOf(a) - lockTimeOf(b));
    const cap = Number(config.maxMarkets ?? 24);
    if (inWindow.length > cap) console.warn(`  ⚠️  ${inWindow.length} auto games in window; capping at maxMarkets=${cap} (soonest kickoff first)`);
    for (const g of inWindow.slice(0, cap)) add(g, null, null);
  }

  // Props (multi-outcome futures) — each configured event expands to ONE target
  // per outcome, since each outcome is its own binary Yes/No market with its own
  // marketId (there is no top-level marketId, so these are skipped by the
  // game/auto paths above). Window-exempt: futures seed year-round until they
  // resolve. Priced per-outcome from the Polymarket event (see polyPropFair).
  const propPhase = config.defaults?.prop || config.defaults?.pre || {};
  for (const p of config.props || []) {
    const g = games.find((x) =>
      (p.gameId && x.gameId === p.gameId) || (p.slug && x.slug === p.slug) || (p.marketId && x.marketId === p.marketId));
    if (!g) { console.warn(`  ⚠️  no v2 prop game for ${p.gameId || p.slug} — skipping`); continue; }
    if (!p.polymarketSlug) { console.warn(`  ⚠️  prop ${g.gameId} has no polymarketSlug — skipping`); continue; }
    const outs = Array.isArray(g.outcomes) ? g.outcomes : [];
    if (!outs.length) { console.warn(`  ⚠️  prop ${g.gameId} has no outcomes[] — skipping`); continue; }
    const params = { ...propPhase, ...(p.params || {}) };
    let n = 0;
    for (const oc of outs) {
      if (!oc.marketId) continue;
      const key = String(oc.marketId).toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push({
        label: `${g.gameId} · ${oc.code || oc.label}`,
        game: g, marketId: oc.marketId, slug: null, params, lockTime: lockTimeOf(g),
        prop: {
          eventSlug: p.polymarketSlug,
          teamName: oc.label || oc.code,
          teamCode: oc.code || null,
          // Sub-market id `PARENT::CODE` — the SAME key the fills (user_trade_events)
          // and the /api/v2/chart endpoint use, so the per-outcome price snapshots
          // we post line up with this outcome's chart. Mirrors subMarketGameId().
          snapshotGameId: `${g.gameId}::${String(oc.code || oc.label).toUpperCase()}`,
        },
      });
      n++;
    }
    console.log(`  ↳ prop ${g.gameId} → ${p.polymarketSlug} (${n} outcomes)`);
  }

  return out;
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
// In-flight on-chain tx guard (merge/redeem) per market, so fast ticks don't
// double-submit while a tx is still pending.
const _txInFlight = new Set();
const _nextAt = new Map();      // marketId -> ms; per-market cadence gate (persists across ticks)
const _ended = new Set();       // marketIds flattened + stopped (resolved / closed / past game window)
const _lastMergeAt = new Map(); // marketId -> ms; merge cooldown (gas guard)
const _lastSnap = new Map();    // gameId -> { aBps, at }; price-history snapshot throttle

// RPC read caches. The bot polls many markets fast; without caching, vault
// reads (markets + 2×sharesOf per market per tick) run ~20+ calls/sec and blow
// the provider's monthly cap. markets() rarely changes (resolved flips once,
// lockTime is static) and sharesOf only moves on a fill, so short TTLs cut RPC
// ~30× with negligible staleness for a seeding bot.
const _marketCache = new Map(); // marketId -> { vm, at }
const _sharesCache = new Map(); // marketId -> { sh0, sh1, at }

// ── RPC budget controls (tunable from config; refreshed each tick) ────────────
// The bot's ONLY provider reads are vault.markets() + sharesOf(). Cached reads
// are reused for the TTL; maxReadsPerSec is a HARD ceiling — over it, the bot
// serves the cached value instead of calling. That bounds the monthly bill:
//   worst case ≈ maxReadsPerSec × 2.63M reads/month (~26 CU each on Alchemy).
let _rpcCfg = { marketTtlMs: 300_000, sharesTtlMs: 60_000, maxReadsPerSec: 3 };
let _rpcWindow = { sec: 0, n: 0 };
let _rpcTotal = 0;
let _rpcLogAt = 0, _rpcLogTotal = 0;
function rpcAllow(cost) {
  const s = Math.floor(Date.now() / 1000);
  if (_rpcWindow.sec !== s) _rpcWindow = { sec: s, n: 0 };
  if (_rpcWindow.n + cost > _rpcCfg.maxReadsPerSec) return false;
  _rpcWindow.n += cost; _rpcTotal += cost;
  return true;
}

async function getMarket(vault, marketId) {
  const c = _marketCache.get(marketId);
  if (c && Date.now() - c.at < _rpcCfg.marketTtlMs) return c.vm;
  if (!rpcAllow(1)) return c ? c.vm : null; // over the rate cap → serve cached (null = treat as open)
  try { const vm = await vault.markets(marketId); _marketCache.set(marketId, { vm, at: Date.now() }); return vm; }
  catch { return c ? c.vm : null; } // transient RPC → reuse last known
}
async function getShares(vault, addr, marketId) {
  const c = _sharesCache.get(marketId);
  if (c && Date.now() - c.at < _rpcCfg.sharesTtlMs) return [c.sh0, c.sh1];
  if (!rpcAllow(2)) return c ? [c.sh0, c.sh1] : [0, 0]; // over the rate cap → serve cached
  try {
    const [sh0, sh1] = await Promise.all([
      vault.sharesOf(marketId, 0, addr).then((x) => Number(x) / 1e6).catch(() => (c ? c.sh0 : 0)),
      vault.sharesOf(marketId, 1, addr).then((x) => Number(x) / 1e6).catch(() => (c ? c.sh1 : 0)),
    ]);
    _sharesCache.set(marketId, { sh0, sh1, at: Date.now() });
    return [sh0, sh1];
  } catch { return c ? [c.sh0, c.sh1] : [0, 0]; }
}

/**
 * Recycle complete sets back to USDC while the market is live. Holding shares of
 * BOTH outcomes means min(sh0,sh1) complete sets worth $1 each; mergePositions
 * burns them and returns USDC — freeing budget with ZERO directional risk and
 * locking in the spread we bought them at. Fire-and-forget (guarded) so it never
 * blocks the tick. Returns the set count being merged (to subtract locally this
 * tick); 0 if nothing to do.
 */
function ensureMerge(marketId, sh0, sh1, ctx, params) {
  const { vault, dry } = ctx;
  const sets = Math.floor(Math.min(sh0, sh1));
  const minMerge = Number(params.mergeMinShares ?? 5);
  if (dry || sets < minMerge || _txInFlight.has(marketId)) return 0;
  // Cooldown: don't fire a merge tx (gas) more than once per mergeCooldownSec.
  const cooldownMs = 1000 * Number(params.mergeCooldownSec ?? 30);
  if (Date.now() - (_lastMergeAt.get(marketId) || 0) < cooldownMs) return 0;
  _lastMergeAt.set(marketId, Date.now());
  _sharesCache.delete(marketId); // force a fresh inventory read next tick (post-merge)
  _txInFlight.add(marketId);
  vault.mergePositions(marketId, BigInt(sets) * SHARE_SCALE)
    .then((tx) => tx.wait())
    .then(() => console.log(`  ↩ merged ${sets} set(s) → +$${sets} USDC recycled (${marketId.slice(0, 10)}…)`))
    .catch((e) => console.warn(`  merge failed (${marketId.slice(0, 10)}…): ${e.message}`))
    .finally(() => _txInFlight.delete(marketId));
  return sets;
}

/**
 * After a market resolves, collect our winnings: redeem() pays out winning
 * shares and zeroes the position. Fire-and-forget (guarded). Returns true if a
 * redeem was submitted.
 */
async function ensureRedeem(marketId, ctx) {
  const { vault, wallet, dry } = ctx;
  if (dry || _txInFlight.has(marketId)) return false;
  let held = 0;
  try {
    const [s0, s1] = await Promise.all([
      vault.sharesOf(marketId, 0, wallet.address).then((x) => Number(x)).catch(() => 0),
      vault.sharesOf(marketId, 1, wallet.address).then((x) => Number(x)).catch(() => 0),
    ]);
    held = s0 + s1;
  } catch { return false; }
  if (held <= 0) return false;
  _txInFlight.add(marketId);
  vault.redeem(marketId)
    .then((tx) => tx.wait())
    .then(() => console.log(`  ✔ redeemed resolved market (${marketId.slice(0, 10)}…)`))
    .catch((e) => console.warn(`  redeem failed (${marketId.slice(0, 10)}…): ${e.message}`))
    .finally(() => _txInFlight.delete(marketId));
  return true;
}

/**
 * Append a throttled book-mid snapshot to the price-history chart series. We
 * already have the live fair each tick, so this is nearly free. Throttled to a
 * ≥1¢ move OR a heartbeat (60s live / 10m pre-game) so the table stays tiny.
 * Fire-and-forget — never blocks the tick or spams on a backend hiccup.
 */
function maybeSnapshot(gameId, fair0Cents, started, dry, heartbeatMs) {
  if (dry || !gameId) return;
  const aBps = Math.round(fair0Cents * 100);
  const bBps = 10000 - aBps;
  const prev = _lastSnap.get(gameId);
  const now = Date.now();
  const moved = !prev || Math.abs(aBps - prev.aBps) >= 100;    // ≥1¢
  // Heartbeat: append a point even without a move so the line doesn't flatline
  // through gaps. Props pass a long heartbeat (they barely move and there are
  // many outcomes) so the history table stays lean; games default by phase.
  const hb = heartbeatMs != null ? heartbeatMs : started ? 60_000 : 600_000;
  const stale = !prev || now - prev.at >= hb;
  if (!moved && !stale) return;
  _lastSnap.set(gameId, { aBps, at: now });
  postJson(`${BACKEND_URL}/api/v2/price-point`, { gameId, aBps, bBps }, V2_SECRET ? { "x-v2-secret": V2_SECRET } : {})
    .catch(() => { /* never block/spam on a backend hiccup */ });
}

/**
 * Reconcile one market: read live fair + book, decide the desired ladder, and
 * (unless within the reprice deadband) cancel our stale bids and post the new
 * ones. Returns a short status string.
 */
async function reprice(tgt, ctx) {
  const { wallet, domain, vault, houseSet, dry } = ctx;
  const { marketId, params, game, label } = tgt;
  let slug = tgt.slug;
  const isProp = !!tgt.prop;

  // 1) Resolved market → pull our bids and redeem any winning shares. Terminal.
  const vm = await getMarket(vault, marketId);
  if (vm?.resolved) {
    await flatten(marketId, ctx);
    const redeeming = await ensureRedeem(marketId, ctx);
    _lastTop.delete(marketId);
    return { msg: `${label} resolved → flat${redeeming ? " + redeem" : ""}`, ended: true };
  }

  // 2) Resolve the Polymarket slug on first sight (auto GAMES only); cached after.
  //    Props carry an explicit event slug (tgt.prop.eventSlug) — no discovery.
  if (!isProp && !slug) {
    slug = await resolvePolySlug(game);
    if (!slug) { await flatten(marketId, ctx); _lastTop.delete(marketId); return { msg: `${label} no Polymarket market yet → flat (retry 5m)`, ended: false, retryMs: 300_000 }; }
  }

  // 3) Live fair from Polymarket. Any failure = dead-man's switch (go flat).
  //    Games: one binary market (teamA vs teamB). Props: this outcome's team's
  //    "Yes" (win) midpoint within the event.
  let fair;
  try {
    fair = isProp
      ? await polyPropFair(tgt.prop.eventSlug, tgt.prop.teamName, tgt.prop.teamCode)
      : await polyFair(slug, game.teamAName || game.teamACode, game.teamBName || game.teamBCode);
  } catch (e) { await flatten(marketId, ctx); _lastTop.delete(marketId); return { msg: `${label} feed error (${e.message}) → flat`, ended: false }; }
  if (fair.closed) { await flatten(marketId, ctx); _lastTop.delete(marketId); return { msg: `${label} Polymarket closed → flat`, ended: true }; }

  // Game phase — drives the snapshot heartbeat and whether we read inventory.
  const started = tgt.lockTime > 0 && Math.floor(Date.now() / 1000) >= tgt.lockTime;
  // Throttled book-mid snapshot for the price-history chart.
  //  - Games: one binary series keyed by the game's gameId.
  //  - Props: one series PER OUTCOME, keyed by the sub-market id `PARENT::CODE`
  //    (matches the fills + /api/v2/chart), aBps = this outcome's Yes/win price.
  //    started=false so the pre-game (10min) heartbeat applies; since props
  //    reprice hourly, each reprice emits at most one point per outcome.
  if (isProp) maybeSnapshot(tgt.prop.snapshotGameId, fair.fair0Cents, false, dry, 6 * 60 * 60 * 1000);
  else maybeSnapshot(game.gameId, fair.fair0Cents, started, dry);

  // 3) Book + inventory. For games, inventory only accrues post-kickoff, so we
  // skip the sharesOf reads pre-game to save RPC. Props trade for months before
  // their (far-future) lockTime, so they must ALWAYS read inventory — otherwise
  // the skew/cap safety never engages and a side could accumulate unbounded.
  const book = await fetchBook(marketId);
  let [sh0, sh1] = (started || isProp) ? await getShares(vault, wallet.address, marketId) : [0, 0];

  // 3.5) Auto-merge complete sets back to USDC (budget reset, keeps the spread).
  //      Subtract locally so this tick's ladder sees the post-merge inventory.
  const merged = ensureMerge(marketId, sh0, sh1, ctx, params);
  if (merged) { sh0 = Math.max(0, sh0 - merged); sh1 = Math.max(0, sh1 - merged); }

  // 4) Desired ladder (inventory-skewed, user-first) → per-side sizing.
  const { perSide, top } = desiredLadder({ fair0Cents: fair.fair0Cents, params, book, houseSet, shares0: sh0, shares1: sh1 });
  const orders = [];
  for (const o of [0, 1]) {
    const { levels, budget } = perSide[o];
    if (!levels.length || budget <= 0) continue;
    const priceSum = levels.reduce((s, c) => s + c / 100, 0) || 1;
    const shares = Math.max(1, Math.floor(budget / priceSum)); // Σ(price×shares) == side budget
    for (const cents of levels) orders.push({ outcome: o, cents, shares });
  }

  // 5) Deadband: if our top on each side hasn't moved beyond repriceDeadbandCents
  //    and we still have live bids, do nothing (no churn on a stable line).
  const prev = _lastTop.get(marketId);
  const current = houseBids(book, houseSet);
  const deadband = Number(params.repriceDeadbandCents ?? 1);
  const withinDeadband = prev && [0, 1].every((o) =>
    (top[o] == null && prev[o] == null) ||
    (top[o] != null && prev[o] != null && Math.abs(top[o] - prev[o]) < deadband));
  if (withinDeadband && current.length) {
    return { msg: `${label} fair ${fair.fair0Cents}¢ | inv ${sh0.toFixed(0)}/${sh1.toFixed(0)} | steady`, ended: false };
  }

  if (dry) {
    const fmt = orders.map((l) => `o${l.outcome}@${l.cents}¢×${l.shares}`).join(" ");
    return { msg: `${label} DRY fair ${fair.fair0Cents}¢ top ${top[0]}/${top[1]} inv ${sh0.toFixed(0)}/${sh1.toFixed(0)} | would rest: ${fmt || "(none — inv capped)"}`, ended: false };
  }

  // 6) Cancel our stale bids, post the new ladder.
  let cancelled = 0;
  for (const b of current) if (await cancelHash(b.hash)) cancelled++;
  let posted = 0;
  for (const l of orders) {
    const order = buildOrder(wallet.address, marketId, l.outcome, l.cents, l.shares);
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
  return { msg: `${label} fair ${fair.fair0Cents}¢ top ${top[0]}/${top[1]} | inv ${sh0.toFixed(0)}/${sh1.toFixed(0)} | -${cancelled} +${posted}`, ended: false };
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
  // Prefer a DEDICATED RPC for the bot (SEED_BOT_RPC_URL) so its polling can't
  // drain — or be throttled by — the shared app key. Falls back to the app RPC.
  const RPC = process.env.SEED_BOT_RPC_URL || process.env.ARBITRUM_RPC_URL || process.env.RPC_URL;
  const EXCHANGE = process.env.EXCHANGE_ADDRESS;
  const KEY = process.env.SEED_BOT_PRIVATE_KEY || process.env.MM_PRIVATE_KEY || process.env.PRIVATE_KEY;
  if (!RPC || !EXCHANGE || !KEY) throw new Error("Set SEED_BOT_RPC_URL (or ARBITRUM_RPC_URL), EXCHANGE_ADDRESS, and SEED_BOT_PRIVATE_KEY (or MM_PRIVATE_KEY/PRIVATE_KEY) in the backend .env");

  const provider = new ethers.JsonRpcProvider(RPC);
  const wallet = new ethers.Wallet(KEY, provider);
  // Hardcode Arbitrum One (42161) instead of an RPC getNetwork() round-trip —
  // removes a startup RPC dependency (a dead/throttled RPC crash-looped the
  // service here). Override via CHAIN_ID only if the deployment ever moves.
  const chainId = Number(process.env.CHAIN_ID || 42161);
  const domain = buildDomain(chainId, EXCHANGE);
  const VAULT_ADDRESS = process.env.VAULT_ADDRESS || "0x14BD1fd3911C22173FeaEcBfD670D09c1143A594";
  const vault = new ethers.Contract(
    VAULT_ADDRESS,
    [
      "function markets(bytes32) view returns (bool exists,bool resolved,uint8 outcomeCount,uint64 lockTime,uint32 payoutDenom)",
      "function sharesOf(bytes32 marketId, uint8 outcome, address user) view returns (uint256)",
      // Writes (merge = recycle a complete set back to USDC while live;
      // redeem = collect winnings after the market resolves). Signed by the bot.
      "function mergePositions(bytes32 marketId, uint256 amount)",
      "function redeem(bytes32 marketId)",
    ],
    wallet
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
    const [bal, alw, eth] = await Promise.all([
      usdc.balanceOf(wallet.address),
      usdc.allowance(wallet.address, EXCHANGE),
      provider.getBalance(wallet.address),
    ]);
    console.log(`USDC balance ${ethers.formatUnits(bal, 6)}  allowance ${alw >= ethers.parseUnits("1", 30) ? "max" : ethers.formatUnits(alw, 6)}  ETH ${ethers.formatEther(eth)}`);
    if (alw === 0n) console.warn("  ⚠️  allowance is 0 — run `--approve` first, or fills will revert.");
    // merge/redeem are on-chain txs — the wallet needs a little Arbitrum ETH for gas.
    if (eth < ethers.parseEther("0.0005")) console.warn("  ⚠️  low ETH — auto-merge/redeem txs will fail without gas; top up a little Arbitrum ETH.");
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

    // Refresh RPC budget controls from config (hot-reloaded) + log usage/min so
    // the on-chain read burn is visible at a glance.
    _rpcCfg = {
      marketTtlMs: 1000 * Number(cfg.marketTtlSec ?? 300),
      sharesTtlMs: 1000 * Number(cfg.sharesTtlSec ?? 60),
      maxReadsPerSec: Number(cfg.maxRpcReadsPerSec ?? 3),
    };
    if (Date.now() - _rpcLogAt > 60_000) {
      if (_rpcLogAt) console.log(`[rpc] ${_rpcTotal} vault reads total (${_rpcTotal - _rpcLogTotal}/min; cap ${_rpcCfg.maxReadsPerSec}/s)`);
      _rpcLogAt = Date.now(); _rpcLogTotal = _rpcTotal;
    }
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

    // Per-market lifecycle + cadence: skip games we've already stopped, retire a
    // game once it's past its window (flatten once, no orders on a final game),
    // and gate each market by its phase cadence (pre-game slow, live fast).
    const nowMs = Date.now();
    const nowSec = Math.floor(nowMs / 1000);
    const maxGameSec = Number(cfg.maxGameHours ?? 5) * 3600;
    for (const t of targets) {
      if (_ended.has(t.marketId)) continue; // already flattened + stopped this run
      if (t.lockTime > 0 && nowSec > t.lockTime + maxGameSec) {
        if (!dry) { const n = await flatten(t.marketId, ctx); if (n) console.log(`  ${t.label} game window over → flattened ${n}, stop`); }
        _ended.add(t.marketId); _nextAt.delete(t.marketId);
        continue;
      }
      const cadenceMs = 1000 * Number(t.params.cadenceSec || 2);
      if (nowMs < (_nextAt.get(t.marketId) || 0)) continue; // not due yet (purely time-based gate)
      let waitMs = cadenceMs;
      try {
        const r = await reprice(t, ctx);
        console.log(`  ${r.msg}`);
        if (r.ended && !dry) { _ended.add(t.marketId); _nextAt.delete(t.marketId); continue; }
        if (r.retryMs) waitMs = r.retryMs; // e.g. back off a game with no Polymarket market
      } catch (e) { console.warn(`  ${t.label}: ${e.message}`); }
      _nextAt.set(t.marketId, nowMs + waitMs);
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
