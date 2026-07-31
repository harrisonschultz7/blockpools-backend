#!/usr/bin/env node
// scripts/clear-stale-books.js
//
// Pull the OPERATOR/MM wallet's resting seed orders once a game has been live
// for a while, so its stale market-maker ladder can't be sniped late-game.
//
// WHY: v2 order-book markets do NOT lock on-chain at kickoff (unlike the AMM
// pools). Left alone, the seeded MM bid ladder stays fillable at its pre-game
// prices as the outcome becomes known — a user could buy the whole winning side
// at stale odds near the end and drain the LP.
//
// SCOPE — READ THIS: this cancels ONLY the operator/MM wallet's own resting
// orders (SWEEP_MAKERS, default 0xD660fa35…). It does NOT touch:
//   • other users' resting limit orders (left in the book, at their own prices),
//   • anyone's FILLED positions (those are on-chain vault shares — the matcher
//     book only holds unfilled orders; a cancel moves no funds).
// Mechanism: GET /book → keep entries whose maker ∈ SWEEP_MAKERS → POST /cancel
// per hash. Uses only existing matcher endpoints (no matcher change/restart).
//
// It never resolves or settles anything — payouts stay the settler's job
// (settle-v2.js). Cancelling an already-gone order is a harmless no-op, so
// re-running each pass is safe and also re-pulls anything the MM re-seeded.
//
// A game is swept while its kickoff is between STALE_CLEAR_AFTER_SEC and
// STALE_CLEAR_MAX_AGE_SEC in the past.
//
// Env (backend .env): GAMES_JSON_PATH, MATCHER_URL (default 127.0.0.1:8090),
//   SWEEP_MAKERS (comma-sep wallets; default operator 0xD660fa35…),
//   STALE_CLEAR_AFTER_SEC   (default 3600  = start 1h after kickoff),
//   STALE_CLEAR_MAX_AGE_SEC (default 43200 = stop 12h after kickoff).
// Run: node scripts/clear-stale-books.js               # one pass
//      node scripts/clear-stale-books.js --loop 300    # every 5 min (systemd)
//      node scripts/clear-stale-books.js --dry         # preview, cancels nothing
//      node scripts/clear-stale-books.js --market <id> # limit to one game

const fs = require("fs");
const path = require("path");
// Load the backend .env (GAMES_JSON_PATH, MATCHER_URL, SWEEP_MAKERS, STALE_CLEAR_*)
// exactly like settle-v2.js does. WITHOUT this, the systemd service (which sets no
// EnvironmentFile) has no GAMES_JSON_PATH and falls back to the wrong games.json
// dir (frontend/ vs frontend-src/), reads a stale list, and silently sweeps
// nothing — the bug that let seed orders get sniped.
require("dotenv").config({ path: path.join(__dirname, "../.env") });

const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const MATCHER_URL = (process.env.MATCHER_URL || "http://127.0.0.1:8090").replace(/\/+$/, "");
const BACKEND_URL = (process.env.BACKEND_URL || "http://127.0.0.1:8080").replace(/\/+$/, "");
const V2_SECRET = (process.env.V2_MATCHER_SECRET || "").trim();
const AFTER_SEC = Number(process.env.STALE_CLEAR_AFTER_SEC || 3600);
const MAX_AGE_SEC = Number(process.env.STALE_CLEAR_MAX_AGE_SEC || 43200);
const DRY = process.argv.includes("--dry");

// Operator/MM wallet(s) whose resting seed orders we pull. ONLY these makers'
// orders are cancelled — every other maker's orders are left untouched.
const SWEEP_MAKERS = new Set(
  (process.env.SWEEP_MAKERS || "0xD660fa35cd16F768e41C8E09729e39385B51F55c")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);

const argValue = (flag) => {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
};
const loopSec = Number(argValue("--loop") || 0);
const marketFilter = argValue("--market");

// kickoff epoch (lockTime): explicit field, else the trailing …-<epoch> of the
// gameId. Same rule the settler uses so both agree on when a game starts.
function lockTimeOf(g) {
  if (g.lockTime != null && Number(g.lockTime) > 0) return Number(g.lockTime);
  const m = String(g.gameId || "").match(/(\d{9,})$/);
  return m ? Number(m[1]) : 0;
}

function loadV2Games() {
  const raw = JSON.parse(fs.readFileSync(GAMES_JSON, "utf8"));
  const out = [];
  for (const list of Object.values(raw)) {
    if (Array.isArray(list)) for (const g of list) if (g && g.v2) out.push(g);
  }
  return out;
}

// Every on-chain market for a game: the binary/prop single id + any group
// sub-market ids. marketIds are stored directly in games.json.
function marketIdsOf(g) {
  const ids = new Set();
  if (g.marketId) ids.add(g.marketId);
  if (Array.isArray(g.outcomes)) for (const o of g.outcomes) if (o && o.marketId) ids.add(o.marketId);
  return [...ids];
}

// Fetch a market's order book { bids:{0,1}, asks:{0,1} } from the matcher.
async function fetchBook(marketId) {
  const res = await fetch(`${MATCHER_URL}/book?marketId=${encodeURIComponent(marketId)}`);
  if (!res.ok) throw new Error(`book HTTP ${res.status}`);
  return res.json();
}

// The operator/MM wallet's resting order hashes in a book (both outcomes, bids +
// asks). Everything else is left alone.
function operatorHashesFromBook(book) {
  const entries = [
    ...(book.bids?.[0] || []), ...(book.bids?.[1] || []),
    ...(book.asks?.[0] || []), ...(book.asks?.[1] || []),
  ];
  return entries
    .filter((e) => SWEEP_MAKERS.has(String(e.maker || "").toLowerCase()))
    .map((e) => e.hash)
    .filter(Boolean);
}

// Per-outcome book MID in bps (0-10000), complementary — identical to the
// frontend useV2BookPrices formula. Snapshotted before we clear so the UI can
// hold the real price afterwards (prices are 1e6-scaled; /100 → bps).
function bookMidBps(book) {
  const SCALE = 1_000_000;
  const bestBid = (a) => (a && a.length ? Math.max(...a.map((o) => Number(o.price))) : undefined);
  const bestAsk = (a) => (a && a.length ? Math.min(...a.map((o) => Number(o.price))) : undefined);
  const mid = (i) => {
    const j = i === 0 ? 1 : 0;
    const bidJ = bestBid(book.bids?.[j]);
    const askJ = bestAsk(book.asks?.[j]);
    const buys = [bestAsk(book.asks?.[i]), bidJ != null ? SCALE - bidJ : undefined].filter((x) => x != null);
    const sells = [bestBid(book.bids?.[i]), askJ != null ? SCALE - askJ : undefined].filter((x) => x != null);
    const ask = buys.length ? Math.min(...buys) : undefined;
    const bid = sells.length ? Math.max(...sells) : undefined;
    if (ask != null && bid != null) return (ask + bid) / 2 / 100;
    if (ask != null) return ask / 100;
    if (bid != null) return bid / 100;
    return undefined;
  };
  return { aBps: mid(0), bBps: mid(1) };
}

async function cancelOrder(hash) {
  const res = await fetch(`${MATCHER_URL}/cancel`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ hash }),
  });
  if (!res.ok) throw new Error(`cancel HTTP ${res.status}`);
}

// Persist the pre-clear book mid so the UI holds the real price after the sweep
// (covers the "seeded but never traded" case — no fills to fall back on).
async function postLastPrice(gameId, aBps, bBps) {
  const res = await fetch(`${BACKEND_URL}/api/v2/last-price`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...(V2_SECRET ? { "x-v2-secret": V2_SECRET } : {}) },
    body: JSON.stringify({ gameId, aBps: Math.round(aBps), bBps: Math.round(bBps), source: "sweep" }),
  });
  if (!res.ok) throw new Error(`last-price HTTP ${res.status}`);
}

async function tick() {
  const now = Math.floor(Date.now() / 1000);
  let games;
  try {
    games = loadV2Games();
  } catch (e) {
    return console.log(`load games failed: ${e.message}`);
  }
  if (marketFilter) games = games.filter((g) => g.gameId === marketFilter || g.slug === marketFilter);

  for (const g of games) {
    const start = lockTimeOf(g);
    if (!start) continue;
    const age = now - start;
    // Only during the live-game window: after the grace period, before it's ancient.
    if (age < AFTER_SEC || age > MAX_AGE_SEC) continue;
    const ageMin = Math.round(age / 60);

    for (const mid of marketIdsOf(g)) {
      try {
        const book = await fetchBook(mid);
        const hashes = operatorHashesFromBook(book);
        if (!hashes.length) continue; // nothing of ours to sweep (already cleared)
        const px = bookMidBps(book); // pre-clear mid → snapshot before we cancel
        if (DRY) {
          console.log(`WOULD cancel ${hashes.length} operator order(s) + snapshot mid [${px.aBps},${px.bBps}] on ${g.gameId} ${mid} (kickoff ${ageMin}m ago)`);
          continue;
        }
        if (px.aBps != null && px.bBps != null) {
          try { await postLastPrice(g.gameId, px.aBps, px.bBps); }
          catch (err) { console.log(`    last-price snapshot failed: ${err.message}`); }
        }
        let n = 0;
        for (const h of hashes) {
          try { await cancelOrder(h); n++; } catch (err) { console.log(`    cancel ${h} failed: ${err.message}`); }
        }
        if (n > 0) console.log(`swept ${n} operator order(s) on ${g.gameId} ${mid} (kickoff ${ageMin}m ago)`);
      } catch (e) {
        console.log(`  ${g.gameId} ${mid}: sweep failed (${e.message})`);
      }
    }
  }
}

(async () => {
  console.log(
    `clear-stale-books: matcher=${MATCHER_URL} makers=[${[...SWEEP_MAKERS].join(",")}] after=${AFTER_SEC}s maxAge=${MAX_AGE_SEC}s${DRY ? " (DRY RUN)" : ""}`
  );
  const safeTick = async () => {
    try {
      await tick();
    } catch (e) {
      console.log(`tick error: ${e.message}`);
    }
  };
  await safeTick();
  if (loopSec > 0) {
    console.log(`looping every ${loopSec}s…`);
    setInterval(safeTick, loopSec * 1000);
  }
})();
