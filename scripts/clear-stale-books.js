#!/usr/bin/env node
// scripts/clear-stale-books.js
//
// Pull v2 market liquidity once a game has been live for a while, so seeded
// market-maker orders can't be sniped late in the game.
//
// WHY: v2 order-book markets do NOT lock on-chain at kickoff (unlike the AMM
// pools). Left alone, the resting MM ladder stays fillable at its stale seed
// prices as the game's outcome becomes known — a user could buy the entire
// winning side near the end at pre-game odds and drain the LP. Clearing the book
// once the game is underway removes that resting liquidity, effectively freezing
// the market for the rest of the game.
//
// WHAT IT DOES: for every v2 game whose kickoff is between STALE_CLEAR_AFTER_SEC
// and STALE_CLEAR_MAX_AGE_SEC in the past, POST /clear-market for each of its
// on-chain markets (binary/prop single id + every group sub-market id). It clears
// the WHOLE book (MM + any user limit orders) — mid-game we don't want ANY
// snipeable resting liquidity. Re-running is safe: clearing an empty book is a
// no-op, and re-clearing each pass also wipes anything re-seeded during the game.
//
// WHAT IT DOES NOT DO: it never resolves or settles anything — payouts stay the
// settler's job (settle-v2.js). It only touches the off-chain matcher book; no
// RPC, no keys, no on-chain writes.
//
// Env (backend .env): GAMES_JSON_PATH, MATCHER_URL (default 127.0.0.1:8090),
//   STALE_CLEAR_AFTER_SEC   (default 3600  = start clearing 1h after kickoff),
//   STALE_CLEAR_MAX_AGE_SEC (default 43200 = stop 12h after kickoff — game's over).
// Run: node scripts/clear-stale-books.js               # one pass
//      node scripts/clear-stale-books.js --loop 300    # every 5 min (systemd/cron)
//      node scripts/clear-stale-books.js --dry         # preview, no POST
//      node scripts/clear-stale-books.js --market <id> # limit to one game

const fs = require("fs");
const path = require("path");

const GAMES_JSON = process.env.GAMES_JSON_PATH || path.join(__dirname, "../../frontend/src/data/games.json");
const MATCHER_URL = (process.env.MATCHER_URL || "http://127.0.0.1:8090").replace(/\/+$/, "");
const AFTER_SEC = Number(process.env.STALE_CLEAR_AFTER_SEC || 3600);
const MAX_AGE_SEC = Number(process.env.STALE_CLEAR_MAX_AGE_SEC || 43200);
const DRY = process.argv.includes("--dry");

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

async function clearMarket(marketId) {
  const res = await fetch(`${MATCHER_URL}/clear-market`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ marketId }),
  });
  if (!res.ok) throw new Error(`clear-market HTTP ${res.status}`);
  const json = await res.json().catch(() => ({}));
  return Number(json.cleared || 0);
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

    for (const mid of marketIdsOf(g)) {
      try {
        if (DRY) {
          console.log(`WOULD clear ${g.gameId} ${mid} (kickoff ${Math.round(age / 60)}m ago)`);
          continue;
        }
        const n = await clearMarket(mid);
        if (n > 0) console.log(`cleared ${g.gameId} ${mid}: ${n} order(s) (kickoff ${Math.round(age / 60)}m ago)`);
      } catch (e) {
        console.log(`  ${g.gameId} ${mid}: clear failed (${e.message})`);
      }
    }
  }
}

(async () => {
  console.log(
    `clear-stale-books: matcher=${MATCHER_URL} after=${AFTER_SEC}s maxAge=${MAX_AGE_SEC}s${DRY ? " (DRY RUN)" : ""}`
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
