// src/routes/cacheRoutes.ts
import { Router } from "express";
import { ENV } from "../config/env";
import {
  keyLeaderboard,
  keyUserBetsPage,
  keyUserClaimsAndStats,
  keyUserSummary,
  keyUserTradesPage,
} from "../cache/cacheKeys";
import {
  serveWithStaleWhileRevalidate,
  refreshLeaderboard,
  refreshUserBetsPage,
  refreshUserClaimsAndStats,
  refreshUserSummary,
  refreshUserTradesPage,
  bustUserCache,
} from "../services/cacheRefresh";
import { upsertUserTradesAndGames } from "../services/persistTrades";
import { invalidateSocialTags } from "./socialTags";

function clampPageSize(v: any) {
  const n = parseInt(String(v || "25"), 10);
  return Math.max(ENV.MIN_PAGE_SIZE, Math.min(ENV.MAX_PAGE_SIZE, n));
}
function clampPage(v: any) {
  const n = parseInt(String(v || "1"), 10);
  return Math.max(1, n);
}
function normAddr(a: string) {
  return (a || "").toLowerCase();
}
function assertAddr(address: string) {
  return /^0x[a-f0-9]{40}$/.test(address);
}

const DEFAULT_LEAGUES = ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL", "WC"];

export const cacheRoutes = Router();

cacheRoutes.get("/meta", async (_req, res) => {
  res.json({ ok: true, now: new Date().toISOString() });
});

// ------------------------------------------------------------
// Cache bust — call immediately after a claim/trade tx confirms
// POST /cache/user/:address/bust
// ------------------------------------------------------------
cacheRoutes.post("/user/:address/bust", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ error: "Invalid address" });

  const cleared = bustUserCache(address);
  res.json({ ok: true, busted: address, cleared });
});

// ------------------------------------------------------------
// Direct-write a CLAIM row immediately on-confirm.
// POST /cache/user/:address/record-claim
// Body: { txHash, contract|gameId, payoutUsd|amountDec, timestamp?, league? }
//
// Why: the profile Trade History + winnings read from the DB
// (public.user_trade_events), which is normally fed by the subgraph
// pull running ~30min behind chain. Writing the CLAIM row here makes
// the win show immediately after the claim tx confirms.
//
// Dedup: this row uses id `claim-direct-<txHash>`. The subgraph backfill
// (refreshUserTradesPage) skips any subgraph claim whose txHash already
// has a CLAIM row in the DB, so the same claim is never persisted twice
// and per-game winnings (SUM(net_out_dec)) are never double-counted.
// ------------------------------------------------------------
cacheRoutes.post("/user/:address/record-claim", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ ok: false, error: "Invalid address" });

  const b = (req.body || {}) as Record<string, unknown>;
  const txHash = b.txHash ? String(b.txHash) : null;
  const gameId = b.contract
    ? String(b.contract).toLowerCase()
    : b.gameId
      ? String(b.gameId).toLowerCase()
      : "";
  const payout =
    b.payoutUsd != null ? String(b.payoutUsd) : b.amountDec != null ? String(b.amountDec) : "0";
  const tsNum = Number(b.timestamp);
  const timestamp = Number.isFinite(tsNum) && tsNum > 0 ? Math.trunc(tsNum) : Math.floor(Date.now() / 1000);
  const league = b.league != null ? String(b.league) : null;

  if (!txHash || !gameId) {
    return res.status(400).json({ ok: false, error: "txHash and contract/gameId are required" });
  }

  const row = {
    id: `claim-direct-${txHash.toLowerCase()}`,
    type: "CLAIM",
    side: null,
    timestamp,
    txHash,
    spotPriceBps: null,
    avgPriceBps: null,
    grossInDec: "0",
    grossOutDec: payout,
    feeDec: "0",
    netStakeDec: "0",
    netOutDec: payout,
    costBasisClosedDec: "0",
    realizedPnlDec: "0",
    game: { id: gameId, league },
    __source: "claim-direct",
  };

  try {
    await upsertUserTradesAndGames({ user: address, tradeRows: [row] });
    bustUserCache(address);
    return res.json({ ok: true, id: row.id });
  } catch (e: any) {
    console.log(`[record-claim] err: ${String(e?.message || e)}`);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ------------------------------------------------------------
// Direct-write a BUY row immediately on-confirm.
// POST /cache/user/:address/record-trade
// Body: { txHash, contract|gameId, outcomeIndex, outcomeCode?, side?,
//         amountUsd|grossInDec, netStakeUsd?, feeUsd?, spotPriceBps?,
//         avgPriceBps?, timestamp?, league? }
//
// Why: identical motivation to /record-claim. The profile Trade History,
// positions, and per-game accounting read from the DB (public.user_trade_events),
// normally fed by the subgraph pull running ~30min behind chain. Smart-wallet
// (ERC-4337) buys in particular were sometimes never picked up at all, so the
// BUY never landed even though the matching CLAIM (direct-written) did. Writing
// the BUY row here makes the bet show immediately after the buy tx confirms.
//
// Dedup: this row uses id `buy-direct-<txHash>`. The subgraph backfill
// (refreshUserTradesPage) DELETEs any `buy-direct-%` row whose txHash matches an
// incoming authoritative subgraph BUY before upserting it, so the same buy is
// never persisted twice (exposure / cost-basis are never double-counted) and the
// estimated price/shares get upgraded to exact on-chain values.
// ------------------------------------------------------------
cacheRoutes.post("/user/:address/record-trade", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ ok: false, error: "Invalid address" });

  const b = (req.body || {}) as Record<string, unknown>;
  const txHash = b.txHash ? String(b.txHash) : null;
  const gameId = b.contract
    ? String(b.contract).toLowerCase()
    : b.gameId
      ? String(b.gameId).toLowerCase()
      : "";

  // outcomeIndex is REQUIRED: persistTrades drops any BUY without it.
  const outcomeIndexNum = Number(b.outcomeIndex);
  const outcomeIndex = Number.isFinite(outcomeIndexNum) ? Math.trunc(outcomeIndexNum) : null;

  const gross =
    b.grossInDec != null ? String(b.grossInDec) : b.amountUsd != null ? String(b.amountUsd) : "0";
  // Without an exact fee split client-side, net stake ~= gross. The subgraph
  // backfill replaces this row with exact fee/net values via the reconcile below.
  const netStake = b.netStakeUsd != null ? String(b.netStakeUsd) : gross;
  const fee = b.feeUsd != null ? String(b.feeUsd) : "0";

  const spotPriceBps = b.spotPriceBps != null ? Number(b.spotPriceBps) : null;
  const avgPriceBps = b.avgPriceBps != null ? Number(b.avgPriceBps) : null;

  const tsNum = Number(b.timestamp);
  const timestamp =
    Number.isFinite(tsNum) && tsNum > 0 ? Math.trunc(tsNum) : Math.floor(Date.now() / 1000);
  const league = b.league != null ? String(b.league) : null;
  const outcomeCode = b.outcomeCode != null ? String(b.outcomeCode) : null;
  const side = b.side != null ? String(b.side) : null;

  if (!txHash || !gameId) {
    return res.status(400).json({ ok: false, error: "txHash and contract/gameId are required" });
  }
  if (outcomeIndex == null) {
    return res.status(400).json({ ok: false, error: "outcomeIndex is required" });
  }

  const row = {
    id: `buy-direct-${txHash.toLowerCase()}`,
    type: "BUY",
    side,
    outcomeIndex,
    outcomeCode,
    timestamp,
    txHash,
    spotPriceBps,
    avgPriceBps,
    grossInDec: gross,
    grossOutDec: "0",
    feeDec: fee,
    netStakeDec: netStake,
    netOutDec: "0",
    costBasisClosedDec: "0",
    realizedPnlDec: "0",
    game: { id: gameId, league },
    __source: "buy-direct",
  };

  try {
    await upsertUserTradesAndGames({ user: address, tradeRows: [row] });
    bustUserCache(address);
    // Drop this game's social-tag counts so the new position is reflected on the
    // next read (open markets otherwise refresh on a 60s TTL).
    invalidateSocialTags(gameId);
    return res.json({ ok: true, id: row.id });
  } catch (e: any) {
    console.log(`[record-trade] err: ${String(e?.message || e)}`);
    return res.status(500).json({ ok: false, error: String(e?.message || e) });
  }
});

// ------------------------------------------------------------
// Leaderboard
// GET /cache/leaderboard?league=ALL&range=ALL&sort=ROI&page=1&pageSize=25
// ------------------------------------------------------------
cacheRoutes.get("/leaderboard", async (req, res) => {
  const league = String(req.query.league || "ALL").toUpperCase();
  const range = String(req.query.range || "ALL").toUpperCase();
  const sort = String(req.query.sort || "ROI").toUpperCase();

  const params = {
    leagues: league === "ALL" ? DEFAULT_LEAGUES : [league],
    range,
    sort,
    page: clampPage(req.query.page),
    pageSize: clampPageSize(req.query.pageSize),
  };

  const cacheKey = keyLeaderboard(params);

  const out = await serveWithStaleWhileRevalidate({
    cacheKey,
    params,
    view: "leaderboard",
    refreshFn: refreshLeaderboard,
    scope: "global",
  });

  res.setHeader("Cache-Control", "public, max-age=3, stale-while-revalidate=30");
  res.json({ ok: true, ...out.payload, cache: out.meta });
});

// ------------------------------------------------------------
// User dropdown summary (bets + claims + userGameStats)
// GET /cache/user/:address/summary?first=20
//
// NOTE: This is legacy "summary", not a trade ledger.
// It will never include SELL.
// ------------------------------------------------------------
cacheRoutes.get("/user/:address/summary", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ error: "Invalid address" });

  const first = Math.max(1, Math.min(100, parseInt(String(req.query.first || "20"), 10)));

  const params = {
    user: address,
    betsFirst: first,
    claimsFirst: Math.min(250, first * 5),
    statsFirst: 250,
  };

  const cacheKey = keyUserSummary(params);

  const out = await serveWithStaleWhileRevalidate({
    cacheKey,
    params,
    view: "userSummary",
    refreshFn: refreshUserSummary,
    scope: "user",
  });

  res.setHeader("Cache-Control", "public, max-age=2, stale-while-revalidate=15");
  res.json({ ok: true, ...out.payload, cache: out.meta });
});

// ------------------------------------------------------------
// User trade ledger (BUY + SELL)
// GET /cache/user/:address/trades?page=1&pageSize=25&league=ALL&range=ALL
// ------------------------------------------------------------
cacheRoutes.get("/user/:address/trades", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ error: "Invalid address" });

  const league = String(req.query.league || "ALL").toUpperCase();
  const range = String(req.query.range || "ALL").toUpperCase();

  const params = {
    user: address,
    leagues: league === "ALL" ? DEFAULT_LEAGUES : [league],
    range,
    page: clampPage(req.query.page),
    pageSize: clampPageSize(req.query.pageSize),
  };

  const cacheKey = keyUserTradesPage(params);

  const out = await serveWithStaleWhileRevalidate({
    cacheKey,
    params,
    view: "userTradesPage",
    refreshFn: refreshUserTradesPage,
    scope: "user",
  });

  res.setHeader("Cache-Control", "public, max-age=2, stale-while-revalidate=15");
  res.json({ ok: true, ...out.payload, cache: out.meta });
});

// ------------------------------------------------------------
// User trade history (paginated bets) - LEGACY
// GET /cache/user/:address/bets?page=1&pageSize=10
// ------------------------------------------------------------
cacheRoutes.get("/user/:address/bets", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ error: "Invalid address" });

  const params = {
    user: address,
    page: clampPage(req.query.page),
    pageSize: clampPageSize(req.query.pageSize),
  };

  const cacheKey = keyUserBetsPage(params);

  const out = await serveWithStaleWhileRevalidate({
    cacheKey,
    params,
    view: "userBetsPage",
    refreshFn: refreshUserBetsPage,
    scope: "user",
  });

  res.setHeader("Cache-Control", "public, max-age=2, stale-while-revalidate=15");
  res.json({ ok: true, ...out.payload, cache: out.meta });
});

// ------------------------------------------------------------
// Profile support: claims + userGameStats
// GET /cache/user/:address/claims-stats
// ------------------------------------------------------------
cacheRoutes.get("/user/:address/claims-stats", async (req, res) => {
  const address = normAddr(String(req.params.address));
  if (!assertAddr(address)) return res.status(400).json({ error: "Invalid address" });

  const params = {
    user: address,
    claimsFirst: 250,
    statsFirst: 250,
  };

  const cacheKey = keyUserClaimsAndStats(params);

  const out = await serveWithStaleWhileRevalidate({
    cacheKey,
    params,
    view: "userClaimsAndStats",
    refreshFn: refreshUserClaimsAndStats,
    scope: "user",
  });

  res.setHeader("Cache-Control", "public, max-age=2, stale-while-revalidate=15");
  res.json({ ok: true, ...out.payload, cache: out.meta });
});

export default cacheRoutes;