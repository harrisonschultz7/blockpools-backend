// src/routes/cacheRoutes.ts
import { Router } from "express";
import { ENV } from "../config/env";
import {
  keyLeaderboard,
  keyUserBetsPage,
  keyUserClaimsAndStats,
  keyUserSummary,
  // NEW
  keyUserTradesPage,
} from "../cache/cacheKeys";
import {
  serveWithStaleWhileRevalidate,
  refreshLeaderboard,
  refreshUserBetsPage,
  refreshUserClaimsAndStats,
  refreshUserSummary,
  // NEW
  refreshUserTradesPage,
} from "../services/cacheRefresh";

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

const DEFAULT_LEAGUES = ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL"];

export const cacheRoutes = Router();

cacheRoutes.get("/meta", async (_req, res) => {
  res.json({ ok: true, now: new Date().toISOString() });
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
// NOTE: This is legacy “summary”, not a trade ledger.
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
// User trade ledger (BUY + SELL) - NEW
// GET /cache/user/:address/trades?page=1&pageSize=25&league=ALL&range=ALL
//
// This is the endpoint your frontend should use for “trade history”
// if you want SELL rows to appear as their own entries.
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

  // Trades change a bit less frequently; same caching policy is fine
  res.setHeader("Cache-Control", "public, max-age=2, stale-while-revalidate=15");
  // payload should be { meta, rows } where rows include `type: BUY|SELL`
  res.json({ ok: true, ...out.payload, cache: out.meta });
});

// ------------------------------------------------------------
// User trade history (paginated bets) - LEGACY
// GET /cache/user/:address/bets?page=1&pageSize=10
//
// This is buy-only (bets entity). KEEP for backward compatibility.
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
