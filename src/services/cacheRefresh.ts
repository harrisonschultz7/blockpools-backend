// src/services/cacheRefresh.ts
import { ENV } from "../config/env";
import { subgraphQuery } from "../subgraph/client";
import {
  pickLeaderboardQuery,
  type LeaderboardSort,
  Q_USER_SUMMARY,
  Q_USER_BETS_PAGE,
  Q_USER_CLAIMS_AND_STATS,
  Q_USER_ACTIVITY_PAGE,
} from "../subgraph/queries";

import { upsertUserTradesAndGames } from "./persistTrades";

type CacheEntry = {
  payload: any;
  lastOkAt: string | null;
  lastErrAt: string | null;
  lastErr: string | null;
  sourceBlock: string | null;
};

const mem = new Map<string, CacheEntry>();
const inflight = new Map<string, Promise<CacheEntry>>();
const lastRevalidateAt = new Map<string, number>();

function nowIso() {
  return new Date().toISOString();
}

function ageSeconds(lastOkAt: string | null) {
  if (!lastOkAt) return Number.POSITIVE_INFINITY;
  const ms = Date.now() - new Date(lastOkAt).getTime();
  return Math.max(0, Math.floor(ms / 1000));
}

async function runRefresh(
  cacheKey: string,
  refreshFn: (params: any) => Promise<any>,
  params: any
) {
  const existing = inflight.get(cacheKey);
  if (existing) return existing;

  const p = (async () => {
    const prev =
      mem.get(cacheKey) ||
      ({
        payload: null,
        lastOkAt: null,
        lastErrAt: null,
        lastErr: null,
        sourceBlock: null,
      } satisfies CacheEntry);

    try {
      const out = await refreshFn(params);

      const next: CacheEntry = {
        payload: out,
        lastOkAt: nowIso(),
        lastErrAt: null,
        lastErr: null,
        sourceBlock:
          out?.meta?.sourceBlock?.toString?.() ??
          out?.meta?.sourceBlock ??
          prev.sourceBlock,
      };

      mem.set(cacheKey, next);
      return next;
    } catch (e: any) {
      const msg = String(e?.message || e);
      const next: CacheEntry = {
        ...prev,
        lastErrAt: nowIso(),
        lastErr: msg,
      };
      mem.set(cacheKey, next);
      return next;
    } finally {
      inflight.delete(cacheKey);
    }
  })();

  inflight.set(cacheKey, p);
  return p;
}

export async function serveWithStaleWhileRevalidate(opts: {
  cacheKey: string;
  params: any;
  view: string;
  scope: "global" | "user";
  refreshFn: (params: any) => Promise<any>;
}) {
  const entry =
    mem.get(opts.cacheKey) ||
    ({
      payload: null,
      lastOkAt: null,
      lastErrAt: null,
      lastErr: null,
      sourceBlock: null,
    } satisfies CacheEntry);

  const age = ageSeconds(entry.lastOkAt);
  const isFresh = age <= ENV.CACHE_TTL_SECONDS;
  const isStaleOk = age <= ENV.CACHE_STALE_SECONDS;

  const now = Date.now();
  const lastRv = lastRevalidateAt.get(opts.cacheKey) || 0;
  const canRevalidate = now - lastRv >= ENV.CACHE_REVALIDATE_SECONDS * 1000;

  if (entry.payload && isFresh) {
    return {
      payload: entry.payload,
      meta: {
        stale: false,
        lastOkAt: entry.lastOkAt,
        ageSeconds: age,
        sourceBlock: entry.sourceBlock,
        lastErrAt: entry.lastErrAt,
        lastErr: entry.lastErr,
      },
    };
  }

  if (entry.payload && isStaleOk) {
    if (canRevalidate) {
      lastRevalidateAt.set(opts.cacheKey, now);
      void runRefresh(opts.cacheKey, opts.refreshFn, opts.params);
    }

    return {
      payload: entry.payload,
      meta: {
        stale: true,
        lastOkAt: entry.lastOkAt,
        ageSeconds: age,
        sourceBlock: entry.sourceBlock,
        lastErrAt: entry.lastErrAt,
        lastErr: entry.lastErr,
      },
    };
  }

  const refreshed = await runRefresh(opts.cacheKey, opts.refreshFn, opts.params);

  if (refreshed.payload) {
    const newAge = ageSeconds(refreshed.lastOkAt);
    return {
      payload: refreshed.payload,
      meta: {
        stale: false,
        lastOkAt: refreshed.lastOkAt,
        ageSeconds: newAge,
        sourceBlock: refreshed.sourceBlock,
        lastErrAt: refreshed.lastErrAt,
        lastErr: refreshed.lastErr,
      },
    };
  }

  return {
    payload: {
      ok: false,
      error: "cache_miss_and_refresh_failed",
      details: refreshed.lastErr || "unknown",
    },
    meta: {
      stale: false,
      lastOkAt: refreshed.lastOkAt,
      ageSeconds: ageSeconds(refreshed.lastOkAt),
      sourceBlock: refreshed.sourceBlock,
      lastErrAt: refreshed.lastErrAt,
      lastErr: refreshed.lastErr,
    },
  };
}

// -------------------- helpers --------------------

function normalizeLeagues(leagues: string[] | undefined) {
  const arr = (leagues || []).map((s) => String(s).toUpperCase()).filter(Boolean);
  if (!arr.length) return null;
  if (arr.includes("ALL")) return null;
  return arr;
}

function toSort(sort: string | undefined): LeaderboardSort {
  const s = String(sort || "ROI").toUpperCase();
  if (s === "TOTAL_STAKED") return "TOTAL_STAKED";
  if (s === "GROSS_VOLUME") return "GROSS_VOLUME";
  if (s === "LAST_UPDATED") return "LAST_UPDATED";
  return "ROI";
}

function tradesWindowFromRange(range: string | undefined) {
  const r = String(range || "ALL").toUpperCase();
  const nowSec = Math.floor(Date.now() / 1000);
  const farFuture = 4102444800;

  if (r === "D30") return { start: nowSec - 30 * 86400, end: nowSec };
  if (r === "D90") return { start: nowSec - 90 * 86400, end: nowSec };
  return { start: 0, end: farFuture };
}

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function canonicalActivityId(id: string): string {
  return String(id || "")
    .replace(/^trade-trade-/, "")
    .replace(/^bet-bet-/, "")
    .replace(/^claim-claim-/, "")
    .replace(/^trade-/, "")
    .replace(/^bet-/, "")
    .replace(/^claim-/, "");
}

function dedupeActivityRows(rows: any[]): { rows: any[]; dropped: number } {
  const bestByKey = new Map<string, any>();

  for (const r of rows || []) {
    const key = canonicalActivityId(r?.id);
    if (!key) continue;

    const prev = bestByKey.get(key);
    if (!prev) {
      bestByKey.set(key, r);
      continue;
    }

    const rIsTrade = r?.__source === "trade";
    const pIsTrade = prev?.__source === "trade";

    const rIsClaim = r?.__source === "claim";
    const pIsClaim = prev?.__source === "claim";

    const rHasTx = !!r?.txHash;
    const pHasTx = !!prev?.txHash;

    // Prefer trades over bets; claims are distinct event types so they usually won't collide,
    // but if they do, keep txHash and prefer trade > claim > bet.
    const rank = (x: any) => (x?.__source === "trade" ? 3 : x?.__source === "claim" ? 2 : 1);
    const takeR = rank(r) > rank(prev) || (rHasTx && !pHasTx) || (rIsTrade && !pIsTrade) || (rIsClaim && !pIsClaim);

    if (takeR) bestByKey.set(key, r);
  }

  const out: any[] = [];
  const seen = new Set<string>();

  for (const r of rows || []) {
    const key = canonicalActivityId(r?.id);
    if (!key || seen.has(key)) continue;
    const best = bestByKey.get(key);
    if (best) out.push(best);
    seen.add(key);
  }

  return { rows: out, dropped: Math.max(0, (rows?.length || 0) - out.length) };
}

// -------------------- Refresh functions --------------------

export async function refreshLeaderboard(params: {
  leagues: string[];
  range: string;
  sort: string;
  page: number;
  pageSize: number;
}) {
  const skip = Math.max(0, (params.page - 1) * params.pageSize);
  const first = params.pageSize;

  const leaguesNorm = normalizeLeagues(params.leagues);
  const leaguesForQuery = leaguesNorm ?? ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL"];

  type Row = {
    user: { id: string };
    league: string;
    roiDec: string;
    betsCount: string;
    lastUpdatedAt: string;
    grossVolumeDec: string;
    totalClaimsDec: string;
    totalPayoutDec: string;
    totalStakedDec: string;
    activePoolsCount: string;
    totalWithdrawnDec: string;
  };

  type Data = {
    _meta?: { block?: { number?: number } };
    userLeagueStats?: Row[];
  };

  const sort = toSort(params.sort);
  const gql = pickLeaderboardQuery(sort);

  const data = await subgraphQuery<Data>(gql, {
    leagues: leaguesForQuery,
    skip,
    first,
  });

  const sourceBlock = data._meta?.block?.number ?? null;
  const rows = data.userLeagueStats || [];

  const r = String(params.range || "ALL").toUpperCase();
  const cutoff =
    r === "D30"
      ? Math.floor(Date.now() / 1000) - 30 * 86400
      : r === "D90"
      ? Math.floor(Date.now() / 1000) - 90 * 86400
      : null;

  const filtered =
    cutoff == null
      ? rows
      : rows.filter((row) => {
          const v = Number(row.lastUpdatedAt || 0);
          return Number.isFinite(v) && v >= cutoff;
        });

  return {
    meta: { sourceBlock },
    rows: filtered,
  };
}

export async function refreshUserSummary(params: {
  user: string;
  betsFirst: number;
  claimsFirst: number;
  statsFirst: number;
}) {
  const data = await subgraphQuery<any>(Q_USER_SUMMARY, {
    user: params.user.toLowerCase(),
    betsFirst: params.betsFirst,
    claimsFirst: params.claimsFirst,
    statsFirst: params.statsFirst,
  });

  return {
    meta: { sourceBlock: data?._meta?.block?.number ?? null },
    bets: data?.bets ?? [],
    claims: data?.claims ?? [],
    userGameStats: data?.userGameStats ?? [],
  };
}

export async function refreshUserBetsPage(params: {
  user: string;
  page: number;
  pageSize: number;
}) {
  const skip = Math.max(0, (params.page - 1) * params.pageSize);
  const first = params.pageSize;

  const data = await subgraphQuery<any>(Q_USER_BETS_PAGE, {
    user: params.user.toLowerCase(),
    skip,
    first,
  });

  return {
    meta: { sourceBlock: data?._meta?.block?.number ?? null },
    rows: data?.bets ?? [],
  };
}

/**
 * ✅ User trade ledger (BUY + SELL persisted):
 * - SELL rows come from `trades`
 * - BUY rows are backfilled from `bets` as BUY-like rows
 *
 * Claims are persisted in refreshUserClaimsAndStats() below.
 */
export async function refreshUserTradesPage(params: {
  user: string;
  leagues: string[];
  range: string;
  page: number;
  pageSize: number;
}) {
  const page = Math.max(1, Number(params.page || 1));
  const pageSize = Math.max(1, Math.min(Number(params.pageSize || 25), 50));

  const leaguesNorm = normalizeLeagues(params.leagues);
  const leaguesForQuery = leaguesNorm ?? ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL"];

  const { start, end } = tradesWindowFromRange(params.range);

  const overfetch = Math.max(page * pageSize * 2, 120);
  const first = Math.min(overfetch, 1000);

  const data = await subgraphQuery<any>(Q_USER_ACTIVITY_PAGE, {
    user: params.user.toLowerCase(),
    leagues: leaguesForQuery,
    start: String(start),
    end: String(end),
    first,
    skipTrades: 0,
    skipBets: 0,
  });

  const sourceBlock = data?._meta?.block?.number ?? null;

  const trades = Array.isArray(data?.trades) ? data.trades : [];
  const bets = Array.isArray(data?.bets) ? data.bets : [];

  const betAsTrades = bets.map((b: any) => {
    const g = b?.game ?? {};
    const ts = toNum(b?.timestamp);
    const priceBps = b?.priceBps ?? b?.spotPriceBps ?? b?.avgPriceBps ?? null;

    const sharesOutDec = b?.sharesOutDec ?? b?.sharesOut ?? null;

    return {
      id: `bet-${b.id}`,
      type: "BUY",
      side: b?.side ?? "A",
      timestamp: ts,
      txHash: b?.txHash ?? null,

      spotPriceBps: priceBps,
      avgPriceBps: priceBps,

      priceBps,
      sharesOutDec,
      sharesOut: b?.sharesOut ?? null,

      grossInDec: b?.grossAmount ?? "0",
      grossOutDec: "0",
      feeDec: b?.fee ?? "0",
      netStakeDec: b?.amountDec ?? "0",
      netOutDec: "0",

      costBasisClosedDec: "0",
      realizedPnlDec: "0",

      game: g,
      __source: "bet",
    };
  });

  const tradeRows = trades.map((t: any) => ({
    ...t,
    id: `trade-${t.id}`,
    timestamp: toNum(t?.timestamp),
    __source: "trade",
  }));

  const mergedSorted = [...tradeRows, ...betAsTrades].sort((a, b) => {
    const dt = toNum(b?.timestamp) - toNum(a?.timestamp);
    if (dt !== 0) return dt;
    return String(b?.id || "").localeCompare(String(a?.id || ""));
  });

  const deduped = dedupeActivityRows(mergedSorted);

  // ✅ persist BUY/SELL
  try {
    await upsertUserTradesAndGames({
      user: params.user,
      tradeRows: deduped.rows,
    });
  } catch (e: any) {
    console.log(`[persistTrades BUY/SELL] err: ${String(e?.message || e)}`);
  }

  const startIdx = (page - 1) * pageSize;
  const rows = deduped.rows.slice(startIdx, startIdx + pageSize);

  return {
    meta: {
      sourceBlock,
      droppedDupes: deduped.dropped,
    },
    rows,
  };
}

/**
 * ✅ Claims persistence:
 * - Fetch claims from subgraph
 * - Persist each as type='CLAIM' into public.user_trade_events
 *   (net_out_dec and gross_out_dec set to claim amount)
 *
 * This is the missing piece causing Supabase net_out_dec to stay 0 and ROI to be blank.
 */
export async function refreshUserClaimsAndStats(params: {
  user: string;
  claimsFirst?: number;
  statsFirst?: number;
}) {
  const data = await subgraphQuery<any>(Q_USER_CLAIMS_AND_STATS, {
    user: params.user.toLowerCase(),
    claimsFirst: params.claimsFirst ?? 250,
    statsFirst: params.statsFirst ?? 250,
  });

  const sourceBlock = data?._meta?.block?.number ?? null;
  const claims = Array.isArray(data?.claims) ? data.claims : [];

  // Map claims -> tradeRows for persistence (CLAIM events)
  const claimRows = claims.map((c: any) => {
    const g = c?.game ?? {};
    const winnerSide = String(g?.winnerSide || "").toUpperCase().trim();
    const side = winnerSide === "A" || winnerSide === "B" ? winnerSide : null;

    const amt = c?.amountDec ?? "0";
    const ts = toNum(c?.timestamp);

    return {
      id: `claim-${c.id}`, // unique namespace
      type: "CLAIM",
      side,
      timestamp: ts,
      txHash: c?.txHash ?? null,

      spotPriceBps: null,
      avgPriceBps: null,

      grossInDec: "0",
      grossOutDec: amt,
      feeDec: "0",
      netStakeDec: "0",
      netOutDec: amt,

      costBasisClosedDec: "0",
      realizedPnlDec: "0",

      game: g,
      __source: "claim",
    };
  });

  // ✅ persist CLAIM rows (and game metadata)
  if (claimRows.length) {
    try {
      await upsertUserTradesAndGames({
        user: params.user,
        tradeRows: claimRows,
      });
    } catch (e: any) {
      console.log(`[persistTrades CLAIM] err: ${String(e?.message || e)}`);
    }
  }

  return {
    meta: { sourceBlock },
    claims,
    userGameStats: data?.userGameStats ?? [],
  };
}
