// src/services/cacheRefresh.ts
import { ENV } from "../config/env";
import { subgraphQuery } from "../subgraph/client";
import {
  pickLeaderboardQuery,
  type LeaderboardSort,
  Q_USER_SUMMARY,
  Q_USER_BETS_PAGE,
  Q_USER_CLAIMS_AND_STATS,
} from "../subgraph/queries";

type CacheEntry = {
  payload: any;
  lastOkAt: string | null;
  lastErrAt: string | null;
  lastErr: string | null;
  sourceBlock: string | null;
};

const mem = new Map<string, CacheEntry>();
const inflight = new Map<string, Promise<CacheEntry>>(); // de-dupe concurrent refreshes
const lastRevalidateAt = new Map<string, number>(); // debounce stampede

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
      mem.get(cacheKey) || ({
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
    mem.get(opts.cacheKey) || ({
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
  // Your subgraph expects league strings like "NFL", "NBA", "NHL", etc.
  // If you pass ["ALL"] or empty, do not filter by league_in.
  const arr = (leagues || []).map((s) => String(s).toUpperCase()).filter(Boolean);
  if (!arr.length) return null;
  if (arr.includes("ALL")) return null;
  return arr;
}

function rangeCutoffSec(range: string | undefined) {
  // Range is an API concern; schema doesn't implement it.
  // We filter post-query using lastUpdatedAt (a BigInt stored as string).
  const r = String(range || "ALL").toUpperCase();
  const nowSec = Math.floor(Date.now() / 1000);

  if (r === "D30") return nowSec - 30 * 86400;
  if (r === "D90") return nowSec - 90 * 86400;
  return null; // ALL
}

function toSort(sort: string | undefined): LeaderboardSort {
  const s = String(sort || "ROI").toUpperCase();
  if (s === "TOTAL_STAKED") return "TOTAL_STAKED";
  if (s === "GROSS_VOLUME") return "GROSS_VOLUME";
  if (s === "LAST_UPDATED") return "LAST_UPDATED";
  return "ROI";
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

  // If leagues null => no filter, but Graph requires [String!] variable.
  // We'll pass ALL leagues by omitting league_in entirely (handled by query by passing null).
  // However, GraphQL variables cannot be "omitted"; easiest is: pass null and query uses league_in: $leagues.
  // Since schema expects league_in: [String!], we instead do one of:
  // 1) If no filter: send a very large "all leagues list" (not ideal).
  // 2) Better: have two query variants (with and without where). We’ll do simplest here:
  //    if no filter: set leagues to ["NFL","NBA","NHL","MLB","EPL","UCL"] etc.
  //
  // To avoid guessing, we keep existing behavior but require caller to pass actual list.
  // If your endpoint sends ["ALL"], we turn that into null and then into a safe list fallback.
  const leaguesNorm = normalizeLeagues(params.leagues);

  // If you want "ALL" without maintaining a list, you should implement a no-where query variant.
  // For now, use a sane default set that matches your frontend options.
  const leaguesForQuery =
    leaguesNorm ?? ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL"];

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

  // Apply "range" server-side using lastUpdatedAt
  const cutoff = rangeCutoffSec(params.range);
  const filtered =
    cutoff == null
      ? rows
      : rows.filter((r) => {
          const v = Number(r.lastUpdatedAt || 0);
          return Number.isFinite(v) && v >= cutoff;
        });

  return {
    meta: { sourceBlock },
    rows: filtered,
  };
}

// Optional endpoints (used by profile pages / user detail)
// These now match schema and variables in revised queries.ts

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

  return {
    meta: { sourceBlock: data?._meta?.block?.number ?? null },
    claims: data?.claims ?? [],
    userGameStats: data?.userGameStats ?? [],
  };
}
