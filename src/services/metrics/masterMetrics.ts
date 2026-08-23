// src/services/metrics/masterMetrics.ts
import { pool } from "../../db";
import { subgraphQuery } from "../../subgraph/client";
import {
  type LeaderboardSort,
  Q_USER_BETS_WINDOW_PAGE,
} from "../../subgraph/queries";

/**
 * IMPORTANT:
 * - Leaderboard totals must match Profile page totals.
 *
 * Two ledgers, two purposes:
 *
 * ACTIVITY (volume) — public.user_trade_events windowed on e.timestamp:
 * - Total Traded = SUM(gross_in_dec) over BUYs placed in the window (open or
 *   settled — it's an activity stat). Also feeds betsCount, games touched,
 *   favoriteLeague and the GROSS_VOLUME/TOTAL_STAKED sorts.
 *
 * PERFORMANCE (ROI) — public.user_realized_events windowed on realized_at
 * (see db/migrations/2026-08-16_user_realized_events.sql; shared with the
 * league-chat expert gate, social tags and profile stats):
 * - One row per realized cash event (SELL / CLAIM / LOSS) with the buy cost
 *   basis MATCHED to it (average-cost per user+game+outcome cohort).
 * - ROI = SUM(realized_return) / SUM(realized_cost) - 1, windowed by when the
 *   position CLOSED (sell time, claim time, or game settlement for losses).
 *   A payout can never appear in a window without the stake that produced it —
 *   this is what fixed the 1,098%-style inflated 30d ROIs.
 * - Open positions and unclaimed winnings are excluded; promo free-bet events
 *   are excluded (user staked nothing).
 *
 * The Return columns (returnAmount/claimReturn/sellReturn) come from the
 * realized ledger so they always pair coherently with roiNet.
 */

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL" | "WC";

/* =========================
   Excluded addresses
========================= */

// Wallets that must never appear on the leaderboard or in single-user
// stats lookups. The promo funding wallet holds and trades on behalf of
// users via the bonus-credit flow — its volume already attributes to the
// users via beneficiary_address, so including it here would double-count.
//
// Add new system addresses (treasury, market-makers, etc.) by lower-casing
// them and dropping them in this array. The SQL helpers compare against
// LOWER(user_address) so case doesn't matter, but the literals MUST be
// lowercase.
const EXCLUDED_LEADERBOARD_ADDRESSES: string[] = [
  "0x8b05f283f46f757959e87239922d78e108bbbf2c", // promo funding wallet
];

function isExcludedFromLeaderboard(addr: string | null | undefined): boolean {
  if (!addr) return false;
  return EXCLUDED_LEADERBOARD_ADDRESSES.includes(String(addr).toLowerCase());
}

/* =========================
   Helpers
========================= */

function asLower(a: string) {
  return String(a || "").toLowerCase();
}

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function computeWindow(range: RangeKey, anchorTs: number) {
  if (range === "ALL") return { start: 0, end: anchorTs };
  const days = range === "D30" ? 30 : 90;
  return { start: anchorTs - days * 86400, end: anchorTs };
}

function leagueList(league: LeagueKey): string[] {
  if (league === "ALL") return ["MLB", "NFL", "NBA", "NHL", "EPL", "UCL", "WC"];
  return [league];
}

function cacheKey(parts: Record<string, any>) {
  return Object.entries(parts)
    .map(([k, v]) => `${k}=${String(v)}`)
    .join("|");
}

/* =========================
   In-memory TTL cache
========================= */

type CacheEntry<T> = { exp: number; val: T };
const memCache = new Map<string, CacheEntry<any>>();

function cacheGet<T>(key: string): T | null {
  const hit = memCache.get(key);
  if (!hit) return null;
  if (Date.now() > hit.exp) {
    memCache.delete(key);
    return null;
  }
  return hit.val as T;
}

function cacheSet<T>(key: string, val: T, ttlMs: number) {
  memCache.set(key, { exp: Date.now() + ttlMs, val });
}

/* =========================
   API shapes
========================= */

export type LeaderboardRowApi = {
  id: string;

  // Total Traded (ACTIVITY): BUY gross placed in the window, open or settled
  tradedGross: number;

  // ✅ Explicit Return fields (REALIZED ledger — pairs with roiNet)
  returnAmount: number; // realized SELL + CLAIM returns (canonical "Return")
  claimReturn: number; // realized CLAIM returns only
  sellReturn: number; // realized SELL returns only

  // Legacy fields (keep for UI compat)
  claimsFinal: number; // some UI uses as "return"
  wonFinal?: number; // some UI uses as "return"

  // ROI = realized_return / realized_cost - 1 (matched-cohort, realized only)
  roiNet: number | null;

  tradesNet: number; // games touched
  betsCount: number; // BUY+SELL count

  poolsJoined: number;
  favoriteLeague?: string | null;

  sellsNet?: number; // kept for UI compat
  sellsPnl?: number; // kept for UI compat (we store SELL net_out here)
  sellsRoi?: number | null;

  user?: string;
};

/* =========================
   DB-backed Leaderboard
========================= */

type DbAggRow = {
  user_id: string;
  buy_gross: string | number | null; // ACTIVITY: BUY gross placed in window
  trade_count: string | number | null; // ACTIVITY: BUY+SELL event count
  games_touched: string | number | null;
  last_ts: string | number | null; // last TRADE EVENT ts in window
  // PERFORMANCE (realized ledger):
  realized_cost: string | number | null;
  realized_claim: string | number | null; // realized return from CLAIM events
  realized_sell: string | number | null; // realized return from SELL events
};

async function getCandidateUsersFromDb(params: {
  leagues: string[]; // already expanded (no ALL)
  start: number;
  end: number;
  limit: number;
}) {
  const max = Math.max(1, Math.min(params.limit, 2000));

  // ✅ Candidate selection = users with most recent TRADE EVENT in window
  // (activity-based, so users with only-open positions still surface — their
  // roiNet is simply null until something realizes).
  //
  // Key on effective_user_address (= COALESCE(beneficiary_address, user_address))
  // so promo (free-bet) activity funded by the promo wallet is attributed to the
  // actual beneficiary, not the funding wallet. The funding wallet itself is still
  // excluded as a candidate so it never surfaces as its own leaderboard row.
  const sql = `
    SELECT
      LOWER(e.effective_user_address) AS user_id,
      MAX(e.timestamp)::bigint AS last_ts
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE e.timestamp >= $1
      AND e.timestamp <= $2
      AND g.league = ANY($3::text[])
      AND LOWER(e.effective_user_address) <> ALL($5::text[])
    GROUP BY LOWER(e.effective_user_address)
    ORDER BY last_ts DESC
    LIMIT $4
  `;

  const res = await pool.query(sql, [
    params.start,
    params.end,
    params.leagues,
    max,
    EXCLUDED_LEADERBOARD_ADDRESSES,
  ]);
  return (res.rows || []).map((r: any) => asLower(r.user_id)).filter(Boolean);
}

async function fetchLeaderboardAggFromDb(params: {
  users: string[];
  leagues: string[];
  start: number;
  end: number;
}): Promise<{
  byUser: Map<string, DbAggRow>;
  buyByUserLeague: Map<string, number>; // key = `${user}|${league}`
}> {
  if (!params.users.length) {
    return { byUser: new Map(), buyByUserLeague: new Map() };
  }

  // ✅ ACTIVITY rollup — all trades PLACED in the window (e.timestamp), open or
  // settled. This is what Total Traded / bets / games-touched display.
  const sqlActivity = `
    SELECT
      LOWER(e.effective_user_address) AS user_id,
      COALESCE(SUM(CASE WHEN e.type = 'BUY' THEN COALESCE(e.gross_in_dec::numeric, 0) ELSE 0 END), 0) AS buy_gross,
      COUNT(*)      FILTER (WHERE e.type IN ('BUY','SELL'))::int AS trade_count,
      COUNT(DISTINCT e.game_id)::int                             AS games_touched,
      MAX(e.timestamp)::bigint                                   AS last_ts
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE e.timestamp >= $1
      AND e.timestamp <= $2
      AND g.league = ANY($3::text[])
      AND LOWER(e.effective_user_address) = ANY($4::text[])
    GROUP BY LOWER(e.effective_user_address)
  `;

  // ✅ PERFORMANCE rollup — canonical realized ledger, windowed on realized_at.
  // Every realized_return arrives WITH its matched realized_cost, so a payout
  // can never be orphaned from its stake (the old ROI-inflation bug).
  const sqlRealized = `
    SELECT
      user_address AS user_id,
      COALESCE(SUM(realized_cost), 0)::numeric AS realized_cost,
      COALESCE(SUM(realized_return) FILTER (WHERE kind = 'CLAIM'), 0)::numeric AS realized_claim,
      COALESCE(SUM(realized_return) FILTER (WHERE kind = 'SELL'),  0)::numeric AS realized_sell
    FROM public.user_realized_events
    WHERE realized_at >= $1
      AND realized_at <= $2
      AND league = ANY($3::text[])
      AND user_address = ANY($4::text[])
    GROUP BY user_address
  `;

  const sqlParams = [params.start, params.end, params.leagues, params.users];
  const [resActivity, resRealized] = await Promise.all([
    pool.query(sqlActivity, sqlParams),
    pool.query(sqlRealized, sqlParams),
  ]);

  const byUser = new Map<string, DbAggRow>();
  for (const r of resActivity.rows || []) {
    const u = asLower(r.user_id);
    if (!u) continue;
    byUser.set(u, {
      user_id: u,
      buy_gross: r.buy_gross,
      trade_count: r.trade_count,
      games_touched: r.games_touched,
      last_ts: r.last_ts,
      realized_cost: 0,
      realized_claim: 0,
      realized_sell: 0,
    });
  }
  for (const r of resRealized.rows || []) {
    const u = asLower(r.user_id);
    if (!u) continue;
    const row = byUser.get(u) || {
      user_id: u,
      buy_gross: 0,
      trade_count: 0,
      games_touched: 0,
      last_ts: 0,
      realized_cost: 0,
      realized_claim: 0,
      realized_sell: 0,
    };
    row.realized_cost = r.realized_cost;
    row.realized_claim = r.realized_claim;
    row.realized_sell = r.realized_sell;
    byUser.set(u, row);
  }

  // ✅ ACTIVITY buy volume by (user,league) for favoriteLeague — e.timestamp window
  const sqlLeague = `
    SELECT
      LOWER(e.effective_user_address) AS user_id,
      g.league AS league,
      COALESCE(SUM(CASE WHEN e.type = 'BUY' THEN COALESCE(e.gross_in_dec::numeric, 0) ELSE 0 END), 0) AS buy_gross
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE e.timestamp >= $1
      AND e.timestamp <= $2
      AND g.league = ANY($3::text[])
      AND LOWER(e.effective_user_address) = ANY($4::text[])
    GROUP BY LOWER(e.effective_user_address), g.league
  `;

  const resLeague = await pool.query(sqlLeague, [
    params.start,
    params.end,
    params.leagues,
    params.users,
  ]);

  const buyByUserLeague = new Map<string, number>();
  for (const r of resLeague.rows || []) {
    const u = asLower(r.user_id);
    const lg = String(r.league || "").toUpperCase();
    if (!u || !lg) continue;
    const v = toNum(r.buy_gross);
    buyByUserLeague.set(`${u}|${lg}`, v);
  }

  return { byUser, buyByUserLeague };
}

/* =========================
   Public API
========================= */

export async function getLeaderboardUsers(params: {
  league: LeagueKey;
  range: RangeKey;
  sort: LeaderboardSort;
  limit: number;
  anchorTs?: number;
  userFilter?: string; // ✅ optional single-user filter
}): Promise<{ asOf: string; rows: LeaderboardRowApi[] }> {
  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const { start, end } = computeWindow(params.range, anchorTs);
  const leagues = leagueList(params.league);

  const limit = Math.max(1, Math.min(params.limit || 250, 500));

  // ✅ bump cache version (semantics: activity volume + realized ROI)
  const key = cacheKey({
    v: "lb_users_db_v7_activity_vol_realized_roi",
    league: params.league,
    range: params.range,
    sort: params.sort,
    limit,
    anchorTs,
    userFilter: params.userFilter ?? "none",
  });

  const cached = cacheGet<{ asOf: string; rows: LeaderboardRowApi[] }>(key);
  if (cached) return cached;

  // ✅ 1) Candidate users: if userFilter provided, use it directly; otherwise get top candidates
  let users: string[];

  if (params.userFilter) {
    // Block direct stats lookups for system addresses (promo funding wallet,
    // etc.) so e.g. /api/leaderboard/users?user=0x8B05... returns no rows.
    if (isExcludedFromLeaderboard(params.userFilter)) {
      const out = { asOf: new Date().toISOString(), rows: [] as LeaderboardRowApi[] };
      cacheSet(key, out, 60_000);
      return out;
    }
    users = [params.userFilter.toLowerCase()];
  } else {
    const candidateUsers = await getCandidateUsersFromDb({
      leagues,
      start,
      end,
      limit: Math.min(2000, Math.max(limit * 6, limit)),
    });
    users = candidateUsers.slice(0, Math.min(candidateUsers.length, 2000));
  }

  if (!users.length) {
    const out = { asOf: new Date().toISOString(), rows: [] as LeaderboardRowApi[] };
    cacheSet(key, out, 60_000);
    return out;
  }

  // ✅ 2) Aggregate FROM DB (canonical)
  const { byUser, buyByUserLeague } = await fetchLeaderboardAggFromDb({
    users,
    leagues,
    start,
    end,
  });

  // ✅ 3) Build API rows
  const rows: LeaderboardRowApi[] = users.map((u) => {
    const r =
      byUser.get(u) ||
      ({
        user_id: u,
        buy_gross: 0,
        trade_count: 0,
        games_touched: 0,
        last_ts: 0,
        realized_cost: 0,
        realized_claim: 0,
        realized_sell: 0,
      } as DbAggRow);

    // ACTIVITY: Total Traded = BUYs placed in window (open or settled)
    const totalBuy = toNum(r.buy_gross);

    // PERFORMANCE: realized returns paired with their matched stakes
    const realizedCost = toNum(r.realized_cost);
    const claimTotal = toNum(r.realized_claim);
    const sellNetOut = toNum(r.realized_sell);
    const totalReturn = claimTotal + sellNetOut;
    const roiNet = realizedCost > 0 ? totalReturn / realizedCost - 1 : null;

    // favorite league by BUY gross
    let fav: string | null = null;
    let best = -1;
    for (const lg of leagues) {
      const v = buyByUserLeague.get(`${u}|${lg}`) || 0;
      if (v > best) {
        best = v;
        fav = lg;
      }
    }

    return {
      id: u,

      tradedGross: totalBuy,

      // ✅ explicit canonical return fields
      returnAmount: totalReturn,
      claimReturn: claimTotal,
      sellReturn: sellNetOut,

      // legacy fields (keep for UI compat)
      claimsFinal: totalReturn,
      wonFinal: totalReturn,

      roiNet,

      tradesNet: toNum(r.games_touched),
      poolsJoined: toNum(r.games_touched),
      betsCount: toNum(r.trade_count), // BUY+SELL

      favoriteLeague: fav,

      // keep for UI compat; store SELL cash-back here
      sellsPnl: sellNetOut,
      sellsNet: 0,
      sellsRoi: null,

      user: u,
    };
  });

  // ✅ 4) Sort + limit final rows
  const sort = String(params.sort || "ROI").toUpperCase() as LeaderboardSort;

  rows.sort((a, b) => {
    switch (sort) {
      case "GROSS_VOLUME":
      case "TOTAL_STAKED":
        return (b.tradedGross ?? 0) - (a.tradedGross ?? 0);
      case "LAST_UPDATED":
        // no last_ts exposed in API row, keep stable sort on ROI for now
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
      case "ROI":
      default:
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
    }
  });

  const out = { asOf: new Date().toISOString(), rows: rows.slice(0, limit) };
  cacheSet(key, out, 60_000);
  return out;
}

/* ======================================================================
   KEEP EXISTING RECENT (subgraph-based) so we don’t break your UI right now
   ====================================================================== */

type G_Bet = {
  id: string;
  user: { id: string };
  amountDec: string;
  grossAmount: string;
  fee: string;
  timestamp: string;
  side: "A" | "B";
  game: {
    id: string;
    league: string;
    lockTime: string;
    isFinal: boolean;
    winnerSide?: string | null;
    winnerTeamCode?: string | null;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_UserRecentBetsResp = { _meta?: any; bets: G_Bet[] };

export type RecentTradeRowApi = {
  id: string;
  timestamp: number;
  type: "BUY" | "SELL";
  side: "A" | "B";
  amountDec: number;
  grossAmountDec: number;
  feeDec?: number;
  realizedPnlDec?: number;
  costBasisClosedDec?: number;
  netPositionDec: number;
  game: {
    id: string;
    league: string;
    lockTime: number;
    winnerSide?: "A" | "B" | null;
    isFinal: boolean;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

function safeLeague(v: any): string {
  return String(v || "").toUpperCase();
}

function normalizeWinnerSide(
  winnerSideRaw: any,
  winnerTeamCodeRaw?: any
): "A" | "B" | null {
  const side = String(winnerSideRaw ?? "").trim().toUpperCase();
  const code = String(winnerTeamCodeRaw ?? "").trim().toUpperCase();

  if (
    code === "TIE" ||
    code === "DRAW" ||
    code === "PUSH" ||
    side === "TIE" ||
    side === "DRAW" ||
    side === "PUSH"
  ) {
    return null;
  }

  if (
    !side ||
    side === "0" ||
    side === "NONE" ||
    side === "NULL" ||
    side === "UNSET"
  ) {
    return null;
  }

  if (side === "A" || side === "B") return side;
  return null;
}

function dedupeById<T extends { id?: string | null }>(rows: T[]): T[] {
  const out: T[] = [];
  const seen = new Set<string>();
  for (const r of rows || []) {
    const id = String(r?.id || "");
    if (!id) continue;
    if (seen.has(id)) continue;
    seen.add(id);
    out.push(r);
  }
  return out;
}

async function fetchUserRecentBets(params: {
  user: string;
  leagues: string[];
  start: number;
  end: number;
  limit: number;
}): Promise<G_Bet[]> {
  const first = Math.max(1, Math.min(params.limit, 50));
  const resp = await subgraphQuery<G_UserRecentBetsResp>(Q_USER_BETS_WINDOW_PAGE, {
    user: params.user,
    leagues: params.leagues,
    start: String(params.start),
    end: String(params.end),
    first,
    skip: 0,
  });

  return (resp?.bets || []).slice(0, first);
}

export async function getUserRecent(params: {
  user: string;
  league: LeagueKey;
  limit: number;
  anchorTs?: number;
  range?: RangeKey;
  includeLegacy?: boolean;
}): Promise<{
  asOf: string;
  user: string;
  rows: RecentTradeRowApi[];
  claimByGame: Record<string, number>;
}> {
  // unchanged behavior (legacy)
  const user = asLower(params.user);
  const limit = Math.max(1, Math.min(params.limit || 10, 50));

  // Same exclusion guard as getLeaderboardUsers — system addresses (promo
  // funding wallet, etc.) must not surface recent activity in this endpoint.
  if (isExcludedFromLeaderboard(user)) {
    return {
      asOf: new Date().toISOString(),
      user,
      rows: [],
      claimByGame: {},
    };
  }

  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const range = params.range ?? "ALL";
  const { start, end } = computeWindow(range, anchorTs);
  const leagues = leagueList(params.league);

  const key = cacheKey({
    v: "lb_recent_v1_keep_subgraph",
    user,
    league: params.league,
    range,
    limit,
    anchorTs,
    includeLegacy: params.includeLegacy ? 1 : 0,
  });

  const cached = cacheGet<{
    asOf: string;
    user: string;
    rows: RecentTradeRowApi[];
    claimByGame: Record<string, number>;
  }>(key);
  if (cached) return cached;

  const betsRaw = await fetchUserRecentBets({ user, leagues, start, end, limit });
  const bets = dedupeById(betsRaw || []);

  const rows: RecentTradeRowApi[] = bets
    .map((b): RecentTradeRowApi => {
      const g = b.game || ({} as any);
      const gLeague = safeLeague(g.league);

      const gid = String(g.id || "").toLowerCase();
      const ts = toNum(b.timestamp);

      const sideRaw = String(b.side || "").toUpperCase();
      const side: "A" | "B" = sideRaw === "B" ? "B" : "A";

      const winnerSide = normalizeWinnerSide(
        (g as any).winnerSide,
        (g as any).winnerTeamCode
      );

      return {
        id: b.id,
        timestamp: ts,
        type: "BUY",
        side,
        amountDec: toNum(b.amountDec),
        grossAmountDec: toNum(b.grossAmount),
        feeDec: toNum(b.fee) || 0,
        realizedPnlDec: 0,
        costBasisClosedDec: 0,
        netPositionDec: 0,
        game: {
          id: gid,
          league: gLeague || "—",
          lockTime: toNum(g.lockTime),
          isFinal: !!g.isFinal,
          winnerSide,
          teamACode: (g as any).teamACode ?? null,
          teamBCode: (g as any).teamBCode ?? null,
          teamAName: (g as any).teamAName ?? null,
          teamBName: (g as any).teamBName ?? null,
        },
      };
    })
    .slice(0, limit);

  const out = { asOf: new Date().toISOString(), user, rows, claimByGame: {} };
  cacheSet(key, out, 30_000);
  return out;
}