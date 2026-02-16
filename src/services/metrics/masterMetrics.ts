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
 * - Profile totals are computed from the canonical backend ledger:
 *   public.user_trade_events + public.games
 *
 * Canonical semantics (MATCH tradeAggRoutes.ts):
 * - Total Traded = SUM(gross_in_dec) WHERE type='BUY'
 * - Total Return (cash back) = SUM(net_out_dec) WHERE type IN ('SELL','CLAIM')
 * - ROI = (TotalReturn / TotalTraded) - 1
 *
 * KEY FIX (multi-outcome + upcoming games compatibility):
 * - Range windows MUST be applied to the TRADE EVENT timestamp (e.timestamp),
 *   not the game lock time. Filtering by g.lock_time incorrectly excludes
 *   trades on games that haven't locked yet (common for EPL/UCL three-way).
 */

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

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
  if (league === "ALL") return ["MLB", "NFL", "NBA", "NHL", "EPL", "UCL"];
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

  // Total Traded (Profile-consistent): BUY gross only
  tradedGross: number;

  // ✅ Explicit Return fields (prevents UI binding mistakes)
  returnAmount: number; // SELL net_out + CLAIM net_out (canonical "Return")
  claimReturn: number; // CLAIM net_out only
  sellReturn: number; // SELL net_out only

  // Legacy fields (keep for UI compat)
  claimsFinal: number; // some UI uses as "return"
  wonFinal?: number; // some UI uses as "return"

  // ROI = (return / totalBuy) - 1
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
  buy_gross: string | number | null;
  claim_total: string | number | null;
  sell_net_out: string | number | null; // net_out for SELL (cash back)
  trade_count: string | number | null; // BUY+SELL event count
  games_touched: string | number | null;
  last_ts: string | number | null; // last TRADE EVENT ts (not lock_time)
};

async function getCandidateUsersFromDb(params: {
  leagues: string[]; // already expanded (no ALL)
  start: number;
  end: number;
  limit: number;
}) {
  const max = Math.max(1, Math.min(params.limit, 2000));

  // ✅ Candidate selection = users with most recent TRADE EVENT in window
  // (NOT game lock_time)
  const sql = `
    SELECT
      LOWER(e.user_address) AS user_id,
      MAX(e.timestamp)::bigint AS last_ts
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE e.timestamp >= $1
      AND e.timestamp <= $2
      AND g.league = ANY($3::text[])
    GROUP BY LOWER(e.user_address)
    ORDER BY last_ts DESC
    LIMIT $4
  `;

  const res = await pool.query(sql, [params.start, params.end, params.leagues, max]);
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

  // ✅ Main per-user rollup (canonical) — window by e.timestamp
  const sqlAgg = `
    WITH filtered AS (
      SELECT
        LOWER(e.user_address) AS user_id,
        g.league AS league,
        e.game_id,
        e.type,
        (CASE WHEN e.gross_in_dec IS NULL THEN 0 ELSE e.gross_in_dec::numeric END) AS gross_in,
        (CASE WHEN e.net_out_dec  IS NULL THEN 0 ELSE e.net_out_dec::numeric  END) AS net_out,
        e.timestamp::bigint AS ts
      FROM public.user_trade_events e
      JOIN public.games g ON g.game_id = e.game_id
      WHERE e.timestamp >= $1
        AND e.timestamp <= $2
        AND g.league = ANY($3::text[])
        AND LOWER(e.user_address) = ANY($4::text[])
    )
    SELECT
      user_id,
      SUM(gross_in) FILTER (WHERE type = 'BUY')::numeric       AS buy_gross,
      SUM(net_out)  FILTER (WHERE type = 'CLAIM')::numeric     AS claim_total,
      SUM(net_out)  FILTER (WHERE type = 'SELL')::numeric      AS sell_net_out,
      COUNT(*)      FILTER (WHERE type IN ('BUY','SELL'))::int AS trade_count,
      COUNT(DISTINCT game_id)::int                             AS games_touched,
      MAX(ts)::bigint                                          AS last_ts
    FROM filtered
    GROUP BY user_id
  `;

  const resAgg = await pool.query(sqlAgg, [
    params.start,
    params.end,
    params.leagues,
    params.users,
  ]);

  const byUser = new Map<string, DbAggRow>();
  for (const r of resAgg.rows || []) {
    const u = asLower(r.user_id);
    if (!u) continue;
    byUser.set(u, r as DbAggRow);
  }

  // ✅ Buy volume by (user,league) for favoriteLeague — window by e.timestamp
  const sqlLeague = `
    WITH filtered AS (
      SELECT
        LOWER(e.user_address) AS user_id,
        g.league AS league,
        e.type,
        (CASE WHEN e.gross_in_dec IS NULL THEN 0 ELSE e.gross_in_dec::numeric END) AS gross_in
      FROM public.user_trade_events e
      JOIN public.games g ON g.game_id = e.game_id
      WHERE e.timestamp >= $1
        AND e.timestamp <= $2
        AND g.league = ANY($3::text[])
        AND LOWER(e.user_address) = ANY($4::text[])
    )
    SELECT
      user_id,
      league,
      SUM(gross_in) FILTER (WHERE type='BUY')::numeric AS buy_gross
    FROM filtered
    GROUP BY user_id, league
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

  // ✅ bump cache version (window semantics changed: lock_time -> timestamp)
  const key = cacheKey({
    v: "lb_users_db_v4_event_timestamp_window_explicit_returns",
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
        claim_total: 0,
        sell_net_out: 0,
        trade_count: 0,
        games_touched: 0,
        last_ts: 0,
      } as DbAggRow);

    const totalBuy = toNum(r.buy_gross);
    const claimTotal = toNum(r.claim_total);
    const sellNetOut = toNum(r.sell_net_out);

    // Canonical: "Total Return" = CLAIM net_out + SELL net_out
    const totalReturn = claimTotal + sellNetOut;
    const roiNet = totalBuy > 0 ? totalReturn / totalBuy - 1 : null;

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
