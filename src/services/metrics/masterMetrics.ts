// src/services/metrics/masterMetrics.ts
import { subgraphQuery } from "../../subgraph/client";
import {
  pickLeaderboardQuery,
  type LeaderboardSort,
  Q_ACTIVE_USERS_FROM_TRADES_WINDOW,
  Q_ACTIVE_USERS_FROM_BETS_WINDOW,

  // ✅ must exist in src/subgraph/queries.ts
  Q_USER_BETS_WINDOW_PAGE,
} from "../../subgraph/queries";

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

const GQL_MAX_FIRST = 1000;

// ---------------------------
// Helpers
// ---------------------------
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

function safeLeague(v: any): string {
  return String(v || "").toUpperCase();
}

function cacheKey(parts: Record<string, any>) {
  return Object.entries(parts)
    .map(([k, v]) => `${k}=${String(v)}`)
    .join("|");
}

// ---------------------------
// In-memory TTL cache
// ---------------------------
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

// ---------------------------
// Queries (local to this file)
// ---------------------------

// Windowed bulk pull (paged) for a user set, keyed by GAME.lockTime window
const Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE = /* GraphQL */ `
  query UsersTradesClaimsStatsBulkPage(
    $users: [String!]!
    $leagues: [String!]!
    $start: BigInt!
    $end: BigInt!
    $first: Int!
    $skipTrades: Int!
    $skipClaims: Int!
    $skipBets: Int!
  ) {
    userGameStats(
      first: 1000
      where: {
        user_in: $users
        league_in: $leagues
        game_: { lockTime_gte: $start, lockTime_lte: $end }
      }
    ) {
      user { id }
      stakedDec
      withdrawnDec
      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        winnerTeamCode
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }

    claims(
      first: $first
      skip: $skipClaims
      where: {
        user_in: $users
        game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
      }
      orderBy: timestamp
      orderDirection: desc
    ) {
      id
      user { id }
      amountDec
      timestamp
      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }

    trades(
      first: $first
      skip: $skipTrades
      where: {
        user_in: $users
        game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
      }
      orderBy: timestamp
      orderDirection: desc
    ) {
      id
      user { id }
      league
      type
      side
      timestamp
      txHash
      spotPriceBps
      avgPriceBps
      grossInDec
      grossOutDec
      feeDec
      netStakeDec
      netOutDec
      costBasisClosedDec
      realizedPnlDec
      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }

    # ✅ IMPORTANT: bets cover Legacy + AMM buy events, so BUY totals are consistent and not double-counted.
    bets(
      first: $first
      skip: $skipBets
      where: {
        user_in: $users
        game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
      }
      orderBy: timestamp
      orderDirection: desc
    ) {
      id
      user { id }
      amountDec
      grossAmount
      fee
      timestamp
      side
      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }
  }
`;

// Recent trades page for a single user
// IMPORTANT: window stats/claims by GAME.lockTime so D30/D90 behave intuitively.
const Q_USER_RECENT_TRADES_PAGE = /* GraphQL */ `
  query UserRecentTradesPage(
    $user: String!
    $leagues: [String!]!
    $start: BigInt!
    $end: BigInt!
    $first: Int!
    $skipTrades: Int!
  ) {
    userGameStats(
      first: 1000
      where: {
        user: $user
        league_in: $leagues
        game_: { lockTime_gte: $start, lockTime_lte: $end }
      }
    ) {
      user { id }
      stakedDec
      withdrawnDec
      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        winnerTeamCode
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }

    claims(
      first: 1000
      where: {
        user: $user
        game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
      }
    ) {
      id
      user { id }
      amountDec
      timestamp
      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }

    trades(
      first: $first
      skip: $skipTrades
      where: {
        user: $user
        timestamp_gte: $start
        timestamp_lte: $end
        game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
      }
      orderBy: timestamp
      orderDirection: desc
    ) {
      id
      user { id }
      league
      type
      side
      timestamp
      txHash

      spotPriceBps
      avgPriceBps

      grossInDec
      grossOutDec
      feeDec
      netStakeDec
      netOutDec

      costBasisClosedDec
      realizedPnlDec

      game {
        id
        league
        lockTime
        isFinal
        winnerSide
        teamACode
        teamBCode
        teamAName
        teamBName
      }
    }
  }
`;

// ---------------------------
// Subgraph types (minimal)
// ---------------------------
type G_UserLeagueStats = {
  user: { id: string };
  league: string;
};

type G_Trade = {
  id: string;
  user: { id: string };
  league: string;
  type: "BUY" | "SELL";
  side: "A" | "B";
  timestamp: string;
  txHash: string;

  spotPriceBps?: string | null;
  avgPriceBps?: string | null;

  grossInDec: string;
  grossOutDec: string;
  feeDec: string;
  netStakeDec: string;
  netOutDec: string;

  costBasisClosedDec: string;
  realizedPnlDec: string;

  game: {
    id: string;
    league: string;
    lockTime: string;
    isFinal: boolean;
    winnerSide?: string | null;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_Claim = {
  id?: string;
  user: { id: string };
  amountDec: string;
  timestamp: string;
  game: {
    id: string;
    league: string;
    lockTime: string;
    isFinal: boolean;
    winnerSide?: string | null;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_UserGameStat = {
  user: { id: string };
  stakedDec: string;
  withdrawnDec: string;
  game: {
    id: string;
    league: string;
    lockTime: string;
    isFinal: boolean;
    winnerSide?: string | null;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_LeaderboardResp = { _meta?: any; userLeagueStats: G_UserLeagueStats[] };

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
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_BulkPageResp = {
  _meta?: any;
  userGameStats: G_UserGameStat[];
  claims: G_Claim[];
  trades: G_Trade[];
  bets: G_Bet[];
};

// used by Q_ACTIVE_USERS_FROM_TRADES_WINDOW
type G_ActiveUsersFromTradesResp = {
  trades: Array<{ user: { id: string } }>;
};

type G_UserRecentBetsResp = {
  _meta?: any;
  bets: G_Bet[];
};

// ---------------------------
// API shapes
// ---------------------------
//
// NOTE: We keep legacy field names for compatibility with existing UI.
// Semantics (per your agreed definitions):
// - tradedGross  => Total Traded (BUYS only)
// - claimsFinal  => P/L (claims + sell proceeds)
// - roiNet       => (P/L / TotalBuy) - 1
// - betsCount    => number of trades (buys + sells)
//
type LeaderboardRowApi = {
  id: string;

  // ✅ Total Traded (BUYS only)
  tradedGross: number;

  // ✅ P/L (claims + net sell proceeds)
  claimsFinal: number;

  // ✅ ROI = (P/L / TotalBuy) - 1
  roiNet: number | null;

  // kept for UI compatibility (some UIs label this "Trades Net" or "Games")
  tradesNet: number;

  // ✅ number of trades (buys + sells)
  betsCount: number;

  poolsJoined: number;
  favoriteLeague?: string | null;

  // optional sell analytics (still useful)
  sellsNet?: number;
  sellsPnl?: number;
  sellsRoi?: number | null;

  // back-compat
  user?: string;
  wonFinal?: number;
};

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

// ---------------------------
// Candidate shortlist (range-correct)
// ---------------------------
async function collectActiveUsersFromTradesWindow(params: {
  leagues: string[];
  start: number;
  end: number;
  targetUsers: number;
  maxPages?: number;
}): Promise<string[]> {
  const target = Math.max(1, Math.min(params.targetUsers, 2000));
  const first = GQL_MAX_FIRST;
  const maxPages = Math.max(1, Math.min(params.maxPages ?? 10, 25));

  const out: string[] = [];
  const seen = new Set<string>();

  let skip = 0;
  for (let pageNo = 0; pageNo < maxPages; pageNo++) {
    const resp = await subgraphQuery<G_ActiveUsersFromTradesResp>(
      Q_ACTIVE_USERS_FROM_TRADES_WINDOW,
      {
        leagues: params.leagues,
        start: String(params.start),
        end: String(params.end),
        first,
        skip,
      }
    );

    const rows = resp.trades || [];
    for (const r of rows) {
      const u = asLower(r?.user?.id);
      if (!u) continue;
      if (seen.has(u)) continue;
      seen.add(u);
      out.push(u);
      if (out.length >= target) return out;
    }

    if (rows.length < first) break;
    skip += first;
  }

  return out;
}

type G_ActiveUsersFromBetsResp = {
  bets: Array<{ user: { id: string } }>;
};

async function collectActiveUsersFromBetsWindow(params: {
  leagues: string[];
  start: number;
  end: number;
  targetUsers: number;
  maxPages?: number;
}): Promise<string[]> {
  const target = Math.max(1, Math.min(params.targetUsers, 2000));
  const first = GQL_MAX_FIRST;
  const maxPages = Math.max(1, Math.min(params.maxPages ?? 10, 25));

  const out: string[] = [];
  const seen = new Set<string>();

  let skip = 0;
  for (let pageNo = 0; pageNo < maxPages; pageNo++) {
    const resp = await subgraphQuery<G_ActiveUsersFromBetsResp>(
      Q_ACTIVE_USERS_FROM_BETS_WINDOW,
      {
        leagues: params.leagues,
        start: String(params.start),
        end: String(params.end),
        first,
        skip,
      }
    );

    const rows = resp.bets || [];
    for (const r of rows) {
      const u = asLower(r?.user?.id);
      if (!u) continue;
      if (seen.has(u)) continue;
      seen.add(u);
      out.push(u);
      if (out.length >= target) return out;
    }

    if (rows.length < first) break;
    skip += first;
  }

  return out;
}

// ---------------------------
// Internal bulk fetchers (paged)
// ---------------------------
async function fetchBulkWindowed(params: {
  users: string[];
  leagues: string[];
  start: number;
  end: number;
  maxTrades?: number;
  maxClaims?: number;
}): Promise<{
  userGameStats: G_UserGameStat[];
  trades: G_Trade[];
  claims: G_Claim[];
  bets: G_Bet[];
}> {
  const first = GQL_MAX_FIRST;

  const maxTrades = Math.max(0, Math.min(params.maxTrades ?? 3000, 10000));
  const maxClaims = Math.max(0, Math.min(params.maxClaims ?? 3000, 10000));
  const maxBets = maxTrades; // keep aligned

  // First page includes stats; we only need stats once.
  const base = await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
    users: params.users,
    leagues: params.leagues,
    start: String(params.start),
    end: String(params.end),
    first,
    skipTrades: 0,
    skipClaims: 0,
    skipBets: 0,
  });

  const outStats = base.userGameStats || [];
  const outTrades: G_Trade[] = [];
  const outClaims: G_Claim[] = [];
  const outBets: G_Bet[] = [];

  // bets pagination (Legacy + AMM buys)
  {
    let skip = 0;
    while (outBets.length < maxBets) {
      const page =
        skip === 0
          ? base.bets || []
          : (
              await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
                users: params.users,
                leagues: params.leagues,
                start: String(params.start),
                end: String(params.end),
                first,
                skipTrades: 0,
                skipClaims: 0,
                skipBets: skip,
              })
            ).bets || [];

      outBets.push(...page);

      if (page.length < first) break;
      skip += first;
      if (skip > 20000) break;
    }
  }

  // trades pagination
  {
    let skip = 0;
    while (outTrades.length < maxTrades) {
      const page =
        skip === 0
          ? base.trades || []
          : (
              await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
                users: params.users,
                leagues: params.leagues,
                start: String(params.start),
                end: String(params.end),
                first,
                skipTrades: skip,
                skipClaims: 0,
                skipBets: 0,
              })
            ).trades || [];

      outTrades.push(...page);

      if (page.length < first) break;
      skip += first;
      if (skip > 20000) break;
    }
  }

  // claims pagination
  {
    let skip = 0;
    while (outClaims.length < maxClaims) {
      const page =
        skip === 0
          ? base.claims || []
          : (
              await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
                users: params.users,
                leagues: params.leagues,
                start: String(params.start),
                end: String(params.end),
                first,
                skipTrades: 0,
                skipClaims: skip,
                skipBets: 0,
              })
            ).claims || [];

      outClaims.push(...page);

      if (page.length < first) break;
      skip += first;
      if (skip > 20000) break;
    }
  }

  return {
    userGameStats: outStats,
    trades: outTrades.slice(0, maxTrades),
    claims: outClaims.slice(0, maxClaims),
    bets: outBets.slice(0, maxBets),
  };
}

async function fetchUserRecentTrades(params: {
  user: string;
  leagues: string[];
  start: number;
  end: number;
  limit: number;
}): Promise<G_BulkPageResp> {
  const first = GQL_MAX_FIRST;

  // pull more than limit so filtering/sorting stays stable
  const need = Math.max(params.limit * 6, 200);
  const maxPull = Math.min(Math.max(need, 200), 3000);

  let allTrades: G_Trade[] = [];
  let stats: G_UserGameStat[] = [];
  let claims: G_Claim[] = [];

  let skip = 0;
  while (allTrades.length < maxPull) {
    const page = await subgraphQuery<G_BulkPageResp>(Q_USER_RECENT_TRADES_PAGE, {
      user: params.user,
      leagues: params.leagues,
      start: String(params.start),
      end: String(params.end),
      first,
      skipTrades: skip,
    });

    if (skip === 0) {
      stats = page.userGameStats || [];
      claims = page.claims || [];
    }

    const chunk = page.trades || [];
    allTrades.push(...chunk);

    if (chunk.length < first) break;
    skip += first;
    if (skip > 10000) break;
  }

  return { userGameStats: stats, claims, trades: allTrades.slice(0, maxPull) } as any;
}

// ✅ legacy recent bets (range+league aware via game.lockTime window)
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

// ---------------------------
// Public API
// ---------------------------
export async function getLeaderboardUsers(params: {
  league: LeagueKey;
  range: RangeKey;
  sort: LeaderboardSort;
  limit: number;
  anchorTs?: number;
}): Promise<{ asOf: string; rows: LeaderboardRowApi[] }> {
  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const { start, end } = computeWindow(params.range, anchorTs);
  const leagues = leagueList(params.league);

  const limit = Math.max(1, Math.min(params.limit || 250, 500));

  const key = cacheKey({
    v: "lb_users_v6_window_buy_pnl_roi",
    league: params.league,
    range: params.range,
    sort: params.sort,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; rows: LeaderboardRowApi[] }>(key);
  if (cached) return cached;

  // ---------------------------
  // Step 1: candidate users
  // ---------------------------
  //
  // For D30/D90: shortlist from BETS + TRADES in the GAME.lockTime window (range-correct).
  // For ALL: use userLeagueStats (fast) but clamp first <= 500.
  //
  let users: string[] = [];

  if (params.range !== "ALL") {
    const [fromBets, fromTrades] = await Promise.all([
      collectActiveUsersFromBetsWindow({ leagues, start, end, targetUsers: limit, maxPages: 15 }),
      collectActiveUsersFromTradesWindow({
        leagues,
        start,
        end,
        targetUsers: limit,
        maxPages: 15,
      }),
    ]);

    users = Array.from(new Set([...fromBets, ...fromTrades])).slice(0, limit);

    // fallback if window has no activity
    if (!users.length) {
      const q = pickLeaderboardQuery(params.sort);
      const lb = await subgraphQuery<G_LeaderboardResp>(q, {
        leagues,
        skip: 0,
        first: Math.min(limit, 500),
      });

      users = Array.from(
        new Set(lb.userLeagueStats.map((x) => asLower(x.user.id)).filter(Boolean))
      );
    }
  } else {
    const q = pickLeaderboardQuery(params.sort);
    const lb = await subgraphQuery<G_LeaderboardResp>(q, {
      leagues,
      skip: 0,
      first: Math.min(limit, 500),
    });

    users = Array.from(new Set(lb.userLeagueStats.map((x) => asLower(x.user.id)).filter(Boolean)));
  }

  if (!users.length) {
    const out = { asOf: new Date().toISOString(), rows: [] as LeaderboardRowApi[] };
    cacheSet(key, out, 60_000);
    return out;
  }

  // ---------------------------
  // Step 2: bulk fetch within lockTime window
  // ---------------------------
  const bulk = await fetchBulkWindowed({
    users,
    leagues,
    start,
    end,
    maxTrades: 5000,
    maxClaims: 5000,
  });

  const inLockWindow = (lockTime: number) => lockTime >= start && lockTime <= end;

  // ---------------------------
  // Step 3: aggregate per user-game (no double counting)
  // ---------------------------
  //
  // Rules:
  // - BUY totals come from bets (Legacy + AMM buys)
  // - SELL totals come from trades filtered to SELL only
  // - CLAIM totals come from claims
  //
  type UserGameAgg = {
    league: string;
    lockTime: number;
    isFinal: boolean;

    buyGross: number; // Σ buy gross (capital in)
    buyCount: number; // # buys

    sellNet: number; // Σ sell net proceeds (cash out)
    sellGross: number; // Σ sell gross out (optional analytics)
    sellCostClosed: number; // Σ closed cost basis (for sellsRoi)
    sellPnl: number; // Σ realized pnl (sell-only analytic)
    sellCount: number; // # sells

    claimTotal: number; // Σ claim amount
  };

  const byUserGame = new Map<string, UserGameAgg>();

  // Seed from stats (ensures league/isFinal/lockTime exist)
  for (const s of bulk.userGameStats || []) {
    const u = asLower(s.user.id);
    const lockTime = toNum(s.game.lockTime);
    const gLeague = safeLeague(s.game.league);
    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(s.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) ||
      ({
        league: gLeague,
        lockTime,
        isFinal: !!s.game.isFinal,

        buyGross: 0,
        buyCount: 0,

        sellNet: 0,
        sellGross: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      } as UserGameAgg);

    cur.isFinal = !!s.game.isFinal;
    byUserGame.set(k, cur);
  }

  // Trades (AMM) — ONLY SELL rows (prevents double-counting buys)
  for (const t of bulk.trades || []) {
    if (t.type !== "SELL") continue;

    const u = asLower(t.user.id);
    const lockTime = toNum(t.game.lockTime);
    const gLeague = safeLeague(t.game.league);
    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(t.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) ||
      ({
        league: gLeague,
        lockTime,
        isFinal: !!t.game.isFinal,

        buyGross: 0,
        buyCount: 0,

        sellNet: 0,
        sellGross: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      } as UserGameAgg);

    cur.sellGross += toNum(t.grossOutDec);
    cur.sellNet += toNum(t.netOutDec);
    cur.sellCostClosed += toNum(t.costBasisClosedDec);
    cur.sellPnl += toNum(t.realizedPnlDec);
    cur.sellCount += 1;

    cur.isFinal = !!t.game.isFinal;
    byUserGame.set(k, cur);
  }

  // Bets (covers Legacy + AMM buys)
  for (const b of bulk.bets || []) {
    const u = asLower(b.user.id);
    const lockTime = toNum(b.game.lockTime);
    const gLeague = safeLeague(b.game.league);
    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(b.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) ||
      ({
        league: gLeague,
        lockTime,
        isFinal: !!b.game.isFinal,

        buyGross: 0,
        buyCount: 0,

        sellNet: 0,
        sellGross: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      } as UserGameAgg);

    // Use grossAmount for “capital in” consistency across Legacy and AMM buys.
    cur.buyGross += toNum(b.grossAmount);
    cur.buyCount += 1;

    cur.isFinal = !!b.game.isFinal;
    byUserGame.set(k, cur);
  }

  // Claims
  for (const c of bulk.claims || []) {
    const u = asLower(c.user.id);
    const lockTime = toNum(c.game.lockTime);
    const gLeague = safeLeague(c.game.league);
    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(c.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) ||
      ({
        league: gLeague,
        lockTime,
        isFinal: !!c.game.isFinal,

        buyGross: 0,
        buyCount: 0,

        sellNet: 0,
        sellGross: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      } as UserGameAgg);

    cur.claimTotal += toNum(c.amountDec);
    cur.isFinal = !!c.game.isFinal;
    byUserGame.set(k, cur);
  }

  // ---------------------------
  // Step 4: roll up per-user leaderboard metrics (WINDOW totals)
  // ---------------------------
  //
  // Agreed definitions:
  // - Total Traded = total BUY gross (capital in)
  // - P/L = claims + sell proceeds (netOut)
  // - ROI = (P/L / TotalBuy) - 1
  // - # Trades = buys + sells
  //
  type UserAgg = {
    buyGross: number; // Total Traded (buys only)
    sellNet: number; // sell proceeds (net)
    claimTotal: number; // winnings claimed
    sellPnl: number; // analytics
    sellCost: number; // analytics

    tradesCount: number; // buys + sells
    gamesTouched: number; // count of games with any activity

    favLeagueVolume: Record<string, number>; // based on buyGross (per your "Total Traded" meaning)
  };

  const perUser = new Map<string, UserAgg>();

  for (const [keyUG, g] of byUserGame.entries()) {
    const [u] = keyUG.split("|");

    const hasAny = g.buyGross > 0 || g.sellGross > 0 || g.claimTotal > 0;
    if (!hasAny) continue;

    const agg =
      perUser.get(u) ||
      ({
        buyGross: 0,
        sellNet: 0,
        claimTotal: 0,
        sellPnl: 0,
        sellCost: 0,

        tradesCount: 0,
        gamesTouched: 0,

        favLeagueVolume: {},
      } as UserAgg);

    // totals
    agg.buyGross += g.buyGross;
    agg.sellNet += g.sellNet;
    agg.claimTotal += g.claimTotal;

    // sell analytics
    agg.sellPnl += g.sellPnl;
    agg.sellCost += g.sellCostClosed;

    // counts
    agg.tradesCount += g.buyCount + g.sellCount;
    agg.gamesTouched += 1;

    // favorite league volume based on BUY gross only
    agg.favLeagueVolume[g.league] = (agg.favLeagueVolume[g.league] || 0) + g.buyGross;

    perUser.set(u, agg);
  }

  const rows: LeaderboardRowApi[] = users.map((u) => {
    const agg =
      perUser.get(u) ||
      ({
        buyGross: 0,
        sellNet: 0,
        claimTotal: 0,
        sellPnl: 0,
        sellCost: 0,
        tradesCount: 0,
        gamesTouched: 0,
        favLeagueVolume: {},
      } as UserAgg);

    const totalBuy = agg.buyGross;
    const pnl = agg.claimTotal + agg.sellNet;
    const roiNet = totalBuy > 0 ? pnl / totalBuy - 1 : null;

    const fav =
      Object.entries(agg.favLeagueVolume).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

    const sellsRoi = agg.sellCost > 0 ? agg.sellPnl / agg.sellCost : null;

    return {
      id: u,

      // ✅ Total Traded (BUYS only)
      tradedGross: totalBuy,

      // ✅ P/L (claims + sell proceeds)
      claimsFinal: pnl,
      wonFinal: pnl,

      // ✅ ROI = (P/L / TotalBuy) - 1
      roiNet,

      // keep for UI compat (this is "games touched" in-window)
      tradesNet: agg.gamesTouched,
      poolsJoined: agg.gamesTouched,

      // ✅ # trades = buys + sells
      betsCount: agg.tradesCount,

      favoriteLeague: fav,

      // sell analytics (optional)
      sellsNet: agg.sellNet,
      sellsPnl: agg.sellPnl,
      sellsRoi,

      // back-compat
      user: u,
    };
  });

  // Sort
  const sort = String(params.sort || "ROI").toUpperCase() as LeaderboardSort;
  rows.sort((a, b) => {
    switch (sort) {
      case "GROSS_VOLUME":
        return (b.tradedGross ?? 0) - (a.tradedGross ?? 0);
      case "TOTAL_STAKED":
        // legacy label; mapped to Total Traded (buys)
        return (b.tradedGross ?? 0) - (a.tradedGross ?? 0);
      case "LAST_UPDATED":
        // not available here; fallback to ROI
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
      case "ROI":
      default:
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
    }
  });

  const out = { asOf: new Date().toISOString(), rows };
  cacheSet(key, out, 90_000);
  return out;
}

export async function getUserRecent(params: {
  user: string;
  league: LeagueKey;
  limit: number;
  anchorTs?: number;
  range?: RangeKey;

  // allow API to force legacy inclusion
  includeLegacy?: boolean;
}): Promise<{
  asOf: string;
  user: string;
  rows: RecentTradeRowApi[];
  claimByGame: Record<string, number>;
}> {
  const user = asLower(params.user);
  const limit = Math.max(1, Math.min(params.limit || 10, 50));

  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const range = params.range ?? "ALL";
  const { start, end } = computeWindow(range, anchorTs);
  const leagues = leagueList(params.league);

  const key = cacheKey({
    v: "lb_recent_trades_v7_claims_windowed",
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

  const bundle = await fetchUserRecentTrades({
    user,
    leagues,
    start,
    end,
    limit,
  });

  // net position snapshot by game = staked - withdrawn (range+league windowed)
  const netPositionByGame: Record<string, number> = {};
  for (const s of bundle.userGameStats || []) {
    const gLeague = safeLeague(s.game?.league);
    if (!leagues.includes(gLeague)) continue;
    const gid = String(s.game?.id || "").toLowerCase();
    if (!gid) continue;
    const netPos = Math.max(0, toNum(s.stakedDec) - toNum(s.withdrawnDec));
    netPositionByGame[gid] = Math.max(netPositionByGame[gid] || 0, netPos);
  }

  // claims by game (range+league windowed)
  const claimByGame: Record<string, number> = {};
  for (const c of bundle.claims || []) {
    const gLeague = safeLeague(c.game?.league);
    if (!leagues.includes(gLeague)) continue;
    const gid = String(c.game?.id || "").toLowerCase();
    if (!gid) continue;
    claimByGame[gid] = (claimByGame[gid] || 0) + toNum(c.amountDec);
  }

  const tradeRows: RecentTradeRowApi[] = (bundle.trades || [])
    .map((t): RecentTradeRowApi | null => {
      const g = t.game || ({} as any);
      const gLeague = safeLeague(g.league);
      if (!leagues.includes(gLeague)) return null;

      const sideRaw = String(t.side || "").toUpperCase();
      const side: "A" | "B" = sideRaw === "B" ? "B" : "A";
      const type: "BUY" | "SELL" = t.type === "SELL" ? "SELL" : "BUY";

      const gid = String(g.id || "").toLowerCase();
      const ts = toNum(t.timestamp);

      const winnerRaw = String(g.winnerSide || "").toUpperCase();
      const winnerSide: "A" | "B" | null =
        winnerRaw === "A" || winnerRaw === "B" ? (winnerRaw as any) : null;

      // Normalize columns for UI:
      // BUY: amount=netStake, gross=grossIn
      // SELL: amount=netOut,  gross=grossOut
      const amountDec = type === "BUY" ? toNum(t.netStakeDec) : toNum(t.netOutDec);
      const grossAmountDec = type === "BUY" ? toNum(t.grossInDec) : toNum(t.grossOutDec);

      return {
        id: t.id,
        timestamp: ts,
        type,
        side,
        amountDec,
        grossAmountDec,
        feeDec: toNum(t.feeDec) || 0,
        realizedPnlDec: type === "SELL" ? toNum(t.realizedPnlDec) : 0,
        costBasisClosedDec: type === "SELL" ? toNum(t.costBasisClosedDec) : 0,
        netPositionDec: netPositionByGame[gid] || 0,
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
    .filter((x): x is RecentTradeRowApi => x !== null);

  // legacy fallback: if explicitly requested OR user has no trades (legacy user)
  let legacyRows: RecentTradeRowApi[] = [];
  if (params.includeLegacy || tradeRows.length === 0) {
    const bets = await fetchUserRecentBets({ user, leagues, start, end, limit });

    legacyRows = (bets || [])
      .map((b): RecentTradeRowApi | null => {
        const g = b.game || ({} as any);
        const gLeague = safeLeague(g.league);
        if (!leagues.includes(gLeague)) return null;

        const gid = String(g.id || "").toLowerCase();
        const ts = toNum(b.timestamp);

        const sideRaw = String(b.side || "").toUpperCase();
        const side: "A" | "B" = sideRaw === "B" ? "B" : "A";

        const winnerRaw = String(g.winnerSide || "").toUpperCase();
        const winnerSide: "A" | "B" | null =
          winnerRaw === "A" || winnerRaw === "B" ? (winnerRaw as any) : null;

        // Legacy bets are effectively BUY rows for the dropdown UX
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
          netPositionDec: netPositionByGame[gid] || 0,
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
      .filter((x): x is RecentTradeRowApi => x !== null);
  }

  // merge, sort newest-first, clamp to limit
  const rows = [...tradeRows, ...legacyRows]
    .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
    .slice(0, limit);

  const out = { asOf: new Date().toISOString(), user, rows, claimByGame };
  cacheSet(key, out, 45_000);
  return out;
}
