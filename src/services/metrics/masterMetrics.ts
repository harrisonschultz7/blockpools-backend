// src/services/metrics/masterMetrics.ts
import { subgraphQuery } from "../../subgraph/client";
import { pickLeaderboardQuery, type LeaderboardSort } from "../../subgraph/queries";

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

function clamp0(n: number) {
  return n < 0 ? 0 : n;
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
// Queries
// ---------------------------
const Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE = /* GraphQL */ `
  query UsersTradesClaimsStatsBulkPage(
    $users: [String!]!
    $leagues: [String!]!
    $start: BigInt!
    $end: BigInt!
    $first: Int!
    $skipTrades: Int!
    $skipClaims: Int!
  ) {
    # userGameStats is typically small; keep it fixed and not paginated.
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
      where: { user_in: $users, game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
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
      where: { user_in: $users, game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
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

const Q_USER_RECENT_TRADES_PAGE = /* GraphQL */ `
  query UserRecentTradesPage(
    $user: String!
    $leagues: [String!]!
    $start: BigInt!
    $end: BigInt!
    $first: Int!
    $skipTrades: Int!
  ) {
    userGameStats(first: 1000, where: { user: $user, league_in: $leagues }) {
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

    claims(first: 1000, where: { user: $user, game_: { league_in: $leagues } }) {
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
      where: { user: $user, timestamp_gte: $start, timestamp_lte: $end, game_: { league_in: $leagues } }
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
// Subgraph types
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

  spotPriceBps: string;
  avgPriceBps: string;

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
    winnerTeamCode?: string | null;
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
    winnerTeamCode?: string | null;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_LeaderboardResp = { _meta?: any; userLeagueStats: G_UserLeagueStats[] };
type G_BulkPageResp = {
  _meta?: any;
  userGameStats: G_UserGameStat[];
  claims: G_Claim[];
  trades: G_Trade[];
};

// ---------------------------
// API shapes
// ---------------------------
type LeaderboardRowApi = {
  id: string;
  tradedGross: number;
  claimsFinal: number;
  roiNet: number | null;

  tradesNet: number;
  betsCount: number;
  poolsJoined: number;
  favoriteLeague?: string | null;

  sellsNet?: number;
  sellsPnl?: number;
  sellsRoi?: number | null;

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
// Internal bulk fetchers (paginate within Graph limits)
// ---------------------------
async function fetchBulkWindowed(params: {
  users: string[];
  leagues: string[];
  start: number;
  end: number;
  // hard cap on total rows pulled per collection to protect backend
  maxTrades?: number;
  maxClaims?: number;
}): Promise<{ userGameStats: G_UserGameStat[]; trades: G_Trade[]; claims: G_Claim[] }> {
  const first = GQL_MAX_FIRST;

  const maxTrades = Math.max(0, Math.min(params.maxTrades ?? 3000, 10000));
  const maxClaims = Math.max(0, Math.min(params.maxClaims ?? 3000, 10000));

  // Pull first page to also get userGameStats (we only need it once)
  const base = await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
    users: params.users,
    leagues: params.leagues,
    start: String(params.start),
    end: String(params.end),
    first,
    skipTrades: 0,
    skipClaims: 0,
  });

  const outStats = base.userGameStats || [];

  const outTrades: G_Trade[] = [];
  const outClaims: G_Claim[] = [];

  // trades pagination
  {
    let skip = 0;
    while (outTrades.length < maxTrades) {
      const page = skip === 0 ? base.trades || [] : (
        await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
          users: params.users,
          leagues: params.leagues,
          start: String(params.start),
          end: String(params.end),
          first,
          skipTrades: skip,
          skipClaims: 0, // unused on this pass
        })
      ).trades || [];

      outTrades.push(...page);

      if (page.length < first) break;
      skip += first;

      // safety: avoid pathological scans
      if (skip > 20000) break;
    }
  }

  // claims pagination
  {
    let skip = 0;
    while (outClaims.length < maxClaims) {
      const page = skip === 0 ? base.claims || [] : (
        await subgraphQuery<G_BulkPageResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK_PAGE, {
          users: params.users,
          leagues: params.leagues,
          start: String(params.start),
          end: String(params.end),
          first,
          skipTrades: 0, // unused on this pass
          skipClaims: skip,
        })
      ).claims || [];

      outClaims.push(...page);

      if (page.length < first) break;
      skip += first;

      if (skip > 20000) break;
    }
  }

  return { userGameStats: outStats, trades: outTrades.slice(0, maxTrades), claims: outClaims.slice(0, maxClaims) };
}

async function fetchUserRecentTrades(params: {
  user: string;
  leagues: string[];
  start: number;
  end: number;
  limit: number;
}): Promise<G_BulkPageResp> {
  // pull enough trades pages so we can return `limit` after filtering/sorting
  const first = GQL_MAX_FIRST;
  const need = Math.max(params.limit * 5, 200); // heuristic buffer
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
    v: "lb_users_v4_trades_paged",
    league: params.league,
    range: params.range,
    sort: params.sort,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; rows: LeaderboardRowApi[] }>(key);
  if (cached) return cached;

  // Step 1: shortlist candidate users (fast)
  const q = pickLeaderboardQuery(params.sort);
  const lb = await subgraphQuery<G_LeaderboardResp>(q, {
    leagues,
    skip: 0,
    first: limit,
  });

  const users = Array.from(new Set(lb.userLeagueStats.map((x) => asLower(x.user.id)).filter(Boolean)));
  if (!users.length) {
    const out = { asOf: new Date().toISOString(), rows: [] as LeaderboardRowApi[] };
    cacheSet(key, out, 60_000);
    return out;
  }

  // Step 2: fetch windowed activity with pagination (within Graph constraints)
  const bulk = await fetchBulkWindowed({
    users,
    leagues,
    start,
    end,
    maxTrades: 4000,
    maxClaims: 4000,
  });

  const inLockWindow = (lockTime: number) => lockTime >= start && lockTime <= end;

  // Step 3: per-user per-game aggregates
  const byUserGame = new Map<
    string,
    {
      league: string;
      lockTime: number;
      isFinal: boolean;
      winnerSide?: string | null;

      staked: number;
      withdrawn: number;

      buyGross: number;
      buyCount: number;

      sellGross: number;
      sellNet: number;
      sellCostClosed: number;
      sellPnl: number;
      sellCount: number;

      claimTotal: number;
    }
  >();

  // stats snapshot
  for (const s of bulk.userGameStats || []) {
    const u = asLower(s.user.id);
    const lockTime = toNum(s.game.lockTime);
    const gLeague = safeLeague(s.game.league);

    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(s.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) || {
        league: gLeague,
        lockTime,
        isFinal: !!s.game.isFinal,
        winnerSide: (s.game as any).winnerSide ?? null,

        staked: 0,
        withdrawn: 0,

        buyGross: 0,
        buyCount: 0,

        sellGross: 0,
        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      };

    cur.staked = Math.max(cur.staked, toNum(s.stakedDec));
    cur.withdrawn = Math.max(cur.withdrawn, toNum(s.withdrawnDec));
    cur.isFinal = !!s.game.isFinal;

    byUserGame.set(k, cur);
  }

  // trades
  for (const t of bulk.trades || []) {
    const u = asLower(t.user.id);
    const lockTime = toNum(t.game.lockTime);
    const gLeague = safeLeague(t.game.league);

    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(t.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) || {
        league: gLeague,
        lockTime,
        isFinal: !!t.game.isFinal,
        winnerSide: t.game.winnerSide ?? null,

        staked: 0,
        withdrawn: 0,

        buyGross: 0,
        buyCount: 0,

        sellGross: 0,
        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      };

    if (t.type === "BUY") {
      cur.buyGross += toNum(t.grossInDec);
      cur.buyCount += 1;
    } else if (t.type === "SELL") {
      cur.sellGross += toNum(t.grossOutDec);
      cur.sellNet += toNum(t.netOutDec);
      cur.sellCostClosed += toNum(t.costBasisClosedDec);
      cur.sellPnl += toNum(t.realizedPnlDec);
      cur.sellCount += 1;
    }

    cur.isFinal = !!t.game.isFinal;
    byUserGame.set(k, cur);
  }

  // claims
  for (const c of bulk.claims || []) {
    const u = asLower(c.user.id);
    const lockTime = toNum(c.game.lockTime);
    const gLeague = safeLeague(c.game.league);

    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(c.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) || {
        league: gLeague,
        lockTime,
        isFinal: !!c.game.isFinal,
        winnerSide: (c.game as any).winnerSide ?? null,

        staked: 0,
        withdrawn: 0,

        buyGross: 0,
        buyCount: 0,

        sellGross: 0,
        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      };

    cur.claimTotal += toNum(c.amountDec);
    cur.isFinal = !!c.game.isFinal;
    byUserGame.set(k, cur);
  }

  // Step 4: per-user metrics (FINAL-only)
  const perUser = new Map<
    string,
    {
      buyGrossFinal: number;
      sellNetFinal: number;
      sellPnlFinal: number;
      sellCostFinal: number;

      claimFinal: number;
      tradedFinal: number;

      gamesFinalWithAny: number;
      buyCountFinal: number;

      poolsJoinedFinal: number;
      favoriteLeague: Record<string, number>;
    }
  >();

  for (const [keyUG, g] of byUserGame.entries()) {
    const [u] = keyUG.split("|");
    if (!g.isFinal) continue;

    const traded = g.buyGross + g.sellGross;
    const hasBuy = g.buyGross > 0;

    const agg =
      perUser.get(u) || {
        buyGrossFinal: 0,
        sellNetFinal: 0,
        sellPnlFinal: 0,
        sellCostFinal: 0,

        claimFinal: 0,
        tradedFinal: 0,

        gamesFinalWithAny: 0,
        buyCountFinal: 0,

        poolsJoinedFinal: 0,
        favoriteLeague: {},
      };

    agg.tradedFinal += traded;

    if (hasBuy) {
      agg.buyGrossFinal += g.buyGross;
      agg.sellNetFinal += g.sellNet;
      agg.sellPnlFinal += g.sellPnl;
      agg.sellCostFinal += g.sellCostClosed;

      agg.claimFinal += g.claimTotal;

      agg.buyCountFinal += g.buyCount;
      agg.gamesFinalWithAny += 1;
      agg.poolsJoinedFinal += 1;

      agg.favoriteLeague[g.league] = (agg.favoriteLeague[g.league] || 0) + traded;
    }

    perUser.set(u, agg);
  }

  const rows: LeaderboardRowApi[] = users.map((u) => {
    const agg =
      perUser.get(u) || {
        buyGrossFinal: 0,
        sellNetFinal: 0,
        sellPnlFinal: 0,
        sellCostFinal: 0,
        claimFinal: 0,
        tradedFinal: 0,
        gamesFinalWithAny: 0,
        buyCountFinal: 0,
        poolsJoinedFinal: 0,
        favoriteLeague: {},
      };

    const denom = agg.buyGrossFinal;
    const totalReturn = agg.claimFinal + agg.sellNetFinal;
    const roiNet = denom > 0 ? totalReturn / denom - 1 : null;

    const fav = Object.entries(agg.favoriteLeague).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;
    const sellsRoi = agg.sellCostFinal > 0 ? agg.sellPnlFinal / agg.sellCostFinal : null;

    return {
      id: u,
      tradedGross: agg.tradedFinal,
      claimsFinal: agg.claimFinal,
      roiNet,
      tradesNet: agg.gamesFinalWithAny,
      betsCount: agg.buyCountFinal,
      poolsJoined: agg.poolsJoinedFinal,
      favoriteLeague: fav,
      sellsNet: agg.sellNetFinal,
      sellsPnl: agg.sellPnlFinal,
      sellsRoi,
      user: u,
      wonFinal: agg.claimFinal,
    };
  });

  const sort = String(params.sort || "ROI").toUpperCase() as LeaderboardSort;
  rows.sort((a, b) => {
    switch (sort) {
      case "GROSS_VOLUME":
      case "TOTAL_STAKED":
        return (b.tradedGross ?? 0) - (a.tradedGross ?? 0);
      case "LAST_UPDATED":
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
      case "ROI":
      default:
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
    }
  });

  const out = { asOf: new Date().toISOString(), rows };
  cacheSet(key, out, 120_000);
  return out;
}

export async function getUserRecent(params: {
  user: string;
  league: LeagueKey;
  limit: number;
  anchorTs?: number;
  range?: RangeKey;
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
    v: "lb_recent_trades_v4_paged",
    user,
    league: params.league,
    range,
    limit,
    anchorTs,
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

  const netPositionByGame: Record<string, number> = {};
  for (const s of bundle.userGameStats || []) {
    const gLeague = safeLeague(s.game?.league);
    if (!leagues.includes(gLeague)) continue;
    const gid = String(s.game?.id || "").toLowerCase();
    if (!gid) continue;
    const netPos = clamp0(toNum(s.stakedDec) - toNum(s.withdrawnDec));
    netPositionByGame[gid] = Math.max(netPositionByGame[gid] || 0, netPos);
  }

  const claimByGame: Record<string, number> = {};
  for (const c of bundle.claims || []) {
    const gLeague = safeLeague(c.game?.league);
    if (!leagues.includes(gLeague)) continue;
    const gid = String(c.game?.id || "").toLowerCase();
    if (!gid) continue;
    claimByGame[gid] = (claimByGame[gid] || 0) + toNum(c.amountDec);
  }

  const rows = (bundle.trades || [])
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
    .filter((x): x is RecentTradeRowApi => x !== null)
    .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
    .slice(0, limit);

  const out = { asOf: new Date().toISOString(), user, rows, claimByGame };
  cacheSet(key, out, 60_000);
  return out;
}
