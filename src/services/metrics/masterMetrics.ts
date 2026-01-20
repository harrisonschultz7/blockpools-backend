// src/services/metrics/masterMetrics.ts
import { subgraphQuery } from "../../subgraph/client";
import { pickLeaderboardQuery, type LeaderboardSort } from "../../subgraph/queries";

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

/**
 * IMPORTANT (updated):
 * - LeaderboardDesktop.tsx expects rows shaped like:
 *   { id, tradedGross, claimsFinal, roiNet, tradesNet, betsCount, poolsJoined, favoriteLeague }
 *
 * This revision:
 * - Uses Trade entities (BUY + SELL) so profitable exits show up in ROI.
 * - roiNet is computed from cashflow on FINAL games in the lockTime window:
 *     roiNet = (claimsFinal + sellProceedsFinal) / buyGrossFinal - 1
 *   where:
 *     buyGrossFinal = sum(trade.grossInDec for BUY)
 *     sellProceedsFinal = sum(trade.netOutDec for SELL)
 *
 * - Adds extra fields (safe additive) that you can use in the UI later:
 *   sellsNet, sellsPnl, sellsRoi
 *
 * - Recent dropdown now returns Trade rows (BUY + SELL), not just Bet rows.
 */

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

// ---------------------------
// In-memory TTL cache (simple starter)
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

function cacheKey(parts: Record<string, any>) {
  return Object.entries(parts)
    .map(([k, v]) => `${k}=${String(v)}`)
    .join("|");
}

// ---------------------------
// Subgraph queries (local to this file)
// ---------------------------

// Candidate users is still sourced from userLeagueStats for fast top-N.
// NOTE: If you later want “pure traders who only sold” to appear, we can add a trades-based candidate fetch.
// For now, we preserve your existing “fast shortlist” approach via pickLeaderboardQuery().

const Q_USERS_TRADES_CLAIMS_STATS_BULK = /* GraphQL */ `
  query UsersTradesClaimsStatsBulk(
    $users: [String!]!
    $leagues: [String!]!
    $start: BigInt!
    $end: BigInt!
    $first: Int!
  ) {
    userGameStats(
      first: $first
      where: { user_in: $users, league_in: $leagues, game_: { lockTime_gte: $start, lockTime_lte: $end } }
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
      where: { user_in: $users, game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
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

const Q_USER_RECENT_TRADES_BUNDLE = /* GraphQL */ `
  query UserRecentTradesBundle(
    $user: String!
    $leagues: [String!]!
    $start: BigInt!
    $end: BigInt!
    $first: Int!
  ) {
    userGameStats(first: 2000, where: { user: $user, league_in: $leagues }) {
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

    claims(first: 2000, where: { user: $user, game_: { league_in: $leagues } }) {
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
// Subgraph types (minimal)
// ---------------------------
type G_UserLeagueStats = {
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

type G_LeaderboardResp = { _meta: any; userLeagueStats: G_UserLeagueStats[] };
type G_BulkResp = {
  _meta?: any;
  userGameStats: G_UserGameStat[];
  claims: G_Claim[];
  trades: G_Trade[];
};

// ---------------------------
// API shapes returned to frontend
// ---------------------------
type LeaderboardRowApi = {
  id: string; // user address lower
  tradedGross: number;
  claimsFinal: number;

  // Updated ROI: incorporates SELL cash-outs
  roiNet: number | null;

  tradesNet: number;
  betsCount: number;
  poolsJoined: number;
  favoriteLeague?: string | null;

  // ✅ NEW additive fields (won't break existing UI)
  sellsNet?: number; // sum netOutDec for SELL (final games in window)
  sellsPnl?: number; // sum realizedPnlDec for SELL
  sellsRoi?: number | null; // sellsPnl / sellsCostBasisClosed

  // Back-compat
  user?: string;
  wonFinal?: number;
};

// Recent rows now include BUY + SELL
export type RecentTradeRowApi = {
  id: string;
  timestamp: number;
  type: "BUY" | "SELL";
  side: "A" | "B";

  // For BUY: amountDec=netStake, grossAmountDec=grossIn
  // For SELL: amountDec=netOut,  grossAmountDec=grossOut
  amountDec: number;
  grossAmountDec: number;

  feeDec?: number;
  realizedPnlDec?: number;
  costBasisClosedDec?: number;

  // Derived net position snapshot (staked - withdrawn, clamped >=0)
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
// Public API (routes call these)
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
    v: "lb_users_v4_trades",
    league: params.league,
    range: params.range,
    sort: params.sort,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; rows: LeaderboardRowApi[] }>(key);
  if (cached) return cached;

  // Step 1: shortlist candidate users (fast top-N)
  const q = pickLeaderboardQuery(params.sort);
  const lb = await subgraphQuery<G_LeaderboardResp>(q, {
    leagues,
    skip: 0,
    first: limit,
  });

  const users = Array.from(
    new Set(lb.userLeagueStats.map((x) => asLower(x.user.id)).filter(Boolean))
  );

  if (!users.length) {
    const out = { asOf: new Date().toISOString(), rows: [] as LeaderboardRowApi[] };
    cacheSet(key, out, 60_000);
    return out;
  }

  // Step 2: fetch bulk windowed activity (FINAL logic is applied in code, but we reduce data by lockTime window here)
  const bulk = await subgraphQuery<G_BulkResp>(Q_USERS_TRADES_CLAIMS_STATS_BULK, {
    users,
    leagues,
    start: String(start),
    end: String(end),
    first: 5000,
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

      teamACode?: string | null;
      teamBCode?: string | null;
      teamAName?: string | null;
      teamBName?: string | null;

      // snapshot exposure
      staked: number;
      withdrawn: number;

      // trade cashflow
      buyGross: number; // sum BUY grossInDec
      buyNetStake: number; // sum BUY netStakeDec
      buyCount: number;

      sellGross: number; // sum SELL grossOutDec (volume)
      sellNet: number; // sum SELL netOutDec (cash received)
      sellCostClosed: number; // sum costBasisClosedDec
      sellPnl: number; // sum realizedPnlDec
      sellCount: number;

      // claims (final payouts)
      claimTotal: number;

      lastTradeTs: number;
      lastSide?: "A" | "B" | null;
    }
  >();

  // userGameStats: stake/withdraw + game metadata (for continuity + netPosition logic)
  for (const s of bulk.userGameStats || []) {
    const u = asLower(s.user.id);
    const lockTime = toNum(s.game.lockTime);
    const gLeague = safeLeague(s.game.league);

    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const k = `${u}|${String(s.game.id || "").toLowerCase()}`;
    const cur =
      byUserGame.get(k) || {
        league: gLeague,
        lockTime,
        isFinal: !!s.game.isFinal,
        winnerSide: (s.game as any).winnerSide ?? null,
        teamACode: (s.game as any).teamACode ?? null,
        teamBCode: (s.game as any).teamBCode ?? null,
        teamAName: (s.game as any).teamAName ?? null,
        teamBName: (s.game as any).teamBName ?? null,

        staked: 0,
        withdrawn: 0,

        buyGross: 0,
        buyNetStake: 0,
        buyCount: 0,

        sellGross: 0,
        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,

        lastTradeTs: 0,
        lastSide: null,
      };

    cur.staked = Math.max(cur.staked, toNum(s.stakedDec));
    cur.withdrawn = Math.max(cur.withdrawn, toNum(s.withdrawnDec));
    cur.isFinal = !!s.game.isFinal;

    byUserGame.set(k, cur);
  }

  // trades: BUY + SELL (primary source of volume + sell ROI)
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
        teamACode: t.game.teamACode ?? null,
        teamBCode: t.game.teamBCode ?? null,
        teamAName: t.game.teamAName ?? null,
        teamBName: t.game.teamBName ?? null,

        staked: 0,
        withdrawn: 0,

        buyGross: 0,
        buyNetStake: 0,
        buyCount: 0,

        sellGross: 0,
        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,

        lastTradeTs: 0,
        lastSide: null,
      };

    const ts = toNum(t.timestamp);
    if (ts >= cur.lastTradeTs) {
      cur.lastTradeTs = ts;
      const s = String(t.side || "").toUpperCase();
      cur.lastSide = s === "A" || s === "B" ? (s as "A" | "B") : cur.lastSide;
    }

    if (t.type === "BUY") {
      cur.buyGross += toNum(t.grossInDec);
      cur.buyNetStake += toNum(t.netStakeDec);
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

  // claims: final payouts
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
        teamACode: (c.game as any).teamACode ?? null,
        teamBCode: (c.game as any).teamBCode ?? null,
        teamAName: (c.game as any).teamAName ?? null,
        teamBName: (c.game as any).teamBName ?? null,

        staked: 0,
        withdrawn: 0,

        buyGross: 0,
        buyNetStake: 0,
        buyCount: 0,

        sellGross: 0,
        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,

        lastTradeTs: 0,
        lastSide: null,
      };

    cur.claimTotal += toNum(c.amountDec);
    cur.isFinal = !!c.game.isFinal;
    byUserGame.set(k, cur);
  }

  // Step 4: per-user leaderboard metrics (FINAL-only)
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

    // We count only games where there was at least a BUY (capital deployed)
    // (If you later add "airdrop-only sells" you can broaden this.)
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

    // tradedGross: volume notion = BUY gross in + SELL gross out
    const traded = g.buyGross + g.sellGross;
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

    // Updated ROI: include SELL proceeds and CLAIMS relative to gross buy capital deployed.
    const denom = agg.buyGrossFinal;
    const totalReturn = agg.claimFinal + agg.sellNetFinal;
    const roiNet = denom > 0 ? totalReturn / denom - 1 : null;

    const fav =
      Object.entries(agg.favoriteLeague).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

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
      case "TOTAL_STAKED":
        return (b.tradedGross ?? 0) - (a.tradedGross ?? 0);
      case "GROSS_VOLUME":
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
    v: "lb_recent_trades_v4",
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

  // Pull recent trades + net position snapshots + claims
  const bundle = await subgraphQuery<G_BulkResp>(Q_USER_RECENT_TRADES_BUNDLE, {
    user,
    leagues,
    start: String(start),
    end: String(end),
    first: 200, // fetch a bit more than limit so we can filter/sort safely
  });

  // Net position snapshot by game: staked - withdrawn (clamped)
  const netPositionByGame: Record<string, number> = {};
  for (const s of bundle.userGameStats || []) {
    const gLeague = safeLeague(s.game?.league);
    if (!leagues.includes(gLeague)) continue;

    const gid = String(s.game?.id || "").toLowerCase();
    if (!gid) continue;

    const netPos = clamp0(toNum(s.stakedDec) - toNum(s.withdrawnDec));
    netPositionByGame[gid] = Math.max(netPositionByGame[gid] || 0, netPos);
  }

  // Claims by game (for UI ribbons/labels)
  const claimByGame: Record<string, number> = {};
  for (const c of bundle.claims || []) {
    const gLeague = safeLeague(c.game?.league);
    if (!leagues.includes(gLeague)) continue;

    const gid = String(c.game?.id || "").toLowerCase();
    if (!gid) continue;

    claimByGame[gid] = (claimByGame[gid] || 0) + toNum(c.amountDec);
  }

  // Build recent trade rows
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

      // Normalize cash columns for UI:
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
    .filter((x): x is RecentTradeRowApi => x !== null)
    .sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0))
    .slice(0, limit);

  const out = {
    asOf: new Date().toISOString(),
    user,
    rows,
    claimByGame,
  };

  cacheSet(key, out, 60_000);
  return out;
}
