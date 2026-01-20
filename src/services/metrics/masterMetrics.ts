// src/services/metrics/masterMetrics.ts
import { subgraphQuery } from "../../subgraph/client";
import {
  pickLeaderboardQuery,
  type LeaderboardSort,
  Q_USERS_NET_BULK,
} from "../../subgraph/queries";

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

/**
 * IMPORTANT:
 * - Your frontend LeaderboardDesktop.tsx expects leaderboard rows shaped like:
 *   { id, tradedGross, claimsFinal, roiNet, tradesNet, betsCount, poolsJoined, favoriteLeague }
 *
 * - Your frontend recent dropdown expects:
 *   { rows: ApiRecentBetRow[], claimByGame?: Record<string, number> }
 *   where each bet row includes: { id, timestamp, side, amountDec, grossAmountDec, game:{...} }
 *
 * This file ensures:
 * - Leaderboard is FINAL-only + lockTime-windowed (correct for ROI)
 * - Recent is TRADE-by-TRADE + bet.timestamp-windowed (so PENDING shows)
 * - Recent includes netPositionDec per game (staked-withdrawn, clamped >=0)
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

type G_Bet = {
  id?: string;
  user: { id: string };
  amountDec: string;
  grossAmount?: string | null;
  fee?: string | null;
  timestamp: string;
  side?: string | null;
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
    winnerTeamCode?: string | null;
    teamACode?: string | null;
    teamBCode?: string | null;
    teamAName?: string | null;
    teamBName?: string | null;
  };
};

type G_LeaderboardResp = { _meta: any; userLeagueStats: G_UserLeagueStats[] };
type G_NetBulkResp = {
  _meta: any;
  userGameStats: G_UserGameStat[];
  claims: G_Claim[];
  bets: G_Bet[];
};

// ---------------------------
// API shapes returned to frontend
// ---------------------------
type LeaderboardRowApi = {
  id: string; // user address lower
  tradedGross: number;
  claimsFinal: number;
  roiNet: number | null;
  tradesNet: number;
  betsCount: number;
  poolsJoined: number;
  favoriteLeague?: string | null;

  // Back-compat (older naming your UI may have tolerated)
  user?: string;
  wonFinal?: number;
};

type RecentBetRowApi = {
  id: string;
  timestamp: number;
  side: "A" | "B";
  amountDec: number;
  grossAmountDec: number;

  // New: derived net position for the game at latest stat snapshot (staked - withdrawn)
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
    v: "lb_users_v3",
    league: params.league,
    range: params.range,
    sort: params.sort,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; rows: LeaderboardRowApi[] }>(key);
  if (cached) return cached;

  // Step 1: Get candidate users via userLeagueStats (fast top-N)
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

  // Step 2: Fetch bulk activity for those users
  // Keep within The Graph limits.
  const bulk = await subgraphQuery<G_NetBulkResp>(Q_USERS_NET_BULK, {
    users,
    first: 1000,
  });

  // Leaderboard windowing is based on game lockTime (final-only)
  const inLockWindow = (lockTime: number) => lockTime >= start && lockTime <= end;

  // Step 3: Build per-user per-game aggregates in window (final-only for ROI)
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

      staked: number;
      withdrawn: number;
      grossTraded: number;
      claimTotal: number;
      lastBetTs: number;
      lastSide?: "A" | "B" | null;

      betCount: number;
    }
  >();

  // userGameStats: stake/withdraw + game metadata
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
        grossTraded: 0,
        claimTotal: 0,
        lastBetTs: 0,
        lastSide: null,
        betCount: 0,
      };

    cur.staked = Math.max(cur.staked, toNum(s.stakedDec));
    cur.withdrawn = Math.max(cur.withdrawn, toNum(s.withdrawnDec));
    cur.isFinal = !!s.game.isFinal;

    byUserGame.set(k, cur);
  }

  // bets: traded gross + last side + betCount
  for (const b of bulk.bets || []) {
    const u = asLower(b.user.id);
    const lockTime = toNum(b.game.lockTime);
    const gLeague = safeLeague(b.game.league);

    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(b.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    const cur =
      byUserGame.get(k) || {
        league: gLeague,
        lockTime,
        isFinal: !!b.game.isFinal,
        winnerSide: b.game.winnerSide ?? null,
        teamACode: b.game.teamACode ?? null,
        teamBCode: b.game.teamBCode ?? null,
        teamAName: b.game.teamAName ?? null,
        teamBName: b.game.teamBName ?? null,
        staked: 0,
        withdrawn: 0,
        grossTraded: 0,
        claimTotal: 0,
        lastBetTs: 0,
        lastSide: null,
        betCount: 0,
      };

    const gross = b.grossAmount != null ? toNum(b.grossAmount) : toNum(b.amountDec);
    cur.grossTraded += gross;
    cur.betCount += 1;

    const ts = toNum(b.timestamp);
    if (ts >= cur.lastBetTs) {
      cur.lastBetTs = ts;
      const s = String(b.side || "").toUpperCase();
      cur.lastSide = s === "A" || s === "B" ? (s as "A" | "B") : cur.lastSide;
    }

    cur.isFinal = !!b.game.isFinal;
    byUserGame.set(k, cur);
  }

  // claims: winnings
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
        grossTraded: 0,
        claimTotal: 0,
        lastBetTs: 0,
        lastSide: null,
        betCount: 0,
      };

    cur.claimTotal += toNum(c.amountDec);
    cur.isFinal = !!c.game.isFinal;
    byUserGame.set(k, cur);
  }

  // Step 4: Reduce to per-user leaderboard metrics (final-only)
  const perUser = new Map<
    string,
    {
      stakeFinal: number;
      claimFinal: number;
      tradedFinal: number;
      gamesFinalWithNet: number;
      betsCountFinal: number;
      poolsJoinedFinal: number;
      favoriteLeague: Record<string, number>;
    }
  >();

  for (const [keyUG, g] of byUserGame.entries()) {
    const [u] = keyUG.split("|");
    if (!g.isFinal) continue;

    const netStake = clamp0(g.staked - g.withdrawn);
    const agg =
      perUser.get(u) || {
        stakeFinal: 0,
        claimFinal: 0,
        tradedFinal: 0,
        gamesFinalWithNet: 0,
        betsCountFinal: 0,
        poolsJoinedFinal: 0,
        favoriteLeague: {},
      };

    agg.stakeFinal += netStake;
    agg.claimFinal += g.claimTotal;
    agg.tradedFinal += g.grossTraded;

    if (netStake > 0) agg.gamesFinalWithNet += 1;

    agg.poolsJoinedFinal += 1;
    agg.betsCountFinal += g.betCount;

    agg.favoriteLeague[g.league] = (agg.favoriteLeague[g.league] || 0) + g.grossTraded;

    perUser.set(u, agg);
  }

  const rows: LeaderboardRowApi[] = users.map((u) => {
    const agg =
      perUser.get(u) || {
        stakeFinal: 0,
        claimFinal: 0,
        tradedFinal: 0,
        gamesFinalWithNet: 0,
        betsCountFinal: 0,
        poolsJoinedFinal: 0,
        favoriteLeague: {},
      };

    const denom = agg.stakeFinal;
    const roiNet = denom > 0 ? agg.claimFinal / denom - 1 : null;

    const fav =
      Object.entries(agg.favoriteLeague).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

    return {
      id: u,
      tradedGross: agg.tradedFinal,
      claimsFinal: agg.claimFinal,
      roiNet,
      tradesNet: agg.gamesFinalWithNet,
      betsCount: agg.betsCountFinal,
      poolsJoined: agg.poolsJoinedFinal,
      favoriteLeague: fav,
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
  rows: RecentBetRowApi[];
  claimByGame: Record<string, number>;
}> {
  const user = asLower(params.user);
  const limit = Math.max(1, Math.min(params.limit || 5, 50));

  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const range = params.range ?? "ALL";
  const { start, end } = computeWindow(range, anchorTs);
  const leagues = leagueList(params.league);

  const key = cacheKey({
    v: "lb_recent_trades_v3",
    user,
    league: params.league,
    range,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{
    asOf: string;
    user: string;
    rows: RecentBetRowApi[];
    claimByGame: Record<string, number>;
  }>(key);
  if (cached) return cached;

  // Fetch bulk activity for the single user (bounded by The Graph limits)
  const bulk = await subgraphQuery<G_NetBulkResp>(Q_USERS_NET_BULK, {
    users: [user],
    first: 1000,
  });

  // For trade-by-trade history:
  // - filter by BET timestamp (so pending/future games show)
  const inBetWindow = (ts: number) => ts >= start && ts <= end;

  // ---- Build netPositionByGame from userGameStats (staked - withdrawn) ----
  // This is the "current position" snapshot from subgraph aggregation.
  const netPositionByGame: Record<string, number> = {};
  for (const s of bulk.userGameStats || []) {
    const gLeague = safeLeague(s.game?.league);
    if (!leagues.includes(gLeague)) continue;

    const gid = String(s.game?.id || "").toLowerCase();
    if (!gid) continue;

    const netPos = clamp0(toNum(s.stakedDec) - toNum(s.withdrawnDec));
    // Use max in case multiple stat rows or partial updates exist
    netPositionByGame[gid] = Math.max(netPositionByGame[gid] || 0, netPos);
  }

  // ---- Build claimByGame (claim totals per game) ----
  // No time filtering here; pending games have no claims anyway.
  const claimByGame: Record<string, number> = {};
  for (const c of bulk.claims || []) {
    const gLeague = safeLeague(c.game?.league);
    if (!leagues.includes(gLeague)) continue;

    const gid = String(c.game?.id || "").toLowerCase();
    if (!gid) continue;

    claimByGame[gid] = (claimByGame[gid] || 0) + toNum(c.amountDec);
  }

  // ---- Trade-by-trade rows from bets (includes PENDING) ----
  const rows = (bulk.bets || [])
    .map((b): RecentBetRowApi | null => {
      const g = b.game || ({} as any);

      const ts = toNum(b.timestamp);
      if (range !== "ALL" && !inBetWindow(ts)) return null;

      const gLeague = safeLeague(g.league);
      if (!leagues.includes(gLeague)) return null;

      const sideRaw = String(b.side || "").toUpperCase();
      const side: "A" | "B" = sideRaw === "B" ? "B" : "A";

      const amountDec = toNum(b.amountDec);
      const grossAmountDec = b.grossAmount != null ? toNum(b.grossAmount) : amountDec;

      const winnerRaw = String(g.winnerSide || "").toUpperCase();
      const winnerSide: "A" | "B" | null =
        winnerRaw === "A" || winnerRaw === "B" ? (winnerRaw as any) : null;

      const gid = String(g.id || "").toLowerCase();
      const betId =
        String((b as any).id || "") ||
        `${user}:${gid}:${ts}:${side}`;

      return {
        id: betId,
        timestamp: ts,
        side,
        amountDec,
        grossAmountDec,
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
    .filter((x): x is RecentBetRowApi => x !== null)
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
