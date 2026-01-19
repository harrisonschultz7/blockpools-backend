// src/services/metrics/masterMetrics.ts
import { subgraphQuery } from "../../subgraph/client";
import {
  pickLeaderboardQuery,
  type LeaderboardSort,
  Q_USERS_NET_BULK,
} from "../../subgraph/queries";

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

type LeaderboardRow = {
  user: string;              // 0x...
  roiNet: number | null;     // null if denom 0
  tradedGross: number;       // final-only, in-window
  wonFinal: number;          // claim totals final-only, in-window
  tradesNet: number;         // distinct final games where netStake>0 in-window
  betsCount: number;         // count of bets final-only, in-window (or total; here final-only)
  poolsJoined: number;       // distinct games with any activity (final-only, in-window)
  favoriteLeague?: string | null;
};

type RecentBetRow = {
  gameId: string;
  league: string;
  lockTime: number;
  isFinal: boolean;
  winnerSide?: string | null;

  teamACode?: string | null;
  teamBCode?: string | null;
  teamAName?: string | null;
  teamBName?: string | null;

  side?: "A" | "B" | null;   // best-effort (latest bet side in that game)
  netStake: number;          // max(staked-withdrawn,0)
  grossTraded: number;       // sum grossAmount (fallback amountDec)
  claimTotal: number;        // sum claims
};

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
  user: { id: string };
  amountDec: string;
  timestamp: string;
  game: {
    id: string;
    league: string;
    lockTime: string;
    isFinal: boolean;
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
type G_NetBulkResp = { _meta: any; userGameStats: G_UserGameStat[]; claims: G_Claim[]; bets: G_Bet[] };

// ---------------------------
// Public API (views call these)
// ---------------------------
export async function getLeaderboardUsers(params: {
  league: LeagueKey;
  range: RangeKey;
  sort: LeaderboardSort;
  limit: number;
  anchorTs?: number;
}): Promise<{ asOf: string; rows: LeaderboardRow[] }> {
  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const { start, end } = computeWindow(params.range, anchorTs);

  const leagues = leagueList(params.league);
  const limit = Math.max(1, Math.min(params.limit || 250, 500));

  const key = cacheKey({
    v: "lb_users_v1",
    league: params.league,
    range: params.range,
    sort: params.sort,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; rows: LeaderboardRow[] }>(key);
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
    const out = { asOf: new Date().toISOString(), rows: [] as LeaderboardRow[] };
    cacheSet(key, out, 60_000);
    return out;
  }

  // Step 2: Fetch bulk activity for those users
  // NOTE: TheGraph will cap results; if you exceed, we’ll add pagination later.
  const bulk = await subgraphQuery<G_NetBulkResp>(Q_USERS_NET_BULK, {
    users,
    first: 5000,
  });

  // Step 3: Build per-user per-game aggregates in window (final-only for ROI)
  const byUserGame = new Map<string, {
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
  }>();

  const inWindow = (lockTime: number) => lockTime >= start && lockTime <= end;

  // userGameStats: stake/withdraw + game metadata
  for (const s of bulk.userGameStats) {
    const u = asLower(s.user.id);
    const lockTime = toNum(s.game.lockTime);
    if (!inWindow(lockTime)) continue;
    if (!leagues.includes(String(s.game.league))) continue;

    const k = `${u}|${s.game.id}`;
    const cur = byUserGame.get(k) || {
      league: String(s.game.league),
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
    };

    // In your schema, these are already decimal strings
    cur.staked = Math.max(cur.staked, toNum(s.stakedDec));
    cur.withdrawn = Math.max(cur.withdrawn, toNum(s.withdrawnDec));
    cur.isFinal = !!s.game.isFinal;

    byUserGame.set(k, cur);
  }

  // bets: traded gross + last side
  for (const b of bulk.bets) {
    const u = asLower(b.user.id);
    const lockTime = toNum(b.game.lockTime);
    if (!inWindow(lockTime)) continue;
    if (!leagues.includes(String(b.game.league))) continue;

    const k = `${u}|${b.game.id}`;
    const cur = byUserGame.get(k) || {
      league: String(b.game.league),
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
    };

    const gross = b.grossAmount != null ? toNum(b.grossAmount) : toNum(b.amountDec);
    cur.grossTraded += gross;

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
  for (const c of bulk.claims) {
    const u = asLower(c.user.id);
    const lockTime = toNum(c.game.lockTime);
    if (!inWindow(lockTime)) continue;
    if (!leagues.includes(String(c.game.league))) continue;

    const k = `${u}|${c.game.id}`;
    const cur = byUserGame.get(k) || {
      league: String(c.game.league),
      lockTime,
      isFinal: !!c.game.isFinal,
      winnerSide: null,
      teamACode: null,
      teamBCode: null,
      teamAName: null,
      teamBName: null,
      staked: 0,
      withdrawn: 0,
      grossTraded: 0,
      claimTotal: 0,
      lastBetTs: 0,
      lastSide: null,
    };

    cur.claimTotal += toNum(c.amountDec);
    cur.isFinal = !!c.game.isFinal;

    byUserGame.set(k, cur);
  }

  // Step 4: Reduce to per-user leaderboard metrics (final-only)
  const perUser = new Map<string, {
    stakeFinal: number;
    claimFinal: number;
    tradedFinal: number;
    gamesFinalWithNet: number;
    betsCountFinal: number;
    poolsJoinedFinal: number;
    favoriteLeague: Record<string, number>;
  }>();

  for (const [keyUG, g] of byUserGame.entries()) {
    const [u] = keyUG.split("|");
    if (!g.isFinal) continue; // leaderboard metrics are FINAL-only

    const netStake = clamp0(g.staked - g.withdrawn);
    const agg = perUser.get(u) || {
      stakeFinal: 0,
      claimFinal: 0,
      tradedFinal: 0,
      gamesFinalWithNet: 0,
      betsCountFinal: 0,
      poolsJoinedFinal: 0,
      favoriteLeague: {},
    };

    // stakeFinal: sum net stakes across final games
    agg.stakeFinal += netStake;

    // claimFinal: sum claims across final games
    agg.claimFinal += g.claimTotal;

    // tradedFinal: sum gross traded across final games
    agg.tradedFinal += g.grossTraded;

    // tradesNet: count games where netStake>0
    if (netStake > 0) agg.gamesFinalWithNet += 1;

    // pools joined = distinct final games with any activity
    agg.poolsJoinedFinal += 1;

    // betsCountFinal: approximate by “has traded >0” (we don’t have per-game bet counts here without tracking)
    // If you want exact bet count, we can add a counter in the bets loop per user+game.
    if (g.grossTraded > 0) agg.betsCountFinal += 1;

    agg.favoriteLeague[g.league] = (agg.favoriteLeague[g.league] || 0) + g.grossTraded;

    perUser.set(u, agg);
  }

  const rows: LeaderboardRow[] = users.map((u) => {
    const agg = perUser.get(u) || {
      stakeFinal: 0,
      claimFinal: 0,
      tradedFinal: 0,
      gamesFinalWithNet: 0,
      betsCountFinal: 0,
      poolsJoinedFinal: 0,
      favoriteLeague: {},
    };

    const denom = agg.stakeFinal;
    const roiNet = denom > 0 ? (agg.claimFinal / denom) - 1 : null;

    const fav = Object.entries(agg.favoriteLeague).sort((a, b) => (b[1] - a[1]))[0]?.[0] ?? null;

    return {
      user: u,
      roiNet,
      tradedGross: agg.tradedFinal,
      wonFinal: agg.claimFinal,
      tradesNet: agg.gamesFinalWithNet,
      betsCount: agg.betsCountFinal,
      poolsJoined: agg.poolsJoinedFinal,
      favoriteLeague: fav,
    };
  });

  // Sort output based on requested sort (server-side)
  // Note: sort keys here are based on computed metrics, not userLeagueStats fields.
  const sort = String(params.sort || "ROI").toUpperCase() as LeaderboardSort;
  rows.sort((a, b) => {
    switch (sort) {
      case "TOTAL_STAKED":
        // we don’t return totalStaked; use tradedGross as proxy or add stakeFinal to response if needed
        return b.tradedGross - a.tradedGross;
      case "GROSS_VOLUME":
        return b.tradedGross - a.tradedGross;
      case "LAST_UPDATED":
        // we’re not using lastUpdated in this computed output; keep stable
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
      case "ROI":
      default:
        return (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18);
    }
  });

  const out = { asOf: new Date().toISOString(), rows };
  cacheSet(key, out, 120_000); // 2 minutes
  return out;
}

export async function getUserRecent(params: {
  user: string;
  league: LeagueKey;
  limit: number;
  anchorTs?: number;
  range?: RangeKey; // optional; if omitted, no range filter
}): Promise<{ asOf: string; user: string; recent: RecentBetRow[] }> {
  const user = asLower(params.user);
  const limit = Math.max(1, Math.min(params.limit || 5, 20));

  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const range = params.range ?? "ALL";
  const { start, end } = computeWindow(range, anchorTs);
  const leagues = leagueList(params.league);

  const key = cacheKey({
    v: "lb_recent_v1",
    user,
    league: params.league,
    range,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; user: string; recent: RecentBetRow[] }>(key);
  if (cached) return cached;

  // Pull a reasonable window of data for the user and shape per-game rows.
  // Reuse Q_USERS_NET_BULK with a single user (simple and consistent with leaderboard math).
  const bulk = await subgraphQuery<G_NetBulkResp>(Q_USERS_NET_BULK, {
    users: [user],
    first: 5000,
  });

  const inWindow = (lockTime: number) => lockTime >= start && lockTime <= end;

  const byGame = new Map<string, {
    gameId: string;
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
  }>();

  for (const s of bulk.userGameStats) {
    const lockTime = toNum(s.game.lockTime);
    if (!inWindow(lockTime)) continue;
    if (!leagues.includes(String(s.game.league))) continue;

    const id = s.game.id;
    const cur = byGame.get(id) || {
      gameId: id,
      league: String(s.game.league),
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
    };

    cur.staked = Math.max(cur.staked, toNum(s.stakedDec));
    cur.withdrawn = Math.max(cur.withdrawn, toNum(s.withdrawnDec));
    cur.isFinal = !!s.game.isFinal;

    byGame.set(id, cur);
  }

  for (const b of bulk.bets) {
    const lockTime = toNum(b.game.lockTime);
    if (!inWindow(lockTime)) continue;
    if (!leagues.includes(String(b.game.league))) continue;

    const id = b.game.id;
    const cur = byGame.get(id) || {
      gameId: id,
      league: String(b.game.league),
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
    };

    cur.grossTraded += (b.grossAmount != null ? toNum(b.grossAmount) : toNum(b.amountDec));

    const ts = toNum(b.timestamp);
    if (ts >= cur.lastBetTs) {
      cur.lastBetTs = ts;
      const s = String(b.side || "").toUpperCase();
      cur.lastSide = s === "A" || s === "B" ? (s as "A" | "B") : cur.lastSide;
    }

    cur.isFinal = !!b.game.isFinal;
    byGame.set(id, cur);
  }

  for (const c of bulk.claims) {
    const lockTime = toNum(c.game.lockTime);
    if (!inWindow(lockTime)) continue;
    if (!leagues.includes(String(c.game.league))) continue;

    const id = c.game.id;
    const cur = byGame.get(id) || {
      gameId: id,
      league: String(c.game.league),
      lockTime,
      isFinal: !!c.game.isFinal,
      winnerSide: null,
      teamACode: null,
      teamBCode: null,
      teamAName: null,
      teamBName: null,
      staked: 0,
      withdrawn: 0,
      grossTraded: 0,
      claimTotal: 0,
      lastBetTs: 0,
      lastSide: null,
    };

    cur.claimTotal += toNum(c.amountDec);
    cur.isFinal = !!c.game.isFinal;
    byGame.set(id, cur);
  }

  const recent = Array.from(byGame.values())
    .sort((a, b) => b.lockTime - a.lockTime)
    .slice(0, limit)
    .map((g) => ({
      gameId: g.gameId,
      league: g.league,
      lockTime: g.lockTime,
      isFinal: g.isFinal,
      winnerSide: g.winnerSide ?? null,

      teamACode: g.teamACode ?? null,
      teamBCode: g.teamBCode ?? null,
      teamAName: g.teamAName ?? null,
      teamBName: g.teamBName ?? null,

      side: g.lastSide ?? null,
      netStake: clamp0(g.staked - g.withdrawn),
      grossTraded: g.grossTraded,
      claimTotal: g.claimTotal,
    }));

  const out = { asOf: new Date().toISOString(), user, recent };
  cacheSet(key, out, 60_000);
  return out;
}
