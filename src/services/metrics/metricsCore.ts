// src/services/metrics/metricsCore.ts
import { subgraphQuery } from "../../subgraph/client";
import type { LeagueKey, RangeKey } from "./types";

const GQL_MAX_FIRST = 1000;

function asLower(a: string) {
  return String(a || "").toLowerCase();
}

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function safeLeague(v: any): string {
  return String(v || "").toUpperCase();
}

export function computeWindow(range: RangeKey, anchorTs: number) {
  if (range === "ALL") return { start: 0, end: anchorTs };
  const days = range === "D30" ? 30 : 90;
  return { start: anchorTs - days * 86400, end: anchorTs };
}

export function leagueList(league: LeagueKey): string[] {
  if (league === "ALL") return ["MLB", "NFL", "NBA", "NHL", "EPL", "UCL"];
  return [league];
}

// ---------------------------
// Subgraph query (copied semantics from master)
// Window is keyed by GAME.lockTime
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

type G_UserGameStat = {
  user: { id: string };
  stakedDec: string;
  withdrawnDec: string;
  game: {
    id: string;
    league: string;
    lockTime: string;
    isFinal: boolean;
  };
};

type G_Trade = {
  id: string;
  user: { id: string };
  league: string;
  type: "BUY" | "SELL";
  timestamp: string;
  grossOutDec: string;
  netOutDec: string;
  costBasisClosedDec: string;
  realizedPnlDec: string;
  game: { id: string; league: string; lockTime: string; isFinal: boolean };
};

type G_Claim = {
  id: string;
  user: { id: string };
  amountDec: string;
  timestamp: string;
  game: { id: string; league: string; lockTime: string; isFinal: boolean };
};

type G_Bet = {
  id: string;
  user: { id: string };
  grossAmount: string;
  amountDec: string;
  timestamp: string;
  game: { id: string; league: string; lockTime: string; isFinal: boolean };
};

type G_BulkPageResp = {
  userGameStats: G_UserGameStat[];
  trades: G_Trade[];
  claims: G_Claim[];
  bets: G_Bet[];
};

export async function fetchBulkWindowed(params: {
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
  const maxBets = maxTrades;

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

  // bets pagination
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

export type UserMetricsAgg = {
  tradedGross: number; // BUY only
  claimsFinal: number; // P/L = claims + sell proceeds
  roiNet: number | null;
  betsCount: number; // buys + sells
  tradesNet: number; // games touched
  favoriteLeague?: string | null;

  // optional
  sellsNet?: number;
  sellsPnl?: number;
  sellsRoi?: number | null;
};

type UserGameAgg = {
  league: string;
  lockTime: number;
  isFinal: boolean;

  buyGross: number;
  buyCount: number;

  sellNet: number;
  sellCostClosed: number;
  sellPnl: number;
  sellCount: number;

  claimTotal: number;
};

export function aggregateUsersFromBulk(params: {
  users: string[];
  leagues: string[];
  start: number;
  end: number;
  bulk: Awaited<ReturnType<typeof fetchBulkWindowed>>;
  // optional: additional filter at user-game level
  // return true to include this user-game aggregate
  includeUserGame?: (u: string, gameId: string, lockTime: number, league: string) => boolean;
}): Map<string, UserMetricsAgg> {
  const { users, leagues, start, end, bulk } = params;

  const inLockWindow = (lockTime: number) => lockTime >= start && lockTime <= end;

  const byUserGame = new Map<string, UserGameAgg>();

  // Seed from stats (ensures game meta exists)
  for (const s of bulk.userGameStats || []) {
    const u = asLower(s.user.id);
    const lockTime = toNum(s.game.lockTime);
    const gLeague = safeLeague(s.game.league);
    if (!inLockWindow(lockTime)) continue;
    if (!leagues.includes(gLeague)) continue;

    const gid = String(s.game.id || "").toLowerCase();
    const k = `${u}|${gid}`;

    byUserGame.set(
      k,
      byUserGame.get(k) || {
        league: gLeague,
        lockTime,
        isFinal: !!s.game.isFinal,

        buyGross: 0,
        buyCount: 0,

        sellNet: 0,
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,

        claimTotal: 0,
      }
    );
  }

  // Trades (SELL only)
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
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,
        claimTotal: 0,
      } as UserGameAgg);

    cur.sellNet += toNum(t.netOutDec);
    cur.sellCostClosed += toNum(t.costBasisClosedDec);
    cur.sellPnl += toNum(t.realizedPnlDec);
    cur.sellCount += 1;

    byUserGame.set(k, cur);
  }

  // Bets (Legacy + AMM buys)
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
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,
        claimTotal: 0,
      } as UserGameAgg);

    cur.buyGross += toNum(b.grossAmount);
    cur.buyCount += 1;

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
        sellCostClosed: 0,
        sellPnl: 0,
        sellCount: 0,
        claimTotal: 0,
      } as UserGameAgg);

    cur.claimTotal += toNum(c.amountDec);

    byUserGame.set(k, cur);
  }

  // Roll up per-user (same as master semantics)
  type UserAgg = {
    buyGross: number;
    sellNet: number;
    claimTotal: number;

    sellPnl: number;
    sellCost: number;

    tradesCount: number;
    gamesTouched: number;

    favLeagueVolume: Record<string, number>;
  };

  const perUser = new Map<string, UserAgg>();

  for (const [k, g] of byUserGame.entries()) {
    const [u, gid] = k.split("|");
    const include = params.includeUserGame
      ? params.includeUserGame(u, gid, g.lockTime, g.league)
      : true;

    if (!include) continue;

    const hasAny = g.buyGross > 0 || g.sellNet > 0 || g.claimTotal > 0;
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

    agg.buyGross += g.buyGross;
    agg.sellNet += g.sellNet;
    agg.claimTotal += g.claimTotal;

    agg.sellPnl += g.sellPnl;
    agg.sellCost += g.sellCostClosed;

    agg.tradesCount += g.buyCount + g.sellCount;
    agg.gamesTouched += 1;

    agg.favLeagueVolume[g.league] = (agg.favLeagueVolume[g.league] || 0) + g.buyGross;

    perUser.set(u, agg);
  }

  const out = new Map<string, UserMetricsAgg>();

  for (const u of users) {
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

    out.set(u, {
      tradedGross: totalBuy,
      claimsFinal: pnl,
      roiNet,
      betsCount: agg.tradesCount,
      tradesNet: agg.gamesTouched,
      favoriteLeague: fav,
      sellsNet: agg.sellNet,
      sellsPnl: agg.sellPnl,
      sellsRoi,
    });
  }

  return out;
}
