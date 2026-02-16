// src/subgraph/queries.ts
//
// Canonical query strings aligned to your current schema + backward-compatible exports
// required by existing backend services (cacheRefresh/masterMetrics/etc).
//
// Notes:
// - MULTI support lives on `trades` via outcomeIndex/outcomeCode.
// - Legacy code paths still expect bets/claims/userGameStats queries to exist.
// - TheGraph enforces `first <= 1000`.

export const Q_META_ONLY = `
query MetaOnly {
  _meta { block { number } }
}
`;

/* =========================================================
   Activity-based shortlist (range-aware)
   ========================================================= */

export const Q_ACTIVE_USERS_FROM_TRADES_WINDOW = `
query ActiveUsersFromTradesWindow(
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skip: Int!
) {
  trades(
    first: $first
    skip: $skip
    where: {
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    user { id }
  }
}
`;

export const Q_ACTIVE_USERS_FROM_CLAIMS_WINDOW = `
query ActiveUsersFromClaimsWindow(
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skip: Int!
) {
  claims(
    first: $first
    skip: $skip
    where: {
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    user { id }
  }
}
`;

/* =========================================================
   Leaderboard (UserLeagueStat) - range-agnostic (unchanged)
   ========================================================= */

export const Q_LEADERBOARD_BY_ROI = `
query LeaderboardByRoi($leagues: [String!], $skip: Int!, $first: Int!) {
  _meta { block { number } }

  userLeagueStats(
    first: $first
    skip: $skip
    where: { league_in: $leagues }
    orderBy: roiDec
    orderDirection: desc
  ) {
    user { id }
    league
    roiDec
    betsCount
    lastUpdatedAt
    grossVolumeDec
    totalClaimsDec
    totalPayoutDec
    totalStakedDec
    activePoolsCount
    totalWithdrawnDec
  }
}
`;

export const Q_LEADERBOARD_BY_TOTAL_STAKED = `
query LeaderboardByTotalStaked($leagues: [String!], $skip: Int!, $first: Int!) {
  _meta { block { number } }

  userLeagueStats(
    first: $first
    skip: $skip
    where: { league_in: $leagues }
    orderBy: totalStakedDec
    orderDirection: desc
  ) {
    user { id }
    league
    roiDec
    betsCount
    lastUpdatedAt
    grossVolumeDec
    totalClaimsDec
    totalPayoutDec
    totalStakedDec
    activePoolsCount
    totalWithdrawnDec
  }
}
`;

export const Q_LEADERBOARD_BY_GROSS_VOLUME = `
query LeaderboardByGrossVolume($leagues: [String!], $skip: Int!, $first: Int!) {
  _meta { block { number } }

  userLeagueStats(
    first: $first
    skip: $skip
    where: { league_in: $leagues }
    orderBy: grossVolumeDec
    orderDirection: desc
  ) {
    user { id }
    league
    roiDec
    betsCount
    lastUpdatedAt
    grossVolumeDec
    totalClaimsDec
    totalPayoutDec
    totalStakedDec
    activePoolsCount
    totalWithdrawnDec
  }
}
`;

export const Q_LEADERBOARD_BY_LAST_UPDATED = `
query LeaderboardByLastUpdated($leagues: [String!], $skip: Int!, $first: Int!) {
  _meta { block { number } }

  userLeagueStats(
    first: $first
    skip: $skip
    where: { league_in: $leagues }
    orderBy: lastUpdatedAt
    orderDirection: desc
  ) {
    user { id }
    league
    roiDec
    betsCount
    lastUpdatedAt
    grossVolumeDec
    totalClaimsDec
    totalPayoutDec
    totalStakedDec
    activePoolsCount
    totalWithdrawnDec
  }
}
`;

/* =========================================================
   User summary (legacy-ish) - keep if used anywhere
   ========================================================= */

export const Q_USER_SUMMARY = `
query UserSummary($user: String!, $tradesFirst: Int!, $claimsFirst: Int!, $statsFirst: Int!) {
  _meta { block { number } }

  trades(
    first: $tradesFirst
    orderBy: timestamp
    orderDirection: desc
    where: { user: $user }
  ) {
    id
    type
    timestamp
    txHash

    outcomeIndex
    outcomeCode

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
      marketType
      outcomesCount
      resolutionType
      winningOutcomeIndex
      winnerTeamCode
      teamACode
      teamBCode
      teamAName
      teamBName
    }
  }

  claims(
    first: $claimsFirst
    orderBy: timestamp
    orderDirection: desc
    where: { user: $user }
  ) {
    id
    amountDec
    timestamp
    game {
      id
      league
      lockTime
      isFinal
      resolutionType
      winnerTeamCode
      winningOutcomeIndex
    }
  }

  userGameStats(
    first: $statsFirst
    orderBy: game__lockTime
    orderDirection: desc
    where: { user: $user }
  ) {
    stakedDec
    withdrawnDec
    game {
      id
      league
      lockTime
      isFinal
      marketType
      outcomesCount
      resolutionType
      winningOutcomeIndex
      winnerTeamCode
      teamACode
      teamBCode
      teamAName
      teamBName
    }
  }
}
`;

/* =========================================================
   User activity page - TRADES ONLY (canonical)
   ========================================================= */

export const Q_USER_ACTIVITY_PAGE = `
query UserActivityPage(
  $user: String!
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skipTrades: Int!
) {
  _meta { block { number } }

  trades(
    first: $first
    skip: $skipTrades
    where: {
      user: $user
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    type
    timestamp
    txHash

    outcomeIndex
    outcomeCode

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
      marketType
      outcomesCount
      resolutionType
      winningOutcomeIndex
      winnerTeamCode
      teamACode
      teamBCode
      teamAName
      teamBName
    }
  }
}
`;

/* =========================================================
   Backward-compatible exports (required by current backend)
   - cacheRefresh.ts imports Q_USER_BETS_PAGE + Q_USER_CLAIMS_AND_STATS
   - masterMetrics.ts (legacy recent) imports Q_USER_BETS_WINDOW_PAGE
   ========================================================= */

/**
 * Used by masterMetrics.ts legacy recent endpoint.
 */
export const Q_USER_BETS_WINDOW_PAGE = `
query UserBetsWindowPage(
  $user: String!
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skip: Int!
) {
  _meta { block { number } }

  bets(
    first: $first
    skip: $skip
    where: {
      user: $user
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    amountDec
    grossAmount
    fee
    timestamp
    side
    priceBps
    sharesOut
    sharesOutDec
    game {
      id
      league
      teamACode
      teamBCode
      teamAName
      teamBName
      lockTime
      winnerSide
      winnerTeamCode
      isFinal
    }
  }
}
`;

/**
 * Used by cacheRefresh.ts legacy bets cache.
 */
export const Q_USER_BETS_PAGE = `
query UserBetsPage($user: String!, $first: Int!, $skip: Int!) {
  _meta { block { number } }

  bets(
    first: $first
    skip: $skip
    orderBy: timestamp
    orderDirection: desc
    where: { user: $user }
  ) {
    id
    amountDec
    grossAmount
    fee
    timestamp
    side
    priceBps
    sharesOut
    sharesOutDec
    game {
      id
      league
      teamACode
      teamBCode
      teamAName
      teamBName
      lockTime
      winnerSide
      winnerTeamCode
      isFinal
    }
  }
}
`;

/**
 * Used by cacheRefresh.ts legacy claims/stats cache.
 */
export const Q_USER_CLAIMS_AND_STATS = `
query UserClaimsAndStats($user: String!, $claimsFirst: Int!, $statsFirst: Int!) {
  _meta { block { number } }

  claims(
    first: $claimsFirst
    orderBy: timestamp
    orderDirection: desc
    where: { user: $user }
  ) {
    id
    amountDec
    timestamp
    game {
      id
      league
      lockTime
      isFinal
      winnerSide
      winnerTeamCode
    }
  }

  userGameStats(
    first: $statsFirst
    orderBy: game__lockTime
    orderDirection: desc
    where: { user: $user }
  ) {
    stakedDec
    withdrawnDec
    game {
      id
      league
      lockTime
      isFinal
      winnerSide
      winnerTeamCode
    }
  }
}
`;

/* =========================================================
   Helper: pick leaderboard query by sort
   ========================================================= */

export type LeaderboardSort = "ROI" | "TOTAL_STAKED" | "GROSS_VOLUME" | "LAST_UPDATED";

export function pickLeaderboardQuery(sort: string | undefined) {
  const s = String(sort || "ROI").toUpperCase();
  switch (s) {
    case "TOTAL_STAKED":
      return Q_LEADERBOARD_BY_TOTAL_STAKED;
    case "GROSS_VOLUME":
      return Q_LEADERBOARD_BY_GROSS_VOLUME;
    case "LAST_UPDATED":
      return Q_LEADERBOARD_BY_LAST_UPDATED;
    case "ROI":
    default:
      return Q_LEADERBOARD_BY_ROI;
  }
}
