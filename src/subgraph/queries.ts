// src/subgraph/queries.ts
//
// Queries aligned to your schema.graphql.
//
// Entities you actively use for leaderboard accuracy (range-aware):
// - trades (BUY/SELL)
// - claims
// - userGameStats
//
// Notes:
// - TheGraph enforces `first` <= 1000.
// - For D30/D90 correctness, shortlist users from *activity* (trades) within the window.
// - userLeagueStats is useful for ALL-time / coarse ranking, but it is NOT range-aware.

export const Q_META_ONLY = `
query MetaOnly {
  _meta { block { number } }
}
`;

// -----------------------------
// Activity-based shortlist (recommended)
// -----------------------------
//
// These queries are used to obtain a candidate set of users who were active within a window.
// They are range-aware via game.lockTime filters (anchor window applied server-side).
//
// Typical server strategy:
// - Pull pages until you collect ~limit unique users (or a cap).
// - Then compute per-user metrics from bulk trades/claims/stats.

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

export const Q_ACTIVE_USERS_FROM_BETS_WINDOW = `
query ActiveUsersFromBetsWindow(
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skip: Int!
) {
  bets(
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


// Optional: if you want to ensure users who only CLAIM (but have no trades in window)
// are included, you can union in candidates from claims.
// Many products skip this because "trader leaderboard" should reflect trading activity,
// but this is here if you want completeness.

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

// -----------------------------
// Leaderboard (UserLeagueStat) - range-agnostic
// -----------------------------
//
// We keep these because they can still be useful for ALL-time or as a fallback.
// Your server can select which query string to use based on req.query.sort.

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

// -----------------------------
// User summary (legacy: bets + claims + stats)
// -----------------------------
//
// Keep this for any pages still using bets-based history.
// Your new leaderboard dropdown should use the trades-based recent endpoint instead.

export const Q_USER_SUMMARY = `
query UserSummary($user: String!, $betsFirst: Int!, $claimsFirst: Int!, $statsFirst: Int!) {
  _meta { block { number } }

  bets(
    first: $betsFirst
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
    game {
      id
      league
      teamACode
      teamBCode
      teamAName
      teamBName
      lockTime
      winnerSide
      isFinal
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
      winnerSide
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
      teamACode
      teamBCode
      teamAName
      teamBName
    }
  }
}
`;

// -----------------------------
// User trades (paged) - new
// -----------------------------
//
// If you want a generic trades page query (outside of masterMetrics.ts),
// this is a clean canonical version.

export const Q_USER_ACTIVITY_PAGE = `
query UserActivityPage(
  $user: String!
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skipTrades: Int!
  $skipBets: Int!
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
    game { id league lockTime isFinal winnerSide winnerTeamCode teamACode teamBCode teamAName teamBName }
  }

  bets(
    first: $first
    skip: $skipBets
    where: {
      user: $user
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    timestamp
    side
    amountDec
    grossAmount
    fee
    priceBps
    sharesOutDec
    game { id league lockTime isFinal winnerSide winnerTeamCode teamACode teamBCode teamAName teamBName }
  }
}
`;

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
      isFinal
    }
  }
}
`;


// -----------------------------
// User bets (paged) - legacy
// -----------------------------
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
      isFinal
    }
  }
}
`;

// -----------------------------
// User claims + stats (legacy)
// -----------------------------
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
    }
  }
}
`;

// -----------------------------
// Bulk net (multiple users) - legacy bets-based v1/v2
// -----------------------------
export const Q_USERS_NET_BULK = `
query UsersNetBulk($users: [String!]!, $first: Int!) {
  _meta { block { number } }

  userGameStats(
    first: $first
    where: { user_in: $users }
    orderBy: game__lockTime
    orderDirection: desc
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
    where: { user_in: $users }
    orderBy: timestamp
    orderDirection: desc
  ) {
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

  bets(
    first: $first
    where: { user_in: $users }
    orderBy: timestamp
    orderDirection: desc
  ) {
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

export const Q_USERS_NET_BULK_V2 = `
query UsersNetBulkV2($users: [String!]!, $statsFirst: Int!, $claimsFirst: Int!, $betsFirst: Int!) {
  _meta { block { number } }

  userGameStats(
    first: $statsFirst
    where: { user_in: $users }
    orderBy: game__lockTime
    orderDirection: desc
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
    first: $claimsFirst
    where: { user_in: $users }
    orderBy: timestamp
    orderDirection: desc
  ) {
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

  bets(
    first: $betsFirst
    where: { user_in: $users }
    orderBy: timestamp
    orderDirection: desc
  ) {
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



// -----------------------------
// Helper: pick leaderboard query by sort
// -----------------------------
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
