// src/subgraph/queries.ts
//
// Queries aligned to your schema.graphql:
//
// Entities available:
// - userLeagueStats(where: { league_in: [...] }, orderBy: roiDec|totalStakedDec|grossVolumeDec|lastUpdatedAt|...)
// - bets(where: { user: "..."} orderBy: timestamp)
// - claims(where: { user: "..."} orderBy: timestamp)
// - userGameStats(where: { user: "..."} orderBy: game__lockTime)
//
// No custom fields like `leaderboardRows`, `userBetsPage`, or `user.stats` exist in your schema.

export const Q_META_ONLY = `
query MetaOnly {
  _meta { block { number } }
}
`;

// -----------------------------
// Leaderboard
// -----------------------------
//
// We provide two variants because GraphQL variables cannot be used to choose orderBy field.
// Your server can pick which query string to use based on req.query.sort.
//
// NOTE: "range" is not a schema filter; apply range in your server after fetch using lastUpdatedAt.
//
// Default sort = ROI (desc)
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

// Alternative sort = TOTAL_STAKED (desc) - useful for "volume" style leaderboards
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

// Alternative sort = GROSS_VOLUME (desc)
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

// Alternative sort = LAST_UPDATED (desc)
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
// User summary (dropdown / profile top)
// -----------------------------
//
// Pulls latest bets + latest claims + per-game net stats.
// - Bet/Claim ordering uses `timestamp` (schema) not createdAt.
// - userGameStats ordering uses `game__lockTime` (schema).
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
// User bets (paged)
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
// User claims + stats (not paged)
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
// Bulk net (multiple users)
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
    game { id league lockTime isFinal }
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
    game { id league lockTime isFinal }
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
    game { id league lockTime isFinal winnerSide }
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
