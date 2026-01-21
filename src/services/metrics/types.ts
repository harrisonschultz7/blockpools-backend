// src/services/metrics/types.ts
export type RangeKey = "ALL" | "D30" | "D90";
export type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

export type GroupLeaderboardRowApi = {
  id: string;
  slug: string;
  name: string;
  bio?: string | null;
  membersCount: number;

  // same semantics as masterMetrics:
  tradedGross: number; // BUY volume (bets.grossAmount)
  claimsFinal: number; // P/L = claims + sell net proceeds
  roiNet: number | null;

  // analytics / compat
  betsCount: number; // buys + sells count
  tradesNet: number; // games touched
  favoriteLeague?: string | null;

  updatedAt: string;
};

export type GroupMemberRowApi = {
  userAddress: string;
  joinedAt: string;
  leftAt: string | null;

  tradedGross: number;
  claimsFinal: number;
  roiNet: number | null;

  betsCount: number;
  tradesNet: number;
  favoriteLeague?: string | null;
};
