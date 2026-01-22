// src/services/metrics/groupMetrics.ts
import type { GroupLeaderboardRowApi, GroupMemberRowApi, LeagueKey, RangeKey } from "./types";
import { cacheGet, cacheKey, cacheSet } from "./cache";
import { aggregateUsersFromBulk, computeWindow, fetchBulkWindowed, leagueList } from "./metricsCore";
import { getGroupBySlug, getGroupMemberIntervals, listGroups } from "../groups/groupRepo";

function tsToSec(ts: string | null | undefined): number {
  if (!ts) return 0;
  const ms = Date.parse(ts);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : 0;
}

function clamp(n: number, lo: number, hi: number) {
  return Math.max(lo, Math.min(hi, n));
}

function chunk<T>(arr: T[], size: number): T[][] {
  const out: T[][] = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

/**
 * Interval convention:
 * - Membership active on [joined_at, left_at)
 *   - joined_at is inclusive
 *   - left_at is exclusive (the moment membership ends)
 *
 * This avoids ambiguity and matches typical "ended at timestamp T" semantics.
 */

// Membership interval check for attributing a game at lockTime
function withinInterval(lockTimeSec: number, joinedAtSec: number, leftAtSec: number | null) {
  if (lockTimeSec < joinedAtSec) return false;
  if (leftAtSec !== null && lockTimeSec >= leftAtSec) return false; // left_at is exclusive
  return true;
}

// "Active member" as-of anchorTs (same interval convention: [join, left))
function activeAt(asOfSec: number, joinedAtSec: number, leftAtSec: number | null) {
  if (joinedAtSec <= 0) return false;
  if (joinedAtSec > asOfSec) return false;
  if (leftAtSec !== null && asOfSec >= leftAtSec) return false;
  return true;
}

function countActiveMembers(
  intervals: Array<{ user: string; joinedAtSec: number; leftAtSec: number | null }>,
  asOfSec: number
) {
  const active = new Set<string>();
  for (const it of intervals) {
    if (activeAt(asOfSec, it.joinedAtSec, it.leftAtSec)) active.add(it.user);
  }
  return active.size;
}

// Build the includeUserGame filter for a given group membership map
function buildGroupIncludeFilter(intervals: Array<{ user: string; joinedAtSec: number; leftAtSec: number | null }>) {
  const byUser = new Map<string, Array<{ joinedAtSec: number; leftAtSec: number | null }>>();
  for (const r of intervals) {
    if (!byUser.has(r.user)) byUser.set(r.user, []);
    byUser.get(r.user)!.push({ joinedAtSec: r.joinedAtSec, leftAtSec: r.leftAtSec });
  }

  // sort intervals for each user
  for (const [u, list] of byUser.entries()) {
    list.sort((a, b) => a.joinedAtSec - b.joinedAtSec);
    byUser.set(u, list);
  }

  return (u: string, _gameId: string, lockTimeSec: number) => {
    const list = byUser.get(u);
    if (!list || !list.length) return false;
    // include if lockTime is in ANY active interval
    for (const it of list) {
      if (withinInterval(lockTimeSec, it.joinedAtSec, it.leftAtSec)) return true;
    }
    return false;
  };
}

/**
 * Realized-only semantics:
 * - ROI denominator and "Traded" should reflect buys for FINAL games only.
 * - This prevents open bets from dragging ROI down and correctly attributes bets
 *   to the month they finalize (still windowed by game.lockTime, but realized gating
 *   is done by game.isFinal).
 */
const REALIZED_ONLY = true;

export async function getGroupsLeaderboard(params: {
  league: LeagueKey;
  range: RangeKey; // D30 for ranking, but supports ALL/D90 too
  limit?: number;
  anchorTs?: number;
}): Promise<{ asOf: string; rows: GroupLeaderboardRowApi[] }> {
  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);
  const limit = clamp(params.limit ?? 200, 1, 500);

  const key = cacheKey({
    v: REALIZED_ONLY ? "groups_lb_v3_realized_only" : "groups_lb_v2_active_members_semantics",
    league: params.league,
    range: params.range,
    limit,
    anchorTs,
  });

  const cached = cacheGet<{ asOf: string; rows: GroupLeaderboardRowApi[] }>(key);
  if (cached) return cached;

  // Load groups
  const groups = await listGroups(limit);
  if (!groups.length) {
    const out = { asOf: new Date().toISOString(), rows: [] as GroupLeaderboardRowApi[] };
    cacheSet(key, out, 30_000);
    return out;
  }

  const leagues = leagueList(params.league);
  const { start, end } = computeWindow(params.range, anchorTs);

  const rows: GroupLeaderboardRowApi[] = [];

  for (const g of groups) {
    const intervalsRaw = await getGroupMemberIntervals(g.id);

    const intervals = intervalsRaw
      .map((m) => ({
        user: String(m.user_address || "").toLowerCase(),
        joinedAtSec: tsToSec(m.joined_at),
        leftAtSec: m.left_at ? tsToSec(m.left_at) : null,
      }))
      .filter((x) => x.user && x.joinedAtSec > 0);

    // Active members as-of anchorTs (THIS is what UI should display)
    const activeMembersCount = countActiveMembers(intervals, anchorTs);

    // Members to consider for metrics: all users with any interval row,
    // because includeUserGame will enforce interval windows per game.
    const members = Array.from(new Set(intervals.map((x) => x.user)));

    if (!members.length) {
      rows.push({
        id: g.id,
        slug: g.slug,
        name: g.name,
        bio: g.bio ?? null,
        membersCount: 0,
        tradedGross: 0,
        claimsFinal: 0,
        roiNet: null,
        betsCount: 0,
        tradesNet: 0,
        favoriteLeague: null,
        updatedAt: new Date().toISOString(),
      });
      continue;
    }

    const memberChunks = chunk(members, 120);
    const includeUserGame = buildGroupIncludeFilter(intervals);

    let sumTraded = 0;
    let sumPnL = 0;
    let sumBets = 0;
    let sumTradesNet = 0;

    for (const batch of memberChunks) {
      const bulk = await fetchBulkWindowed({
        users: batch,
        leagues,
        start,
        end,
        maxTrades: 5000,
        maxClaims: 5000,
      });

      const perUser = aggregateUsersFromBulk({
        users: batch,
        leagues,
        start,
        end,
        bulk,
        realizedOnly: REALIZED_ONLY,
        includeUserGame: (u, gameId, lockTime, _league) => includeUserGame(u, gameId, lockTime),
      });

      for (const u of batch) {
        const m = perUser.get(u);
        if (!m) continue;

        // realized-only "Traded" is tradedRealized; fallback defensively
        const traded = REALIZED_ONLY ? (m.tradedRealized ?? 0) : (m.tradedGross ?? 0);

        sumTraded += traded;
        sumPnL += m.claimsFinal || 0;
        sumBets += m.betsCount || 0;
        sumTradesNet += m.tradesNet || 0;
      }
    }

    const roiNet = sumTraded > 0 ? sumPnL / sumTraded - 1 : null;

    rows.push({
      id: g.id,
      slug: g.slug,
      name: g.name,
      bio: g.bio ?? null,
      membersCount: activeMembersCount, // ACTIVE AS-OF anchorTs
      tradedGross: sumTraded, // now represents realized traded when REALIZED_ONLY=true
      claimsFinal: sumPnL,
      roiNet,
      betsCount: sumBets,
      tradesNet: sumTradesNet,
      favoriteLeague: null,
      updatedAt: new Date().toISOString(),
    });
  }

  // Rank by ROI (desc), null last
  rows.sort((a, b) => (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18));

  const out = { asOf: new Date().toISOString(), rows };
  cacheSet(key, out, 90_000);
  return out;
}

export async function getGroupSummaryBySlug(params: {
  slug: string;
  league: LeagueKey;
  range: RangeKey;
  anchorTs?: number;
}): Promise<{
  asOf: string;
  group: { id: string; slug: string; name: string; bio: string | null };
  tradedGross: number; // now realized traded when REALIZED_ONLY=true
  claimsFinal: number;
  roiNet: number | null;
}> {
  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);

  const key = cacheKey({
    v: REALIZED_ONLY ? "group_summary_v3_realized_only" : "group_summary_v2_active_members_semantics",
    slug: params.slug,
    league: params.league,
    range: params.range,
    anchorTs,
  });

  const cached = cacheGet<any>(key);
  if (cached) return cached;

  const g = await getGroupBySlug(params.slug);
  if (!g) throw new Error("Group not found");

  const leagues = leagueList(params.league);
  const { start, end } = computeWindow(params.range, anchorTs);

  const intervalsRaw = await getGroupMemberIntervals(g.id);
  const intervals = intervalsRaw
    .map((m) => ({
      user: String(m.user_address || "").toLowerCase(),
      joinedAtSec: tsToSec(m.joined_at),
      leftAtSec: m.left_at ? tsToSec(m.left_at) : null,
      joined_at: m.joined_at,
      left_at: m.left_at,
    }))
    .filter((x) => x.user && x.joinedAtSec > 0);

  const members = Array.from(new Set(intervals.map((x) => x.user)));

  if (!members.length) {
    const out = {
      asOf: new Date().toISOString(),
      group: { id: g.id, slug: g.slug, name: g.name, bio: g.bio ?? null },
      tradedGross: 0,
      claimsFinal: 0,
      roiNet: null as number | null,
    };
    cacheSet(key, out, 30_000);
    return out;
  }

  const includeUserGame = buildGroupIncludeFilter(intervals);

  let sumTraded = 0;
  let sumPnL = 0;

  for (const batch of chunk(members, 120)) {
    const bulk = await fetchBulkWindowed({
      users: batch,
      leagues,
      start,
      end,
      maxTrades: 5000,
      maxClaims: 5000,
    });

    const perUser = aggregateUsersFromBulk({
      users: batch,
      leagues,
      start,
      end,
      bulk,
      realizedOnly: REALIZED_ONLY,
      includeUserGame: (u, gameId, lockTime, _league) => includeUserGame(u, gameId, lockTime),
    });

    for (const u of batch) {
      const m = perUser.get(u);
      if (!m) continue;
      const traded = REALIZED_ONLY ? (m.tradedRealized ?? 0) : (m.tradedGross ?? 0);
      sumTraded += traded;
      sumPnL += m.claimsFinal || 0;
    }
  }

  const roiNet = sumTraded > 0 ? sumPnL / sumTraded - 1 : null;

  const out = {
    asOf: new Date().toISOString(),
    group: { id: g.id, slug: g.slug, name: g.name, bio: g.bio ?? null },
    tradedGross: sumTraded, // realized traded when REALIZED_ONLY=true
    claimsFinal: sumPnL,
    roiNet,
  };

  cacheSet(key, out, 60_000);
  return out;
}

export async function getGroupMembersBySlug(params: {
  slug: string;
  league: LeagueKey;
  range: RangeKey;
  anchorTs?: number;
}): Promise<{ asOf: string; rows: GroupMemberRowApi[] }> {
  const anchorTs = params.anchorTs ?? Math.floor(Date.now() / 1000);

  const key = cacheKey({
    v: REALIZED_ONLY ? "group_members_v3_realized_only" : "group_members_v2_active_members_semantics",
    slug: params.slug,
    league: params.league,
    range: params.range,
    anchorTs,
  });

  const cached = cacheGet<any>(key);
  if (cached) return cached;

  const g = await getGroupBySlug(params.slug);
  if (!g) throw new Error("Group not found");

  const leagues = leagueList(params.league);
  const { start, end } = computeWindow(params.range, anchorTs);

  const intervalsRaw = await getGroupMemberIntervals(g.id);
  const intervals = intervalsRaw
    .map((m) => ({
      user: String(m.user_address || "").toLowerCase(),
      joinedAtSec: tsToSec(m.joined_at),
      leftAtSec: m.left_at ? tsToSec(m.left_at) : null,
      joined_at: m.joined_at,
      left_at: m.left_at,
    }))
    .filter((x) => x.user && x.joinedAtSec > 0);

  const members = Array.from(new Set(intervals.map((x) => x.user)));
  if (!members.length) {
    const out = { asOf: new Date().toISOString(), rows: [] as GroupMemberRowApi[] };
    cacheSet(key, out, 30_000);
    return out;
  }

  const includeUserGame = buildGroupIncludeFilter(intervals);

  // compute per user metrics (membership scoped)
  const totals = new Map<
    string,
    { tradedGross: number; claimsFinal: number; roiNet: number | null; betsCount: number; tradesNet: number }
  >();

  for (const batch of chunk(members, 120)) {
    const bulk = await fetchBulkWindowed({
      users: batch,
      leagues,
      start,
      end,
      maxTrades: 5000,
      maxClaims: 5000,
    });

    const perUser = aggregateUsersFromBulk({
      users: batch,
      leagues,
      start,
      end,
      bulk,
      realizedOnly: REALIZED_ONLY,
      includeUserGame: (u, gameId, lockTime, _league) => includeUserGame(u, gameId, lockTime),
    });

    for (const u of batch) {
      const m = perUser.get(u);
      if (!m) continue;

      const traded = REALIZED_ONLY ? (m.tradedRealized ?? 0) : (m.tradedGross ?? 0);

      totals.set(u, {
        tradedGross: traded, // now realized traded when REALIZED_ONLY=true
        claimsFinal: m.claimsFinal || 0,
        roiNet: m.roiNet ?? null,
        betsCount: m.betsCount || 0,
        tradesNet: m.tradesNet || 0,
      });
    }
  }

  // Pick the latest interval for display per user (join/left info shown in table).
  const latestIntervalByUser = new Map<string, { joined_at: string; left_at: string | null }>();
  for (const it of intervals) {
    const cur = latestIntervalByUser.get(it.user);
    if (!cur) {
      latestIntervalByUser.set(it.user, { joined_at: it.joined_at, left_at: it.left_at });
      continue;
    }
    if (tsToSec(it.joined_at) > tsToSec(cur.joined_at)) {
      latestIntervalByUser.set(it.user, { joined_at: it.joined_at, left_at: it.left_at });
    }
  }

  const rows: GroupMemberRowApi[] = members.map((u) => {
    const t = totals.get(u) || { tradedGross: 0, claimsFinal: 0, roiNet: null, betsCount: 0, tradesNet: 0 };
    const interval = latestIntervalByUser.get(u);

    return {
      userAddress: u,
      joinedAt: interval?.joined_at || new Date(0).toISOString(),
      leftAt: interval?.left_at ?? null,
      tradedGross: t.tradedGross, // realized traded when REALIZED_ONLY=true
      claimsFinal: t.claimsFinal,
      roiNet: t.roiNet,
      betsCount: t.betsCount,
      tradesNet: t.tradesNet,
      favoriteLeague: null,
    };
  });

  // Rank by ROI desc, null last
  rows.sort((a, b) => (b.roiNet ?? -1e18) - (a.roiNet ?? -1e18));

  const out = { asOf: new Date().toISOString(), rows };
  cacheSet(key, out, 60_000);
  return out;
}
