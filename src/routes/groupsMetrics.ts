// src/routes/groupsMetrics.ts
//
// Groups Leaderboard API (backend-computed group metrics)
//
// Endpoints:
//   GET /api/groups/_ping
//   GET /api/groups/leaderboard?league=ALL&range=D30&limit=200&anchorTs=...
//
// Notes:
// - Rankings are based on ROI within the requested window (default D30).
// - Group membership windows (joined_at/left_at) are enforced by the service layer.
// - This router only parses params + returns JSON; it does not do heavy work.
//

import { Router, type Request, type Response, type NextFunction } from "express";

// Import the entire module so we can tolerate different export names.
import * as groupMetricsService from "../services/metrics/groupMetrics";

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

const r = Router();

// --- Debug ping (proves router is mounted) ---
r.get("/groups/_ping", (_req: Request, res: Response) => res.json({ ok: true }));

function parseRange(x: any): RangeKey {
  const v = String(x || "D30").toUpperCase();
  if (v === "ALL" || v === "D30" || v === "D90") return v as RangeKey;
  return "D30";
}

function parseLeague(x: any): LeagueKey {
  const v = String(x || "ALL").toUpperCase();
  const allowed = new Set(["ALL", "MLB", "NFL", "NBA", "NHL", "EPL", "UCL"]);
  return (allowed.has(v) ? v : "ALL") as LeagueKey;
}

function parseLimit(x: any): number {
  const n = Number(x);
  if (!Number.isFinite(n)) return 200;
  return Math.max(1, Math.min(Math.floor(n), 500));
}

function parseAnchorTs(x: any): number | undefined {
  if (x == null) return undefined;
  const n = Math.floor(Number(x));
  if (!Number.isFinite(n) || n <= 0) return undefined;
  return n;
}

type GroupsLeaderboardArgs = {
  league: LeagueKey;
  range: RangeKey;
  limit: number;
  anchorTs?: number;
};

// Resolve service function regardless of the exact export name you used.
function resolveGroupsLeaderboardFn(): ((args: GroupsLeaderboardArgs) => Promise<any>) | null {
  const svc: any = groupMetricsService as any;

  const fn =
    svc.getGroupsLeaderboard ||
    svc.getGroupsMetrics ||
    svc.getGroupsLeaderboardRows ||
    svc.buildGroupsLeaderboard ||
    null;

  return typeof fn === "function" ? fn : null;
}

// GET /api/groups/leaderboard?league=ALL&range=D30&limit=200&anchorTs=...
r.get("/groups/leaderboard", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const league = parseLeague(req.query.league);
    const range = parseRange(req.query.range);
    const limit = parseLimit(req.query.limit);
    const anchorTs = parseAnchorTs(req.query.anchorTs);

    const fn = resolveGroupsLeaderboardFn();
    if (!fn) {
      return res.status(500).json({
        error: "Groups leaderboard service function not found",
        detail:
          "Expected one of: getGroupsLeaderboard | getGroupsMetrics | getGroupsLeaderboardRows | buildGroupsLeaderboard in src/services/metrics/groupMetrics.ts",
        exports: Object.keys(groupMetricsService || {}),
      });
    }

    const out = await fn({ league, range, limit, anchorTs });

    res.json({
      league,
      range,
      limit,
      anchorTs: anchorTs ?? null,
      ...out,
    });
  } catch (e) {
    next(e);
  }
});

export default r;
