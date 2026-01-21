// src/routes/groupsMetrics.ts
//
// Groups Leaderboard API (backend-computed group metrics)
//
// Endpoints:
//   GET /api/groups/leaderboard?league=ALL&range=D30&limit=200&anchorTs=...
//
// Notes:
// - Rankings are based on ROI within the requested window (default D30).
// - Group membership windows (joined_at/left_at) are enforced by the service layer.
// - This router only parses params + returns JSON; it does not do heavy work.
//

import { Router } from "express";

// We intentionally import the whole module so we can support multiple export names
// without breaking compilation if you renamed the function.
import * as groupMetricsService from "../services/metrics/groupMetrics";

type RangeKey = "ALL" | "D30" | "D90";
type LeagueKey = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

const r = Router();

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
  // keep sane defaults; groups can scale later with pagination
  return Math.max(1, Math.min(Math.floor(n), 500));
}

// Resolve the service function regardless of the exact export name you used.
function resolveGroupsLeaderboardFn(): (args: any) => Promise<any> {
  const svc: any = groupMetricsService as any;

  return (
    svc.getGroupsLeaderboard ||
    svc.getGroupsMetrics ||
    svc.getGroupsLeaderboardRows ||
    svc.buildGroupsLeaderboard ||
    null
  );
}

// GET /api/groups/leaderboard?league=ALL&range=D30&limit=200&anchorTs=...
r.get("/groups/leaderboard", async (req, res, next) => {
  try {
    const league = parseLeague(req.query.league);
    const range = parseRange(req.query.range);
    const limit = parseLimit(req.query.limit);

    const anchorTs =
      req.query.anchorTs != null ? Math.floor(Number(req.query.anchorTs)) : undefined;

    const fn = resolveGroupsLeaderboardFn();
    if (!fn) {
      throw new Error(
        "Groups leaderboard service function not found. Expected export: getGroupsLeaderboard (or similar) from src/services/metrics/groupMetrics.ts"
      );
    }

    // Service should return:
    //   { asOf: string, rows: GroupLBRowApi[] }
    // where GroupLBRowApi includes:
    //   id, slug, name, bio?, membersCount, tradedGross, claimsFinal, roiNet, created_at?, created_by?
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
