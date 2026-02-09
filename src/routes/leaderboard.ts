// src/routes/leaderboard.ts
import { Router } from "express";
import { getLeaderboardUsers, getUserRecent } from "../services/metrics/masterMetrics";
import type { LeaderboardSort } from "../subgraph/queries";

const r = Router();

function parseRange(x: any): "ALL" | "D30" | "D90" {
  const v = String(x || "ALL").toUpperCase();
  if (v === "D30" || v === "D90") return v;
  return "ALL";
}

function parseLeague(x: any): any {
  const v = String(x || "ALL").toUpperCase();
  const allowed = new Set(["ALL", "MLB", "NFL", "NBA", "NHL", "EPL", "UCL"]);
  return allowed.has(v) ? v : "ALL";
}

function parseSort(x: any): LeaderboardSort {
  const v = String(x || "ROI").toUpperCase();
  const allowed = new Set(["ROI", "TOTAL_STAKED", "GROSS_VOLUME", "LAST_UPDATED"]);
  return (allowed.has(v) ? v : "ROI") as LeaderboardSort;
}

// GET /api/leaderboard/users?league=ALL&range=D30&sort=ROI&limit=250&anchorTs=...&user=0xabc...
r.get("/leaderboard/users", async (req, res, next) => {
  try {
    const league = parseLeague(req.query.league);
    const range = parseRange(req.query.range);
    const sort = parseSort(req.query.sort);
    const limit = Math.max(1, Math.min(Number(req.query.limit || 250), 500));
    const anchorTs =
      req.query.anchorTs != null ? Math.floor(Number(req.query.anchorTs)) : undefined;
    
    // ✅ Optional user filter for Profile page single-user stats
    const userFilter = req.query.user
      ? String(req.query.user).toLowerCase().trim()
      : undefined;

    const out = await getLeaderboardUsers({
      league,
      range,
      sort,
      limit,
      anchorTs,
      userFilter,
    });

    res.json({
      league,
      range,
      sort,
      limit,
      anchorTs: anchorTs ?? null,
      userFilter: userFilter ?? null,
      ...out
    });
  } catch (e) {
    next(e);
  }
});

// GET /api/leaderboard/users/:address/recent?league=ALL&limit=5&range=ALL&anchorTs=...&includeLegacy=1
r.get("/leaderboard/users/:address/recent", async (req, res, next) => {
  try {
    const user = String(req.params.address || "").toLowerCase();
    const league = parseLeague(req.query.league);
    const limit = Math.max(1, Math.min(Number(req.query.limit || 5), 20));
    const range = parseRange(req.query.range);
    const anchorTs =
      req.query.anchorTs != null ? Math.floor(Number(req.query.anchorTs)) : undefined;
    
    // ✅ Optional: allow frontend to force legacy inclusion
    const includeLegacy =
      req.query.includeLegacy != null
        ? Number(req.query.includeLegacy) === 1
        : false;

    const out = await getUserRecent({ user, league, limit, range, anchorTs, includeLegacy });
    
    res.json({ league, range, limit, anchorTs: anchorTs ?? null, includeLegacy, ...out });
  } catch (e) {
    next(e);
  }
});

export default r;