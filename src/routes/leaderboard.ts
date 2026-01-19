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

// GET /api/leaderboard/users?league=ALL&range=D30&sort=ROI&limit=250&anchorTs=...
r.get("/leaderboard/users", async (req, res, next) => {
  try {
    const league = parseLeague(req.query.league);
    const range = parseRange(req.query.range);
    const sort = parseSort(req.query.sort);

    const limit = Math.max(1, Math.min(Number(req.query.limit || 250), 500));
    const anchorTs =
      req.query.anchorTs != null ? Math.floor(Number(req.query.anchorTs)) : undefined;

    const out = await getLeaderboardUsers({ league, range, sort, limit, anchorTs });
    res.json({ league, range, sort, limit, anchorTs: anchorTs ?? null, ...out });
  } catch (e) {
    next(e);
  }
});

// GET /api/leaderboard/users/:address/recent?league=ALL&limit=5&range=ALL&anchorTs=...
r.get("/leaderboard/users/:address/recent", async (req, res, next) => {
  try {
    const user = String(req.params.address || "").toLowerCase();
    const league = parseLeague(req.query.league);
    const limit = Math.max(1, Math.min(Number(req.query.limit || 5), 20));

    const range = parseRange(req.query.range);
    const anchorTs =
      req.query.anchorTs != null ? Math.floor(Number(req.query.anchorTs)) : undefined;

    const out = await getUserRecent({ user, league, limit, range, anchorTs });
    res.json({ league, range, limit, anchorTs: anchorTs ?? null, ...out });
  } catch (e) {
    next(e);
  }
});

export default r;
