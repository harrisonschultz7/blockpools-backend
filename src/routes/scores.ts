// routes/scores.ts
//
// Thin Goalserve proxy — called by LiveScoreTicker.tsx on the frontend.
// Returns the raw Goalserve JSON payload for the requested league + date.
// The normalisation/matching is done client-side (LiveScoreTicker.tsx) using
// the same pipeline as settlement-bot.ts, so this route stays simple.
//
// GET /api/scores/live?league=NFL&teamACode=PHI&teamBCode=KC
//                     &teamAName=Philadelphia+Eagles&teamBName=Kansas+City+Chiefs
//                     &lockTime=1741234567
//
// The route:
//  1. Maps league → Goalserve sportPath + leaguePaths (same logic as settlement-bot.ts)
//  2. Builds URL(s) for lockTime day ET + next day ET
//  3. Fetches from Goalserve (with a short in-memory cache per URL, TTL 55s)
//  4. Tries each URL in order; returns the first payload that isn't empty
//
// Env required:
//   GOALSERVE_API_KEY
// Optional:
//   GOALSERVE_BASE_URL   (default: https://www.goalserve.com/getfeed)

import { Router, Request, Response } from "express";

const router = Router();

// ── Config ─────────────────────────────────────────────────────────────────

const GOALSERVE_API_KEY = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL =
  (process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed").replace(/\/+$/, "");

// Cache TTL slightly under 60s so the client poll window never gets stale data
const CACHE_TTL_MS = 55_000;
const FETCH_TIMEOUT_MS = 12_000;

// ── In-memory cache (survives restart-free for the process lifetime) ────────

interface CacheEntry {
  ts: number;
  data: any;
}

const _cache: Record<string, CacheEntry> = {};

function cacheGet(key: string): any | null {
  const entry = _cache[key];
  if (!entry) return null;
  if (Date.now() - entry.ts > CACHE_TTL_MS) {
    delete _cache[key];
    return null;
  }
  return entry.data;
}

function cacheSet(key: string, data: any) {
  _cache[key] = { ts: Date.now(), data };
}

// ── Helpers — mirrored from settlement-bot.ts ───────────────────────────────

function goalserveLeaguePaths(leagueLabel: string): {
  sportPath: string;
  leaguePaths: string[];
} {
  const L = String(leagueLabel || "").trim().toLowerCase();
  if (L === "nfl") return { sportPath: "football", leaguePaths: ["nfl-scores"] };
  if (L === "nba") return { sportPath: "bsktbl",   leaguePaths: ["nba-scores"] };
  if (L === "nhl") return { sportPath: "hockey",   leaguePaths: ["nhl-scores"] };
  if (L === "mlb") return { sportPath: "baseball", leaguePaths: ["mlb-scores"] };
  if (
    L === "epl" || L === "premier league" ||
    L === "england - premier league" || L === "england premier league"
  ) return { sportPath: "commentaries", leaguePaths: ["1204"] };
  if (L === "ucl" || L === "uefa champions league" || L === "champions league")
    return { sportPath: "commentaries", leaguePaths: ["1005"] };
  return { sportPath: "", leaguePaths: [] };
}

function epochToEtISO(epochSec: number): string {
  const dt = new Date(epochSec * 1000);
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts: Record<string, string> = {};
  for (const p of fmt.formatToParts(dt)) parts[p.type] = p.value;
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function addDaysISO(iso: string, days: number): string {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return [
    dt.getUTCFullYear(),
    String(dt.getUTCMonth() + 1).padStart(2, "0"),
    String(dt.getUTCDate()).padStart(2, "0"),
  ].join("-");
}

function buildGoalserveUrls(league: string, lockTime: number): string[] {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  if (!sportPath || !leaguePaths.length) return [];

  const d0 = epochToEtISO(lockTime);
  // Also check the next day for late-night games that roll past midnight ET
  const d1 = addDaysISO(d0, 1);
  const dates = [d0, d1];

  const urls: string[] = [];
  for (const iso of dates) {
    const [Y, M, D] = iso.split("-");
    const ddmmyyyy = `${D}.${M}.${Y}`;
    for (const lp of leaguePaths) {
      urls.push(
        `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}` +
        `/${sportPath}/${lp}?date=${encodeURIComponent(ddmmyyyy)}&json=1`
      );
    }
  }
  return urls;
}

async function fetchWithTimeout(url: string, timeoutMs: number): Promise<any> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), timeoutMs);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

// ── Route handler ───────────────────────────────────────────────────────────

router.get("/live", async (req: Request, res: Response) => {
  try {
    const {
      league = "",
      teamACode = "",
      teamBCode = "",
      teamAName = "",
      teamBName = "",
      lockTime: lockTimeStr = "0",
    } = req.query as Record<string, string>;

    if (!league || !teamAName || !teamBName) {
      return res.status(400).json({ error: "Missing required params: league, teamAName, teamBName" });
    }

    if (!GOALSERVE_API_KEY) {
      return res.status(503).json({ error: "GOALSERVE_API_KEY not configured on server" });
    }

    const lockTime = Number(lockTimeStr) || 0;
    const urls = buildGoalserveUrls(league, lockTime);

    if (!urls.length) {
      return res.status(400).json({ error: `Unsupported league: ${league}` });
    }

    let lastError: string | null = null;

    for (const url of urls) {
      try {
        // Check cache first
        const cached = cacheGet(url);
        if (cached) {
          return res.json(cached);
        }

        const data = await fetchWithTimeout(url, FETCH_TIMEOUT_MS);
        cacheSet(url, data);

        // Return on first successful fetch — client will do team matching
        return res.json(data);
      } catch (e: any) {
        lastError = e?.message || "fetch failed";
        // Try next URL
      }
    }

    // All URLs failed
    return res.status(502).json({ error: `Goalserve fetch failed: ${lastError}` });
  } catch (e: any) {
    console.error("[/api/scores/live]", e?.message || e);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;