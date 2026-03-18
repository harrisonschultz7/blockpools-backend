// routes/scores.ts
//
// Thin Goalserve proxy — called by LiveScoreTicker.tsx on the frontend.
//
// TWO-TIER CACHE:
//   Tier 1 — Postgres (game_score_cache table)
//     • Written when a game is detected as FINAL
//     • Read first on every request — returns instantly, Goalserve never called again
//     • Survives server restarts and deploys
//
//   Tier 2 — In-process memory (55s TTL)
//     • Used for live/upcoming games to deduplicate concurrent requests
//     • Multiple game cards polling simultaneously → only 1 upstream call per minute
//     • Cleared on restart (fine — live scores need fresh data anyway)
//
// STALE LOCK DETECTION:
//   If lockTime is > 60 days in the past we use today's date instead.
//   This prevents hammering Goalserve for games from prior seasons.
//
// GET /api/scores/live?league=NBA&teamACode=IND&teamBCode=NYK
//                     &teamAName=Indiana+Pacers&teamBName=New+York+Knicks
//                     &lockTime=1773790200
//                     &contractAddress=0xabc...   ← used as Postgres cache key
//
// Env required:  GOALSERVE_API_KEY
// Env optional:  GOALSERVE_BASE_URL

import { Router, Request, Response } from "express";
import { pool } from "../db";

const router = Router();

// ── Config ──────────────────────────────────────────────────────────────────

const GOALSERVE_API_KEY = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL =
  (process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed").replace(/\/+$/, "");

const CACHE_TTL_MS           = 55_000;   // in-memory TTL for live games
const FETCH_TIMEOUT_MS       = 12_000;
const STALE_LOCK_THRESHOLD_SEC = 60 * 86_400; // 60 days

// ── Tier 2: In-memory cache (live/upcoming games only) ──────────────────────

interface MemCacheEntry { ts: number; data: any; }
const _memCache: Record<string, MemCacheEntry> = {};

function memGet(key: string): any | null {
  const e = _memCache[key];
  if (!e) return null;
  if (Date.now() - e.ts > CACHE_TTL_MS) { delete _memCache[key]; return null; }
  return e.data;
}

function memSet(key: string, data: any) {
  _memCache[key] = { ts: Date.now(), data };
}

// ── Tier 1: Postgres cache (final games only) ───────────────────────────────

async function pgCacheGet(contractAddress: string): Promise<any | null> {
  if (!contractAddress) return null;
  try {
    const { rows } = await pool.query(
      `SELECT score_data FROM game_score_cache
       WHERE contract_address = $1 AND is_final = TRUE
       LIMIT 1`,
      [contractAddress.toLowerCase()]
    );
    return rows[0]?.score_data ?? null;
  } catch (e) {
    // Non-fatal — fall through to Goalserve
    console.warn("[scores] pgCacheGet error", (e as any)?.message);
    return null;
  }
}

async function pgCacheSet(
  contractAddress: string,
  league: string,
  data: any,
  isFinal: boolean
) {
  if (!contractAddress) return;
  try {
    await pool.query(
      `INSERT INTO game_score_cache
         (contract_address, league, score_data, is_final, fetched_at)
       VALUES ($1, $2, $3, $4, now())
       ON CONFLICT (contract_address)
       DO UPDATE SET
         score_data = EXCLUDED.score_data,
         is_final   = EXCLUDED.is_final,
         fetched_at = now()`,
      [contractAddress.toLowerCase(), league.toUpperCase(), JSON.stringify(data), isFinal]
    );
  } catch (e) {
    console.warn("[scores] pgCacheSet error", (e as any)?.message);
  }
}

// ── Detect whether a Goalserve payload contains a final result ──────────────
//
// Goalserve uses different status strings per sport:
//   NFL/NBA/NHL/MLB:  "Final", "FT", "F/OT", "F/SO", "F/2OT" etc.
//   Soccer (EPL/UCL): "Finished", "FT", "AET", "AP"
//
// We scan all matches in the payload and look for any that match
// teamAName/teamBName and are in a final state.

const FINAL_STATUSES = new Set([
  "final", "ft", "f/ot", "f/so", "f/2ot", "f/3ot",
  "finished", "aet", "ap", "full time", "ended",
]);

function isFinalStatus(status: string): boolean {
  return FINAL_STATUSES.has(String(status || "").toLowerCase().trim());
}

function extractMatchStatus(
  data: any,
  teamAName: string,
  teamBName: string
): { found: boolean; isFinal: boolean } {
  if (!data || typeof data !== "object") return { found: false, isFinal: false };

  // Collect all matches/events from the payload (structure varies by sport)
  const candidates: any[] = [];

  // NFL / NBA / NHL / MLB shape: data.scores.category.match[] or data.scores.match[]
  const scores = data?.scores ?? data?.score ?? data;
  const categories = scores?.category
    ? Array.isArray(scores.category) ? scores.category : [scores.category]
    : [scores];

  for (const cat of categories) {
    const matches = cat?.match ?? cat?.game ?? cat?.event ?? [];
    const arr = Array.isArray(matches) ? matches : [matches];
    candidates.push(...arr);
  }

  // Soccer shape: data.scores.category.match[] (same but nested under tournament)
  const tournaments = data?.scores?.tournament ?? [];
  const tArr = Array.isArray(tournaments) ? tournaments : [tournaments];
  for (const t of tArr) {
    const ms = t?.match ?? [];
    candidates.push(...(Array.isArray(ms) ? ms : [ms]));
  }

  const aLower = teamAName.toLowerCase();
  const bLower = teamBName.toLowerCase();

  for (const m of candidates) {
    if (!m || typeof m !== "object") continue;

    // Try to identify home/away team names from various Goalserve field names
    const home = String(
      m?.localteam?.name ?? m?.hometeam?.name ?? m?.home_team ?? m?.home ?? ""
    ).toLowerCase();
    const away = String(
      m?.visitorteam?.name ?? m?.awayteam?.name ?? m?.away_team ?? m?.away ?? ""
    ).toLowerCase();

    const matchesGame =
      (home.includes(aLower) || away.includes(aLower) || aLower.includes(home)) &&
      (home.includes(bLower) || away.includes(bLower) || bLower.includes(home));

    if (matchesGame) {
      const status = String(m?.status ?? m?.statuscode ?? m?.state ?? "").toLowerCase().trim();
      return { found: true, isFinal: isFinalStatus(status) };
    }
  }

  return { found: false, isFinal: false };
}

// ── Goalserve URL helpers (unchanged from original) ─────────────────────────

function goalserveLeaguePaths(leagueLabel: string): {
  sportPath: string;
  leaguePaths: string[];
} {
  const L = String(leagueLabel || "").trim().toLowerCase();
  if (L === "nfl") return { sportPath: "football",     leaguePaths: ["nfl-scores"] };
  if (L === "nba") return { sportPath: "bsktbl",       leaguePaths: ["nba-scores"] };
  if (L === "nhl") return { sportPath: "hockey",       leaguePaths: ["nhl-scores"] };
  if (L === "mlb") return { sportPath: "baseball",     leaguePaths: ["mlb-scores"] };
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
    year: "numeric", month: "2-digit", day: "2-digit",
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

  const nowSec = Math.floor(Date.now() / 1000);
  const effectiveLockTime =
    lockTime > 0 && nowSec - lockTime > STALE_LOCK_THRESHOLD_SEC
      ? nowSec
      : lockTime;

  const d0 = epochToEtISO(effectiveLockTime);
  const d1 = addDaysISO(d0, 1);

  const urls: string[] = [];
  for (const iso of [d0, d1]) {
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

// ── Route handler ────────────────────────────────────────────────────────────

router.get("/live", async (req: Request, res: Response) => {
  try {
    const {
      league       = "",
      teamACode    = "",
      teamBCode    = "",
      teamAName    = "",
      teamBName    = "",
      lockTime: lockTimeStr = "0",
      contractAddress = "",
    } = req.query as Record<string, string>;

    if (!league || !teamAName || !teamBName) {
      return res.status(400).json({
        error: "Missing required params: league, teamAName, teamBName",
      });
    }

    if (!GOALSERVE_API_KEY) {
      return res.status(503).json({ error: "GOALSERVE_API_KEY not configured on server" });
    }

    const lockTime = Number(lockTimeStr) || 0;

    // ── Tier 1: Postgres — instant return for finished games ──────────────
    if (contractAddress) {
      const pgHit = await pgCacheGet(contractAddress);
      if (pgHit) {
        res.setHeader("X-Score-Cache", "postgres-final");
        return res.json(pgHit);
      }
    }

    const urls = buildGoalserveUrls(league, lockTime);
    if (!urls.length) {
      return res.status(400).json({ error: `Unsupported league: ${league}` });
    }

    let lastError: string | null = null;

    for (const url of urls) {
      try {
        // ── Tier 2: In-memory cache — deduplicate concurrent live polls ──
        const memHit = memGet(url);
        if (memHit) {
          res.setHeader("X-Score-Cache", "memory-live");
          return res.json(memHit);
        }

        const data = await fetchWithTimeout(url, FETCH_TIMEOUT_MS);

        // ── Determine if this game is final ──────────────────────────────
        const { found, isFinal } = extractMatchStatus(data, teamAName, teamBName);

        if (isFinal && contractAddress) {
          // Write to Postgres — this game will never hit Goalserve again
          void pgCacheSet(contractAddress, league, data, true);
          // Also evict from memory cache so next request hits Postgres
          const memKey = url;
          delete _memCache[memKey];
        } else {
          // Not final — cache in memory only (55s TTL)
          memSet(url, data);

          // If we found the game but it's not final yet, still persist to
          // Postgres as non-final so we have a record (optional but useful)
          if (found && contractAddress) {
            void pgCacheSet(contractAddress, league, data, false);
          }
        }

        res.setHeader("X-Score-Cache", isFinal ? "goalserve-final" : "goalserve-live");
        return res.json(data);
      } catch (e: any) {
        lastError = e?.message || "fetch failed";
        // Try next URL
      }
    }

    return res.status(502).json({ error: `Goalserve fetch failed: ${lastError}` });
  } catch (e: any) {
    console.error("[/api/scores/live]", e?.message || e);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;