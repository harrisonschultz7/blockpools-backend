// routes/standings.ts
//
// Goalserve standings proxy with Postgres persistence + in-memory TTL cache.
//
// CACHE STRATEGY:
//   Tier 1 — Postgres (league_standings_cache table)
//     • Upserted on every successful Goalserve fetch
//     • Read first on every request — returns instantly if fresh enough
//     • "Fresh" = fetched_at < POSTGRES_STALE_MS ago (default 4 h)
//     • Survives server restarts; background cron keeps it warm
//
//   Tier 2 — In-process memory (MEMORY_TTL_MS, default 5 min)
//     • Deduplicates concurrent frontend polls within the same process
//     • Cleared on restart (fine — standings data isn't millisecond-sensitive)
//
// BACKGROUND CRON:
//   Call `startStandingsCron()` once from your server entry point.
//   It refreshes all configured leagues every CRON_INTERVAL_MS (default 4 h).
//   First run fires 30 s after startup so the server is fully initialised.
//
// LEAGUE IDs (Goalserve soccer — add more as needed):
//   UCL  → 1005   EPL → 1204   La Liga → 1399
//   Serie A → 1269  Bundesliga → 1229  Ligue 1 → 1408
//
// GET /api/standings/:league          e.g. /api/standings/UCL
// GET /api/standings/:league?season=2023-2024   (optional, defaults to current)
//
// Env required:  GOALSERVE_API_KEY
// Env optional:  GOALSERVE_BASE_URL

import { Router, Request, Response } from "express";
import { pool } from "../db";

const router = Router();

// ── Config ───────────────────────────────────────────────────────────────────

const GOALSERVE_API_KEY  = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL = (
  process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed"
).replace(/\/+$/, "");

const FETCH_TIMEOUT_MS   = 14_000;
const MEMORY_TTL_MS      = 5 * 60_000;       // 5 min in-process cache
const POSTGRES_STALE_MS  = 4 * 60 * 60_000;  // 4 h — serve Postgres if fresher than this
const CRON_INTERVAL_MS   = 4 * 60 * 60_000;  // refresh every 4 h
const CRON_STARTUP_DELAY = 30_000;            // wait 30 s after boot before first cron run

// ── League → Goalserve ID mapping ────────────────────────────────────────────

const LEAGUE_IDS: Record<string, string> = {
  UCL:        "1005",
  EPL:        "1204",
  LA_LIGA:    "1399",
  SERIE_A:    "1269",
  BUNDESLIGA: "1229",
  LIGUE_1:    "1408",
  MLS:        "1316",
};

// Leagues to warm in the background cron
const CRON_LEAGUES = ["UCL", "EPL"] as const;

// ── In-memory cache ───────────────────────────────────────────────────────────

interface MemEntry { ts: number; data: NormalisedStandings }
const _mem: Record<string, MemEntry> = {};

function memGet(key: string): NormalisedStandings | null {
  const e = _mem[key];
  if (!e) return null;
  if (Date.now() - e.ts > MEMORY_TTL_MS) { delete _mem[key]; return null; }
  return e.data;
}
function memSet(key: string, data: NormalisedStandings) {
  _mem[key] = { ts: Date.now(), data };
}

// ── Postgres helpers ──────────────────────────────────────────────────────────

async function pgGet(league: string, season: string): Promise<NormalisedStandings | null> {
  try {
    const { rows } = await pool.query<{ standings_data: NormalisedStandings; fetched_at: Date }>(
      `SELECT standings_data, fetched_at
         FROM league_standings_cache
        WHERE league = $1 AND season = $2
        LIMIT 1`,
      [league.toUpperCase(), season]
    );
    if (!rows[0]) return null;
    const ageMs = Date.now() - new Date(rows[0].fetched_at).getTime();
    if (ageMs > POSTGRES_STALE_MS) return null; // stale — re-fetch
    return rows[0].standings_data;
  } catch (e) {
    console.warn("[standings] pgGet error", (e as any)?.message);
    return null;
  }
}

async function pgSet(league: string, season: string, data: NormalisedStandings) {
  try {
    await pool.query(
      `INSERT INTO league_standings_cache (league, season, standings_data, fetched_at)
       VALUES ($1, $2, $3, now())
       ON CONFLICT (league, season)
       DO UPDATE SET standings_data = EXCLUDED.standings_data, fetched_at = now()`,
      [league.toUpperCase(), season, JSON.stringify(data)]
    );
  } catch (e) {
    console.warn("[standings] pgSet error", (e as any)?.message);
  }
}

// ── Normalised data shape ─────────────────────────────────────────────────────

export interface StandingsTeam {
  rank:        number;
  teamId:      string;
  teamName:    string;
  shortName:   string;   // up to 3-char code where available
  played:      number;
  won:         number;
  drawn:       number;
  lost:        number;
  goalsFor:    number;
  goalsAgainst:number;
  goalDiff:    number;
  points:      number;
  form:        string;   // e.g. "WWDLW"  (last 5, newest last)
  group?:      string;   // populated for UCL group stage
  note?:       string;   // e.g. "Champions League", "Relegation"
}

export interface NormalisedStandings {
  league:     string;
  season:     string;
  updatedAt:  string;   // ISO
  // For league-table formats (EPL, La Liga, etc.)
  table?:     StandingsTeam[];
  // For UCL group/knockout phase
  groups?:    { name: string; teams: StandingsTeam[] }[];
  // Phase indicator — "group" | "knockout" | "league"
  phase:      "group" | "knockout" | "league";
  // Raw Goalserve payload kept for debugging / extended use
  _raw?:      any;
}

// ── Goalserve fetch + normalisation ──────────────────────────────────────────

async function fetchWithTimeout(url: string): Promise<any> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    // Goalserve returns JSON when ?json=1 is appended
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

function currentSeason(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = now.getMonth() + 1; // 1-based
  // Soccer seasons typically start in July/August
  return m >= 7 ? `${y}-${y + 1}` : `${y - 1}-${y}`;
}

function safeInt(v: any): number {
  const n = parseInt(String(v ?? "0"), 10);
  return isNaN(n) ? 0 : n;
}

function normaliseTeamRow(t: any, rank: number, group?: string): StandingsTeam {
  // Goalserve soccer standings nest stats under t.overall, points under t.total
  const ov = t?.overall ?? t;
  const played = safeInt(ov?.gp ?? t?.gp ?? t?.played ?? t?.pld ?? t?.mp);
  const won    = safeInt(ov?.w  ?? t?.w  ?? t?.won);
  const drawn  = safeInt(ov?.d  ?? t?.d  ?? t?.drawn ?? t?.draw);
  const lost   = safeInt(ov?.l  ?? t?.l  ?? t?.lost  ?? t?.defeat);
  const gf     = safeInt(ov?.gs ?? ov?.gf ?? t?.gf ?? t?.goals_for    ?? t?.goalsfor);
  const ga     = safeInt(ov?.ga ?? t?.ga  ?? t?.goals_against ?? t?.goalsagainst);

  // Points: t.total.p  OR  t.pts  OR  t.points
  const pts = safeInt(t?.total?.p ?? t?.pts ?? t?.points);

  // Goal diff: t.total.gd  OR  compute from gf - ga
  const gd = t?.total?.gd !== undefined ? safeInt(t.total.gd) : gf - ga;

  // Form: t.recent_form  OR  t.last_6  OR  t.form  OR  t.last5
  let form = String(t?.recent_form ?? t?.last_6 ?? t?.form ?? t?.last5 ?? "")
    .replace(/[^WDLwdl,]/g, "")
    .replace(/,/g, "")
    .toUpperCase()
    .slice(-5);

  const name      = String(t?.name ?? t?.team_name ?? t?.teamname ?? "");
  const shortRaw  = String(t?.short_name ?? t?.abbr ?? t?.code ?? "");
  const shortName = shortRaw || name.slice(0, 3).toUpperCase();

  // Note: t.description.value  OR  t.note  OR  t.status
  const note = String(t?.description?.value ?? t?.note ?? t?.status ?? "");

  return {
    rank,
    teamId:       String(t?.id ?? t?.team_id ?? ""),
    teamName:     name,
    shortName,
    played, won, drawn, lost,
    goalsFor:     gf,
    goalsAgainst: ga,
    goalDiff:     gd,
    points:       pts,
    form,
    ...(group ? { group } : {}),
    note,
  };
}

/**
 * Parse whatever Goalserve returns for a standings endpoint.
 * Goalserve's JSON structure is inconsistent — this handles the common shapes.
 */
function normalise(raw: any, league: string, season: string): NormalisedStandings {
  const base: NormalisedStandings = {
    league,
    season,
    updatedAt: new Date().toISOString(),
    phase: "league",
    _raw: raw,
  };

  // Goalserve wraps everything — dig to the relevant node.
  // Known shapes:
  //   Soccer league:  raw.standings.tournament.team[]
  //   Soccer UCL:     raw.standings.tournament.group[].team[]  (or category.group[])
  //   Other:          raw.standings.category  /  raw.standings.data
  const standings = raw?.standings ?? raw?.standing ?? raw;

  // ── Try tournament path first (EPL, UCL, La Liga etc.) ───────────────────
  const tournament = standings?.tournament;
  if (tournament) {
    // UCL group phase: tournament.group[]
    const groupsRaw: any[] =
      tournament?.group  ? (Array.isArray(tournament.group)  ? tournament.group  : [tournament.group])  :
      tournament?.groups ? (Array.isArray(tournament.groups) ? tournament.groups : [tournament.groups]) :
      [];

    if (groupsRaw.length > 0) {
      base.phase = "group";
      base.groups = groupsRaw.map((g: any) => {
        const teamsRaw: any[] = g?.team ? (Array.isArray(g.team) ? g.team : [g.team]) : [];
        return {
          name:  String(g?.name ?? g?.group_name ?? g?.id ?? ""),
          teams: teamsRaw.map((t, i) => normaliseTeamRow(t, safeInt(t?.position ?? i + 1), g?.name)),
        };
      });
      return base;
    }

    // Standard league table: tournament.team[]
    const teamsRaw: any[] =
      tournament?.team     ? (Array.isArray(tournament.team)     ? tournament.team     : [tournament.team])     :
      tournament?.teams    ? (Array.isArray(tournament.teams)    ? tournament.teams    : [tournament.teams])    :
      tournament?.standing ? (Array.isArray(tournament.standing) ? tournament.standing : [tournament.standing]) :
      [];

    if (teamsRaw.length > 0) {
      base.phase = "league";
      base.table = teamsRaw
        .map((t) => normaliseTeamRow(t, safeInt(t?.position ?? t?.rank ?? 0)))
        .sort((a, b) => a.rank - b.rank);
      return base;
    }
  }

  // ── Fallback: category / data path ───────────────────────────────────────
  const data = standings?.category ?? standings?.data ?? standings;

  const groupsRaw: any[] =
    data?.group   ? (Array.isArray(data.group)   ? data.group   : [data.group])   :
    data?.groups  ? (Array.isArray(data.groups)  ? data.groups  : [data.groups])  :
    [];

  if (groupsRaw.length > 0) {
    base.phase = "group";
    base.groups = groupsRaw.map((g: any) => {
      const teamsRaw: any[] = g?.team ? (Array.isArray(g.team) ? g.team : [g.team]) : [];
      return {
        name:  String(g?.name ?? g?.group_name ?? g?.id ?? ""),
        teams: teamsRaw.map((t, i) => normaliseTeamRow(t, i + 1, g?.name)),
      };
    });
    return base;
  }

  const teamsRaw: any[] =
    data?.team     ? (Array.isArray(data.team)     ? data.team     : [data.team])     :
    data?.teams    ? (Array.isArray(data.teams)    ? data.teams    : [data.teams])    :
    data?.standing ? (Array.isArray(data.standing) ? data.standing : [data.standing]) :
    [];

  if (teamsRaw.length > 0) {
    base.phase = "league";
    base.table = teamsRaw.map((t, i) =>
      normaliseTeamRow(t, safeInt(t?.rank ?? t?.position ?? i + 1))
    );
    return base;
  }

  // Nothing found
  console.warn("[standings] Could not parse standings payload for", league);
  base.table = [];
  return base;
}

// ── Main fetch + cache pipeline ───────────────────────────────────────────────

async function getStandings(league: string, season: string): Promise<NormalisedStandings> {
  const cacheKey = `${league}:${season}`;

  // Tier 2 — memory
  const memHit = memGet(cacheKey);
  if (memHit) return memHit;

  // Tier 1 — Postgres
  const pgHit = await pgGet(league, season);
  if (pgHit) {
    memSet(cacheKey, pgHit);
    return pgHit;
  }

  // Fetch from Goalserve
  const leagueId = LEAGUE_IDS[league.toUpperCase()];
  if (!leagueId) throw new Error(`Unknown league: ${league}`);

  const url =
    `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}` +
    `/standings/${leagueId}?season=${encodeURIComponent(season)}&json=1`;

  const raw  = await fetchWithTimeout(url);
  const data = normalise(raw, league.toUpperCase(), season);

  // Persist
  await pgSet(league, season, data);
  memSet(cacheKey, data);

  return data;
}

// ── Route ─────────────────────────────────────────────────────────────────────

router.get("/:league", async (req: Request, res: Response) => {
  try {
    if (!GOALSERVE_API_KEY) {
      return res.status(503).json({ error: "GOALSERVE_API_KEY not configured" });
    }

    const league = String(req.params.league || "").toUpperCase();
    const season = String(req.query.season || currentSeason());

    if (!LEAGUE_IDS[league]) {
      return res.status(400).json({
        error: `Unknown league: ${league}`,
        knownLeagues: Object.keys(LEAGUE_IDS),
      });
    }

    const data = await getStandings(league, season);
    res.setHeader("Cache-Control", "public, max-age=300, stale-while-revalidate=3600");
    return res.json(data);
  } catch (e: any) {
    console.error("[/api/standings]", e?.message || e);
    return res.status(502).json({ error: e?.message || "Fetch failed" });
  }
});

// ── Background cron ───────────────────────────────────────────────────────────

let _cronTimer: ReturnType<typeof setInterval> | null = null;

export function startStandingsCron() {
  if (_cronTimer) return; // idempotent

  const run = async () => {
    for (const league of CRON_LEAGUES) {
      const season = currentSeason();
      try {
        // Bypass memory cache — force Goalserve fetch
        const cacheKey = `${league}:${season}`;
        delete _mem[cacheKey];

        const leagueId = LEAGUE_IDS[league];
        const url =
          `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}` +
          `/standings/${leagueId}?season=${encodeURIComponent(season)}&json=1`;

        const raw  = await fetchWithTimeout(url);
        const data = normalise(raw, league, season);
        await pgSet(league, season, data);
        memSet(cacheKey, data);
        console.log(`[standings:cron] refreshed ${league} ${season}`);
      } catch (e: any) {
        console.error(`[standings:cron] failed for ${league}:`, e?.message);
      }
      // Small delay between requests to be polite to Goalserve
      await new Promise((r) => setTimeout(r, 2_000));
    }
  };

  // First run 30 s after startup
  setTimeout(() => {
    run().catch(console.error);
    _cronTimer = setInterval(() => run().catch(console.error), CRON_INTERVAL_MS);
  }, CRON_STARTUP_DELAY);

  console.log(`[standings:cron] scheduled — interval ${CRON_INTERVAL_MS / 3_600_000} h`);
}

export default router;