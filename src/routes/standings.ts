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
// LEAGUE IDs:
//   Soccer:  UCL → 1005   EPL → 1204   La Liga → 1399
//            Serie A → 1269  Bundesliga → 1229  Ligue 1 → 1408
//   Basketball: NBA → 1046  (endpoint: /bsktbl/{id}_table)
//   Hockey:     NHL → 1007  (endpoint: /hockey/{id}_table)
//   Baseball:   MLB         (endpoint: /baseball/mlb_standings)
//
// GET /api/standings/:league          e.g. /api/standings/UCL
// GET /api/standings/:league?season=2023-2024   (optional, soccer only)
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

// ── League config ─────────────────────────────────────────────────────────────

// Soccer leagues — use /standings/{id}?season=...
const SOCCER_LEAGUE_IDS: Record<string, string> = {
  UCL:        "1005",
  EPL:        "1204",
  LA_LIGA:    "1399",
  SERIE_A:    "1269",
  BUNDESLIGA: "1229",
  LIGUE_1:    "1408",
  MLS:        "1316",
};

// NA sport leagues — use /{sport}/{id}_table (no season param)
const NA_LEAGUE_CONFIG: Record<string, { sport: string; id: string }> = {
  NBA: { sport: "bsktbl", id: "1046" },
  NHL: { sport: "hockey", id: "1007" },
};

// MLB uses its own dedicated endpoint
const MLB_STANDINGS_URL = () =>
  `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/baseball/mlb_standings?json=1`;

// MLB season is just the calendar year (e.g. "2026")
function mlbSeason(): string {
  return String(new Date().getFullYear());
}

const ALL_KNOWN_LEAGUES = new Set([
  ...Object.keys(SOCCER_LEAGUE_IDS),
  ...Object.keys(NA_LEAGUE_CONFIG),
  "MLB",
]);

// Leagues warmed by background cron
const CRON_SOCCER_LEAGUES = ["UCL", "EPL"] as const;
const CRON_NA_LEAGUES     = ["NBA", "NHL"] as const;
const CRON_MLB            = true;

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
    if (ageMs > POSTGRES_STALE_MS) return null;
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
  rank:         number;
  teamId:       string;
  teamName:     string;
  shortName:    string;
  played:       number;
  won:          number;
  drawn:        number;
  lost:         number;
  goalsFor:     number;
  goalsAgainst: number;
  goalDiff:     number;
  points:       number;
  form:         string;   // "WWDLW" — last 5, newest last
  group?:       string;
  note?:        string;
}

// NA (NBA/NHL) conference team
export interface NATeamStanding {
  rank:    number;
  teamId:  string;
  name:    string;
  w:       number;
  l:       number;
  otl?:    number;   // NHL only
  pts?:    number;   // NHL only
  pct:     number;
  streak:  string;   // e.g. "W3" or "L2"
  playoff: boolean;
}

export interface NAConference {
  name:  string;
  teams: NATeamStanding[];
}

// MLB division standing
export interface MLBTeamStanding {
  rank:     number;
  teamId:   string;
  name:     string;
  shortName: string;
  w:        number;
  l:        number;
  pct:      number;
  gb:       string;   // games behind, "0" for leader
  streak:   string;   // e.g. "W3" or "L2"
  home:     string;   // "30-11"
  away:     string;   // "25-16"
  last10:   string;   // "7-3"
}

export interface MLBDivision {
  name:  string;
  teams: MLBTeamStanding[];
}

export interface MLBLeague {
  name:      string;   // "American League" | "National League"
  divisions: MLBDivision[];
}

export interface NormalisedStandings {
  league:    string;
  season:    string;
  updatedAt: string;
  // Soccer
  table?:    StandingsTeam[];
  groups?:   { name: string; teams: StandingsTeam[] }[];
  phase:     "group" | "knockout" | "league" | "conference" | "division";
  // NBA / NHL
  conferences?: NAConference[];
  // MLB
  mlbLeagues?: MLBLeague[];
  _raw?: any;
}

// ── Goalserve fetch ───────────────────────────────────────────────────────────

async function fetchWithTimeout(url: string): Promise<any> {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(url, { signal: ctrl.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(t);
  }
}

function currentSeason(): string {
  const now = new Date();
  const y = now.getFullYear();
  const m = now.getMonth() + 1;
  return m >= 7 ? `${y}-${y + 1}` : `${y - 1}-${y}`;
}

function safeInt(v: any): number {
  const n = parseInt(String(v ?? "0"), 10);
  return isNaN(n) ? 0 : n;
}

// ── Soccer normalisation (unchanged) ─────────────────────────────────────────

function normaliseTeamRow(t: any, rank: number, group?: string): StandingsTeam {
  const ov     = t?.overall ?? t;
  const played = safeInt(ov?.gp ?? t?.gp ?? t?.played ?? t?.pld ?? t?.mp);
  const won    = safeInt(ov?.w  ?? t?.w  ?? t?.won);
  const drawn  = safeInt(ov?.d  ?? t?.d  ?? t?.drawn ?? t?.draw);
  const lost   = safeInt(ov?.l  ?? t?.l  ?? t?.lost  ?? t?.defeat);
  const gf     = safeInt(ov?.gs ?? ov?.gf ?? t?.gf ?? t?.goals_for    ?? t?.goalsfor);
  const ga     = safeInt(ov?.ga ?? t?.ga  ?? t?.goals_against ?? t?.goalsagainst);
  const pts    = safeInt(t?.total?.p ?? t?.pts ?? t?.points);
  const gd     = t?.total?.gd !== undefined ? safeInt(t.total.gd) : gf - ga;
  let form     = String(t?.recent_form ?? t?.last_6 ?? t?.form ?? t?.last5 ?? "")
    .replace(/[^WDLwdl,]/g, "").replace(/,/g, "").toUpperCase().slice(-5);
  const name      = String(t?.name ?? t?.team_name ?? t?.teamname ?? "");
  const shortRaw  = String(t?.short_name ?? t?.abbr ?? t?.code ?? "");
  const shortName = shortRaw || name.slice(0, 3).toUpperCase();
  const note      = String(t?.description?.value ?? t?.note ?? t?.status ?? "");
  return {
    rank, teamId: String(t?.id ?? t?.team_id ?? ""), teamName: name, shortName,
    played, won, drawn, lost, goalsFor: gf, goalsAgainst: ga, goalDiff: gd,
    points: pts, form, ...(group ? { group } : {}), note,
  };
}

function normaliseSoccer(raw: any, league: string, season: string): NormalisedStandings {
  const base: NormalisedStandings = {
    league, season, updatedAt: new Date().toISOString(), phase: "league", _raw: raw,
  };
  const standings  = raw?.standings ?? raw?.standing ?? raw;
  const tournament = standings?.tournament;

  if (tournament) {
    const groupsRaw: any[] =
      tournament?.group  ? (Array.isArray(tournament.group)  ? tournament.group  : [tournament.group])  :
      tournament?.groups ? (Array.isArray(tournament.groups) ? tournament.groups : [tournament.groups]) : [];

    if (groupsRaw.length > 0) {
      base.phase  = "group";
      base.groups = groupsRaw.map((g: any) => {
        const teamsRaw: any[] = g?.team ? (Array.isArray(g.team) ? g.team : [g.team]) : [];
        return {
          name:  String(g?.name ?? g?.group_name ?? g?.id ?? ""),
          teams: teamsRaw.map((t, i) => normaliseTeamRow(t, safeInt(t?.position ?? i + 1), g?.name)),
        };
      });
      return base;
    }

    const teamsRaw: any[] =
      tournament?.team     ? (Array.isArray(tournament.team)     ? tournament.team     : [tournament.team])     :
      tournament?.teams    ? (Array.isArray(tournament.teams)    ? tournament.teams    : [tournament.teams])    :
      tournament?.standing ? (Array.isArray(tournament.standing) ? tournament.standing : [tournament.standing]) : [];

    if (teamsRaw.length > 0) {
      base.phase = "league";
      base.table = teamsRaw.map((t) => normaliseTeamRow(t, safeInt(t?.position ?? t?.rank ?? 0))).sort((a, b) => a.rank - b.rank);
      return base;
    }
  }

  const data      = standings?.category ?? standings?.data ?? standings;
  const groupsRaw: any[] =
    data?.group  ? (Array.isArray(data.group)  ? data.group  : [data.group])  :
    data?.groups ? (Array.isArray(data.groups) ? data.groups : [data.groups]) : [];

  if (groupsRaw.length > 0) {
    base.phase  = "group";
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
    data?.standing ? (Array.isArray(data.standing) ? data.standing : [data.standing]) : [];

  if (teamsRaw.length > 0) {
    base.phase = "league";
    base.table = teamsRaw.map((t, i) => normaliseTeamRow(t, safeInt(t?.rank ?? t?.position ?? i + 1)));
    return base;
  }

  console.warn("[standings] Could not parse soccer payload for", league);
  base.table = [];
  return base;
}

// ── NA normalisation (NBA / NHL) ──────────────────────────────────────────────

// Converts Goalserve recent_form "WWLWW" → streak string "W2" / "L1"
function formToStreak(form: string): string {
  if (!form) return "W0";
  const chars = form.replace(/[^WLwl]/g, "").toUpperCase();
  if (!chars.length) return "W0";
  const last = chars[chars.length - 1];
  let count = 0;
  for (let i = chars.length - 1; i >= 0; i--) {
    if (chars[i] === last) count++;
    else break;
  }
  return `${last}${count}`;
}

function extractConferences(raw: any): any[] {
  // Shape: raw.standings.category[] → each has .league[] → each has .name + .team[]
  const s   = raw?.standings;
  const cat = s?.category;
  if (!cat) return [];
  const cats = Array.isArray(cat) ? cat : [cat];
  const results: any[] = [];
  for (const c of cats) {
    const l = c?.league;
    if (!l) continue;
    const leagues = Array.isArray(l) ? l : [l];
    results.push(...leagues);
  }
  return results;
}

function normaliseNA(raw: any, league: string, isHockey: boolean): NormalisedStandings {
  const leagues = extractConferences(raw);
  const conferences: NAConference[] = [];

  for (const lg of leagues) {
    const confName = String(lg?.name ?? "Conference");
    const teamsRaw = Array.isArray(lg?.team) ? lg.team : lg?.team ? [lg.team] : [];

    const teams: NATeamStanding[] = teamsRaw.map((t: any) => {
      const w   = safeInt(t.w);
      const l   = safeInt(t.l);
      const otl = isHockey ? safeInt(t.lo ?? t.otl ?? 0) : undefined;
      const pts = isHockey ? safeInt(t.pts) : undefined;
      const gp  = safeInt(t.gp) || (w + l + (otl ?? 0));
      const pct = isHockey
        ? (gp > 0 && pts !== undefined ? pts / (gp * 2) : 0)
        : (gp > 0 ? w / gp : 0);
      const isPlayoff = String(t?.description?.value ?? "").includes("Play Offs");

      return {
        rank:    safeInt(t.pos),
        teamId:  String(t.id ?? ""),
        name:    String(t.name ?? ""),
        w, l,
        ...(isHockey ? { otl, pts } : {}),
        pct:     Math.round(pct * 1000) / 1000,
        streak:  formToStreak(String(t.recent_form ?? "")),
        playoff: isPlayoff,
      };
    }).sort((a: NATeamStanding, b: NATeamStanding) => a.rank - b.rank);

    if (teams.length > 0) conferences.push({ name: confName, teams });
  }

  // Sort East before West for display consistency
  conferences.sort((a, b) => {
    const order = (n: string) => n.toLowerCase().includes("east") ? 0 : 1;
    return order(a.name) - order(b.name);
  });

  return {
    league:      league.toUpperCase(),
    season:      currentSeason(),
    updatedAt:   new Date().toISOString(),
    phase:       "conference",
    conferences,
  };
}

// ── MLB normalisation ─────────────────────────────────────────────────────────
//
// Goalserve /baseball/mlb_standings shape:
//   standings.category[] → each is a league (AL / NL)
//     .league[] → each is a division
//       .team[] → each is a team row

function normaliseMLB(raw: any): NormalisedStandings {
  const season = mlbSeason();
  const cat = raw?.standings?.category;
  const categories = cat ? (Array.isArray(cat) ? cat : [cat]) : [];

  const mlbLeagues: MLBLeague[] = [];

  for (const league of categories) {
    const leagueName = String(league?.name ?? league?.["@name"] ?? "");
    const divRaw = league?.league;
    if (!divRaw) continue;
    const divArr = Array.isArray(divRaw) ? divRaw : [divRaw];

    const divisions: MLBDivision[] = [];

    for (const div of divArr) {
      const divName = String(div?.name ?? div?.["@name"] ?? "");
      const teamsRaw = div?.team ? (Array.isArray(div.team) ? div.team : [div.team]) : [];

      const teams: MLBTeamStanding[] = teamsRaw.map((t: any, i: number) => {
        const w   = safeInt(t?.w ?? t?.["@w"]);
        const l   = safeInt(t?.l ?? t?.["@l"]);
        const gp  = w + l;
        const pct = gp > 0 ? Math.round((w / gp) * 1000) / 1000 : 0;
        const gb  = String(t?.gb ?? t?.["@gb"] ?? "-").trim();

        // Streak: Goalserve gives "streak_type" (W/L) + "streak_total"
        const sType  = String(t?.streak_type  ?? t?.["@streak_type"]  ?? "W").toUpperCase();
        const sTotal = safeInt(t?.streak_total ?? t?.["@streak_total"] ?? 0);
        const streak = `${sType}${sTotal}`;

        // Record splits
        const home   = String(t?.home   ?? t?.["@home"]   ?? "");
        const away   = String(t?.away   ?? t?.["@away"]   ?? "");
        const last10 = String(t?.last10 ?? t?.["@last10"] ?? t?.l10 ?? "");

        const name      = String(t?.name     ?? t?.["@name"]      ?? "");
        const shortName = String(t?.short_name ?? t?.["@short_name"] ?? t?.abbr ?? name.slice(0, 3).toUpperCase());

        return {
          rank:  safeInt(t?.pos ?? t?.["@pos"] ?? i + 1),
          teamId: String(t?.id ?? t?.["@id"] ?? ""),
          name,
          shortName,
          w, l, pct, gb, streak, home, away, last10,
        };
      }).sort((a: MLBTeamStanding, b: MLBTeamStanding) => a.rank - b.rank);

      if (teams.length) divisions.push({ name: divName, teams });
    }

    if (divisions.length) mlbLeagues.push({ name: leagueName, divisions });
  }

  // Sort AL before NL
  mlbLeagues.sort((a, b) => {
    const order = (n: string) => n.toLowerCase().includes("american") ? 0 : 1;
    return order(a.name) - order(b.name);
  });

  return {
    league:    "MLB",
    season,
    updatedAt: new Date().toISOString(),
    phase:     "division",
    mlbLeagues,
  };
}

// ── Main fetch + cache pipeline ───────────────────────────────────────────────

async function getStandings(league: string, season: string): Promise<NormalisedStandings> {
  const cacheKey = `${league}:${season}`;

  const memHit = memGet(cacheKey);
  if (memHit) return memHit;

  const pgHit = await pgGet(league, season);
  if (pgHit) { memSet(cacheKey, pgHit); return pgHit; }

  const isNA  = !!NA_LEAGUE_CONFIG[league];
  const isMLB = league === "MLB";

  let raw: any;
  if (isMLB) {
    raw = await fetchWithTimeout(MLB_STANDINGS_URL());
  } else if (isNA) {
    const cfg = NA_LEAGUE_CONFIG[league];
    const url = `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/${cfg.sport}/${cfg.id}_table?json=1`;
    raw = await fetchWithTimeout(url);
  } else {
    const leagueId = SOCCER_LEAGUE_IDS[league];
    if (!leagueId) throw new Error(`Unknown league: ${league}`);
    const url = `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/standings/${leagueId}?season=${encodeURIComponent(season)}&json=1`;
    raw = await fetchWithTimeout(url);
  }

  const data = isMLB
    ? normaliseMLB(raw)
    : isNA
      ? normaliseNA(raw, league, league === "NHL")
      : normaliseSoccer(raw, league.toUpperCase(), season);

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
    const season = league === "MLB"
      ? mlbSeason()
      : String(req.query.season || currentSeason());

    if (!ALL_KNOWN_LEAGUES.has(league)) {
      return res.status(400).json({
        error:        `Unknown league: ${league}`,
        knownLeagues: Array.from(ALL_KNOWN_LEAGUES),
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
    // Soccer leagues (season-aware)
    for (const league of CRON_SOCCER_LEAGUES) {
      const season = currentSeason();
      try {
        const cacheKey = `${league}:${season}`;
        delete _mem[cacheKey];
        const cfg = SOCCER_LEAGUE_IDS[league];
        const url = `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/standings/${cfg}?season=${encodeURIComponent(season)}&json=1`;
        const raw  = await fetchWithTimeout(url);
        const data = normaliseSoccer(raw, league, season);
        await pgSet(league, season, data);
        memSet(cacheKey, data);
        console.log(`[standings:cron] refreshed ${league} ${season}`);
      } catch (e: any) {
        console.error(`[standings:cron] failed for ${league}:`, e?.message);
      }
      await new Promise((r) => setTimeout(r, 2_000));
    }

    // NA leagues (no season param)
    for (const league of CRON_NA_LEAGUES) {
      const season = currentSeason();
      try {
        const cacheKey = `${league}:${season}`;
        delete _mem[cacheKey];
        const cfg = NA_LEAGUE_CONFIG[league];
        const url = `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/${cfg.sport}/${cfg.id}_table?json=1`;
        const raw  = await fetchWithTimeout(url);
        const data = normaliseNA(raw, league, league === "NHL");
        await pgSet(league, season, data);
        memSet(cacheKey, data);
        console.log(`[standings:cron] refreshed ${league}`);
      } catch (e: any) {
        console.error(`[standings:cron] failed for ${league}:`, e?.message);
      }
      await new Promise((r) => setTimeout(r, 2_000));
    }

    // MLB (calendar-year season, dedicated endpoint)
    if (CRON_MLB) {
      const season = mlbSeason();
      const cacheKey = `MLB:${season}`;
      try {
        delete _mem[cacheKey];
        const raw  = await fetchWithTimeout(MLB_STANDINGS_URL());
        const data = normaliseMLB(raw);
        await pgSet("MLB", season, data);
        memSet(cacheKey, data);
        console.log(`[standings:cron] refreshed MLB ${season}`);
      } catch (e: any) {
        console.error(`[standings:cron] failed for MLB:`, e?.message);
      }
    }
  };

  setTimeout(() => {
    run().catch(console.error);
    _cronTimer = setInterval(() => run().catch(console.error), CRON_INTERVAL_MS);
  }, CRON_STARTUP_DELAY);

  console.log(`[standings:cron] scheduled — interval ${CRON_INTERVAL_MS / 3_600_000} h`);
}

export default router;