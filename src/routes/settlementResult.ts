// routes/settlementResult.ts
//
// Server-side settlement resolver for the CRE workflow.
//
// WHY THIS EXISTS: CRE's HTTP capability has a small per-node response buffer.
// Goalserve's daily league feeds (esp. soccer /commentaries and busy MLB days)
// exceed it -> every DON node errors "response buffer too small" -> consensus
// fails. This endpoint does the heavy Goalserve fetch + team-matching HERE (Node,
// no buffer limit) and returns a TINY JSON the DON workflow can consume.
//
// The matching/winner logic is a faithful port of the old Chainlink Functions
// source.js (and the CRE workflow's resolve.ts) — strict team matching, closest
// kickoff to lockTime, final-status detection.
//
// GET /api/scores/settlement
//   ?league=WC&dateFrom=2026-06-11&dateTo=2026-06-13
//   &teamAName=USA&teamBName=Mexico&teamACode=USA&teamBCode=MEX
//   &lockTime=1780000000&drawOutcomeCode=DRAW
//
// Returns 200:
//   { found, isFinal, outcome, homeName, awayName, homeScore, awayScore, status }
//   outcome: 0 = Team A wins, 1 = Team B wins, 2 = score draw, 3 = void/tie
//   (isFinal=false or found=false => the workflow throws and the watcher retries)
//
// Env required: GOALSERVE_API_KEY   Env optional: GOALSERVE_BASE_URL

import { Router, Request, Response } from "express";

const router = Router();

const GOALSERVE_API_KEY = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL = (
  process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed"
).replace(/\/+$/, "");
const FETCH_TIMEOUT_MS = 15_000;

export const OUTCOME_A = 0;
export const OUTCOME_B = 1;
export const OUTCOME_DRAW = 2;
export const OUTCOME_VOID = 3;

const s = (v: unknown) => String(v == null ? "" : v).trim();
const trimU = (v: unknown) => String(v || "").trim().toUpperCase();

function goalserveLeaguePaths(leagueLabel: string): { sportPath: string; leaguePaths: string[] } {
  const L = String(leagueLabel || "").trim().toLowerCase();
  if (L === "nfl") return { sportPath: "football", leaguePaths: ["nfl-scores"] };
  if (L === "nba") return { sportPath: "bsktbl", leaguePaths: ["nba-scores"] };
  if (L === "nhl") return { sportPath: "hockey", leaguePaths: ["nhl-scores"] };
  if (L === "mlb") return { sportPath: "baseball", leaguePaths: ["usa"] };
  if (L === "epl" || L === "premier league" || L === "england - premier league" || L === "england premier league")
    return { sportPath: "commentaries", leaguePaths: ["1204"] };
  if (L === "ucl" || L === "uefa champions league" || L === "champions league")
    return { sportPath: "commentaries", leaguePaths: ["1005"] };
  if (L === "wc" || L === "world cup" || L === "fifa world cup" || L === "worldcup")
    return { sportPath: "commentaries", leaguePaths: ["1056"] };
  return { sportPath: "", leaguePaths: [] };
}

const finalsSet = new Set([
  "final", "finished", "full time", "full-time", "ft", "after over time", "after overtime",
  "final/ot", "final ot", "final aot", "final after ot", "after penalties", "after penalty shots",
  "after shootout", "aet", "after extra time", "ap", "ended",
]);

function isFinalStatus(raw: string): boolean {
  const st = (raw || "").trim().toLowerCase();
  if (!st) return false;
  if (finalsSet.has(st)) return true;
  if (st.includes("after over time") || st.includes("after overtime") || st.includes("after ot")) return true;
  if (st.includes("full time") || st === "full-time") return true;
  if (st.includes("after penalties")) return true;
  if (st.includes("after penalty")) return true;
  if (st.includes("shootout")) return true;
  if (st.includes("final") && !st.includes("semi") && !st.includes("quarter") && !st.includes("half")) return true;
  return false;
}

function norm(str: string): string {
  return (str || "")
    .normalize("NFD")
    .replace(/[̀-ͯ]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

const NATION_ALIASES: Record<string, string> = {
  "korea republic": "south korea",
  "republic of korea": "south korea",
  "korea dpr": "north korea",
  "dpr korea": "north korea",
  "ir iran": "iran",
  "iran ir": "iran",
  "united states": "usa",
  "united states of america": "usa",
  "bosnia herzegovina": "bosnia and herzegovina",
  bosnia: "bosnia and herzegovina",
  "cote divoire": "ivory coast",
  czechia: "czech republic",
  turkiye: "turkey",
  "cape verde islands": "cape verde",
  "cabo verde": "cape verde",
  holland: "netherlands",
  "united arab emirates": "uae",
};

function canonName(str: string): string {
  const n = norm(str);
  return NATION_ALIASES[n] || n;
}

function acronym(str: string): string {
  const parts = (str || "").split(/[^a-zA-Z0-9]+/).filter(Boolean);
  return parts.map((p) => (p[0] || "").toUpperCase()).join("");
}

function parseDatetimeUTC(str: string): number | undefined {
  if (!str) return;
  const m = String(str).match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return;
  const [, dd, MM, yyyy, HH, mm] = m;
  const t = Date.UTC(+yyyy, +MM - 1, +dd, +HH, +mm, 0, 0);
  if (!isFinite(t)) return;
  return Math.floor(t / 1000);
}

function parseDateAndTimeAsUTC(dateStr: string, timeStr?: string): number | undefined {
  if (!dateStr) return;
  const md = String(dateStr).match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (!md) return;
  const [, dd, MM, yyyy] = md;
  let h = 0;
  let mi = 0;
  if (timeStr) {
    const ampm = String(timeStr).trim().toUpperCase();
    let mh = ampm.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/);
    if (mh) {
      h = +mh[1];
      mi = +mh[2];
      const mer = mh[3];
      if (mer === "PM" && h < 12) h += 12;
      if (mer === "AM" && h === 12) h = 0;
    } else {
      mh = ampm.match(/^(\d{1,2}):(\d{2})$/);
      if (mh) {
        h = +mh[1];
        mi = +mh[2];
      }
    }
  }
  const t = Date.UTC(+yyyy, +MM - 1, +dd, h, mi, 0, 0);
  if (!isFinite(t)) return;
  return Math.floor(t / 1000);
}

function kickoffEpochFromRaw(raw: any): number | undefined {
  const t1 = parseDatetimeUTC(raw?.datetime_utc || raw?.["@datetime_utc"]);
  if (t1) return t1;
  const date = raw?.formatted_date || raw?.date || raw?.["@formatted_date"] || raw?.["@date"];
  const time = raw?.time || raw?.start_time || raw?.start || raw?.["@time"];
  return parseDateAndTimeAsUTC(date, time);
}

function collectCandidateGames(payload: any): any[] {
  if (!payload) return [];
  const gg = payload?.games?.game;
  if (gg != null) {
    if (Array.isArray(gg)) return gg;
    if (typeof gg === "object") return [gg];
  }
  const cat = payload?.scores?.category;
  if (cat) {
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = cats.flatMap((c: any) => {
      if (Array.isArray(c?.match)) return c.match;
      if (c?.match) return [c.match];
      return [];
    });
    if (matches.length) return matches;
  }
  const comm = payload?.commentaries?.tournament;
  if (comm) {
    const ts = Array.isArray(comm) ? comm : [comm];
    const matches = ts.flatMap((t: any) => {
      if (Array.isArray(t?.match)) return t.match;
      if (t?.match) return [t.match];
      return [];
    });
    if (matches.length) return matches;
  }
  if (Array.isArray(payload?.game)) return payload.game;
  if (Array.isArray(payload)) return payload;
  if (typeof payload === "object") {
    const arrs = Object.keys(payload).sort().map((k) => (payload as any)[k]).filter(Array.isArray) as any[];
    if (arrs.length) return arrs.flat();
  }
  return [];
}

function normalizeGameRow(r: any) {
  const homeName =
    r?.hometeam?.name || r?.hometeam?.["@name"] || r?.home_name || r?.home || r?.home_team ||
    (r?.localteam && (r.localteam["@name"] || r.localteam.name)) || "";
  const awayName =
    r?.awayteam?.name || r?.awayteam?.["@name"] || r?.away_name || r?.away || r?.away_team ||
    (r?.visitorteam && (r.visitorteam["@name"] || r.visitorteam.name)) || "";
  const homeScore = Number(
    r?.hometeam?.totalscore ?? r?.hometeam?.["@totalscore"] ?? r?.hometeam?.["@goals"] ??
      r?.home_score ?? r?.home_final ??
      (r?.localteam && (r.localteam["@goals"] || r.localteam["@ft_score"])) ?? 0
  );
  const awayScore = Number(
    r?.awayteam?.totalscore ?? r?.awayteam?.["@totalscore"] ?? r?.awayteam?.["@goals"] ??
      r?.away_score ?? r?.away_final ??
      (r?.visitorteam && (r.visitorteam["@goals"] || r.visitorteam["@ft_score"])) ?? 0
  );
  const status = String(r?.status || r?.game_status || r?.state || r?.["@status"] || "").trim();
  return { homeName, awayName, homeScore, awayScore, status };
}

function teamMatchesOneSide(apiName: string, wantName: string, wantCode?: string): boolean {
  const nApi = norm(apiName);
  const nWant = norm(wantName);
  const code = trimU(wantCode);
  if (!nApi) return false;
  if (nWant && canonName(apiName) === canonName(wantName)) return true;
  const apiAcr = acronym(apiName);
  const wantAcr = acronym(wantName);
  if (code && apiAcr && apiAcr === code) return true;
  if (wantAcr && apiAcr && apiAcr === wantAcr) return true;
  const apiParts = nApi.split(" ").filter(Boolean);
  const wantParts = nWant.split(" ").filter(Boolean);
  if (!apiParts.length || !wantParts.length) return false;
  const apiMascot = apiParts[apiParts.length - 1];
  const wantMascot = wantParts[wantParts.length - 1];
  if (apiMascot && wantMascot && apiMascot === wantMascot) return true;
  return false;
}

function unorderedTeamsMatchByTokens(
  homeName: string, awayName: string, AName: string, BName: string, ACode?: string, BCode?: string
): boolean {
  const hA = teamMatchesOneSide(homeName, AName, ACode);
  const aB = teamMatchesOneSide(awayName, BName, BCode);
  const hB = teamMatchesOneSide(homeName, BName, BCode);
  const aA = teamMatchesOneSide(awayName, AName, ACode);
  return (hA && aB) || (hB && aA);
}

function isoToDdmmyyyy(iso: string): string {
  const [Y, M, D] = String(iso).split("-");
  return `${D}.${M}.${Y}`;
}

function* iterateDateRange(fromISO: string, toISO: string): Generator<string> {
  if (!fromISO || !toISO) return;
  const from = new Date(fromISO + "T00:00:00Z");
  const to = new Date(toISO + "T00:00:00Z");
  for (let d = new Date(from); d < to; d.setUTCDate(d.getUTCDate() + 1)) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    yield `${yyyy}-${mm}-${dd}`;
  }
}

async function fetchJson(url: string): Promise<any> {
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

type SettlementResult = {
  found: boolean;
  isFinal: boolean;
  outcome: number | null;
  homeName?: string;
  awayName?: string;
  homeScore?: number;
  awayScore?: number;
  status?: string;
};

async function resolveOutcome(p: {
  league: string;
  dateFrom: string;
  dateTo: string;
  teamACode?: string;
  teamBCode?: string;
  teamAName: string;
  teamBName: string;
  lockTime?: number;
}): Promise<SettlementResult> {
  const league = s(p.league).toLowerCase();
  const dateFrom = s(p.dateFrom);
  const dateTo = s(p.dateTo);
  const teamACode = trimU(p.teamACode);
  const teamBCode = trimU(p.teamBCode);
  const teamAName = s(p.teamAName);
  const teamBName = s(p.teamBName);
  const lockTime = Number(p.lockTime || 0);

  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  if (!sportPath || !leaguePaths.length) throw new Error(`Unsupported league: ${league}`);
  if (!GOALSERVE_API_KEY) throw new Error("GOALSERVE_API_KEY not configured");

  const candidates: Array<{
    homeName: string; awayName: string; homeScore: number; awayScore: number; status: string; kickoff?: number;
  }> = [];

  for (const iso of iterateDateRange(dateFrom, dateTo)) {
    const ddmmyyyy = isoToDdmmyyyy(iso);
    let foundFinalThisDate = false;
    for (const lp of leaguePaths) {
      const url =
        `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/${sportPath}/` +
        `${lp}?date=${encodeURIComponent(ddmmyyyy)}&json=1`;
      const feed = await fetchJson(url);
      const rawGames = collectCandidateGames(feed);
      if (!rawGames.length) continue;
      for (const r of rawGames) {
        const g = normalizeGameRow(r);
        if (!unorderedTeamsMatchByTokens(g.homeName, g.awayName, teamAName, teamBName, teamACode, teamBCode)) continue;
        candidates.push({ ...g, kickoff: kickoffEpochFromRaw(r) });
        if (isFinalStatus(g.status)) foundFinalThisDate = true;
      }
    }
    if (foundFinalThisDate) break;
  }

  if (!candidates.length) return { found: false, isFinal: false, outcome: null };

  candidates.sort((a, b) => {
    const t1 = typeof a.kickoff === "number" ? Math.abs(a.kickoff - lockTime) : Number.MAX_SAFE_INTEGER;
    const t2 = typeof b.kickoff === "number" ? Math.abs(b.kickoff - lockTime) : Number.MAX_SAFE_INTEGER;
    if (t1 !== t2) return t1 - t2;
    return (isFinalStatus(b.status) ? 1 : 0) - (isFinalStatus(a.status) ? 1 : 0);
  });

  const best = candidates[0];
  const base = {
    found: true,
    homeName: best.homeName,
    awayName: best.awayName,
    homeScore: best.homeScore,
    awayScore: best.awayScore,
    status: best.status,
  };

  if (!isFinalStatus(best.status)) return { ...base, isFinal: false, outcome: null };

  let outcome: number;
  if (best.homeScore > best.awayScore) {
    const homeIsA = teamMatchesOneSide(best.homeName, teamAName, teamACode);
    const homeIsB = teamMatchesOneSide(best.homeName, teamBName, teamBCode);
    outcome = homeIsA && !homeIsB ? OUTCOME_A : homeIsB && !homeIsA ? OUTCOME_B : OUTCOME_VOID;
  } else if (best.awayScore > best.homeScore) {
    const awayIsA = teamMatchesOneSide(best.awayName, teamAName, teamACode);
    const awayIsB = teamMatchesOneSide(best.awayName, teamBName, teamBCode);
    outcome = awayIsA && !awayIsB ? OUTCOME_A : awayIsB && !awayIsA ? OUTCOME_B : OUTCOME_VOID;
  } else {
    outcome = OUTCOME_DRAW;
  }

  return { ...base, isFinal: true, outcome };
}

router.get("/settlement", async (req: Request, res: Response) => {
  try {
    const q = req.query as Record<string, string>;
    const league = s(q.league);
    const teamAName = s(q.teamAName);
    const teamBName = s(q.teamBName);
    if (!league || !teamAName || !teamBName || !s(q.dateFrom) || !s(q.dateTo)) {
      return res.status(400).json({ error: "Missing required params: league, dateFrom, dateTo, teamAName, teamBName" });
    }
    const result = await resolveOutcome({
      league,
      dateFrom: s(q.dateFrom),
      dateTo: s(q.dateTo),
      teamACode: q.teamACode,
      teamBCode: q.teamBCode,
      teamAName,
      teamBName,
      lockTime: Number(q.lockTime || 0),
    });
    return res.json(result);
  } catch (e: any) {
    console.error("[/api/scores/settlement]", e?.message || e);
    return res.status(502).json({ error: String(e?.message || e) });
  }
});

export default router;
