// Chainlink Functions source script for BlockPools
// Output: the winner's team code (e.g. "PHI" or "NYG")
// or "TIE" if scores are equal, or "ERR" if nothing final found.

const leagueMap = {
  mlb: "4424", nfl: "4391", nba: "4387", nhl: "4380",
  epl: "4328", ucl: "4480",
};
const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const API_KEY = secrets.THESPORTSDB_API_KEY || "";

const headerVariants = [
  { "X-API-KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X-API-KEY": API_KEY, "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
];

function mapLeagueId(lbl) {
  const k = String(lbl || "").toLowerCase();
  return leagueMap[k] || "";
}

// ---------- helpers ----------
function arrFrom(j, keys) {
  if (!j || typeof j !== "object") return [];
  for (const k of keys) {
    const v = j?.[k];
    if (Array.isArray(v)) return v;
  }
  for (const v of Object.values(j)) if (Array.isArray(v)) return v;
  return [];
}

async function v2Fetch(path) {
  const url = `${V2_BASE}${path}`;
  for (const h of headerVariants) {
    try {
      const r = await Functions.makeHttpRequest({ url, headers: h, timeout: 15000 });
      if (r && r.data && Object.keys(r.data).length) return r.data;
    } catch {}
  }
  return null;
}

async function v2PreviousLeagueEvents(idLeague) {
  if (!idLeague) return [];
  const j = await v2Fetch(`/schedule/previous/league/${idLeague}`);
  return arrFrom(j, ["schedule", "events"]);
}
async function v2LookupEvent(idEvent) {
  const j = await v2Fetch(`/lookup/event/${encodeURIComponent(String(idEvent))}`);
  const arr = arrFrom(j, ["events", "schedule", "results", "lookup"]);
  return arr.length ? arr[0] : null;
}
async function v2LookupEventResults(idEvent) {
  const j = await v2Fetch(`/lookup/event_results/${encodeURIComponent(String(idEvent))}`);
  const arr = arrFrom(j, ["results", "events", "schedule"]);
  return arr.length ? arr[0] : null;
}
async function v2ListSeasons(idLeague) {
  const j = await v2Fetch(`/list/seasons/${idLeague}`);
  const arr = arrFrom(j, ["seasons"]);
  return arr.map((s) => String(s?.strSeason || s)).filter(Boolean);
}
async function v2ScheduleLeagueSeason(idLeague, season) {
  const j = await v2Fetch(`/schedule/league/${idLeague}/${encodeURIComponent(season)}`);
  return arrFrom(j, ["schedule", "events"]);
}

function norm(s) {
  return (s || "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}
function eqCode(a, b) {
  if (!a || !b) return false;
  return String(a).trim().toUpperCase() === String(b).trim().toUpperCase();
}
function eqName(a, b) {
  const x = norm(a), y = norm(b);
  return !!x && !!y && x === y;
}
function fuzzyName(a, b) {
  const x = norm(a), y = norm(b);
  return !!x && !!y && (x.includes(y) || y.includes(x));
}
function strongTeamEq(target, candidate, targetCode) {
  if (eqCode(candidate, targetCode)) return 3;
  if (eqName(candidate, target)) return 2;
  if (fuzzyName(candidate, target)) return 1;
  return 0;
}

function tsFromEvent(e) {
  if (e?.strTimestamp) {
    const ms = Date.parse(e.strTimestamp);
    if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  if (e?.dateEvent && e?.strTime) {
    const s = /Z$/.test(e.strTime) ? `${e.dateEvent}T${e.strTime}` : `${e.dateEvent}T${e.strTime}Z`;
    const ms = Date.parse(s);
    if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  if (e?.dateEvent) {
    const ms = Date.parse(`${e.dateEvent}T00:00:00Z`);
    if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  return 0;
}

function pickEvent(events, aName, bName, aCode, bCode, kickoff) {
  const scored = [];
  for (const e of events) {
    const h = e?.strHomeTeam, w = e?.strAwayTeam;
    if (!h || !w) continue;
    const aHome = strongTeamEq(aName, h, aCode);
    const bAway = strongTeamEq(bName, w, bCode);
    const aAway = strongTeamEq(aName, w, aCode);
    const bHome = strongTeamEq(bName, h, bCode);
    const align1 = Math.min(aHome, bAway);
    const align2 = Math.min(aAway, bHome);
    const align = Math.max(align1, align2);
    if (align > 0) {
      const ts = tsFromEvent(e);
      const dist = Math.abs(ts - (kickoff || ts));
      scored.push({ e, align, dist });
    }
  }
  scored.sort((x, y) => (y.align - x.align) || (x.dist - y.dist));
  return scored.length ? scored[0].e : null;
}

function looksFinal(ev) {
  const status = String(ev?.strStatus ?? ev?.strProgress ?? "").toLowerCase();
  const hasScores = (ev?.intHomeScore != null && ev?.intAwayScore != null);
  if (/^(ft|aot|aet|pen|finished|full time)$/.test(status)) return true;
  if (/final|finished|ended|complete/.test(status)) return true;
  return hasScores && !status;
}

// ---------- ENTRY ----------
const N = args.length;
if (N !== 8 && N !== 9) throw Error("8 or 9 args required");

const leagueLabel = args[0];
const dateFrom    = args[1];
const _dateTo     = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7] || 0);
const idEventOpt  = N === 9 ? String(args[8] || "") : "";

if (!API_KEY) return Functions.encodeString("ERR");

const idLeague = mapLeagueId(leagueLabel);
let ev = null;

// 1) direct id
if (idEventOpt) {
  ev = (await v2LookupEventResults(idEventOpt)) || (await v2LookupEvent(idEventOpt));
}

// 2) previous/league
if (!ev && idLeague) {
  const prev = await v2PreviousLeagueEvents(idLeague);
  ev = pickEvent(prev, teamAName, teamBName, teamACode, teamBCode, lockTime);
}

// 3) season fallback
if (!ev && idLeague) {
  const seasons = await v2ListSeasons(idLeague);
  for (const ssn of seasons.slice(-2).reverse()) {
    const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
    if (!seasonEvents?.length) continue;
    const daySlice = seasonEvents.filter((e) => (e?.dateEvent || e?.dateEventLocal) === dateFrom);
    const cand = pickEvent(daySlice.length ? daySlice : seasonEvents, teamAName, teamBName, teamACode, teamBCode, lockTime);
    if (cand) { ev = cand; break; }
  }
}

if (!ev || !looksFinal(ev)) return Functions.encodeString("ERR");

// Determine winner code
const hs = Number(ev.intHomeScore || 0);
const as = Number(ev.intAwayScore || 0);
let winnerCode = "";
if (hs === as) winnerCode = "TIE";
else {
  // Map home/away -> contract team codes
  const homeMatchA = strongTeamEq(teamAName, ev.strHomeTeam, teamACode);
  const homeMatchB = strongTeamEq(teamBName, ev.strHomeTeam, teamBCode);
  if (hs > as) {
    winnerCode = homeMatchA >= homeMatchB ? teamACode : teamBCode;
  } else {
    // away wins
    winnerCode = homeMatchA >= homeMatchB ? teamBCode : teamACode;
  }
}

return Functions.encodeString(winnerCode || "ERR");
