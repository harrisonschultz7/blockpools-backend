// Chainlink Functions source script for BlockPools
// Output: "<winnerAB>|<idEvent>|<homeScore>|<awayScore>"
// winnerAB is "A" or "B" based on your contract's A/B (not home/away)

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

// ---------- HTTP helpers ----------
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

// ---------- matching helpers (code > exact name > fuzzy) ----------
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
  // priority: exact code > exact name > fuzzy
  if (eqCode(candidate, targetCode)) return 3;
  if (eqName(candidate, target))   return 2;
  if (fuzzyName(candidate, target))return 1;
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

// pick event closest to kickoff that ALSO aligns with A/B vs home/away using strong matching
function pickEvent(events, aName, bName, aCode, bCode, kickoff) {
  const scored = [];
  for (const e of events) {
    const h = e?.strHomeTeam, w = e?.strAwayTeam;
    if (!h || !w) continue;

    // two possible alignments: A=home,B=away OR A=away,B=home
    const aHome = strongTeamEq(aName, h, aCode);
    const bAway = strongTeamEq(bName, w, bCode);
    const aAway = strongTeamEq(aName, w, aCode);
    const bHome = strongTeamEq(bName, h, bCode);

    // require a non-zero match on both sides for a valid alignment
    const align1 = Math.min(aHome, bAway); // A->home, B->away
    const align2 = Math.min(aAway, bHome); // A->away, B->home
    const align = Math.max(align1, align2);

    if (align > 0) {
      const ts = tsFromEvent(e);
      const dist = Math.abs(ts - (kickoff || ts));
      // sort by alignment strength desc, then time distance asc
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

// Map (home/away) → contract A/B using strong matching rules consistently
function winnerAB(ev, aName, bName, aCode, bCode) {
  const homeName = ev?.strHomeTeam || "";
  const awayName = ev?.strAwayTeam || "";
  const hs = ev?.intHomeScore != null ? Number(ev.intHomeScore) : null;
  const as = ev?.intAwayScore != null ? Number(ev.intAwayScore) : null;
  if (hs == null || as == null) return { w: "", hs: 0, as: 0 };

  // decide which side is home/away using the same scoring
  const aHome = strongTeamEq(aName, homeName, aCode);
  const bAway = strongTeamEq(bName, awayName, bCode);
  const aAway = strongTeamEq(aName, awayName, aCode);
  const bHome = strongTeamEq(bName, homeName, bCode);

  // prefer the alignment with higher min-score
  const align1 = Math.min(aHome, bAway); // A->home
  const align2 = Math.min(aAway, bHome); // A->away

  let aIsHome = false;
  if (align1 > align2) aIsHome = true;
  else if (align2 > align1) aIsHome = false;
  else {
    // tie: break by higher single-side strength, then default to name equality
    const homeBias = Math.max(aHome, bHome);
    const awayBias = Math.max(aAway, bAway);
    if (homeBias !== awayBias) aIsHome = aHome >= bHome; // if A matches home stronger, A->home
    else aIsHome = eqName(homeName, aName); // final tiny nudge
  }

  if (hs > as) return { w: aIsHome ? "A" : "B", hs, as };
  if (as > hs) return { w: aIsHome ? "B" : "A", hs, as };
  return { w: "D", hs, as }; // draw support if needed by your contract
}

// ---------- ENTRY ----------
const N = args.length;
if (N !== 8 && N !== 9) throw Error("8 or 9 args required");

const leagueLabel = args[0];
const dateFrom    = args[1]; // ET Y-M-D
const _dateTo     = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7] || 0);
const idEventOpt  = N === 9 ? String(args[8] || "") : "";

if (!API_KEY) return Functions.encodeString("ERR|missing_key|||");

const idLeague = mapLeagueId(leagueLabel);
let ev = null;

// 1) direct id
if (idEventOpt) {
  ev = (await v2LookupEventResults(idEventOpt)) || (await v2LookupEvent(idEventOpt));
}

// 2) previous/league with strong alignment filter
if (!ev && idLeague) {
  const prev = await v2PreviousLeagueEvents(idLeague);
  ev = pickEvent(prev, teamAName, teamBName, teamACode, teamBCode, lockTime);
}

// 3) season fallback for the day (handles >10 previous)
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

if (!ev) return Functions.encodeString("ERR|no_event|||");
if (!looksFinal(ev)) return Functions.encodeString(`ERR|not_final|${String(ev.idEvent||"")}|${ev.intHomeScore ?? ""}|${ev.intAwayScore ?? ""}`);

const { w, hs, as } = winnerAB(ev, teamAName, teamBName, teamACode, teamBCode);
if (!w) return Functions.encodeString(`ERR|map_fail|${String(ev.idEvent||"")}|${hs}|${as}`);

return Functions.encodeString(`${w}|${String(ev.idEvent||"")}|${hs}|${as}`);
