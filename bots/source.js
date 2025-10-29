// Chainlink Functions source script for BlockPools
// Output: winner team code (e.g., "PHI", "NYG") or "TIE" or "ERR"
// Safety-first policy:
//   • Require v2 `lookup/event_results/:id` to exist & be final
//   • Cross-check with at least one additional source
//   • If scores/winner disagree across sources → return "ERR"
//   • If teams don't align to contract (by name/code) → return "ERR"
//   • If anything is missing/ambiguous → return "ERR"
//
// Args (8 or 9):
// [0] leagueLabel ("nfl","mlb","nba","nhl","epl","ucl")
// [1] dateFrom (ET "YYYY-MM-DD")
// [2] dateTo   (ET "YYYY-MM-DD" next-day window)
// [3] teamACode
// [4] teamBCode
// [5] teamAName
// [6] teamBName
// [7] lockTime (unix seconds)  // used for proximity if needed
// [8] idEvent (optional; strongly validated; never forces wrong match)

const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const V1_BASE = "https://www.thesportsdb.com/api/v1/json";
const API_KEY = secrets.THESPORTSDB_API_KEY || "";

// --- Tunables (conservative defaults) ---
const REQUIRE_RESULTS   = true;   // must have lookup/event_results present & final
const REQUIRE_CONSENSUS = true;   // at least MIN_SOURCES agreeing winner
const MIN_SOURCES       = 2;      // require >= 2 agreeing sources
const SEASONS_TO_SCAN   = 2;      // seasons to look back for schedule/league if needed

const leagueMap = { mlb:"4424", nfl:"4391", nba:"4387", nhl:"4380", epl:"4328", ucl:"4480" };
function mapLeagueId(lbl){ return leagueMap[String(lbl||"").toLowerCase()] || ""; }

const headerVariants = [
  { "X-API-KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X-API-KEY": API_KEY, "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
];

function arrFrom(j, keys){
  if (!j || typeof j!=="object") return [];
  for (const k of keys){ const v=j?.[k]; if (Array.isArray(v)) return v; }
  for (const v of Object.values(j)) if (Array.isArray(v)) return v;
  return [];
}
async function v2Fetch(path){
  const url = `${V2_BASE}${path}`;
  for (const h of headerVariants){
    try{
      const r = await Functions.makeHttpRequest({ url, headers:h, timeout:15000 });
      if (r && r.data && Object.keys(r.data).length) return r.data;
    }catch{}
  }
  return null;
}
async function v2PreviousLeagueEvents(idLeague){
  if (!idLeague) return [];
  const j = await v2Fetch(`/schedule/previous/league/${idLeague}`);
  return arrFrom(j, ["schedule","events"]);
}
async function v2LookupEvent(idEvent){
  const j = await v2Fetch(`/lookup/event/${encodeURIComponent(String(idEvent))}`);
  const a = arrFrom(j, ["events","schedule","results","lookup"]);
  return a.length ? a[0] : null;
}
async function v2LookupEventResults(idEvent){
  const j = await v2Fetch(`/lookup/event_results/${encodeURIComponent(String(idEvent))}`);
  const a = arrFrom(j, ["results","events","schedule","lookup"]);
  return a.length ? a[0] : null;
}
async function v2ListSeasons(idLeague){
  const j = await v2Fetch(`/list/seasons/${idLeague}`);
  const a = arrFrom(j, ["seasons"]);
  return a.map(s=>String(s?.strSeason||s)).filter(Boolean);
}
async function v2ScheduleLeagueSeason(idLeague, season){
  const j = await v2Fetch(`/schedule/league/${idLeague}/${encodeURIComponent(season)}`);
  return arrFrom(j, ["schedule","events"]);
}

// Optional v1 day slice (only used for consensus corroboration)
async function v1EventsDay(dateISO, leagueLabel){
  if (!API_KEY) return [];
  const url = `${V1_BASE}/${API_KEY}/eventsday.php?d=${encodeURIComponent(dateISO)}&l=${encodeURIComponent(leagueLabel||"")}`;
  try{
    const r = await Functions.makeHttpRequest({ url, timeout:12000 });
    if (r && r.data) return arrFrom(r.data, ["events","schedule","results"]);
  }catch{}
  return [];
}

// ---------------- Normalization + aliasing ----------------
function norm(s){
  return (s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"")
    .replace(/[’'`]/g,"").replace(/[^a-z0-9 ]/gi," ")
    .replace(/\s+/g," ").trim().toLowerCase();
}
function eqCode(a,b){ return !!a && !!b && String(a).trim().toUpperCase()===String(b).trim().toUpperCase(); }
function eqName(a,b){ const x=norm(a), y=norm(b); return !!x && !!y && x===y; }
function fuzzyName(a,b){ const x=norm(a), y=norm(b); return !!x && !!y && (x.includes(y)||y.includes(x)); }
// Strength 3: code match; 2: exact name; 1: fuzzy contain
function strongTeamEq(targetName, candidateName, targetCode){
  if (eqCode(candidateName, targetCode)) return 3;
  if (eqName(candidateName, targetName)) return 2;
  if (fuzzyName(candidateName, targetName)) return 1;
  return 0;
}

// Alias map for common short codes / variations
const TEAM_ALIASES = new Map([
  ["AFC BOURNEMOUTH","AFC BOURNEMOUTH"],
  ["BOURNEMOUTH","AFC BOURNEMOUTH"],
  ["AFCB","AFC BOURNEMOUTH"],
  ["NOTTINGHAM FOREST","NOTTINGHAM FOREST"],
  ["NOTTINGHAM","NOTTINGHAM FOREST"],
  ["N FOREST","NOTTINGHAM FOREST"],
  ["NF","NOTTINGHAM FOREST"],
]);
function canon(s){ return String(s ?? "").normalize("NFKD").toUpperCase().replace(/\s+/g," ").trim(); }
function aliasName(raw){ const c = canon(raw); return TEAM_ALIASES.get(c) || c; }
function sameTeam(a,b){ return aliasName(a) === aliasName(b); }

/**
 * Order-agnostic team matcher.
 * Returns { ok, reason?, homeIsA? }.
 * - ok=true when provider(home,away) == {TeamA, TeamB} as a set
 * - homeIsA tells if provider HOME corresponds to your Team A (true) or Team B (false)
 */
function matchTeamsUnordered({
  providerHome, providerAway,
  teamAName, teamBName, teamACode, teamBCode
}){
  const pH = aliasName(providerHome);
  const pA = aliasName(providerAway);
  const A  = aliasName(teamAName) || aliasName(teamACode);
  const B  = aliasName(teamBName) || aliasName(teamBCode);

  const setProvider = new Set([pH, pA]);
  const setLocal    = new Set([A, B]);
  const setsEqual   = setProvider.size === setLocal.size && [...setProvider].every(v => setLocal.has(v));
  if (!setsEqual){
    return { ok:false, reason:`Team set mismatch: provider={${pH}, ${pA}} local={${A}, ${B}}` };
  }
  if (sameTeam(pH, A)) return { ok:true, homeIsA:true };
  if (sameTeam(pH, B)) return { ok:true, homeIsA:false };
  return { ok:false, reason:"Ambiguous mapping for home team" };
}

function tsFromEvent(e){
  if (e?.strTimestamp){ const ms=Date.parse(e.strTimestamp); if(!Number.isNaN(ms)) return (ms/1000)|0; }
  if (e?.dateEvent && e?.strTime){
    const s=/Z$/.test(e.strTime)?`${e.dateEvent}T${e.strTime}`:`${e.dateEvent}T${e.strTime}Z`;
    const ms=Date.parse(s); if(!Number.isNaN(ms)) return (ms/1000)|0;
  }
  if (e?.dateEvent){ const ms=Date.parse(`${e.dateEvent}T00:00:00Z`); if(!Number.isNaN(ms)) return (ms/1000)|0; }
  return 0;
}
function looksFinal(ev){
  const status=String(ev?.strStatus??ev?.strProgress??"").toLowerCase();
  const hasScores=(ev?.intHomeScore!=null && ev?.intAwayScore!=null);
  if (/^(ft|aot|aet|pen|finished|full time)$/.test(status)) return true;
  if (/final|finished|ended|complete/.test(status)) return true;
  return hasScores && !status;
}

// Candidate picker (still helpful when idEvent not provided)
function pickEvent(events, aName, bName, aCode, bCode, kickoff){
  const scored=[];
  for (const e of events||[]){
    const h=e?.strHomeTeam, w=e?.strAwayTeam; if(!h||!w) continue;
    // unordered match via set equality
    const m = matchTeamsUnordered({
      providerHome:h, providerAway:w,
      teamAName:aName, teamBName:bName, teamACode:aCode, teamBCode:bCode
    });
    if (!m.ok) continue;
    const ts=tsFromEvent(e), dist=Math.abs(ts-(kickoff||ts));
    // align score: stronger when we can map home->A/B confidently
    const align = 1 + (m.homeIsA === true || m.homeIsA === false ? 1 : 0);
    scored.push({e,align,dist});
  }
  scored.sort((x,y)=>(y.align-x.align)||(x.dist-y.dist));
  return scored.length?scored[0].e:null;
}

function sameScorePair(e1,e2){
  const a1=Number(e1?.intAwayScore), h1=Number(e1?.intHomeScore);
  const a2=Number(e2?.intAwayScore), h2=Number(e2?.intHomeScore);
  return Number.isFinite(a1)&&Number.isFinite(h1)&&Number.isFinite(a2)&&Number.isFinite(h2) && a1===a2 && h1===h2;
}

function winnerCodeFromSource(ev, teamAName, teamBName, teamACode, teamBCode){
  const hs = Number(ev?.intHomeScore ?? NaN);
  const as = Number(ev?.intAwayScore ?? NaN);
  if (!Number.isFinite(hs) || !Number.isFinite(as)) return null;

  const m = matchTeamsUnordered({
    providerHome: ev?.strHomeTeam, providerAway: ev?.strAwayTeam,
    teamAName, teamBName, teamACode, teamBCode
  });
  if (!m.ok) return null;

  // Map provider's home/away scores to your A/B
  const scoreA = m.homeIsA ? hs : as;
  const scoreB = m.homeIsA ? as : hs;

  if (scoreA === scoreB) return "TIE";
  return scoreA > scoreB ? teamACode : teamBCode;
}

// --------------- MAIN EXECUTION -----------------
const N = args.length;
if (N!==8 && N!==9) return Functions.encodeString("ERR"); // strict arity
if (!API_KEY) return Functions.encodeString("ERR");

const leagueLabel = args[0];
const dateFrom    = args[1]; // ET YYYY-MM-DD (used for season/day filter)
const dateTo      = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7]||0);
const idEventOpt  = N===9 ? String(args[8]||"") : "";

const idLeague = mapLeagueId(leagueLabel);
if (!idLeague) return Functions.encodeString("ERR");

// 1) Primary discovery by teams/dates (previous/league; schedule/league fallback)
let evPrev = null;
try {
  const prev = await v2PreviousLeagueEvents(idLeague);
  evPrev = pickEvent(prev, teamAName, teamBName, teamACode, teamBCode, lockTime);
} catch {}

let evSeason = null;
try {
  const seasons = await v2ListSeasons(idLeague);
  for (const ssn of seasons.slice(-SEASONS_TO_SCAN).reverse()){
    const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
    if (!seasonEvents?.length) continue;
    const daySlice = seasonEvents.filter(e => (e?.dateEvent || e?.dateEventLocal)===dateFrom);
    const cand = pickEvent(daySlice.length?daySlice:seasonEvents, teamAName, teamBName, teamACode, teamBCode, lockTime);
    if (cand){ evSeason=cand; break; }
  }
} catch {}

// 2) Optional idEvent (strongly validated and never forces wrong match)
let evMeta = null, evResults = null;
if (idEventOpt){
  try { evResults = await v2LookupEventResults(idEventOpt); } catch {}
  try { evMeta    = await v2LookupEvent(idEventOpt); } catch {}
  // Validate idEvent candidate aligns to our teams (unordered)
  function aligns(e){
    if (!e) return false;
    const m = matchTeamsUnordered({
      providerHome:e.strHomeTeam, providerAway:e.strAwayTeam,
      teamAName, teamBName, teamACode, teamBCode
    });
    return !!m.ok;
  }
  if (evMeta && !aligns(evMeta)) evMeta = null;
  if (evResults && !aligns(evResults)) evResults = null;
}

// 3) Build candidate sources & enforce strict consistency
const sources = [];
if (evResults) sources.push({ name:"results", ev:evResults });
if (evMeta)    sources.push({ name:"meta",    ev:evMeta });
if (evPrev)    sources.push({ name:"prev",    ev:evPrev });
if (evSeason)  sources.push({ name:"season",  ev:evSeason });

// Must have at least one source
if (!sources.length) return Functions.encodeString("ERR");

// If requiring results: must exist and be final
if (REQUIRE_RESULTS) {
  if (!evResults) return Functions.encodeString("ERR");
  if (!looksFinal(evResults)) return Functions.encodeString("ERR");
}

// Every present source must map to same fixture (unordered mapping must succeed)
for (const s of sources){
  const h=s.ev?.strHomeTeam, w=s.ev?.strAwayTeam;
  if (!h || !w) return Functions.encodeString("ERR");
  const m = matchTeamsUnordered({
    providerHome:h, providerAway:w,
    teamAName, teamBName, teamACode, teamBCode
  });
  if (!m.ok) return Functions.encodeString("ERR");
}

// If we have both results & meta, their scores must match exactly
if (evResults && evMeta) {
  if (!sameScorePair(evResults, evMeta)) return Functions.encodeString("ERR");
}

// Also: if prev/season picked, ensure their scores (if present) match results/meta
for (const s of sources){
  if (s.name==="prev" || s.name==="season"){
    if (evResults && s.ev?.intHomeScore!=null && s.ev?.intAwayScore!=null){
      if (!sameScorePair(s.ev, evResults)) return Functions.encodeString("ERR");
    }
    if (evMeta && s.ev?.intHomeScore!=null && s.ev?.intAwayScore!=null){
      if (!sameScorePair(s.ev, evMeta)) return Functions.encodeString("ERR");
    }
  }
}

// Compute winners from each source that is final and has scores
const winners = [];
for (const s of sources){
  const isFinal = looksFinal(s.ev);
  const hasScores = (s.ev?.intHomeScore!=null && s.ev?.intAwayScore!=null);
  if (isFinal && hasScores){
    const code = winnerCodeFromSource(s.ev, teamAName, teamBName, teamACode, teamBCode);
    if (code === "TIE" || code === teamACode || code === teamBCode) {
      winners.push({ src:s.name, code });
    } else {
      return Functions.encodeString("ERR");
    }
  }
}

// Need enough agreeing sources
if (!winners.length) return Functions.encodeString("ERR");
if (REQUIRE_CONSENSUS && winners.length < MIN_SOURCES) return Functions.encodeString("ERR");

// Build consensus & ensure no disagreement
const counts = winners.reduce((m, {code}) => (m[code]=(m[code]||0)+1, m), {});
let top = null, topCount = 0;
for (const [code,count] of Object.entries(counts)){
  if (count > topCount){ topCount = count; top = code; }
}
if (!top) return Functions.encodeString("ERR");

// Reject any disagreement for maximum safety
if (Object.keys(counts).length > 1) {
  return Functions.encodeString("ERR");
}

// Passed all checks → return the winner code
return Functions.encodeString(top);
