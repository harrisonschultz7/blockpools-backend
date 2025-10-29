// Chainlink Functions source script for BlockPools
// Output: winner team code (e.g., "PHI", "NYG") or "TIE" or "ERR"
// Safety-first policy:
//   • Must be final by provider (lookup/event_results)
//   • Reject if event date is not within ±24h of contract lockTime
//   • Reject if wrong league, preseason, or old game
//   • Cross-check across multiple sources for consensus
//   • Return "ERR" on any ambiguity or mismatch
//
// Args (8 or 9):
// [0] leagueLabel ("nfl","mlb","nba","nhl","epl","ucl")
// [1] dateFrom (ET "YYYY-MM-DD")
// [2] dateTo   (ET "YYYY-MM-DD" next-day window)
// [3] teamACode
// [4] teamBCode
// [5] teamAName
// [6] teamBName
// [7] lockTime (unix seconds)
// [8] idEvent (optional)

const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const V1_BASE = "https://www.thesportsdb.com/api/v1/json";
const API_KEY = secrets.THESPORTSDB_API_KEY || "";

// --- Tunables ---
const REQUIRE_RESULTS   = true;
const REQUIRE_CONSENSUS = true;
const MIN_SOURCES       = 2;
const SEASONS_TO_SCAN   = 2;

// --- Anti-mismatch guardrails ---
const MAX_EVENT_DRIFT_SECS = 24 * 3600; // 24 hours
const ALLOWABLE_LEAGUE_IDS = {
  "nfl": "4391",
  "mlb": "4424",
  "nba": "4387",
  "nhl": "4380",
  "epl": "4328",
  "ucl": "4480",
};

// ---------------- League Mapping ----------------
const leagueMap = { mlb:"4424", nfl:"4391", nba:"4387", nhl:"4380", epl:"4328", ucl:"4480" };
function mapLeagueId(lbl){ return leagueMap[String(lbl||"").toLowerCase()] || ""; }

// ---------------- API Helpers ----------------
const headerVariants = [
  { "X-API-KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
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

// ---------------- Normalization + Matching ----------------
function norm(s){
  return (s||"").normalize("NFD").replace(/[\u0300-\u036f]/g,"")
    .replace(/[’'`]/g,"").replace(/[^a-z0-9 ]/gi," ")
    .replace(/\s+/g," ").trim().toLowerCase();
}
function eqCode(a,b){ return !!a && !!b && String(a).trim().toUpperCase()===String(b).trim().toUpperCase(); }
function eqName(a,b){ const x=norm(a), y=norm(b); return !!x && !!y && x===y; }
function fuzzyName(a,b){ const x=norm(a), y=norm(b); return !!x && !!y && (x.includes(y)||y.includes(x)); }
function strongTeamEq(targetName, candidateName, targetCode){
  if (eqCode(candidateName, targetCode)) return 3;
  if (eqName(candidateName, targetName)) return 2;
  if (fuzzyName(candidateName, targetName)) return 1;
  return 0;
}

// Alias map
const TEAM_ALIASES = new Map([
  ["AFCB","AFC BOURNEMOUTH"],
  ["BOURNEMOUTH","AFC BOURNEMOUTH"],
  ["NOTTINGHAM","NOTTINGHAM FOREST"],
  ["NF","NOTTINGHAM FOREST"],
]);
function canon(s){ return String(s ?? "").normalize("NFKD").toUpperCase().replace(/\s+/g," ").trim(); }
function aliasName(raw){ const c = canon(raw); return TEAM_ALIASES.get(c) || c; }
function sameTeam(a,b){ return aliasName(a) === aliasName(b); }

function matchTeamsUnordered({ providerHome, providerAway, teamAName, teamBName, teamACode, teamBCode }){
  const pH = aliasName(providerHome);
  const pA = aliasName(providerAway);
  const A  = aliasName(teamAName) || aliasName(teamACode);
  const B  = aliasName(teamBName) || aliasName(teamBCode);

  const setProvider = new Set([pH, pA]);
  const setLocal    = new Set([A, B]);
  const setsEqual   = setProvider.size === setLocal.size && [...setProvider].every(v => setLocal.has(v));
  if (!setsEqual) return { ok:false, reason:`Team set mismatch: provider={${pH},${pA}} local={${A},${B}}` };
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

// ---------------- Date & League Guardrails ----------------
function sameLeague(e, expectedLeagueId){
  return String(e?.idLeague || "") === String(expectedLeagueId || "");
}
function eventTimestamp(e){
  const ts = tsFromEvent(e);
  return Number.isFinite(ts) ? ts : 0;
}
function withinDateWindow(e, lockTime, dateFrom){
  const eTs = eventTimestamp(e);
  if (lockTime > 0 && eTs > 0) return Math.abs(eTs - lockTime) <= MAX_EVENT_DRIFT_SECS;
  const ed = e?.dateEvent || e?.dateEventLocal;
  return ed && ed === dateFrom;
}
function rejectOutOfWindow(e, lockTime, dateFrom, expectedLeagueId){
  if (!sameLeague(e, expectedLeagueId)) return "league mismatch";
  if (!withinDateWindow(e, lockTime, dateFrom)) return "date drift";
  return null;
}

// ---------------- Scoring ----------------
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
  const scoreA = m.homeIsA ? hs : as;
  const scoreB = m.homeIsA ? as : hs;
  if (scoreA === scoreB) return "TIE";
  return scoreA > scoreB ? teamACode : teamBCode;
}

// ---------------- Main Execution ----------------
const N = args.length;
if (N!==8 && N!==9) return Functions.encodeString("ERR");
if (!API_KEY) return Functions.encodeString("ERR");

const leagueLabel = args[0];
const dateFrom    = args[1];
const dateTo      = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7]||0);
const idEventOpt  = N===9 ? String(args[8]||"") : "";
const idLeague    = mapLeagueId(leagueLabel);
const expectedLeagueId = ALLOWABLE_LEAGUE_IDS[leagueLabel.toLowerCase()] || "";

if (!idLeague) return Functions.encodeString("ERR");

let evPrev=null, evSeason=null, evMeta=null, evResults=null;

// 1) Try previous league events
try {
  const prev = await v2PreviousLeagueEvents(idLeague);
  const filtered = (prev||[]).filter(e => !rejectOutOfWindow(e, lockTime, dateFrom, expectedLeagueId));
  evPrev = filtered.find(e => matchTeamsUnordered({
    providerHome:e.strHomeTeam, providerAway:e.strAwayTeam,
    teamAName, teamBName, teamACode, teamBCode
  }).ok);
} catch {}

// 2) Try season lookup
try {
  const seasons = await v2ListSeasons(idLeague);
  for (const ssn of seasons.slice(-SEASONS_TO_SCAN).reverse()){
    const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
    const filtered = (seasonEvents||[]).filter(e => !rejectOutOfWindow(e, lockTime, dateFrom, expectedLeagueId));
    const cand = filtered.find(e => matchTeamsUnordered({
      providerHome:e.strHomeTeam, providerAway:e.strAwayTeam,
      teamAName, teamBName, teamACode, teamBCode
    }).ok);
    if (cand){ evSeason=cand; break; }
  }
} catch {}

// 3) idEvent direct lookup
if (idEventOpt){
  try { evResults = await v2LookupEventResults(idEventOpt); } catch {}
  try { evMeta    = await v2LookupEvent(idEventOpt); } catch {}

  if (evMeta && rejectOutOfWindow(evMeta, lockTime, dateFrom, expectedLeagueId)) evMeta=null;
  if (evResults && rejectOutOfWindow(evResults, lockTime, dateFrom, expectedLeagueId)) evResults=null;
}

// --- Build Sources ---
const sources=[];
if (evResults) sources.push({name:"results",ev:evResults});
if (evMeta)    sources.push({name:"meta",ev:evMeta});
if (evPrev)    sources.push({name:"prev",ev:evPrev});
if (evSeason)  sources.push({name:"season",ev:evSeason});
if (!sources.length) return Functions.encodeString("ERR");

// --- Require final + consistency ---
if (REQUIRE_RESULTS){
  if (!evResults) return Functions.encodeString("ERR");
  if (!looksFinal(evResults)) return Functions.encodeString("ERR");
}
for (const s of sources){
  const h=s.ev?.strHomeTeam, w=s.ev?.strAwayTeam;
  if (!h || !w) return Functions.encodeString("ERR");
  const m=matchTeamsUnordered({providerHome:h,providerAway:w,teamAName,teamBName,teamACode,teamBCode});
  if (!m.ok) return Functions.encodeString("ERR");
}
if (evResults && evMeta && !sameScorePair(evResults,evMeta)) return Functions.encodeString("ERR");
for (const s of sources){
  if (["prev","season"].includes(s.name)){
    if (evResults && s.ev?.intHomeScore!=null && s.ev?.intAwayScore!=null){
      if (!sameScorePair(s.ev,evResults)) return Functions.encodeString("ERR");
    }
  }
}

// --- Compute winners ---
const winners=[];
for (const s of sources){
  if (looksFinal(s.ev) && s.ev?.intHomeScore!=null && s.ev?.intAwayScore!=null){
    const code = winnerCodeFromSource(s.ev,teamAName,teamBName,teamACode,teamBCode);
    if (code==="TIE"||code===teamACode||code===teamBCode) winners.push({src:s.name,code});
    else return Functions.encodeString("ERR");
  }
}
if (!winners.length) return Functions.encodeString("ERR");
if (REQUIRE_CONSENSUS && winners.length<MIN_SOURCES) return Functions.encodeString("ERR");

// --- Consensus ---
const counts = winners.reduce((m,{code}) => (m[code]=(m[code]||0)+1,m),{});
let top=null,topCount=0;
for (const [code,count] of Object.entries(counts)){
  if (count>topCount){topCount=count;top=code;}
}
if (!top) return Functions.encodeString("ERR");
if (Object.keys(counts).length>1) return Functions.encodeString("ERR");

// ✅ Passed all filters
return Functions.encodeString(top);
