// Chainlink Functions source script for BlockPools
// Output: winner team code (e.g., "PHI", "NYG") or "TIE" or "ERR"
// Order of operations (by design):
//  1) Find by teams + lockTime (league previous + season fallback with ET±1 day)
//  2) If not found, try idEvent (only accept if teams align)

const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const API_KEY = secrets.THESPORTSDB_API_KEY || "";

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
  const a = arrFrom(j, ["results","events","schedule"]);
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

/* ------------------------------ Matching utils ----------------------------- */

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

function tsFromEvent(e){
  if (e?.strTimestamp){ const ms=Date.parse(e.strTimestamp); if(!Number.isNaN(ms)) return (ms/1000)|0; }
  if (e?.dateEvent && e?.strTime){
    const s=/Z$/.test(e.strTime)?`${e.dateEvent}T${e.strTime}`:`${e.dateEvent}T${e.strTime}Z`;
    const ms=Date.parse(s); if(!Number.isNaN(ms)) return (ms/1000)|0;
  }
  if (e?.dateEvent){ const ms=Date.parse(`${e.dateEvent}T00:00:00Z`); if(!Number.isNaN(ms)) return (ms/1000)|0; }
  return 0;
}

// ET day helpers
function addDaysISO(iso, days){
  const [y,m,d]=iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m-1, d));
  dt.setUTCDate(dt.getUTCDate()+days);
  const y2=dt.getUTCFullYear(), m2=String(dt.getUTCMonth()+1).padStart(2,"0"), d2=String(dt.getUTCDate()).padStart(2,"0");
  return `${y2}-${m2}-${d2}`;
}
function matchesEtDayOrNeighbor(e, gameDateEt){
  const d  = e?.dateEvent || "";
  const dl = e?.dateEventLocal || "";
  const prev = addDaysISO(gameDateEt, -1);
  const next = addDaysISO(gameDateEt,  1);
  return (
    d === gameDateEt || dl === gameDateEt ||
    d === prev      || dl === prev      ||
    d === next      || dl === next
  );
}

// Kickoff-aware picker (45m tolerance)
function pickEvent(events, aName, bName, aCode, bCode, kickoff){
  const TOL = 45 * 60;
  const scored=[];
  for (const e of events){
    const h=e?.strHomeTeam, w=e?.strAwayTeam; if(!h||!w) continue;
    const aHome=strongTeamEq(aName,h,aCode), bAway=strongTeamEq(bName,w,bCode);
    const aAway=strongTeamEq(aName,w,aCode), bHome=strongTeamEq(bName,h,bCode);
    const align=Math.max(Math.min(aHome,bAway), Math.min(aAway,bHome));
    if (align>0){
      const ts=tsFromEvent(e) || (kickoff||0);
      const delta=Math.abs(ts-(kickoff||ts));
      const dist = Math.max(0, delta - TOL);
      scored.push({e,align,dist});
    }
  }
  scored.sort((x,y)=>(y.align-x.align)||(x.dist-y.dist));
  return scored.length?scored[0].e:null;
}

function looksFinal(ev){
  const status=String(ev?.strStatus??ev?.strProgress??"").toLowerCase();
  const hasScores=(ev?.intHomeScore!=null && ev?.intAwayScore!=null);
  if (/^(ft|aot|aet|pen|finished|full time)$/.test(status)) return true;
  if (/final|finished|ended|complete/.test(status)) return true;
  return hasScores && !status;
}

function decideWinnerCode(ev, teamAName, teamBName, teamACode, teamBCode){
  const hs=Number(ev?.intHomeScore||0), as=Number(ev?.intAwayScore||0);
  if (hs===as) return "TIE";
  const home=ev?.strHomeTeam||"", away=ev?.strAwayTeam||"";
  const aHome=strongTeamEq(teamAName,home,teamACode), bHome=strongTeamEq(teamBName,home,teamBCode);
  // if home wins, map whichever contract team aligns more strongly with home
  if (hs>as) return (aHome>=bHome) ? String(teamACode||"").toUpperCase() : String(teamBCode||"").toUpperCase();
  // away wins
  return (aHome>=bHome) ? String(teamBCode||"").toUpperCase() : String(teamACode||"").toUpperCase();
}

/* ---------------------------------- Entry ---------------------------------- */

const N = args.length;
if (N!==8 && N!==9) throw Error("8 or 9 args required");

const leagueLabel = args[0];
const dateFrom    = args[1]; // ET YYYY-MM-DD
const _dateTo     = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7]||0);
const idEventOpt  = N===9 ? String(args[8]||"") : "";

if (!API_KEY) return Functions.encodeString("ERR");

const idLeague = mapLeagueId(leagueLabel);
let ev = null;

// 1) TEAM/DATE SEARCH FIRST (preferred)
if (idLeague){
  // previous/league
  const prev = await v2PreviousLeagueEvents(idLeague);
  ev = pickEvent(prev, teamAName, teamBName, teamACode, teamBCode, lockTime);

  // season fallback with ET±1 day window (same-day slice preferred, else closest)
  if (!ev){
    const seasons = await v2ListSeasons(idLeague);
    for (const ssn of seasons.slice(-2).reverse()){
      const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
      if (!seasonEvents?.length) continue;
      const daySlice = seasonEvents.filter(e => matchesEtDayOrNeighbor(e, dateFrom));
      const pool = daySlice.length ? daySlice : seasonEvents;
      const cand = pickEvent(pool, teamAName, teamBName, teamACode, teamBCode, lockTime);
      if (cand){ ev=cand; break; }
    }
  }
}

// 2) ONLY IF NOT FOUND, TRY idEvent — and accept it only if teams align
if (!ev && idEventOpt){
  const cand = (await v2LookupEventResults(idEventOpt)) || (await v2LookupEvent(idEventOpt));
  if (cand){
    const aHome=strongTeamEq(teamAName, cand.strHomeTeam, teamACode);
    const bAway=strongTeamEq(teamBName, cand.strAwayTeam, teamBCode);
    const aAway=strongTeamEq(teamAName, cand.strAwayTeam, teamACode);
    const bHome=strongTeamEq(teamBName, cand.strHomeTeam, teamBCode);
    const align = Math.max(Math.min(aHome,bAway), Math.min(aAway,bHome));
    if (align>0) ev=cand; // good idEvent for our fixture
  }
}

if (!ev || !looksFinal(ev)) return Functions.encodeString("ERR");

return Functions.encodeString(decideWinnerCode(ev, teamAName, teamBName, teamACode, teamBCode));
