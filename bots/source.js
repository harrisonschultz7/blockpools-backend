// === Chainlink Functions SOURCE CODE (diagnostics + tolerant matching + by-ID support) ===

// ---------- Args ----------
// [0]=league, [1]=d0, [2]=d1, [3]=A code, [4]=B code, [5]=A name, [6]=B name, [7]=kickoff epoch, [8]=optional tsdbEventId
const Lraw=(args[0]||'').trim();
const d0=args[1];
const d1=args[2];
const A=(args[3]||'').toUpperCase();
const B=(args[4]||'').toUpperCase();
const a=(args[5]||'').trim();
const b=(args[6]||'').trim();
const t=Number(args[7]||0);
const tsdbEventIdRaw = (args.length >= 9 ? args[8] : null);
const tsdbEventId = tsdbEventIdRaw != null && tsdbEventIdRaw !== "" ? String(tsdbEventIdRaw) : null;

// ---------- League normalization ----------
function cleanLeagueLabel(s){
  return decodeURIComponent(String(s||""))
    .replace(/[_]+/g," ")
    .replace(/\s+/g," ")
    .trim();
}
const L = cleanLeagueLabel(Lraw);
const lcn = L.toLowerCase();

// tag used only for choosing per-league keys/endpoints
function leagueTag(lc){
  if (/\bmlb\b/.test(lc)) return "MLB";
  if (/\bnfl\b/.test(lc)) return "NFL";
  if (/\bnba\b/.test(lc)) return "NBA";
  if (/\bnhl\b/.test(lc)) return "NHL";
  if (/premier/.test(lc) || /\bepl\b/.test(lc)) return "EPL";
  if (/champions/.test(lc) || /\bucl\b/.test(lc)) return "UCL";
  return "GEN";
}
const TAG = leagueTag(lcn);

// ---------- Secret selection ----------
function pickApiKey(tag){
  const s = secrets || {};
  if (s.THESPORTSDB_API_KEY) return s.THESPORTSDB_API_KEY;
  switch(tag){
    case "MLB": return s.MLB_API_KEY || "";
    case "NFL": return s.NFL_API_KEY || "";
    case "NBA": return s.NBA_API_KEY || "";
    case "NHL": return s.NHL_API_KEY || "";
    case "EPL": return s.EPL_API_KEY || "";
    case "UCL": return s.UCL_API_KEY || "";
    default:    return "";
  }
}
const API = pickApiKey(TAG);
if (!API) throw Error("missing API key (expected THESPORTSDB_API_KEY or league-specific *_API_KEY)");

function pickEndpoint(tag){
  const s = secrets || {};
  const base = "https://www.thesportsdb.com/api/v1/json";
  const map = {
    MLB: s.MLB_ENDPOINT,
    NFL: s.NFL_ENDPOINT,
    NBA: s.NBA_ENDPOINT,
    NHL: s.NHL_ENDPOINT,
    EPL: s.EPL_ENDPOINT,
    UCL: s.UCL_ENDPOINT,
  };
  const ep = map[tag];
  const chosen = (typeof ep === "string" && ep.trim().length) ? ep.trim() : base;
  return chosen.replace(/\/+$/,""); // no trailing slash
}
const BASE = pickEndpoint(TAG);

// ---------- Debug helpers (safe masking) ----------
function maskKey(k) {
  if (!k) return "none";
  const s = String(k);
  if (s.length <= 4) return `len${s.length}:${s}`;
  return `len${s.length}:${s.slice(0,2)}…${s.slice(-2)}`;
}
function keyNameUsed(tag) {
  if (secrets?.THESPORTSDB_API_KEY) return "THESPORTSDB_API_KEY";
  switch(tag){
    case "MLB": return secrets?.MLB_API_KEY ? "MLB_API_KEY" : "none";
    case "NFL": return secrets?.NFL_API_KEY ? "NFL_API_KEY" : "none";
    case "NBA": return secrets?.NBA_API_KEY ? "NBA_API_KEY" : "none";
    case "NHL": return secrets?.NHL_API_KEY ? "NHL_API_KEY" : "none";
    case "EPL": return secrets?.EPL_API_KEY ? "EPL_API_KEY" : "none";
    case "UCL": return secrets?.UCL_API_KEY ? "UCL_API_KEY" : "none";
    default:    return "none";
  }
}

console.log("SRC_VERSION: 2025-10-15T-REV1");
console.log(JSON.stringify({
  leagueLabel: L,
  tag: TAG,
  baseEndpoint: BASE,
  keyName: keyNameUsed(TAG),
  keyMask: maskKey(API),
  dates: { d0, d1 },
  kickoffEpoch: t,
  teamInputs: { a, b, A, B },
  tsdbEventId
}, null, 2));

// ---------- Normalization & matching ----------
function norm(s){
  return (s||"")
    .normalize("NFD").replace(/[\u0300-\u036f]/g,"")
    .replace(/[’'`]/g,"")
    .replace(/[^a-z0-9 ]/gi," ")
    .replace(/\s+/g," ")
    .trim()
    .toLowerCase();
}

function sameTeam(x, y) {
  const nx = norm(x), ny = norm(y);
  if (!nx || !ny) return false;
  if (nx === ny) return true;
  return nx.includes(ny) || ny.includes(nx);
}

// ---------- HTTP helpers ----------
const DEFAULT_BASE = "https://www.thesportsdb.com/api/v1/json";
const cacheBust = () => `cb=${Date.now()}`;

async function httpJson(url, label){
  const r = await Functions.makeHttpRequest({ url });
  console.log(`HTTP ${label} status=${r?.status ?? "no"}`);
  return r?.data || null;
}

async function fetchDay(d){
  if(!d) return [];
  const attempts = [];
  attempts.push({ base: BASE, withLeague: true,  label: "chosen-base+key" });
  if (BASE !== DEFAULT_BASE) attempts.push({ base: DEFAULT_BASE, withLeague: true, label: "default-base+key" });
  attempts.push({ base: DEFAULT_BASE, withLeague: false, label: "default-base+key:no-league" });

  for (const at of attempts) {
    const base = at.base.replace(/\/+$/,"");
    const url = at.withLeague
      ? `${base}/${API}/eventsday.php?d=${encodeURIComponent(d)}&l=${encodeURIComponent(L)}&${cacheBust()}`
      : `${base}/${API}/eventsday.php?d=${encodeURIComponent(d)}&${cacheBust()}`;
    const data = await httpJson(url, `${at.label} d=${d} withL=${at.withLeague}`);
    const events = Array.isArray(data?.events) ? data.events : [];
    if (events.length) return events;
  }
  return [];
}

async function fetchById(id){
  const base = (BASE || DEFAULT_BASE).replace(/\/+$/,"");
  const url  = `${base}/${API}/lookupevent.php?id=${encodeURIComponent(String(id))}&${cacheBust()}`;
  const data = await httpJson(url, `lookupevent id=${id}`);
  const ev = data?.events;
  if (Array.isArray(ev) && ev.length) return ev[0];
  return null;
}

// ---------- Pull data ----------
let selectedEvent = null;
let selectedFrom = "none";

// Preferred path: by-ID if provided
if (tsdbEventId) {
  const ev = await fetchById(tsdbEventId);
  if (ev) {
    selectedEvent = ev;
    selectedFrom = "byId";
  }
}

// Fallback path: day slice(s) + tolerant matching
if (!selectedEvent) {
  const ev0 = await fetchDay(d0);
  const ev1 = await fetchDay(d1);
  console.log(`DBG counts: d0=${ev0.length} d1=${ev1.length}`);
  const all = [...ev0, ...ev1];
  console.log("DBG titles (sample):", all.slice(0,6).map(e => e.strEvent || e.strEventAlternate));

  let cand = all.filter(e=>{
    const h=e.strHomeTeam, w=e.strAwayTeam;
    return (sameTeam(h,a)&&sameTeam(w,b)) || (sameTeam(h,b)&&sameTeam(w,a));
  });

  if(!cand.length){
    const la=norm(a), lb=norm(b);
    const alt = all.filter(e=>{
      const s1=norm(e.strEvent||""), s2=norm(e.strEventAlternate||"");
      return (s1.includes(la)||s2.includes(la)) && (s1.includes(lb)||s2.includes(lb));
    });
    if (alt.length) cand = alt;
  }

  if(!cand.length){
    console.log("Available events:", all.map(e => e.strEvent || e.strEventAlternate));
    console.log("Looking for:", a, "vs", b);
    console.log("Normalized A:", norm(a), "B:", norm(b));
    throw Error(`no match (seen=${all.length}, a=${a}, b=${b})`);
  }

  function ep(e){
    let x=e.strTimestamp||'';
    if(x){ x=Date.parse(x); if(!Number.isNaN(x)) return (x/1000)|0; }
    const de=e.dateEvent, tm=e.strTime;
    if(de&&tm){
      let i=`${de}T${tm}`;
      if(!/Z$/.test(i)) i+='Z';
      x=Date.parse(i);
      if(!Number.isNaN(x)) return (x/1000)|0;
    }
    if(de){
      x=Date.parse(`${de}T00:00:00Z`);
      if(!Number.isNaN(x)) return (x/1000)|0;
    }
    return null;
  }

  cand.sort((x,y)=>{
    const dx=(ep(x)||1e15), dy=(ep(y)||1e15);
    return Math.abs(dx-t)-Math.abs(dy-t) || (dx-dy);
  });

  selectedEvent = cand[0];
  selectedFrom = "dayMatch";
}

// ---------- Finality & scoring ----------
function isFinal(ev){
  const S=String(ev?.strStatus||'').toUpperCase();
  const P=String(ev?.strProgress||'');
  if (/^(FT|AOT|AET|PEN|FINISHED)$/.test(S)) return true;
  if (/final/i.test(S) || /final/i.test(P)) return true;

  // Some feeds return empty status but non-null scores when ended
  const hs=+ev?.intHomeScore, as=+ev?.intAwayScore;
  if(Number.isFinite(hs) && Number.isFinite(as) && !S) return true;

  return false;
}

if (!selectedEvent) throw Error("no_event");

// Optional sport-specific extra guard (MLB)
if (TAG === "MLB") {
  // If TSDB provides innings/progress strings, you could enforce innings>=9 here.
  // Kept lenient due to field variability; rely on consensus gate in the bot.
}

if (!isFinal(selectedEvent)) {
  throw Error('not final');
}

const hs=+selectedEvent.intHomeScore, as=+selectedEvent.intAwayScore;
if(!Number.isFinite(hs)||!Number.isFinite(as)) throw Error('bad score');

// ---------- Winner (return codes A/B) ----------
let W='Tie';
if(hs>as) W = sameTeam(selectedEvent.strHomeTeam,a) ? A : B;
else if(as>hs) W = sameTeam(selectedEvent.strAwayTeam,a) ? A : B;

console.log(`WINNER: ${W} | from=${selectedFrom} | ${selectedEvent.strEvent || selectedEvent.strEventAlternate} | ${hs}-${as}`);
return Functions.encodeString(W);
