// === Chainlink Functions SOURCE CODE (works with multiple secret names) ===
const Lraw=(args[0]||'').trim(),
      d0=args[1],
      d1=args[2],
      A=(args[3]||'').toUpperCase(),
      B=(args[4]||'').toUpperCase(),
      a=(args[5]||'').trim(),
      b=(args[6]||'').trim(),
      t=Number(args[7]||0);

// --- League normalization (accepts "MLB", "EPL", "English Premier League", "UEFA_Champions_League", etc.)
function cleanLeagueLabel(s){
  return decodeURIComponent(String(s||""))
    .replace(/[_]+/g," ")
    .replace(/\s+/g," ")
    .trim();
}
const L = cleanLeagueLabel(Lraw);
const lcn = L.toLowerCase();

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

// --- Pick API key from secrets: prefer global THESPORTSDB_API_KEY, else league-specific
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

// --- Optional per-league endpoint override; default to TSDB public base
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

// --- Robust team normalization & matching
function norm(s){
  return (s||"")
    .normalize("NFD").replace(/[\u0300-\u036f]/g,"")
    .replace(/[’'`]/g,"")
    .replace(/[^a-z0-9 ]/gi," ")
    .replace(/\s+/g," ")
    .trim()
    .toLowerCase();
}
const alias = new Map(); // add soccer club aliases if you need them later
function sameTeam(x,y){
  const nx=norm(x), ny=norm(y);
  if (nx===ny) return true;
  const ax=alias.get(nx); if (ax && ax===ny) return true;
  const ay=alias.get(ny); if (ay && ay===nx) return true;
  return false;
}

// --- Fetch a day's events (with tiny retry on 429/5xx)
async function fetchDay(d, attempt=0){
  if(!d) return [];
  const url = `${BASE}/${API}/eventsday.php?d=${encodeURIComponent(d)}&l=${encodeURIComponent(L)}`;
  const r = await Functions.makeHttpRequest({ url });
  if ((r?.status >= 500 || r?.status === 429) && attempt < 1) {
    await new Promise(res=>setTimeout(res, 250));
    return fetchDay(d, attempt+1);
  }
  const ev = (r && r.data && r.data.events) || [];
  return Array.isArray(ev) ? ev : [];
}

// --- Event epoch helper for proximity sort
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

// --- Download & filter
const ev0 = await fetchDay(d0);
const ev1 = await fetchDay(d1);
const all = [...ev0, ...ev1];

let cand = all.filter(e=>{
  const h=e.strHomeTeam, w=e.strAwayTeam;
  return (sameTeam(h,a)&&sameTeam(w,b)) || (sameTeam(h,b)&&sameTeam(w,a));
});

// as a light fallback, check title strings
if(!cand.length){
  const la=norm(a), lb=norm(b);
  const alt = all.filter(e=>{
    const s1=norm(e.strEvent||""), s2=norm(e.strEventAlternate||"");
    return (s1.includes(la)||s2.includes(la)) && (s1.includes(lb)||s2.includes(lb));
  });
  if (alt.length) cand = alt;
}

if(!cand.length) throw Error(`no match (seen=${all.length}, a=${a}, b=${b})`);

// pick the event nearest to lock epoch t
cand.sort((x,y)=>{
  const dx=(ep(x)||1e15), dy=(ep(y)||1e15);
  return Math.abs(dx-t)-Math.abs(dy-t) || (dx-dy);
});

const e=cand[0];
const S=String(e.strStatus||'').toUpperCase();
const P=String(e.strProgress||'');
if(!(/^(FT|AOT|AET|PEN|FINISHED)$/.test(S) || /final/i.test(S) || /final/i.test(P))) {
  throw Error('not final');
}

const hs=+e.intHomeScore, as=+e.intAwayScore;
if(!Number.isFinite(hs)||!Number.isFinite(as)) throw Error('bad score');

// Decide winner (return A/B's code)
let W='Tie';
if(hs>as) W = sameTeam(e.strHomeTeam,a) ? A : B;
else if(as>hs) W = sameTeam(e.strAwayTeam,a) ? A : B;

return Functions.encodeString(W);
