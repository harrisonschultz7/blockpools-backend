// ---- Instrumented SOURCE for settlement ----
const L=(args[0]||'').trim(),
      d0=args[1],
      d1=args[2],
      A=(args[3]||'').toUpperCase(),
      B=(args[4]||'').toUpperCase(),
      a=(args[5]||'').trim(),
      b=(args[6]||'').trim(),
      t=Number(args[7]||0);

function norm(s){
  return (s||"")
    .normalize("NFD").replace(/[\u0300-\u036f]/g,"")   // strip accents
    .replace(/[’'`]/g,"")                              // drop apostrophes/quotes
    .replace(/[^a-z0-9 ]/gi," ")                       // drop punctuation
    .replace(/\s+/g," ")                               // collapse spaces
    .trim()
    .toLowerCase();
}
const alias = new Map(); // add soccer aliases later if needed
function sameTeam(x,y){
  const nx=norm(x), ny=norm(y);
  if (nx===ny) return true;
  const ax=alias.get(nx); if (ax && ax===ny) return true;
  const ay=alias.get(ny); if (ay && ay===nx) return true;
  return false;
}

async function fetchDay(d){
  if(!d) return [];
  const u=`https://www.thesportsdb.com/api/v1/json/${secrets.API_KEY}/eventsday.php?d=${encodeURIComponent(d)}&l=${encodeURIComponent(L)}`;
  const r=await Functions.makeHttpRequest({url:u});
  const ev=(r && r.data && r.data.events) || [];
  return Array.isArray(ev) ? ev : [];
}

function epoch(e){
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

const ev0 = await fetchDay(d0);
const ev1 = await fetchDay(d1);

// --- diagnostics ---
console.log(`DBG league=${L} d0=${d0} d1=${d1} A=${A} B=${B} a=${a} b=${b} t=${t}`);
console.log(`DBG counts: d0=${ev0.length} d1=${ev1.length}`);
if (ev0.length) {
  console.log(`DBG sample d0: ${ev0.slice(0,3).map(e=>`${e.strHomeTeam} vs ${e.strAwayTeam} [${e.dateEvent}]`).join(" | ")}`);
}
if (ev1.length) {
  console.log(`DBG sample d1: ${ev1.slice(0,3).map(e=>`${e.strHomeTeam} vs ${e.strAwayTeam} [${e.dateEvent}]`).join(" | ")}`);
}

const all = [...ev0, ...ev1];

const candidates = all.filter(e => {
  const h=e.strHomeTeam, w=e.strAwayTeam;
  return (sameTeam(h,a)&&sameTeam(w,b)) || (sameTeam(h,b)&&sameTeam(w,a));
});

if(!candidates.length){
  // extra: try strEvent/Alternate as a very light fallback
  const la = norm(a), lb = norm(b);
  const alt = all.filter(e=>{
    const s1 = norm(e.strEvent||"");
    const s2 = norm(e.strEventAlternate||"");
    const hasA = s1.includes(la) || s2.includes(la);
    const hasB = s1.includes(lb) || s2.includes(lb);
    return hasA && hasB;
  });

  if(!alt.length){
    throw Error(`no match (seen=${all.length}, a=${a}, b=${b})`);
  } else {
    candidates.push(...alt);
  }
}

candidates.sort((x,y)=>{
  const dx=(epoch(x)||1e15), dy=(epoch(y)||1e15);
  return Math.abs(dx-t)-Math.abs(dy-t) || (dx-dy);
});

const e=candidates[0];
const S=String(e.strStatus||'').toUpperCase();
const P=String(e.strProgress||'');
if(!(/^(FT|AOT|AET|PEN|FINISHED)$/.test(S) || /final/i.test(S) || /final/i.test(P))) {
  throw Error('not final');
}

const hs=+e.intHomeScore, as=+e.intAwayScore;
if(!Number.isFinite(hs)||!Number.isFinite(as)) throw Error('bad score');

let W='Tie';
if(hs>as) W = sameTeam(e.strHomeTeam,a) ? A : B;
else if(as>hs) W = sameTeam(e.strAwayTeam,a) ? A : B;

console.log(`DBG chosen: ${e.strHomeTeam} ${hs} - ${as} ${e.strAwayTeam} -> ${W}`);
return Functions.encodeString(W);
