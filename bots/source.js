const L=(args[0]||'').trim(),d0=args[1],d1=args[2],A=(args[3]||'').toUpperCase(),B=(args[4]||'').toUpperCase(),
a=(args[5]||'').trim(),b=(args[6]||'').trim(),t=Number(args[7]||0);

// STRONGER normalizer than trim+lowercase
function norm(s){
  return (s||"")
    .normalize("NFD").replace(/[\u0300-\u036f]/g,"")   // strip accents
    .replace(/[’'`]/g,"")                              // drop apostrophes/quotes
    .replace(/[^a-z0-9 ]/gi," ")                       // drop punctuation
    .replace(/\s+/g," ")                               // collapse spaces
    .trim()
    .toLowerCase();
}

// Alias hook (empty for MLB; you can add soccer clubs later)
const alias = new Map();
function sameTeam(x,y){
  const nx=norm(x), ny=norm(y);
  if (nx===ny) return true;
  const ax=alias.get(nx); if (ax && ax===ny) return true;
  const ay=alias.get(ny); if (ay && ay===nx) return true;
  return false;
}

async function q(d){
  if(!d) return [];
  const u=`https://www.thesportsdb.com/api/v1/json/${secrets.API_KEY}/eventsday.php?d=${encodeURIComponent(d)}&l=${encodeURIComponent(L)}`;
  const r=await Functions.makeHttpRequest({url:u});
  const e=(r&&r.data&&r.data.events)||[];
  return Array.isArray(e)?e:[];
}

function ep(e){
  let x=e.strTimestamp||'';
  if(x){ x=Date.parse(x); if(!Number.isNaN(x)) return (x/1000)|0 }
  const de=e.dateEvent, tm=e.strTime;
  if(de&&tm){ let i=`${de}T${tm}`; if(!/Z$/.test(i)) i+='Z'; x=Date.parse(i); if(!Number.isNaN(x)) return (x/1000)|0 }
  if(de){ x=Date.parse(`${de}T00:00:00Z`); if(!Number.isNaN(x)) return (x/1000)|0 }
  return null;
}

// Only change: use sameTeam() instead of raw f() equality
const E=[...(await q(d0)), ...(await q(d1))].filter(e=>{
  const h=e.strHomeTeam, w=e.strAwayTeam;
  return (sameTeam(h,a)&&sameTeam(w,b)) || (sameTeam(h,b)&&sameTeam(w,a));
});

if(!E.length) throw Error('no match');

E.sort((x,y)=>{
  const dx=(ep(x)||1e15), dy=(ep(y)||1e15);
  return Math.abs(dx-t)-Math.abs(dy-t) || (dx-dy);
});

const e=E[0], S=String(e.strStatus||'').toUpperCase(), P=String(e.strProgress||'');
if(!(/^(FT|AOT|AET|PEN|FINISHED)$/.test(S) || /final/i.test(S) || /final/i.test(P))) throw Error('not final');

const hs=+e.intHomeScore, as=+e.intAwayScore;
if(!Number.isFinite(hs)||!Number.isFinite(as)) throw Error('bad score');

// Winner compute unchanged, just use sameTeam() for the side check
let W='Tie';
if(hs>as) W = sameTeam(e.strHomeTeam,a) ? A : B;
else if(as>hs) W = sameTeam(e.strAwayTeam,a) ? A : B;

return Functions.encodeString(W);
