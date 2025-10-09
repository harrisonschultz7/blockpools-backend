// === Chainlink Functions SOURCE CODE (runs off-chain on DON) ===
// Put exactly what you had in SOURCE_CODE here (without wrapping quotes).
const L=(args[0]||'').trim(),d0=args[1],d1=args[2],A=(args[3]||'').toUpperCase(),B=(args[4]||'').toUpperCase(),
a=(args[5]||'').trim(),b=(args[6]||'').trim(),t=Number(args[7]||0);
const f=s=>(s||'').trim().toLowerCase();
async function q(d){if(!d)return[];const u=`https://www.thesportsdb.com/api/v1/json/${secrets.API_KEY}/eventsday.php?d=${d}&l=${L}`;
const r=await Functions.makeHttpRequest({url:u});const e=(r&&r.data&&r.data.events)||[];return Array.isArray(e)?e:[]}
function ep(e){let x=e.strTimestamp||'';if(x){x=Date.parse(x);if(!Number.isNaN(x))return (x/1000)|0}
const de=e.dateEvent,tm=e.strTime;if(de&&tm){let i=`${de}T${tm}`;if(!/Z$/.test(i))i+='Z';x=Date.parse(i);if(!Number.isNaN(x))return (x/1000)|0}
if(de){x=Date.parse(`${de}T00:00:00Z`);if(!Number.isNaN(x))return (x/1000)|0}return null}
const E=[...(await q(d0)),...(await q(d1))].filter(e=>{const h=f(e.strHomeTeam),w=f(e.strAwayTeam);return(h===f(a)&&w===f(b))||(h===f(b)&&w===f(a))});
if(!E.length)throw Error('no match');
E.sort((x,y)=>{const dx=(ep(x)||1e15),dy=(ep(y)||1e15);return Math.abs(dx-t)-Math.abs(dy-t)||(dx-dy)});
const e=E[0],S=String(e.strStatus||'').toUpperCase(),P=String(e.strProgress||'');
if(!(/^(FT|AOT|AET|PEN|FINISHED)$/.test(S)||/final/i.test(S)||/final/i.test(P)))throw Error('not final');
const hs=+e.intHomeScore,as=+e.intAwayScore;if(!Number.isFinite(hs)||!Number.isFinite(as))throw Error('bad score');
let W='Tie';if(hs>as)W=(f(e.strHomeTeam)===f(a))?A:B;else if(as>hs)W=(f(e.strAwayTeam)===f(a))?A:B;
return Functions.encodeString(W);
