// === Chainlink Functions SOURCE CODE — v2 migration (prev/league + by-ID + results) ===
//
// Args:
// [0]=league(label), [1]=d0, [2]=d1, [3]=A code, [4]=B code, [5]=A name, [6]=B name, [7]=kickoff epoch, [8]=optional tsdbEventId
//
// Behavior:
// - Prefer by-ID via v2 `lookup/event` + `lookup/event_results`
// - Fallback to v2 `schedule/previous/league/{idLeague}` (10 most recent) and pick closest to kickoff
// - Finality detection via status + non-null scores
// - Returns winner code "A"/"B" (or "Tie") mapped to the caller's team codes
//
// Secrets expected: THESPORTSDB_API_KEY (preferred) or league-specific *_API_KEY

// ---------- Args ----------
const Lraw = (args[0] || "").trim();
const d0 = args[1];      // kept for logging/backcompat (not required by v2 flow)
const d1 = args[2];
const A = (args[3] || "").toUpperCase();
const B = (args[4] || "").toUpperCase();
const a = (args[5] || "").trim();
const b = (args[6] || "").trim();
const t = Number(args[7] || 0);
const tsdbEventIdRaw = (args.length >= 9 ? args[8] : null);
const tsdbEventId = tsdbEventIdRaw != null && tsdbEventIdRaw !== "" ? String(tsdbEventIdRaw) : null;

// ---------- League normalization ----------
function cleanLeagueLabel(s) {
  return decodeURIComponent(String(s || ""))
    .replace(/[_]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}
const L = cleanLeagueLabel(Lraw);
const lcn = L.toLowerCase();

function leagueTag(lc) {
  if (/\bmlb\b/.test(lc)) return "MLB";
  if (/\bnfl\b/.test(lc)) return "NFL";
  if (/\bnba\b/.test(lc)) return "NBA";
  if (/\bnhl\b/.test(lc)) return "NHL";
  if (/premier/.test(lc) || /\bepl\b/.test(lc)) return "EPL";
  if (/champions/.test(lc) || /\bucl\b/.test(lc)) return "UCL";
  return "GEN";
}
const TAG = leagueTag(lcn);

// ---------- League IDs (v2 uses idLeague) ----------
const LEAGUE_ID = {
  MLB: "4424",
  NFL: "4391",
  NBA: "4387",
  NHL: "4380",
  EPL: "4328",
  UCL: "4480",
};
function leagueIdFromTag(tag) { return LEAGUE_ID[tag] || null; }

// ---------- Secret selection ----------
function pickApiKey(tag) {
  const s = secrets || {};
  if (s.THESPORTSDB_API_KEY) return s.THESPORTSDB_API_KEY;
  switch (tag) {
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

// ---------- Debug helpers ----------
function maskKey(k) {
  if (!k) return "none";
  const s = String(k);
  if (s.length <= 4) return `len${s.length}:${s}`;
  return `len${s.length}:${s.slice(0,2)}…${s.slice(-2)}`;
}
function keyNameUsed(tag) {
  if (secrets?.THESPORTSDB_API_KEY) return "THESPORTSDB_API_KEY";
  switch (tag) {
    case "MLB": return secrets?.MLB_API_KEY ? "MLB_API_KEY" : "none";
    case "NFL": return secrets?.NFL_API_KEY ? "NFL_API_KEY" : "none";
    case "NBA": return secrets?.NBA_API_KEY ? "NBA_API_KEY" : "none";
    case "NHL": return secrets?.NHL_API_KEY ? "NHL_API_KEY" : "none";
    case "EPL": return secrets?.EPL_API_KEY ? "EPL_API_KEY" : "none";
    case "UCL": return secrets?.UCL_API_KEY ? "UCL_API_KEY" : "none";
    default:    return "none";
  }
}

console.log("SRC_VERSION: 2025-10-27T-v2-REV1");
console.log(JSON.stringify({
  leagueLabel: L,
  tag: TAG,
  leagueId: leagueIdFromTag(TAG),
  keyName: keyNameUsed(TAG),
  keyMask: maskKey(API),
  dates: { d0, d1 },
  kickoffEpoch: t,
  teamInputs: { a, b, A, B },
  tsdbEventId
}, null, 2));

// ---------- Normalization ----------
function norm(s) {
  return (s || "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}
function sameTeam(x, y) {
  const nx = norm(x), ny = norm(y);
  if (!nx || !ny) return false;
  if (nx === ny) return true;
  return nx.includes(ny) || ny.includes(nx);
}

// ---------- v2 HTTP helpers ----------
const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const V2_HEADERS = { "X-API-KEY": API };

async function httpJsonV2(url, label) {
  const r = await Functions.makeHttpRequest({ url, headers: V2_HEADERS });
  console.log(`HTTP[v2] ${label} status=${r?.status ?? "no"}`);
  return r?.data || null;
}

// ---------- v2 fetchers ----------
async function v2PreviousLeagueEvents(tag) {
  const id = leagueIdFromTag(tag);
  if (!id) return [];
  const url = `${V2_BASE}/schedule/previous/league/${id}`;
  const data = await httpJsonV2(url, `previous league ${tag}`);
  return Array.isArray(data?.events) ? data.events : [];
}

async function v2LookupEvent(idEvent) {
  const url = `${V2_BASE}/lookup/event/${encodeURIComponent(String(idEvent))}`;
  const data = await httpJsonV2(url, `lookup event ${idEvent}`);
  const ev = data?.events;
  return Array.isArray(ev) && ev.length ? ev[0] : null;
}

async function v2LookupEventResults(idEvent) {
  const url = `${V2_BASE}/lookup/event_results/${encodeURIComponent(String(idEvent))}`;
  const data = await httpJsonV2(url, `event_results ${idEvent}`);
  const ev = data?.results ?? data?.events ?? null;
  return Array.isArray(ev) && ev.length ? ev[0] : null;
}

// ---------- Time helper ----------
function eventEpochSec(e) {
  if (!e) return null;
  let x = e.strTimestamp || "";
  if (x) {
    const ms = Date.parse(x);
    if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  const de = e.dateEvent, tm = e.strTime;
  if (de && tm) {
    const s = /Z$/.test(tm) ? `${de}T${tm}` : `${de}T${tm}Z`;
    const ms = Date.parse(s);
    if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  if (de) {
    const ms = Date.parse(`${de}T00:00:00Z`);
    if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  return null;
}

// ---------- Pull data (v2 flow) ----------
let selectedEvent = null;
let selectedFrom = "none";

// Preferred: by-ID (+ promote to results if available)
if (tsdbEventId) {
  const ev = await v2LookupEvent(tsdbEventId);
  if (ev) {
    const res = await v2LookupEventResults(tsdbEventId);
    selectedEvent = res || ev;
    selectedFrom = res ? "byId+results" : "byId";
  }
}

// Fallback: within previous-league window, pick closest to kickoff and matching teams
if (!selectedEvent) {
  const recent = await v2PreviousLeagueEvents(TAG); // ~last 10
  const cand = recent.filter(e => {
    const h = e.strHomeTeam, w = e.strAwayTeam;
    return (sameTeam(h, a) && sameTeam(w, b)) || (sameTeam(h, b) && sameTeam(w, a));
  });
  cand.sort((x, y) => (Math.abs((eventEpochSec(x) || 1e15) - t) - Math.abs((eventEpochSec(y) || 1e15) - t)));
  selectedEvent = cand[0] || null;
  selectedFrom = selectedEvent ? "prevLeagueMatch" : "none";
}

if (!selectedEvent) throw Error("no_event");

// If we matched by ID but not via results, try to promote to results for better finality/status
if (tsdbEventId && selectedFrom === "byId" && selectedEvent?.idEvent) {
  const res = await v2LookupEventResults(selectedEvent.idEvent);
  if (res) { selectedEvent = res; selectedFrom = "byId+results"; }
}

// ---------- Finality & scoring ----------
function isFinal(ev) {
  const S = String(ev?.strStatus || "").toUpperCase();
  const P = String(ev?.strProgress || "");
  const hs = +ev?.intHomeScore, as = +ev?.intAwayScore;

  if (/^(FT|AOT|AET|PEN|FINISHED|FULL TIME)$/.test(S)) return true;
  if (/final|finished|ended|complete/i.test(S) || /full\s*time/i.test(S)) return true;
  if (Number.isFinite(hs) && Number.isFinite(as) && !S) return true; // scores present, empty status
  return false;
}

if (!isFinal(selectedEvent)) throw Error("not final");

const hs = +selectedEvent.intHomeScore, as = +selectedEvent.intAwayScore;
if (!Number.isFinite(hs) || !Number.isFinite(as)) throw Error("bad score");

// ---------- Winner (returns A/B/Tie) ----------
let W = "Tie";
if (hs > as) W = sameTeam(selectedEvent.strHomeTeam, a) ? A : B;
else if (as > hs) W = sameTeam(selectedEvent.strAwayTeam, a) ? A : B;

console.log(`WINNER: ${W} | from=${selectedFrom} | id=${selectedEvent.idEvent || "?"} | ${selectedEvent.strEvent || selectedEvent.strEventAlternate} | ${hs}-${as}`);
return Functions.encodeString(W);
