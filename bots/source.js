// Chainlink Functions source script for BlockPools
// Returns a tiny string to keep callback gas low:
//   "<winner>|<idEvent>|<homeScore>|<awayScore>"
// where winner is "A" or "B" mapped to your contract's A/B (not home/away).

const leagueMap = {
  mlb: "4424", nfl: "4391", nba: "4387", nhl: "4380",
  epl: "4328", ucl: "4480",
};
const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const API_KEY = secrets.THESPORTSDB_API_KEY || "";

// Some deployments accept either X-API-KEY or X_API_KEY
const headerVariants = [
  { "X-API-KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X-API-KEY": API_KEY, "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
];

function mapLeagueId(lbl) {
  const k = String(lbl || "").toLowerCase();
  return leagueMap[k] || "";
}

// ---------- helpers ----------
function firstArrayByKeys(j, keys) {
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
  return firstArrayByKeys(j, ["schedule", "events"]);
}
async function v2LookupEvent(idEvent) {
  const j = await v2Fetch(`/lookup/event/${encodeURIComponent(String(idEvent))}`);
  const arr = firstArrayByKeys(j, ["events", "schedule", "results", "lookup"]);
  return arr.length ? arr[0] : null;
}
async function v2LookupEventResults(idEvent) {
  const j = await v2Fetch(`/lookup/event_results/${encodeURIComponent(String(idEvent))}`);
  const arr = firstArrayByKeys(j, ["results", "events", "schedule"]);
  return arr.length ? arr[0] : null;
}
async function v2ListSeasons(idLeague) {
  const j = await v2Fetch(`/list/seasons/${idLeague}`);
  const arr = firstArrayByKeys(j, ["seasons"]);
  return arr.map((s) => String(s?.strSeason || s)).filter(Boolean);
}
async function v2ScheduleLeagueSeason(idLeague, season) {
  const j = await v2Fetch(`/schedule/league/${idLeague}/${encodeURIComponent(season)}`);
  return firstArrayByKeys(j, ["schedule", "events"]);
}

function norm(s) {
  return (s || "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}
function sameTeam(a, b) {
  const x = norm(a), y = norm(b);
  if (!x || !y) return false;
  return x === y || x.includes(y) || y.includes(x);
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
function pickClosestByKickoff(events, aName, bName, kickoff) {
  const cand = events.filter((e) => {
    const h = e?.strHomeTeam, w = e?.strAwayTeam;
    return (sameTeam(h, aName) && sameTeam(w, bName)) || (sameTeam(h, bName) && sameTeam(w, aName));
  });
  cand.sort((x, y) => Math.abs(tsFromEvent(x) - kickoff) - Math.abs(tsFromEvent(y) - kickoff));
  return cand[0] || null;
}
function looksFinal(ev) {
  const status = String(ev?.strStatus ?? ev?.strProgress ?? "").toLowerCase();
  const hasScores = (ev?.intHomeScore != null && ev?.intAwayScore != null);
  if (/^(ft|aot|aet|pen|finished|full time)$/.test(status)) return true;
  if (/final|finished|ended|complete/.test(status)) return true;
  return hasScores && !status; // some feeds omit status when final but scores set
}

// Map event (home/away) winner to contract "A" or "B"
function winnerABFromEvent(ev, teamAName, teamBName, teamACode, teamBCode) {
  const homeName = ev?.strHomeTeam || "";
  const awayName = ev?.strAwayTeam || "";
  const hs = ev?.intHomeScore != null ? Number(ev.intHomeScore) : null;
  const as = ev?.intAwayScore != null ? Number(ev.intAwayScore) : null;

  if (hs == null || as == null) return { winner: "", hs: 0, as: 0 };

  // Determine which contract side (A/B) is home/away.
  // Prefer name match, then fall back to codes.
  const homeIsA = sameTeam(homeName, teamAName) || sameTeam(homeName, teamACode);
  const homeIsB = sameTeam(homeName, teamBName) || sameTeam(homeName, teamBCode);
  const awayIsA = sameTeam(awayName, teamAName) || sameTeam(awayName, teamACode);
  const awayIsB = sameTeam(awayName, teamBName) || sameTeam(awayName, teamBCode);

  // If we can confidently map:
  if (homeIsA || awayIsA || homeIsB || awayIsB) {
    // Winner by score
    if (hs > as) {
      // Home wins
      if (homeIsA) return { winner: "A", hs, as };
      if (homeIsB) return { winner: "B", hs, as };
    } else if (as > hs) {
      // Away wins
      if (awayIsA) return { winner: "A", hs, as };
      if (awayIsB) return { winner: "B", hs, as };
    } else {
      // draw/tie
      return { winner: "D", hs, as }; // if your contract treats draws specially
    }
  }

  // Fallback: do a looser compare using names only
  if (sameTeam(homeName, teamAName)) {
    if (hs > as) return { winner: "A", hs, as };
    if (as > hs) return { winner: "B", hs, as };
    return { winner: "D", hs, as };
  }
  if (sameTeam(homeName, teamBName)) {
    if (hs > as) return { winner: "B", hs, as };
    if (as > hs) return { winner: "A", hs, as };
    return { winner: "D", hs, as };
  }
  if (sameTeam(awayName, teamAName)) {
    if (as > hs) return { winner: "A", hs, as };
    if (hs > as) return { winner: "B", hs, as };
    return { winner: "D", hs, as };
  }
  if (sameTeam(awayName, teamBName)) {
    if (as > hs) return { winner: "B", hs, as };
    if (hs > as) return { winner: "A", hs, as };
    return { winner: "D", hs, as };
  }

  // Unknown mapping
  return { winner: "", hs, as };
}

// ---------- ENTRY ----------
const N = args.length;
if (N !== 8 && N !== 9) {
  throw Error("8 or 9 args required");
}

const leagueLabel = args[0];
const dateFrom    = args[1]; // ET Y-M-D (used in season day slice)
const _dateTo     = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7] || 0);
const idEventOpt  = N === 9 ? String(args[8] || "") : "";

if (!API_KEY) throw Error("missing THESPORTSDB_API_KEY in secrets");

const idLeague = mapLeagueId(leagueLabel);
let ev = null;

// 1) direct ID (results first, then event)
if (idEventOpt) {
  ev = (await v2LookupEventResults(idEventOpt)) || (await v2LookupEvent(idEventOpt));
}

// 2) previous/league by kickoff proximity
if (!ev && idLeague) {
  const prev = await v2PreviousLeagueEvents(idLeague);
  ev = pickClosestByKickoff(prev, teamAName, teamBName, lockTime);
}

// 3) season schedule day-slice fallback (handles >10 previous)
if (!ev && idLeague) {
  const seasons = await v2ListSeasons(idLeague);
  for (const ssn of seasons.slice(-2).reverse()) {
    const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
    if (!seasonEvents?.length) continue;
    const daySlice = seasonEvents.filter((e) => (e?.dateEvent || e?.dateEventLocal) === dateFrom);
    const cand = pickClosestByKickoff(daySlice.length ? daySlice : seasonEvents, teamAName, teamBName, lockTime);
    if (cand) { ev = cand; break; }
  }
}

if (!ev) {
  // tiny error string to keep gas low
  return Functions.encodeString("ERR|no_event|||");
}

if (!looksFinal(ev)) {
  return Functions.encodeString(`ERR|not_final|${String(ev.idEvent||"")}|${ev.intHomeScore ?? ""}|${ev.intAwayScore ?? ""}`);
}

// Winner → A/B mapping
const { winner, hs, as } = winnerABFromEvent(ev, teamAName, teamBName, teamACode, teamBCode);
if (!winner) {
  // Couldn’t map to A/B (names changed etc.)
  return Functions.encodeString(`ERR|map_fail|${String(ev.idEvent||"")}|${hs}|${as}`);
}

// SUCCESS — tiny payload
return Functions.encodeString(`${winner}|${String(ev.idEvent||"")}|${hs}|${as}`);
