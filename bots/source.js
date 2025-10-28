// Chainlink Functions source script (runs inside DON sandbox)
// Accepts 8 or 9 args:
// 0: leagueLabel ("NFL","NBA","EPL", etc.)
// 1: dateFrom (YYYY-MM-DD, ET window start)
// 2: dateTo   (YYYY-MM-DD, ET window end) — not always used directly
// 3: teamACode
// 4: teamBCode
// 5: teamAName (human readable)
// 6: teamBName
// 7: lockTime  (unix seconds)
// 8: idEvent   (optional; when present we’ll try direct lookup first)

const leagueMap = {
  mlb: "4424", nfl: "4391", nba: "4387", nhl: "4380",
  epl: "4328", ucl: "4480",
};

function mapLeagueId(leagueLabel) {
  const lk = String(leagueLabel || "").toLowerCase();
  return leagueMap[lk] || "";
}

// v2 base & tolerant header variants (TSDB sometimes accepts X_API_KEY)
const V2_BASE = "https://www.thesportsdb.com/api/v2/json";
const API_KEY = secrets.THESPORTSDB_API_KEY || "";
const headerVariants = [
  { "X-API-KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
  { "X-API-KEY": API_KEY, "X_API_KEY": API_KEY, "Accept": "application/json", "User-Agent": "blockpools-functions/1.0" },
];

// Helpers to normalize TSDB v2 responses
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
      if (!r || !r.data) continue;
      const data = r.data;
      // Consider non-empty object as success
      if (data && Object.keys(data).length) return data;
    } catch (e) {
      // swallow and try next header variant
    }
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

async function v2PreviousTeamEvents(idTeam) {
  if (!idTeam) return [];
  const j = await v2Fetch(`/schedule/previous/team/${encodeURIComponent(idTeam)}`);
  return firstArrayByKeys(j, ["schedule", "events"]);
}

async function v2ListSeasons(idLeague) {
  if (!idLeague) return [];
  const j = await v2Fetch(`/list/seasons/${idLeague}`);
  const arr = firstArrayByKeys(j, ["seasons"]);
  return arr.map((s) => String(s?.strSeason || s)).filter(Boolean);
}

async function v2ScheduleLeagueSeason(idLeague, season) {
  if (!idLeague || !season) return [];
  const j = await v2Fetch(`/schedule/league/${idLeague}/${encodeURIComponent(season)}`);
  return firstArrayByKeys(j, ["schedule", "events"]);
}

// team mapping helpers
function strip(s) {
  return (s || "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}
function sameTeam(x, y) {
  const a = strip(x), b = strip(y);
  if (!a || !b) return false;
  return a === b || a.includes(b) || b.includes(a);
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
  return hasScores && !status;
}

// === ENTRY ===
const N = args.length;
if (N !== 8 && N !== 9) {
  throw Error("8 or 9 args required");
}

const leagueLabel = args[0];
const dateFrom    = args[1];
const dateTo      = args[2];
const teamACode   = args[3];
const teamBCode   = args[4];
const teamAName   = args[5];
const teamBName   = args[6];
const lockTime    = Number(args[7] || 0);
const idEventOpt  = N === 9 ? String(args[8] || "") : "";

if (!API_KEY) {
  throw Error("missing THESPORTSDB_API_KEY in secrets");
}

const idLeague = mapLeagueId(leagueLabel);
let eventObj = null;
let mark = "none";

// 1) Prefer by explicit idEvent when provided
if (idEventOpt) {
  const byRes = await v2LookupEventResults(idEventOpt);
  eventObj = byRes || await v2LookupEvent(idEventOpt);
  mark = eventObj ? "id_lookup" : "id_missing_v2";
}

// 2) previous/league match by names near kickoff
if (!eventObj && idLeague) {
  const prev = await v2PreviousLeagueEvents(idLeague);
  const cand = pickClosestByKickoff(prev, teamAName, teamBName, lockTime);
  if (cand) { eventObj = cand; mark = "prev_league_match"; }
}

// 3) season day slice as a wider fallback (handles >10 rolling)
if (!eventObj && idLeague) {
  const seasons = await v2ListSeasons(idLeague);
  for (const ssn of seasons.slice(-2).reverse()) {
    const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
    if (!seasonEvents?.length) continue;
    const daySlice = seasonEvents.filter((e) => (e?.dateEvent || e?.dateEventLocal) === dateFrom);
    const candidate = pickClosestByKickoff(daySlice.length ? daySlice : seasonEvents, teamAName, teamBName, lockTime);
    if (candidate) { eventObj = candidate; mark = daySlice.length ? "season_day_match" : "season_closest"; break; }
  }
}

if (!eventObj) {
  return Functions.encodeString(JSON.stringify({ ok: false, reason: "no_event", mark }));
}

if (!looksFinal(eventObj)) {
  return Functions.encodeString(JSON.stringify({ ok: false, reason: "not_final", mark, idEvent: eventObj.idEvent || "" }));
}

// Normalize a simple return payload your fulfillment can parse
const payload = {
  ok: true,
  mark,
  idEvent: String(eventObj.idEvent || ""),
  home: {
    id: String(eventObj.idHomeTeam || ""),
    name: String(eventObj.strHomeTeam || ""),
    score: eventObj.intHomeScore != null ? Number(eventObj.intHomeScore) : null,
  },
  away: {
    id: String(eventObj.idAwayTeam || ""),
    name: String(eventObj.strAwayTeam || ""),
    score: eventObj.intAwayScore != null ? Number(eventObj.intAwayScore) : null,
  },
  status: String(eventObj.strStatus || eventObj.strProgress || ""),
  dateEvent: String(eventObj.dateEvent || eventObj.dateEventLocal || ""),
  strTimestamp: String(eventObj.strTimestamp || ""),
};

return Functions.encodeString(JSON.stringify(payload));
