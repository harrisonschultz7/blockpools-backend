// @ts-nocheck
// Hardened Chainlink Functions source.js for BlockPools
// Ensures correct matchup, same-day timestamp window, and finality before returning a winner.

const MAX_EVENT_DRIFT_SECS = 2 * 3600;   // ±2 h drift allowed
const REQUIRE_SAME_DAY = true;           // must be same calendar day UTC
const REQUIRE_RESULTS = true;            // require lookup/event_results to be final

/* ----------------------------- Helper functions ---------------------------- */

function looksFinal(ev) {
  const s = String(ev?.strStatus ?? ev?.strProgress ?? "").toLowerCase();
  const hasScores =
    ev?.intHomeScore != null && ev?.intAwayScore != null && ev.intHomeScore !== "" && ev.intAwayScore !== "";
  if (/^(ft|aot|aet|pen|finished|full time)$/.test(s)) return true;
  if (/final|finished|ended|complete/.test(s)) return true;
  return hasScores && !s;
}

function tsFromEvent(e) {
  if (e?.strTimestamp) {
    const ms = Date.parse(e.strTimestamp);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  if (e?.dateEvent && e?.strTime) {
    const s = /Z$/.test(e.strTime) ? `${e.dateEvent}T${e.strTime}` : `${e.dateEvent}T${e.strTime}Z`;
    const ms = Date.parse(s);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  if (e?.dateEvent) {
    const ms = Date.parse(`${e.dateEvent}T00:00:00Z`);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  return 0;
}

function sameDayUTC(tsA, tsB) {
  const a = new Date(tsA * 1000).toISOString().slice(0, 10);
  const b = new Date(tsB * 1000).toISOString().slice(0, 10);
  return a === b;
}

function withinStrictWindow(eventTs, lockTime) {
  if (!eventTs || !lockTime) return false;
  if (REQUIRE_SAME_DAY && !sameDayUTC(eventTs, lockTime)) return false;
  if (Math.abs(eventTs - lockTime) > MAX_EVENT_DRIFT_SECS) return false;
  return true;
}

function normTeam(s) {
  return (s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function unorderedTeamsMatch(ev, aName, bName, aCode, bCode) {
  const home = normTeam(ev?.strHomeTeam);
  const away = normTeam(ev?.strAwayTeam);
  const A1 = normTeam(aName) || normTeam(aCode);
  const B1 = normTeam(bName) || normTeam(bCode);
  const set1 = new Set([home, away]);
  const set2 = new Set([A1, B1]);
  if (set1.size !== set2.size) return false;
  for (const v of set1) if (!set2.has(v)) return false;
  return true;
}

/* ------------------------------ API functions ------------------------------ */

async function fetchJson(url) {
  try {
    const res = await Functions.makeHttpRequest({ url });
    if (res.error) throw new Error(res.error);
    return res.data;
  } catch (e) {
    console.log("[ERR] fetchJson", e.message);
    return null;
  }
}

/* ------------------------------- Main logic -------------------------------- */

async function main(args) {
  // args8 or args9
  const [league, dateFrom, dateTo, teamAcode, teamBcode, teamAname, teamBname, lockTimeStr, eventIdMaybe] = args;
  const lockTime = Number(lockTimeStr || 0);
  const eventId = eventIdMaybe && !isNaN(Number(eventIdMaybe)) ? eventIdMaybe : "";

  const apiKey = Secrets.get("THESPORTSDB_API_KEY");
  const base = "https://www.thesportsdb.com/api/v2/json/" + apiKey;

  const idLeagueMap = {
    nfl: "4391",
    mlb: "4424",
    nba: "4387",
    nhl: "4380",
    epl: "4328",
    ucl: "4480",
  };
  const idLeague = idLeagueMap[String(league).toLowerCase()] || "";

  console.log(`[START] ${league} ${teamAname} vs ${teamBname} lock=${lockTime}`);

  let ev = null;

  // Try 1) direct id lookup
  if (eventId) {
    const r1 = await fetchJson(`${base}/lookup/event_results/${eventId}`);
    const r2 = await fetchJson(`${base}/lookup/event/${eventId}`);
    ev =
      (r1 && (r1.results?.[0] || r1.lookup?.[0])) ||
      (r2 && (r2.lookup?.[0] || r2.results?.[0])) ||
      null;
  }

  // Try 2) previous league slice
  if (!ev && idLeague) {
    const j = await fetchJson(`${base}/schedule/previous/league/${idLeague}`);
    const arr = j?.schedule || j?.events || [];
    for (const e of arr) {
      if (!unorderedTeamsMatch(e, teamAname, teamBname, teamAcode, teamBcode)) continue;
      const eTs = tsFromEvent(e);
      if (!withinStrictWindow(eTs, lockTime)) continue;
      ev = e;
      break;
    }
  }

  if (!ev) {
    console.log("[NO MATCH]");
    return Functions.encodeString("ERR");
  }

  // Ensure final and valid
  if (!looksFinal(ev)) {
    console.log("[NOT FINAL]", ev.strStatus);
    return Functions.encodeString("ERR");
  }

  // Check timestamp window again strictly
  const eTs = tsFromEvent(ev);
  if (!withinStrictWindow(eTs, lockTime)) {
    console.log("[OUT OF WINDOW]", eTs, lockTime);
    return Functions.encodeString("ERR");
  }

  // Determine winner
  const homeScore = Number(ev.intHomeScore || 0);
  const awayScore = Number(ev.intAwayScore || 0);
  if (isNaN(homeScore) || isNaN(awayScore)) {
    console.log("[INVALID SCORES]");
    return Functions.encodeString("ERR");
  }

  let winner = "ERR";
  if (homeScore > awayScore) winner = "A";
  else if (awayScore > homeScore) winner = "B";
  else winner = "T"; // tie

  console.log(`[OK] ${teamAname}=${awayScore}, ${teamBname}=${homeScore} → winner=${winner}`);
  return Functions.encodeString(winner);
}

return main(args);
