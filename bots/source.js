// @ts-nocheck
// Hardened Chainlink Functions source.js for BlockPools
// - Uses DON-hosted secrets (secrets.THESPORTSDB_API_KEY / optional secrets.TSDB_ENDPOINT)
// - Strict same-day and drift window checks
// - Resolves correct A/B winner by mapping A/B to home/away

/* ------------------------------- Tunables --------------------------------- */
const MAX_EVENT_DRIFT_SECS = 2 * 3600;   // ±2 h drift allowed
const REQUIRE_SAME_DAY = true;           // must be same calendar day UTC
const REQUIRE_RESULTS = true;            // require lookup/event_results to be final

/* ----------------------------- Helper functions ---------------------------- */

function needSecret(name) {
  if (!secrets || !secrets[name]) throw Error(`ERR_MISSING_SECRET:${name}`);
  return secrets[name];
}

function looksFinal(ev) {
  const s = String(ev?.strStatus ?? ev?.strProgress ?? "").trim().toLowerCase();
  const hasScores =
    ev?.intHomeScore != null && ev?.intAwayScore != null &&
    ev.intHomeScore !== "" && ev.intAwayScore !== "";

  // Common final markers seen in TSDB:
  if (/^(ft|aot|aet|ap|pen|finished|full time|final)$/.test(s)) return true;
  if (/final|finished|ended|complete/.test(s)) return true;
  // Some feeds leave status blank but provide final scores
  return hasScores && !s;
}

function tsFromEvent(e) {
  // Prefer explicit timestamp
  if (e?.strTimestamp) {
    const ms = Date.parse(e.strTimestamp);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  // Fallback: date + time
  if (e?.dateEvent && e?.strTime) {
    const s = /Z$/.test(e.strTime) ? `${e.dateEvent}T${e.strTime}` : `${e.dateEvent}T${e.strTime}Z`;
    const ms = Date.parse(s);
    if (!Number.isNaN(ms)) return Math.floor(ms / 1000);
  }
  // Fallback: midnight UTC on date
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

// Determine if Team A corresponds to home or away in the matched event
function teamAIsHome(ev, aName, aCode, bName, bCode) {
  const home = normTeam(ev?.strHomeTeam);
  const away = normTeam(ev?.strAwayTeam);
  const A1 = normTeam(aName) || normTeam(aCode);
  const B1 = normTeam(bName) || normTeam(bCode);
  if (A1 && A1 === home) return true;
  if (A1 && A1 === away) return false;

  // If A didn't match cleanly, try B as a hint
  if (B1 && B1 === home) return false; // then A must be away
  if (B1 && B1 === away) return true;  // then A must be home

  // Fallback: if names are ambiguous, infer by codes first
  const aCodeN = normTeam(aCode);
  if (aCodeN && aCodeN === home) return true;
  if (aCodeN && aCodeN === away) return false;

  // Last resort: default to home=false (A=away); safer than assuming home
  return false;
}

/* ------------------------------ API functions ------------------------------ */

async function fetchJson(url) {
  const res = await Functions.makeHttpRequest({ url });
  if (res.error) throw new Error(`HTTP_ERR: ${url} :: ${res.error}`);
  return res.data;
}

/* ------------------------------- Main logic -------------------------------- */

async function main(args) {
  // Support args8 or args9 (last slot may be eventId)
  const [league, dateFrom, dateTo, teamAcode, teamBcode, teamAname, teamBname, lockTimeStr, eventIdMaybe] = args;
  const lockTime = Number(lockTimeStr || 0);
  const eventId = eventIdMaybe && !isNaN(Number(eventIdMaybe)) ? String(eventIdMaybe) : "";

  // --- Secrets (DON-hosted) ---
  const TSDB_KEY  = needSecret('THESPORTSDB_API_KEY');
  const TSDB_BASE = secrets.TSDB_ENDPOINT || 'https://www.thesportsdb.com/api/v2/json';
  // Base looks like: https://www.thesportsdb.com/api/v2/json/{APIKEY}
  const base = `${TSDB_BASE}/${TSDB_KEY}`;

  const idLeagueMap = {
    nfl: "4391",
    mlb: "4424",
    nba: "4387",
    nhl: "4380",
    epl: "4328",
    ucl: "4480",
  };
  const idLeague = idLeagueMap[String(league).toLowerCase()] || "";

  console.log(`[START] league=${league} A=${teamAname}/${teamAcode} B=${teamBname}/${teamBcode} lock=${lockTime}`);

  let ev = null;

  // Try 1) direct id lookup (preferred if provided)
  if (eventId) {
    try {
      const r1 = await fetchJson(`${base}/lookup/event_results/${eventId}`);
      const r2 = await fetchJson(`${base}/lookup/event/${eventId}`);
      ev =
        (r1 && (r1.results?.[0] || r1.lookup?.[0])) ||
        (r2 && (r2.lookup?.[0] || r2.results?.[0])) ||
        null;
    } catch (e) {
      console.log(`[WARN] direct id lookup failed: ${e.message}`);
    }
  }

  // Try 2) previous/league slice and match by teams + window
  if (!ev && idLeague) {
    try {
      const j = await fetchJson(`${base}/schedule/previous/league/${idLeague}`);
      const arr = j?.schedule || j?.events || [];
      for (const e of arr) {
        if (!unorderedTeamsMatch(e, teamAname, teamBname, teamAcode, teamBcode)) continue;
        const eTs = tsFromEvent(e);
        if (!withinStrictWindow(eTs, lockTime)) continue;
        ev = e;
        break;
      }
    } catch (e) {
      console.log(`[ERR] league slice fetch failed: ${e.message}`);
    }
  }

  if (!ev) {
    console.log("[NO MATCH]");
    return Functions.encodeString("ERR");
  }

  // Optionally require that event_results indicates final
  if (REQUIRE_RESULTS && ev?.idEvent) {
    try {
      const rr = await fetchJson(`${base}/lookup/event_results/${ev.idEvent}`);
      const er = (rr && (rr.results?.[0] || rr.lookup?.[0])) || null;
      if (er) ev = Object.assign({}, ev, er); // merge in any definitive fields
    } catch (e) {
      console.log(`[WARN] results lookup failed: ${e.message}`);
    }
  }

  // Ensure final and valid
  if (!looksFinal(ev)) {
    console.log("[NOT FINAL]", ev.strStatus || ev.strProgress || "(none)");
    return Functions.encodeString("ERR");
  }

  // Check timestamp window again strictly
  const eTs = tsFromEvent(ev);
  if (!withinStrictWindow(eTs, lockTime)) {
    console.log("[OUT OF WINDOW]", eTs, lockTime);
    return Functions.encodeString("ERR");
  }

  // Winner relative to A/B (map A to home/away correctly)
  const homeScore = Number(ev.intHomeScore || 0);
  const awayScore = Number(ev.intAwayScore || 0);
  if (!Number.isFinite(homeScore) || !Number.isFinite(awayScore)) {
    console.log("[INVALID SCORES]");
    return Functions.encodeString("ERR");
  }

  const aIsHome = teamAIsHome(ev, teamAname, teamAcode, teamBname, teamBcode);
  const aScore = aIsHome ? homeScore : awayScore;
  const bScore = aIsHome ? awayScore : homeScore;

  let winner = "T";
  if (aScore > bScore) winner = "A";
  else if (bScore > aScore) winner = "B";

  console.log(
    `[OK] ${ev.strHomeTeam}(${homeScore}) vs ${ev.strAwayTeam}(${awayScore}) | A=${aScore} B=${bScore} | winner=${winner}`
  );
  return Functions.encodeString(winner);
}

return main(args);
