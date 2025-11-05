// @ts-nocheck
// Chainlink Functions source.js — Goalserve (simple winner resolver)
// Returns: "1" (Team A), "2" (Team B), "0" (Tie), or "ERR" on failure.

// ----------------------------- Helpers -------------------------------------

function needSecret(name) {
  const bag = typeof secrets === "undefined" ? undefined : secrets;
  if (!bag || !bag[name]) throw Error(`ERR_MISSING_SECRET:${name}`);
  return bag[name];
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

// lockTime (unix seconds or ms) -> "dd.MM.yyyy" (Goalserve expects dots)
function toGoalserveDate(input) {
  if (!input) {
    // fallback to "today" UTC if missing (not ideal, but safe)
    const d = new Date();
    const dd = String(d.getUTCDate()).padStart(2, "0");
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const yyyy = d.getUTCFullYear();
    return `${dd}.${mm}.${yyyy}`;
  }
  const n = Number(input);
  const ms = n > 1e12 ? n : n * 1000; // handle secs vs ms
  const d = new Date(ms);
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yyyy = d.getUTCFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

async function fetchJson(url) {
  const res = await Functions.makeHttpRequest({ url, timeout: 12000 });
  if (res.error) throw new Error(`HTTP_ERR: ${url} :: ${res.error}`);
  return res.data;
}

// Flatten common Goalserve shapes into an array of game-like objects
function extractGames(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;

  if (Array.isArray(payload.games?.game)) return payload.games.game;
  if (Array.isArray(payload.game)) return payload.game;
  if (Array.isArray(payload.events)) return payload.events;

  // As a last resort, collect object values that look like game objects
  if (typeof payload === "object") {
    const vals = Object.values(payload);
    const arrs = vals.filter(v => Array.isArray(v)).flat();
    if (arrs.length) return arrs;
    return vals.filter(v => v && typeof v === "object");
  }
  return [];
}

// ------------------------------ Main ---------------------------------------

async function main(args) {
  // Keep same arg order you already use:
  // [league, dateFrom, dateTo, teamAcode, teamBcode, teamAname, teamBname, lockTimeStr, eventIdMaybe]
  const [league, _dateFrom, _dateTo, _teamAcode, _teamBcode, teamAname, teamBname, lockTimeStr] = args;

  // --- Secrets ---
  const API_KEY = needSecret("GOALSERVE_API_KEY");
  const base = (typeof secrets !== "undefined" && secrets.GOALSERVE_BASE_URL) || "https://www.goalserve.com/getfeed";

  // Build date from lockTime to avoid ambiguity
  const gsDate = toGoalserveDate(lockTimeStr);

  // League → path mapping (you can extend later)
  // You provided NFL endpoint: /football/nfl-scores
  let sportPath = "football";
  let leaguePath = "nfl-scores";
  const L = String(league || "").toLowerCase();
  if (L !== "nfl") {
    // default to NFL unless you add more mappings
    sportPath = "football";
    leaguePath = "nfl-scores";
  }

  const url =
    `${base}/${encodeURIComponent(API_KEY)}/${sportPath}/${leaguePath}` +
    `?date=${encodeURIComponent(gsDate)}&json=1`;

  // Fetch day slate and find the game matching your Team A/B (unordered)
  const data = await fetchJson(url);
  const games = extractGames(data);

  if (!games.length) {
    console.log("[NO GAMES]", url);
    return Functions.encodeString("ERR");
  }

  const A = normTeam(teamAname);
  const B = normTeam(teamBname);

  const match = games.find(g => {
    const home = normTeam(g?.hometeam?.name ?? g?.home_name ?? g?.home ?? "");
    const away = normTeam(g?.awayteam?.name ?? g?.away_name ?? g?.away ?? "");
    // unordered check: sets must match
    if (!home || !away || !A || !B) return false;
    return (home === A && away === B) || (home === B && away === A);
  });

  if (!match) {
    console.log("[NO MATCHED GAME FOR TEAMS]", teamAname, teamBname);
    return Functions.encodeString("ERR");
  }

  const status = String(match?.status || "").trim().toLowerCase();
  const isFinal = status === "final" || status === "finished" || status === "full time";

  if (!isFinal) {
    console.log("[NOT FINAL]", match?.status);
    return Functions.encodeString("ERR");
  }

  // Goalserve totals (strings). Example: hometeam.totalscore / awayteam.totalscore
  const homeScore = Number(match?.hometeam?.totalscore ?? match?.home_score ?? 0);
  const awayScore = Number(match?.awayteam?.totalscore ?? match?.away_score ?? 0);

  // Determine whether Team A is home or away in THIS matched game
  const homeName = normTeam(match?.hometeam?.name ?? match?.home_name ?? match?.home ?? "");
  const awayName = normTeam(match?.awayteam?.name ?? match?.away_name ?? match?.away ?? "");

  const teamAIsHome =
    A && homeName && A === homeName
      ? true
      : A && awayName && A === awayName
        ? false
        : false; // default (shouldn't happen if we matched above)

  let winner = "0"; // tie
  if (homeScore > awayScore) winner = teamAIsHome ? "1" : "2";
  else if (awayScore > homeScore) winner = teamAIsHome ? "2" : "1";

  console.log(`[WINNER] home=${homeScore} away=${awayScore} :: AisHome=${teamAIsHome} -> ${winner}`);
  return Functions.encodeString(winner);
}

return main(args);
