// @ts-nocheck
// Chainlink Functions source.js — Goalserve winner resolver
// Returns: "1" (Team A), "2" (Team B), "0" (Tie), or "ERR" on failure.

// ----------------------------- Helpers -------------------------------------

function getSecret(name, { required = false, fallback = undefined } = {}) {
  const bag = typeof secrets === "undefined" ? undefined : secrets;
  const val = bag ? bag[name] : undefined;
  if (required && !val) throw Error(`ERR_MISSING_SECRET:${name}`);
  return val ?? fallback;
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

// Format date per Goalserve config: "ISO" => "yyyy-mm-dd", "DMY" => "dd.mm.yyyy"
function fmtGsDateFromLock(lockTimeLike, mode /* "ISO" | "DMY" */) {
  // If lockTime is absent, use today UTC (safe fallback)
  let d;
  if (lockTimeLike == null || lockTimeLike === "") {
    d = new Date();
  } else {
    const n = Number(lockTimeLike);
    const ms = n > 1e12 ? n : n * 1000; // accept seconds or ms
    d = new Date(ms);
  }
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return (String(mode).toUpperCase() === "DMY") ? `${dd}.${mm}.${yyyy}` : `${yyyy}-${mm}-${dd}`;
}

async function fetchJson(url, headers) {
  const res = await Functions.makeHttpRequest({
    url,
    headers,
    timeout: 15000,
  });
  if (res.error) throw new Error(`HTTP_ERR: ${url} :: ${res.error}`);
  if (!res.data) throw new Error(`HTTP_NO_DATA: ${url}`);
  return res.data;
}

// Flatten common Goalserve shapes into an array of game-like objects
function extractGames(payload) {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;

  // Typical JSON shapes we’ve seen
  if (Array.isArray(payload.games?.game)) return payload.games.game;
  if (Array.isArray(payload.game)) return payload.game;
  if (Array.isArray(payload.events)) return payload.events;

  // Fallbacks
  if (typeof payload === "object") {
    const vals = Object.values(payload);
    const arrays = vals.filter(v => Array.isArray(v));
    if (arrays.length) return arrays.flat();
    return vals.filter(v => v && typeof v === "object");
  }
  return [];
}

function readTeamName(obj, side /* "home" | "away" */) {
  // Try a handful of likely fields
  if (!obj) return "";
  const sideTeam = side === "home"
    ? (obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {})
    : (obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {});
  const direct = side === "home"
    ? (obj.home_name ?? obj.home ?? obj.homeTeamName)
    : (obj.away_name ?? obj.away ?? obj.awayTeamName);

  const guess =
    (typeof sideTeam === "object" ? (sideTeam.name ?? sideTeam.team ?? sideTeam.title) : undefined)
    ?? direct
    ?? "";

  return String(guess);
}

function readScore(obj, side /* "home" | "away" */) {
  if (!obj) return 0;
  const sideTeam = side === "home"
    ? (obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {})
    : (obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {});
  const altField = side === "home" ? (obj.home_score ?? obj.homeScore) : (obj.away_score ?? obj.awayScore);

  const val =
    (typeof sideTeam === "object" ? (sideTeam.totalscore ?? sideTeam.score ?? sideTeam.total) : undefined)
    ?? altField
    ?? 0;

  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
}

function isFinalStatus(raw) {
  const s = String(raw || "").trim().toLowerCase();
  // Include common “final” variants
  return (
    s === "final" ||
    s === "finished" ||
    s === "full time" ||
    s === "ft" ||
    s === "aot" || // after overtime (sometimes)
    s === "ended"
  );
}

// ------------------------------ Main ---------------------------------------

async function main(args) {
  // Arg order (unchanged):
  // [league, dateFrom, dateTo, teamAcode, teamBcode, teamAname, teamBname, lockTimeStr, eventIdMaybe]
  const [league, _dateFrom, _dateTo, _teamAcode, _teamBcode, teamAname, teamBname, lockTimeStr] = args;

  // ---- Config from DON secrets (with safe fallbacks) ----
  // Auth modes:
  //   - path: base is ".../getfeed" (we'll append "/<KEY>")
  //           OR base is already ".../getfeed/<KEY>"
  //   - header: base is ".../getfeed", we’ll send { "X-API-KEY": <KEY> }
  const baseRaw = getSecret("GOALSERVE_BASE_URL", { fallback: "https://www.goalserve.com/getfeed" });
  const authMode = (getSecret("GOALSERVE_AUTH", { fallback: "path" }) || "path").toLowerCase(); // "path" | "header"
  const apiKey   = getSecret("GOALSERVE_API_KEY", { fallback: "" });
  const dateFmt  = (getSecret("GOALSERVE_DATE_FMT", { fallback: "DMY" }) || "DMY").toUpperCase(); // "DMY" | "ISO"

  // League → path mapping (you provided NFL endpoint: /football/nfl-scores)
  let sportPath = "football";
  let leaguePath = "nfl-scores";
  const L = String(league || "").toLowerCase();
  if (L !== "nfl") {
    // Keep default NFL unless you expand later
    sportPath = "football";
    leaguePath = "nfl-scores";
  }

  const baseClean = String(baseRaw).replace(/\/+$/, ""); // strip trailing slashes
  const gsDate = fmtGsDateFromLock(lockTimeStr, dateFmt);

  // Build base w/ key when using path-auth
  let baseWithAuth = baseClean;
  if (authMode === "path") {
    // If base already includes a key segment after /getfeed/, keep it as-is.
    // Otherwise append "/<apiKey>" (and require that we have one).
    const hasKey = /\/getfeed\/[^/]+$/i.test(baseClean);
    if (!hasKey) {
      if (!apiKey) throw new Error("ERR_MISSING_SECRET:GOALSERVE_API_KEY (path mode)");
      baseWithAuth = `${baseClean}/${encodeURIComponent(apiKey)}`;
    }
  }

  // Optional header when using header-auth
  const headers = (authMode === "header" && apiKey)
    ? { "X-API-KEY": apiKey }
    : undefined;

  const url = `${baseWithAuth}/${sportPath}/${leaguePath}?date=${encodeURIComponent(gsDate)}&json=1`;

  // ---- Fetch day slate and find the matching game ----
  const payload = await fetchJson(url, headers);
  const games = extractGames(payload);

  if (!Array.isArray(games) || games.length === 0) {
    console.log("[NO GAMES]", url);
    return Functions.encodeString("ERR");
  }

  const A = normTeam(teamAname);
  const B = normTeam(teamBname);
  if (!A || !B) {
    console.log("[BAD INPUT TEAMS]", teamAname, teamBname);
    return Functions.encodeString("ERR");
  }

  const match = games.find((g) => {
    const home = normTeam(readTeamName(g, "home"));
    const away = normTeam(readTeamName(g, "away"));
    if (!home || !away) return false;
    // Unordered set equality
    return (home === A && away === B) || (home === B && away === A);
  });

  if (!match) {
    console.log("[NO MATCHED GAME FOR TEAMS]", { teamAname, teamBname, lookedUpOn: gsDate, url });
    return Functions.encodeString("ERR");
  }

  const status = match?.status ?? match?.state ?? match?.match_status ?? "";
  if (!isFinalStatus(status)) {
    console.log("[NOT FINAL]", status);
    return Functions.encodeString("ERR");
  }

  const homeScore = readScore(match, "home");
  const awayScore = readScore(match, "away");

  const homeName = normTeam(readTeamName(match, "home"));
  const awayName = normTeam(readTeamName(match, "away"));

  // Determine whether Team A is home or away in THIS matched game
  const teamAIsHome =
    (A && homeName && A === homeName) ? true :
    (A && awayName && A === awayName) ? false :
    false;

  let winner = "0"; // tie by default
  if (homeScore > awayScore) winner = teamAIsHome ? "1" : "2";
  else if (awayScore > homeScore) winner = teamAIsHome ? "2" : "1";

  console.log(`[WINNER] date=${gsDate} url=${url} status=${status} home=${homeScore} away=${awayScore} AisHome=${teamAIsHome} -> ${winner}`);
  return Functions.encodeString(winner);
}

return main(args);
