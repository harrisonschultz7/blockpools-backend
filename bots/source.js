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
  let d;
  if (lockTimeLike == null || lockTimeLike === "") {
    d = new Date(); // fallback: today (shouldn't really happen)
  } else {
    const n = Number(lockTimeLike);
    const ms = n > 1e12 ? n : n * 1000; // accept seconds or ms
    d = new Date(ms);
  }
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return (String(mode).toUpperCase() === "DMY")
    ? `${dd}.${mm}.${yyyy}`
    : `${yyyy}-${mm}-${dd}`;
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

// Flatten Goalserve shapes into an array of game-like objects
function extractGames(payload) {
  if (!payload) return [];

  // raw array
  if (Array.isArray(payload)) return payload;

  // common soccer-style / generic
  if (Array.isArray(payload.games?.game)) return payload.games.game;
  if (Array.isArray(payload.game)) return payload.game;

  // NFL "nfl-scores" shape:
  // { scores: { category: { match: [...] } } }
  // or { scores: { category: [ { match: [...] }, ... ] } }
  if (payload.scores && payload.scores.category) {
    const cat = payload.scores.category;
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = cats.flatMap(c =>
      Array.isArray(c?.match) ? c.match : []
    );
    if (matches.length) return matches;
  }

  // events variant
  if (Array.isArray(payload.events)) return payload.events;

  // fallback: look for any arrays inside
  if (typeof payload === "object") {
    const vals = Object.values(payload);
    const arrays = vals.filter(v => Array.isArray(v));
    if (arrays.length) return arrays.flat();
    return vals.filter(v => v && typeof v === "object");
  }

  return [];
}

function readTeamName(obj, side /* "home" | "away" */) {
  if (!obj) return "";

  const sideTeam = side === "home"
    ? (obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {})
    : (obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {});
  const direct = side === "home"
    ? (obj.home_name ?? obj.home ?? obj.homeTeamName)
    : (obj.away_name ?? obj.away ?? obj.awayTeamName);

  const guess =
    (typeof sideTeam === "object"
      ? (sideTeam.name ?? sideTeam.team ?? sideTeam.title)
      : undefined)
    ?? direct
    ?? "";

  return String(guess);
}

function readScore(obj, side /* "home" | "away" */) {
  if (!obj) return 0;
  const sideTeam = side === "home"
    ? (obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {})
    : (obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {});
  const altField = side === "home"
    ? (obj.home_score ?? obj.homeScore)
    : (obj.away_score ?? obj.awayScore);

  const val =
    (typeof sideTeam === "object"
      ? (sideTeam.totalscore ?? sideTeam.score ?? sideTeam.total)
      : undefined)
    ?? altField
    ?? 0;

  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
}

function isFinalStatus(raw) {
  const s = String(raw || "").trim().toLowerCase();
  const n = s.replace(/\s+/g, " ").replace(/[()]/g, "").trim();
  return [
    "final",
    "finished",
    "full time",
    "ft",
    "ended",
    "game over",
    "aot",
    "after overtime",
    "final ot",
    "final/ot",
    "final overtime",
  ].includes(n);
}

// ------------------------------ Main ---------------------------------------

async function main(args) {
  // New args from settlement-bot:
  // [league, dateFrom, dateTo, teamAcode, teamBcode, teamAname, teamBname, lockTimeStr]
  const [
    league,
    _dateFrom,
    _dateTo,
    _teamAcode,
    _teamBcode,
    teamAname,
    teamBname,
    lockTimeStr,
  ] = args;

  // ---- Config from DON secrets ----
  const baseRaw = getSecret("GOALSERVE_BASE_URL", {
    fallback: "https://www.goalserve.com/getfeed",
  });
  const authMode =
    (getSecret("GOALSERVE_AUTH", { fallback: "path" }) || "path").toLowerCase(); // "path" | "header"
  const apiKey = getSecret("GOALSERVE_API_KEY", { fallback: "" });
  const dateFmt =
    (getSecret("GOALSERVE_DATE_FMT", { fallback: "DMY" }) || "DMY").toUpperCase(); // "DMY" | "ISO"

  // League → path mapping (for now: always NFL-style)
  let sportPath = "football";
  let leaguePath = "nfl-scores";
  const L = String(league || "").toLowerCase();
  if (L === "nfl") {
    sportPath = "football";
    leaguePath = "nfl-scores";
  }
  // (Extend here for NBA/MLB later.)

  const baseClean = String(baseRaw).replace(/\/+$/, "");
  const gsDate = fmtGsDateFromLock(lockTimeStr, dateFmt);

  // Build base with key when using path-auth
  let baseWithAuth = baseClean;
  if (authMode === "path") {
    const hasKey = /\/getfeed\/[^/]+$/i.test(baseClean);
    if (!hasKey) {
      if (!apiKey) throw new Error("ERR_MISSING_SECRET:GOALSERVE_API_KEY(path)");
      baseWithAuth = `${baseClean}/${encodeURIComponent(apiKey)}`;
    }
  }

  // Headers if using header-auth
  const headers =
    authMode === "header" && apiKey
      ? { "X-API-KEY": apiKey }
      : undefined;

  const url = `${baseWithAuth}/${sportPath}/${leaguePath}?date=${encodeURIComponent(
    gsDate
  )}&json=1`;

  // ---- Fetch & extract ----
  const payload = await fetchJson(url, headers);
  const games = extractGames(payload);

  if (!Array.isArray(games) || games.length === 0) {
    console.log("[NO GAMES]", { url, gsDate });
    return Functions.encodeString("ERR");
  }

  const A = normTeam(teamAname);
  const B = normTeam(teamBname);
  if (!A || !B) {
    console.log("[BAD INPUT TEAMS]", { teamAname, teamBname });
    return Functions.encodeString("ERR");
  }

  // Find matching game by team names (unordered)
  const match = games.find((g) => {
    const home = normTeam(readTeamName(g, "home"));
    const away = normTeam(readTeamName(g, "away"));
    if (!home || !away) return false;
    return (
      (home === A && away === B) ||
      (home === B && away === A)
    );
  });

  if (!match) {
    console.log("[NO MATCHED GAME FOR TEAMS]", {
      teamAname,
      teamBname,
      gsDate,
      url,
      sample: games.slice(0, 3).map((g) => ({
        home: readTeamName(g, "home"),
        away: readTeamName(g, "away"),
        status:
          g?.status ??
          g?.state ??
          g?.match_status ??
          g?.game_status ??
          g?.status_text,
      })),
    });
    return Functions.encodeString("ERR");
  }

  const status =
    match?.status ??
    match?.state ??
    match?.match_status ??
    match?.game_status ??
    match?.status_text ??
    "";
  if (!isFinalStatus(status)) {
    console.log("[NOT FINAL]", { status, url, gsDate });
    return Functions.encodeString("ERR");
  }

  const homeScore = readScore(match, "home");
  const awayScore = readScore(match, "away");
  const homeName = normTeam(readTeamName(match, "home"));
  const awayName = normTeam(readTeamName(match, "away"));

  // Determine whether Team A is home in this matched game
  let teamAIsHome = false;
  if (A && homeName && A === homeName) {
    teamAIsHome = true;
  } else if (A && awayName && A === awayName) {
    teamAIsHome = false;
  }

  let winner = "0"; // tie
  if (homeScore > awayScore) {
    winner = teamAIsHome ? "1" : "2";
  } else if (awayScore > homeScore) {
    winner = teamAIsHome ? "2" : "1";
  }

  console.log("[WINNER]", {
    gsDate,
    url,
    status,
    homeScore,
    awayScore,
    homeName,
    awayName,
    teamAname,
    teamBname,
    teamAIsHome,
    winner,
  });

  return Functions.encodeString(winner);
}

return main(args);
