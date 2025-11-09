// @ts-nocheck
// Chainlink Functions source.js — Goalserve NFL winner resolver
// Returns: teamACode, teamBCode, "TIE", or "ERR".

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

function isoToDMY(iso) {
  // "2025-11-06" -> "06.11.2025"
  if (!iso || typeof iso !== "string") return "";
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return "";
  const [, y, mm, dd] = m;
  return `${dd}.${mm}.${y}`;
}

// Fallback: derive UTC date from lockTime if dateFrom missing
function fmtDateFromLock(lockTimeLike, mode /* "ISO" | "DMY" */) {
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
  return String(mode).toUpperCase() === "DMY"
    ? `${dd}.${mm}.${yyyy}`
    : `${yyyy}-${mm}-${dd}`;
}

async function fetchJson(url, headers) {
  const res = await Functions.makeHttpRequest({
    url,
    headers,
    timeout: 15000,
  });

  if (res.error) {
    throw new Error(
      `HTTP_ERR: ${url} :: ${JSON.stringify(res.error)} :: status=${
        res.response && res.response.status
      }`
    );
  }
  if (!res.data) {
    throw new Error(
      `HTTP_NO_DATA: ${url} :: status=${
        res.response && res.response.status
      }`
    );
  }
  return res.data;
}

// Flatten common Goalserve shapes into an array of match objects
function extractGames(payload) {
  if (!payload) return [];

  if (Array.isArray(payload)) return payload;

  if (Array.isArray(payload.games?.game)) return payload.games.game;

  if (Array.isArray(payload.game)) return payload.game;

  if (payload.scores && payload.scores.category) {
    const cat = payload.scores.category;
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = [];
    for (const c of cats) {
      if (!c) continue;
      if (Array.isArray(c.match)) matches.push(...c.match);
      else if (c.match) matches.push(c.match);
    }
    if (matches.length) return matches;
  }

  if (typeof payload === "object") {
    const arrays = Object.values(payload).filter((v) => Array.isArray(v));
    if (arrays.length) return arrays.flat();
  }

  return [];
}

function readTeamName(obj, side /* "home" | "away" */) {
  if (!obj) return "";

  const sideTeam =
    side === "home"
      ? obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {}
      : obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {};

  const direct =
    side === "home"
      ? obj.home_name ?? obj.home ?? obj.homeTeamName
      : obj.away_name ?? obj.away ?? obj.awayTeamName;

  const guess =
    (sideTeam && typeof sideTeam === "object"
      ? sideTeam.name ?? sideTeam.team ?? sideTeam.title
      : undefined) ??
    direct ??
    "";

  return String(guess);
}

function readScore(obj, side /* "home" | "away" */) {
  if (!obj) return 0;

  const sideTeam =
    side === "home"
      ? obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {}
      : obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {};

  const altField =
    side === "home"
      ? obj.home_score ?? obj.homeScore
      : obj.away_score ?? obj.awayScore;

  const val =
    (sideTeam && typeof sideTeam === "object"
      ? sideTeam.totalscore ?? sideTeam.score ?? sideTeam.total
      : undefined) ??
    altField ??
    0;

  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
}

function extractStatus(match) {
  const cand = [
    match?.status,
    match?.game_status,
    match?.state,
    match?.status_text,
    match?.statusShort,
  ];
  for (const s of cand) {
    if (s && String(s).trim()) return String(s).trim();
  }
  return "";
}

// IMPORTANT: mirror settlement-bot OT logic.
function isFinalStatus(raw) {
  const s = String(raw || "").toLowerCase().replace(/\s+/g, " ").trim();

  const finals = new Set([
    "final",
    "finished",
    "full time",
    "ft",
    "ended",
    "game over",
    "aot",
    "after overtime",
    "after over time",
    "final ot",
    "final/ot",
    "final aot",
    "final after ot",
    "final overtime",
  ]);

  if (finals.has(s)) return true;

  // Phrases that clearly mean the game is done
  if (s.includes("after over time") || s.includes("after overtime") || s.includes("after ot"))
    return true;

  // Generic final catch, avoid obvious non-end states
  if (
    /\bfinal\b/.test(s) &&
    !s.includes("semi") &&
    !s.includes("quarter") &&
    !s.includes("half")
  ) {
    return true;
  }

  return false;
}

// ------------------------------ Main ---------------------------------------

async function main(args) {
  // Arg order from settlement-bot:
  // [0] league
  // [1] dateFrom (ISO yyyy-mm-dd, ET-based)
  // [2] dateTo   (unused here)
  // [3] teamACode
  // [4] teamBCode
  // [5] teamAname
  // [6] teamBname
  // [7] lockTimeStr
  const [
    league,
    _dateFrom,
    _dateTo,
    teamACodeRaw,
    teamBCodeRaw,
    teamAname,
    teamBname,
    lockTimeStr,
  ] = args;

  const teamACode = String(teamACodeRaw || "").toUpperCase();
  const teamBCode = String(teamBCodeRaw || "").toUpperCase();

  // ---- Config from DON secrets ----
  const baseRaw = getSecret("GOALSERVE_BASE_URL", {
    fallback: "https://www.goalserve.com/getfeed",
  });
  const authMode = (
    getSecret("GOALSERVE_AUTH", { fallback: "path" }) || "path"
  ).toLowerCase(); // "path" | "header"
  const apiKey = getSecret("GOALSERVE_API_KEY", { fallback: "" });
  const dateFmt = (
    getSecret("GOALSERVE_DATE_FMT", { fallback: "DMY" }) || "DMY"
  ).toUpperCase(); // "DMY" | "ISO"

  // ---- League → endpoint (NFL only for now) ----
  let sportPath = "football";
  let leaguePath = "nfl-scores";
  const L = String(league || "").toLowerCase();
  if (L !== "nfl") {
    sportPath = "football";
    leaguePath = "nfl-scores";
  }

  // ---- Date selection ----
  let baseIso = "";
  if (typeof _dateFrom === "string" && /^\d{4}-\d{2}-\d{2}$/.test(_dateFrom)) {
    baseIso = _dateFrom;
  } else {
    baseIso = fmtDateFromLock(lockTimeStr, "ISO");
  }

  const gsDate = dateFmt === "DMY" ? isoToDMY(baseIso) : baseIso;

  // ---- Build URL with auth ----
  const baseClean = String(baseRaw).replace(/\/+$/, "");

  let baseWithAuth = baseClean;
  let headers = undefined;

  if (authMode === "path") {
    const hasKey = /\/getfeed\/[^/]+$/i.test(baseClean);
    if (!hasKey) {
      if (!apiKey) {
        throw new Error("ERR_MISSING_SECRET:GOALSERVE_API_KEY (path mode)");
      }
      baseWithAuth = `${baseClean}/${encodeURIComponent(apiKey)}`;
    }
  } else if (authMode === "header") {
    if (!apiKey) {
      throw new Error("ERR_MISSING_SECRET:GOALSERVE_API_KEY (header mode)");
    }
    headers = { "X-API-KEY": apiKey };
  }

  const url = `${baseWithAuth}/${sportPath}/${leaguePath}?date=${encodeURIComponent(
    gsDate
  )}&json=1`;

  // ---- Fetch slate ----
  const payload = await fetchJson(url, headers);
  const games = extractGames(payload);

  if (!Array.isArray(games) || games.length === 0) {
    console.log("[NO GAMES]", { url, gsDate, league });
    return Functions.encodeString("ERR");
  }

  const A = normTeam(teamAname);
  const B = normTeam(teamBname);
  if (!A || !B) {
    console.log("[BAD INPUT TEAMS]", { teamAname, teamBname });
    return Functions.encodeString("ERR");
  }

  // ---- Find matching game (unordered teams) ----
  const candidates = games.filter((g) => {
    const home = normTeam(readTeamName(g, "home"));
    const away = normTeam(readTeamName(g, "away"));
    if (!home || !away) return false;

    const setWanted = new Set([A, B]);
    const setHave = new Set([home, away]);
    return setHave.has(A) && setHave.has(B) && setWanted.size === setHave.size;
  });

  if (!candidates.length) {
    console.log("[NO MATCHED GAME FOR TEAMS]", {
      url,
      gsDate,
      teamAname,
      teamBname,
      sample: games.slice(0, 3).map((g) => ({
        home: readTeamName(g, "home"),
        away: readTeamName(g, "away"),
        status: extractStatus(g),
      })),
    });
    return Functions.encodeString("ERR");
  }

  // Prefer a final game; else first candidate
  let match =
    candidates.find((g) => isFinalStatus(extractStatus(g))) || candidates[0];

  const status = extractStatus(match);

  if (!isFinalStatus(status)) {
    console.log("[NOT FINAL]", { status, url, gsDate });
    return Functions.encodeString("ERR");
  }

  const homeScore = readScore(match, "home");
  const awayScore = readScore(match, "away");

  const homeName = normTeam(readTeamName(match, "home"));
  const awayName = normTeam(readTeamName(match, "away"));

  // Determine whether Team A is home or away
  const teamAIsHome =
    A && homeName && A === homeName
      ? true
      : A && awayName && A === awayName
      ? false
      : null;

  // ---- Winner → return team code string ----
  let winnerCode = "TIE";

  if (homeScore > awayScore) {
    if (teamAIsHome === true) winnerCode = teamACode || "TIE";
    else if (teamAIsHome === false) winnerCode = teamBCode || "TIE";
    else winnerCode = teamACode || "TIE";
  } else if (awayScore > homeScore) {
    if (teamAIsHome === true) winnerCode = teamBCode || "TIE";
    else if (teamAIsHome === false) winnerCode = teamACode || "TIE";
    else winnerCode = teamBCode || "TIE";
  }

  console.log("[WINNER]", {
    url,
    gsDate,
    status,
    homeName,
    awayName,
    homeScore,
    awayScore,
    teamACode,
    teamBCode,
    teamAIsHome,
    winnerCode,
  });

  return Functions.encodeString(winnerCode);
}

return main(args);
