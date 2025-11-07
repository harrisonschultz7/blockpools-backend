// @ts-nocheck
// Chainlink Functions source.js — Goalserve winner resolver
// For GamePool.sol fulfillRequest:
//
//   if response == teamACode  => TeamA wins
//   if response == teamBCode  => TeamB wins
//   if response == "TIE"/"Tie"=> Tie
//   else revert("Unrecognized team code")
//
// This script:
//  - Fetches NFL scores from Goalserve
//  - Finds the matching game by teams
//  - Ensures status is final
//  - Returns EXACTLY one of: teamACode, teamBCode, "TIE"
//  - Returns "ERR" on any uncertainty (so the callback reverts safely)

// --------- Helpers: secrets, normalize, date formatting ---------

function getSecret(name, { required = false, fallback = undefined } = {}) {
  const bag = typeof secrets === "undefined" ? undefined : secrets;
  const val = bag ? bag[name] : undefined;
  if (required && (val === undefined || val === null || String(val).trim() === "")) {
    throw Error(`ERR_MISSING_SECRET:${name}`);
  }
  return (val !== undefined && val !== null) ? val : fallback;
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

function acronym(s) {
  const parts = (s || "")
    .replace(/[\u0300-\u036f]/g, "")
    .split(/[^a-zA-Z0-9]+/)
    .filter(Boolean);
  return parts.map(p => p[0]?.toUpperCase() || "").join("");
}

// lockTimeStr is epoch seconds (from settlement-bot args[7])
// mode: "DMY" => dd.mm.yyyy, "ISO" => yyyy-mm-dd
function fmtGsDateFromLock(lockTimeStr, mode) {
  const m = (String(mode || "DMY")).toUpperCase();
  let n = Number(lockTimeStr);
  if (!Number.isFinite(n) || n <= 0) {
    // Fallback to "today" UTC if somehow missing
    n = Math.floor(Date.now() / 1000);
  }
  const d = new Date(n * 1000);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  return m === "ISO" ? `${yyyy}-${mm}-${dd}` : `${dd}.${mm}.${yyyy}`;
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

// --------- Goalserve payload helpers ---------

// Flatten common Goalserve shapes (esp. NFL scores)
function extractGames(payload) {
  if (!payload) return [];

  // Direct lists
  if (Array.isArray(payload.games?.game)) return payload.games.game;
  if (Array.isArray(payload.game)) return payload.game;

  // NFL scores shape: { scores: { category: { match: [...] } } }
  const cat = payload?.scores?.category;
  if (cat) {
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = cats.flatMap((c) =>
      Array.isArray(c?.match) ? c.match : []
    );
    if (matches.length) return matches;
  }

  if (Array.isArray(payload)) return payload;

  if (typeof payload === "object") {
    const vals = Object.values(payload);
    const arrays = vals.filter((v) => Array.isArray(v));
    if (arrays.length) return arrays.flat();
  }

  return [];
}

function readTeamName(obj, side /* "home" | "away" */) {
  if (!obj) return "";

  const teamObj = side === "home"
    ? (obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {})
    : (obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {});

  const direct = side === "home"
    ? (obj.home_name ?? obj.home ?? obj.homeTeamName)
    : (obj.away_name ?? obj.away ?? obj.awayTeamName);

  const guess =
    (typeof teamObj === "object"
      ? (teamObj.name ?? teamObj.team ?? teamObj.title)
      : undefined) ||
    direct ||
    "";

  return String(guess);
}

function readScore(obj, side /* "home" | "away" */) {
  if (!obj) return 0;

  const teamObj = side === "home"
    ? (obj.hometeam ?? obj.homeTeam ?? obj.home_team ?? {})
    : (obj.awayteam ?? obj.awayTeam ?? obj.away_team ?? {});

  const altField =
    side === "home"
      ? (obj.home_score ?? obj.homeScore)
      : (obj.away_score ?? obj.awayScore);

  const val =
    (typeof teamObj === "object"
      ? (teamObj.totalscore ?? teamObj.score ?? teamObj.total)
      : undefined) ??
    altField ??
    0;

  const n = Number(val);
  return Number.isFinite(n) ? n : 0;
}

function readStatus(rawGame) {
  const cand = [
    rawGame?.status,
    rawGame?.game_status,
    rawGame?.state,
    rawGame?.status_text,
    rawGame?.statusShort,
    rawGame?.match_status,
  ];
  for (let i = 0; i < cand.length; i++) {
    const v = cand[i];
    if (v !== undefined && v !== null && String(v).trim() !== "") {
      return String(v).trim();
    }
  }
  return "";
}

function isFinalStatus(statusRaw) {
  const s = String(statusRaw || "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[()]/g, "")
    .trim();

  // Broad set of "final" markers
  const finals = [
    "final",
    "finished",
    "full time",
    "ft",
    "ended",
    "game over",
    "final ot",
    "final/ot",
    "after overtime",
    "aot"
  ];

  if (finals.includes(s)) return true;

  // Fuzzy: if the raw blob clearly contains "final"
  if (/\bfinal\b/i.test(String(statusRaw || ""))) return true;

  return false;
}

// Match candidate game to our teams using names + codes
function gameMatchesTeams(g, teamAName, teamBName, teamACode, teamBCode) {
  const home = readTeamName(g, "home");
  const away = readTeamName(g, "away");

  const nHome = normTeam(home);
  const nAway = normTeam(away);
  if (!nHome || !nAway) return false;

  const nA = normTeam(teamAName);
  const nB = normTeam(teamBName);
  const cA = String(teamACode || "").toUpperCase();
  const cB = String(teamBCode || "").toUpperCase();

  const acrHome = acronym(home);
  const acrAway = acronym(away);
  const acrA = acronym(teamAName);
  const acrB = acronym(teamBName);

  function sideMatches(sideNameNorm, sideANameNorm, sideACode, sideAacr) {
    if (!sideNameNorm) return false;
    if (sideNameNorm === sideANameNorm && sideANameNorm) return true;
    if (sideACode && (sideACode === acrHome || sideACode === acrAway)) {
      // already checked via acr later; keep simple
    }
    if (sideAacr && acronym(sideNameNorm) === sideAacr) return true;
    // token overlap
    const tWant = new Set(sideANameNorm.split(" ").filter((t) => t.length > 2));
    const tSide = new Set(sideNameNorm.split(" "));
    for (const t of tWant) if (tSide.has(t)) return true;
    return false;
  }

  // We accept if unordered teams match:
  // (home == A && away == B) || (home == B && away == A)
  const homeIsA =
    sideMatches(nHome, nA, cA, acrA) ||
    (cA && (acrHome === cA));
  const awayIsB =
    sideMatches(nAway, nB, cB, acrB) ||
    (cB && (acrAway === cB));

  const homeIsB =
    sideMatches(nHome, nB, cB, acrB) ||
    (cB && (acrHome === cB));
  const awayIsA =
    sideMatches(nAway, nA, cA, acrA) ||
    (cA && (acrAway === cA));

  return (homeIsA && awayIsB) || (homeIsB && awayIsA);
}

// --------------------------- Main ---------------------------------

async function main(args) {
  // Args from settlement-bot:
  // [0]=league
  // [1]=dateFrom (ISO)  — not strictly used here
  // [2]=dateTo   (ISO)  — not strictly used here
  // [3]=teamACode
  // [4]=teamBCode
  // [5]=teamAName
  // [6]=teamBName
  // [7]=lockTime (epoch seconds)
  const [
    league,
    _dateFrom,
    _dateTo,
    rawTeamACode,
    rawTeamBCode,
    teamAName,
    teamBName,
    lockTimeStr,
  ] = args;

  const teamACode = String(rawTeamACode || "").trim();
  const teamBCode = String(rawTeamBCode || "").trim();

  // ---- Secrets / config ----
  const baseRaw = getSecret("GOALSERVE_BASE_URL", {
    fallback: "https://www.goalserve.com/getfeed",
  });
  const apiKey = getSecret("GOALSERVE_API_KEY", { fallback: "" });
  const authMode = (getSecret("GOALSERVE_AUTH", { fallback: "path" }) || "path")
    .toLowerCase(); // "path" | "header"
  const dateFmt = (
    getSecret("GOALSERVE_DATE_FMT", { fallback: "DMY" }) || "DMY"
  ).toUpperCase(); // "DMY" | "ISO"

  // Only NFL for now; always use nfl-scores
  let sportPath = "football";
  let leaguePath = "nfl-scores";
  // (If you add other leagues later, switch on `league` here.)

  const baseClean = String(baseRaw).replace(/\/+$/, "");

  // Path-style auth: /getfeed/<KEY>/...
  let baseWithAuth = baseClean;
  if (authMode === "path") {
    const hasInlineKey = /\/getfeed\/[^/]+$/i.test(baseClean);
    if (!hasInlineKey) {
      if (!apiKey) throw new Error("ERR_MISSING_SECRET:GOALSERVE_API_KEY");
      baseWithAuth = `${baseClean}/${encodeURIComponent(apiKey)}`;
    }
  }

  const headers =
    authMode === "header" && apiKey
      ? { "X-API-KEY": apiKey }
      : undefined;

  const gsDate = fmtGsDateFromLock(lockTimeStr, dateFmt);
  const url = `${baseWithAuth}/${sportPath}/${leaguePath}?date=${encodeURIComponent(
    gsDate
  )}&json=1`;

  // ---- Fetch & flatten ----
  const payload = await fetchJson(url, headers);
  const games = extractGames(payload);

  if (!Array.isArray(games) || games.length === 0) {
    console.log("[NO GAMES]", { url, gsDate, league });
    return Functions.encodeString("ERR");
  }

  const AnameNorm = normTeam(teamAName);
  const BnameNorm = normTeam(teamBName);

  if (!AnameNorm || !BnameNorm) {
    console.log("[BAD INPUT TEAMS]", { teamAName, teamBName });
    return Functions.encodeString("ERR");
  }

  // ---- Find matching game ----
  const candidates = games.filter((g) =>
    gameMatchesTeams(g, teamAName, teamBName, teamACode, teamBCode)
  );

  if (!candidates.length) {
    console.log("[NO MATCHED GAME FOR TEAMS]", {
      teamAName,
      teamBName,
      teamACode,
      teamBCode,
      date: gsDate,
      url,
      sample: games.slice(0, 3).map((g) => ({
        home: readTeamName(g, "home"),
        away: readTeamName(g, "away"),
        status: readStatus(g),
      })),
    });
    return Functions.encodeString("ERR");
  }

  // If multiple, pick the one with a final-looking status first
  candidates.sort((g1, g2) => {
    const f1 = isFinalStatus(readStatus(g1)) ? 1 : 0;
    const f2 = isFinalStatus(readStatus(g2)) ? 1 : 0;
    return f2 - f1;
  });

  const match = candidates[0];
  const status = readStatus(match);

  if (!isFinalStatus(status)) {
    console.log("[NOT FINAL]", { status, date: gsDate, url });
    return Functions.encodeString("ERR");
  }

  const homeScore = readScore(match, "home");
  const awayScore = readScore(match, "away");
  const homeNorm = normTeam(readTeamName(match, "home"));
  const awayNorm = normTeam(readTeamName(match, "away"));

  const AisHome = homeNorm === AnameNorm;
  const AisAway = awayNorm === AnameNorm;

  // Sanity: if we somehow can't map A to home/away, bail safely
  if (!AisHome && !AisAway) {
    console.log("[AMBIGUOUS SIDE]", {
      teamAName,
      home: readTeamName(match, "home"),
      away: readTeamName(match, "away"),
    });
    return Functions.encodeString("ERR");
  }

  // ---- Decide winner code to match Solidity expectations ----
  let winnerCode = "TIE";

  if (homeScore > awayScore) {
    // home wins
    winnerCode = AisHome ? teamACode : teamBCode;
  } else if (awayScore > homeScore) {
    // away wins
    winnerCode = AisAway ? teamACode : teamBCode;
  } else {
    // tie
    winnerCode = "TIE";
  }

  // Extra guard: if winnerCode is empty, treat as ERR
  if (!winnerCode || String(winnerCode).trim() === "") {
    console.log("[EMPTY WINNER CODE]", {
      homeScore,
      awayScore,
      teamACode,
      teamBCode,
    });
    return Functions.encodeString("ERR");
  }

  console.log("[WINNER RESOLVED]", {
    date: gsDate,
    url,
    status,
    home: {
      name: readTeamName(match, "home"),
      score: homeScore,
    },
    away: {
      name: readTeamName(match, "away"),
      score: awayScore,
    },
    teamACode,
    teamBCode,
    winnerCode,
  });

  // This is what GamePool.fulfillRequest expects.
  return Functions.encodeString(String(winnerCode));
}

return main(args);
