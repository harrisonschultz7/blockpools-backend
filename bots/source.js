// bots/source.js
// Chainlink Functions source for BlockPools settlement (optimized args model)
// Supports: NFL, NBA, NHL, EPL, UCL via Goalserve
//
// NEW ARGS (1):
//  0: packed JSON string with fields:
//     {
//       league: "NFL" | "NBA" | "NHL" | "EPL" | "UCL",
//       dateFrom: "yyyy-MM-dd",
//       dateTo:   "yyyy-MM-dd",
//       teamACode: "DAL",
//       teamBCode: "WAS",
//       teamAName: "Dallas Cowboys",
//       teamBName: "Washington Commanders",
//       lockTime: "173..."   // epoch seconds as string (or number)
//     }
//
// RETURNS (string, via Functions.encodeString):
//  - teamACode        => Team A wins  (e.g. "PHI")
//  - teamBCode        => Team B wins  (e.g. "KC")
//  - "TIE"            => Tie
//
// The GamePool.finalizeFromCoordinator(response) should interpret this string.

if (!Array.isArray(args) || args.length < 1) {
  throw Error("Invalid args: expected 1 packed JSON string");
}

const packedRaw = String(args[0] || "").trim();
if (!packedRaw) throw Error("Invalid args: packed JSON empty");

// Parse packed payload
let payload;
try {
  payload = JSON.parse(packedRaw);
} catch (e) {
  throw Error("Invalid args: packed JSON parse failed");
}

function s(v) {
  return String(v == null ? "" : v).trim();
}

const league = s(payload.league).toLowerCase();
const dateFrom = s(payload.dateFrom);
const dateTo = s(payload.dateTo);

const teamACode = s(payload.teamACode).toUpperCase();
const teamBCode = s(payload.teamBCode).toUpperCase();
const teamAName = s(payload.teamAName);
const teamBName = s(payload.teamBName);

// lockTime may come as number or string
const lockTime = Number(payload.lockTime || 0);

if (!league || !dateFrom || !dateTo || !teamAName || !teamBName) {
  throw Error("Missing required packed args fields");
}

const GOALSERVE_API_KEY = secrets.GOALSERVE_API_KEY;
if (!GOALSERVE_API_KEY) {
  throw Error("Missing GOALSERVE_API_KEY in secrets");
}

const GOALSERVE_BASE_URL =
  secrets.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers (aligned with settlement-bot.ts semantics)
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Map on-chain league label -> Goalserve path.
 *
 * NFL: /football/nfl-scores?date=dd.MM.yyyy&json=1
 * NBA: /bsktbl/nba-scores?date=dd.MM.yyyy&json=1
 * NHL: /hockey/nhl-scores?date=dd.MM.yyyy&json=1
 * EPL: /commentaries/1204?date=dd.MM.yyyy&json=1        (England - Premier League)
 * UCL: /commentaries/1005?date=dd.MM.yyyy&json=1        (UEFA Champions League)
 */
function goalserveLeaguePaths(leagueLabel) {
  const L = String(leagueLabel || "").trim().toLowerCase();

  if (L === "nfl") {
    return { sportPath: "football", leaguePaths: ["nfl-scores"] };
  }
  if (L === "nba") {
    return { sportPath: "bsktbl", leaguePaths: ["nba-scores"] };
  }
  if (L === "nhl") {
    return { sportPath: "hockey", leaguePaths: ["nhl-scores"] };
  }

  // EPL / England Premier League
  if (
    L === "epl" ||
    L === "premier league" ||
    L === "england - premier league" ||
    L === "england premier league"
  ) {
    return { sportPath: "commentaries", leaguePaths: ["1204"] };
  }

  // UCL / UEFA Champions League
  if (
    L === "ucl" ||
    L === "uefa champions league" ||
    L === "champions league"
  ) {
    return { sportPath: "commentaries", leaguePaths: ["1005"] };
  }

  return { sportPath: "", leaguePaths: [] };
}

// Known final-ish labels (incl OT / soccer variants)
const finalsSet = new Set([
  "final",
  "finished",
  "full time",
  "full-time",
  "ft",
  "after over time",
  "after overtime",
  "final/ot",
  "final ot",
  "final aot",
  "final after ot",
]);

function isFinalStatus(raw) {
  const st = (raw || "").trim().toLowerCase();
  if (!st) return false;

  if (finalsSet.has(st)) return true;

  if (st.includes("after over time") || st.includes("after overtime") || st.includes("after ot")) return true;
  if (st.includes("full time") || st === "full-time") return true;

  if (st.includes("final") && !st.includes("semi") && !st.includes("quarter") && !st.includes("half")) {
    return true;
  }

  return false;
}

function norm(str) {
  return (str || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();
}

function trimU(s) {
  return String(s || "").trim().toUpperCase();
}

function acronym(s) {
  const parts = (s || "").split(/[^a-zA-Z0-9]+/).filter(Boolean);
  return parts.map((p) => (p[0] || "").toUpperCase()).join("");
}

async function fetchJsonWithRetry(url, tries = 3) {
  let lastErr;
  for (let i = 0; i < tries; i++) {
    const resp = await Functions.makeHttpRequest({ url });
    if (!resp) {
      lastErr = Error("No response");
    } else if (resp.error) {
      lastErr = Error(`HTTP error: ${resp.error}`);
    } else if (resp.status < 200 || resp.status >= 300) {
      lastErr = Error(`HTTP ${resp.status}`);
    } else if (resp.data == null) {
      lastErr = Error("Empty body");
    } else {
      return resp.data;
    }
  }
  throw lastErr;
}

// Parse "dd.MM.yyyy HH:mm" as UTC
function parseDatetimeUTC(s) {
  if (!s) return;
  const m = String(s).match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return;
  const [, dd, MM, yyyy, HH, mm] = m;
  const t = Date.UTC(+yyyy, +MM - 1, +dd, +HH, +mm, 0, 0);
  if (!isFinite(t)) return;
  return Math.floor(t / 1000);
}

// Parse "dd.MM.yyyy" + optional time as UTC-ish
function parseDateAndTimeAsUTC(dateStr, timeStr) {
  if (!dateStr) return;
  const md = String(dateStr).match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (!md) return;
  const [, dd, MM, yyyy] = md;

  let h = 0,
    mi = 0;
  if (timeStr) {
    const ampm = String(timeStr).trim().toUpperCase();
    let mh = ampm.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/);
    if (mh) {
      h = +mh[1];
      mi = +mh[2];
      const mer = mh[3];
      if (mer === "PM" && h < 12) h += 12;
      if (mer === "AM" && h === 12) h = 0;
    } else {
      mh = ampm.match(/^(\d{1,2}):(\d{2})$/);
      if (mh) {
        h = +mh[1];
        mi = +mh[2];
      }
    }
  }

  const t = Date.UTC(+yyyy, +MM - 1, +dd, h, mi, 0, 0);
  if (!isFinite(t)) return;
  return Math.floor(t / 1000);
}

function kickoffEpochFromRaw(raw) {
  const t1 = parseDatetimeUTC(raw?.datetime_utc || raw?.["@datetime_utc"]);
  if (t1) return t1;

  const date =
    raw?.formatted_date ||
    raw?.date ||
    raw?.["@formatted_date"] ||
    raw?.["@date"];
  const time =
    raw?.time || raw?.start_time || raw?.start || raw?.["@time"];

  return parseDateAndTimeAsUTC(date, time);
}

// Extract candidate matches for all supported shapes
function collectCandidateGames(payload) {
  if (!payload) return [];

  // NFL
  if (Array.isArray(payload?.games?.game)) return payload.games.game;

  // NBA / NHL: scores.category.match[]
  const cat = payload?.scores?.category;
  if (cat) {
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = cats.flatMap((c) => {
      if (Array.isArray(c?.match)) return c.match;
      if (c?.match) return [c.match];
      return [];
    });
    if (matches.length) return matches;
  }

  // EPL / UCL: commentaries.tournament.match[]
  const comm = payload?.commentaries?.tournament;
  if (comm) {
    const ts = Array.isArray(comm) ? comm : [comm];
    const matches = ts.flatMap((t) => {
      if (Array.isArray(t?.match)) return t.match;
      if (t?.match) return [t.match];
      return [];
    });
    if (matches.length) return matches;
  }

  if (Array.isArray(payload?.game)) return payload.game;
  if (Array.isArray(payload)) return payload;

  if (typeof payload === "object") {
    const arrs = Object.values(payload).filter(Array.isArray);
    if (arrs.length) return arrs.flat();
  }

  return [];
}

function normalizeGameRow(r) {
  const homeName =
    r?.hometeam?.name ||
    r?.home_name ||
    r?.home ||
    r?.home_team ||
    (r?.localteam && (r.localteam["@name"] || r.localteam.name)) ||
    "";

  const awayName =
    r?.awayteam?.name ||
    r?.away_name ||
    r?.away ||
    r?.away_team ||
    (r?.visitorteam && (r.visitorteam["@name"] || r.visitorteam.name)) ||
    "";

  const homeScore = Number(
    r?.hometeam?.totalscore ??
      r?.home_score ??
      r?.home_final ??
      (r?.localteam && (r.localteam["@goals"] || r.localteam["@ft_score"])) ??
      0
  );

  const awayScore = Number(
    r?.awayteam?.totalscore ??
      r?.away_score ??
      r?.away_final ??
      (r?.visitorteam && (r.visitorteam["@goals"] || r.visitorteam["@ft_score"])) ??
      0
  );

  const status = String(
    r?.status ||
      r?.game_status ||
      r?.state ||
      r?.["@status"] ||
      ""
  ).trim();

  return { homeName, awayName, homeScore, awayScore, status };
}

// Team matching (strict to avoid "New York" vs "New Orleans" false positives).
function teamMatchesOneSide(apiName, wantName, wantCode) {
  const nApi = norm(apiName);
  const nWant = norm(wantName);
  const code = trimU(wantCode);

  if (!nApi) return false;

  // 1) exact normalized name
  if (nWant && nApi === nWant) return true;

  // 2) code/acronym match
  const apiAcr = acronym(apiName);
  const wantAcr = acronym(wantName);
  if (code && apiAcr && apiAcr === code) return true;
  if (wantAcr && apiAcr && apiAcr === wantAcr) return true;

  // 3) mascot token
  const apiParts = nApi.split(" ").filter(Boolean);
  const wantParts = nWant.split(" ").filter(Boolean);
  if (!apiParts.length || !wantParts.length) return false;

  const apiMascot = apiParts[apiParts.length - 1];
  const wantMascot = wantParts[wantParts.length - 1];
  if (apiMascot && wantMascot && apiMascot === wantMascot) return true;

  return false;
}

function unorderedTeamsMatchByTokens(homeName, awayName, AName, BName, ACode, BCode) {
  const hA = teamMatchesOneSide(homeName, AName, ACode);
  const aB = teamMatchesOneSide(awayName, BName, BCode);
  const hB = teamMatchesOneSide(homeName, BName, BCode);
  const aA = teamMatchesOneSide(awayName, AName, ACode);
  return (hA && aB) || (hB && aA);
}

// yyyy-MM-dd -> dd.MM.yyyy
function isoToDdmmyyyy(iso) {
  const [Y, M, D] = String(iso).split("-");
  return `${D}.${M}.${Y}`;
}

// Iterate dates from fromISO to toISO-1
function* iterateDateRange(fromISO, toISO) {
  if (!fromISO || !toISO) return;
  const from = new Date(fromISO + "T00:00:00Z");
  const to = new Date(toISO + "T00:00:00Z");
  for (let d = new Date(from); d < to; d.setUTCDate(d.getUTCDate() + 1)) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    yield `${yyyy}-${mm}-${dd}`;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core lookup
// ─────────────────────────────────────────────────────────────────────────────

async function lookupWinnerCode() {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  if (!sportPath || !leaguePaths.length) {
    throw Error(`Unsupported league: ${league}`);
  }

  const targetAName = teamAName;
  const targetBName = teamBName;
  const targetACode = teamACode;
  const targetBCode = teamBCode;

  const candidates = [];

  for (const iso of iterateDateRange(dateFrom, dateTo)) {
    const ddmmyyyy = isoToDdmmyyyy(iso);

    for (const lp of leaguePaths) {
      const url =
        `${GOALSERVE_BASE_URL.replace(/\/+$/, "")}/` +
        `${encodeURIComponent(GOALSERVE_API_KEY)}/` +
        `${sportPath}/` +
        `${lp}?date=${encodeURIComponent(ddmmyyyy)}&json=1`;

      const payload = await fetchJsonWithRetry(url);
      const rawGames = collectCandidateGames(payload);
      if (!rawGames.length) continue;

      for (const r of rawGames) {
        const g = normalizeGameRow(r);
        if (
          !unorderedTeamsMatchByTokens(
            g.homeName,
            g.awayName,
            targetAName,
            targetBName,
            targetACode,
            targetBCode
          )
        ) {
          continue;
        }

        const kickoff = kickoffEpochFromRaw(r);
        candidates.push({
          homeName: g.homeName,
          awayName: g.awayName,
          homeScore: g.homeScore,
          awayScore: g.awayScore,
          status: g.status,
          kickoff,
        });
      }
    }
  }

  if (!candidates.length) {
    throw Error("No matching games found in Goalserve feed");
  }

  // Prefer:
  // 1) closest kickoff to lockTime (if present)
  // 2) final over non-final
  candidates.sort((a, b) => {
    const t1 =
      typeof a.kickoff === "number"
        ? Math.abs(a.kickoff - lockTime)
        : Number.MAX_SAFE_INTEGER;
    const t2 =
      typeof b.kickoff === "number"
        ? Math.abs(b.kickoff - lockTime)
        : Number.MAX_SAFE_INTEGER;

    if (t1 !== t2) return t1 - t2;

    const f1 = isFinalStatus(a.status) ? 1 : 0;
    const f2 = isFinalStatus(b.status) ? 1 : 0;
    return f2 - f1;
  });

  const best = candidates[0];

  if (!isFinalStatus(best.status)) {
    throw Error(`Game not final yet (status="${best.status}")`);
  }

  // Determine winner and return the *team code* your contract expects.
  let winnerCode = "TIE";

  if (best.homeScore > best.awayScore) {
    const homeIsA = teamMatchesOneSide(best.homeName, targetAName, targetACode);
    const homeIsB = teamMatchesOneSide(best.homeName, targetBName, targetBCode);

    if (homeIsA && !homeIsB) winnerCode = targetACode;
    else if (homeIsB && !homeIsA) winnerCode = targetBCode;
    else winnerCode = "TIE";
  } else if (best.awayScore > best.homeScore) {
    const awayIsA = teamMatchesOneSide(best.awayName, targetAName, targetACode);
    const awayIsB = teamMatchesOneSide(best.awayName, targetBName, targetBCode);

    if (awayIsA && !awayIsB) winnerCode = targetACode;
    else if (awayIsB && !awayIsA) winnerCode = targetBCode;
    else winnerCode = "TIE";
  } else {
    winnerCode = "TIE";
  }

  console.log(
    `WINNER_CODE: ${winnerCode} | ${best.homeName} vs ${best.awayName} | ${best.homeScore}-${best.awayScore} | status=${best.status}`
  );

  return winnerCode;
}

// ─────────────────────────────────────────────────────────────────────────────
// EXEC
// ─────────────────────────────────────────────────────────────────────────────

const winnerCode = await lookupWinnerCode();
return Functions.encodeString(winnerCode);
