// bots/source.js
// Chainlink Functions source for BlockPools settlement
// Supports: NFL, NBA, NHL via Goalserve
//
// ARGS (8):
//  0: league       (e.g. "NFL", "NBA", "NHL")
//  1: dateFrom     (yyyy-MM-dd, ET) - usually lockDate
//  2: dateTo       (yyyy-MM-dd, ET) - usually lockDate+1 for safety
//  3: teamACode
//  4: teamBCode
//  5: teamAName
//  6: teamBName
//  7: lockTime     (epoch seconds)
//
// RETURNS (uint8 encoded):
//  0 = no decision / not final / no matching game
//  1 = Team A wins
//  2 = Team B wins
//  3 = Tie

// NOTE: This runs in the Chainlink Functions runtime.
// - Use `secrets.GOALSERVE_API_KEY` + optional `secrets.GOALSERVE_BASE_URL`.
// - HTTP via Functions.makeHttpRequest.

if (!Array.isArray(args) || args.length < 8) {
  throw Error("Invalid args: expected 8");
}

const [
  leagueRaw,
  dateFromISO,
  dateToISO,
  teamACodeRaw,
  teamBCodeRaw,
  teamANameRaw,
  teamBNameRaw,
  lockTimeRaw,
] = args;

const league = String(leagueRaw || "").trim().toLowerCase();
const dateFrom = String(dateFromISO || "").trim();
const dateTo = String(dateToISO || "").trim();
const teamACode = String(teamACodeRaw || "").trim().toUpperCase();
const teamBCode = String(teamBCodeRaw || "").trim().toUpperCase();
const teamAName = String(teamANameRaw || "").trim();
const teamBName = String(teamBNameRaw || "").trim();
const lockTime = Number(lockTimeRaw || 0);

if (!league || !dateFrom || !dateTo || !teamAName || !teamBName) {
  throw Error("Missing required args");
}

const GOALSERVE_API_KEY = secrets.GOALSERVE_API_KEY;
if (!GOALSERVE_API_KEY) {
  throw Error("Missing GOALSERVE_API_KEY in secrets");
}

const GOALSERVE_BASE_URL =
  secrets.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed";

// ─────────────────────────────────────────────────────────────────────────────
// Helpers (mirrors settlement-bot.ts semantics)
// ─────────────────────────────────────────────────────────────────────────────

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

  return { sportPath: "", leaguePaths: [] };
}

// Known final-ish labels (incl OT variants)
const finalsSet = new Set([
  "final",
  "finished",
  "full time",
  "ft",
  "after over time",
  "after overtime",
  "final/ot",
  "final ot",
  "final aot",
  "final after ot",
]);

function isFinalStatus(raw) {
  const s = (raw || "").trim().toLowerCase();
  if (!s) return false;

  if (finalsSet.has(s)) return true;

  if (
    s.includes("after over time") ||
    s.includes("after overtime") ||
    s.includes("after ot")
  )
    return true;

  if (
    s.includes("final") &&
    !s.includes("semi") &&
    !s.includes("quarter") &&
    !s.includes("half")
  )
    return true;

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
  const m = String(s).match(
    /^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})$/
  );
  if (!m) return;
  const [, dd, MM, yyyy, HH, mm] = m;
  const t = Date.UTC(+yyyy, +MM - 1, +dd, +HH, +mm, 0, 0);
  if (!isFinite(t)) return;
  return Math.floor(t / 1000);
}

// Parse "dd.MM.yyyy" + optional time as UTC-ish
function parseDateAndTimeAsUTC(dateStr, timeStr) {
  if (!dateStr) return;
  const md = String(dateStr).match(
    /^(\d{1,2})\.(\d{1,2})\.(\d{4})$/
  );
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
  // Prefer explicit datetime_utc if matches our expected format
  const t1 = parseDatetimeUTC(raw?.datetime_utc);
  if (t1) return t1;

  // Fall back to formatted_date/date + time/start/start_time
  const date = raw?.formatted_date || raw?.date;
  const time = raw?.time || raw?.start_time || raw?.start;
  return parseDateAndTimeAsUTC(date, time);
}

// Extract all candidate games from various Goalserve shapes
function collectCandidateGames(payload) {
  if (!payload) return [];

  if (Array.isArray(payload?.games?.game)) {
    return payload.games.game;
  }

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
    "";
  const awayName =
    r?.awayteam?.name ||
    r?.away_name ||
    r?.away ||
    r?.away_team ||
    "";

  const homeScore = Number(
    r?.hometeam?.totalscore ??
      r?.home_score ??
      r?.home_final ??
      0
  );
  const awayScore = Number(
    r?.awayteam?.totalscore ??
      r?.away_score ??
      r?.away_final ??
      0
  );

  const status = String(
    r?.status || r?.game_status || r?.state || ""
  ).trim();

  return { homeName, awayName, homeScore, awayScore, status };
}

// Team matching identical to settlement-bot.ts
function teamMatchesOneSide(apiName, wantName, wantCode) {
  const nApi = norm(apiName);
  const nWant = norm(wantName);
  const code = trimU(wantCode);

  if (!nApi) return false;

  if (nApi && nWant && nApi === nWant) return true;

  const apiAcr = acronym(apiName);
  const wantAcr = acronym(wantName);

  if (code && apiAcr === code) return true;
  if (wantAcr && apiAcr && apiAcr === wantAcr) return true;

  const tokens = new Set(nApi.split(" "));
  const wantTokens = new Set(nWant.split(" "));
  const overlap = [...wantTokens].some(
    (t) => t.length > 2 && tokens.has(t)
  );

  return overlap;
}

function unorderedTeamsMatchByTokens(
  homeName,
  awayName,
  AName,
  BName,
  ACode,
  BCode
) {
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

// Iterate dates from fromISO to toISO-1 (safety window)
function* iterateDateRange(fromISO, toISO) {
  if (!fromISO || !toISO) return;
  const from = new Date(fromISO + "T00:00:00Z");
  const to = new Date(toISO + "T00:00:00Z");
  for (
    let d = new Date(from);
    d < to;
    d.setUTCDate(d.getUTCDate() + 1)
  ) {
    const yyyy = d.getUTCFullYear();
    const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(d.getUTCDate()).padStart(2, "0");
    yield `${yyyy}-${mm}-${dd}`;
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// Core lookup
// ─────────────────────────────────────────────────────────────────────────────

async function lookupWinner() {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(
    league
  );
  if (!sportPath || !leaguePaths.length) {
    // unsupported league, no decision
    return 0;
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
    // nothing matched
    return 0;
  }

  // Choose:
  // 1. Closest kickoff to lockTime (if available)
  // 2. Prefer final over non-final
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
    // Not final yet; no decision
    return 0;
  }

  let winnerFlag = 3; // default Tie/push
  if (best.homeScore > best.awayScore) {
    // Decide if home is A or B
    const homeIsA = teamMatchesOneSide(
      best.homeName,
      targetAName,
      targetACode
    );
    const homeIsB = teamMatchesOneSide(
      best.homeName,
      targetBName,
      targetBCode
    );
    if (homeIsA && !homeIsB) winnerFlag = 1;
    else if (homeIsB && !homeIsA) winnerFlag = 2;
  } else if (best.awayScore > best.homeScore) {
    const awayIsA = teamMatchesOneSide(
      best.awayName,
      targetAName,
      targetACode
    );
    const awayIsB = teamMatchesOneSide(
      best.awayName,
      targetBName,
      targetBCode
    );
    if (awayIsA && !awayIsB) winnerFlag = 1;
    else if (awayIsB && !awayIsA) winnerFlag = 2;
  }

  // If we somehow couldn't distinguish, treat as tie (3) instead of error.
  return winnerFlag;
}

// ─────────────────────────────────────────────────────────────────────────────
// EXEC
// ─────────────────────────────────────────────────────────────────────────────

const winnerEnum = await lookupWinner();

// Encoded uint8: 0,1,2,3
return Functions.encodeUint8(winnerEnum);
