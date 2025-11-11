// bots/source.js
// Chainlink Functions source for BlockPools: NFL + NBA final score resolver via Goalserve.
// Expects args:
// [0] league        (e.g. "NFL", "NBA")
// [1] fromDateIso   (YYYY-MM-DD) - game day ET
// [2] toDateIso     (YYYY-MM-DD) - unused for Goalserve, kept for future providers
// [3] teamACode
// [4] teamBCode
// [5] teamAName
// [6] teamBName
// [7] lockTime (epoch seconds, as string)

const GOALSERVE_API_KEY = Secrets.GOALSERVE_API_KEY;
const GOALSERVE_BASE_URL =
  Secrets.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed";

if (!GOALSERVE_API_KEY) {
  throw Error("Missing GOALSERVE_API_KEY in DON-hosted secrets");
}

const [
  league,
  fromDateIso,
  _toDateIso,
  teamACode,
  teamBCode,
  teamAName,
  teamBName,
  lockTimeStr,
] = args;

// ───────────────────────── helpers: league → path ─────────────────────────

function goalserveLeaguePaths(leagueLabel) {
  const L = String(leagueLabel || "").trim().toLowerCase();

  if (L === "nfl") {
    return { sportPath: "football", leaguePaths: ["nfl-scores"] };
  }

  if (L === "nba") {
    // As per provider docs & your sample:
    // /bsktbl/nba-scores?date=dd.MM.yyyy&json=1
    return { sportPath: "bsktbl", leaguePaths: ["nba-scores"] };
  }

  return { sportPath: "", leaguePaths: [] };
}

// ───────────────────────── helpers: status / names ─────────────────────────

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
  const s = String(raw || "").trim().toLowerCase();
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

const norm = (s) =>
  String(s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const trimU = (s) => String(s || "").trim().toUpperCase();

function acronym(s) {
  const parts = String(s || "")
    .split(/[^a-zA-Z0-9]+/)
    .filter(Boolean);
  return parts.map((p) => p[0]?.toUpperCase() || "").join("");
}

// ───────────────────────── helpers: payload parsing ─────────────────────────

function collectCandidateGames(payload) {
  if (!payload) return [];

  // NFL style: payload.games.game[]
  if (Array.isArray(payload?.games?.game)) return payload.games.game;

  // Basketball sample: scores.category.match[]
  const cat = payload?.scores?.category;
  if (cat) {
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = cats.flatMap((c) =>
      Array.isArray(c?.match) ? c.match : c?.match ? [c.match] : []
    );
    if (matches.length) return matches;
  }

  if (Array.isArray(payload?.game)) return payload.game;

  // Last-resort: flatten array-like values
  if (Array.isArray(payload)) return payload;
  if (typeof payload === "object") {
    const arrs = Object.values(payload).filter((v) => Array.isArray(v));
    if (arrs.length) return arrs.flat();
  }

  return [];
}

function normalizeGameRow(r) {
  const homeName =
    r?.hometeam?.name ?? r?.home_name ?? r?.home ?? r?.home_team ?? "";
  const awayName =
    r?.awayteam?.name ?? r?.away_name ?? r?.away ?? r?.away_team ?? "";
  const homeScore = Number(
    r?.hometeam?.totalscore ?? r?.home_score ?? r?.home_final ?? 0
  );
  const awayScore = Number(
    r?.awayteam?.totalscore ?? r?.away_score ?? r?.away_final ?? 0
  );
  const status = String(
    r?.status || r?.game_status || r?.state || ""
  ).trim();
  return { homeName, awayName, homeScore, awayScore, status };
}

// datetime_utc: "11.10.2025 23:00"
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

// date: "11.10.2025", time: "7:00 PM" or "19:00"
function parseDateAndTimeAsUTC(dateStr, timeStr) {
  if (!dateStr) return;
  const md = String(dateStr).match(/^(\d{1,2})\.(\d{1,2})\.(\d{4})$/);
  if (!md) return;
  const [, dd, MM, yyyy] = md;
  let h = 0;
  let mi = 0;

  if (timeStr) {
    const t = String(timeStr).trim().toUpperCase();
    const mh = t.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/);
    if (mh) {
      h = +mh[1];
      mi = +mh[2];
      const mer = mh[3];
      if (mer === "PM" && h < 12) h += 12;
      if (mer === "AM" && h === 12) h = 0;
    } else {
      const mh24 = t.match(/^(\d{1,2}):(\d{2})$/);
      if (mh24) {
        h = +mh24[1];
        mi = +mh24[2];
      }
    }
  }

  const ts = Date.UTC(+yyyy, +MM - 1, +dd, h, mi, 0, 0);
  if (!isFinite(ts)) return;
  return Math.floor(ts / 1000);
}

function kickoffEpochFromRaw(r) {
  const t1 = parseDatetimeUTC(r?.datetime_utc);
  if (t1) return t1;
  return parseDateAndTimeAsUTC(
    r?.date ?? r?.formatted_date,
    r?.time ?? r?.start_time ?? r?.start
  );
}

// ───────────────────────── helpers: team matching ─────────────────────────

function teamMatchesOneSide(apiName, wantName, wantCode) {
  const nApi = norm(apiName);
  const nWant = norm(wantName);
  const code = trimU(wantCode);

  if (!nApi) return false;

  // exact normalized
  if (nApi && nWant && nApi === nWant) return true;

  const apiAcr = acronym(apiName);
  const wantAcr = acronym(wantName);
  if (code && apiAcr === code) return true;
  if (wantAcr && apiAcr && apiAcr === wantAcr) return true;

  // token overlap heuristic
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

// ───────────────────────── HTTP helper (via Functions) ─────────────────────────

async function fetchJsonWithRetry(url, tries = 3, backoffMs = 400) {
  let lastError;
  for (let i = 0; i < tries; i++) {
    const res = await Functions.makeHttpRequest({ url });
    if (!res || res.error || res.response?.status >= 400) {
      lastError = res?.error || res?.response?.status;
      if (i < tries - 1) {
        await new Promise((r) =>
          setTimeout(r, backoffMs * (i + 1))
        );
      }
    } else {
      return res.data;
    }
  }
  throw Error(
    `HTTP error for ${url}: ${lastError || "unknown"}`
  );
}

// ───────────────────────── main resolver ─────────────────────────

async function resolveWinner() {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  if (!sportPath || !leaguePaths.length) {
    // unsupported league → return 0
    return Functions.encodeUint256(0);
  }

  // fromDateIso: "YYYY-MM-DD" → "dd.MM.yyyy"
  const [Y, M, D] = String(fromDateIso || "").split("-");
  if (!Y || !M || !D) {
    return Functions.encodeUint256(0);
  }
  const ddmmyyyy = `${D}.${M}.${Y}`;
  const lockTime = Number(lockTimeStr || 0);

  const AName = String(teamAName || "");
  const BName = String(teamBName || "");
  const ACode = String(teamACode || "");
  const BCode = String(teamBCode || "");

  for (const lp of leaguePaths) {
    const url = `${GOALSERVE_BASE_URL.replace(
      /\/+$/,
      ""
    )}/${GOALSERVE_API_KEY}/${sportPath}/${lp}?date=${ddmmyyyy}&json=1`;

    const payload = await fetchJsonWithRetry(url);
    const rawGames = collectCandidateGames(payload);
    if (!rawGames.length) continue;

    const games = rawGames.map((r) => {
      const g = normalizeGameRow(r);
      return {
        ...g,
        __kickoff: kickoffEpochFromRaw(r),
      };
    });

    const candidates = games.filter((g) =>
      unorderedTeamsMatchByTokens(
        g.homeName,
        g.awayName,
        AName,
        BName,
        ACode,
        BCode
      )
    );

    if (!candidates.length) {
      continue;
    }

    // Prefer closest to lockTime, then final
    candidates.sort((g1, g2) => {
      const t1 =
        typeof g1.__kickoff === "number"
          ? g1.__kickoff
          : Number.MAX_SAFE_INTEGER;
      const t2 =
        typeof g2.__kickoff === "number"
          ? g2.__kickoff
          : Number.MAX_SAFE_INTEGER;

      const d1 = Math.abs(t1 - lockTime);
      const d2 = Math.abs(t2 - lockTime);
      if (d1 !== d2) return d1 - d2;

      const f1 = isFinalStatus(g1.status) ? 1 : 0;
      const f2 = isFinalStatus(g2.status) ? 1 : 0;
      return f2 - f1;
    });

    const match = candidates[0];

    if (!isFinalStatus(match.status)) {
      // Not final yet → 0 (bot will treat as pending)
      return Functions.encodeUint256(0);
    }

    const homeIsA = teamMatchesOneSide(
      match.homeName,
      AName,
      ACode
    );
    const homeIsB = teamMatchesOneSide(
      match.homeName,
      BName,
      BCode
    );

    let winner = "TIE";
    if (match.homeScore > match.awayScore) {
      winner = homeIsA ? "A" : homeIsB ? "B" : "TIE";
    } else if (match.awayScore > match.homeScore) {
      winner = homeIsA ? "B" : homeIsB ? "A" : "TIE";
    }

    let winnerIdx = 0;
    if (winner === "A") winnerIdx = 1;
    else if (winner === "B") winnerIdx = 2;
    else if (winner === "TIE") winnerIdx = 3;

    return Functions.encodeUint256(winnerIdx);
  }

  // No matching game found
  return Functions.encodeUint256(0);
}

return await resolveWinner();
