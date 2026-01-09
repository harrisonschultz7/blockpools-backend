// bots/settlement-bot.ts
// @ts-nocheck
//
// "Watcher → markReady" mode (hardened):
// - Off-chain Goalserve check decides when a game is FINAL.
// - On-chain action is ONLY: SettlementCoordinator.markReady(pool)
// - Chainlink Automation (on SettlementCoordinator) then sends the Functions request.
//
// HARDENING GOALS:
// - lockTime is treated as the game-start anchor (proxy).
// - NEVER match a previously played game: do not select a candidate whose kickoff < lockTime.
// - Prefer returning "not final"/errors over picking the wrong historical game.
// - Avoid premature markReady if REQUIRE_FINAL_CHECK is disabled accidentally.
//
// Required env:
//   RPC_URL
//   PRIVATE_KEY
//   SETTLEMENT_COORDINATOR_ADDRESS
//   GOALSERVE_API_KEY   (if REQUIRE_FINAL_CHECK=true)
//
// Optional env:
//   DRY_RUN=1
//   REQUIRE_FINAL_CHECK=1|0 (default true)
//   ALLOW_UNSAFE_NO_FINAL_CHECK=1|0 (default false)  <-- must be true to allow REQUIRE_FINAL_CHECK=false mode
//   POSTGAME_MIN_ELAPSED=600
//   REQUEST_GAP_SECONDS=120
//   READ_CONCURRENCY=25
//   TX_SEND_CONCURRENCY=3
//   MAX_TX_PER_RUN=20
//   REQUEST_DELAY_MS=0
//   GAMES_PATH=... (optional override)
//   CONTRACTS="0x...,0x..." (fallback if no games.json)
//   KICKOFF_MIN_TOLERANCE_SECONDS=0  (default 0; allows kickoff >= lockTime - tolerance)
//   KICKOFF_MAX_LOOKAHEAD_SECONDS=172800 (default 48h; ignore kickoff absurdly far after lockTime)
//   REQUIRE_KICKOFF_FOR_MATCH=true (default true; safest)
//   FINAL_DEBOUNCE_SECONDS=300 (default 300; require final to be observed for at least this long across runs)
//   FINAL_CACHE_PATH=/opt/blockpools/.final-cache.json
//

try {
  require("dotenv").config();
} catch {}

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ethers } from "ethers";
import { gamePoolAbi as IMPORTED_GAMEPOOL_ABI } from "./gamepool.abi";

/* ────────────────────────────────────────────────────────────────────────────
   ESM-safe __dirname / __filename
──────────────────────────────────────────────────────────────────────────── */
const __filename =
  typeof (globalThis as any).__filename !== "undefined"
    ? (globalThis as any).__filename
    : fileURLToPath(import.meta.url);

const __dirname =
  typeof (globalThis as any).__dirname !== "undefined"
    ? (globalThis as any).__dirname
    : path.dirname(__filename);

/* ────────────────────────────────────────────────────────────────────────────
   Config / ENV
──────────────────────────────────────────────────────────────────────────── */
const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const SETTLEMENT_COORDINATOR_ADDRESS = (process.env.SETTLEMENT_COORDINATOR_ADDRESS || "").trim();

const DRY_RUN = /^(1|true)$/i.test(String(process.env.DRY_RUN || ""));

const REQUIRE_FINAL_CHECK =
  process.env.REQUIRE_FINAL_CHECK == null
    ? true
    : /^(1|true)$/i.test(String(process.env.REQUIRE_FINAL_CHECK || ""));

const ALLOW_UNSAFE_NO_FINAL_CHECK = /^(1|true)$/i.test(
  String(process.env.ALLOW_UNSAFE_NO_FINAL_CHECK || "")
);

const POSTGAME_MIN_ELAPSED = Number(process.env.POSTGAME_MIN_ELAPSED || 600);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

const READ_CONCURRENCY = Number(process.env.READ_CONCURRENCY || 25);
const TX_SEND_CONCURRENCY = Number(process.env.TX_SEND_CONCURRENCY || 3);
const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 20);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 0);

// Kickoff constraints (NO BACKWARD LOOK)
const KICKOFF_MIN_TOLERANCE_SECONDS = Number(process.env.KICKOFF_MIN_TOLERANCE_SECONDS || 0);
const KICKOFF_MAX_LOOKAHEAD_SECONDS = Number(process.env.KICKOFF_MAX_LOOKAHEAD_SECONDS || 48 * 3600);
const REQUIRE_KICKOFF_FOR_MATCH =
  process.env.REQUIRE_KICKOFF_FOR_MATCH == null
    ? true
    : /^(1|true)$/i.test(String(process.env.REQUIRE_KICKOFF_FOR_MATCH || ""));

// Debounce final across runs
const FINAL_DEBOUNCE_SECONDS = Number(process.env.FINAL_DEBOUNCE_SECONDS || 300);
const FINAL_CACHE_PATH = process.env.FINAL_CACHE_PATH || "/opt/blockpools/.final-cache.json";

// Goalserve
const GOALSERVE_API_KEY = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL = process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed";
const GOALSERVE_DEBUG = /^(1|true)$/i.test(String(process.env.GOALSERVE_DEBUG || ""));

// games.json discovery
const GAMES_PATH_OVERRIDE = process.env.GAMES_PATH || "";
const GAMES_CANDIDATES = [
  path.resolve(__dirname, "..", "src", "data", "games.json"),
  path.resolve(__dirname, "..", "games.json"),
  path.resolve(__dirname, "..", "..", "frontend-src", "src", "data", "games.json"),
  path.resolve(process.cwd(), "src", "data", "games.json"),
  path.resolve(process.cwd(), "games.json"),
];

/* ────────────────────────────────────────────────────────────────────────────
   ABI loader (GamePool view-only)
──────────────────────────────────────────────────────────────────────────── */
const FALLBACK_MIN_ABI = [
  { inputs: [], name: "league", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamAName", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamBName", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamACode", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamBCode", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "isLocked", outputs: [{ type: "bool" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "winningTeam", outputs: [{ type: "uint8" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "lockTime", outputs: [{ type: "uint256" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "owner", outputs: [{ type: "address" }], stateMutability: "view", type: "function" },
] as const;

function loadGamePoolAbi(): { abi: any; source: "imported" | "minimal" } {
  if (IMPORTED_GAMEPOOL_ABI && Array.isArray(IMPORTED_GAMEPOOL_ABI) && IMPORTED_GAMEPOOL_ABI.length) {
    console.warn("⚠️  Using ABI from local import (gamepool.abi).");
    return { abi: IMPORTED_GAMEPOOL_ABI, source: "imported" };
  }
  console.warn("⚠️  Using minimal fallback ABI (read-only).");
  return { abi: FALLBACK_MIN_ABI, source: "minimal" };
}

const { abi: poolAbi } = loadGamePoolAbi();

/* ────────────────────────────────────────────────────────────────────────────
   SettlementCoordinator ABI (minimal)
──────────────────────────────────────────────────────────────────────────── */
const SETTLEMENT_COORDINATOR_ABI = [
  "function markReady(address pool) external",
  "function ready(address pool) view returns (bool)",
  "function pending(address pool) view returns (bool)",
  "function isKnownPool(address pool) view returns (bool)",
];

/* ────────────────────────────────────────────────────────────────────────────
   Small utils
──────────────────────────────────────────────────────────────────────────── */
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function limiter(concurrency: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  const next = () => {
    active--;
    if (queue.length) queue.shift()!();
  };
  return async function run<T>(fn: () => Promise<T>): Promise<T> {
    if (active >= concurrency) await new Promise<void>((res) => queue.push(res));
    active++;
    try {
      return await fn();
    } finally {
      next();
    }
  };
}

function epochToEtISO(epochSec: number) {
  const dt = new Date(epochSec * 1000);
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts: any = {};
  for (const p of fmt.formatToParts(dt)) parts[p.type] = p.value;
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function addDaysISO(iso: string, days: number) {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
}

/* ────────────────────────────────────────────────────────────────────────────
   FINAL debounce cache
──────────────────────────────────────────────────────────────────────────── */
type FinalCache = Record<string, { firstSeen: number; lastSeen: number }>;

function loadFinalCache(): FinalCache {
  try {
    return JSON.parse(fs.readFileSync(FINAL_CACHE_PATH, "utf8"));
  } catch {
    return {};
  }
}

function saveFinalCache(c: FinalCache) {
  try {
    fs.writeFileSync(FINAL_CACHE_PATH, JSON.stringify(c, null, 2));
  } catch {}
}

function cacheKeyForPool(addr: string) {
  return String(addr || "").toLowerCase();
}

/* ────────────────────────────────────────────────────────────────────────────
   games.json loader
──────────────────────────────────────────────────────────────────────────── */
type GameMeta = {
  contractAddress: string;
  tsdbEventId?: number | string;
  date?: string;
  time?: string;
  teamA?: string;
  teamB?: string;
};

function normalizeGameList(raw: any): GameMeta[] {
  const out: GameMeta[] = [];
  if (!raw) return out;

  if (!Array.isArray(raw) && typeof raw === "object" && raw.contractAddress) {
    out.push(raw as GameMeta);
    return out;
  }

  if (Array.isArray(raw)) {
    for (const it of raw) if (it && it.contractAddress) out.push(it as GameMeta);
    return out;
  }

  if (!Array.isArray(raw) && typeof raw === "object") {
    for (const arr of Object.values(raw)) {
      if (Array.isArray(arr)) {
        for (const it of arr) {
          if (it && it.contractAddress) out.push(it as GameMeta);
        }
      }
    }
  }
  return out;
}

function readGamesMetaAtPath(p: string): GameMeta[] | null {
  if (!fs.existsSync(p)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(p, "utf8"));
    const items = normalizeGameList(parsed);
    if (items.length) {
      console.log(`Using games from ${p} (${items.length} contracts)`);
      return items;
    }
  } catch (e) {
    console.warn(`Failed to parse ${p}:`, (e as Error).message);
  }
  return null;
}

function loadGamesMeta(): GameMeta[] {
  if (GAMES_PATH_OVERRIDE) {
    const fromOverride = readGamesMetaAtPath(GAMES_PATH_OVERRIDE);
    if (fromOverride) return fromOverride;
    console.warn(`GAMES_PATH was set but not readable/usable: ${GAMES_PATH_OVERRIDE}`);
  }

  for (const p of GAMES_CANDIDATES) {
    const fromLocal = readGamesMetaAtPath(p);
    if (fromLocal) return fromLocal;
  }

  const envList = (process.env.CONTRACTS || "").trim();
  if (envList) {
    const arr = envList.split(/[,\s]+/).filter(Boolean);
    const filtered = arr.filter((a) => {
      try {
        return ethers.isAddress(a);
      } catch {
        return false;
      }
    });
    if (filtered.length) {
      console.log(`Using CONTRACTS from env (${filtered.length})`);
      return Array.from(new Set(filtered)).map((addr) => ({ contractAddress: addr }));
    }
  }

  console.warn("No contracts found in games.json or CONTRACTS env. Nothing to do.");
  return [];
}

/* ────────────────────────────────────────────────────────────────────────────
   Goalserve helpers
──────────────────────────────────────────────────────────────────────────── */

// final-ish labels
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

function isFinalStatus(raw: string): boolean {
  const s = (raw || "").trim().toLowerCase();
  if (!s) return false;
  if (finalsSet.has(s)) return true;
  if (s.includes("after over time") || s.includes("after overtime") || s.includes("after ot")) return true;
  if (s.includes("full time") || s === "full-time") return true;
  if (s.includes("final") && !s.includes("semi") && !s.includes("quarter") && !s.includes("half")) return true;
  return false;
}

const norm = (s: string) =>
  (s || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "")
    .replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const trimU = (s?: string) => String(s || "").trim().toUpperCase();

function acronym(s: string): string {
  const parts = (s || "").split(/[^a-zA-Z0-9]+/).filter(Boolean);
  return parts.map((p) => (p[0] || "").toUpperCase()).join("");
}

async function fetchJsonWithRetry(url: string, tries = 3, backoffMs = 400) {
  let lastErr: any;
  for (let i = 0; i < tries; i++) {
    try {
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      return await res.json();
    } catch (e) {
      lastErr = e;
      if (i < tries - 1) await sleep(backoffMs * (i + 1));
    }
  }
  throw lastErr;
}

/**
 * Map on-chain league label -> Goalserve sportPath + leaguePaths.
 */
function goalserveLeaguePaths(leagueLabel: string): { sportPath: string; leaguePaths: string[] } {
  const L = String(leagueLabel || "").trim().toLowerCase();
  if (L === "nfl") return { sportPath: "football", leaguePaths: ["nfl-scores"] };
  if (L === "nba") return { sportPath: "bsktbl", leaguePaths: ["nba-scores"] };
  if (L === "nhl") return { sportPath: "hockey", leaguePaths: ["nhl-scores"] };
  if (L === "epl" || L === "premier league" || L === "england - premier league" || L === "england premier league")
    return { sportPath: "commentaries", leaguePaths: ["1204"] };
  if (L === "ucl" || L === "uefa champions league" || L === "champions league")
    return { sportPath: "commentaries", leaguePaths: ["1005"] };
  return { sportPath: "", leaguePaths: [] };
}

function parseDatetimeUTC(s?: string): number | undefined {
  if (!s) return;
  const m = String(s).match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return;
  const [, dd, MM, yyyy, HH, mm] = m;
  const t = Date.UTC(+yyyy, +MM - 1, +dd, +HH, +mm, 0, 0);
  return isFinite(t) ? Math.floor(t / 1000) : undefined;
}

function parseDateAndTimeAsUTC(dateStr?: string, timeStr?: string): number | undefined {
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
  return isFinite(t) ? Math.floor(t / 1000) : undefined;
}

function kickoffEpochFromRaw(raw: any): number | undefined {
  const t1 = parseDatetimeUTC(raw?.datetime_utc || raw?.["@datetime_utc"]);
  if (t1) return t1;

  const date = raw?.formatted_date || raw?.date || raw?.["@formatted_date"] || raw?.["@date"];
  const time = raw?.time || raw?.start_time || raw?.start || raw?.["@time"];

  return parseDateAndTimeAsUTC(date, time);
}

function collectCandidateGames(payload: any): any[] {
  if (!payload) return [];
  if (Array.isArray(payload?.games?.game)) return payload.games.game;

  const cat = payload?.scores?.category;
  if (cat) {
    const cats = Array.isArray(cat) ? cat : [cat];
    const matches = cats.flatMap((c: any) => {
      if (Array.isArray(c?.match)) return c.match;
      if (c?.match) return [c.match];
      return [];
    });
    if (matches.length) return matches;
  }

  const comm = payload?.commentaries?.tournament;
  if (comm) {
    const ts = Array.isArray(comm) ? comm : [comm];
    const matches = ts.flatMap((t: any) => {
      if (Array.isArray(t?.match)) return t.match;
      if (t?.match) return [t.match];
      return [];
    });
    if (matches.length) return matches;
  }

  if (Array.isArray(payload?.game)) return payload.game;
  if (Array.isArray(payload)) return payload;

  if (typeof payload === "object") {
    const arrs = Object.values(payload).filter((v) => Array.isArray(v)) as any[];
    if (arrs.length) return arrs.flat();
  }

  return [];
}

function normalizeGameRow(r: any) {
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

  const status = String(r?.status || r?.game_status || r?.state || r?.["@status"] || "").trim();

  return { homeName, awayName, homeScore, awayScore, status };
}

// Strict matching to avoid false positives
function teamMatchesOneSide(apiName: string, wantName: string, wantCode: string): boolean {
  const nApi = norm(apiName);
  const nWant = norm(wantName);
  const code = trimU(wantCode);

  if (!nApi) return false;

  // 1) Exact normalized name
  if (nWant && nApi === nWant) return true;

  // 2) Code/acronym match
  const apiAcr = acronym(apiName);
  const wantAcr = acronym(wantName);
  if (code && apiAcr && apiAcr === code) return true;
  if (wantAcr && apiAcr && apiAcr === wantAcr) return true;

  // 3) Mascot match (last token)
  const apiParts = nApi.split(" ").filter(Boolean);
  const wantParts = nWant.split(" ").filter(Boolean);
  if (!apiParts.length || !wantParts.length) return false;

  const apiMascot = apiParts[apiParts.length - 1];
  const wantMascot = wantParts[wantParts.length - 1];
  if (apiMascot && wantMascot && apiMascot === wantMascot) return true;

  return false;
}

function unorderedTeamsMatchByTokens(
  homeName: string,
  awayName: string,
  AName: string,
  BName: string,
  ACode: string,
  BCode: string
) {
  const hA = teamMatchesOneSide(homeName, AName, ACode);
  const aB = teamMatchesOneSide(awayName, BName, BCode);
  const hB = teamMatchesOneSide(homeName, BName, BCode);
  const aA = teamMatchesOneSide(awayName, AName, ACode);
  return (hA && aB) || (hB && aA);
}

// Build candidate URL list for lockTime day ET and the next day ET
function buildGoalserveUrlsForLockTime(league: string, lockTime: number): string[] {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  if (!sportPath || !leaguePaths.length) return [];

  const d0 = epochToEtISO(lockTime);
  const d1 = addDaysISO(d0, 1);
  const dateIsos = [d0, d1];

  const urls: string[] = [];
  for (const iso of dateIsos) {
    const [Y, M, D] = iso.split("-");
    const ddmmyyyy = `${D}.${M}.${Y}`;
    for (const lp of leaguePaths) {
      const url =
        `${GOALSERVE_BASE_URL.replace(/\/+$/, "")}/${encodeURIComponent(GOALSERVE_API_KEY)}` +
        `/${sportPath}/${lp}?date=${encodeURIComponent(ddmmyyyy)}&json=1`;
      urls.push(url);
    }
  }
  return urls;
}

type FinalCheckResult = {
  ok: boolean;
  winner?: "A" | "B" | "TIE";
  winnerCode?: string;
  reason?: string;
  debug?: any;
};

// CRITICAL: never look backward. Only accept kickoff >= lockTime - tolerance.
function kickoffIsAcceptable(kickoff: number | undefined, lockTime: number): boolean {
  if (kickoff == null) return !REQUIRE_KICKOFF_FOR_MATCH;
  const minOk = lockTime - Math.max(0, KICKOFF_MIN_TOLERANCE_SECONDS);
  const maxOk = lockTime + Math.max(0, KICKOFF_MAX_LOOKAHEAD_SECONDS);
  return kickoff >= minOk && kickoff <= maxOk;
}

async function confirmFinalGoalserve(params: {
  league: string;
  lockTime: number;
  teamAName: string;
  teamBName: string;
  teamACode?: string;
  teamBCode?: string;
}): Promise<FinalCheckResult> {
  if (!GOALSERVE_API_KEY) return { ok: false, reason: "missing GOALSERVE_API_KEY" };

  const aName = params.teamAName;
  const bName = params.teamBName;
  const aCode = params.teamACode ?? "";
  const bCode = params.teamBCode ?? "";

  const urls = buildGoalserveUrlsForLockTime(params.league, params.lockTime);
  if (!urls.length) return { ok: false, reason: "unsupported league" };

  let bestMatch: any = null;
  let bestUrl: string | null = null;

  // Try all relevant urls; only accept candidates matching teams AND kickoff constraint
  for (const url of urls) {
    try {
      const payload = await fetchJsonWithRetry(url, 3, 500);
      const rawGames = collectCandidateGames(payload);
      if (!rawGames.length) continue;

      const rows = rawGames.map((r: any) => {
        const g = normalizeGameRow(r);
        const kickoff = kickoffEpochFromRaw(r);
        return { ...g, __kickoff: kickoff, __raw: r };
      });

      // Team match
      const teamMatches = rows.filter((g: any) =>
        unorderedTeamsMatchByTokens(g.homeName, g.awayName, aName, bName, aCode, bCode)
      );
      if (!teamMatches.length) continue;

      // Kickoff constraint (NO BACKWARD LOOK)
      const kickoffFiltered = teamMatches.filter((g: any) => kickoffIsAcceptable(g.__kickoff, params.lockTime));
      if (!kickoffFiltered.length) {
        if (GOALSERVE_DEBUG) {
          console.log(
            `[DBG] Found team matches but all failed kickoff window (lockTime=${params.lockTime}) at ${url}`
          );
        }
        continue;
      }

      // Prefer FINAL first, then closest kickoff to lockTime
      kickoffFiltered.sort((g1: any, g2: any) => {
        const f1 = isFinalStatus(g1.status || "") ? 1 : 0;
        const f2 = isFinalStatus(g2.status || "") ? 1 : 0;
        if (f1 !== f2) return f2 - f1;

        const t1 = g1.__kickoff ?? Number.MAX_SAFE_INTEGER;
        const t2 = g2.__kickoff ?? Number.MAX_SAFE_INTEGER;
        const d1 = Math.abs(t1 - params.lockTime);
        const d2 = Math.abs(t2 - params.lockTime);
        return d1 - d2;
      });

      const candidate = kickoffFiltered[0];

      // Keep the best candidate across urls using the same ordering
      if (!bestMatch) {
        bestMatch = candidate;
        bestUrl = url;
      } else {
        const candIsFinal = isFinalStatus(candidate.status || "") ? 1 : 0;
        const bestIsFinal = isFinalStatus(bestMatch.status || "") ? 1 : 0;

        if (candIsFinal !== bestIsFinal) {
          if (candIsFinal > bestIsFinal) {
            bestMatch = candidate;
            bestUrl = url;
          }
        } else {
          const ct = candidate.__kickoff ?? Number.MAX_SAFE_INTEGER;
          const bt = bestMatch.__kickoff ?? Number.MAX_SAFE_INTEGER;
          const cd = Math.abs(ct - params.lockTime);
          const bd = Math.abs(bt - params.lockTime);
          if (cd < bd) {
            bestMatch = candidate;
            bestUrl = url;
          }
        }
      }
    } catch (e: any) {
      if (GOALSERVE_DEBUG) console.log(`[GOALSERVE_ERR] ${url} :: ${e?.message || e}`);
    }
  }

  if (!bestMatch) {
    return {
      ok: false,
      reason: REQUIRE_KICKOFF_FOR_MATCH ? "no match after lockTime (kickoff constrained)" : "no match",
      debug: GOALSERVE_DEBUG ? { tried: urls.length } : undefined,
    };
  }

  console.log(
    `[GOALSERVE] ${params.league} | Team A: ${params.teamAName} (${params.teamACode || "-"}) ` +
      `vs Team B: ${params.teamBName} (${params.teamBCode || "-"}) | ` +
      `API Home: ${bestMatch.homeName} ${bestMatch.homeScore} | API Away: ${bestMatch.awayName} ${bestMatch.awayScore} | ` +
      `status=${bestMatch.status || ""} | kickoff=${bestMatch.__kickoff ?? "?"} | url=${GOALSERVE_DEBUG ? bestUrl : "(hidden)"}`
  );

  // Final status requirement
  const isFinal = isFinalStatus(bestMatch.status || "");
  if (!isFinal) {
    return {
      ok: false,
      reason: "not final",
      debug: GOALSERVE_DEBUG ? { url: bestUrl, status: bestMatch.status, kickoff: bestMatch.__kickoff } : undefined,
    };
  }

  // Winner mapping
  const homeIsA = teamMatchesOneSide(bestMatch.homeName, aName, aCode);
  const homeIsB = teamMatchesOneSide(bestMatch.homeName, bName, bCode);
  const awayIsA = teamMatchesOneSide(bestMatch.awayName, aName, aCode);
  const awayIsB = teamMatchesOneSide(bestMatch.awayName, bName, bCode);

  const mapsHomeA_AwayB = homeIsA && awayIsB && !homeIsB && !awayIsA;
  const mapsHomeB_AwayA = homeIsB && awayIsA && !homeIsA && !awayIsB;

  if (!mapsHomeA_AwayB && !mapsHomeB_AwayA) {
    return {
      ok: false,
      reason: "ambiguous team mapping",
      debug: GOALSERVE_DEBUG
        ? {
            url: bestUrl,
            picked: {
              home: bestMatch.homeName,
              away: bestMatch.awayName,
              homeScore: bestMatch.homeScore,
              awayScore: bestMatch.awayScore,
              status: bestMatch.status,
              kickoff: bestMatch.__kickoff,
            },
            flags: { homeIsA, homeIsB, awayIsA, awayIsB },
          }
        : undefined,
    };
  }

  let winner: "A" | "B" | "TIE" = "TIE";

  if (bestMatch.homeScore === bestMatch.awayScore) {
    winner = "TIE";
  } else if (bestMatch.homeScore > bestMatch.awayScore) {
    winner = mapsHomeA_AwayB ? "A" : "B";
  } else {
    winner = mapsHomeA_AwayB ? "B" : "A";
  }

  let winnerCode = "Tie";
  if (winner === "A") winnerCode = params.teamACode || params.teamAName;
  else if (winner === "B") winnerCode = params.teamBCode || params.teamBName;

  return {
    ok: true,
    winner,
    winnerCode,
    debug: GOALSERVE_DEBUG
      ? {
          url: bestUrl,
          picked: {
            home: bestMatch.homeName,
            away: bestMatch.awayName,
            status: bestMatch.status,
            homeScore: bestMatch.homeScore,
            awayScore: bestMatch.awayScore,
            kickoff: bestMatch.__kickoff,
          },
        }
      : undefined,
  };
}

/* ────────────────────────────────────────────────────────────────────────────
   MAIN
──────────────────────────────────────────────────────────────────────────── */
async function main() {
  console.log(`[BOT] settlement-bot (markReady) starting @ ${new Date().toISOString()}`);

  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  if (!SETTLEMENT_COORDINATOR_ADDRESS || !ethers.isAddress(SETTLEMENT_COORDINATOR_ADDRESS)) {
    throw new Error("Missing/invalid SETTLEMENT_COORDINATOR_ADDRESS env var (expected 0x...)");
  }

  console.log(`[CFG] DRY_RUN=${DRY_RUN} (env=${process.env.DRY_RUN ?? "(unset)"})`);
  console.log(
    `[CFG] REQUIRE_FINAL_CHECK=${REQUIRE_FINAL_CHECK} ` +
      `ALLOW_UNSAFE_NO_FINAL_CHECK=${ALLOW_UNSAFE_NO_FINAL_CHECK} ` +
      `POSTGAME_MIN_ELAPSED=${POSTGAME_MIN_ELAPSED}s REQUEST_GAP_SECONDS=${REQUEST_GAP_SECONDS}s`
  );
  console.log(
    `[CFG] KICKOFF_MIN_TOLERANCE_SECONDS=${KICKOFF_MIN_TOLERANCE_SECONDS}s ` +
      `KICKOFF_MAX_LOOKAHEAD_SECONDS=${KICKOFF_MAX_LOOKAHEAD_SECONDS}s ` +
      `REQUIRE_KICKOFF_FOR_MATCH=${REQUIRE_KICKOFF_FOR_MATCH}`
  );
  console.log(
    `[CFG] FINAL_DEBOUNCE_SECONDS=${FINAL_DEBOUNCE_SECONDS}s FINAL_CACHE_PATH=${FINAL_CACHE_PATH}`
  );
  console.log(`[CFG] SettlementCoordinator=${SETTLEMENT_COORDINATOR_ADDRESS}`);
  console.log(`[CFG] Provider=Goalserve (NFL + NBA + NHL + EPL + UCL)`);

  if (!REQUIRE_FINAL_CHECK && !ALLOW_UNSAFE_NO_FINAL_CHECK) {
    console.log(
      `⚠️  REQUIRE_FINAL_CHECK=false but ALLOW_UNSAFE_NO_FINAL_CHECK is not set. ` +
        `This run will NOT markReady without provider final confirmation.`
    );
  }

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const coordinator = new ethers.Contract(SETTLEMENT_COORDINATOR_ADDRESS, SETTLEMENT_COORDINATOR_ABI, wallet);

  const gamesMeta = loadGamesMeta();
  if (!gamesMeta.length) {
    console.log("No games to process.");
    return;
  }

  const readLimit = limiter(READ_CONCURRENCY);
  const sendLimit = limiter(TX_SEND_CONCURRENCY);
  const botAddr = (await wallet.getAddress()).toLowerCase();

  type PoolState = {
    addr: string;
    league: string;
    teamAName: string;
    teamBName: string;
    teamACode: string;
    teamBCode: string;
    isLocked: boolean;
    winningTeam: number;
    lockTime: number;
    isOwner: boolean;
  };

  const states: PoolState[] = [];

  await Promise.all(
    gamesMeta.map(({ contractAddress }) =>
      readLimit(async () => {
        const addr = String(contractAddress || "").trim();
        if (!ethers.isAddress(addr)) return;

        const pool = new ethers.Contract(addr, poolAbi, wallet);

        let onchainOwner = "(read failed)";
        try {
          onchainOwner = await pool.owner();
        } catch {}

        const isOwner = onchainOwner !== "(read failed)" && String(onchainOwner).toLowerCase() === botAddr;
        if (!isOwner) return;

        try {
          const [lg, ta, tb, tca, tcb, locked, win, lt] = await Promise.all([
            pool.league(),
            pool.teamAName(),
            pool.teamBName(),
            pool.teamACode(),
            pool.teamBCode(),
            pool.isLocked(),
            pool.winningTeam().then(Number),
            pool.lockTime().then(Number),
          ]);

          states.push({
            addr,
            league: String(lg || ""),
            teamAName: String(ta || ""),
            teamBName: String(tb || ""),
            teamACode: String(tca || ""),
            teamBCode: String(tcb || ""),
            isLocked: Boolean(locked),
            winningTeam: Number(win),
            lockTime: Number(lt),
            isOwner,
          });
        } catch (e: any) {
          if (GOALSERVE_DEBUG) console.warn(`[READ FAIL] ${addr}:`, e?.message || e);
        }
      })
    )
  );

  const nowSec = Math.floor(Date.now() / 1000);

  // Eligible on-chain state (unresolved, locked)
  const gated = states.filter((s) => s.isOwner && s.isLocked && s.winningTeam === 0);

  // Time gating (based on lockTime; treated as game-start proxy)
  const timeGated = gated.filter((s) => {
    if (!s.lockTime) return false; // require lockTime for safety
    const afterGap = nowSec >= s.lockTime + REQUEST_GAP_SECONDS;
    const afterMin = nowSec >= s.lockTime + POSTGAME_MIN_ELAPSED;
    return afterGap && afterMin;
  });

  if (!timeGated.length) {
    console.log("No eligible pools after time gates. Submitted 0 transaction(s).");
    return;
  }

  if (REQUIRE_FINAL_CHECK && !GOALSERVE_API_KEY) {
    console.log("GOALSERVE_API_KEY not set; cannot confirm final state. Submitted 0 transaction(s).");
    return;
  }

  const finalCache = loadFinalCache();

  // Off-chain FINAL confirmation
  const finalEligible: PoolState[] = [];

  for (const s of timeGated) {
    // If final check is disabled, require explicit override
    if (!REQUIRE_FINAL_CHECK) {
      if (!ALLOW_UNSAFE_NO_FINAL_CHECK) {
        console.log(`[SKIP] Unsafe mode not enabled; skipping without provider final check: ${s.addr}`);
        continue;
      }
      console.log(`[WARN] Unsafe mode enabled; treating eligible without provider final check: ${s.addr}`);
      finalEligible.push(s);
      continue;
    }

    const pre = await confirmFinalGoalserve({
      league: s.league,
      lockTime: s.lockTime,
      teamAName: s.teamAName,
      teamBName: s.teamBName,
      teamACode: s.teamACode,
      teamBCode: s.teamBCode,
    });

    if (pre.ok) {
      // Debounce: require FINAL to persist across runs for FINAL_DEBOUNCE_SECONDS
      const key = cacheKeyForPool(s.addr);
      const entry = finalCache[key];

      if (!entry) {
        finalCache[key] = { firstSeen: nowSec, lastSeen: nowSec };
        saveFinalCache(finalCache);
        console.log(
          `[FINAL-1] ${s.league} ${s.teamAName} vs ${s.teamBName} :: final observed, waiting ${FINAL_DEBOUNCE_SECONDS}s before markReady`
        );
        continue;
      }

      finalCache[key] = { firstSeen: entry.firstSeen, lastSeen: nowSec };
      saveFinalCache(finalCache);

      const age = nowSec - entry.firstSeen;
      if (age < FINAL_DEBOUNCE_SECONDS) {
        console.log(
          `[FINAL-WAIT] ${s.league} ${s.teamAName} vs ${s.teamBName} :: final observed for ${age}s (<${FINAL_DEBOUNCE_SECONDS}s). Waiting.`
        );
        continue;
      }

      console.log(`[FINAL] ${s.league} ${s.teamAName} vs ${s.teamBName} :: winner=${pre.winner} (${pre.winnerCode})`);
      finalEligible.push(s);
    } else if (pre.reason === "not final") {
      // If not final, clear debounce state so it must be re-observed
      const key = cacheKeyForPool(s.addr);
      if (finalCache[key]) {
        delete finalCache[key];
        saveFinalCache(finalCache);
      }
      console.log(`[PENDING] ${s.league} ${s.teamAName} vs ${s.teamBName} :: not final yet`);
    } else {
      // Any other error: clear debounce state and skip
      const key = cacheKeyForPool(s.addr);
      if (finalCache[key]) {
        delete finalCache[key];
        saveFinalCache(finalCache);
      }
      if (GOALSERVE_DEBUG) {
        console.log(`[SKIP][DBG] ${s.league} ${s.teamAName} vs ${s.teamBName} :: ${pre.reason || "no match"}`);
        if (pre.debug) console.log(pre.debug);
      } else {
        console.log(`[SKIP] ${s.league} ${s.teamAName} vs ${s.teamBName} :: ${pre.reason || "no match"}`);
      }
    }
  }

  if (!finalEligible.length) {
    console.log("No games confirmed FINAL (and debounced). Submitted 0 transaction(s).");
    return;
  }

  console.log(`✅ Provider confirmed FINAL for ${finalEligible.length} pool(s). Proceeding to markReady.`);

  let submitted = 0;

  for (const s of finalEligible) {
    if (submitted >= MAX_TX_PER_RUN) break;

    // Safety: only markReady for pools the coordinator knows about
    let known = false;
    let alreadyReady = false;
    let alreadyPending = false;

    try {
      [known, alreadyReady, alreadyPending] = await Promise.all([
        coordinator.isKnownPool(s.addr),
        coordinator.ready(s.addr),
        coordinator.pending(s.addr),
      ]);
    } catch (e: any) {
      console.log(`[warn] coordinator reads failed for ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName}). Skipping.`);
      continue;
    }

    if (!known) {
      console.log(`[skip] not registered in SettlementCoordinator: ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName})`);
      continue;
    }

    if (alreadyPending) {
      console.log(`[ok]  already pending: ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName})`);
      continue;
    }

    if (alreadyReady) {
      console.log(`[ok]  already ready:   ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName})`);
      continue;
    }

    if (DRY_RUN) {
      console.log(`[DRY_RUN] Would markReady ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName})`);
      submitted++;
      continue;
    }

    await sendLimit(async () => {
      try {
        if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);

        const tx = await coordinator.markReady(s.addr);
        console.log(`[TX] markReady ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName}) :: ${tx.hash}`);
        const r = await tx.wait(1);
        if (r.status !== 1) throw new Error("markReady tx failed");
        submitted++;
      } catch (e: any) {
        console.error(`[ERR] markReady ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName}):`, e?.reason || e?.message || e);
      }
    });
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
