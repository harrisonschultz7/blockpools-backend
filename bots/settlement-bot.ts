// bots/settlement-bot.ts
// @ts-nocheck
try { require("dotenv").config(); } catch {}

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ethers } from "ethers";
import { gamePoolAbi as IMPORTED_GAMEPOOL_ABI } from "./gamepool.abi";

// ---- debug banner (helps prove which file is running) ----
console.log(`[BOT BANNER] using LOCAL settlement-bot.ts :: ${new Date().toISOString()}`);

/* ──────────────────────────────────────────────────────────────────────────────
   ESM-safe __dirname / __filename
────────────────────────────────────────────────────────────────────────────── */
const __filename =
  typeof (globalThis as any).__filename !== "undefined"
    ? (globalThis as any).__filename
    : fileURLToPath(import.meta.url);
const __dirname =
  typeof (globalThis as any).__dirname !== "undefined"
    ? (globalThis as any).__dirname
    : path.dirname(__filename);

/* ──────────────────────────────────────────────────────────────────────────────
   Config / ENV
────────────────────────────────────────────────────────────────────────────── */
const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const SUBSCRIPTION_ID = BigInt(process.env.SUBSCRIPTION_ID!);
const FUNCTIONS_GAS_LIMIT = Number(process.env.FUNCTIONS_GAS_LIMIT || 300000);
const DON_SECRETS_SLOT = Number(process.env.DON_SECRETS_SLOT || 0);

const DRY_RUN = /^(1|true)$/i.test(String(process.env.DRY_RUN || ""));

/** Always require provider-final check */
const REQUIRE_FINAL_CHECK = true;

/** Minimum time after lock before we consider settlement (seconds). */
const POSTGAME_MIN_ELAPSED = Number(process.env.POSTGAME_MIN_ELAPSED || 600);
/** Cooldown after lock, to avoid racey starts (seconds). */
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

// Concurrency controls
const READ_CONCURRENCY   = Number(process.env.READ_CONCURRENCY   || 25);
const TX_SEND_CONCURRENCY= Number(process.env.TX_SEND_CONCURRENCY|| 3);

const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 0);

// 🔐 Goalserve secrets
const GOALSERVE_API_KEY  = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL = (process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed").replace(/\/+$/,"");

// DON pointer (activeSecrets.json) lookup
const GITHUB_OWNER = process.env.GITHUB_OWNER || "harrisonschultz7";
const GITHUB_REPO  = process.env.GITHUB_REPO  || "blockpools-backend";
const GITHUB_REF   = process.env.GITHUB_REF   || "main";
const GH_PAT       = process.env.GH_PAT;

// games.json discovery
const GAMES_PATH_OVERRIDE = process.env.GAMES_PATH || "";
const GAMES_CANDIDATES = [
  path.resolve(__dirname, "..", "src", "data", "games.json"),
  path.resolve(__dirname, "..", "games.json"),
  path.resolve(__dirname, "..", "..", "frontend-src", "src", "data", "games.json"),
  path.resolve(process.cwd(), "src", "data", "games.json"),
  path.resolve(process.cwd(), "games.json"),
];

// Functions source discovery
const SOURCE_CANDIDATES = [
  path.resolve(__dirname, "source.js"),
  path.resolve(__dirname, "..", "bots", "source.js"),
  path.resolve(process.cwd(), "bots", "source.js"),
];

/* ──────────────────────────────────────────────────────────────────────────────
   ABI loader
────────────────────────────────────────────────────────────────────────────── */
const MIN_ABI = [
  { inputs: [], name: "league",      outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamAName",   outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamBName",   outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamACode",   outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamBCode",   outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "isLocked",    outputs: [{ type: "bool"   }], stateMutability: "view", type: "function" },
  { inputs: [], name: "requestSent", outputs: [{ type: "bool"   }], stateMutability: "view", type: "function" },
  { inputs: [], name: "winningTeam", outputs: [{ type: "uint8"  }], stateMutability: "view", type: "function" },
  { inputs: [], name: "lockTime",    outputs: [{ type: "uint256"}], stateMutability: "view", type: "function" },
  { inputs: [], name: "owner",       outputs: [{ type: "address"}], stateMutability: "view", type: "function" },
  {
    inputs: [
      { type: "string",   name: "source" },
      { type: "string[]", name: "args" },
      { type: "uint64",   name: "subscriptionId" },
      { type: "uint32",   name: "gasLimit" },
      { type: "uint8",    name: "donHostedSecretsSlotID" },
      { type: "uint64",   name: "donHostedSecretsVersion" },
      { type: "bytes32",  name: "donID" },
    ],
    name: "sendRequest",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
] as const;

function loadGamePoolAbi(): { abi: any } {
  const ARTIFACT_PATH_ENV = process.env.ARTIFACT_PATH?.trim();
  const candidates = [
    ARTIFACT_PATH_ENV && (path.isAbsolute(ARTIFACT_PATH_ENV) ? ARTIFACT_PATH_ENV : path.resolve(process.cwd(), ARTIFACT_PATH_ENV)),
    path.resolve(__dirname, "..", "..", "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(__dirname, "..", "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(process.cwd(), "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
  ].filter(Boolean) as string[];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const parsed = JSON.parse(fs.readFileSync(p, "utf8"));
        console.log(`✅ Using ABI from ${p}`);
        return { abi: parsed.abi };
      }
    } catch {}
  }

  if (IMPORTED_GAMEPOOL_ABI && Array.isArray(IMPORTED_GAMEPOOL_ABI) && IMPORTED_GAMEPOOL_ABI.length) {
    console.warn("⚠️  Using ABI from local import (gamepool.abi).");
    return { abi: IMPORTED_GAMEPOOL_ABI };
  }

  console.warn("⚠️  Could not locate GamePool.json or imported ABI. Using minimal ABI.");
  return { abi: MIN_ABI };
}
const { abi: poolAbi } = loadGamePoolAbi();

/* ──────────────────────────────────────────────────────────────────────────────
   Small utils
────────────────────────────────────────────────────────────────────────────── */
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));
function limiter(concurrency: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  const next = () => { active--; if (queue.length) queue.shift()!(); };
  return async function run<T>(fn: () => Promise<T>): Promise<T> {
    if (active >= concurrency) await new Promise<void>(res => queue.push(res));
    active++; try { return await fn(); } finally { next(); }
  };
}
const finalsSet = new Set(["final", "finished", "full time", "ft"]);

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
  return parts.map(p => p[0]?.toUpperCase() || "").join("");
}

function epochToEtISO(epochSec: number) {
  const dt = new Date(epochSec * 1000);
  const fmt = new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" });
  const parts: Record<string, string> = {};
  for (const p of fmt.formatToParts(dt)) parts[p.type] = p.value;
  return `${parts.year}-${parts.month}-${parts.day}`;
}
function addDaysISO(iso: string, days: number) {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
}

const finalsRegex = /\bfinal\b/i;

/* ──────────────────────────────────────────────────────────────────────────────
   games.json loader (robust to multiple shapes)
────────────────────────────────────────────────────────────────────────────── */
type GameMeta = {
  contractAddress: string;
  tsdbEventId?: number | string;
  date?: string; time?: string; teamA?: string; teamB?: string;
};

function normalizeGameList(raw: any): GameMeta[] {
  const out: GameMeta[] = [];
  if (!raw) return out;

  if (!Array.isArray(raw) && typeof raw === "object" && raw.contractAddress) {
    out.push(raw as GameMeta); return out;
  }
  if (Array.isArray(raw)) {
    for (const it of raw) if (it && it.contractAddress) out.push(it as GameMeta);
    return out;
  }
  if (!Array.isArray(raw) && typeof raw === "object") {
    for (const arr of Object.values(raw)) {
      if (Array.isArray(arr)) for (const it of arr) if (it && it.contractAddress) out.push(it as GameMeta);
    }
  }
  return out;
}

function readGamesMetaAtPath(p: string): GameMeta[] | null {
  if (!fs.existsSync(p)) return null;
  try {
    const raw = fs.readFileSync(p, "utf8");
    const parsed = JSON.parse(raw);
    const items = normalizeGameList(parsed);
    if (items.length) { console.log(`Using games from ${p} (${items.length} contracts)`); return items; }
  } catch (e) { console.warn(`Failed to parse ${p}:`, (e as Error).message); }
  return null;
}

function loadGamesMeta(): GameMeta[] {
  const envList = (process.env.CONTRACTS || "").trim();
  if (envList) {
    const arr = envList.split(/[,\s]+/).filter(Boolean);
    const filtered = arr.filter((a) => { try { return ethers.utils.isAddress(a); } catch { return false; }});
    if (filtered.length) {
      console.log(`Using CONTRACTS from env (${filtered.length})`);
      return Array.from(new Set(filtered)).map(addr => ({ contractAddress: addr }));
    }
  }

  if (GAMES_PATH_OVERRIDE) {
    const fromOverride = readGamesMetaAtPath(GAMES_PATH_OVERRIDE);
    if (fromOverride) return fromOverride;
    console.warn(`GAMES_PATH was set but not readable/usable: ${GAMES_PATH_OVERRIDE}`);
  }
  for (const p of GAMES_CANDIDATES) {
    const fromLocal = readGamesMetaAtPath(p); if (fromLocal) return fromLocal;
  }
  console.warn("No contracts found in games.json or CONTRACTS env. Nothing to do.");
  return [];
}

function loadSourceCode(): string {
  for (const p of SOURCE_CANDIDATES) {
    try {
      if (fs.existsSync(p)) {
        const src = fs.readFileSync(p, "utf8");
        if (src && src.trim().length > 0) { console.log(`🧠 Loaded Functions source from: ${p}`); return src; }
      }
    } catch {}
  }
  throw new Error(`Could not find source.js. Tried:\n- ${SOURCE_CANDIDATES.join("\n- ")}`);
}

/* ──────────────────────────────────────────────────────────────────────────────
   DON pointer (activeSecrets.json)
────────────────────────────────────────────────────────────────────────────── */
function loadActiveSecretsLocal(): { secretsVersion: number; donId: string } | null {
  const p = path.join(process.cwd(), "activeSecrets.json");
  try {
    if (fs.existsSync(p)) {
      const j = JSON.parse(fs.readFileSync(p, "utf8"));
      if (j?.secretsVersion && j?.donId) return { secretsVersion: Number(j.secretsVersion), donId: String(j.donId) };
    }
  } catch {}
  return null;
}

async function loadActiveSecrets(): Promise<{ secretsVersion: number; donId: string; source: string }> {
  const envVersion = process.env.DON_SECRETS_VERSION ?? process.env.SECRETS_VERSION;
  const envDonId = process.env.DON_ID;
  if (envVersion && envDonId) return { secretsVersion: Number(envVersion), donId: envDonId, source: "env" };

  const local = loadActiveSecretsLocal();
  if (local) return { ...local, source: "local" };

  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/activeSecrets.json?ref=${GITHUB_REF}`;
  const headers: any = {
    ...(GH_PAT ? { Authorization: `Bearer ${GH_PAT}` } : {}),
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "blockpools-settlement-bot/1.0",
    Accept: "application/vnd.github+json",
  };
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`activeSecrets.json HTTP ${res.status}`);
  const data = await res.json() as any;
  const json = JSON.parse(Buffer.from(data.content, "base64").toString("utf8"));
  return { secretsVersion: Number(json.secretsVersion ?? json.version), donId: json.donId || "fun-ethereum-sepolia-1", source: "github" };
}

/* ──────────────────────────────────────────────────────────────────────────────
   Goalserve helpers: fetching, parsing, matching
────────────────────────────────────────────────────────────────────────────── */
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

function collectCandidateGames(payload: any): any[] {
  if (!payload) return [];
  // common shapes
  if (Array.isArray(payload?.games?.game)) return payload.games.game;
  if (Array.isArray(payload?.game)) return payload.game;
  // scores category (nfl-scores)
  const cat = payload?.scores?.category;
  if (cat) {
    const cats = Array.isArray(cat) ? cat : [cat];
    const m = cats.flatMap((c: any) => Array.isArray(c?.match) ? c.match : []);
    if (m.length) return m;
  }
  if (Array.isArray(payload)) return payload;
  if (typeof payload === "object") {
    const arrs = Object.values(payload).filter(v => Array.isArray(v)) as any[];
    if (arrs.length) return arrs.flat();
  }
  return [];
}

function normalizeGameRow(r: any) {
  const homeName = r?.hometeam?.name ?? r?.home_name ?? r?.home ?? "";
  const awayName = r?.awayteam?.name ?? r?.away_name ?? r?.away ?? "";

  const homeScore = Number(r?.hometeam?.totalscore ?? r?.home_score ?? 0);
  const awayScore = Number(r?.awayteam?.totalscore ?? r?.away_score ?? 0);

  const status = String(r?.status || r?.game_status || r?.state || r?.status_text || r?.statusShort || "").trim();

  return { homeName, awayName, homeScore, awayScore, status, __raw: r };
}

// Parse "26.10.2025 17:00" (UTC)
function parseDatetimeUTC(s?: string): number | undefined {
  if (!s) return;
  const m = s.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{1,2}):(\d{2})$/);
  if (!m) return;
  const [_, dd, MM, yyyy, HH, mm] = m;
  const t = Date.UTC(Number(yyyy), Number(MM)-1, Number(dd), Number(HH), Number(mm), 0);
  return isFinite(t) ? Math.floor(t/1000) : undefined;
}

// Parse "26.10.2025" + "1:00 PM"
function parseDateAndTimeAsUTC(dateStr?: string, timeStr?: string): number | undefined {
  if (!dateStr) return;
  const md = dateStr.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!md) return;
  const [_, dd, MM, yyyy] = md;
  let h = 0, mi = 0;
  if (timeStr) {
    const ampm = timeStr.trim().toUpperCase();
    const mh = ampm.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/);
    if (mh) {
      h = Number(mh[1]); mi = Number(mh[2]);
      const mer = mh[3];
      if (mer === "PM" && h < 12) h += 12;
      if (mer === "AM" && h === 12) h = 0;
    } else {
      const mh24 = ampm.match(/^(\d{1,2}):(\d{2})$/);
      if (mh24) { h = Number(mh24[1]); mi = Number(mh24[2]); }
    }
  }
  const t = Date.UTC(Number(yyyy), Number(MM)-1, Number(dd), h, mi, 0);
  return isFinite(t) ? Math.floor(t/1000) : undefined;
}

function kickoffEpochFromRaw(raw: any): number | undefined {
  const t1 = parseDatetimeUTC(raw?.datetime_utc);
  if (t1) return t1;
  return parseDateAndTimeAsUTC(raw?.date ?? raw?.formatted_date, raw?.time ?? raw?.start_time ?? raw?.start);
}

// Team matching (unordered: (home,away) == (A,B) or (B,A))
function teamMatchesOneSide(apiName: string, wantName: string, wantCode: string): boolean {
  const nApi = norm(apiName);
  const nWant = norm(wantName);
  const code = trimU(wantCode);
  if (!nApi) return false;

  // exact normalized name
  if (nApi && nWant && nApi === nWant) return true;

  // acronym match (e.g., "NYG" ↔ "New York Giants")
  const apiAcr = acronym(apiName);
  const wantAcr = acronym(wantName);
  if (code && apiAcr === code) return true;
  if (wantAcr && apiAcr && apiAcr === wantAcr) return true;

  // token containment
  const tokens = new Set(nApi.split(" "));
  const wantTokens = new Set(nWant.split(" "));
  const overlap = [...wantTokens].some(t => t.length > 2 && tokens.has(t));
  if (overlap) return true;

  return false;
}

function unorderedTeamsMatch(homeName: string, awayName: string, AName: string, BName: string, ACode: string, BCode: string) {
  const hA = teamMatchesOneSide(homeName, AName, ACode);
  const aB = teamMatchesOneSide(awayName, BName, BCode);
  const hB = teamMatchesOneSide(homeName, BName, BCode);
  const aA = teamMatchesOneSide(awayName, AName, ACode);
  return (hA && aB) || (hB && aA);
}

function goalserveLeaguePaths(leagueLabel: string): { sportPath: string, leaguePaths: string[] } {
  // NFL-first
  return { sportPath: "football", leaguePaths: ["nfl-scores", "nfl"] };
}

async function tryFetchGoalserve(league: string, lockTime: number) {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  const offsets = [0, +1, -1];
  const tried: string[] = [];

  for (const off of offsets) {
    const baseISO = epochToEtISO(lockTime);
    const d = addDaysISO(baseISO, off);
    const [Y, M, D] = d.split("-");
    const ddmmyyyy = `${D}.${M}.${Y}`;

    for (const lp of leaguePaths) {
      const url = `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/${sportPath}/${lp}?date=${encodeURIComponent(ddmmyyyy)}&json=1`;
      tried.push(url);
      try {
        const payload = await fetchJsonWithRetry(url, 3, 500);
        const rawGames = collectCandidateGames(payload);
        if (rawGames.length) {
          const games = rawGames.map((r) => {
            const g = normalizeGameRow(r);
            return { ...g, __kickoff: kickoffEpochFromRaw(r), __raw: r };
          });
          return { ok: true, dateTried: ddmmyyyy, path: lp, games, url, tried };
        }
      } catch {
        // continue
      }
    }
  }
  return { ok: false, tried };
}

async function confirmFinalGoalserve(params: {
  league: string;
  lockTime: number;
  teamAName: string; teamBName: string;
  teamACode?: string; teamBCode?: string;
}): Promise<{ ok: boolean; winner?: "A" | "B" | "TIE"; reason?: string, debug?: any }> {
  if (!GOALSERVE_API_KEY) return { ok: false, reason: "missing GOALSERVE_API_KEY" };

  const resp = await tryFetchGoalserve(params.league, params.lockTime);
  if (!resp.ok) return { ok: false, reason: "no games (all fetch attempts failed)" };

  const aName = params.teamAName, bName = params.teamBName;
  const aCode = params.teamACode ?? "", bCode = params.teamBCode ?? "";

  // 1) Filter to team matches (unordered)
  const candidates = resp.games.filter(g =>
    unorderedTeamsMatch(g.homeName, g.awayName, aName, bName, aCode, bCode)
  );

  if (!candidates.length) {
    const sample = resp.games.slice(0, 4).map(g => `${g.awayName || "?"} @ ${g.homeName || "?"}`).join(" | ");
    return { ok: false, reason: `no team match (sample: ${sample || "none"})`, debug: { url: resp.url, date: resp.dateTried } };
  }

  // 2) Sort by proximity to lockTime, then prefer already-final
  candidates.sort((g1, g2) => {
    const t1 = g1.__kickoff ?? Number.MAX_SAFE_INTEGER;
    const t2 = g2.__kickoff ?? Number.MAX_SAFE_INTEGER;
    const d1 = Math.abs(t1 - params.lockTime);
    const d2 = Math.abs(t2 - params.lockTime);
    if (d1 !== d2) return d1 - d2;
    const f1 = finalsSet.has((g1.status || "").toLowerCase()) ? 1 : 0;
    const f2 = finalsSet.has((g2.status || "").toLowerCase()) ? 1 : 0;
    return f2 - f1;
  });

  const match = candidates[0];

  // resilient final detection (field or raw JSON contains "final")
  let isFinal = finalsSet.has((match.status || "").toLowerCase());
  if (!isFinal) {
    try {
      const rawStr = JSON.stringify(match.__raw || {});
      if (finalsRegex.test(rawStr)) isFinal = true;
    } catch {}
  }
  if (!isFinal) return { ok: false, reason: "not final", debug: { url: resp.url, date: resp.dateTried, picked: match } };

  // Decide winner by home/away scores vs which side is A or B
  const homeIsA = teamMatchesOneSide(match.homeName, aName, aCode);
  const homeIsB = teamMatchesOneSide(match.homeName, bName, bCode);

  let winner: "A" | "B" | "TIE" = "TIE";
  if (match.homeScore > match.awayScore) winner = homeIsA ? "A" : homeIsB ? "B" : "TIE";
  else if (match.awayScore > match.homeScore) winner = homeIsA ? "B" : homeIsB ? "A" : "TIE";

  return {
    ok: true,
    winner,
    debug: {
      url: resp.url,
      date: resp.dateTried,
      picked: {
        home: match.homeName,
        away: match.awayName,
        status: match.status,
        homeScore: match.homeScore,
        awayScore: match.awayScore,
        kickoff: match.__kickoff,
      }
    }
  };
}

/* ──────────────────────────────────────────────────────────────────────────────
   Error decoding helpers
────────────────────────────────────────────────────────────────────────────── */
const FUNCTIONS_ROUTER_ERRORS = [
  "error EmptyArgs()", "error EmptySource()", "error InsufficientBalance()",
  "error InvalidConsumer(address consumer)", "error InvalidSubscription()",
  "error SubscriptionIsPaused()", "error OnlyRouterCanFulfill()",
  "error RequestIsAlreadyPending()", "error UnsupportedDON()"
];
const routerIface = new ethers.utils.Interface(FUNCTIONS_ROUTER_ERRORS);
const iface = new ethers.utils.Interface(poolAbi);
function decodeRevert(data?: string) {
  if (!data || typeof data !== "string" || !data.startsWith("0x") || data.length < 10) return "unknown";
  try { return iface.parseError(data).name; } catch {}
  try { return routerIface.parseError(data).name; } catch {}
  try {
    if (data.slice(0,10) === "0x08c379a0") {
      const [msg] = ethers.utils.defaultAbiCoder.decode(["string"], "0x"+data.slice(10));
      return `Error("${msg}")`;
    }
  } catch {}
  return "unknown";
}

/* ──────────────────────────────────────────────────────────────────────────────
   MAIN
────────────────────────────────────────────────────────────────────────────── */
async function main() {
  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  if (!process.env.SUBSCRIPTION_ID) throw new Error("Missing SUBSCRIPTION_ID");

  console.log(`[CFG] DRY_RUN=${DRY_RUN} (env=${process.env.DRY_RUN ?? "(unset)"})`);
  console.log(`[CFG] SUBSCRIPTION_ID=${process.env.SUBSCRIPTION_ID}`);
  console.log(`[CFG] REQUIRE_FINAL_CHECK=${REQUIRE_FINAL_CHECK} POSTGAME_MIN_ELAPSED=${POSTGAME_MIN_ELAPSED}s`);
  console.log(`[CFG] Provider=Goalserve (finality precheck enabled)`);
  console.log(`[CFG] Scores provider = ${process.env.SCORES_PROVIDER || "goalserve"}`);

  const provider = new ethers.providers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const { secretsVersion, donId, source } = await loadActiveSecrets();
  const donHostedSecretsVersion = BigInt(secretsVersion);
  const donBytes = ethers.utils.formatBytes32String(donId);
  console.log(`🔐 Loaded DON pointer from ${source}`);
  console.log(`   secretsVersion = ${secretsVersion}`);
  console.log(`   donId          = ${donId}`);

  const SOURCE = loadSourceCode();

  const gamesMeta = loadGamesMeta();
  if (!gamesMeta.length) {
    console.log("No games to process.");
    return;
  }

  const readLimit = limiter(READ_CONCURRENCY);
  const sendLimit = limiter(TX_SEND_CONCURRENCY);
  const botAddr = (await wallet.getAddress()).toLowerCase();

  type PoolState = {
    addr: string; league: string;
    teamAName: string; teamBName: string;
    teamACode: string; teamBCode: string;
    isLocked: boolean; requestSent: boolean; winningTeam: number;
    lockTime: number; isOwner: boolean;
  };

  const states: PoolState[] = [];
  await Promise.all(
    gamesMeta.map(({ contractAddress }) =>
      readLimit(async () => {
        const addr = contractAddress;
        const pool = new ethers.Contract(addr, poolAbi, wallet);

        let onchainOwner = "(read failed)";
        try { onchainOwner = await pool.owner(); } catch {}
        const isOwner = onchainOwner !== "(read failed)" && onchainOwner.toLowerCase() === botAddr;
        if (!isOwner) return;

        try {
          const [lg, ta, tb, tca, tcb, locked, req, win, lt] = await Promise.all([
            pool.league(), pool.teamAName(), pool.teamBName(),
            pool.teamACode(), pool.teamBCode(),
            pool.isLocked(), pool.requestSent(),
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
            requestSent: Boolean(req),
            winningTeam: Number(win),
            lockTime: Number(lt),
            isOwner,
          });
        } catch (e) {
          console.warn(`[READ FAIL] ${addr}:`, (e as Error).message);
        }
      })
    )
  );

  const nowSec = Math.floor(Date.now() / 1000);

  // Pools we own, are locked, not requested, and no winningTeam yet
  const gated = states.filter(s => s.isOwner && s.isLocked && !s.requestSent && s.winningTeam === 0);

  // Time gates
  const timeGated = gated.filter(s =>
    (s.lockTime === 0 || nowSec >= s.lockTime + REQUEST_GAP_SECONDS) &&
    (s.lockTime === 0 || nowSec >= s.lockTime + POSTGAME_MIN_ELAPSED)
  );

  if (!timeGated.length) {
    console.log("No eligible pools after time gates. Submitted 0 transaction(s).");
    return;
  }

  if (REQUIRE_FINAL_CHECK && !GOALSERVE_API_KEY) {
    console.log("GOALSERVE_API_KEY not set; cannot confirm final state. Submitted 0 transaction(s).");
    return;
  }

  // STEP 2: Provider finality checks — NFL-first with date offsets and closest-kickoff tie-break
  const finalEligible: PoolState[] = [];
  for (const s of timeGated) {
    const pre = await confirmFinalGoalserve({
      league: s.league,
      lockTime: s.lockTime,
      teamAName: s.teamAName,
      teamBName: s.teamBName,
      teamACode: s.teamACode,
      teamBCode: s.teamBCode,
    });

    if (pre.ok) {
      const dbg = pre.debug || {};
      console.log(`[OK] FINAL: ${s.league} ${s.teamAName} vs ${s.teamBName} :: winner=${pre.winner} :: date=${dbg.date} url=${dbg.url}`);
      if (dbg.picked) {
        console.log(`     picked=${dbg.picked.away} @ ${dbg.picked.home} status=${dbg.picked.status} score=${dbg.picked.awayScore}-${dbg.picked.homeScore} kickoff=${dbg.picked.kickoff}`);
      }
      finalEligible.push(s);
    } else {
      console.log(`[SKIP] ${s.league} ${s.teamAName} vs ${s.teamBName} :: ${pre.reason}`);
    }
  }

  if (!finalEligible.length) {
    console.log("No games confirmed FINAL. Submitted 0 transaction(s).");
    return;
  }
  console.log(`✅ Provider confirmed FINAL for ${finalEligible.length} pool(s). Proceeding.`);

  /* STEP 3: Simulate & send — always 8 args (no event id) */
  const { secretsVersion: sv } = await loadActiveSecrets();
  const donHostedSecretsVersion2 = BigInt(sv);

  const buildArgs8 = (s: PoolState): string[] => {
    const d0 = epochToEtISO(s.lockTime), d1 = addDaysISO(d0, 1);
    return [
      s.league,
      d0,
      d1,
      s.teamACode.toUpperCase(),
      s.teamBCode.toUpperCase(),
      s.teamAName,
      s.teamBName,
      String(s.lockTime),
    ];
  };

  let submitted = 0;

  for (const s of finalEligible) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(s.addr, poolAbi, wallet);
    const args = buildArgs8(s);

    // Simulate
    try {
      await pool.callStatic.sendRequest(
        SOURCE,
        args,
        SUBSCRIPTION_ID,
        FUNCTIONS_GAS_LIMIT,
        DON_SECRETS_SLOT,
        donHostedSecretsVersion2,
        ethers.utils.formatBytes32String(s.league.includes("Arbitrum") ? "fun-arbitrum-sepolia-1" : "fun-ethereum-sepolia-1")
      );
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      console.error(`[SIM ERR] ${s.addr} => ${decodeRevert(data)}`);
      continue;
    }

    // Send
    if (!DRY_RUN) {
      await sendLimit(async () => {
        try {
          if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
          const tx = await pool.sendRequest(
            SOURCE,
            args,
            SUBSCRIPTION_ID,
            FUNCTIONS_GAS_LIMIT,
            DON_SECRETS_SLOT,
            donHostedSecretsVersion2,
            ethers.utils.formatBytes32String("fun-ethereum-sepolia-1")
          );
          console.log(`[OK] sendRequest ${s.addr} (args8): ${tx.hash}`);
          submitted++;
        } catch (e: any) {
          const data = e?.data ?? e?.error?.data;
          console.error(`[ERR] sendRequest ${s.addr}:`, e?.reason || e?.message || e);
          if (data?.slice) console.error(` selector = ${data.slice(0,10)} (${decodeRevert(data)})`);
        }
      });
    } else {
      console.log(`[DRY_RUN] Would sendRequest ${s.addr} using args8`);
    }
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => { console.error(e); process.exit(1); });
