// bots/settlement-bot.ts
// @ts-nocheck

try { require("dotenv").config(); } catch {}

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execFileSync } from "child_process";
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
const SUBSCRIPTION_ID = BigInt(process.env.SUBSCRIPTION_ID!);                  // uint64
const FUNCTIONS_GAS_LIMIT = Number(process.env.FUNCTIONS_GAS_LIMIT || 300000); // uint32
const DON_SECRETS_SLOT = Number(process.env.DON_SECRETS_SLOT || 0);            // uint8

const DRY_RUN = /^(1|true)$/i.test(String(process.env.DRY_RUN || ""));
const REQUIRE_FINAL_CHECK = true;
const POSTGAME_MIN_ELAPSED = Number(process.env.POSTGAME_MIN_ELAPSED || 600);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

const READ_CONCURRENCY    = Number(process.env.READ_CONCURRENCY    || 25);
const TX_SEND_CONCURRENCY = Number(process.env.TX_SEND_CONCURRENCY || 3);
const MAX_TX_PER_RUN   = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 0);

// Goalserve
const GOALSERVE_API_KEY  = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL = process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed";
const GOALSERVE_DEBUG    = /^(1|true)$/i.test(String(process.env.GOALSERVE_DEBUG || ""));

// Git (for activeSecrets.json fallback)
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

/* ────────────────────────────────────────────────────────────────────────────
   ABI loader
──────────────────────────────────────────────────────────────────────────── */
const FALLBACK_MIN_ABI = [
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

function loadGamePoolAbi(): { abi: any; source: "artifact" | "imported" | "minimal" } {
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
        return { abi: parsed.abi, source: "artifact" };
      }
    } catch {}
  }

  if (IMPORTED_GAMEPOOL_ABI && Array.isArray(IMPORTED_GAMEPOOL_ABI) && IMPORTED_GAMEPOOL_ABI.length) {
    console.warn("⚠️  Using ABI from local import (gamepool.abi).");
    return { abi: IMPORTED_GAMEPOOL_ABI, source: "imported" };
  }

  console.warn("⚠️  Could not locate GamePool.json or imported ABI. Using minimal ABI.");
  return { abi: FALLBACK_MIN_ABI, source: "minimal" };
}

const { abi: poolAbi } = loadGamePoolAbi();
const iface = new ethers.utils.Interface(poolAbi);

/* ────────────────────────────────────────────────────────────────────────────
   Small utils
──────────────────────────────────────────────────────────────────────────── */
const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

function limiter(concurrency: number) {
  let active = 0;
  const queue: Array<() => void> = [];
  const next = () => { active--; if (queue.length) queue.shift()!(); };
  return async function run<T>(fn: () => Promise<T>): Promise<T> {
    if (active >= concurrency) await new Promise<void>(res => queue.push(res));
    active++;
    try { return await fn(); }
    finally { next(); }
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
   games.json loader
──────────────────────────────────────────────────────────────────────────── */
type GameMeta = {
  contractAddress: string;
  tsdbEventId?: number | string;
  date?: string; time?: string; teamA?: string; teamB?: string;
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
    const filtered = arr.filter(a => {
      try { return ethers.utils.isAddress(a); } catch { return false; }
    });
    if (filtered.length) {
      console.log(`Using CONTRACTS from env (${filtered.length})`);
      return Array.from(new Set(filtered)).map(addr => ({ contractAddress: addr }));
    }
  }

  console.warn("No contracts found in games.json or CONTRACTS env. Nothing to do.");
  return [];
}

function loadSourceCode(): string {
  for (const p of SOURCE_CANDIDATES) {
    try {
      if (fs.existsSync(p)) {
        const src = fs.readFileSync(p, "utf8");
        if (src && src.trim().length > 0) {
          console.log(`🧠 Loaded Functions source from: ${p}`);
          return src;
        }
      }
    } catch {}
  }
  throw new Error(`Could not find source.js. Tried:\n- ${SOURCE_CANDIDATES.join("\n- ")}`);
}

/* ────────────────────────────────────────────────────────────────────────────
   DON pointer helpers (activeSecrets.json)
──────────────────────────────────────────────────────────────────────────── */
type Pointer = { donId: string; secretsVersion: number; uploadedAt?: string; expiresAt?: string };

function readPointer(file = path.resolve(__dirname, "../activeSecrets.json")): Pointer | null {
  try { return JSON.parse(fs.readFileSync(file, "utf8")); } catch { return null; }
}

function pointerExpiringSoon(p?: Pointer, bufferMs = 60_000): boolean {
  if (!p?.expiresAt) return false;
  const t = Date.parse(p.expiresAt);
  return Number.isFinite(t) && Date.now() >= (t - bufferMs);
}

async function loadActiveSecretsFromGithub(): Promise<{ secretsVersion: number; donId: string }> {
  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/activeSecrets.json?ref=${GITHUB_REF}`;
  const headers: any = {
    ...(GH_PAT ? { Authorization: `Bearer ${GH_PAT}` } : {}),
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "blockpools-settlement-bot/1.0",
    Accept: "application/vnd.github+json",
  };
  const res = await fetch(url, { headers } as any);
  if (!res.ok) throw new Error(`activeSecrets.json HTTP ${res.status}`);
  const data = await res.json();
  const json = JSON.parse(Buffer.from(data.content, "base64").toString("utf8"));
  return {
    secretsVersion: Number(json.secretsVersion ?? json.version),
    donId: json.donId || "fun-ethereum-sepolia-1",
  };
}

/**
 * Ensure we have a pointer. Default strategy is **reuse**:
 *  - local activeSecrets.json (preferred; written by upload-and-settle.sh step 1)
 *  - else GitHub activeSecrets.json
 *  - else env DON_SECRETS_VERSION + DON_ID
 * If strategy=upload, try uploader and gracefully fall back to reuse on native errors.
 */
function ensureFreshPointer(): Pointer {
  const strategy = (process.env.SECRETS_STRATEGY || "reuse").toLowerCase(); // default: reuse
  const pointerPath = path.resolve(__dirname, "../activeSecrets.json");

  const reuse = (): Pointer => {
    const local = readPointer(pointerPath);
    if (local?.donId && local?.secretsVersion) {
      console.log("🔐 Loaded DON pointer from local");
      console.log(`   secretsVersion = ${local.secretsVersion}`);
      console.log(`   donId          = ${local.donId}`);
      return local;
    }
    const envVersion = process.env.DON_SECRETS_VERSION ?? process.env.SECRETS_VERSION;
    const envDonId = process.env.DON_ID;
    if (envVersion && envDonId) {
      const p = { secretsVersion: Number(envVersion), donId: String(envDonId) };
      console.log("🔐 Loaded DON pointer from env");
      console.log(`   secretsVersion = ${p.secretsVersion}`);
      console.log(`   donId          = ${p.donId}`);
      return p as any;
    }
    throw new Error("No activeSecrets.json locally and no env DON_SECRETS_VERSION/DON_ID set.");
  };

  if (strategy !== "upload") return reuse();

  // upload path (try/catch → fallback to reuse on native addon errors)
  const upload = (): Pointer => {
    console.log("[SECRETS] Uploading DON-hosted secrets...");
    let out: string;
    try {
      out = execFileSync("node", ["--enable-source-maps", "upload-secrets.js"], {
        cwd: path.resolve(__dirname, ".."),
        stdio: ["ignore", "pipe", "inherit"],
        encoding: "utf8",
      }).trim();
    } catch (e: any) {
      const msg = String(e?.message || e);
      if (/bcrypto|native|loady/i.test(msg)) {
        console.warn("[SECRETS] Native uploader path failed; reusing existing pointer instead.");
        return reuse();
      }
      throw e;
    }

    let p: Pointer;
    try { p = JSON.parse(out); }
    catch { throw new Error(`[SECRETS] Uploader did not return valid JSON. Got: ${out.slice(0, 200)}...`); }

    if (!p?.donId || !p?.secretsVersion) {
      throw new Error("[SECRETS] Uploader JSON missing donId or secretsVersion.");
    }

    const tmp = `${pointerPath}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(p, null, 2));
    fs.renameSync(tmp, pointerPath);

    console.log(`[SECRETS] Pointer refreshed donId=${p.donId} secretsVersion=${p.secretsVersion}`);
    if (p.expiresAt) console.log(`[SECRETS] ExpiresAt=${p.expiresAt}`);
    return p;
  };

  return upload();
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
  return parts.map(p => (p[0] || "").toUpperCase()).join("");
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
  if (L === "nfl")  return { sportPath: "football",     leaguePaths: ["nfl-scores"] };
  if (L === "nba")  return { sportPath: "bsktbl",       leaguePaths: ["nba-scores"] };
  if (L === "nhl")  return { sportPath: "hockey",       leaguePaths: ["nhl-scores"] };
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
  let h = 0, mi = 0;

  if (timeStr) {
    const ampm = String(timeStr).trim().toUpperCase();
    let mh = ampm.match(/^(\d{1,2}):(\d{2})\s*([AP]M)?$/);
    if (mh) {
      h = +mh[1]; mi = +mh[2];
      const mer = mh[3];
      if (mer === "PM" && h < 12) h += 12;
      if (mer === "AM" && h === 12) h = 0;
    } else {
      mh = ampm.match(/^(\d{1,2}):(\d{2})$/);
      if (mh) { h = +mh[1]; mi = +mh[2]; }
    }
  }

  const t = Date.UTC(+yyyy, +MM - 1, +dd, h, mi, 0, 0);
  return isFinite(t) ? Math.floor(t / 1000) : undefined;
}

function kickoffEpochFromRaw(raw: any): number | undefined {
  const t1 = parseDatetimeUTC(raw?.datetime_utc || raw?.["@datetime_utc"]);
  if (t1) return t1;

  const date =
    raw?.formatted_date ||
    raw?.date ||
    raw?.["@formatted_date"] ||
    raw?.["@date"];
  const time =
    raw?.time ||
    raw?.start_time ||
    raw?.start ||
    raw?.["@time"];

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
    const arrs = Object.values(payload).filter(v => Array.isArray(v)) as any[];
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

  const status = String(
    r?.status ||
    r?.game_status ||
    r?.state ||
    r?.["@status"] ||
    ""
  ).trim();

  return { homeName, awayName, homeScore, awayScore, status };
}

function teamMatchesOneSide(apiName: string, wantName: string, wantCode: string): boolean {
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
  const overlap = [...wantTokens].some(t => t.length > 2 && tokens.has(t));
  return overlap;
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

async function tryFetchGoalserve(league: string, lockTime: number) {
  const { sportPath, leaguePaths } = goalserveLeaguePaths(league);
  if (!sportPath || !leaguePaths.length) return { ok: false, reason: "unsupported league" };

  const baseISO = epochToEtISO(lockTime);
  const [Y, M, D] = baseISO.split("-");
  const ddmmyyyy = `${D}.${M}.${Y}`;
  const tried: string[] = [];

  for (const lp of leaguePaths) {
    const url =
      `${GOALSERVE_BASE_URL.replace(/\/+$/, "")}/${encodeURIComponent(GOALSERVE_API_KEY)}` +
      `/${sportPath}/${lp}?date=${encodeURIComponent(ddmmyyyy)}&json=1`;
    tried.push(url);

    try {
      const payload = await fetchJsonWithRetry(url, 3, 500);
      const rawGames = collectCandidateGames(payload);
      if (!rawGames.length) continue;

      const games = rawGames.map((r: any) => {
        const g = normalizeGameRow(r);
        return { ...g, __kickoff: kickoffEpochFromRaw(r), __raw: r };
      });

      return { ok: true, dateTried: ddmmyyyy, path: lp, games, url };
    } catch (e: any) {
      if (GOALSERVE_DEBUG) console.log(`[GOALSERVE_ERR] ${url} :: ${e?.message || e}`);
    }
  }

  return { ok: false, reason: "no games", tried };
}

async function confirmFinalGoalserve(params: {
  league: string;
  lockTime: number;
  teamAName: string;
  teamBName: string;
  teamACode?: string;
  teamBCode?: string;
}): Promise<{ ok: boolean; winner?: "A" | "B" | "TIE"; winnerCode?: string; reason?: string; debug?: any }> {
  if (!GOALSERVE_API_KEY) return { ok: false, reason: "missing GOALSERVE_API_KEY" };

  const resp = await tryFetchGoalserve(params.league, params.lockTime);
  if (!resp.ok) return { ok: false, reason: resp.reason || "no games" };

  const aName = params.teamAName;
  const bName = params.teamBName;
  const aCode = params.teamACode ?? "";
  const bCode = params.teamBCode ?? "";

  const candidates = resp.games.filter((g: any) =>
    unorderedTeamsMatchByTokens(g.homeName, g.awayName, aName, bName, aCode, bCode)
  );

  if (!candidates.length) {
    return { ok: false, reason: "no team match", debug: GOALSERVE_DEBUG ? { url: resp.url, date: resp.dateTried } : undefined };
  }

  candidates.sort((g1: any, g2: any) => {
    const t1 = g1.__kickoff ?? Number.MAX_SAFE_INTEGER;
    const t2 = g2.__kickoff ?? Number.MAX_SAFE_INTEGER;
    const d1 = Math.abs(t1 - params.lockTime);
    const d2 = Math.abs(t2 - params.lockTime);
    if (d1 !== d2) return d1 - d2;

    const f1 = isFinalStatus(g1.status || "") ? 1 : 0;
    const f2 = isFinalStatus(g2.status || "") ? 1 : 0;
    return f2 - f1;
  });

  const match = candidates[0];
  const isFinal = isFinalStatus(match.status || "");
  if (!isFinal) {
    return { ok: false, reason: "not final", debug: GOALSERVE_DEBUG ? { url: resp.url, date: resp.dateTried, status: match.status } : undefined };
  }

  const homeIsA = teamMatchesOneSide(match.homeName, aName, aCode);
  const homeIsB = teamMatchesOneSide(match.homeName, bName, bCode);

  let winner: "A" | "B" | "TIE" = "TIE";
  if (match.homeScore > match.awayScore) winner = homeIsA ? "A" : homeIsB ? "B" : "TIE";
  else if (match.awayScore > match.homeScore) winner = homeIsA ? "B" : homeIsB ? "A" : "TIE";

  let winnerCode = "Tie";
  if (winner === "A") winnerCode = params.teamACode || params.teamAName;
  else if (winner === "B") winnerCode = params.teamBCode || params.teamBName;

  return {
    ok: true,
    winner,
    winnerCode,
    debug: GOALSERVE_DEBUG ? {
      url: resp.url,
      date: resp.dateTried,
      picked: {
        home: match.homeName,
        away: match.awayName,
        status: match.status,
        homeScore: match.homeScore,
        awayScore: match.awayScore,
        kickoff: match.__kickoff,
      },
    } : undefined,
  };
}

/* ────────────────────────────────────────────────────────────────────────────
   Error decoding helpers
──────────────────────────────────────────────────────────────────────────── */
const FUNCTIONS_ROUTER_ERRORS = [
  "error EmptyArgs()",
  "error EmptySource()",
  "error InsufficientBalance()",
  "error InvalidConsumer(address consumer)",
  "error InvalidSubscription()",
  "error SubscriptionIsPaused()",
  "error OnlyRouterCanFulfill()",
  "error RequestIsAlreadyPending()",
  "error UnsupportedDON()",
];
const routerIface = new ethers.utils.Interface(FUNCTIONS_ROUTER_ERRORS);

function decodeRevert(data?: string) {
  if (!data || typeof data !== "string" || !data.startsWith("0x") || data.length < 10) return "unknown";
  try { return iface.parseError(data).name; } catch {}
  try { return routerIface.parseError(data).name; } catch {}
  try {
    if (data.slice(0, 10) === "0x08c379a0") {
      const [msg] = ethers.utils.defaultAbiCoder.decode(["string"], "0x" + data.slice(10));
      return `Error("${msg}")`;
    }
  } catch {}
  return "unknown";
}

/* ────────────────────────────────────────────────────────────────────────────
   MAIN
──────────────────────────────────────────────────────────────────────────── */
async function main() {
  console.log(`[BOT] settlement-bot starting @ ${new Date().toISOString()}`);

  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  if (!process.env.SUBSCRIPTION_ID) throw new Error("Missing SUBSCRIPTION_ID");

  console.log(`[CFG] DRY_RUN=${DRY_RUN} (env=${process.env.DRY_RUN ?? "(unset)"})`);
  console.log(`[CFG] SUBSCRIPTION_ID=${process.env.SUBSCRIPTION_ID}`);
  console.log(`[CFG] REQUIRE_FINAL_CHECK=${REQUIRE_FINAL_CHECK} POSTGAME_MIN_ELAPSED=${POSTGAME_MIN_ELAPSED}s`);
  console.log(`[CFG] Provider=Goalserve (NFL + NBA + NHL + EPL + UCL)`);

  const provider = new ethers.providers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  // === REUSE FIRST: read pointer written by step [1/3] in your shell script ===
  const pointer = ensureFreshPointer();
  const donHostedSecretsVersion = BigInt(pointer.secretsVersion);
  const donBytes = ethers.utils.formatBytes32String(pointer.donId);

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
            pool.league(),
            pool.teamAName(),
            pool.teamBName(),
            pool.teamACode(),
            pool.teamBCode(),
            pool.isLocked(),
            pool.requestSent(),
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
        } catch (e: any) {
          if (GOALSERVE_DEBUG) console.warn(`[READ FAIL] ${addr}:`, e?.message || e);
        }
      })
    )
  );

  const nowSec = Math.floor(Date.now() / 1000);

  const gated = states.filter(
    s => s.isOwner && s.isLocked && !s.requestSent && s.winningTeam === 0
  );

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
      const label = pre.winnerCode || pre.winner || "?";
      console.log(`[FINAL] ${s.league} ${s.teamAName} vs ${s.teamBName} :: winner=${label}`);
      finalEligible.push(s);
    } else if (pre.reason === "not final") {
      console.log(`[PENDING] ${s.league} ${s.teamAName} vs ${s.teamBName} :: not final yet`);
    } else if (GOALSERVE_DEBUG) {
      console.log(`[SKIP][DBG] ${s.league} ${s.teamAName} vs ${s.teamBName} :: ${pre.reason || "no match"}`);
    }
  }

  if (!finalEligible.length) {
    console.log("No games confirmed FINAL. Submitted 0 transaction(s).");
    return;
  }

  console.log(`✅ Provider confirmed FINAL for ${finalEligible.length} pool(s). Proceeding.`);

  // Refresh pointer JUST by reuse (local→GitHub), no internal upload
  let pointer2: Pointer | null = readPointer(path.resolve(__dirname, "../activeSecrets.json"));
  if (!pointer2) {
    try {
      const gh = await loadActiveSecretsFromGithub();
      pointer2 = { donId: gh.donId, secretsVersion: gh.secretsVersion };
    } catch {
      pointer2 = pointer; // fall back to initial pointer
    }
  }
  const donHostedSecretsVersion2 = BigInt(pointer2.secretsVersion);
  const donBytes2 = ethers.utils.formatBytes32String(pointer2.donId);

  const buildArgs8 = (s: PoolState): string[] => {
    const d0 = epochToEtISO(s.lockTime);
    const d1 = addDaysISO(d0, 1);
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

    try {
      await pool.callStatic.sendRequest(
        SOURCE,
        args,
        SUBSCRIPTION_ID,
        FUNCTIONS_GAS_LIMIT,
        DON_SECRETS_SLOT,
        donHostedSecretsVersion2,
        donBytes2
      );
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      const decoded = decodeRevert(data);
      console.error(`[SIM ERR] ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName}) => ${decoded}`);
      continue;
    }

    if (!DRY_RUN) {
      await limiter(TX_SEND_CONCURRENCY)(async () => {
        try {
          if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
          const tx = await pool.sendRequest(
            SOURCE,
            args,
            SUBSCRIPTION_ID,
            FUNCTIONS_GAS_LIMIT,
            DON_SECRETS_SLOT,
            donHostedSecretsVersion2,
            donBytes2
          );
          console.log(`[TX] sendRequest ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName}) :: ${tx.hash}`);
          submitted++;
        } catch (e: any) {
          const data = e?.data ?? e?.error?.data;
          console.error(`[ERR] sendRequest ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName}):`, e?.reason || e?.message || e);
          if (data?.slice) console.error(` selector = ${data.slice(0, 10)} (${decodeRevert(data)})`);
        }
      });
    } else {
      console.log(`[DRY_RUN] Would sendRequest ${s.addr} (${s.league} ${s.teamAName} vs ${s.teamBName})`);
    }
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
