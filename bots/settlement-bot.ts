// @ts-nocheck
try { require("dotenv").config(); } catch {}

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ethers } from "ethers";
import { gamePoolAbi as IMPORTED_GAMEPOOL_ABI } from "./gamepool.abi";

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
const SUBSCRIPTION_ID = BigInt(process.env.SUBSCRIPTION_ID!);                  // uint64
const FUNCTIONS_GAS_LIMIT = Number(process.env.FUNCTIONS_GAS_LIMIT || 300000); // uint32
const DON_SECRETS_SLOT = Number(process.env.DON_SECRETS_SLOT || 0);            // uint8

const DRY_RUN = process.env.DRY_RUN === "1";

/** Always require provider-final check. */
const REQUIRE_FINAL_CHECK = true;

/** Timing gates */
const POSTGAME_MIN_ELAPSED = Number(process.env.POSTGAME_MIN_ELAPSED || 600);   // seconds after lock before checking
const REQUEST_GAP_SECONDS  = Number(process.env.REQUEST_GAP_SECONDS  || 120);   // short cooldown after lock

/** Strict match window for provider event vs lockTime */
const MAX_EVENT_DRIFT_SECS = Number(process.env.MAX_EVENT_DRIFT_SECS || (2 * 3600)); // +/- 2h
const REQUIRE_SAME_DAY = true;

/** Concurrency + rate limiting */
const READ_CONCURRENCY    = Number(process.env.READ_CONCURRENCY    || 25);
const TX_SIM_CONCURRENCY  = Number(process.env.TX_SIM_CONCURRENCY  || 10);
const TX_SEND_CONCURRENCY = Number(process.env.TX_SEND_CONCURRENCY || 3);

const MAX_TX_PER_RUN   = Number(process.env.MAX_TX_PER_RUN   || 8);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 0);

/** Goalserve secrets */
const GOALSERVE_API_KEY  = process.env.GOALSERVE_API_KEY || "";
const GOALSERVE_BASE_URL = process.env.GOALSERVE_BASE_URL || "https://www.goalserve.com/getfeed";

/** DON pointer (activeSecrets.json) lookup */
const GITHUB_OWNER = process.env.GITHUB_OWNER || "harrisonschultz7";
const GITHUB_REPO  = process.env.GITHUB_REPO  || "blockpools-backend";
const GITHUB_REF   = process.env.GITHUB_REF   || "main";
const GH_PAT       = process.env.GH_PAT;

/** games.json discovery */
const GAMES_PATH_OVERRIDE = process.env.GAMES_PATH || "";
const GAMES_CANDIDATES = [
  path.resolve(__dirname, "..", "src", "data", "games.json"),
  path.resolve(__dirname, "..", "games.json"),
  path.resolve(__dirname, "..", "..", "frontend-src", "src", "data", "games.json"),
  path.resolve(process.cwd(), "src", "data", "games.json"),
  path.resolve(process.cwd(), "games.json"),
];

/** Functions source discovery */
const SOURCE_CANDIDATES = [
  "C:\\Users\\harri\\OneDrive\\functions-betting-app\\functions-hardhat-starter-kit\\blockpools-backend\\bots\\source.js",
  path.resolve(__dirname, "source.js"),
  path.resolve(__dirname, "..", "bots", "source.js"),
  path.resolve(process.cwd(), "bots", "source.js"),
];

/* ──────────────────────────────────────────────────────────────────────────────
   ABI loader
────────────────────────────────────────────────────────────────────────────── */
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

/* ──────────────────────────────────────────────────────────────────────────────
   Utils
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

/* ──────────────────────────────────────────────────────────────────────────────
   games.json loader (robust to multiple shapes)
────────────────────────────────────────────────────────────────────────────── */
type GameMeta = {
  contractAddress: string;
  tsdbEventId?: number | string; // ignored (legacy)
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
  if (GAMES_PATH_OVERRIDE) {
    const fromOverride = readGamesMetaAtPath(GAMES_PATH_OVERRIDE);
    if (fromOverride) return fromOverride;
    console.warn(`GAMES_PATH was set but not readable/usable: ${GAMES_PATH_OVERRIDE}`);
  }
  for (const p of GAMES_CANDIDATES) {
    const fromLocal = readGamesMetaAtPath(p); if (fromLocal) return fromLocal;
  }
  const envList = (process.env.CONTRACTS || "").trim();
  if (envList) {
    const arr = envList.split(/[,\s]+/).filter(Boolean);
    const filtered = arr.filter((a) => { try { return ethers.utils.isAddress(a); } catch { return false; }});
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
        if (src && src.trim().length > 0) { console.log(`🧠 Loaded Functions source from: ${p}`); return src; }
      }
    } catch {}
  }
  throw new Error(`Could not find source.js. Tried:\n- ${SOURCE_CANDIDATES.join("\n- ")}`);
}

/* ──────────────────────────────────────────────────────────────────────────────
   DON pointer (activeSecrets.json)
────────────────────────────────────────────────────────────────────────────── */
async function loadActiveSecrets(): Promise<{ secretsVersion: number; donId: string; source: string }> {
  const envVersion = process.env.DON_SECRETS_VERSION ?? process.env.SECRETS_VERSION;
  const envDonId = process.env.DON_ID;
  if (envVersion && envDonId) return { secretsVersion: Number(envVersion), donId: envDonId, source: "env" };

  const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/activeSecrets.json?ref=${GITHUB_REF}`;
  const headers: any = {
    ...(GH_PAT ? { Authorization: `Bearer ${GH_PAT}` } : {}),
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "blockpools-settlement-bot/1.0",
    Accept: "application/vnd.github+json",
  };
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`activeSecrets.json HTTP ${res.status}`);
  const data = await res.json();
  const json = JSON.parse(Buffer.from(data.content, "base64").toString("utf8"));
  return { secretsVersion: Number(json.secretsVersion ?? json.version), donId: json.donId || "fun-ethereum-sepolia-1", source: "github" };
}

/* ──────────────────────────────────────────────────────────────────────────────
   Goalserve adapter (finality + matching, no team maps)
────────────────────────────────────────────────────────────────────────────── */
// Normalize for equality (legacy)
const norm = (s: string) =>
  (s || "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "").replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ").trim().toLowerCase();

// Token sets for robust matching ("new york giants" == "new york giants", codes ignored)
function normTokens(s?: string): string[] {
  return String(s ?? "")
    .normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "").replace(/[^a-z0-9 ]/gi, " ")
    .toLowerCase().split(/\s+/).filter(Boolean);
}
function sameTokenSet(a: Set<string>, b: Set<string>) {
  return a.size === b.size && [...a].every(t => b.has(t));
}
function unorderedTeamsMatchByTokens(providerHome: string, providerAway: string, aName?: string, bName?: string, aCode?: string, bCode?: string) {
  // Prefer full names; fallback to codes if names are empty
  const A = new Set(normTokens(aName || aCode));
  const B = new Set(normTokens(bName || bCode));
  const PH = new Set(normTokens(providerHome));
  const PA = new Set(normTokens(providerAway));
  if (!A.size || !B.size || !PH.size || !PA.size) return false;
  return (sameTokenSet(PH, A) && sameTokenSet(PA, B)) || (sameTokenSet(PH, B) && sameTokenSet(PA, A));
}

// lockTime -> "dd.MM.yyyy" (UTC)
function toGoalserveDate(epochSec?: number) {
  const ms = (epochSec ?? Math.floor(Date.now()/1000)) * 1000;
  const d = new Date(ms);
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const yyyy = d.getUTCFullYear();
  return `${dd}.${mm}.${yyyy}`;
}

// Minimal league->path (only NFL needed now; extend if you add more)
function goalservePaths(leagueLabel: string) {
  const L = String(leagueLabel || "").toLowerCase();
  if (L === "nfl") return { sportPath: "football", leaguePath: "nfl-scores" };
  // default to NFL so we don't break unexpectedly
  return { sportPath: "football", leaguePath: "nfl-scores" };
}

async function fetchGoalserveDay(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Goalserve HTTP ${res.status}`);
  return res.json(); // requires &json=1 in URL
}

function extractGames(payload: any): any[] {
  if (!payload) return [];
  if (Array.isArray(payload)) return payload;
  if (Array.isArray(payload.games?.game)) return payload.games.game;
  if (Array.isArray(payload.game)) return payload.game;
  if (Array.isArray(payload.events)) return payload.events;
  if (typeof payload === "object") {
    const vals = Object.values(payload);
    const arrs = vals.filter(v => Array.isArray(v)).flat();
    if (arrs.length) return arrs;
    return vals.filter(v => v && typeof v === "object");
  }
  return [];
}

function gsHomeName(e: any): string {
  return String(e?.hometeam?.name ?? e?.home_name ?? e?.home ?? "").trim();
}
function gsAwayName(e: any): string {
  return String(e?.awayteam?.name ?? e?.away_name ?? e?.away ?? "").trim();
}
function gsIsFinal(e: any): boolean {
  const s = String(e?.status ?? "").toLowerCase().trim();
  return s === "final" || /finished|full\s*time|ended|complete/.test(s);
}
function gsEventTs(e: any): number {
  // Prefer datetime_utc: "26.10.2025 17:00"
  const dt = String(e?.datetime_utc ?? "").trim();
  if (dt) {
    const [d, t] = dt.split(" ");
    const [day, mon, year] = (d || "").split(".").map(Number);
    if (day && mon && year) {
      const iso = `${year}-${String(mon).padStart(2,"0")}-${String(day).padStart(2,"0")}T${(t || "00:00")}:00Z`;
      const ms = Date.parse(iso);
      if (!Number.isNaN(ms)) return (ms / 1000) | 0;
    }
  }
  // Fallback: date: "26.10.2025"
  const d = String(e?.date ?? e?.formatted_date ?? "").trim();
  if (d) {
    const [day, mon, year] = d.split(".").map(Number);
    if (day && mon && year) {
      const ms = Date.parse(`${year}-${String(mon).padStart(2,"0")}-${String(day).padStart(2,"0")}T00:00:00Z`);
      if (!Number.isNaN(ms)) return (ms / 1000) | 0;
    }
  }
  return 0;
}
function withinStrictWindow(eventTs: number, lockTime: number) {
  if (!eventTs || !lockTime) return false;
  if (REQUIRE_SAME_DAY) {
    const a = new Date(eventTs * 1000).toISOString().slice(0,10);
    const b = new Date(lockTime * 1000).toISOString().slice(0,10);
    if (a !== b) return false;
  }
  return Math.abs(eventTs - lockTime) <= MAX_EVENT_DRIFT_SECS;
}

async function confirmFinalGoalserve(params: {
  league: string;
  lockTime: number;
  teamAName: string; teamBName: string;
  teamACode?: string; teamBCode?: string;
}): Promise<{ ok: boolean; reason?: string; winner?: "A" | "B" | "TIE" }> {
  if (!GOALSERVE_API_KEY) return { ok: false, reason: "missing GOALSERVE_API_KEY" };

  const { sportPath, leaguePath } = goalservePaths(params.league);
  const date = toGoalserveDate(params.lockTime);
  const url = `${GOALSERVE_BASE_URL}/${encodeURIComponent(GOALSERVE_API_KEY)}/${sportPath}/${leaguePath}?date=${encodeURIComponent(date)}&json=1`;

  let payload: any;
  try {
    payload = await fetchGoalserveDay(url);
  } catch (e: any) {
    return { ok: false, reason: `fetch fail: ${e?.message || e}` };
  }

  const events = extractGames(payload);
  if (!events.length) return { ok: false, reason: "no games" };

  // find a final, in-window, team-matched event
  for (const ev of events) {
    const home = gsHomeName(ev);
    const away = gsAwayName(ev);
    if (!home || !away) continue;

    // order-agnostic token match on FULL NAMES
    if (!unorderedTeamsMatchByTokens(home, away, params.teamAName, params.teamBName, params.teamACode, params.teamBCode)) {
      continue;
    }

    if (!gsIsFinal(ev)) continue;

    const eTs = gsEventTs(ev);
    if (!withinStrictWindow(eTs, params.lockTime)) continue;

    // Compute winner (for log only)
    const hs = Number(ev?.hometeam?.totalscore ?? ev?.home_score ?? 0);
    const as = Number(ev?.awayteam?.totalscore ?? ev?.away_score ?? 0);
    let winner: "A" | "B" | "TIE" = "TIE";
    if (hs > as) {
      winner = norm(home) === norm(params.teamAName) ? "A" : "B";
    } else if (as > hs) {
      winner = norm(home) === norm(params.teamAName) ? "B" : "A";
    }
    return { ok: true, winner };
  }

  return { ok: false, reason: "no team match / not final / out of window" };
}

/* ──────────────────────────────────────────────────────────────────────────────
   Error decoding
────────────────────────────────────────────────────────────────────────────── */
const FUNCTIONS_ROUTER_ERRORS = [
  "error EmptyArgs()", "error EmptySource()", "error InsufficientBalance()",
  "error InvalidConsumer(address consumer)", "error InvalidSubscription()",
  "error SubscriptionIsPaused()", "error OnlyRouterCanFulfill()",
  "error RequestIsAlreadyPending()", "error UnsupportedDON()"
];
const routerIface = new ethers.utils.Interface(FUNCTIONS_ROUTER_ERRORS);
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
  if (!gamesMeta.length) return;

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

  // Pools we own, are locked, not requested, no winner yet
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

  // STEP 2: Provider finality checks — Goalserve (no team maps)
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
      console.log(`[OK] FINAL via Goalserve: ${s.league} ${s.teamAName} vs ${s.teamBName} (winner≈${pre.winner || "?"})`);
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

  /* STEP 3: Simulate & send — always 8 args (new source.js) */
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

    // simulate
    try {
      await pool.callStatic.sendRequest(
        SOURCE,
        args,
        SUBSCRIPTION_ID,
        FUNCTIONS_GAS_LIMIT,
        DON_SECRETS_SLOT,
        donHostedSecretsVersion2,
        donBytes
      );
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      console.error(`[SIM ERR] ${s.addr} => ${decodeRevert(data)}`);
      continue;
    }

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
            donBytes
          );
          console.log(`[OK] sendRequest ${s.addr} (args8): ${tx.hash}`);
          submitted++;
        } catch (e: any) {
          const data = e?.data ?? e?.error?.data;
          let decoded = "unknown"; try { decoded = iface.parseError(data).name; } catch {}
          console.error(`[ERR] sendRequest ${s.addr}:`, e?.reason || e?.message || e);
          if (data) console.error(` selector = ${data.slice?.(0,10)} (${decoded})`);
        }
      });
    } else {
      console.log(`[DRY_RUN] Would sendRequest ${s.addr} using args8`);
    }
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => { console.error(e); process.exit(1); });
