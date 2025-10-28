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

const REQUIRE_FINAL_CHECK = process.env.REQUIRE_FINAL_CHECK !== "0";
const POSTGAME_MIN_ELAPSED = Number(process.env.POSTGAME_MIN_ELAPSED || 600);  // sec after lock
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);    // guard after lock

// Concurrency controls
const READ_CONCURRENCY   = Number(process.env.READ_CONCURRENCY   || 25);
const TSDB_CONCURRENCY   = Number(process.env.TSDB_CONCURRENCY   || 8);
const TX_SIM_CONCURRENCY = Number(process.env.TX_SIM_CONCURRENCY || 10);
const TX_SEND_CONCURRENCY= Number(process.env.TX_SEND_CONCURRENCY|| 3);

const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 0);

// TheSportsDB keys / flags
const THESPORTSDB_API_KEY = process.env.THESPORTSDB_API_KEY || "";
const ALLOW_V1_FALLBACK = process.env.ALLOW_V1_FALLBACK === "1"; // finality check only

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

/* day-window helper: match ET day, previous, or next */
function matchesEtDayOrNeighbor(e: any, gameDateEt: string) {
  const d  = e?.dateEvent || "";
  const dl = e?.dateEventLocal || "";
  const prev = addDaysISO(gameDateEt, -1);
  const next = addDaysISO(gameDateEt,  1);
  return (
    d === gameDateEt || dl === gameDateEt ||
    d === prev      || dl === prev      ||
    d === next      || dl === next
  );
}

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
   v2 + optional v1 fetchers, matching, finality
────────────────────────────────────────────────────────────────────────────── */
function mapLeagueId(leagueOnChain: string): string {
  const lk = String(leagueOnChain || "").toLowerCase();
  const MAP: Record<string, string> = {
    mlb: "4424", nfl: "4391", nba: "4387", nhl: "4380",
    epl: "4328", ucl: "4480",
  };
  return MAP[lk] || "";
}

const V2_BASE = "https://www.thesportsdb.com/api/v2/json";

// send both header variants + UA + Accept, and retry if empty
const v2HeaderVariants: Array<Record<string, string>> = [
  { "X-API-KEY": THESPORTSDB_API_KEY, "Accept": "application/json", "User-Agent": "blockpools-settlement-bot/1.0" },
  { "X_API_KEY": THESPORTSDB_API_KEY, "Accept": "application/json", "User-Agent": "blockpools-settlement-bot/1.0" },
  { "X-API-KEY": THESPORTSDB_API_KEY, "X_API_KEY": THESPORTSDB_API_KEY, "Accept": "application/json", "User-Agent": "blockpools-settlement-bot/1.0" },
];

/* ───── Helpers to tolerate schedule/events/results array keys ───── */
function firstArrayByKeys(j: any, keys: string[]): any[] {
  if (!j || typeof j !== "object") return [];
  for (const k of keys) {
    const v = j?.[k];
    if (Array.isArray(v)) return v;
  }
  // last-resort: return the first array found at any top-level key
  for (const v of Object.values(j)) if (Array.isArray(v)) return v;
  return [];
}

async function v2Fetch(path: string) {
  const url = `${V2_BASE}${path}`;
  for (const headers of v2HeaderVariants) {
    try {
      const r = await fetch(url, { headers });
      const txt = await r.text();
      if (!r.ok) {
        console.warn(`[v2Fetch] ${r.status} ${r.statusText} ${path} :: ${txt.slice(0,160)}`);
        continue;
      }
      try {
        const j = txt ? JSON.parse(txt) : null;
        if (j && (Object.keys(j).length > 0)) {
          const keys = Object.keys(j);
          console.log(`[v2Fetch] ok ${path} keys=${keys.join(",")}`);
          return j;
        }
        console.warn(`[v2Fetch] empty JSON for ${path} with headers variant`);
      } catch (e) {
        console.warn(`[v2Fetch] JSON parse error for ${path}:`, (e as Error).message);
      }
    } catch (e) {
      console.warn(`[v2Fetch] request error ${path}:`, (e as Error).message);
    }
  }
  return null;
}

async function v2PreviousLeagueEvents(idLeague: string) {
  if (!idLeague) return [];
  const j = await v2Fetch(`/schedule/previous/league/${idLeague}`);
  return firstArrayByKeys(j, ["schedule", "events"]);
}
async function v2LookupEvent(idEvent: string | number) {
  const j = await v2Fetch(`/lookup/event/${encodeURIComponent(String(idEvent))}`);
  const arr = firstArrayByKeys(j, ["events", "schedule", "results"]);
  return arr.length ? arr[0] : null;
}
async function v2LookupEventResults(idEvent: string | number) {
  const j = await v2Fetch(`/lookup/event_results/${encodeURIComponent(String(idEvent))}`);
  const arr = firstArrayByKeys(j, ["results", "events", "schedule"]);
  return arr.length ? arr[0] : null;
}
async function v2PreviousTeamEvents(idTeam: string) {
  if (!idTeam) return [];
  const j = await v2Fetch(`/schedule/previous/team/${encodeURIComponent(idTeam)}`);
  return firstArrayByKeys(j, ["schedule", "events"]);
}
async function v2ListSeasons(idLeague: string): Promise<string[]> {
  if (!idLeague) return [];
  const j = await v2Fetch(`/list/seasons/${idLeague}`);
  const arr = firstArrayByKeys(j, ["seasons"]);
  return arr.map((s: any) => String(s?.strSeason || s)).filter(Boolean);
}
async function v2ScheduleLeagueSeason(idLeague: string, season: string) {
  if (!idLeague || !season) return [];
  const j = await v2Fetch(`/schedule/league/${idLeague}/${encodeURIComponent(season)}`);
  return firstArrayByKeys(j, ["schedule", "events"]);
}

/* Optional v1 day-slice fallback for FINALITY ONLY */
const V1_BASE = "https://www.thesportsdb.com/api/v1/json";
async function v1EventsDay(dateISO: string, leagueLabel?: string) {
  const key = THESPORTSDB_API_KEY;
  const urls = leagueLabel
    ? [
        `${V1_BASE}/${key}/eventsday.php?d=${encodeURIComponent(dateISO)}&l=${encodeURIComponent(leagueLabel)}`,
        `${V1_BASE}/${key}/eventsday.php?d=${encodeURIComponent(dateISO)}`
      ]
    : [`${V1_BASE}/${key}/eventsday.php?d=${encodeURIComponent(dateISO)}`];
  for (const u of urls) {
    try {
      const r = await fetch(u, { headers: { "Accept": "application/json", "User-Agent": "blockpools-settlement-bot/1.0" } });
      if (!r.ok) continue;
      const j = await r.json().catch(() => null);
      const arr = firstArrayByKeys(j, ["events", "schedule", "results"]);
      if (arr.length) return arr;
    } catch {}
  }
  return [];
}

/* Matching helpers */
const teamIdCache = new Map<string, Map<string, string>>(); // idLeague -> (normName -> idTeam)
async function ensureLeagueTeamsCached(idLeague: string) {
  if (!idLeague || teamIdCache.has(idLeague)) return;
  const j = await v2Fetch(`/list/teams/${idLeague}`);
  const map = new Map<string, string>();
  const norm = (s: string) =>
    (s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "")
      .replace(/[’'`]/g, "").replace(/[^a-z0-9 ]/gi, " ")
      .replace(/\s+/g, " ").trim().toLowerCase();
  for (const t of (firstArrayByKeys(j, ["teams"]) as any[])) {
    const n = norm(t?.strTeam || "");
    if (n && t?.idTeam) map.set(n, String(t.idTeam));
  }
  teamIdCache.set(idLeague, map);
}
function findIdTeam(idLeague: string, teamName: string): string | "" {
  const m = teamIdCache.get(idLeague); if (!m) return "";
  const norm = (s: string) =>
    (s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "")
      .replace(/[’'`]/g, "").replace(/[^a-z0-9 ]/gi, " ")
      .replace(/\s+/g, " ").trim().toLowerCase();
  const key = norm(teamName);
  for (const [k, v] of m) if (k === key || k.includes(key) || key.includes(k)) return v;
  return "";
}
function tsFromEvent(e: any): number {
  if (e?.strTimestamp) { const ms = Date.parse(e.strTimestamp); if (!Number.isNaN(ms)) return (ms / 1000) | 0; }
  if (e?.dateEvent && e?.strTime) {
    const s = /Z$/.test(e.strTime) ? `${e.dateEvent}T${e.strTime}` : `${e.dateEvent}T${e.strTime}Z`;
    const ms = Date.parse(s); if (!Number.isNaN(ms)) return (ms / 1000) | 0;
  }
  if (e?.dateEvent) { const ms = Date.parse(`${e.dateEvent}T00:00:00Z`); if (!Number.isNaN(ms)) return (ms / 1000) | 0; }
  return 0;
}
function normTeam(s: string) {
  return (s || "").normalize("NFD").replace(/[\u0300-\u036f]/g, "")
    .replace(/[’'`]/g, "").replace(/[^a-z0-9 ]/gi, " ")
    .replace(/\s+/g, " ").trim().toLowerCase();
}
function sameTeam(x?: string, y?: string) {
  const nx = normTeam(String(x || "")), ny = normTeam(String(y || "")); if (!nx || !ny) return false;
  return nx === ny || nx.includes(ny) || ny.includes(nx);
}

/* ID-aware + tolerant nearest-kickoff */
function pickClosestByKickoff(
  events: any[],
  aName: string,
  bName: string,
  kickoff: number,
  aId?: string,
  bId?: string
) {
  const TOL = 45 * 60; // 45-minute grace for listed kickoff vs lock
  const cand = events.filter(e => {
    if (aId && bId && e?.idHomeTeam && e?.idAwayTeam) {
      const h = String(e.idHomeTeam), w = String(e.idAwayTeam);
      if ((h === aId && w === bId) || (h === bId && w === aId)) return true;
    }
    const h = e?.strHomeTeam, w = e?.strAwayTeam;
    return (sameTeam(h, aName) && sameTeam(w, bName)) || (sameTeam(h, bName) && sameTeam(w, aName));
  });

  const ts = (e: any) => tsFromEvent(e) || kickoff;
  cand.sort((x, y) => {
    const dx = Math.max(0, Math.abs(ts(x) - kickoff) - TOL);
    const dy = Math.max(0, Math.abs(ts(y) - kickoff) - TOL);
    return dx - dy;
  });

  return cand[0] || null;
}

function looksFinal(ev: any) {
  const status = String(ev?.strStatus ?? ev?.strProgress ?? "").toLowerCase();
  const hasScores = (ev?.intHomeScore != null && ev?.intAwayScore != null);
  if (/^(ft|aot|aet|pen|finished|full time)$/.test(status)) return true;
  if (/final|finished|ended|complete/.test(status)) return true;
  return hasScores && !status;
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
  console.log(`[CFG] ALLOW_V1_FALLBACK=${ALLOW_V1_FALLBACK}`);

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

  const metaByAddr = new Map<string, { tsdbEventId?: number | string }>();
  for (const g of gamesMeta) {
    if (g?.contractAddress) metaByAddr.set(g.contractAddress.toLowerCase(), { tsdbEventId: g.tsdbEventId });
  }

  const readLimit = limiter(READ_CONCURRENCY);
  const simLimit  = limiter(TX_SIM_CONCURRENCY);
  const sendLimit = limiter(TX_SEND_CONCURRENCY);
  const botAddr = (await wallet.getAddress()).toLowerCase();

  type PoolState = {
    addr: string; league: string;
    teamAName: string; teamBName: string;
    teamACode: string; teamBCode: string;
    isLocked: boolean; requestSent: boolean; winningTeam: number;
    lockTime: number; isOwner: boolean;
    tsdbEventId?: number | string;
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
            tsdbEventId: metaByAddr.get(addr.toLowerCase())?.tsdbEventId
          });
        } catch (e) {
          console.warn(`[READ FAIL] ${addr}:`, (e as Error).message);
        }
      })
    )
  );

  const nowSec = Math.floor(Date.now() / 1000);
  const gated = states.filter(s => s.isOwner && s.isLocked && !s.requestSent && s.winningTeam === 0);
  const timeGated = gated.filter(s =>
    (s.lockTime === 0 || nowSec >= s.lockTime + REQUEST_GAP_SECONDS) &&
    (s.lockTime === 0 || !REQUIRE_FINAL_CHECK || nowSec >= s.lockTime + POSTGAME_MIN_ELAPSED)
  );
  if (!timeGated.length) { console.log("No eligible pools after gates. Submitted 0 transaction(s)."); return; }

  /* STEP 2: Provider finality checks (v2-first, v1 optional fallback) */
  let finalEligible: PoolState[] = timeGated;
  if (REQUIRE_FINAL_CHECK) {
    if (!THESPORTSDB_API_KEY) { console.log("REQUIRE_FINAL_CHECK=1 but no THESPORTSDB_API_KEY set. Skipping to avoid wasted LINK."); return; }

    // Preload previous-events per league
    const leagues = new Set<string>();
    for (const s of timeGated) leagues.add(mapLeagueId(s.league));

    const prevCache = new Map<string, any[]>();
    await Promise.all(
      Array.from(leagues).map(id =>
        limiter(TSDB_CONCURRENCY)(async () => {
          const arr = await v2PreviousLeagueEvents(id);
          prevCache.set(id, arr);
        })
      )
    );

    const eligible: PoolState[] = [];
    for (const s of timeGated) {
      const idLeague = mapLeagueId(s.league);
      let ev: any | null = null;
      let mark = "none";

      // Try to resolve an event ID fast-path
      const dynId = await resolveEventIdIfMissing(s);
      if (dynId) {
        const byId = (await v2LookupEventResults(dynId)) || (await v2LookupEvent(dynId));
        if (byId) { ev = byId; mark = "id_lookup"; }
      }

      // previous/league (cached)
      if (!ev) {
        const prev = prevCache.get(idLeague) || [];
        ev = pickClosestByKickoff(prev, s.teamAName, s.teamBName, s.lockTime);
        mark = ev ? "prev_league_match" : "no_prev_match";
      }

      // previous/team
      if (!ev) {
        await ensureLeagueTeamsCached(idLeague);
        const idTeamA = findIdTeam(idLeague, s.teamAName);
        const idTeamB = findIdTeam(idLeague, s.teamBName);
        const pools: any[] = [];
        if (idTeamA) pools.push(v2PreviousTeamEvents(idTeamA));
        if (idTeamB) pools.push(v2PreviousTeamEvents(idTeamB));
        if (pools.length) {
          const results = (await Promise.all(pools)).flat();
          ev = pickClosestByKickoff(results, s.teamAName, s.teamBName, s.lockTime, idTeamA, idTeamB);
          mark = ev ? "prev_team_match" : "no_team_match";
        }
      }

      // season fallback with ET±1d window
      if (!ev) {
        const gameDate = epochToEtISO(s.lockTime);
        const seasons = await v2ListSeasons(idLeague);
        for (const ssn of seasons.slice(-2).reverse()) {
          const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
          if (!seasonEvents?.length) continue;
          const daySlice = seasonEvents.filter((e: any) => matchesEtDayOrNeighbor(e, gameDate));
          const pool = daySlice.length ? daySlice : seasonEvents;
          await ensureLeagueTeamsCached(idLeague);
          const idTeamA = findIdTeam(idLeague, s.teamAName);
          const idTeamB = findIdTeam(idLeague, s.teamBName);
          const candidate = pickClosestByKickoff(pool, s.teamAName, s.teamBName, s.lockTime, idTeamA, idTeamB);
          if (candidate) { ev = candidate; mark = daySlice.length ? "season_day_match" : "season_closest"; break; }
        }
      }

      // OPTIONAL v1 DAY FALLBACK (finality only)
      if (!ev && ALLOW_V1_FALLBACK) {
        const gameDate = epochToEtISO(s.lockTime);
        const v1 = await v1EventsDay(gameDate, s.league);
        if (v1?.length) {
          ev = pickClosestByKickoff(v1, s.teamAName, s.teamBName, s.lockTime);
          mark = ev ? "v1_day_match" : "v1_no_match";
        }
      }

      if (ev && looksFinal(ev)) {
        console.log(`[OK] Final via ${mark}: ${s.league} ${s.teamAName} vs ${s.teamBName} (idEvent=${ev?.idEvent || "?"})`);
        eligible.push(s);
      } else {
        console.log(`[SKIP] Not final by provider (${s.league} ${s.teamAName} vs ${s.teamBName}) via ${mark} status="${String(ev?.strStatus||ev?.strProgress||"")}"`);
      }
    }

    finalEligible = eligible;
    if (!finalEligible.length) { console.log("No games confirmed final by provider. Submitted 0 transaction(s)."); return; }
    console.log(`✅ Provider final for ${finalEligible.length} pool(s).`);
  }

  /* STEP 3: Simulate & send — adaptive 9 ⇄ 8 args */
  const { secretsVersion: sv } = await loadActiveSecrets();
  const donHostedSecretsVersion2 = BigInt(sv);

  async function resolveEventIdIfMissing(s: PoolState): Promise<string | ""> {
    if (s.tsdbEventId != null && s.tsdbEventId !== "") return String(s.tsdbEventId);

    const idLeague = mapLeagueId(s.league);
    if (!idLeague) return "";

    // previous/league
    const prev = await v2PreviousLeagueEvents(idLeague);
    const byPrev = pickClosestByKickoff(prev, s.teamAName, s.teamBName, s.lockTime);
    if (byPrev?.idEvent) return String(byPrev.idEvent);

    // previous/team
    await ensureLeagueTeamsCached(idLeague);
    const idTeamA = findIdTeam(idLeague, s.teamAName);
    const idTeamB = findIdTeam(idLeague, s.teamBName);
    const pools: any[] = [];
    if (idTeamA) pools.push(v2PreviousTeamEvents(idTeamA));
    if (idTeamB) pools.push(v2PreviousTeamEvents(idTeamB));
    if (pools.length) {
      const results = (await Promise.all(pools)).flat();
      const byTeamPrev = pickClosestByKickoff(results, s.teamAName, s.teamBName, s.lockTime, idTeamA, idTeamB);
      if (byTeamPrev?.idEvent) return String(byTeamPrev.idEvent);
    }

    // season with ET±1d window
    const gameDate = epochToEtISO(s.lockTime);
    const seasons = await v2ListSeasons(idLeague);
    for (const ssn of seasons.slice(-2).reverse()) {
      const seasonEvents = await v2ScheduleLeagueSeason(idLeague, ssn);
      if (!seasonEvents?.length) continue;
      const daySlice = seasonEvents.filter((e: any) => matchesEtDayOrNeighbor(e, gameDate));
      const pool = daySlice.length ? daySlice : seasonEvents;
      const candidate = pickClosestByKickoff(pool, s.teamAName, s.teamBName, s.lockTime, idTeamA, idTeamB);
      if (candidate?.idEvent) return String(candidate.idEvent);
    }
    return "";
  }

  const buildArgs9 = (s: PoolState, idOverride?: string): string[] => {
    const d0 = epochToEtISO(s.lockTime), d1 = addDaysISO(d0, 1);
    const leagueArg = s.league; // keep human label; source.js normalizes internally
    const id = (idOverride ?? String(s.tsdbEventId ?? "")).trim();
    return [leagueArg, d0, d1, s.teamACode.toUpperCase(), s.teamBCode.toUpperCase(), s.teamAName, s.teamBName, String(s.lockTime), id];
  };
  const buildArgs8 = (s: PoolState): string[] => {
    const d0 = epochToEtISO(s.lockTime), d1 = addDaysISO(d0, 1);
    const leagueArg = s.league;
    return [leagueArg, d0, d1, s.teamACode.toUpperCase(), s.teamBCode.toUpperCase(), s.teamAName, s.teamBName, String(s.lockTime)];
  };

  async function simulate(pool: any, args: string[]) {
    try {
      await pool.callStatic.sendRequest(SOURCE, args, SUBSCRIPTION_ID, FUNCTIONS_GAS_LIMIT, DON_SECRETS_SLOT, donHostedSecretsVersion2, donBytes);
      return { ok: true as const, err: null as null | string };
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data; const decoded = decodeRevert(data);
      return { ok: false as const, err: decoded || "unknown" };
    }
  }

  let submitted = 0;

  for (const s of finalEligible) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(s.addr, poolAbi, wallet);

    const dynamicId = await resolveEventIdIfMissing(s);
    if (dynamicId) console.log(`[INFO] Resolved idEvent=${dynamicId} for ${s.teamAName} vs ${s.teamBName}`);

    const hasId = Boolean(dynamicId || (s.tsdbEventId != null && s.tsdbEventId !== ""));

    const candidates: Array<{ label: "args9" | "args8"; args: string[] }> = hasId
      ? [{ label: "args9", args: buildArgs9(s, dynamicId) }, { label: "args8", args: buildArgs8(s) }]
      : [{ label: "args8", args: buildArgs8(s) }, { label: "args9", args: buildArgs9(s, dynamicId) }];

    let choice: { label: "args9" | "args8"; args: string[] } | null = null;

    for (const c of candidates) {
      const sim = await simLimit(() => simulate(pool, c.args));
      if (sim.ok) { choice = c; break; }
      else console.error(`[SIM ERR] ${s.addr} (${c.label}) => ${sim.err}`);
    }

    if (!choice) continue;

    if (!DRY_RUN) {
      await sendLimit(async () => {
        try {
          if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
          const tx = await pool.sendRequest(SOURCE, choice.args, SUBSCRIPTION_ID, FUNCTIONS_GAS_LIMIT, DON_SECRETS_SLOT, donHostedSecretsVersion2, donBytes);
          const idShown = choice.label === "args9" ? (choice.args[8] || '""') : "(none)";
          console.log(`[OK] sendRequest ${s.addr} using ${choice.label} id=${idShown}: ${tx.hash}`);
          submitted++;
        } catch (e: any) {
          const data = e?.data ?? e?.error?.data;
          let decoded = "unknown"; try { decoded = iface.parseError(data).name; } catch {}
          console.error(`[ERR] sendRequest ${s.addr} (${choice.label}):`, e?.reason || e?.message || e);
          if (data) console.error(` selector = ${data.slice?.(0,10)} (${decoded})`);
        }
      });
    } else {
      const idShown = choice.label === "args9" ? (choice.args[8] || '""') : "(none)";
      console.log(`[DRY_RUN] Would sendRequest ${s.addr} using ${choice.label} id=${idShown}`);
    }
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => { console.error(e); process.exit(1); });
