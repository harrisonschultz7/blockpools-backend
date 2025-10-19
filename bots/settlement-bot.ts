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
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);    // sec after lock before request

// Concurrency controls
const READ_CONCURRENCY = Number(process.env.READ_CONCURRENCY || 25);
const TSDB_CONCURRENCY = Number(process.env.TSDB_CONCURRENCY || 8);
const TX_SIM_CONCURRENCY = Number(process.env.TX_SIM_CONCURRENCY || 10);
const TX_SEND_CONCURRENCY = Number(process.env.TX_SEND_CONCURRENCY || 3);

const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS || 0);

// TheSportsDB
const THESPORTSDB_API_KEY = process.env.THESPORTSDB_API_KEY || "";

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
    "User-Agent": "settlement-bot",
    Accept: "application/vnd.github+json",
  };
  const res = await fetch(url, { headers });
  if (!res.ok) throw new Error(`activeSecrets.json HTTP ${res.status}`);
  const data = await res.json();
  const json = JSON.parse(Buffer.from(data.content, "base64").toString("utf8"));
  return { secretsVersion: Number(json.secretsVersion ?? json.version), donId: json.donId || "fun-ethereum-sepolia-1", source: "github" };
}

/* ──────────────────────────────────────────────────────────────────────────────
   TheSportsDB helpers
────────────────────────────────────────────────────────────────────────────── */
function mapLeagueForTSDB(leagueOnChain: string): string {
  const lk = String(leagueOnChain || "").toLowerCase();
  const TSDB_LABEL: Record<string, string> = {
    mlb: "MLB", nfl: "NFL", nba: "NBA", nhl: "NHL",
    epl: "English%20Premier%20League", ucl: "UEFA%20Champions%20League",
  };
  return TSDB_LABEL[lk] || leagueOnChain;
}

const FINAL_MARKERS = ["final", "ft", "match finished", "ended", "game finished", "full time", "aet"];
const cacheBust = () => `cb=${Date.now()}`;

function looksFinal(ev: any) {
  const status = String(ev?.strStatus ?? ev?.strProgress ?? "").toLowerCase();
  const desc   = String(ev?.strDescriptionEN ?? "").toLowerCase();
  const hasScores = (ev?.intHomeScore != null && ev?.intAwayScore != null);
  if (FINAL_MARKERS.some(m => status.includes(m) || desc.includes(m))) return true;
  if (status === "ft") return true;
  return hasScores && !status;
}

async function tsdbDayEvents(dateISO: string, leagueParam?: string) {
  const base = "https://www.thesportsdb.com/api/v1/json";
  const key  = THESPORTSDB_API_KEY;
  const urls = leagueParam
    ? [
        `${base}/${key}/eventsday.php?d=${encodeURIComponent(dateISO)}&l=${leagueParam}&${cacheBust()}`,
        `${base}/${key}/eventsday.php?d=${encodeURIComponent(dateISO)}&${cacheBust()}`,
      ]
    : [`${base}/${key}/eventsday.php?d=${encodeURIComponent(dateISO)}&${cacheBust()}`];

  for (const u of urls) {
    const r = await fetch(u);
    if (!r.ok) continue;
    const j = await r.json().catch(() => null);
    if (Array.isArray(j?.events) && j.events.length) return j.events;
  }
  return [];
}

async function tsdbEventById(eventId: number | string) {
  const base = "https://www.thesportsdb.com/api/v1/json";
  const key  = THESPORTSDB_API_KEY;
  const u = `${base}/${key}/lookupevent.php?id=${encodeURIComponent(String(eventId))}&${cacheBust()}`;
  const r = await fetch(u);
  if (!r.ok) return null;
  const j = await r.json().catch(() => null);
  const ev = j?.events;
  if (Array.isArray(ev) && ev.length) return ev[0];
  return null;
}

function indexDayEvents(events: any[]) {
  const norm = (s: string) => (s || "").toLowerCase().trim();
  const keyFrom = (a: string, b: string) => `${norm(a)}|${norm(b)}`;
  const map = new Map<string, any>();
  for (const e of events) {
    const h = e?.strHomeTeam || "";
    const a = e?.strAwayTeam || "";
    const alt = e?.strEventAlternate || "";
    map.set(keyFrom(h, a), e);
    if (alt) map.set(`alt|${(alt || "").toLowerCase()}`, e);
  }
  return {
    getMatch(teamAName: string, teamBName: string) {
      const k1 = keyFrom(teamAName, teamBName);
      const k2 = keyFrom(teamBName, teamAName);
      const e1 = map.get(k1); if (e1) return e1;
      const e2 = map.get(k2); if (e2) return e2;
      for (const [k, e] of map) {
        if (!k.startsWith("alt|")) continue;
        const alt = k.slice(4);
        if (alt.includes((teamAName || "").toLowerCase()) && alt.includes((teamBName || "").toLowerCase())) return e;
      }
      return null;
    }
  };
}

function consensusFromCaches(params: {
  teamAName: string; teamBName: string;
  date0Idx?: ReturnType<typeof indexDayEvents>;
  date1Idx?: ReturnType<typeof indexDayEvents>;
  byIdEvent?: any | null;
}) {
  const { teamAName, teamBName, date0Idx, date1Idx, byIdEvent } = params;

  let byIdOk: boolean | null = null;
  let byIdStatus: string | undefined = undefined;
  if (byIdEvent !== undefined) {
    if (byIdEvent) {
      byIdOk = looksFinal(byIdEvent);
      byIdStatus = String(byIdEvent?.strStatus ?? byIdEvent?.strProgress ?? "");
    } else { byIdOk = false; byIdStatus = "id_not_found"; }
  }

  function scan(idx?: ReturnType<typeof indexDayEvents>) {
    if (!idx) return { final: false, status: "no_slice" };
    const m = idx.getMatch(teamAName, teamBName);
    if (!m) return { final: false, status: "no_match" };
    return { final: looksFinal(m), status: String(m?.strStatus ?? m?.strProgress ?? "") };
  }

  const r0 = scan(date0Idx);
  const r1 = scan(date1Idx);
  const best = r0.final ? r0 : (r1.final ? r1 : (r0.status !== "no_slice" ? r0 : r1));

  if (byIdOk === null) return { final: best.final, status: best.status };
  return { final: Boolean(byIdOk && best.final), status: `id:${byIdStatus}|day:${best.status}` };
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

  /* STEP 2: Provider finality checks (optional) */
  let finalEligible: PoolState[] = timeGated;
  if (REQUIRE_FINAL_CHECK) {
    if (!THESPORTSDB_API_KEY) { console.log("REQUIRE_FINAL_CHECK=1 but no THESPORTSDB_API_KEY set. Skipping to avoid wasted LINK."); return; }

    type DayKey = string;
    const dayKeys = new Set<DayKey>(); const idKeys = new Set<string>();
    for (const s of timeGated) {
      const leagueParam = mapLeagueForTSDB(s.league);
      const date0 = epochToEtISO(s.lockTime);
      const date1 = addDaysISO(date0, 1);
      dayKeys.add(`${leagueParam}|${date0}`); dayKeys.add(`${leagueParam}|${date1}`);
      if (s.tsdbEventId != null && s.tsdbEventId !== "") idKeys.add(String(s.tsdbEventId));
    }

    const dayCache = new Map<DayKey, ReturnType<typeof indexDayEvents> | undefined>();
    await Promise.all(
      Array.from(dayKeys).map(key =>
        limiter(TSDB_CONCURRENCY)(async () => {
          const [leagueParam, dateISO] = key.split("|");
          const events = await tsdbDayEvents(dateISO, leagueParam);
          dayCache.set(key, events.length ? indexDayEvents(events) : undefined);
        })
      )
    );

    const idCache = new Map<string, any | null>();
    await Promise.all(
      Array.from(idKeys).map(id =>
        limiter(TSDB_CONCURRENCY)(async () => {
          const ev = await tsdbEventById(id);
          idCache.set(id, ev || null);
        })
      )
    );

    const eligible: PoolState[] = [];
    for (const s of timeGated) {
      const leagueParam = mapLeagueForTSDB(s.league);
      const date0 = epochToEtISO(s.lockTime);
      const date1 = addDaysISO(date0, 1);
      const date0Idx = dayCache.get(`${leagueParam}|${date0}`);
      const date1Idx = dayCache.get(`${leagueParam}|${date1}`);
      const byIdEvent = (s.tsdbEventId != null && s.tsdbEventId !== "") ? idCache.get(String(s.tsdbEventId)) : undefined;

      const c = consensusFromCaches({ teamAName: s.teamAName, teamBName: s.teamBName, date0Idx, date1Idx, byIdEvent });
      if (c.final) eligible.push(s);
      else console.log(`[SKIP] Not final yet by TSDB (${leagueParam} ${s.teamAName} vs ${s.teamBName}) status="${c.status}"`);
    }

    finalEligible = eligible;
    if (!finalEligible.length) { console.log("No games confirmed final by provider consensus. Submitted 0 transaction(s)."); return; }
    console.log(`✅ Provider consensus final for ${finalEligible.length} pool(s).`);
  }

  /* STEP 3: Simulate & send — adaptive 9 ⇄ 8 args */
  const { secretsVersion: sv } = await loadActiveSecrets();
  const donHostedSecretsVersion2 = BigInt(sv);

  const buildArgs9 = (s: PoolState): string[] => {
    const d0 = epochToEtISO(s.lockTime), d1 = addDaysISO(d0, 1);
    const leagueArg = mapLeagueForTSDB(s.league);
    const id = (s.tsdbEventId != null && s.tsdbEventId !== "") ? String(s.tsdbEventId) : "";
    return [leagueArg, d0, d1, String(s.teamACode).toUpperCase(), String(s.teamBCode).toUpperCase(), s.teamAName, s.teamBName, String(s.lockTime), id];
  };
  const buildArgs8 = (s: PoolState): string[] => {
    const d0 = epochToEtISO(s.lockTime), d1 = addDaysISO(d0, 1);
    const leagueArg = mapLeagueForTSDB(s.league);
    return [leagueArg, d0, d1, String(s.teamACode).toUpperCase(), String(s.teamBCode).toUpperCase(), s.teamAName, s.teamBName, String(s.lockTime)];
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
    const hasId = (s.tsdbEventId != null && s.tsdbEventId !== "");

    // If we have an ID: try 9 → 8.
    // If we don't have an ID: try 8 → 9 (with empty id) to cover 9-arg-only contracts.
    const candidates: Array<{ label: "args9" | "args8"; args: string[] }> = hasId
      ? [{ label: "args9", args: buildArgs9(s) }, { label: "args8", args: buildArgs8(s) }]
      : [{ label: "args8", args: buildArgs8(s) }, { label: "args9", args: buildArgs9(s) }];

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
          const idShown = choice.label === "args9" ? choice.args[8] || '""' : "(none)";
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
      const idShown = choice.label === "args9" ? choice.args[8] || '""' : "(none)";
      console.log(`[DRY_RUN] Would sendRequest ${s.addr} using ${choice.label} id=${idShown}`);
    }
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => { console.error(e); process.exit(1); });
