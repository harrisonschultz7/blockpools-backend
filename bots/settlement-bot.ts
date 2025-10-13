// @ts-nocheck
try { require("dotenv").config(); } catch {}

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { ethers } from "ethers";
import { gamePoolAbi as IMPORTED_GAMEPOOL_ABI } from "./gamepool.abi";

/* =========================
   ESM-safe __dirname / __filename
   ========================= */
const __filename =
  typeof (globalThis as any).__filename !== "undefined"
    ? (globalThis as any).__filename
    : fileURLToPath(import.meta.url);
const __dirname =
  typeof (globalThis as any).__dirname !== "undefined"
    ? (globalThis as any).__dirname
    : path.dirname(__filename);

/* =========================
   Env / configuration
   ========================= */
const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const SUBSCRIPTION_ID = BigInt(process.env.SUBSCRIPTION_ID!);                  // uint64
const FUNCTIONS_GAS_LIMIT = Number(process.env.FUNCTIONS_GAS_LIMIT || 300000); // uint32
const DON_SECRETS_SLOT = Number(process.env.DON_SECRETS_SLOT || 0);            // uint8
const COMPAT_TSDB = process.env.COMPAT_TSDB === "1";

// DRY_RUN = "1" means simulate; anything else (unset/"0") sends real txs
const DRY_RUN = process.env.DRY_RUN === "1";

const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

// Finality pre-gate config
const THESPORTSDB_API_KEY = process.env.THESPORTSDB_API_KEY || "";
const REQUIRE_FINAL_CHECK = process.env.REQUIRE_FINAL_CHECK !== "0";  // enable by default
const POSTGAME_MIN_ELAPSED = Number(process.env.POSTGAME_MIN_ELAPSED || 300); // additional buffer after lock (sec)

/* === DON pointer (activeSecrets.json) lookup === */
const GITHUB_OWNER = process.env.GITHUB_OWNER || "harrisonschultz7";
const GITHUB_REPO  = process.env.GITHUB_REPO  || "blockpools-backend";
const GITHUB_REF   = process.env.GITHUB_REF   || "main";
const GH_PAT       = process.env.GH_PAT;

/* === Where games.json is === */
const GAMES_PATH_OVERRIDE = process.env.GAMES_PATH || "";
const GAMES_CANDIDATES = [
  path.resolve(__dirname, "..", "src", "data", "games.json"),
  path.resolve(__dirname, "..", "games.json"),
];

/* === Where Functions SOURCE lives (your Windows path + fallbacks) === */
const SOURCE_CANDIDATES = [
  "C:\\Users\\harri\\OneDrive\\functions-betting-app\\functions-hardhat-starter-kit\\blockpools-backend\\bots\\source.js",
  path.resolve(__dirname, "source.js"),
  path.resolve(__dirname, "..", "bots", "source.js"),
  path.resolve(process.cwd(), "bots", "source.js"),
];

/* =========================
   ABI loader (artifact -> imported -> fallback)
   ========================= */
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
  // NEW signature: sendRequest(source, args, subId, gas, slot, version, donID)
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

function loadGamePoolAbi(): { abi: any; fromArtifact: boolean; source: "artifact" | "imported" | "minimal" } {
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
        return { abi: parsed.abi, fromArtifact: true, source: "artifact" };
      }
    } catch {}
  }

  if (IMPORTED_GAMEPOOL_ABI && Array.isArray(IMPORTED_GAMEPOOL_ABI) && IMPORTED_GAMEPOOL_ABI.length) {
    console.warn("⚠️  Using ABI from local import (gamepool.abi).");
    return { abi: IMPORTED_GAMEPOOL_ABI, fromArtifact: false, source: "imported" };
  }

  console.warn("⚠️  Could not locate GamePool.json or imported ABI. Using minimal ABI.");
  return { abi: FALLBACK_MIN_ABI, fromArtifact: false, source: "minimal" };
}

const { abi: poolAbi, fromArtifact } = loadGamePoolAbi();
const iface = new ethers.Interface(poolAbi);

/* =========================
   Helpers
   ========================= */
async function loadActiveSecrets(): Promise<{ secretsVersion: number; donId: string; source: string }> {
  const envVersion = process.env.DON_SECRETS_VERSION ?? process.env.SECRETS_VERSION;
  const envDonId = process.env.DON_ID;
  if (envVersion && envDonId) {
    return { secretsVersion: Number(envVersion), donId: envDonId, source: "env" };
  }

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
  return {
    secretsVersion: Number(json.secretsVersion ?? json.version),
    donId: json.donId || "fun-ethereum-sepolia-1",
    source: "github",
  };
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

function readGamesAtPath(p: string): string[] | null {
  if (!fs.existsSync(p)) return null;
  try {
    const raw = fs.readFileSync(p, "utf8");
    const grouped = JSON.parse(raw) as Record<string, Array<{ contractAddress: string }>>;
    const addrs = Object.values(grouped).flat().map((g) => g?.contractAddress).filter(Boolean);
    const uniq = Array.from(new Set(addrs));
    if (uniq.length) {
      console.log(`Using games from ${p} (${uniq.length} contracts)`);
      return uniq;
    }
  } catch (e) {
    console.warn(`Failed to parse ${p}:`, (e as Error).message);
  }
  return null;
}

function loadContractsFromGames(): string[] {
  if (GAMES_PATH_OVERRIDE) {
    const fromOverride = readGamesAtPath(GAMES_PATH_OVERRIDE);
    if (fromOverride) return fromOverride;
    console.warn(`GAMES_PATH was set but not readable/usable: ${GAMES_PATH_OVERRIDE}`);
  }
  for (const p of GAMES_CANDIDATES) {
    const fromLocal = readGamesAtPath(p);
    if (fromLocal) return fromLocal;
  }
  const envList = (process.env.CONTRACTS || "").trim();
  if (envList) {
    const arr = envList.split(/[,\s]+/).filter(Boolean);
    const filtered = arr.filter((a) => {
      try { return ethers.isAddress(a); } catch { return false; }
    });
    if (filtered.length) {
      console.log(`Using CONTRACTS from env (${filtered.length})`);
      return Array.from(new Set(filtered));
    }
  }
  console.warn("No contracts found in games.json or CONTRACTS env. Nothing to do.");
  return [];
}

/** Map on-chain league -> TheSportsDB label for contract SOURCE (&l= param). */
function mapLeagueForTSDB(leagueOnChain: string): string {
  const lk = String(leagueOnChain || "").toLowerCase();
  const TSDB_LABEL: Record<string, string> = {
    mlb: "MLB",
    nfl: "NFL",
    nba: "NBA",
    nhl: "NHL",
    epl: "English%20Premier%20League",
    ucl: "UEFA%20Champions%20League",
  };
  return TSDB_LABEL[lk] || leagueOnChain;
}

/* =========================
   Finality pre-gate (TheSportsDB)
   ========================= */
const FINAL_MARKERS = [
  "final", "ft", "match finished", "ended", "game finished", "full time", "aet"
];

// Returns true if TSDB shows the game as final on dateISO (or next day)
async function tsdbLooksFinal(opts: {
  leagueParam: string;  // e.g. "MLB" or "English%20Premier%20League"
  dateISO: string;      // YYYY-MM-DD (local ET ISO)
  altDateISO?: string;  // YYYY-MM-DD next day (handles late-night finishes)
  teamAName: string;
  teamBName: string;
}): Promise<{ final: boolean; rawStatus?: string; debug?: any }> {
  if (!THESPORTSDB_API_KEY) return { final: false, rawStatus: "no_api_key" };

  const q = async (d: string) => {
    const u = `https://www.thesportsdb.com/api/v1/json/${THESPORTSDB_API_KEY}/eventsday.php?d=${d}&l=${opts.leagueParam}`;
    const r = await fetch(u);
    if (!r.ok) return null;
    const j = await r.json().catch(() => null);
    return j?.events || [];
  };

  const norm = (s: string) => (s || "").toLowerCase().trim();
  const wantedA = norm(opts.teamAName);
  const wantedB = norm(opts.teamBName);

  const scan = (events: any[]) => {
    for (const e of events) {
      const h = norm(e.strHomeTeam);
      const a = norm(e.strAwayTeam);
      const alt = norm(e.strEventAlternate || ""); // e.g., "Away @ Home"
      const namesMatch =
        ((h.includes(wantedA) && a.includes(wantedB)) ||
         (h.includes(wantedB) && a.includes(wantedA)) ||
         (alt.includes(wantedA) && alt.includes(wantedB)));

      if (!namesMatch) continue;

      const status = norm(e.strStatus ?? e.strProgress ?? "");
      const desc   = norm(e.strDescriptionEN ?? "");
      const looksFinal =
        FINAL_MARKERS.some(m => status.includes(m) || desc.includes(m));

      const hasScores = (e.intHomeScore != null && e.intAwayScore != null);
      if (looksFinal || status === "ft" || (hasScores && status === "")) {
        return { final: true, rawStatus: status || (hasScores ? "scores_present" : "") , debug: e };
      }
      return { final: false, rawStatus: status || desc, debug: e };
    }
    return { final: false, rawStatus: "no_match" };
  };

  const e0 = await q(opts.dateISO) || [];
  const r0 = scan(e0);
  if (r0.final) return r0;

  if (opts.altDateISO) {
    const e1 = await q(opts.altDateISO) || [];
    const r1 = scan(e1);
    if (r1.final) return r1;
    return r1;
  }

  return r0;
}

/* =========================
   Main
   ========================= */
async function main() {
  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  if (!process.env.SUBSCRIPTION_ID) throw new Error("Missing SUBSCRIPTION_ID");

  console.log(`[CFG] DRY_RUN=${DRY_RUN} (env=${process.env.DRY_RUN ?? "(unset)"})`);
  console.log(`[CFG] SUBSCRIPTION_ID=${process.env.SUBSCRIPTION_ID}`);
  console.log(`[CFG] REQUIRE_FINAL_CHECK=${REQUIRE_FINAL_CHECK} POSTGAME_MIN_ELAPSED=${POSTGAME_MIN_ELAPSED}s`);

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const { secretsVersion, donId, source } = await loadActiveSecrets();
  const donHostedSecretsVersion = BigInt(secretsVersion);
  const donID = ethers.encodeBytes32String(donId);
  console.log(`🔐 Loaded DON pointer from ${source}`);
  console.log(`   secretsVersion = ${secretsVersion}`);
  console.log(`   donId          = ${donId}`);

  const SOURCE = loadSourceCode();

  const contracts = loadContractsFromGames();
  if (!contracts.length) return;

  let submitted = 0;

  for (const addr of contracts) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(addr, poolAbi, wallet);

    // Ownership check (optional)
    const botAddr = await wallet.getAddress();
    let onchainOwner = "(read failed)";
    try { onchainOwner = await pool.owner(); } catch {}
    const isOwner = onchainOwner !== "(read failed)" && onchainOwner.toLowerCase() === botAddr.toLowerCase();
    console.log(`[OWN] pool=${addr} owner=${onchainOwner} bot=${botAddr} isOwner=${isOwner}`);
    if (!isOwner) continue;

    // Read state
    let league: string, teamAName: string, teamBName: string, teamACode: string, teamBCode: string;
    let isLocked: boolean, requestSent: boolean, winningTeam: number, lockTime: number;

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
      league = String(lg || "");
      teamAName = String(ta || "");
      teamBName = String(tb || "");
      teamACode = String(tca || "");
      teamBCode = String(tcb || "");
      isLocked = Boolean(locked);
      requestSent = Boolean(req);
      winningTeam = Number(win);
      lockTime = Number(lt);
    } catch (e) {
      console.error(`[ERR] read state ${addr}:`, (e as Error).message);
      continue;
    }

    console.log(`[DBG] ${addr} locked=${isLocked} reqSent=${requestSent} win=${winningTeam} lockTime=${lockTime}`);

    // Basic on-chain gates
    const nowSec = Math.floor(Date.now() / 1000);
    if (!isLocked || requestSent || winningTeam !== 0) { console.log(`[SKIP] state gate`); continue; }

    // Ensure a small gap after scheduled lock (helps with late API flips / clock skew)
    if (lockTime > 0 && nowSec < lockTime + REQUEST_GAP_SECONDS) {
      console.log(`[SKIP] gap after lock: need ${lockTime + REQUEST_GAP_SECONDS - nowSec}s more`);
      continue;
    }

    // Optional: Require additional post-game buffer AND provider finality
    if (REQUIRE_FINAL_CHECK) {
      const date0 = epochToEtISO(lockTime);
      const date1 = addDaysISO(date0, 1);
      const leagueParam = mapLeagueForTSDB(league);

      // Add a small post-game buffer regardless of API status
      if (lockTime > 0 && nowSec < lockTime + POSTGAME_MIN_ELAPSED) {
        console.log(`[SKIP] postgame buffer (${POSTGAME_MIN_ELAPSED}s) not elapsed yet`);
        continue;
      }

      try {
        const finalCheck = await tsdbLooksFinal({
          leagueParam,
          dateISO: date0,
          altDateISO: date1,
          teamAName,
          teamBName
        });

        if (!finalCheck.final) {
          console.log(`[SKIP] not final yet by TSDB (${leagueParam} ${teamAName} vs ${teamBName}) status="${finalCheck.rawStatus}"`);
          continue;
        }

        console.log(`[OK] Final confirmed by TSDB (${leagueParam}) status="${finalCheck.rawStatus}"`);
      } catch (e) {
        console.warn(`[WARN] TSDB final check failed, skipping to avoid wasted LINK: ${(e as Error).message}`);
        continue;
      }
    }

    // Build args consistent with your Functions JS (expects 8 args normally)
    const d0 = epochToEtISO(lockTime);
    const d1 = addDaysISO(d0, 1);
    const leagueArg = mapLeagueForTSDB(league);
    const fullArgs = [
      leagueArg,                       // L
      d0,                              // date0
      d1,                              // date1
      String(teamACode).toUpperCase(), // A code
      String(teamBCode).toUpperCase(), // B code
      teamAName,                       // A name
      teamBName,                       // B name
      String(lockTime),                // kickoff epoch
    ];
    const compatArgs = [
      leagueArg,
      d0,
      String(teamACode).toUpperCase(),
      String(teamBCode).toUpperCase(),
      teamAName,
      teamBName,
    ];
    const args = COMPAT_TSDB ? compatArgs : fullArgs;
    console.log(`[ARGS] ${addr} ${JSON.stringify(args)}`);

    // Static-call probe (gasless simulation) — NOTE: includes SOURCE first
    try {
      await pool.sendRequest.staticCall(
        SOURCE,
        args,
        SUBSCRIPTION_ID,
        FUNCTIONS_GAS_LIMIT,
        DON_SECRETS_SLOT,
        BigInt(donHostedSecretsVersion),
        donID
      );
      console.log(`[SIM OK] ${addr}`);
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      let decoded = "unknown";
      if (fromArtifact && data) {
        try { decoded = iface.parseError(data).name; } catch {}
      }
      console.error(`[SIM ERR] ${addr} selector=${data?.slice?.(0,10)}${decoded ? ` (${decoded})` : ""}`);
      continue;
    }

    // Send tx
    if (!DRY_RUN) {
      try {
        console.log(`[TX] sendRequest(${addr}) ...`);
        const tx = await pool.sendRequest(
          SOURCE,
          args,
          SUBSCRIPTION_ID,
          FUNCTIONS_GAS_LIMIT,
          DON_SECRETS_SLOT,
          BigInt(donHostedSecretsVersion),
          donID
        );
        console.log(`[OK] sendRequest ${addr}: ${tx.hash}`);
        submitted++;
      } catch (e: any) {
        const data = e?.data ?? e?.error?.data;
        let decoded = "unknown custom error";
        if (fromArtifact && data) {
          try { decoded = iface.parseError(data).name; } catch {}
        }
        console.error(`[ERR] sendRequest ${addr}:`, e?.reason || e?.message || e);
        if (data) console.error(` selector = ${data.slice(0,10)} (${decoded})`);
      }
    } else {
      console.log(`[DRY_RUN] Skipped ${addr}`);
    }
  }

  console.log(`Submitted ${submitted} transaction(s).`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
