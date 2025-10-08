// @ts-nocheck
try { require("dotenv").config(); } catch {}

import fs from "fs";
import path from "path";
import { ethers } from "ethers";

// ===== Env =====
const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const SUBSCRIPTION_ID = BigInt(process.env.SUBSCRIPTION_ID!);
const FUNCTIONS_GAS_LIMIT = Number(process.env.FUNCTIONS_GAS_LIMIT || 300000);
const DON_SECRETS_SLOT = Number(process.env.DON_SECRETS_SLOT || 0);
const COMPAT_TSDB = process.env.COMPAT_TSDB === "1";

// "1" = dry run only; "0" = actually send txs
const DRY_RUN = process.env.DRY_RUN === "1";

const TSDB_KEY = process.env.THESPORTSDB_API_KEY || "0";
const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

const GITHUB_OWNER = process.env.GITHUB_OWNER || "harrisonschultz7";
const GITHUB_REPO = process.env.GITHUB_REPO || "blockpools-backend";
const GITHUB_REF = process.env.GITHUB_REF || "main";
const GH_PAT = process.env.GH_PAT;

const GAMES_PATH_OVERRIDE = process.env.GAMES_PATH || "";
const GAMES_CANDIDATES = [
  path.resolve(__dirname, "..", "src", "data", "games.json"),
  path.resolve(__dirname, "..", "games.json"),
];

// ===== ABI loader with fallback =====
const FALLBACK_MIN_ABI = [
  { inputs: [], name: "league", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamAName", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamBName", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamACode", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "teamBCode", outputs: [{ type: "string" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "isLocked", outputs: [{ type: "bool" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "requestSent", outputs: [{ type: "bool" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "winningTeam", outputs: [{ type: "uint8" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "lockTime", outputs: [{ type: "uint256" }], stateMutability: "view", type: "function" },
  { inputs: [], name: "owner", outputs: [{ type: "address" }], stateMutability: "view", type: "function" },
  {
    inputs: [
      { type: "string[]", name: "args" },
      { type: "uint64", name: "subscriptionId" },
      { type: "uint32", name: "gasLimit" },
      { type: "uint8", name: "donHostedSecretsSlotID" },
      { type: "uint64", name: "donHostedSecretsVersion" },
      { type: "bytes32", name: "donID" },
    ],
    name: "sendRequest",
    outputs: [],
    stateMutability: "nonpayable",
    type: "function",
  },
] as const;

function loadGamePoolAbi(): { abi: any; fromArtifact: boolean } {
  const ARTIFACT_PATH_ENV = process.env.ARTIFACT_PATH?.trim();
  const candidates = [
    ARTIFACT_PATH_ENV && (path.isAbsolute(ARTIFACT_PATH_ENV)
      ? ARTIFACT_PATH_ENV
      : path.resolve(process.cwd(), ARTIFACT_PATH_ENV)),
    path.resolve(__dirname, "..", "..", "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(__dirname, "..", "..", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(__dirname, "..", "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(__dirname, "..", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(process.cwd(), "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(process.cwd(), "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
  ].filter(Boolean) as string[];

  for (const p of candidates) {
    try {
      if (p && fs.existsSync(p)) {
        const parsed = JSON.parse(fs.readFileSync(p, "utf8"));
        console.log(`✅ Using ABI from ${p}`);
        return { abi: parsed.abi, fromArtifact: true };
      }
    } catch {}
  }

  console.warn("⚠️  Could not locate GamePool.json. Falling back to minimal ABI (custom error names may not decode).");
  return { abi: FALLBACK_MIN_ABI, fromArtifact: false };
}

const { abi: poolAbi, fromArtifact } = loadGamePoolAbi();
const iface = new ethers.Interface(poolAbi);

// ===== Helpers =====
async function loadActiveSecrets(): Promise<{ secretsVersion: number; donId: string; source: string }> {
  const envVersion = process.env.DON_SECRETS_VERSION ?? process.env.SECRETS_VERSION;
  const envDonId = process.env.DON_ID;
  if (envVersion && envDonId) {
    return { secretsVersion: Number(envVersion), donId: envDonId, source: "env" };
  }

  try {
    const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/activeSecrets.json?ref=${GITHUB_REF}`;
    const headers: any = {
      ...(GH_PAT ? { Authorization: `Bearer ${GH_PAT}` } : {}),
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "settlement-bot",
      Accept: "application/vnd.github+json",
    };
    const res = await fetch(url, { headers });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();
    const json = JSON.parse(Buffer.from(data.content, "base64").toString("utf8"));
    return {
      secretsVersion: Number(json.secretsVersion ?? json.version),
      donId: json.donId || "fun-ethereum-sepolia-1",
      source: "github",
    };
  } catch (e: any) {
    console.warn("⚠️  Could not fetch activeSecrets.json from GitHub:", e?.message || e);
  }

  try {
    const localPath = path.join(__dirname, "..", "activeSecrets.json");
    const json = JSON.parse(fs.readFileSync(localPath, "utf8"));
    return {
      secretsVersion: Number(json.secretsVersion ?? json.version),
      donId: json.donId || "fun-ethereum-sepolia-1",
      source: "local",
    };
  } catch {
    throw new Error("Failed to load activeSecrets.json from env, GitHub, or local.");
  }
}

const f = (s: string) => (s || "").trim().toLowerCase();
function epochToEtISO(epochSec: number) {
  const dt = new Date(epochSec * 1000);
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  const parts: Record<string, string> = {};
  for (const p of fmt.formatToParts(dt)) parts[p.type] = p.value;
  return `${parts.year}-${parts.month}-${parts.day}`;
}
function addDaysISO(iso: string, days: number) {
  const [y, m, d] = iso.split("-").map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  dt.setUTCDate(dt.getUTCDate() + days);
  const y2 = dt.getUTCFullYear();
  const m2 = String(dt.getUTCMonth() + 1).padStart(2, "0");
  const d2 = String(dt.getUTCDate()).padStart(2, "0");
  return `${y2}-${m2}-${d2}`;
}

// ===== games.json loader =====
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
      try {
        return ethers.isAddress(a);
      } catch {
        return false;
      }
    });
    if (filtered.length) {
      console.log(`Using CONTRACTS from env (${filtered.length})`);
      return Array.from(new Set(filtered));
    }
  }
  console.warn("No contracts found in games.json or CONTRACTS env. Nothing to do.");
  return [];
}

// ===== Main =====
async function main() {
  if (!RPC_URL) throw new Error("RPC_URL missing");
  if (!PRIVATE_KEY) throw new Error("PRIVATE_KEY missing");
  if (!process.env.SUBSCRIPTION_ID) throw new Error("SUBSCRIPTION_ID missing");

  console.log(`[CFG] DRY_RUN=${DRY_RUN} (env=${process.env.DRY_RUN})`);
  console.log(`[CFG] SUBSCRIPTION_ID=${process.env.SUBSCRIPTION_ID}`);
  console.log(`[CFG] FUNCTIONS_GAS_LIMIT=${FUNCTIONS_GAS_LIMIT}`);

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const { secretsVersion, donId, source } = await loadActiveSecrets();
  if (!Number.isFinite(secretsVersion)) throw new Error("Invalid secretsVersion.");
  console.log(`🔐 Loaded DON pointer from ${source}`);
  console.log(`   secretsVersion = ${secretsVersion}`);
  console.log(`   donId          = ${donId}`);

  const donHostedSecretsVersion = BigInt(secretsVersion);
  const donID = ethers.encodeBytes32String(donId);
  const contracts = loadContractsFromGames();
  if (!contracts.length) return;

  let submitted = 0;

  for (const addr of contracts) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(addr, poolAbi, wallet);

    // Verify ownership
    const botAddr = await wallet.getAddress();
    let onchainOwner = "(read failed)";
    try { onchainOwner = await pool.owner(); } catch {}
    const isOwner = onchainOwner !== "(read failed)" && onchainOwner.toLowerCase() === botAddr.toLowerCase();
    console.log(`[OWN] pool=${addr} owner=${onchainOwner} bot=${botAddr} isOwner=${isOwner}`);
    if (!isOwner) {
      console.error(`[SKIP] ${addr} signer is not the owner. This would revert "Not contract owner".`);
      continue;
    }

    let league, teamAName, teamBName, teamACode, teamBCode;
    let isLocked, requestSent, winningTeam, lockTime;
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
      league = String(lg || "").toLowerCase();
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

    if (!isLocked || requestSent || winningTeam !== 0) continue;
    if (lockTime > 0 && Date.now() / 1000 < lockTime + REQUEST_GAP_SECONDS) continue;

    const d0 = epochToEtISO(lockTime);
    const d1 = addDaysISO(d0, 1);

    const fullArgs = [
      league,
      d0,
      d1,
      String(teamACode).toUpperCase(),
      String(teamBCode).toUpperCase(),
      teamAName,
      teamBName,
      String(lockTime),
    ];
    const compatArgs = [
      league,
      d0,
      String(teamACode).toUpperCase(),
      String(teamBCode).toUpperCase(),
      teamAName,
      teamBName,
    ];
    const args = COMPAT_TSDB ? compatArgs : fullArgs;
    console.log(`[DBG] ${addr} args=${JSON.stringify(args)}`);

    // Static call
    try {
      console.log(`[SIM] Static-call test for ${addr}`);
      await pool.sendRequest.staticCall(
        args,
        SUBSCRIPTION_ID,
        FUNCTIONS_GAS_LIMIT,
        DON_SECRETS_SLOT,
        donHostedSecretsVersion,
        donID
      );
      console.log(`[SIM OK] Static call succeeded, proceeding with tx`);
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      let decoded = "unknown";
      try { if (data) decoded = iface.parseError(data).name; } catch {}
      console.error(`[SIM-ERR] ${addr} selector=${data?.slice?.(0,10)} (${decoded})`);
      continue;
    }

    try {
      if (DRY_RUN) {
        console.log(`[DRY_RUN] Would call sendRequest(${addr})`);
      } else {
        console.log(`[TX] sendRequest(${addr}) ...`);
        const tx = await pool.sendRequest(
          args,
          SUBSCRIPTION_ID,
          FUNCTIONS_GAS_LIMIT,
          DON_SECRETS_SLOT,
          donHostedSecretsVersion,
          donID
        );
        console.log(`[OK] sendRequest sent for ${addr}: ${tx.hash}`);
      }
      submitted++;
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      let decoded = "unknown custom error";
      try { if (data) decoded = iface.parseError(data).name; } catch {}
      console.error(`[ERR] sendRequest ${addr}:`, e?.reason || e?.message || e);
      if (data) console.error(` selector = ${data.slice(0,10)} (${decoded})`);
    }
  }

  console.log(`Submitted ${submitted} request(s) this run`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
