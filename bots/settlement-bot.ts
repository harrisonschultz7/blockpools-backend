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

// ✅ DRY_RUN = "1" means simulate; "0" means send real txs
const DRY_RUN = process.env.DRY_RUN === "1";

const TSDB_KEY = process.env.THESPORTSDB_API_KEY || "0";
const MAX_TX_PER_RUN = Number(process.env.MAX_TX_PER_RUN || 8);
const REQUEST_GAP_SECONDS = Number(process.env.REQUEST_GAP_SECONDS || 120);

const GITHUB_OWNER = process.env.GITHUB_OWNER || "harrisonschultz7";
const GITHUB_REPO = process.env.GITHUB_REPO || "blockpools-backend";
const GITHUB_REF = process.env.GITHUB_REF || "main";
const GH_PAT = process.env.GH_PAT;

// Optional override
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
    path.resolve(__dirname, "..", "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
    path.resolve(process.cwd(), "build", "artifacts", "contracts", "GamePool.sol", "GamePool.json"),
  ].filter(Boolean) as string[];

  for (const p of candidates) {
    try {
      if (fs.existsSync(p)) {
        const parsed = JSON.parse(fs.readFileSync(p, "utf8"));
        console.log(`✅ Using ABI from ${p}`);
        return { abi: parsed.abi, fromArtifact: true };
      }
    } catch {}
  }

  console.warn("⚠️  Could not locate GamePool.json. Using minimal ABI.");
  return { abi: FALLBACK_MIN_ABI, fromArtifact: false };
}

const { abi: poolAbi, fromArtifact } = loadGamePoolAbi();
const iface = new ethers.Interface(poolAbi);

// ===== Helpers =====
async function loadActiveSecrets() {
  const envVersion = process.env.DON_SECRETS_VERSION ?? process.env.SECRETS_VERSION;
  const envDonId = process.env.DON_ID;
  if (envVersion && envDonId) {
    return { secretsVersion: Number(envVersion), donId: envDonId, source: "env" };
  }

  try {
    const url = `https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/contents/activeSecrets.json?ref=${GITHUB_REF}`;
    const headers = {
      ...(GH_PAT ? { Authorization: `Bearer ${GH_PAT}` } : {}),
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "settlement-bot",
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
    console.warn("⚠️  Could not fetch activeSecrets.json:", e.message);
  }
  throw new Error("Failed to load DON pointer.");
}

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
  return `${dt.getUTCFullYear()}-${String(dt.getUTCMonth() + 1).padStart(2, "0")}-${String(dt.getUTCDate()).padStart(2, "0")}`;
}
function readGamesAtPath(p: string): string[] | null {
  if (!fs.existsSync(p)) return null;
  try {
    const raw = fs.readFileSync(p, "utf8");
    const grouped = JSON.parse(raw) as Record<string, Array<{ contractAddress: string }>>;
    const addrs = Object.values(grouped).flat().map((g) => g?.contractAddress).filter(Boolean);
    return Array.from(new Set(addrs));
  } catch {
    return null;
  }
}
function loadContractsFromGames(): string[] {
  if (GAMES_PATH_OVERRIDE) {
    const data = readGamesAtPath(GAMES_PATH_OVERRIDE);
    if (data) return data;
  }
  for (const p of GAMES_CANDIDATES) {
    const data = readGamesAtPath(p);
    if (data) return data;
  }
  return [];
}

// ===== Main =====
async function main() {
  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  if (!process.env.SUBSCRIPTION_ID) throw new Error("Missing SUBSCRIPTION_ID");

  console.log(`[CFG] DRY_RUN=${DRY_RUN} (env=${process.env.DRY_RUN})`);
  console.log(`[CFG] SUBSCRIPTION_ID=${process.env.SUBSCRIPTION_ID}`);

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const { secretsVersion, donId, source } = await loadActiveSecrets();
  const donHostedSecretsVersion = BigInt(secretsVersion);
  const donID = ethers.encodeBytes32String(donId);
  console.log(`🔐 Loaded DON pointer from ${source}`);
  console.log(`   secretsVersion = ${secretsVersion}`);
  console.log(`   donId          = ${donId}`);

  const contracts = loadContractsFromGames();
  if (!contracts.length) return console.log("No contracts found.");

  let submitted = 0;
  for (const addr of contracts) {
    if (submitted >= MAX_TX_PER_RUN) break;

    const pool = new ethers.Contract(addr, poolAbi, wallet);

    // --- Ownership check ---
    const botAddr = await wallet.getAddress();
    let onchainOwner = "(read failed)";
    try { onchainOwner = await pool.owner(); } catch {}
    const isOwner = onchainOwner !== "(read failed)" && onchainOwner.toLowerCase() === botAddr.toLowerCase();
    console.log(`[OWN] pool=${addr} owner=${onchainOwner} bot=${botAddr} isOwner=${isOwner}`);
    if (!isOwner) continue;

    // --- Read game info ---
    const [league, tA, tB, cA, cB, locked, reqSent, winTeam, lTime] = await Promise.all([
      pool.league(),
      pool.teamAName(),
      pool.teamBName(),
      pool.teamACode(),
      pool.teamBCode(),
      pool.isLocked(),
      pool.requestSent(),
      pool.winningTeam(),
      pool.lockTime(),
    ]);
    const isLocked = Boolean(locked);
    const requestSent = Boolean(reqSent);
    const winningTeam = Number(winTeam);
    const lockTime = Number(lTime);
    console.log(`[DBG] ${addr} locked=${isLocked} reqSent=${requestSent} win=${winningTeam} lockTime=${lockTime}`);
    if (!isLocked || requestSent || winningTeam !== 0) continue;
    if (lockTime > 0 && Date.now() / 1000 < lockTime + REQUEST_GAP_SECONDS) continue;

    const d0 = epochToEtISO(lockTime);
    const d1 = addDaysISO(d0, 1);
    const args = COMPAT_TSDB
      ? [league, d0, cA.toUpperCase(), cB.toUpperCase(), tA, tB]
      : [league, d0, d1, cA.toUpperCase(), cB.toUpperCase(), tA, tB, String(lockTime)];
    console.log(`[ARGS] ${addr} ${JSON.stringify(args)}`);

    // --- Static test ---
    try {
      await pool.sendRequest.staticCall(args, SUBSCRIPTION_ID, FUNCTIONS_GAS_LIMIT, DON_SECRETS_SLOT, donHostedSecretsVersion, donID);
      console.log(`[SIM OK] ${addr}`);
    } catch (e: any) {
      const data = e?.data ?? e?.error?.data;
      console.error(`[SIM ERR] ${addr} selector=${data?.slice?.(0,10)}`);
      continue;
    }

    // --- Send tx ---
    if (!DRY_RUN) {
      const tx = await pool.sendRequest(args, SUBSCRIPTION_ID, FUNCTIONS_GAS_LIMIT, DON_SECRETS_SLOT, donHostedSecretsVersion, donID);
      console.log(`[OK] sendRequest ${addr}: ${tx.hash}`);
      submitted++;
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
