// upload-secrets.js — plain Node.js (CommonJS)
// 1) Uploads secrets to DON (direct -> fallback encrypt+gateway)
// 2) Writes activeSecrets.json atomically to repo root
// 3) Commits & pushes to GitHub
// 4) Prints clean JSON {donId, secretsVersion, uploadedAt, expiresAt} to STDOUT

try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const util = require("util");
const { execSync, exec } = require("child_process");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

function must(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`Missing required env: ${name}`);
  return v;
}

// 🔁 DEFAULTS UPDATED TO ARBITRUM MAINNET
const FUNCTIONS_ROUTER =
  process.env.CHAINLINK_FUNCTIONS_ROUTER ||
  "0x97083E831F8F0638855e2A515c90EdCF158DF238"; // Arbitrum One Functions router
const DON_ID = process.env.DON_ID || "fun-arbitrum-mainnet-1";

const SLOT_ID = Number(process.env.SLOT_ID ?? 0);
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));
const TTL_SECONDS = TTL_MINUTES * 60;

// 🔁 MAINNET GATEWAYS (not testnet)
const GATEWAY_URLS = [
  "https://01.functions-gateway.chain.link/",
  "https://02.functions-gateway.chain.link/",
];

// --- helpers to normalize toolkit variations (no TS) ---
function to0xString(val) {
  if (val == null) return null;
  if (typeof val === "string") {
    if (/^0x[0-9a-fA-F]+$/.test(val)) return val;
    if (/^[0-9a-fA-F]+$/.test(val)) return "0x" + val;
    return "0x" + Buffer.from(val, "utf8").toString("hex");
  }
  if (val instanceof Uint8Array) return "0x" + Buffer.from(val).toString("hex");
  if (Buffer.isBuffer(val)) return "0x" + val.toString("hex");
  return null;
}
function extractHexDeep(enc, depth = 0) {
  if (depth > 5) return null;
  const direct = to0xString(enc);
  if (direct) return direct;
  if (enc && typeof enc === "object") {
    const fields = [
      "encryptedSecretsHexstring","encryptedSecretsHexString","encryptedSecretsHex",
      "encryptedSecrets","hexstring","hexString","hex","payload","data","value",
    ];
    for (const f of fields) if (f in enc) {
      const h = to0xString(enc[f]); if (h) return h;
    }
    for (const v of Object.values(enc)) {
      const h = extractHexDeep(v, depth + 1); if (h) return h;
    }
  }
  return null;
}

(async () => {
  // ===== Build secrets bag =====
  const secrets = { CANARY: `upload ${new Date().toISOString()}` };
  const put = (k, v) => { if (v && String(v).trim()) secrets[k] = v; };

  // carry *_API_KEY / *_ENDPOINT automatically
  for (const k of Object.keys(process.env)) {
    if (/_API_KEY$/i.test(k) || /_ENDPOINT$/i.test(k)) put(k, process.env[k]);
  }
  put("GOALSERVE_BASE_URL", process.env.GOALSERVE_BASE_URL);
  put("GOALSERVE_API_KEY", process.env.GOALSERVE_API_KEY);
  put("SCORES_PROVIDER", process.env.SCORES_PROVIDER || "goalserve");

  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.error("[UPLOAD] keys:", Object.keys(secrets).join(", ") || "(none)");
  console.error("[UPLOAD] sha256:", sha, "TTL_MINUTES:", TTL_MINUTES, "SLOT_ID:", SLOT_ID);
  console.error("[CHAINLINK]", { functionsRouter: FUNCTIONS_ROUTER, donId: DON_ID });

  // ===== Signer =====
  // 🔁 Prefer RPC_URL or ARBITRUM_RPC_URL (no more SEPOLIA-only fallback)
  const rpcUrl = process.env.RPC_URL || process.env.ARBITRUM_RPC_URL || must("RPC_URL");
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

  let version, slotId;

  // Path 1: direct upload
  if (typeof sm.uploadSecretsToDON === "function") {
    try {
      const resp = await sm.uploadSecretsToDON({
        secrets,
        secondsUntilExpiration: Math.max(300, Math.min(10080 * 60, TTL_SECONDS)),
        slotId: SLOT_ID,
      });
      version = resp && (resp.version ?? resp.secretsVersion ?? resp);
      slotId = resp && (resp.slotId ?? SLOT_ID);
      console.error("[PATH] Used uploadSecretsToDON");
    } catch (e) {
      console.warn("[PATH] uploadSecretsToDON failed, trying encrypt+gateway:", e && e.message || e);
    }
  }

  // Path 2: encrypt + gateway
  if (!version) {
    const enc = await sm.encryptSecrets(secrets);
    const maybeHex = extractHexDeep(enc);
    if (!maybeHex || maybeHex.length < 10) {
      console.error("[DEBUG] encryptSecrets() typeof:", typeof enc, enc && enc.constructor && enc.constructor.name);
      try { console.error("[DEBUG] keys:", enc && typeof enc === "object" ? Object.getOwnPropertyNames(enc) : null); } catch {}
      try { console.error("[DEBUG] preview:", util.inspect(enc, { depth: 2, maxArrayLength: 10 })); } catch {}
      throw new Error("Could not normalize encrypted payload from encryptSecrets()");
    }
    const resp = await sm.uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring: maybeHex,
      gatewayUrls: GATEWAY_URLS,
      minutesUntilExpiration: TTL_MINUTES,
      slotId: SLOT_ID,
    });
    version = resp && (resp.version ?? resp.secretsVersion ?? resp);
    slotId = resp && (resp.slotId ?? SLOT_ID);
    console.error("[PATH] Used encryptSecrets + uploadEncryptedSecretsToDON");
  }

  const expiresAt = new Date(Date.now() + TTL_SECONDS * 1000).toISOString();
  const record = {
    secretsVersion: Number(version),
    slotId: Number(slotId || SLOT_ID),
    donId: DON_ID,
    uploadedAt: new Date().toISOString(),
    expiresAt,
    canary: secrets.CANARY,
  };

  // ===== Atomic write =====
  const outPath = path.join(process.cwd(), "activeSecrets.json");
  const tmpPath = outPath + ".tmp";
  fs.writeFileSync(tmpPath, JSON.stringify(record, null, 2));
  fs.renameSync(tmpPath, outPath);
  console.error("Wrote", outPath, "=>", JSON.stringify(record));

  // ===== Commit & push to GitHub =====
  const repoDir = process.cwd();
  const branch = process.env.GIT_BRANCH || "main";
  const GH_PAT = process.env.GH_PAT || "";

  // If using HTTPS remote and PAT, inject temporarily
  try {
    const origin = execSync("git remote get-url origin", { cwd: repoDir, encoding: "utf8" }).trim();
    if (GH_PAT && /^https:\/\/github\.com\/.+\/.+\.git$/i.test(origin) && !origin.includes("@")) {
      const withToken = origin.replace(/^https:\/\//i, `https://${GH_PAT}@`);
      execSync(`git remote set-url origin "${withToken}"`, { cwd: repoDir });
    }
  } catch {}

  // Set identity (no-op if already configured)
  try {
    const email = process.env.GIT_AUTHOR_EMAIL || "runner@blockpools.io";
    const name  = process.env.GIT_AUTHOR_NAME  || "BlockPools Bot";
    execSync(`git config user.email "${email}"`, { cwd: repoDir });
    execSync(`git config user.name "${name}"`,  { cwd: repoDir });
  } catch {}

  // Pull --rebase (avoid non-fast-forward)
  try { execSync("git pull --rebase -q", { cwd: repoDir, stdio: "inherit" }); } catch {}

  // Stage, commit if changed, push
  try { execSync("git add activeSecrets.json", { cwd: repoDir, stdio: "inherit" }); } catch {}
  try {
    // commit only if there are staged changes
    execSync(`bash -lc 'git diff --cached --quiet || git
 commit -m "chore(bot): update active secrets to ${record.secretsVersion}"'`, { cwd: repoDir, stdio: "inherit" });
  } catch {}
  try { execSync(`git push -q origin ${branch}`, { cwd: repoDir, stdio: "inherit" }); } catch {}

  // Restore clean origin URL if we injected a PAT
  if (GH_PAT) {
    try {
      const current = execSync("git remote get-url origin", { cwd: repoDir, encoding: "utf8" }).trim();
      const clean = current.replace(`https://${GH_PAT}@`, "https://");
      if (clean) execSync(`git remote set-url origin "${clean}"`, { cwd: repoDir });
    } catch {}
  }

  console.error(`[OK] Uploaded secrets => ${record.secretsVersion} and pushed activeSecrets.json`);

  // ===== Print clean JSON pointer for the bot (ONLY STDOUT) =====
  process.stdout.write(JSON.stringify({
    donId: record.donId,
    secretsVersion: record.secretsVersion,
    uploadedAt: record.uploadedAt,
    expiresAt: record.expiresAt
  }));
})().catch((e) => {
  console.error("Upload failed:", e && (e.stack || e.message) || e);
  process.exit(1);
});
