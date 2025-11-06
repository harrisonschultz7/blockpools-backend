// upload-secrets.js — DON-hosted uploader (24h TTL, old/new toolkit compatible)
try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

/* =======================
 * Network / Router config
 * ======================= */
const FUNCTIONS_ROUTER = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0"; // Ethereum Sepolia router
const DON_ID = "fun-ethereum-sepolia-1"; // DON ID

// Slot ID lets you update an existing slot to keep a stable pointer
const SLOT_ID = Number(process.env.SLOT_ID ?? 0);

// TTL (5 min .. 7 days typical on test envs). Default: 1440 min (24h).
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));
const TTL_SECONDS = TTL_MINUTES * 60;

// Fallback gateway URLs for older toolkit upload path
const GATEWAY_URLS = [
  "https://01.functions-gateway.testnet.chain.link/",
  "https://02.functions-gateway.testnet.chain.link/",
];

/* ==============
 * Helper funcs
 * ============== */
function must(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`Missing required env: ${name}`);
  return v;
}

function to0xHex(maybe) {
  if (!maybe) return null;
  if (typeof maybe === "string") {
    if (/^0x[0-9a-fA-F]*$/.test(maybe)) return maybe;
    if (/^[0-9a-fA-F]+$/.test(maybe)) return "0x" + maybe;
    return null;
  }
  if (typeof maybe === "object") {
    if (typeof maybe.encryptedSecretsHexstring === "string") return to0xHex(maybe.encryptedSecretsHexstring);
    if (typeof maybe.encryptedSecrets === "string") return to0xHex(maybe.encryptedSecrets);
    if (maybe instanceof Uint8Array) return "0x" + Buffer.from(maybe).toString("hex");
  }
  return null;
}

/* ==========================
 * Build the secrets payload
 * ========================== */
(async () => {
  // Canary marker
  const secrets = { CANARY: `vps-upload ${new Date().toISOString()}` };

  // 1) Explicit Goalserve knobs your source.js now supports
  //    Recommended for your current “key-in-path + DMY” setup:
  //    GOALSERVE_BASE_URL=https://www.goalserve.com/getfeed/<KEY>
  //    GOALSERVE_AUTH=path
  //    GOALSERVE_DATE_FMT=DMY
  //    (optional) GOALSERVE_API_KEY=<KEY>
  const GOALSERVE = {
    GOALSERVE_BASE_URL: process.env.GOALSERVE_BASE_URL, // e.g., https://www.goalserve.com/getfeed/XXXXXX
    GOALSERVE_AUTH: process.env.GOALSERVE_AUTH,         // "path" | "header"
    GOALSERVE_DATE_FMT: process.env.GOALSERVE_DATE_FMT, // "DMY" | "ISO"
    GOALSERVE_API_KEY: process.env.GOALSERVE_API_KEY,   // optional (header mode)
  };

  // Warn loudly if your current recommended trio is not present
  console.log("[GOALSERVE] will upload:");
  Object.entries(GOALSERVE).forEach(([k, v]) => {
    const present = v && String(v).trim() ? `present (len=${String(v).length})` : "MISSING/empty";
    console.log(`  ${k}: ${present}`);
  });

  // Apply only those that are set (empty ones are skipped)
  for (const [k, v] of Object.entries(GOALSERVE)) {
    if (v && String(v).trim()) secrets[k] = v;
  }

  // 2) Auto-collect any *_API_KEY and *_ENDPOINT from your env (harmless extras)
  const apiKeyNames = Object.keys(process.env).filter((k) => /_API_KEY$/i.test(k));
  const endpointNames = Object.keys(process.env).filter((k) => /_ENDPOINT$/i.test(k));
  console.log("Discovered *_API_KEY:", apiKeyNames);
  console.log("Discovered *_ENDPOINT:", endpointNames);

  for (const k of apiKeyNames) {
    const v = process.env[k];
    if (v && String(v).trim()) secrets[k] = v;
  }
  for (const k of endpointNames) {
    const v = process.env[k];
    if (v && String(v).trim()) secrets[k] = v;
  }

  // 3) Log the payload fingerprint
  const payloadKeys = Object.keys(secrets);
  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("Uploading DON-hosted payload with keys:", payloadKeys.join(", "));
  console.log("payload sha256:", sha, "TTL_MINUTES:", TTL_MINUTES, "SLOT_ID:", SLOT_ID);

  /* =========================
   * Signer / RPC connection
   * ========================= */
  const rpcUrl = process.env.ARBITRUM_SEPOLIA_RPC_URL || must("SEPOLIA_RPC_URL");
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

  /* =========================
   * Upload to DON (v0.4+ / v0.3)
   * ========================= */
  let version, slotId;

  // Preferred (newer) path
  if (typeof sm.uploadSecretsToDON === "function") {
    try {
      ({ version, slotId } = await sm.uploadSecretsToDON({
        secrets,
        secondsUntilExpiration: Math.max(300, Math.min(10080 * 60, TTL_SECONDS)),
        slotId: SLOT_ID,
      }));
      console.log("Used uploadSecretsToDON");
    } catch (e) {
      console.warn("uploadSecretsToDON failed, falling back:", e.message || e);
    }
  }

  // Back-compat path (toolkit 0.3.x)
  if (!version && typeof sm.encryptSecrets === "function" && typeof sm.uploadEncryptedSecretsToDON === "function") {
    const enc = await sm.encryptSecrets(secrets);
    const encryptedSecretsHexstring = to0xHex(enc);
    if (!encryptedSecretsHexstring) throw new Error("encryptSecrets() did not return a valid hex payload");
    ({ version, slotId } = await sm.uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring,
      gatewayUrls: GATEWAY_URLS,
      minutesUntilExpiration: TTL_MINUTES,
      slotId: SLOT_ID,
    }));
    console.log("Used uploadEncryptedSecretsToDON (fallback, 0.3.x)");
  }

  if (!version) throw new Error("No compatible DON-hosted upload method found on this toolkit build.");

  console.log("DON-hosted secrets uploaded:", { version: Number(version), slotId });

  // Write activeSecrets.json in repo root (so send-request can read it)
  const outPath = path.resolve(__dirname, "../../activeSecrets.json");
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        secretsVersion: Number(version),
        slotId: Number(slotId ?? SLOT_ID),
        donId: DON_ID,
        uploadedAt: new Date().toISOString(),
        canary: secrets.CANARY,
      },
      null,
      2
    )
  );
  console.log("Wrote", outPath);
})().catch((e) => {
  console.error("Upload failed:", e);
  process.exit(1);
});
