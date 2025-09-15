// upload-secrets.js — DON-hosted uploader (24h TTL, old/new toolkit compatible)
try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

// --- Network/Router (Ethereum Sepolia)
const FUNCTIONS_ROUTER = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0";
const DON_ID = "fun-ethereum-sepolia-1";

// IMPORTANT: keep this in sync with your send script (or read from activeSecrets.json there).
const SLOT_ID = Number(process.env.SLOT_ID ?? 0);

// TTL (5 min .. 7 days typical on test envs)
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));
const TTL_SECONDS = TTL_MINUTES * 60;

// Fallback path for older toolkit
const GATEWAY_URLS = [
  "https://01.functions-gateway.testnet.chain.link/",
  "https://02.functions-gateway.testnet.chain.link/",
];

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

(async () => {
  // --- Deep debug: show what REALLY exists in process.env for these keys
  const leagueKeys = ["MLB_API_KEY","NFL_API_KEY","NBA_API_KEY","NHL_API_KEY","EPL_API_KEY","UCL_API_KEY"];
  console.log("Env presence (process.env) for *_API_KEY:");
  for (const k of leagueKeys) {
    const val = process.env[k];
    console.log(
      `  ${k}:`,
      val && String(val).trim() ? `present (len=${String(val).length})` : "MISSING/empty"
    );
  }

  // Collect secrets dynamically
  const secrets = { CANARY: `gh-actions-${new Date().toISOString()}` };

  // Pick up every *_API_KEY present in env
  const apiKeyNames = Object.keys(process.env).filter(k => /_API_KEY$/i.test(k));
  // Also log the names we see in Node (not just in the workflow echo)
  console.log("Discovered *_API_KEY names in process.env:", apiKeyNames);

  for (const k of apiKeyNames) {
    const v = process.env[k];
    if (v && String(v).trim()) secrets[k] = v;
  }

  // Optionally upload *_ENDPOINT too (harmless if unused)
  const endpointNames = Object.keys(process.env).filter(k => /_ENDPOINT$/i.test(k));
  console.log("Discovered *_ENDPOINT names in process.env:", endpointNames);
  for (const k of endpointNames) {
    const v = process.env[k];
    if (v && String(v).trim()) secrets[k] = v;
  }

  // Build + log the payload keys we will upload
  const payloadKeys = Object.keys(secrets);
  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("Building DON-hosted payload. keys:", payloadKeys.join(", "));
  console.log("payload sha256:", sha, "TTL_MINUTES:", TTL_MINUTES, "SLOT_ID:", SLOT_ID);

  // Signer & RPC (prefer Arbitrum var if present later; you’re on Sepolia today)
  const rpcUrl = process.env.ARBITRUM_SEPOLIA_RPC_URL || must("SEPOLIA_RPC_URL");
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

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

  // Back-compat path
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

  // Always write to REPO ROOT so the sender picks up the latest pointer
  const outPath = path.resolve(__dirname, "../../activeSecrets.json");
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      { secretsVersion: Number(version), slotId: Number(slotId ?? SLOT_ID), donId: DON_ID, uploadedAt: new Date().toISOString(), canary: secrets.CANARY },
      null,
      2
    )
  );
  console.log("Wrote", outPath);
})().catch((e) => {
  console.error("Upload failed:", e);
  process.exit(1);
});
