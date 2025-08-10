// upload-secrets.js — resilient DON-hosted uploader with auto-fallback
try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const crypto = require("crypto");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

// Ethereum Sepolia
const FUNCTIONS_ROUTER = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0";
const DON_ID = "fun-ethereum-sepolia-1";
const SLOT_ID = 0;
const TTL_SECONDS = 14 * 24 * 60 * 60; // 14 days

function must(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`❌ Missing required env: ${name}`);
  return v;
}

(async () => {
  // 1) Build secrets from GH Actions env (no repo file)
  const secrets = {
    MLB_API_KEY: must("MLB_API_KEY"),
    NFL_API_KEY: must("NFL_API_KEY"),
    MLB_ENDPOINT: must("MLB_ENDPOINT"),
    NFL_ENDPOINT: must("NFL_ENDPOINT"),
    CANARY: `gh-actions-${new Date().toISOString()}`,
  };

  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("🔐 Building DON-hosted payload (redacted). keys:", Object.keys(secrets).join(", "));
  console.log("   payload sha256:", sha);

  // 2) Init signer + SecretsManager
  const provider = new ethers.providers.JsonRpcProvider(must("SEPOLIA_RPC_URL"));
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({
    signer,
    functionsRouterAddress: FUNCTIONS_ROUTER,
    donId: DON_ID,
  });
  await sm.initialize();

  // 3) Upload to DON-hosted (try modern API, then older encrypted-to-DON API)
  let version, slotId;

  if (typeof sm.uploadSecretsToDON === "function") {
    try {
      ({ version, slotId } = await sm.uploadSecretsToDON({
        secrets,
        secondsUntilExpiration: TTL_SECONDS,
        slotId: SLOT_ID,
      }));
      console.log("ℹ️ Used uploadSecretsToDON");
    } catch (e) {
      console.warn("⚠️ uploadSecretsToDON threw, will fall back:", e.message || e);
    }
  }

  if (!version && typeof sm.uploadEncryptedSecretsToDON === "function") {
    const encryptedSecretsHexstring = await sm.encryptSecrets(secrets);
    ({ version, slotId } = await sm.uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring,
      secondsUntilExpiration: TTL_SECONDS,
      slotId: SLOT_ID,
    }));
    console.log("ℹ️ Used uploadEncryptedSecretsToDON (fallback)");
  }

  if (!version) {
    throw new Error(
      "No compatible DON-hosted upload method found on this toolkit build."
    );
  }

  console.log("✅ DON-hosted secrets uploaded:", { version: Number(version), slotId });

  // 4) Persist short version pointer for send-request.js
  fs.writeFileSync(
    "activeSecrets.json",
    JSON.stringify(
      {
        secretsVersion: Number(version),
        slotId,
        donId: DON_ID,
        uploadedAt: new Date().toISOString(),
        canary: secrets.CANARY,
      },
      null,
      2
    )
  );
  console.log("📝 Wrote activeSecrets.json");
})().catch((e) => {
  console.error("❌ Upload failed:", e);
  process.exit(1);
});
