try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

const FUNCTIONS_ROUTER = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0";
const DON_ID = "fun-ethereum-sepolia-1";
const SLOT_ID = 0;

// TTL: default 24h, editable via DON_TTL_MINUTES (min 5, max 10080)
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));
const TTL_SECONDS = TTL_MINUTES * 60;

// Gateways for old toolkit fallback
const GATEWAY_URLS = [
  "https://01.functions-gateway.testnet.chain.link/",
  "https://02.functions-gateway.testnet.chain.link/"
];

function must(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`❌ Missing required env: ${name}`);
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
  const secrets = {
    MLB_API_KEY: must("MLB_API_KEY"),
    NFL_API_KEY: must("NFL_API_KEY"),
    MLB_ENDPOINT: must("MLB_ENDPOINT"),
    NFL_ENDPOINT: must("NFL_ENDPOINT"),
    CANARY: `gh-actions-${new Date().toISOString()}`
  };

  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("🔐 Building DON-hosted payload (redacted). keys:", Object.keys(secrets).join(", "));
  console.log("   payload sha256:", sha, "TTL_MINUTES:", TTL_MINUTES);

  const provider = new ethers.providers.JsonRpcProvider(must("SEPOLIA_RPC_URL"));
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

  let version, slotId;

  // Newer toolkit (seconds)
  if (typeof sm.uploadSecretsToDON === "function") {
    try {
      ({ version, slotId } = await sm.uploadSecretsToDON({
        secrets,
        secondsUntilExpiration: Math.max(300, Math.min(10080 * 60, TTL_SECONDS)),
        slotId: SLOT_ID
      }));
      console.log("ℹ️ Used uploadSecretsToDON");
    } catch (e) {
      console.warn("⚠️ uploadSecretsToDON threw, falling back:", e.message || e);
    }
  }

  // Old 0.3.x fallback (minutes + gatewayUrls)
  if (!version && typeof sm.encryptSecrets === "function" && typeof sm.uploadEncryptedSecretsToDON === "function") {
    const enc = await sm.encryptSecrets(secrets);
    const encryptedSecretsHexstring = to0xHex(enc);
    if (!encryptedSecretsHexstring) throw new Error("encryptSecrets() did not return a valid hex payload");

    ({ version, slotId } = await sm.uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring,
      gatewayUrls: GATEWAY_URLS,
      minutesUntilExpiration: TTL_MINUTES,
      slotId: SLOT_ID
    }));
    console.log("ℹ️ Used uploadEncryptedSecretsToDON (fallback, 0.3.x)");
  }

  if (!version) throw new Error("No compatible DON-hosted upload method found on this toolkit build.");

  // Write pointer to repo root:
  const OUT = path.resolve(__dirname, "../../activeSecrets.json");
  fs.writeFileSync(
    OUT,
    JSON.stringify(
      { secretsVersion: Number(version), slotId, donId: DON_ID, uploadedAt: new Date().toISOString(), canary: secrets.CANARY },
      null,
      2
    )
  );

  console.log("✅ DON-hosted secrets uploaded:", { version: Number(version), slotId });
  console.log("📝 Wrote", OUT);
})().catch((e) => {
  console.error("❌ Upload failed:", e);
  process.exit(1);
});
