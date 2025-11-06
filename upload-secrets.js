// upload-secrets.js — Lean legacy DON-hosted uploader (Ethereum Sepolia, writes ./activeSecrets.json)
try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

/* =======================
 * Network / Router config
 * ======================= */
// Ethereum Sepolia
const FUNCTIONS_ROUTER = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0";
const DON_ID = "fun-ethereum-sepolia-1";

// Keep a stable pointer with slotId if you want (0 is fine)
const SLOT_ID = Number(process.env.SLOT_ID ?? 0);

// TTL: default 24h
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));

// Two public test gateways
const GATEWAY_URLS = [
  "https://01.functions-gateway.testnet.chain.link/",
  "https://02.functions-gateway.testnet.chain.link/",
];

function must(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`Missing required env: ${name}`);
  return v;
}

(async () => {
  // Build the secrets bag (only set non-empty values)
  const secrets = { CANARY: `legacy-upload ${new Date().toISOString()}` };
  const put = (k, v) => { if (v && String(v).trim()) secrets[k] = v; };

  // Goalserve knobs
  put("GOALSERVE_BASE_URL", process.env.GOALSERVE_BASE_URL);
  put("GOALSERVE_AUTH", process.env.GOALSERVE_AUTH);          // "path" | "header"
  put("GOALSERVE_DATE_FMT", process.env.GOALSERVE_DATE_FMT);  // "DMY" | "ISO"
  put("GOALSERVE_API_KEY", process.env.GOALSERVE_API_KEY);    // optional / header mode

  // Also sweep *_API_KEY and *_ENDPOINT envs
  for (const k of Object.keys(process.env)) {
    if (/_API_KEY$/i.test(k) || /_ENDPOINT$/i.test(k)) {
      put(k, process.env[k]);
    }
  }

  // Log fingerprint only (no values)
  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("[UPLOAD] keys:", Object.keys(secrets).join(", "));
  console.log("[UPLOAD] sha256:", sha, "TTL_MINUTES:", TTL_MINUTES, "SLOT_ID:", SLOT_ID);
  console.log("[CHAINLINK]", { functionsRouter: FUNCTIONS_ROUTER, donId: DON_ID });

  // Signer
  const rpcUrl = must("SEPOLIA_RPC_URL");
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

  // Legacy path only: encrypt -> upload via gateway (keeps deps light)
  const enc = await sm.encryptSecrets(secrets);
  const encryptedSecretsHexstring = (() => {
    if (typeof enc === "string" && /^0x[0-9a-fA-F]*$/.test(enc)) return enc;
    if (typeof enc === "string") return "0x" + Buffer.from(enc, "utf8").toString("hex");
    if (enc instanceof Uint8Array) return "0x" + Buffer.from(enc).toString("hex");
    if (enc && typeof enc === "object" && typeof enc.encryptedSecretsHexstring === "string") {
      const s = enc.encryptedSecretsHexstring;
      return /^0x/.test(s) ? s : "0x" + s;
    }
    throw new Error("encryptSecrets() did not return a valid hex payload");
  })();

  const { version, slotId } = await sm.uploadEncryptedSecretsToDON({
    encryptedSecretsHexstring,
    gatewayUrls: GATEWAY_URLS,
    minutesUntilExpiration: TTL_MINUTES,
    slotId: SLOT_ID,
  });

  console.log("DON-hosted secrets uploaded:", { version: Number(version), slotId });

  // Write pointer at repo root
  const outPath = path.resolve(__dirname, "activeSecrets.json");
  fs.writeFileSync(
    outPath,
    JSON.stringify(
      {
        secretsVersion: Number(version),
        slotId: Number(slotId ?? SLOT_ID),
        donId: DON_ID,
        uploadedAt: new Date().toISOString(),
        canary: secrets.CANARY
      },
      null,
      2
    )
  );
  console.log("Wrote", outPath);
})().catch((e) => {
  console.error("Upload failed:", e?.stack || e);
  process.exit(1);
});
