// upload-secrets.js — robust DON secrets uploader (Ethereum Sepolia)
// Tries direct uploadSecretsToDON first; falls back to encrypt+gateway (0.3.x)
try { require("dotenv").config(); } catch (_) {}

const fs = require("fs");
const path = require("path");
const crypto = require("crypto");
const util = require("util");
const { ethers } = require("ethers");
const { SecretsManager } = require("@chainlink/functions-toolkit");

const FUNCTIONS_ROUTER = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0"; // Ethereum Sepolia
const DON_ID = "fun-ethereum-sepolia-1";

const SLOT_ID = Number(process.env.SLOT_ID ?? 0);
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));
const TTL_SECONDS = TTL_MINUTES * 60;

const GATEWAY_URLS = [
  "https://01.functions-gateway.testnet.chain.link/",
  "https://02.functions-gateway.testnet.chain.link/",
];

function must(name) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`Missing required env: ${name}`);
  return v;
}

function to0xString(val) {
  if (val == null) return null;
  if (typeof val === "string") {
    if (/^0x[0-9a-fA-F]+$/.test(val)) return val;
    if (/^[0-9a-fA-F]+$/.test(val)) return "0x" + val;
    // as a last resort, utf8->hex (legacy shapes)
    return "0x" + Buffer.from(val, "utf8").toString("hex");
  }
  if (val instanceof Uint8Array) return "0x" + Buffer.from(val).toString("hex");
  if (Buffer.isBuffer(val)) return "0x" + val.toString("hex");
  return null;
}

// Very permissive deep extractor for 0.3.x variations
function extractHexDeep(enc, depth = 0) {
  if (depth > 5) return null;
  const direct = to0xString(enc);
  if (direct) return direct;

  if (enc && typeof enc === "object") {
    const fields = [
      "encryptedSecretsHexstring",
      "encryptedSecretsHexString",
      "encryptedSecretsHex",
      "encryptedSecrets",
      "hexstring",
      "hexString",
      "hex",
      "payload",
      "data",
      "value",
    ];
    for (const f of fields) {
      if (f in enc) {
        const h = to0xString(enc[f]);
        if (h) return h;
      }
    }
    for (const v of Object.values(enc)) {
      const h = extractHexDeep(v, depth + 1);
      if (h) return h;
    }
  }
  return null;
}

(async () => {
  // Build the secrets bag
  const secrets = { CANARY: `upload ${new Date().toISOString()}` };
  const put = (k, v) => { if (v && String(v).trim()) secrets[k] = v; };

  put("GOALSERVE_BASE_URL", process.env.GOALSERVE_BASE_URL);
  put("GOALSERVE_AUTH", process.env.GOALSERVE_AUTH);
  put("GOALSERVE_DATE_FMT", process.env.GOALSERVE_DATE_FMT);
  put("GOALSERVE_API_KEY", process.env.GOALSERVE_API_KEY);

  // Sweep *_API_KEY / *_ENDPOINT extras
  for (const k of Object.keys(process.env)) {
    if (/_API_KEY$/i.test(k) || /_ENDPOINT$/i.test(k)) put(k, process.env[k]);
  }

  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("[UPLOAD] keys:", Object.keys(secrets).join(", ") || "(none)");
  console.log("[UPLOAD] sha256:", sha, "TTL_MINUTES:", TTL_MINUTES, "SLOT_ID:", SLOT_ID);
  console.log("[CHAINLINK]", { functionsRouter: FUNCTIONS_ROUTER, donId: DON_ID });

  // Signer
  const rpcUrl = must("SEPOLIA_RPC_URL");
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

  let version, slotId;

  // 1) Try direct DON-hosted (present on some toolkit builds)
  if (typeof sm.uploadSecretsToDON === "function") {
    try {
      ({ version, slotId } = await sm.uploadSecretsToDON({
        secrets,
        secondsUntilExpiration: Math.max(300, Math.min(10080 * 60, TTL_SECONDS)),
        slotId: SLOT_ID,
      }));
      console.log("[PATH] Used uploadSecretsToDON");
    } catch (e) {
      console.warn("[PATH] uploadSecretsToDON failed, will try encrypt+gateway:", e.message || e);
    }
  }

  // 2) Fallback: encrypt -> upload via gateway (0.3.x)
  if (!version) {
    const enc = await sm.encryptSecrets(secrets);
    const maybeHex = extractHexDeep(enc);

    if (!maybeHex || maybeHex.length < 10) {
      console.log("[DEBUG] encryptSecrets() typeof:", typeof enc, enc && enc.constructor && enc.constructor.name);
      try { console.log("[DEBUG] keys:", enc && typeof enc === "object" ? Object.getOwnPropertyNames(enc) : null); } catch (_) {}
      try { console.log("[DEBUG] preview:", util.inspect(enc, { depth: 2, maxArrayLength: 10 })); } catch (_) {}
      throw new Error("Could not normalize encrypted payload from encryptSecrets()");
    }

    ({ version, slotId } = await sm.uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring: maybeHex,
      gatewayUrls: GATEWAY_URLS,
      minutesUntilExpiration: TTL_MINUTES,
      slotId: SLOT_ID,
    }));
    console.log("[PATH] Used encryptSecrets + uploadEncryptedSecretsToDON");
  }

  console.log("DON-hosted secrets uploaded:", { version: Number(version), slotId });

  const outPath = path.resolve(__dirname, "activeSecrets.json");
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
  console.error("Upload failed:", e?.stack || e);
  process.exit(1);
});
