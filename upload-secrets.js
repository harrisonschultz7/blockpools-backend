// bots/upload-secrets.ts — robust DON secrets uploader (Ethereum Sepolia)
// 1) Uploads secrets to DON (tries direct -> falls back to encrypt+gateway)
// 2) Writes activeSecrets.json to REPO ROOT
// 3) Commits & pushes it (auditable trail)

try { require("dotenv").config(); } catch (_) {}

import fs from "fs";
import path from "path";
import crypto from "crypto";
import util from "util";
import { exec as _exec } from "child_process";
import { ethers } from "ethers";
import { SecretsManager } from "@chainlink/functions-toolkit";

const exec = (cmd: string) =>
  new Promise<void>((res, rej) =>
    _exec(cmd, (err, stdout, stderr) => {
      if (stdout) process.stdout.write(stdout);
      if (stderr) process.stderr.write(stderr);
      err ? rej(err) : res();
    })
  );

function must(name: string) {
  const v = process.env[name];
  if (!v || !String(v).trim()) throw new Error(`Missing required env: ${name}`);
  return v;
}

const FUNCTIONS_ROUTER = process.env.CHAINLINK_FUNCTIONS_ROUTER || "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0"; // Sepolia
const DON_ID = process.env.DON_ID || "fun-ethereum-sepolia-1";

const SLOT_ID = Number(process.env.SLOT_ID ?? 0);
const TTL_MINUTES = Math.max(5, Math.min(10080, Number(process.env.DON_TTL_MINUTES || 1440)));
const TTL_SECONDS = TTL_MINUTES * 60;

const GATEWAY_URLS = [
  "https://01.functions-gateway.testnet.chain.link/",
  "https://02.functions-gateway.testnet.chain.link/",
];

// Normalizers for 0.3.x variations
function to0xString(val: any) {
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
function extractHexDeep(enc: any, depth = 0): string | null {
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
  // Build secrets bag
  const secrets: Record<string, string> = { CANARY: `upload ${new Date().toISOString()}` };
  const put = (k: string, v?: string) => { if (v && String(v).trim()) secrets[k] = v; };

  // Carry common provider keys
  for (const k of Object.keys(process.env)) {
    if (/_API_KEY$/i.test(k) || /_ENDPOINT$/i.test(k)) put(k, process.env[k]!);
  }
  put("GOALSERVE_BASE_URL", process.env.GOALSERVE_BASE_URL);
  put("GOALSERVE_API_KEY", process.env.GOALSERVE_API_KEY);
  put("SCORES_PROVIDER", process.env.SCORES_PROVIDER || "goalserve");

  const sha = crypto.createHash("sha256").update(JSON.stringify(secrets)).digest("hex");
  console.log("[UPLOAD] keys:", Object.keys(secrets).join(", ") || "(none)");
  console.log("[UPLOAD] sha256:", sha, "TTL_MINUTES:", TTL_MINUTES, "SLOT_ID:", SLOT_ID);
  console.log("[CHAINLINK]", { functionsRouter: FUNCTIONS_ROUTER, donId: DON_ID });

  // Signer (prefer RPC_URL, fallback SEPOLIA_RPC_URL)
  const rpcUrl = process.env.RPC_URL || must("SEPOLIA_RPC_URL");
  const provider = new ethers.providers.JsonRpcProvider(rpcUrl);
  const signer = new ethers.Wallet(must("PRIVATE_KEY"), provider);

  const sm = new SecretsManager({ signer, functionsRouterAddress: FUNCTIONS_ROUTER, donId: DON_ID });
  await sm.initialize();

  let version: any, slotId: any;

  // Path 1: direct
  if (typeof (sm as any).uploadSecretsToDON === "function") {
    try {
      ({ version, slotId } = await (sm as any).uploadSecretsToDON({
        secrets,
        secondsUntilExpiration: Math.max(300, Math.min(10080 * 60, TTL_SECONDS)),
        slotId: SLOT_ID,
      }));
      console.log("[PATH] Used uploadSecretsToDON");
    } catch (e: any) {
      console.warn("[PATH] uploadSecretsToDON failed, will try encrypt+gateway:", e?.message || e);
    }
  }

  // Path 2: encrypt + gateway
  if (!version) {
    const enc = await sm.encryptSecrets(secrets);
    const maybeHex = extractHexDeep(enc);
    if (!maybeHex || maybeHex.length < 10) {
      console.log("[DEBUG] encryptSecrets() typeof:", typeof enc, enc && (enc as any).constructor?.name);
      try { console.log("[DEBUG] keys:", enc && typeof enc === "object" ? Object.getOwnPropertyNames(enc) : null); } catch {}
      try { console.log("[DEBUG] preview:", util.inspect(enc, { depth: 2, maxArrayLength: 10 })); } catch {}
      throw new Error("Could not normalize encrypted payload from encryptSecrets()");
    }
    ({ version, slotId } = await (sm as any).uploadEncryptedSecretsToDON({
      encryptedSecretsHexstring: maybeHex,
      gatewayUrls: GATEWAY_URLS,
      minutesUntilExpiration: TTL_MINUTES,
      slotId: SLOT_ID,
    }));
    console.log("[PATH] Used encryptSecrets + uploadEncryptedSecretsToDON");
  }

  const record = {
    secretsVersion: Number(version),
    slotId: Number(slotId ?? SLOT_ID),
    donId: DON_ID,
    uploadedAt: new Date().toISOString(),
    canary: secrets.CANARY,
  };

  // WRITE TO REPO ROOT
  const outPath = path.join(process.cwd(), "activeSecrets.json");
  fs.writeFileSync(outPath, JSON.stringify(record, null, 2));
  console.log("Wrote", outPath, "=>", JSON.stringify(record));

  // COMMIT & PUSH (requires SSH write access configured)
  const branch = process.env.GIT_BRANCH || "main";
  await exec(`git status --porcelain`);
  await exec(`git add activeSecrets.json`);
  try { await exec(`git commit -m "chore(bot): update activeSecrets to ${record.secretsVersion}"`); } catch { /* nothing to commit */ }
  await exec(`git push origin ${branch}`);

  console.log(`[OK] Uploaded secrets => ${record.secretsVersion} and pushed activeSecrets.json`);
})().catch((e) => {
  console.error("Upload failed:", e?.stack || e);
  process.exit(1);
});
