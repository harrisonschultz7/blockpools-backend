try { require("dotenv").config(); } catch (_) {}
const { readFileSync, writeFileSync } = require("fs");
const path = require("path");
const { FunctionsDonUtils } = require("./utils/don-helpers.js");

config();

async function uploadSecrets() {
  const privateKey = process.env.PRIVATE_KEY;
  const rpcUrl = process.env.SEPOLIA_RPC_URL;
  const routerAddress = "0xb83E47C2bC239B3bf370bc41e1459A34b41238D0";
  const donId = "fun-ethereum-sepolia-1";

  if (!privateKey || !rpcUrl || !routerAddress) {
    throw new Error("❌ Missing environment variables");
  }

  const donUtils = new FunctionsDonUtils({
    signer: { privateKey, providerUrl: rpcUrl },
    functionsRouterAddress: routerAddress,
    donId,
  });

  await donUtils.initialize();

  const secrets = JSON.parse(readFileSync("secrets/secrets.json", "utf8"));
  const encryptedSecretsHexstring = await donUtils.encryptSecrets(secrets);

  const { encryptedSecretsReference } = await donUtils.uploadEncryptedSecrets({
    encryptedSecretsHexstring,
  });

  console.log("✅ Secrets uploaded successfully!");
  console.log("📦 Reference:", encryptedSecretsReference);

  const outputPath = path.join("secrets", "activeSecrets.json");
  const data = {
    secretsVersion: encryptedSecretsReference.version,
    donId,
    updatedAt: new Date().toISOString(),
  };

  writeFileSync(outputPath, JSON.stringify(data, null, 2));
  console.log(`📝 Updated ${outputPath}`);
}

uploadSecrets().catch((err) => {
  console.error("❌ Upload failed:", err);
  process.exit(1);
});
