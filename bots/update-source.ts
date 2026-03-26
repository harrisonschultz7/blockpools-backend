// scripts/update-source.ts
// Updates the on-chain `source` string in SettlementCoordinator
// Usage: ts-node update-source.ts   (or npx tsx update-source.ts)
//
// Required env:
//   RPC_URL
//   PRIVATE_KEY
//   SETTLEMENT_COORDINATOR_ADDRESS
//   SOURCE_PATH  (optional, defaults to ./source.js relative to this script)

try { require("dotenv").config(); } catch {}

import fs from "fs";
import path from "path";
import { ethers } from "ethers";

const RPC_URL = process.env.RPC_URL!;
const PRIVATE_KEY = process.env.PRIVATE_KEY!;
const COORDINATOR = (process.env.SETTLEMENT_COORDINATOR_ADDRESS || "").trim();
const SOURCE_PATH = process.env.SOURCE_PATH || path.resolve(__dirname, "source.js");

const ABI = ["function setSource(string calldata _source) external"];

async function main() {
  if (!RPC_URL || !PRIVATE_KEY) throw new Error("Missing RPC_URL or PRIVATE_KEY");
  if (!ethers.isAddress(COORDINATOR)) throw new Error("Missing/invalid SETTLEMENT_COORDINATOR_ADDRESS");
  if (!fs.existsSync(SOURCE_PATH)) throw new Error(`source.js not found at: ${SOURCE_PATH}`);

  const source = fs.readFileSync(SOURCE_PATH, "utf8");
  console.log(`[INFO] Source loaded: ${source.length} chars from ${SOURCE_PATH}`);

  const provider = new ethers.JsonRpcProvider(RPC_URL);
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);
  const coordinator = new ethers.Contract(COORDINATOR, ABI, wallet);

  console.log(`[INFO] Calling setSource on ${COORDINATOR}...`);
  const tx = await coordinator.setSource(source);
  console.log(`[TX]   ${tx.hash}`);

  const receipt = await tx.wait(1);
  if (receipt.status !== 1) throw new Error("setSource tx failed");

  console.log(`[OK]   Source updated successfully in block ${receipt.blockNumber}`);
  console.log(`       MLB (and all other leagues) will now work on next Functions request.`);
}

main().catch((e) => {
  console.error(e?.message || e);
  process.exit(1);
});