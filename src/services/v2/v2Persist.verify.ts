// Standalone verification for the v2 stats mapping (economics + resolver).
// Run: npx ts-node --transpile-only src/services/v2/v2Persist.verify.ts
// No DB required. Exits non-zero on any failed assertion.

import { legEconomics } from "./v2Persist";
import { resolveV2Leg, resolveV2Market } from "./v2MarketResolver";

let failures = 0;
function eq(name: string, got: any, want: any) {
  const ok = String(got) === String(want);
  if (!ok) failures++;
  console.log(`${ok ? "PASS" : "FAIL"}  ${name}: got=${got} want=${want}`);
}

const SCALE = 1_000_000n;

// --- Economics ------------------------------------------------------------
// COMPLEMENTARY: both legs settle at maker price P=0.60, fill=50 shares.
{
  const P = 600_000n; // $0.60
  const F = 50n * SCALE; // 50 shares
  const taker = legEconomics("complementary", true, P, F);
  const maker = legEconomics("complementary", false, P, F);
  eq("comp taker price", taker.price, 600_000n);
  eq("comp taker $", taker.amountDec, "30.000000"); // 0.60 * 50
  eq("comp taker bps", taker.bps, 6000);
  eq("comp maker $", maker.amountDec, "30.000000"); // same price both sides
}

// MINT: two buyers fund a $1 set. Maker BUY @ P=0.45 -> pays 0.45; taker BUY
// funds remainder 0.55. fill=100 shares.
{
  const P = 450_000n;
  const F = 100n * SCALE;
  const taker = legEconomics("mint", true, P, F);
  const maker = legEconomics("mint", false, P, F);
  eq("mint taker price", taker.price, 550_000n); // 1 - 0.45
  eq("mint taker $", taker.amountDec, "55.000000");
  eq("mint taker bps", taker.bps, 5500);
  eq("mint maker price", maker.price, 450_000n);
  eq("mint maker $", maker.amountDec, "45.000000");
  eq("mint set total $", (BigInt(taker.microUsd) + BigInt(maker.microUsd)).toString(), (100n * SCALE).toString()); // $100 for 100 sets
}

// MERGE: two sellers paid from a burned $1 set. Maker SELL @ P=0.70 gets 0.70;
// taker SELL gets remainder 0.30. fill=10 shares.
{
  const P = 700_000n;
  const F = 10n * SCALE;
  const taker = legEconomics("merge", true, P, F);
  const maker = legEconomics("merge", false, P, F);
  eq("merge taker price", taker.price, 300_000n);
  eq("merge taker $", taker.amountDec, "3.000000");
  eq("merge maker $", maker.amountDec, "7.000000");
  eq("merge set total $", (BigInt(taker.microUsd) + BigInt(maker.microUsd)).toString(), (10n * SCALE).toString());
}

// Fractional shares round-trip.
{
  const e = legEconomics("complementary", true, 333_333n, 3n * SCALE); // 0.333333 * 3
  eq("fractional $", e.amountDec, "0.999999");
}

// --- Resolver (against live games.json) -----------------------------------
// The one live v2 market: binary MLB-MIL-SF. marketId from games.json.
{
  const MID = "0x87174114adffd9b592fc259c12769ab179458d7603f9cc447a6ac99c37b94d01";
  const m = resolveV2Market(MID);
  if (!m) {
    console.log("SKIP  binary resolver: market not found (games.json not on GAMES_JSON_PATH?)");
  } else {
    eq("binary kind", m.kind, "binary");
    eq("binary marketType", m.marketType, "BINARY");
    eq("binary outcomesCount", m.outcomesCount, 2);
    const leg0 = resolveV2Leg(MID, 0)!;
    const leg1 = resolveV2Leg(MID, 1)!;
    eq("binary leg0 index", leg0.leg.outcomeIndex, 0);
    eq("binary leg0 side", leg0.leg.side, "A");
    eq("binary leg1 index", leg1.leg.outcomeIndex, 1);
    eq("binary leg1 side", leg1.leg.side, "B");
    eq("binary gameId == parent", m.gameId, m.parentGameId);
  }
}

console.log(failures === 0 ? "\nALL PASS" : `\n${failures} FAILURE(S)`);
process.exit(failures === 0 ? 0 : 1);
