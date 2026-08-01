// src/scripts/backfillUnmappedV2Fills.ts
//
// ONE-TIME backfill for v2 fills that settled while the backend's games.json was
// stale. When a fill lands for a market resolveV2Market() can't map, the raw fill
// is still written to v2.fills (with game_id = null) but its BUY/SELL legs are
// logged UNMAPPED and never reach public.user_trade_events — so the trade is real
// on-chain yet invisible on the profile History (blank Buy/Avg/ROI, a lone CLAIM).
//
// Now that games.json includes those markets, this re-runs each null-game_id fill
// through the SAME recordV2Fill path the matcher uses, so the missing buys appear
// with correct price/return/ROI. Idempotent: user_trade_events ids are v2-<tx>-a/b
// with ON CONFLICT DO UPDATE, so re-running never double-counts. It also patches
// the null game_id (+ league / order linkage) on v2.fills and v2.orders.
//
// Run (after `npm run build`, on the host where GAMES_JSON_PATH + DATABASE_URL are set):
//   node dist/scripts/backfillUnmappedV2Fills.js --dry     # preview, writes nothing
//   node dist/scripts/backfillUnmappedV2Fills.js           # apply

import { pool } from "../db";
import { recordV2Fill, V2FillInput } from "../services/v2/v2Persist";
import {
  invalidateV2Markets,
  resolveV2Market,
  resolveV2Leg,
} from "../services/v2/v2MarketResolver";

async function main() {
  const dry = process.argv.includes("--dry");
  invalidateV2Markets(); // force a fresh read of the deployed games.json

  const { rows } = await pool.query(
    `SELECT tx_hash, market_id, match_type, fill_shares, maker_price,
            a_hash, b_hash, a_maker, b_maker, a_side, b_side, a_outcome, b_outcome, block_number
       FROM v2.fills
      WHERE game_id IS NULL
      ORDER BY created_at`
  );
  console.log(`${rows.length} unmapped fill(s) found`);

  let mapped = 0;
  let stillUnmapped = 0;
  let statsRows = 0;
  const markets = new Set<string>();

  for (const r of rows) {
    const entry = resolveV2Market(r.market_id);
    if (!entry) {
      stillUnmapped++;
      console.log(`  ✗ ${r.tx_hash}  market ${r.market_id} STILL not in games.json — skipping`);
      continue;
    }
    markets.add(String(r.market_id).toLowerCase());
    console.log(`  ${dry ? "would remap" : "remapping"} ${r.tx_hash}  ->  ${entry.gameId}`);
    mapped++;
    if (dry) continue;

    const input: V2FillInput = {
      txHash: r.tx_hash,
      matchType: r.match_type,
      fill: r.fill_shares,
      makerPrice: r.maker_price,
      a: { hash: r.a_hash, maker: r.a_maker, marketId: r.market_id, outcome: Number(r.a_outcome), side: Number(r.a_side), price: r.maker_price },
      b: { hash: r.b_hash, maker: r.b_maker, marketId: r.market_id, outcome: Number(r.b_outcome), side: Number(r.b_side), price: r.maker_price },
      blockNumber: r.block_number != null ? Number(r.block_number) : null,
    };
    const res = await recordV2Fill(input);
    statsRows += res.statsRows;
  }

  // Patch the lingering null game_id on the raw fill + order rows (recordV2Fill's
  // v2.fills insert is ON CONFLICT DO NOTHING, so it won't heal the existing row).
  if (!dry) {
    for (const mid of markets) {
      const entry = resolveV2Market(mid);
      if (!entry) continue;
      await pool.query(
        `UPDATE v2.fills SET game_id = $2, league = COALESCE(league, $3)
          WHERE market_id = $1 AND game_id IS NULL`,
        [mid, entry.parentGameId, entry.league]
      );
      const { rows: ords } = await pool.query(
        `SELECT order_hash, outcome FROM v2.orders WHERE market_id = $1 AND game_id IS NULL`,
        [mid]
      );
      for (const o of ords) {
        const leg = resolveV2Leg(mid, Number(o.outcome));
        await pool.query(
          `UPDATE v2.orders
              SET game_id = $2, parent_game_id = $3, parent_outcome_index = $4,
                  outcome_code = $5, market_type = $6, league = COALESCE(league, $7),
                  updated_at = now()
            WHERE order_hash = $1`,
          [o.order_hash, entry.gameId, entry.parentGameId, entry.parentOutcomeIndex,
           leg?.leg.outcomeCode ?? null, entry.marketType, entry.league]
        );
      }
    }
  }

  console.log(
    `\n${dry ? "DRY " : ""}done: ${mapped} fill(s) ${dry ? "would map" : "mapped"}, ` +
      `${stillUnmapped} still unmapped, ${statsRows} user_trade_events leg(s) written across ${markets.size} market(s).`
  );
  await pool.end();
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
