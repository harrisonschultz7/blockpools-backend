// src/workers/portfolioSnapshotWorker.ts
//
// Server-side net-worth snapshot cron. Runs on a schedule (independent of who's
// browsing) so EVERY active trader's profile chart stays current — not just when
// the owner happens to visit. Complements the client-reported snapshot (which
// still fires on own-profile visits).
//
// Equity per user = on-chain USDC balance (cash) + open positions marked to the
// market's last-traded price (mark-to-market). Positions come from the unified
// trade ledger (v1 AMM + v2 order-book), so both are covered. All DB-derived
// except the USDC balances (batched on-chain reads).
//
// Enable with PORTFOLIO_SNAPSHOT_CRON_ENABLED=true. Tunables:
//   SNAPSHOT_INTERVAL_MS (default 6h), SNAPSHOT_ACTIVE_DAYS (60),
//   SNAPSHOT_RPC_CONCURRENCY (8), ARBITRUM_RPC_URL / RPC_URL, V2_USDC_ADDRESS.

import { JsonRpcProvider, Contract } from "ethers";
import { pool } from "../db";

const RPC_URL = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc";
const USDC_ADDRESS = process.env.V2_USDC_ADDRESS || "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
const ACTIVE_DAYS = Number(process.env.SNAPSHOT_ACTIVE_DAYS || 60);
const INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 6 * 3600 * 1000);
const RPC_CONCURRENCY = Math.max(1, Number(process.env.SNAPSHOT_RPC_CONCURRENCY || 8));

const ERC20 = ["function balanceOf(address) view returns (uint256)"];

/** On-chain USDC balance (6dp → USD) for each address, batched with a concurrency cap. */
async function usdcBalances(addrs: string[]): Promise<Map<string, number>> {
  const provider = new JsonRpcProvider(RPC_URL);
  const usdc = new Contract(USDC_ADDRESS, ERC20, provider);
  const out = new Map<string, number>();
  for (let i = 0; i < addrs.length; i += RPC_CONCURRENCY) {
    const chunk = addrs.slice(i, i + RPC_CONCURRENCY);
    await Promise.all(
      chunk.map(async (a) => {
        try {
          const bal = (await usdc.balanceOf(a)) as bigint;
          out.set(a, Number(bal) / 1e6);
        } catch {
          /* leave undefined → treated as 0 cash */
        }
      })
    );
  }
  return out;
}

/**
 * Compute + record one snapshot per active trader. Returns how many were written.
 */
export async function runPortfolioSnapshots(): Promise<{ users: number }> {
  const started = Date.now();

  // 1) Active traders (traded within the window).
  const traders = await pool.query<{ addr: string }>(
    `SELECT DISTINCT lower(user_address) AS addr
       FROM public.user_trade_events
      WHERE timestamp > extract(epoch from now())::bigint - $1`,
    [ACTIVE_DAYS * 86400]
  );
  const addrs = traders.rows.map((r) => r.addr).filter(Boolean);
  if (!addrs.length) return { users: 0 };

  // 2) Open positions valued at each market's last-traded price (unresolved only).
  //    net_shares = Σ BUY($/price) − Σ SELL($/price); value = net_shares × last price.
  const posRes = await pool.query<{ addr: string; positions_usd: string }>(`
    WITH last_price AS (
      SELECT DISTINCT ON (game_id, outcome_index) game_id, outcome_index, avg_price_bps
        FROM public.user_trade_events
       WHERE avg_price_bps > 0
       ORDER BY game_id, outcome_index, timestamp DESC
    ),
    pos AS (
      SELECT lower(t.user_address) AS addr, t.game_id, t.outcome_index,
             SUM(
               CASE WHEN t.type = 'BUY'  THEN t.gross_in_dec / (t.avg_price_bps / 10000.0)
                    WHEN t.type = 'SELL' THEN -COALESCE(t.gross_out_dec, t.net_out_dec, 0) / (t.avg_price_bps / 10000.0)
                    ELSE 0 END
             ) AS net_shares
        FROM public.user_trade_events t
        JOIN public.games g ON g.game_id = t.game_id
       WHERE t.type IN ('BUY','SELL')
         AND t.avg_price_bps > 0
         AND COALESCE(g.is_final, false) = false
         AND COALESCE(upper(g.resolution_type), 'UNRESOLVED') NOT IN ('RESOLVED','FINAL')
       GROUP BY addr, t.game_id, t.outcome_index
    )
    SELECT p.addr, COALESCE(SUM(p.net_shares * (lp.avg_price_bps / 10000.0)), 0) AS positions_usd
      FROM pos p
      LEFT JOIN last_price lp ON lp.game_id = p.game_id AND lp.outcome_index = p.outcome_index
     WHERE p.net_shares > 0.000001
     GROUP BY p.addr
  `);
  const positionsByAddr = new Map<string, number>();
  for (const r of posRes.rows) positionsByAddr.set(r.addr, Number(r.positions_usd) || 0);

  // 3) On-chain USDC (cash).
  const cashByAddr = await usdcBalances(addrs);

  // 4) Insert one snapshot per user (skip users with nothing to show).
  let n = 0;
  for (const addr of addrs) {
    const cash = cashByAddr.get(addr) ?? 0;
    const positions = positionsByAddr.get(addr) ?? 0;
    const equity = cash + positions;
    if (equity <= 0 && positions <= 0) continue;
    try {
      await pool.query(
        `INSERT INTO public.portfolio_snapshots (user_address, cash_usd, positions_usd, equity_usd)
         VALUES ($1, $2, $3, $4)`,
        [addr, cash, positions, equity]
      );
      n++;
    } catch (e: any) {
      console.error(`[snapshot-cron] insert failed for ${addr}: ${e?.message || e}`);
    }
  }
  console.log(
    `[snapshot-cron] wrote ${n}/${addrs.length} snapshots in ${((Date.now() - started) / 1000).toFixed(1)}s`
  );
  return { users: n };
}

/** Start the in-process cron. No-op unless PORTFOLIO_SNAPSHOT_CRON_ENABLED=true. */
export function startPortfolioSnapshotCron(): void {
  const enabled = String(process.env.PORTFOLIO_SNAPSHOT_CRON_ENABLED || "").toLowerCase() === "true";
  if (!enabled) {
    console.log("[snapshot-cron] disabled (set PORTFOLIO_SNAPSHOT_CRON_ENABLED=true to enable)");
    return;
  }
  const tick = () => {
    runPortfolioSnapshots().catch((e) => console.error("[snapshot-cron]", e?.message || e));
  };
  console.log(`[snapshot-cron] enabled — every ${(INTERVAL_MS / 3600000).toFixed(1)}h, active window ${ACTIVE_DAYS}d`);
  setTimeout(tick, 20_000); // first run shortly after boot
  setInterval(tick, INTERVAL_MS);
}
