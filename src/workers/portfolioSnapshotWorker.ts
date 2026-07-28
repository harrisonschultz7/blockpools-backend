// src/workers/portfolioSnapshotWorker.ts
//
// Server-side Profit/Loss snapshot cron. Runs on a schedule (independent of who's
// browsing) so EVERY active trader's profile P&L chart stays current.
//
// P&L per user = open-positions value (marked to each market's last-traded price)
//   + realized cashflow  (SELL proceeds + CLAIM proceeds − BUY cost).
// Fully derived from the trade ledger — no on-chain reads, and deposit/withdrawal
// -neutral (adding funds isn't profit).
//
// Enable with PORTFOLIO_SNAPSHOT_CRON_ENABLED=true. Tunables:
//   SNAPSHOT_INTERVAL_MS (default 6h), SNAPSHOT_ACTIVE_DAYS (60).

import { pool } from "../db";

const ACTIVE_DAYS = Number(process.env.SNAPSHOT_ACTIVE_DAYS || 60);
const INTERVAL_MS = Number(process.env.SNAPSHOT_INTERVAL_MS || 6 * 3600 * 1000);

/** Compute + record one P&L snapshot per active trader. Returns how many were written. */
export async function runPortfolioSnapshots(): Promise<{ users: number }> {
  const started = Date.now();

  const traders = await pool.query<{ addr: string }>(
    `SELECT DISTINCT lower(user_address) AS addr
       FROM public.user_trade_events
      WHERE timestamp > extract(epoch from now())::bigint - $1`,
    [ACTIVE_DAYS * 86400]
  );
  const addrs = traders.rows.map((r) => r.addr).filter(Boolean);
  if (!addrs.length) return { users: 0 };

  // Open positions valued at each market's last-traded price (unresolved only).
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

  // Realized cashflow = money out of trades − money in.
  const realizedRes = await pool.query<{ addr: string; realized: string }>(`
    SELECT lower(user_address) AS addr,
           SUM(CASE WHEN type = 'BUY'   THEN -COALESCE(gross_in_dec, 0)
                    WHEN type = 'SELL'  THEN  COALESCE(gross_out_dec, net_out_dec, 0)
                    WHEN type = 'CLAIM' THEN  COALESCE(net_out_dec, 0)
                    ELSE 0 END) AS realized
      FROM public.user_trade_events
     WHERE lower(user_address) = ANY($1) AND type IN ('BUY','SELL','CLAIM')
     GROUP BY 1
  `, [addrs]);
  const realizedByAddr = new Map<string, number>();
  for (const r of realizedRes.rows) realizedByAddr.set(r.addr, Number(r.realized) || 0);

  let n = 0;
  for (const addr of addrs) {
    const positions = positionsByAddr.get(addr) ?? 0;
    const realized = realizedByAddr.get(addr) ?? 0;
    const pnl = positions + realized;
    try {
      await pool.query(
        `INSERT INTO public.portfolio_snapshots (user_address, positions_usd, pnl_usd)
         VALUES ($1, $2, $3)`,
        [addr, positions, pnl]
      );
      n++;
    } catch (e: any) {
      console.error(`[snapshot-cron] insert failed for ${addr}: ${e?.message || e}`);
    }
  }
  console.log(`[snapshot-cron] wrote ${n}/${addrs.length} P&L snapshots in ${((Date.now() - started) / 1000).toFixed(1)}s`);
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
  setTimeout(tick, 20_000);
  setInterval(tick, INTERVAL_MS);
}
