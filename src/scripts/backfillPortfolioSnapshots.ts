// One-time backfill of daily net-worth snapshots for everyone who traded in the
// last ~60 days, so profile charts aren't empty and the range toggles differ.
//
// Method (matches the forward cron so backfilled + live points are consistent):
//   positions(T) = Σ net shares held at T × the market's last-traded price as of T
//                  (from the trade ledger; a CLAIM before T closes that position)
//   cash(T)      = current on-chain USDC  −  net trade cashflow after T
//                  (BUY out / SELL+CLAIM in). Anchored to today's real balance.
//   equity(T)    = cash(T) + positions(T)
//
// Caveat: the cash side is exact ONLY if the user didn't deposit/withdraw in the
// window (we have no deposit history) — the positions/P&L shape stays correct.
//
// Idempotent: skips any (user, day) that already has a snapshot, so re-running
// never duplicates and never clobbers the forward cron's points.
//
//   npx ts-node --transpile-only src/scripts/backfillPortfolioSnapshots.ts
//   env: BACKFILL_DAYS (60), ARBITRUM_RPC_URL, V2_USDC_ADDRESS, SNAPSHOT_RPC_CONCURRENCY

import { JsonRpcProvider, Contract } from "ethers";
import { pool } from "../db";

const RPC_URL = process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc";
const USDC_ADDRESS = process.env.V2_USDC_ADDRESS || "0xaf88d065e77c8cC2239327C5EDb3A432268e5831";
const DAYS = Number(process.env.BACKFILL_DAYS || 60);
const RPC_CONCURRENCY = Math.max(1, Number(process.env.SNAPSHOT_RPC_CONCURRENCY || 8));
const DAY = 86400;
const ERC20 = ["function balanceOf(address) view returns (uint256)"];

async function usdcBalances(addrs: string[]): Promise<Map<string, number>> {
  const provider = new JsonRpcProvider(RPC_URL);
  const usdc = new Contract(USDC_ADDRESS, ERC20, provider);
  const out = new Map<string, number>();
  for (let i = 0; i < addrs.length; i += RPC_CONCURRENCY) {
    await Promise.all(
      addrs.slice(i, i + RPC_CONCURRENCY).map(async (a) => {
        try {
          out.set(a, Number((await usdc.balanceOf(a)) as bigint) / 1e6);
        } catch {
          /* undefined → 0 cash */
        }
      })
    );
  }
  return out;
}

type Trade = {
  addr: string; ts: number; type: "BUY" | "SELL" | "CLAIM";
  gameId: string; outcome: number; gin: number; gout: number; nout: number; px: number;
};

async function main() {
  const now = Math.floor(Date.now() / 1000);
  const since = now - DAYS * DAY;

  const traders = await pool.query<{ addr: string }>(
    `SELECT DISTINCT lower(user_address) AS addr FROM public.user_trade_events WHERE timestamp > $1`,
    [since]
  );
  const addrs = traders.rows.map((r) => r.addr).filter(Boolean);
  if (!addrs.length) {
    console.log("[backfill] no active traders");
    return;
  }
  console.log(`[backfill] ${addrs.length} traders, ${DAYS}d of daily points`);

  // Full trade history for these users (a position opened >60d ago may still be held).
  const tr = await pool.query(
    `SELECT lower(user_address) AS addr, timestamp AS ts, type, game_id, outcome_index,
            COALESCE(gross_in_dec,0)::float8 AS gin, COALESCE(gross_out_dec,0)::float8 AS gout,
            COALESCE(net_out_dec,0)::float8 AS nout, COALESCE(avg_price_bps,0)::int AS px
       FROM public.user_trade_events
      WHERE lower(user_address) = ANY($1) AND type IN ('BUY','SELL','CLAIM')
      ORDER BY timestamp ASC`,
    [addrs]
  );

  const byUser = new Map<string, Trade[]>();
  const priceTL = new Map<string, Array<{ ts: number; price: number }>>();
  for (const r of tr.rows as any[]) {
    const t: Trade = {
      addr: r.addr, ts: Number(r.ts), type: r.type, gameId: r.game_id,
      outcome: Number(r.outcome_index), gin: r.gin, gout: r.gout, nout: r.nout, px: r.px,
    };
    let a = byUser.get(t.addr);
    if (!a) { a = []; byUser.set(t.addr, a); }
    a.push(t);
    if (t.px > 0) {
      const k = `${t.gameId}:${t.outcome}`;
      let pl = priceTL.get(k);
      if (!pl) { pl = []; priceTL.set(k, pl); }
      pl.push({ ts: t.ts, price: t.px / 10000 });
    }
  }
  const priceBefore = (k: string, T: number): number => {
    const a = priceTL.get(k);
    if (!a) return 0;
    let p = 0;
    for (const e of a) { if (e.ts <= T) p = e.price; else break; }
    return p;
  };

  const cashNow = await usdcBalances(addrs);

  // Existing snapshot days (idempotency) — "addr|YYYY-MM-DD".
  const existing = new Set<string>();
  const ex = await pool.query(
    `SELECT user_address AS addr, to_char(date_trunc('day', snapshot_at),'YYYY-MM-DD') AS d
       FROM public.portfolio_snapshots WHERE user_address = ANY($1) GROUP BY 1,2`,
    [addrs]
  );
  for (const r of ex.rows as any[]) existing.add(`${r.addr}|${r.d}`);

  const rows: Array<[string, number, number, number, string]> = [];
  for (const addr of addrs) {
    const ts = byUser.get(addr) || [];
    if (!ts.length) continue;
    for (let d = DAYS - 1; d >= 1; d--) {
      const T = now - d * DAY;
      const dayStr = new Date(T * 1000).toISOString().slice(0, 10);
      if (existing.has(`${addr}|${dayStr}`)) continue;

      const shares = new Map<string, number>();
      const claimed = new Set<string>();
      let cashDelta = 0;
      for (const r of ts) {
        if (r.ts <= T) {
          if (r.type === "CLAIM") { claimed.add(r.gameId); continue; }
          const price = r.px > 0 ? r.px / 10000 : 0;
          if (price <= 0) continue;
          const k = `${r.gameId}:${r.outcome}`;
          const sh = (r.type === "BUY" ? r.gin : -(r.gout || r.nout)) / price;
          shares.set(k, (shares.get(k) || 0) + sh);
        } else {
          // trade after T → contributes to cash reconstruction
          if (r.type === "BUY") cashDelta += r.gin;
          else if (r.type === "SELL") cashDelta -= (r.gout || r.nout);
          else cashDelta -= r.nout; // CLAIM
        }
      }
      let positions = 0;
      for (const [k, sh] of shares) {
        if (sh <= 1e-6) continue;
        if (claimed.has(k.split(":")[0])) continue;
        positions += sh * priceBefore(k, T);
      }
      const cash = Math.max(0, (cashNow.get(addr) || 0) + cashDelta);
      const equity = cash + positions;
      if (equity <= 0) continue;
      rows.push([addr, cash, positions, equity, new Date(T * 1000).toISOString()]);
    }
  }

  // Batch insert.
  let n = 0;
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const vals: any[] = [];
    const ph = chunk
      .map((r, j) => {
        const b = j * 5;
        vals.push(r[0], r[1], r[2], r[3], r[4]);
        return `($${b + 1},$${b + 2},$${b + 3},$${b + 4},$${b + 5})`;
      })
      .join(",");
    await pool.query(
      `INSERT INTO public.portfolio_snapshots (user_address, cash_usd, positions_usd, equity_usd, snapshot_at) VALUES ${ph}`,
      vals
    );
    n += chunk.length;
  }
  console.log(`[backfill] inserted ${n} historical snapshots across ${addrs.length} users`);
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("[backfill] error", e?.message || e);
    process.exit(1);
  });
