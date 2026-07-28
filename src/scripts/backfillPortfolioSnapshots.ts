// One-time backfill of daily Profit/Loss snapshots for everyone who traded in the
// last ~60 days, so profile P&L charts aren't empty and the range toggles differ.
//
// P&L(T) = open-positions value at T (net shares × last-traded price as of T)
//        + realized cashflow up to T (SELL + CLAIM proceeds − BUY cost).
// Fully ledger-derived — no on-chain reads, deposit/withdrawal-neutral.
//
// Idempotent: skips any (user, day) that already has a snapshot.
//
//   npx ts-node --transpile-only src/scripts/backfillPortfolioSnapshots.ts
//   (or after build: node dist/scripts/backfillPortfolioSnapshots.js)
//   env: BACKFILL_DAYS (60)

import "dotenv/config"; // load .env before ../db reads DATABASE_URL (standalone run)
import { pool } from "../db";

const DAYS = Number(process.env.BACKFILL_DAYS || 60);
const DAY = 86400;

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
  console.log(`[backfill] ${addrs.length} traders, ${DAYS}d of daily P&L points`);

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

  // When each game became FINAL (epoch of updated_at). A resolved game must stop
  // being marked as an open position — its outcome flows through realized (CLAIM
  // proceeds for winners, nothing for losers). Without this, resolved-but-unclaimed
  // positions stay marked at last-traded price forever and hugely inflate P&L.
  const finalAt = new Map<string, number>(); // game_id -> epoch it resolved (Infinity = still open)
  const gq = await pool.query(
    `SELECT DISTINCT g.game_id, g.is_final, extract(epoch from g.updated_at)::bigint AS upd
       FROM public.games g
       JOIN public.user_trade_events t ON t.game_id = g.game_id
      WHERE lower(t.user_address) = ANY($1)`,
    [addrs]
  );
  for (const r of gq.rows as any[]) {
    finalAt.set(r.game_id, r.is_final ? Number(r.upd) : Number.POSITIVE_INFINITY);
  }

  // Existing snapshot days (idempotency) — "addr|YYYY-MM-DD" (UTC).
  const existing = new Set<string>();
  const ex = await pool.query(
    `SELECT user_address AS addr, to_char(snapshot_at AT TIME ZONE 'UTC','YYYY-MM-DD') AS d
       FROM public.portfolio_snapshots WHERE user_address = ANY($1) GROUP BY 1,2`,
    [addrs]
  );
  for (const r of ex.rows as any[]) existing.add(`${r.addr}|${r.d}`);

  const rows: Array<[string, number, number, string]> = []; // addr, positions, pnl, iso
  for (const addr of addrs) {
    const ts = byUser.get(addr) || [];
    if (!ts.length) continue;
    for (let d = DAYS - 1; d >= 1; d--) {
      const T = now - d * DAY;
      const dayStr = new Date(T * 1000).toISOString().slice(0, 10);
      if (existing.has(`${addr}|${dayStr}`)) continue;

      const shares = new Map<string, number>();
      const claimed = new Set<string>();
      let realized = 0;
      for (const r of ts) {
        if (r.ts > T) break; // sorted asc → only trades up to T
        if (r.type === "CLAIM") { claimed.add(r.gameId); realized += r.nout; continue; }
        const price = r.px > 0 ? r.px / 10000 : 0;
        if (r.type === "BUY") { realized -= r.gin; }
        else { realized += (r.gout || r.nout); } // SELL
        if (price <= 0) continue;
        const k = `${r.gameId}:${r.outcome}`;
        const sh = (r.type === "BUY" ? r.gin : -(r.gout || r.nout)) / price;
        shares.set(k, (shares.get(k) || 0) + sh);
      }
      let positions = 0;
      for (const [k, sh] of shares) {
        if (sh <= 1e-6) continue;
        const gid = k.split(":")[0];
        if (claimed.has(gid)) continue;
        if ((finalAt.get(gid) ?? Number.POSITIVE_INFINITY) <= T) continue; // resolved by T → realized, not open
        positions += sh * priceBefore(k, T);
      }
      const pnl = positions + realized;
      rows.push([addr, positions, pnl, new Date(T * 1000).toISOString()]);
    }
  }

  let n = 0;
  const CHUNK = 500;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const vals: any[] = [];
    const ph = chunk
      .map((r, j) => {
        const b = j * 4;
        vals.push(r[0], r[1], r[2], r[3]);
        return `($${b + 1},$${b + 2},$${b + 3},$${b + 4})`;
      })
      .join(",");
    await pool.query(
      `INSERT INTO public.portfolio_snapshots (user_address, positions_usd, pnl_usd, snapshot_at) VALUES ${ph}`,
      vals
    );
    n += chunk.length;
  }
  console.log(`[backfill] inserted ${n} historical P&L snapshots across ${addrs.length} users`);
}

main()
  .then(() => process.exit(0))
  .catch((e) => {
    console.error("[backfill] error", e?.message || e);
    process.exit(1);
  });
