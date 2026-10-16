// src/services/pmWalletValue.ts
//
// PORTFOLIO VALUE over time, for the profile's headline graph.
//
//   value = what the user put in (deposits − withdrawals) + what they have
//           banked (settled results)
//
// A bet that is open does not move it: the cash spent sits in the position at
// cost until the trade is sold or the market resolves, and THEN the gain or loss
// lands. Deposits and withdrawals move it the moment they happen.
//
// SOURCES
//   - Results: the Polymarket activity feed (TRADE buys/sells) per outcome
//     token, average-cost basis; markets that resolved settle the rest at $1 or
//     $0 at their resolution time (Gamma).
//   - Deposits / withdrawals: the wallet's pUSD and USDC.e Transfer logs, read
//     from a Polygon node by scanWalletTransfers (run on a schedule) into
//     public.pm_wallet_transfers. A transfer whose transaction is a trade or a
//     redeem is trade money, not a deposit; what is left is money in or out.
//
// THE STARTING BALANCE. Free Polygon nodes prune logs older than a few weeks,
// so history before the first scan can be incomplete. The value today is known
// exactly (wallet cash + open positions at cost), so whatever the visible events
// do not explain is placed as one starting balance at the wallet's first known
// event. Every point after that is exact, and the last point is always the real
// balance. Set POLYGON_RPC_URL to an archive node (e.g. Alchemy) for exact
// history from the start.

import { pool } from "../db";

const DATA_API = "https://data-api.polymarket.com";
const GAMMA = (process.env.GAMMA_API_URL || "https://gamma-api.polymarket.com").replace(/\/+$/, "");
const TOKENS = [
  "0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb", // pUSD
  "0x2791bca1f2de4661ed88a30c99a7a9449aa84174", // USDC.e
];
const TRANSFER = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
const RPCS = [
  process.env.POLYGON_RPC_URL || "",
  "https://polygon-bor-rpc.publicnode.com",
  "https://polygon.drpc.org",
].filter(Boolean);
/** Free nodes refuse log ranges over 10k blocks. */
const CHUNK = 9_999;
/** How far back a NEW wallet's scan starts (free nodes keep ~weeks of logs). */
const FIRST_SCAN_DAYS = Number(process.env.PM_WALLET_FIRST_SCAN_DAYS || 25);
/** Polygon PoS block time, rounded down so a day-count over-covers. */
const BLOCK_S = 1.9;

let rpcIdx = 0;
async function rpc(method: string, params: any[]): Promise<any> {
  let last = "";
  for (let i = 0; i < 8; i++) {
    const url = RPCS[rpcIdx++ % RPCS.length];
    try {
      const r = await fetch(url, {
        method: "POST", headers: { "content-type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }),
      });
      const j: any = await r.json();
      if (j.result !== undefined) return j.result;
      last = JSON.stringify(j.error || j).slice(0, 160);
    } catch (e: any) {
      last = String(e?.message || e);
    }
    await new Promise((r) => setTimeout(r, 400 * (i + 1)));
  }
  throw new Error(`polygon rpc ${method} failed: ${last}`);
}

async function getJson(url: string): Promise<any> {
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`${r.status} ${url}`);
  return r.json();
}

const padTopic = (addr: string) => "0x" + "0".repeat(24) + addr.toLowerCase().replace(/^0x/, "");

/**
 * Advance one wallet's transfer scan by up to maxChunks block ranges. Returns
 * the number of transfers stored. Safe to run repeatedly: rows are keyed by
 * (wallet, tx, log index).
 */
export async function scanWalletTransfers(wallet: string, maxChunks = 40): Promise<number> {
  const w = wallet.toLowerCase();
  const head = parseInt(await rpc("eth_blockNumber", []), 16) - 5; // a few confirmations
  let state = (await pool.query(`SELECT from_block, next_block FROM public.pm_wallet_scan WHERE wallet = $1`, [w])).rows[0];
  if (!state) {
    const start = Math.max(0, head - Math.ceil((FIRST_SCAN_DAYS * 86400) / BLOCK_S));
    await pool.query(
      `INSERT INTO public.pm_wallet_scan (wallet, from_block, next_block) VALUES ($1, $2, $2) ON CONFLICT (wallet) DO NOTHING`,
      [w, start]);
    state = { from_block: start, next_block: start };
  }
  let next = Number(state.next_block);
  let stored = 0;
  const blockTs = new Map<number, number>();
  for (let n = 0; n < maxChunks && next <= head; n++) {
    const to = Math.min(next + CHUNK, head);
    for (const direction of ["in", "out"] as const) {
      const topics = direction === "in" ? [TRANSFER, null, padTopic(w)] : [TRANSFER, padTopic(w)];
      const logs: any[] = await rpc("eth_getLogs", [{
        fromBlock: "0x" + next.toString(16), toBlock: "0x" + to.toString(16), address: TOKENS, topics,
      }]);
      for (const l of logs) {
        const block = parseInt(l.blockNumber, 16);
        let ts = l.blockTimestamp ? parseInt(l.blockTimestamp, 16) : blockTs.get(block);
        if (!ts) {
          ts = parseInt((await rpc("eth_getBlockByNumber", [l.blockNumber, false])).timestamp, 16);
          blockTs.set(block, ts);
        }
        const r = await pool.query(
          `INSERT INTO public.pm_wallet_transfers (wallet, tx_hash, log_index, block, ts, token, direction, amount)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`,
          [w, String(l.transactionHash).toLowerCase(), parseInt(l.logIndex, 16), block, ts,
           String(l.address).toLowerCase(), direction, (Number(BigInt(l.data)) / 1e6).toFixed(6)]);
        stored += r.rowCount || 0;
      }
    }
    next = to + 1;
    await pool.query(`UPDATE public.pm_wallet_scan SET next_block = $2, updated_at = now() WHERE wallet = $1`, [w, next]);
  }
  return stored;
}

/** Cash the wallet holds now: pUSD + USDC.e. */
async function walletCash(wallet: string): Promise<number | null> {
  const data = "0x70a08231" + "0".repeat(24) + wallet.toLowerCase().replace(/^0x/, "");
  try {
    let sum = 0;
    for (const t of TOKENS) sum += Number(BigInt(await rpc("eth_call", [{ to: t, data }, "latest"]))) / 1e6;
    return sum;
  } catch {
    return null;
  }
}

const gammaCache = new Map<string, { at: number; m: any }>();
async function gammaMarket(conditionId: string): Promise<any | null> {
  const hit = gammaCache.get(conditionId);
  if (hit && Date.now() - hit.at < 10 * 60_000) return hit.m;
  let m: any = null;
  try {
    m = (await getJson(`${GAMMA}/markets?condition_ids=${conditionId}&closed=true`))[0] || null;
    if (!m) m = (await getJson(`${GAMMA}/markets?condition_ids=${conditionId}`))[0] || null;
  } catch { /* unknown */ }
  gammaCache.set(conditionId, { at: Date.now(), m });
  return m;
}
const parseArr = (x: any): any[] => { try { return Array.isArray(x) ? x : JSON.parse(x || "[]"); } catch { return []; } };
const isoTs = (s: any) => Math.floor(Date.parse(String(s || "").replace(" ", "T").replace(/\+00$/, "Z")) / 1000);

export type ValuePoint = { t: number; v: number; kind: "start" | "deposit" | "withdraw" | "result" | "now" };

const seriesCache = new Map<string, { at: number; out: any }>();

/** The value series for one Polymarket wallet. Cached for two minutes. */
export async function walletValueSeries(wallet: string) {
  const w = wallet.toLowerCase();
  const hit = seriesCache.get(w);
  if (hit && Date.now() - hit.at < 120_000) return hit.out;

  // Activity: every trade/redeem (their tx hashes mark trade money).
  const act: any[] = [];
  for (let off = 0; off < 5000; off += 500) {
    const page = await getJson(`${DATA_API}/activity?user=${w}&limit=500&offset=${off}`);
    if (!Array.isArray(page)) break;
    act.push(...page);
    if (page.length < 500) break;
  }
  const actTx = new Set(act.map((a) => String(a.transactionHash || "").toLowerCase()));

  // Deposits / withdrawals: net stable flow per transaction, trade txs excluded.
  const { rows: transfers } = await pool.query(
    `SELECT tx_hash, ts, direction, amount FROM public.pm_wallet_transfers WHERE wallet = $1`, [w]);
  const byTx = new Map<string, { ts: number; net: number }>();
  for (const t of transfers) {
    const e = byTx.get(t.tx_hash) || { ts: Number(t.ts), net: 0 };
    e.net += (t.direction === "in" ? 1 : -1) * Number(t.amount);
    byTx.set(t.tx_hash, e);
  }
  const events: { t: number; v: number; kind: ValuePoint["kind"] }[] = [];
  for (const [tx, e] of byTx) {
    if (actTx.has(tx) || Math.abs(e.net) < 0.01) continue;
    events.push({ t: e.ts, v: e.net, kind: e.net > 0 ? "deposit" : "withdraw" });
  }

  // Results: average cost per outcome token; a sell banks proceeds − basis.
  type Pos = { sh: number; cost: number; cond: string; oi: number };
  const pos = new Map<string, Pos>();
  for (const a of act.filter((x) => x.type === "TRADE").sort((x, y) => x.timestamp - y.timestamp)) {
    const p = pos.get(a.asset) || { sh: 0, cost: 0, cond: a.conditionId, oi: Number(a.outcomeIndex) };
    const size = Number(a.size), usdc = Number(a.usdcSize);
    if (String(a.side).toUpperCase() === "BUY") { p.sh += size; p.cost += usdc; }
    else {
      const basis = p.sh > 0 ? p.cost * Math.min(1, size / p.sh) : 0;
      events.push({ t: Number(a.timestamp), v: usdc - basis, kind: "result" });
      p.sh -= size; p.cost -= basis;
    }
    pos.set(a.asset, p);
  }
  // What is still held: resolved markets settle at $1/$0; the rest is open, at cost.
  let openCost = 0;
  for (const p of pos.values()) {
    if (p.sh < 0.001) continue;
    const m = await gammaMarket(p.cond);
    const px = m ? Number(parseArr(m.outcomePrices)[p.oi]) : NaN;
    const ts = m ? isoTs(m.closedTime || m.umaEndDate) : NaN;
    if (m?.closed && (px >= 0.99 || px <= 0.01) && Number.isFinite(ts)) {
      events.push({ t: ts, v: p.sh * (px >= 0.99 ? 1 : 0) - p.cost, kind: "result" });
    } else {
      openCost += Math.max(0, p.cost);
    }
  }

  events.sort((a, b) => a.t - b.t);
  const cash = await walletCash(w);
  const now = Math.floor(Date.now() / 1000);
  const explained = events.reduce((s, e) => s + e.v, 0);
  const current = cash == null ? null : cash + openCost;
  // Whatever the visible history does not explain -- funding before the logs
  // we can read -- is the starting balance, at the first known event.
  const start = current == null ? 0 : current - explained;
  const firstT = Math.min(
    events.length ? events[0].t : now,
    ...act.map((a) => Number(a.timestamp)).filter(Number.isFinite),
  );

  const points: ValuePoint[] = [];
  let v = start;
  points.push({ t: firstT - 60, v: +start.toFixed(2), kind: "start" });
  for (const e of events) {
    v += e.v;
    points.push({ t: e.t, v: +v.toFixed(2), kind: e.kind });
  }
  points.push({ t: now, v: +(current ?? v).toFixed(2), kind: "now" });

  const out = {
    wallet: w,
    current: current == null ? null : +current.toFixed(2),
    cash: cash == null ? null : +cash.toFixed(2),
    openCost: +openCost.toFixed(2),
    startingBalance: +start.toFixed(2),
    points,
  };
  seriesCache.set(w, { at: Date.now(), out });
  return out;
}

/** Scan every app user's Polymarket wallet a little further. */
export async function scanAllWallets(): Promise<void> {
  const { rows } = await pool.query(
    `SELECT DISTINCT lower(trading_address) AS w FROM public.users WHERE trading_address IS NOT NULL`);
  for (const r of rows) {
    try {
      const n = await scanWalletTransfers(r.w, 60);
      if (n) console.log(`[pm-wallets] ${r.w.slice(0, 10)}… +${n} transfer(s)`);
    } catch (e: any) {
      console.warn(`[pm-wallets] scan ${r.w.slice(0, 10)}… failed: ${e?.message || e}`);
    }
  }
}

export function startPmWalletScanCron(): void {
  if (String(process.env.PM_WALLET_SCAN_ENABLED || "true").toLowerCase() === "false") {
    console.log("[pm-wallets] disabled (PM_WALLET_SCAN_ENABLED=false)");
    return;
  }
  const every = Number(process.env.PM_WALLET_SCAN_INTERVAL_MS || 5 * 60_000);
  console.log(`[pm-wallets] transfer scan enabled — every ${Math.round(every / 60000)}m`);
  let running = false;
  const tick = async () => {
    if (running) return;
    running = true;
    try { await scanAllWallets(); } finally { running = false; }
  };
  setTimeout(() => void tick(), 60_000);
  setInterval(() => void tick(), every);
}
