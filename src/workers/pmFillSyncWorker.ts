// ---------------------------------------------------------------------------
// pm resting-fill sync
//
// A Polymarket limit order that RESTS fills later, while nobody is looking: the
// app only logs what fills at placement (pmLimitFill) and market orders. So a
// resting fill never reached public.user_trade_events -- Profile History, the
// fee columns, ROI and the settlement worker's claim payouts (which sum BUY
// rows) all missed it. Same for orders re-placed after kickoff by the
// carry-over worker, which no browser is watching at all.
//
// This worker closes that gap from the venue's own record. Every user with a
// recorded limit order (public.pm_limit_orders) has their Polymarket wallet's
// trades read from the public data-api (not geoblocked, no auth), and each
// trade that belongs to one of those orders is written as a trade row.
//
// WHICH TRADES BELONG TO AN ORDER. The activity feed does not name the order,
// so a trade is attributed to an order on the same market, outcome and side,
// inside the order's lifetime, by price:
//   - an order the user placed: a MAKER fill, which executes exactly at the
//     limit price, and only 20s+ after placement (anything earlier is the
//     placement fill, which the app already logged);
//   - an order the carry-over worker re-placed: any price at or better than the
//     limit from the moment it was placed (no app logged its placement fill).
// A trade the app already logged (a direct row on the same game, same type,
// within 60s) is never counted twice, and fills are capped at the order size.
//
// Deterministic: every pass recomputes the attribution from scratch over the
// window and inserts with ON CONFLICT DO NOTHING, so re-runs are idempotent and
// pm_limit_orders.filled_shares always reflects what was found. A fully filled
// live order is closed as 'filled' (the app cannot tell -- see pmOpenOrders).
//
// Fees: Polymarket's sports fee is taker-only (feeSchedule.takerOnly), and the
// BlockPools builder fee is 0% maker. Maker fills are recorded with zero fee
// and id 'pmfill-m-…' (the user_trade_events trigger keys the builder fee on
// it); a carried order's crossing fill is a taker fill ('pmfill-t-…').
//
// Disable with PM_FILL_SYNC_ENABLED=false.
// ---------------------------------------------------------------------------
import { pool } from "../db";
import { upsertUserTradesAndGames } from "../services/persistTrades";
import { bustUserCache } from "../services/cacheRefresh";

const DATA_API = "https://data-api.polymarket.com";
const GAMMA = (process.env.GAMMA_API_URL || "https://gamma-api.polymarket.com").replace(/\/+$/, "");
const INTERVAL_MS = Number(process.env.PM_FILL_SYNC_INTERVAL_MS || 2 * 60_000);
/** Orders looked at: live, or closed within this many days. */
const WINDOW_DAYS = 3;
/**
 * Never attribute trades before this (unix s). Fills before the worker existed
 * were partly logged by hand-tuned paths; re-deriving them risks a double
 * count. Backfill older ones deliberately, not by accident.
 */
const SINCE = Number(process.env.PM_FILL_SYNC_SINCE || 1792022400); // 2026-10-15 00:00 UTC
/** A user-placed order's fills inside this many seconds are the placement fill. */
const PLACEMENT_S = 20;
/** A carried order's fills inside this many seconds of placement crossed the book. */
const CARRY_CROSS_S = 15;
const EPS = 1e-6;

type Order = {
  id: string;
  owner_address: string | null;
  market_id: string;
  outcome_index: number;
  side: "buy" | "sell";
  price: number;
  shares: number;
  token_id: string | null;
  game_id: string | null;
  meta: any;
  status: string;
  created: number; // unix s
  closed: number | null;
  carried: boolean;
  wallet: string;
};

type Trade = {
  tx: string;
  ts: number;
  side: "buy" | "sell";
  asset: string;
  outcomeIndex: number;
  price: number;
  size: number;
  usdc: number;
};

type Game = {
  gameId: string; league: string | null; lockTime: number | null;
  teamACode: string | null; teamBCode: string | null;
  teamAName: string | null; teamBName: string | null;
  marketType: string; marketQuestion: string | null;
  takerFeeRate: number;
};

async function getJson(url: string): Promise<any> {
  const r = await fetch(url, { headers: { accept: "application/json" } });
  if (!r.ok) throw new Error(`${r.status} ${url}`);
  return r.json();
}

const parseArr = (x: any): any[] => { try { return Array.isArray(x) ? x : JSON.parse(x || "[]"); } catch { return []; } };

/** Game identity for a market: the order's own record first, Gamma second. */
async function gameFor(marketId: string, orders: Order[]): Promise<Game | null> {
  let gamma: any = null;
  try {
    const list = await getJson(`${GAMMA}/markets?condition_ids=${marketId}&closed=true`);
    gamma = (Array.isArray(list) && list[0]) || null;
    if (!gamma) {
      const open = await getJson(`${GAMMA}/markets?condition_ids=${marketId}`);
      gamma = (Array.isArray(open) && open[0]) || null;
    }
  } catch (e: any) {
    console.warn(`[pm-fills] gamma ${marketId.slice(0, 10)}…: ${e?.message || e}`);
  }
  let takerFeeRate = 0;
  try {
    const fs = typeof gamma?.feeSchedule === "string" ? JSON.parse(gamma.feeSchedule) : gamma?.feeSchedule;
    if (gamma?.feesEnabled && Number(fs?.rate) > 0) takerFeeRate = Number(fs.rate);
  } catch { /* no fee info -> 0 */ }

  const withMeta = orders.find((o) => o.game_id && o.meta) || orders.find((o) => o.game_id);
  if (withMeta?.game_id) {
    const m = withMeta.meta || {};
    return {
      gameId: String(withMeta.game_id).toLowerCase(),
      league: m.league ?? null,
      lockTime: Number(m.lockTime) || null,
      teamACode: m.teamACode ?? null, teamBCode: m.teamBCode ?? null,
      teamAName: m.teamAName ?? null, teamBName: m.teamBName ?? null,
      marketType: m.marketType || "GAME",
      marketQuestion: m.marketQuestion ?? null,
      takerFeeRate,
    };
  }

  // Older orders carry no game id: rebuild the app's id from the market slug
  // ("mlb-cle-tb-2026-10-13" + start -> "mlb-cle-tb-pm-<epoch>"), which is how
  // the frontend registry builds it for two-team moneylines.
  const slug = String(gamma?.slug || "");
  const sm = slug.match(/^([a-z]+)-([a-z0-9]+)-([a-z0-9]+)-\d{4}-\d{2}-\d{2}$/i);
  const lock = Math.floor(Date.parse(String(gamma?.gameStartTime || "").replace(" ", "T").replace(/\+00$/, "Z")) / 1000);
  if (!sm || !(lock > 0)) return null;
  const outcomes = parseArr(gamma?.outcomes);
  return {
    gameId: `${sm[1]}-${sm[2]}-${sm[3]}-pm-${lock}`.toLowerCase(),
    league: sm[1].toUpperCase(),
    lockTime: lock,
    teamACode: sm[2].toUpperCase(), teamBCode: sm[3].toUpperCase(),
    teamAName: outcomes[0] ?? null, teamBName: outcomes[1] ?? null,
    marketType: "GAME",
    marketQuestion: gamma?.question ?? null,
    takerFeeRate,
  };
}

async function tradesFor(wallet: string, marketId: string, since: number): Promise<Trade[]> {
  const list = await getJson(
    `${DATA_API}/activity?user=${wallet}&market=${marketId}&type=TRADE&limit=500&sortDirection=ASC`);
  return (Array.isArray(list) ? list : [])
    .filter((t: any) => String(t.type) === "TRADE" && Number(t.timestamp) >= since)
    .map((t: any) => ({
      tx: String(t.transactionHash || "").toLowerCase(),
      ts: Number(t.timestamp),
      side: String(t.side).toUpperCase() === "SELL" ? "sell" as const : "buy" as const,
      asset: String(t.asset || ""),
      outcomeIndex: Number(t.outcomeIndex),
      price: Number(t.price),
      size: Number(t.size),
      usdc: Number(t.usdcSize),
    }))
    .filter((t) => t.tx && t.size > 0 && t.price > 0)
    .sort((a, b) => a.ts - b.ts || a.tx.localeCompare(b.tx));
}

async function syncGroup(wallet: string, marketId: string, orders: Order[]) {
  const since = Math.max(SINCE, Math.min(...orders.map((o) => o.created)) - 60);
  const trades = await tradesFor(wallet, marketId, since);
  const game = await gameFor(marketId, orders);
  const owner = String(orders.find((o) => o.owner_address)?.owner_address || "").toLowerCase();

  // Direct rows the app already wrote for this game (placement fills, market
  // orders): a trade within 60s of one of the same type is theirs.
  const direct = game && owner
    ? (await pool.query(
        `SELECT type, timestamp FROM public.user_trade_events
          WHERE lower(user_address) = $1 AND game_id = $2 AND id NOT LIKE 'pmfill-%'
            AND type IN ('BUY','SELL') AND timestamp >= $3`,
        [owner, game.gameId, since - 120])).rows.map((r: any) => ({ type: String(r.type), ts: Number(r.timestamp) }))
    : [];

  const filled = new Map<string, number>(orders.map((o) => [o.id, 0]));
  const rows: any[] = [];
  for (const t of trades) {
    const type = t.side === "sell" ? "SELL" : "BUY";
    if (direct.some((d) => d.type === type && Math.abs(d.ts - t.ts) <= 60)) continue;

    const order = orders
      .filter((o) => {
        if (o.side !== t.side) return false;
        if (o.token_id ? o.token_id !== t.asset : o.outcome_index !== t.outcomeIndex) return false;
        if (o.closed != null && t.ts > o.closed + 5) return false;
        if ((o.shares - (filled.get(o.id) || 0)) <= EPS) return false;
        if (o.carried) {
          if (t.ts < o.created - 5) return false;
          return o.side === "sell" ? t.price >= o.price - EPS : t.price <= o.price + EPS;
        }
        return t.ts >= o.created + PLACEMENT_S && Math.abs(t.price - o.price) < 1e-4;
      })
      .sort((a, b) => a.created - b.created)[0];
    if (!order) continue;

    const take = Math.min(t.size, order.shares - (filled.get(order.id) || 0));
    filled.set(order.id, (filled.get(order.id) || 0) + take);
    if (!game || !owner) continue; // counted toward the order; cannot be written

    // The feed's usdcSize is what actually moved, fees included: a buy's cash
    // out (notional + fees), a sell's cash in (notional - fees). A maker fill
    // pays none, so there it equals the notional.
    const taker = order.carried && t.ts <= order.created + CARRY_CROSS_S;
    const notional = take * t.price;
    const cash = t.usdc * (take / t.size);
    const fee = taker ? game.takerFeeRate * take * t.price * (1 - t.price) : 0;
    const isSell = type === "SELL";
    const code = order.outcome_index === 1 ? game.teamBCode : game.teamACode;
    rows.push({
      id: `pmfill-${taker ? "t" : "m"}-${t.tx}-${order.id.slice(2, 14).toLowerCase()}`,
      type,
      side: order.outcome_index === 1 ? "B" : "A",
      outcomeIndex: order.outcome_index,
      outcomeCode: code,
      timestamp: t.ts,
      txHash: t.tx,
      spotPriceBps: Math.round(t.price * 10000),
      avgPriceBps: Math.round(t.price * 10000),
      grossInDec: isSell ? "0" : Math.max(cash, notional).toFixed(6),
      grossOutDec: isSell ? notional.toFixed(6) : "0",
      feeDec: fee.toFixed(6),
      netStakeDec: isSell ? "0" : notional.toFixed(6),
      netOutDec: isSell ? Math.min(cash, notional).toFixed(6) : "0",
      costBasisClosedDec: "0",
      realizedPnlDec: "0",
      game: {
        id: game.gameId, league: game.league, lockTime: game.lockTime,
        teamACode: game.teamACode, teamBCode: game.teamBCode,
        teamAName: game.teamAName, teamBName: game.teamBName,
        marketType: game.marketType, marketQuestion: game.marketQuestion,
      },
      __source: "pm-fill-sync",
    });
  }

  if (rows.length) {
    const fresh = await pool.query(
      `SELECT id FROM public.user_trade_events WHERE id = ANY($1::text[])`, [rows.map((r) => r.id)]);
    const have = new Set(fresh.rows.map((r: any) => r.id));
    const toWrite = rows.filter((r) => !have.has(r.id));
    if (toWrite.length) {
      await upsertUserTradesAndGames({ user: owner, tradeRows: toWrite });
      bustUserCache(owner);
      console.log(`[pm-fills] ${owner.slice(0, 10)}… ${game?.gameId}: logged ${toWrite.length} resting fill(s)`);
    }
  }

  for (const o of orders) {
    const f = filled.get(o.id) || 0;
    await pool.query(
      `UPDATE public.pm_limit_orders
          SET filled_shares = $2,
              status = CASE WHEN status = 'live' AND $2 >= shares - GREATEST(0.01, shares * 0.001) THEN 'closed' ELSE status END,
              closed_reason = CASE WHEN status = 'live' AND $2 >= shares - GREATEST(0.01, shares * 0.001) THEN 'filled' ELSE closed_reason END,
              closed_at = CASE WHEN status = 'live' AND $2 >= shares - GREATEST(0.01, shares * 0.001) THEN now() ELSE closed_at END,
              updated_at = CASE WHEN filled_shares IS DISTINCT FROM $2 THEN now() ELSE updated_at END
        WHERE id = $1`,
      [o.id, f.toFixed(6)]);
  }
}

let running = false;

export async function runPmFillSyncOnce(): Promise<void> {
  if (running) return;
  running = true;
  try {
    const { rows } = await pool.query(
      `SELECT o.id, o.owner_address, o.market_id, o.outcome_index, o.side, o.price, o.shares,
              o.token_id, o.game_id, o.meta, o.status, o.carried_from,
              extract(epoch from o.created_at)::bigint AS created,
              extract(epoch from o.closed_at)::bigint  AS closed,
              lower(u.trading_address) AS wallet
         FROM public.pm_limit_orders o
         JOIN public.users u ON u.id = o.user_id
        WHERE u.trading_address IS NOT NULL
          AND (o.status = 'live' OR o.closed_at > now() - make_interval(days => $1))
          AND (o.closed_at IS NULL OR o.closed_at > to_timestamp($2))`,
      [WINDOW_DAYS, SINCE]);
    const groups = new Map<string, Order[]>();
    for (const r of rows) {
      const o: Order = {
        id: r.id, owner_address: r.owner_address, market_id: String(r.market_id).toLowerCase(),
        outcome_index: Number(r.outcome_index), side: r.side, price: Number(r.price), shares: Number(r.shares),
        token_id: r.token_id, game_id: r.game_id, meta: r.meta, status: r.status,
        created: Number(r.created), closed: r.closed == null ? null : Number(r.closed),
        carried: !!r.carried_from, wallet: r.wallet,
      };
      const k = `${o.wallet}|${o.market_id}`;
      (groups.get(k) || groups.set(k, []).get(k)!).push(o);
    }
    for (const [k, orders] of groups) {
      const [wallet, market] = k.split("|");
      try { await syncGroup(wallet, market, orders); }
      catch (e: any) { console.warn(`[pm-fills] ${wallet.slice(0, 10)}… ${market.slice(0, 10)}…: ${e?.message || e}`); }
    }
  } catch (e: any) {
    console.error(`[pm-fills] run error: ${e?.message || e}`);
  } finally {
    running = false;
  }
}

export function startPmFillSyncCron(): void {
  if (String(process.env.PM_FILL_SYNC_ENABLED || "true").toLowerCase() === "false") {
    console.log("[pm-fills] disabled (PM_FILL_SYNC_ENABLED=false)");
    return;
  }
  console.log(`[pm-fills] cron enabled — every ${Math.round(INTERVAL_MS / 60000)}m`);
  setTimeout(() => void runPmFillSyncOnce(), 45_000);
  setInterval(() => void runPmFillSyncOnce(), INTERVAL_MS);
}
