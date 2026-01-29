// src/workers/backfillTrades.ts
import { ENV } from "../config/env";
import { subgraphQuery } from "../subgraph/client";
import { upsertUserTradesAndGames } from "../services/persistTrades";

/**
 * One-time backfill:
 * - Discover all distinct user addresses from the subgraph (trades + bets).
 * - For each user, page through ALL trades and bets (window=ALL by default).
 * - Normalize + dedupe (same logic as cacheRefresh), then UPSERT into Postgres tables.
 *
 * Run (after build):
 *   node dist/workers/backfillTrades.js
 *
 * Optional env vars:
 *   BACKFILL_LEAGUES= NFL,NBA,NHL,MLB,EPL,UCL   (default all)
 *   BACKFILL_RANGE= ALL | D30 | D90            (default ALL)
 *   BACKFILL_CONCURRENCY= 3                    (default 3)
 *   BACKFILL_SLEEP_MS= 150                     (default 150)
 *   BACKFILL_MAX_USERS= 0                      (0 = no limit)
 *   BACKFILL_START_INDEX= 0                    (skip first N users from discovered set)
 */

const DEFAULT_LEAGUES = ["NFL", "NBA", "NHL", "MLB", "EPL", "UCL"];

const Q_USERS_FROM_TRADES = `
query UsersFromTrades($leagues:[String!]!, $start:BigInt!, $end:BigInt!, $first:Int!, $skip:Int!) {
  trades(
    first: $first
    skip: $skip
    where: { game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
    orderBy: timestamp
    orderDirection: desc
  ) {
    user { id }
  }
}
`;

const Q_USERS_FROM_BETS = `
query UsersFromBets($leagues:[String!]!, $start:BigInt!, $end:BigInt!, $first:Int!, $skip:Int!) {
  bets(
    first: $first
    skip: $skip
    where: { game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
    orderBy: timestamp
    orderDirection: desc
  ) {
    user { id }
  }
}
`;

// Same activity query you already use, but we WILL paginate skipTrades/skipBets.
const Q_USER_ACTIVITY_PAGE_PAGED = `
query UserActivityPagePaged(
  $user: String!
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skipTrades: Int!
  $skipBets: Int!
) {
  _meta { block { number } }

  trades(
    first: $first
    skip: $skipTrades
    where: {
      user: $user
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    type
    side
    timestamp
    txHash
    spotPriceBps
    avgPriceBps
    grossInDec
    grossOutDec
    feeDec
    netStakeDec
    netOutDec
    costBasisClosedDec
    realizedPnlDec
    game { id league lockTime isFinal winnerSide winnerTeamCode teamACode teamBCode teamAName teamBName }
  }

  bets(
    first: $first
    skip: $skipBets
    where: {
      user: $user
      game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end }
    }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    timestamp
    side
    amountDec
    grossAmount
    fee
    priceBps
    sharesOutDec
    game { id league lockTime isFinal winnerSide winnerTeamCode teamACode teamBCode teamAName teamBName }
  }
}
`;

function parseCsvEnv(name: string, fallback: string[]) {
  const v = String(process.env[name] || "").trim();
  if (!v) return fallback;
  return v
    .split(",")
    .map((s) => s.trim().toUpperCase())
    .filter(Boolean);
}

function toNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function tradesWindowFromRange(range: string | undefined) {
  const r = String(range || "ALL").toUpperCase();
  const nowSec = Math.floor(Date.now() / 1000);
  const farFuture = 4102444800; // 2100-01-01

  if (r === "D30") return { start: nowSec - 30 * 86400, end: nowSec };
  if (r === "D90") return { start: nowSec - 90 * 86400, end: nowSec };
  return { start: 0, end: farFuture };
}

function canonicalActivityId(id: string): string {
  return String(id || "")
    .replace(/^trade-trade-/, "")
    .replace(/^bet-bet-/, "")
    .replace(/^trade-/, "")
    .replace(/^bet-/, "");
}

function dedupeActivityRows(rows: any[]): { rows: any[]; dropped: number } {
  const bestByKey = new Map<string, any>();

  for (const r of rows || []) {
    const key = canonicalActivityId(r?.id);
    if (!key) continue;

    const prev = bestByKey.get(key);
    if (!prev) {
      bestByKey.set(key, r);
      continue;
    }

    const rIsTrade = r?.__source === "trade";
    const pIsTrade = prev?.__source === "trade";

    const rHasTx = !!r?.txHash;
    const pHasTx = !!prev?.txHash;

    const takeR = (rIsTrade && !pIsTrade) || (rHasTx && !pHasTx);
    if (takeR) bestByKey.set(key, r);
  }

  const out: any[] = [];
  const seen = new Set<string>();

  for (const r of rows || []) {
    const key = canonicalActivityId(r?.id);
    if (!key || seen.has(key)) continue;
    const best = bestByKey.get(key);
    if (best) out.push(best);
    seen.add(key);
  }

  return { rows: out, dropped: Math.max(0, (rows?.length || 0) - out.length) };
}

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function discoverUsers(leagues: string[], start: number, end: number) {
  const first = 1000;
  const users = new Set<string>();

  async function pageUsers(query: string) {
    let skip = 0;
    for (;;) {
      const data = await subgraphQuery<any>(query, {
        leagues,
        start: String(start),
        end: String(end),
        first,
        skip,
      });

      const arr = (query === Q_USERS_FROM_TRADES ? data?.trades : data?.bets) || [];
      for (const row of arr) {
        const id = String(row?.user?.id || "").toLowerCase();
        if (id) users.add(id);
      }

      if (!Array.isArray(arr) || arr.length < first) break;
      skip += first;
      // small pause so we don’t hammer
      await sleep(75);
    }
  }

  console.log(`[backfill] discovering users (trades)…`);
  await pageUsers(Q_USERS_FROM_TRADES);

  console.log(`[backfill] discovering users (bets)…`);
  await pageUsers(Q_USERS_FROM_BETS);

  return [...users.values()];
}

async function backfillUser(opts: {
  user: string;
  leagues: string[];
  start: number;
  end: number;
  pageSize: number;
  sleepMs: number;
}) {
  const user = opts.user.toLowerCase();
  const first = Math.min(Math.max(1, opts.pageSize), 1000);

  let skipTrades = 0;
  let skipBets = 0;

  let totalPersistedRows = 0;
  let totalDroppedDupes = 0;
  let loops = 0;

  for (;;) {
    loops++;

    const data = await subgraphQuery<any>(Q_USER_ACTIVITY_PAGE_PAGED, {
      user,
      leagues: opts.leagues,
      start: String(opts.start),
      end: String(opts.end),
      first,
      skipTrades,
      skipBets,
    });

    const trades = Array.isArray(data?.trades) ? data.trades : [];
    const bets = Array.isArray(data?.bets) ? data.bets : [];

    // Normalize bets into BUY-like rows
    const betAsTrades = bets.map((b: any) => {
      const g = b?.game ?? {};
      const ts = toNum(b?.timestamp);

      const priceBps = b?.priceBps ?? b?.spotPriceBps ?? b?.avgPriceBps ?? null;
      const sharesOutDec = b?.sharesOutDec ?? b?.sharesOut ?? null;

      return {
        id: `bet-${b.id}`,
        type: "BUY",
        side: b?.side ?? "A",
        timestamp: ts,
        txHash: b?.txHash ?? null,

        spotPriceBps: priceBps,
        avgPriceBps: priceBps,

        priceBps,
        sharesOutDec,
        sharesOut: b?.sharesOut ?? null,

        grossInDec: b?.grossAmount ?? "0",
        grossOutDec: "0",
        feeDec: b?.fee ?? "0",
        netStakeDec: b?.amountDec ?? "0",
        netOutDec: "0",
        costBasisClosedDec: "0",
        realizedPnlDec: "0",

        game: g,
        __source: "bet",
      };
    });

    const tradeRows = trades.map((t: any) => ({
      ...t,
      id: `trade-${t.id}`,
      timestamp: toNum(t?.timestamp),
      __source: "trade",
    }));

    const mergedSorted = [...tradeRows, ...betAsTrades].sort((a, b) => {
      const dt = toNum(b?.timestamp) - toNum(a?.timestamp);
      if (dt !== 0) return dt;
      return String(b?.id || "").localeCompare(String(a?.id || ""));
    });

    const deduped = dedupeActivityRows(mergedSorted);
    totalDroppedDupes += deduped.dropped;

    // Persist this chunk (UPSERT makes duplicates safe)
    if (deduped.rows.length) {
      await upsertUserTradesAndGames({ user, tradeRows: deduped.rows });
      totalPersistedRows += deduped.rows.length;
    }

    // Advance pagination
    // We page trades and bets independently.
    if (trades.length > 0) skipTrades += trades.length;
    if (bets.length > 0) skipBets += bets.length;

    const doneTrades = trades.length < first;
    const doneBets = bets.length < first;

    if (doneTrades && doneBets) break;

    if (opts.sleepMs > 0) await sleep(opts.sleepMs);
  }

  return { user, loops, totalPersistedRows, totalDroppedDupes };
}

async function run() {
if (!ENV.SUBGRAPH_QUERY_URL) {
    console.log(`[backfill] ENV.SUBGRAPH_URL missing — cannot query subgraph.`);
    process.exit(1);
  }

  const leagues = parseCsvEnv("BACKFILL_LEAGUES", DEFAULT_LEAGUES);
  const range = String(process.env.BACKFILL_RANGE || "ALL").toUpperCase();
  const { start, end } = tradesWindowFromRange(range);

  const concurrency = Math.max(1, Math.min(10, Number(process.env.BACKFILL_CONCURRENCY || 3)));
  const sleepMs = Math.max(0, Number(process.env.BACKFILL_SLEEP_MS || 150));
  const maxUsers = Math.max(0, Number(process.env.BACKFILL_MAX_USERS || 0));
  const startIndex = Math.max(0, Number(process.env.BACKFILL_START_INDEX || 0));

  console.log(
    `[backfill] start: leagues=${leagues.join(",")} range=${range} concurrency=${concurrency} sleepMs=${sleepMs}`
  );

  const usersAll = await discoverUsers(leagues, start, end);
  const users = usersAll.slice(startIndex, maxUsers > 0 ? startIndex + maxUsers : undefined);

  console.log(`[backfill] discovered ${usersAll.length} users, processing ${users.length} users…`);

  let idx = 0;
  let ok = 0;
  let err = 0;

  const workers = Array.from({ length: concurrency }, () => (async () => {
    for (;;) {
      const my = idx++;
      if (my >= users.length) return;

      const user = users[my];
      const label = `[backfill] (${my + 1}/${users.length}) ${user}`;

      try {
        const out = await backfillUser({
          user,
          leagues,
          start,
          end,
          pageSize: 1000,
          sleepMs,
        });
        ok++;
        console.log(
          `${label} ok loops=${out.loops} persisted=${out.totalPersistedRows} droppedDupes=${out.totalDroppedDupes}`
        );
      } catch (e: any) {
        err++;
        console.log(`${label} ERR: ${String(e?.message || e)}`);
      }
    }
  })());

  await Promise.all(workers);

  console.log(`[backfill] done ok=${ok} err=${err} totalUsers=${users.length}`);
}

run().catch((e) => {
  console.log(`[backfill] fatal: ${String((e as any)?.message || e)}`);
  process.exit(1);
});
