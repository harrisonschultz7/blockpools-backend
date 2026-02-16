// src/workers/backfillTrades.ts
import { ENV } from "../config/env";
import { subgraphQuery } from "../subgraph/client";
import { upsertUserTradesAndGames, type GameMetaInput } from "../services/persistTrades";

// ✅ Update this path if needed
import gamesJson from "../data/games.json";

/**
 * One-time backfill:
 * - Discover all distinct user addresses from the subgraph (trades + bets + claims).
 * - For each user, page through ALL trades + bets + claims (window=ALL by default).
 * - Normalize + dedupe (trades+bets only), then UPSERT into Postgres tables.
 *
 * CRITICAL FIX:
 * - Pull outcomeIndex/outcomeCode from `trades` (MULTI canonical)
 * - When converting legacy `bets` -> tradeRows, DERIVE outcomeIndex/outcomeCode from side + team codes.
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
  ) { user { id } }
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
  ) { user { id } }
}
`;

// ✅ include users that only have claims
const Q_USERS_FROM_CLAIMS = `
query UsersFromClaims($leagues:[String!]!, $start:BigInt!, $end:BigInt!, $first:Int!, $skip:Int!) {
  claims(
    first: $first
    skip: $skip
    where: { game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
    orderBy: timestamp
    orderDirection: desc
  ) { user { id } }
}
`;

/**
 * CRITICAL:
 * - `trades` are canonical for MULTI and include outcomeIndex/outcomeCode.
 * - `bets` are legacy binary and only have side; we convert them.
 */
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
  trades(
    first: $first
    skip: $skipTrades
    where: { user: $user, game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    type
    timestamp
    txHash

    side
    outcomeIndex
    outcomeCode

    spotPriceBps
    avgPriceBps
    grossInDec
    grossOutDec
    feeDec
    netStakeDec
    netOutDec
    costBasisClosedDec
    realizedPnlDec

    game {
      id league lockTime isFinal winnerSide winnerTeamCode
      marketType outcomesCount resolutionType winningOutcomeIndex
      teamACode teamBCode teamAName teamBName
    }
  }

  bets(
    first: $first
    skip: $skipBets
    where: { user: $user, game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
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

    game {
      id league lockTime isFinal winnerSide winnerTeamCode
      marketType outcomesCount resolutionType winningOutcomeIndex
      teamACode teamBCode teamAName teamBName
    }
  }
}
`;

// ✅ claims paged
const Q_USER_CLAIMS_PAGED = `
query UserClaimsPaged(
  $user: String!
  $leagues: [String!]!
  $start: BigInt!
  $end: BigInt!
  $first: Int!
  $skip: Int!
) {
  claims(
    first: $first
    skip: $skip
    where: { user: $user, game_: { league_in: $leagues, lockTime_gte: $start, lockTime_lte: $end } }
    orderBy: timestamp
    orderDirection: desc
  ) {
    id
    amountDec
    timestamp
    txHash
    game {
      id league lockTime isFinal winnerSide winnerTeamCode
      marketType outcomesCount resolutionType winningOutcomeIndex
      teamACode teamBCode teamAName teamBName
    }
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
  const farFuture = 4102444800;
  if (r === "D30") return { start: nowSec - 30 * 86400, end: nowSec };
  if (r === "D90") return { start: nowSec - 90 * 86400, end: nowSec };
  return { start: 0, end: farFuture };
}

function canonicalActivityId(id: string): string {
  return String(id || "")
    .replace(/^trade-trade-/, "")
    .replace(/^bet-bet-/, "")
    .replace(/^trade-/, "")
    .replace(/^bet-/, "")
    .replace(/^claim-/, "");
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

/**
 * games.json shape:
 * { "NFL": [ {contractAddress, ...} ], "EPL": [ ... ], ... }
 * We normalize into a map keyed by lowercased contractAddress.
 */
function buildGameMetaByAddr(json: any): Record<string, GameMetaInput> {
  const out: Record<string, GameMetaInput> = {};
  const root = json && typeof json === "object" ? json : {};

  for (const leagueKey of Object.keys(root)) {
    const arr = Array.isArray(root[leagueKey]) ? root[leagueKey] : [];
    for (const g of arr) {
      const addr = String(g?.contractAddress || "").toLowerCase().trim();
      if (!addr) continue;

      const teamACode = g?.teamACode ?? g?.teamA ?? null;
      const teamBCode = g?.teamBCode ?? g?.teamB ?? null;

      out[addr] = {
        league: (g?.league ?? leagueKey) ? String(g?.league ?? leagueKey) : undefined,
        lockTime: g?.lockTime != null ? toNum(g.lockTime) : undefined,

        teamACode: teamACode != null ? String(teamACode) : undefined,
        teamBCode: teamBCode != null ? String(teamBCode) : undefined,
        teamAName: g?.teamAName != null ? String(g.teamAName) : undefined,
        teamBName: g?.teamBName != null ? String(g.teamBName) : undefined,

        marketType: g?.marketType != null ? String(g.marketType) : undefined,
        outcomesCount: g?.outcomesCount != null ? toNum(g.outcomesCount) : undefined,
        resolutionType: g?.resolutionType != null ? String(g.resolutionType) : undefined,
        winningOutcomeIndex: g?.winningOutcomeIndex != null ? toNum(g.winningOutcomeIndex) : undefined,

        topic: g?.topic != null ? String(g.topic) : undefined,
        marketQuestion: g?.marketQuestion != null ? String(g.marketQuestion) : undefined,
        marketShort: g?.marketShort != null ? String(g.marketShort) : undefined,
      };
    }
  }

  return out;
}

function attachMetaToGame(game: any, metaByAddr: Record<string, GameMetaInput>) {
  const id = String(game?.id || "").toLowerCase().trim();
  if (!id) return game;

  const meta = metaByAddr[id];
  if (!meta) return game;

  // Only fill missing fields; never stomp subgraph values.
  return {
    ...game,
    league: game?.league ?? meta.league,
    lockTime: game?.lockTime ?? meta.lockTime,

    teamACode: game?.teamACode ?? meta.teamACode,
    teamBCode: game?.teamBCode ?? meta.teamBCode,
    teamAName: game?.teamAName ?? meta.teamAName,
    teamBName: game?.teamBName ?? meta.teamBName,

    marketType: game?.marketType ?? meta.marketType,
    outcomesCount: game?.outcomesCount ?? meta.outcomesCount,
    resolutionType: game?.resolutionType ?? meta.resolutionType,
    winningOutcomeIndex: game?.winningOutcomeIndex ?? meta.winningOutcomeIndex,

    topic: game?.topic ?? meta.topic,
    marketQuestion: game?.marketQuestion ?? meta.marketQuestion,
    marketShort: game?.marketShort ?? meta.marketShort,
  };
}

function deriveOutcomeFromSide(side: any, game: any): { outcomeIndex: number | null; outcomeCode: string | null } {
  const s = String(side || "").toUpperCase().trim();
  const a = game?.teamACode ? String(game.teamACode).toUpperCase().trim() : null;
  const b = game?.teamBCode ? String(game.teamBCode).toUpperCase().trim() : null;

  if (s === "A") return { outcomeIndex: 0, outcomeCode: a };
  if (s === "B") return { outcomeIndex: 1, outcomeCode: b };
  return { outcomeIndex: null, outcomeCode: null };
}

async function discoverUsers(leagues: string[], start: number, end: number) {
  const first = 1000;
  const users = new Set<string>();

  async function pageUsers(query: string, key: "trades" | "bets" | "claims") {
    let skip = 0;
    for (;;) {
      const data = await subgraphQuery<any>(query, {
        leagues,
        start: String(start),
        end: String(end),
        first,
        skip,
      });

      const arr = data?.[key] || [];
      for (const row of arr) {
        const id = String(row?.user?.id || "").toLowerCase();
        if (id) users.add(id);
      }

      if (!Array.isArray(arr) || arr.length < first) break;
      skip += first;
      await sleep(75);
    }
  }

  console.log(`[backfill] discovering users (trades)…`);
  await pageUsers(Q_USERS_FROM_TRADES, "trades");

  console.log(`[backfill] discovering users (bets)…`);
  await pageUsers(Q_USERS_FROM_BETS, "bets");

  console.log(`[backfill] discovering users (claims)…`);
  await pageUsers(Q_USERS_FROM_CLAIMS, "claims");

  return [...users.values()];
}

async function backfillUser(opts: {
  user: string;
  leagues: string[];
  start: number;
  end: number;
  pageSize: number;
  sleepMs: number;
  metaByAddr: Record<string, GameMetaInput>;
}) {
  const user = opts.user.toLowerCase();
  const first = Math.min(Math.max(1, opts.pageSize), 1000);

  let skipTrades = 0;
  let skipBets = 0;
  let skipClaims = 0;

  let totalPersistedRows = 0;
  let totalDroppedDupes = 0;
  let loops = 0;

  // ---- 1) trades+bets paged
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

    // ✅ Convert legacy bets to tradeRows with DERIVED outcomeIndex/outcomeCode
    const betAsTrades = bets.map((b: any) => {
      const g0 = b?.game ?? {};
      const g = attachMetaToGame(g0, opts.metaByAddr);

      const ts = toNum(b?.timestamp);
      const priceBps = b?.priceBps ?? null;

      const { outcomeIndex, outcomeCode } = deriveOutcomeFromSide(b?.side, g);

      return {
        id: `bet-${b.id}`,
        type: "BUY",
        side: b?.side ?? null,
        outcomeIndex,
        outcomeCode,
        timestamp: ts,
        txHash: b?.txHash ?? null,

        spotPriceBps: priceBps,
        avgPriceBps: priceBps,

        grossInDec: b?.grossAmount ?? "0",
        grossOutDec: "0",
        feeDec: b?.fee ?? "0",
        netStakeDec: b?.amountDec ?? "0",
        netOutDec: "0",
        costBasisClosedDec: "0",
        realizedPnlDec: "0",

        sharesOutDec: b?.sharesOutDec ?? null,

        game: g,
        __source: "bet",
      };
    });

    // ✅ Trades are canonical for MULTI — keep outcomeIndex/outcomeCode from subgraph
    const tradeRows = trades.map((t: any) => {
      const g0 = t?.game ?? {};
      const g = attachMetaToGame(g0, opts.metaByAddr);

      return {
        ...t,
        id: `trade-${t.id}`,
        timestamp: toNum(t?.timestamp),
        outcomeIndex: t?.outcomeIndex ?? null,
        outcomeCode: t?.outcomeCode ?? null,
        game: g,
        __source: "trade",
      };
    });

    const mergedSorted = [...tradeRows, ...betAsTrades].sort((a, b) => {
      const dt = toNum(b?.timestamp) - toNum(a?.timestamp);
      if (dt !== 0) return dt;
      return String(b?.id || "").localeCompare(String(a?.id || ""));
    });

    const deduped = dedupeActivityRows(mergedSorted);
    totalDroppedDupes += deduped.dropped;

    if (deduped.rows.length) {
      await upsertUserTradesAndGames({
        user,
        tradeRows: deduped.rows,
        gameMetaById: undefined,
      });
      totalPersistedRows += deduped.rows.length;
    }

    if (trades.length > 0) skipTrades += trades.length;
    if (bets.length > 0) skipBets += bets.length;

    const doneTrades = trades.length < first;
    const doneBets = bets.length < first;

    if (doneTrades && doneBets) break;
    if (opts.sleepMs > 0) await sleep(opts.sleepMs);
  }

  // ---- 2) claims paged (separate loop)
  for (;;) {
    const data = await subgraphQuery<any>(Q_USER_CLAIMS_PAGED, {
      user,
      leagues: opts.leagues,
      start: String(opts.start),
      end: String(opts.end),
      first,
      skip: skipClaims,
    });

    const claims = Array.isArray(data?.claims) ? data.claims : [];

    const claimRows = claims.map((c: any) => {
      const g0 = c?.game ?? {};
      const g = attachMetaToGame(g0, opts.metaByAddr);

      const ts = toNum(c?.timestamp);
      const amt = c?.amountDec ?? "0";

      return {
        id: `claim-${c.id}`,
        type: "CLAIM",
        side: "C",
        timestamp: ts,
        txHash: c?.txHash ?? null,

        spotPriceBps: null,
        avgPriceBps: null,

        grossInDec: "0",
        grossOutDec: amt,
        feeDec: "0",
        netStakeDec: "0",
        netOutDec: amt,

        costBasisClosedDec: "0",
        realizedPnlDec: "0",

        game: g,
        __source: "claim",
      };
    });

    if (claimRows.length) {
      await upsertUserTradesAndGames({
        user,
        tradeRows: claimRows,
        gameMetaById: undefined,
      });
      totalPersistedRows += claimRows.length;
    }

    if (claims.length > 0) skipClaims += claims.length;

    const doneClaims = claims.length < first;
    if (doneClaims) break;

    if (opts.sleepMs > 0) await sleep(opts.sleepMs);
  }

  return { user, loops, totalPersistedRows, totalDroppedDupes };
}

async function run() {
  if (!ENV.SUBGRAPH_QUERY_URL) {
    console.log(`[backfill] ENV.SUBGRAPH_QUERY_URL missing — cannot query subgraph.`);
    process.exit(1);
  }

  const leagues = parseCsvEnv("BACKFILL_LEAGUES", DEFAULT_LEAGUES);
  const range = String(process.env.BACKFILL_RANGE || "ALL").toUpperCase();
  const { start, end } = tradesWindowFromRange(range);

  const concurrency = Math.max(1, Math.min(10, Number(process.env.BACKFILL_CONCURRENCY || 3)));
  const sleepMs = Math.max(0, Number(process.env.BACKFILL_SLEEP_MS || 150));
  const maxUsers = Math.max(0, Number(process.env.BACKFILL_MAX_USERS || 0));
  const startIndex = Math.max(0, Number(process.env.BACKFILL_START_INDEX || 0));

  const metaByAddr = buildGameMetaByAddr(gamesJson);
  console.log(`[backfill] loaded deploy metadata for ${Object.keys(metaByAddr).length} pools`);

  console.log(
    `[backfill] start: leagues=${leagues.join(",")} range=${range} concurrency=${concurrency} sleepMs=${sleepMs}`
  );

  const usersAll = await discoverUsers(leagues, start, end);
  const users = usersAll.slice(startIndex, maxUsers > 0 ? startIndex + maxUsers : undefined);

  console.log(`[backfill] discovered ${usersAll.length} users, processing ${users.length} users…`);

  let idx = 0;
  let ok = 0;
  let err = 0;

  const workers = Array.from({ length: concurrency }, () =>
    (async () => {
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
            metaByAddr,
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
    })()
  );

  await Promise.all(workers);
  console.log(`[backfill] done ok=${ok} err=${err} totalUsers=${users.length}`);
}

run().catch((e) => {
  console.log(`[backfill] fatal: ${String((e as any)?.message || e)}`);
  process.exit(1);
});
