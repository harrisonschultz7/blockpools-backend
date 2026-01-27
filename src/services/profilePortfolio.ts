// src/services/profilePortfolio.ts
import type { Request } from "express";
import { pool } from "../db";

/**
 * This service is intentionally defensive because your DB schema has been in flux.
 *
 * It will:
 * 1) Load a user's trade ledger from the first matching table it can find (see CANDIDATE tables below).
 * 2) Build game metadata (league/team names/lockTime/winner) from the first matching games table it can find.
 * 3) Compute open positions + basic stats.
 *
 * If none of the candidate tables exist, it returns ok:true with empty arrays
 * AND includes a "debug" field so you can see what it attempted (and the first error).
 *
 * Once you confirm the real table names/columns, we can tighten this up to a single query.
 */

/* ===================== Types ===================== */

export type League = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";
export type Range = "ALL" | "D90" | "D30";
export type TradeType = "BUY" | "SELL" | "CLAIM";

export type TradeEvent = {
  id: string;
  timestamp: number; // seconds

  type: TradeType;
  gameId: string;
  side: "A" | "B";

  grossAmountDec: number;
  netAmountDec: number;
  feeDec?: number;

  priceBps: number | null;
  sharesDec?: number | null;

  realizedPnlDec?: number;
  costBasisClosedDec?: number;

  txHash?: string;
  logIndex?: number;
};

export type GameMeta = {
  gameId: string;
  league: string;
  teamACode?: string;
  teamBCode?: string;
  teamAName?: string;
  teamBName?: string;
  lockTime: number;
  isFinal: boolean;
  winnerSide?: "A" | "B" | null;
};

type PositionKey = string; // `${gameId}:${side}`

type PositionState = {
  gameId: string;
  side: "A" | "B";
  league: string;

  netPositionDec: number;
  costBasisOpenDec: number; // remaining cost basis
  avgEntryBps: number | null;
};

/* ===================== Helpers ===================== */

function clampLeague(v: any): League {
  const s = String(v ?? "ALL").toUpperCase();
  return (["ALL", "MLB", "NFL", "NBA", "NHL", "EPL", "UCL"] as const).includes(s as any)
    ? (s as League)
    : "ALL";
}

function clampRange(v: any): Range {
  const s = String(v ?? "ALL").toUpperCase();
  return (["ALL", "D90", "D30"] as const).includes(s as any) ? (s as Range) : "ALL";
}

function rangeWindowSeconds(range: Range): number | null {
  if (range === "D30") return 30 * 24 * 60 * 60;
  if (range === "D90") return 90 * 24 * 60 * 60;
  return null;
}

// Cursor is base64 of: `${timestamp}:${id}`
function decodeCursor(cursor: string | null): { ts: number; id: string } | null {
  if (!cursor) return null;
  try {
    const raw = Buffer.from(cursor, "base64").toString("utf8");
    const idx = raw.indexOf(":");
    if (idx === -1) return null;
    const ts = Number(raw.slice(0, idx));
    const id = raw.slice(idx + 1);
    if (!Number.isFinite(ts) || !id) return null;
    return { ts, id };
  } catch {
    return null;
  }
}

function encodeCursor(ts: number, id: string): string {
  return Buffer.from(`${ts}:${id}`, "utf8").toString("base64");
}

function asNum(v: any, fallback = 0): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : fallback;
}

function asStr(v: any, fallback = ""): string {
  const s = String(v ?? "").trim();
  return s || fallback;
}

function asSide(v: any): "A" | "B" {
  const s = String(v ?? "").toUpperCase();
  return s === "B" ? "B" : "A";
}

function asTradeType(v: any): TradeType {
  const s = String(v ?? "").toUpperCase();
  if (s === "SELL") return "SELL";
  if (s === "CLAIM") return "CLAIM";
  return "BUY";
}

function looksLikeMissingTable(err: any): boolean {
  // Postgres missing relation: 42P01
  return String(err?.code || "") === "42P01" || /relation .* does not exist/i.test(String(err?.message || ""));
}

/**
 * Try a list of SQL candidates until one works.
 * Returns { ok:true, rows, used } on success; else ok:false with debug.
 */
async function queryFirstWorking<T = any>(candidates: Array<{ name: string; sql: string; params: any[] }>): Promise<{
  ok: true;
  rows: T[];
  used: string;
} | {
  ok: false;
  attempted: string[];
  firstError: string;
}> {
  const attempted: string[] = [];
  let firstError = "";

  for (const c of candidates) {
    attempted.push(c.name);
    try {
      const res = await pool.query(c.sql, c.params);
      return { ok: true, rows: (res.rows || []) as T[], used: c.name };
    } catch (err: any) {
      if (!firstError) firstError = String(err?.message || err);
      // if it's missing table, keep trying; otherwise still keep trying but note it
      continue;
    }
  }

  return { ok: false, attempted, firstError: firstError || "no_candidates_worked" };
}

/* ===================== Core accounting ===================== */

/**
 * Weighted-average cost basis per (game,side).
 * (If you prefer strict FIFO lots later, we can swap this.)
 */
function derivePositions(trades: TradeEvent[], gameMetaById: Record<string, GameMeta>) {
  const pos = new Map<PositionKey, PositionState>();
  const realizedByKey = new Map<PositionKey, { realizedPnlDec: number; costBasisClosedDec: number }>();

  const ensure = (t: TradeEvent): PositionState => {
    const g = gameMetaById[t.gameId];
    const league = (g?.league || "—").toUpperCase();
    const key = `${t.gameId}:${t.side}`;
    const cur = pos.get(key);
    if (cur) return cur;

    const fresh: PositionState = {
      gameId: t.gameId,
      side: t.side,
      league,
      netPositionDec: 0,
      costBasisOpenDec: 0,
      avgEntryBps: null,
    };
    pos.set(key, fresh);
    realizedByKey.set(key, { realizedPnlDec: 0, costBasisClosedDec: 0 });
    return fresh;
  };

  const asc = [...trades].sort(
    (a, b) => a.timestamp - b.timestamp || String(a.id).localeCompare(String(b.id))
  );

  for (const t of asc) {
    const p = ensure(t);
    const key = `${t.gameId}:${t.side}`;
    const r = realizedByKey.get(key)!;

    if (t.type === "BUY") {
      const addQty = asNum(t.sharesDec, 0);
      const addCost = asNum(t.netAmountDec, 0);

      if (addQty > 0) {
        const prevQty = p.netPositionDec;
        const prevCost = p.costBasisOpenDec;
        const nextQty = prevQty + addQty;
        const nextCost = prevCost + addCost;

        p.netPositionDec = nextQty;
        p.costBasisOpenDec = nextCost;

        if (t.priceBps != null) {
          const prevBps = p.avgEntryBps ?? t.priceBps;
          p.avgEntryBps = Math.round(((prevBps * prevQty) + (t.priceBps * addQty)) / Math.max(1e-9, nextQty));
        }
      } else {
        // fallback if shares not present
        p.costBasisOpenDec += addCost;
      }
    }

    if (t.type === "SELL") {
      const sellQty = asNum(t.sharesDec, 0);
      const proceeds = asNum(t.netAmountDec, 0);

      const openQty = p.netPositionDec;
      const openCost = p.costBasisOpenDec;

      if (sellQty > 0 && openQty > 0) {
        const closeQty = Math.min(sellQty, openQty);
        const costClosed = (openCost * closeQty) / openQty;
        const pnl = proceeds - costClosed;

        r.realizedPnlDec += pnl;
        r.costBasisClosedDec += costClosed;

        p.netPositionDec = openQty - closeQty;
        p.costBasisOpenDec = openCost - costClosed;

        t.realizedPnlDec = pnl;
        t.costBasisClosedDec = costClosed;
      }
    }
  }

  return { pos, realizedByKey };
}

/* ===================== Data loading ===================== */

/**
 * CANDIDATE tables for user trade ledger.
 *
 * You MUST update these once you confirm the actual table you created in your backend caching work.
 * This is the “best-effort” set based on typical naming from your recent commits.
 *
 * Expected columns (any subset ok, we coalesce):
 * - id (text)
 * - timestamp / ts / block_time (seconds)
 * - type (BUY/SELL/CLAIM)
 * - game_id
 * - side (A/B)
 * - gross_amount_dec / gross_amount / gross
 * - net_amount_dec / net_amount / net
 * - fee_dec / fee
 * - price_bps
 * - shares_dec / shares
 * - tx_hash
 * - log_index
 * - league (optional)
 */
async function loadUserLedger(args: {
  userLower: string;
  league: League;
  range: Range;
  anchorTs?: number | null;
  cursor?: string | null;
  limit: number;
}): Promise<{
  trades: TradeEvent[];
  nextCursor: string | null;
  gameMetaById: Record<string, GameMeta>;
  debug?: any;
}> {
  const decoded = decodeCursor(args.cursor ?? null);
  const window = rangeWindowSeconds(args.range);
  const anchorTs = args.anchorTs ?? null;

  // Time bounds:
  // - If D30/D90: include [anchor-window, anchor]
  // - If ALL: no bounds
  const timeMin = window && anchorTs ? anchorTs - window : null;
  const timeMax = window && anchorTs ? anchorTs : null;

  // For pagination, we use descending (newest first).
  // If cursor is present, we fetch rows strictly older than cursor (ts,id).
  const cursorTs = decoded?.ts ?? null;
  const cursorId = decoded?.id ?? null;

  // We try multiple candidate SQLs with different table/column assumptions.
  // IMPORTANT: these queries assume there is a "user" column holding the user's address lowercased.
  // If your column differs (e.g., user_address), update below.
  const paramsBase: any[] = [];
  let p = 1;

  const userParam = `$${p++}`;
  paramsBase.push(args.userLower);

  const leagueParam = `$${p++}`;
  paramsBase.push(args.league);

  const limitParam = `$${p++}`;
  paramsBase.push(args.limit);

  // optional time bounds
  const timeMinParam = timeMin != null ? `$${p++}` : null;
  if (timeMin != null) paramsBase.push(timeMin);

  const timeMaxParam = timeMax != null ? `$${p++}` : null;
  if (timeMax != null) paramsBase.push(timeMax);

  const cursorTsParam = cursorTs != null ? `$${p++}` : null;
  if (cursorTs != null) paramsBase.push(cursorTs);

  const cursorIdParam = cursorId != null ? `$${p++}` : null;
  if (cursorId != null) paramsBase.push(cursorId);

  // shared WHERE snippets
  const whereUser = `LOWER(t.user) = ${userParam}`;
  const whereLeague = args.league === "ALL" ? `TRUE` : `UPPER(COALESCE(t.league, '')) = ${leagueParam}`;
  const whereTime =
    timeMinParam && timeMaxParam
      ? `(t.timestamp >= ${timeMinParam} AND t.timestamp <= ${timeMaxParam})`
      : timeMinParam
        ? `(t.timestamp >= ${timeMinParam})`
        : timeMaxParam
          ? `(t.timestamp <= ${timeMaxParam})`
          : `TRUE`;

  const whereCursor =
    cursorTsParam && cursorIdParam
      ? `((t.timestamp < ${cursorTsParam}) OR (t.timestamp = ${cursorTsParam} AND t.id < ${cursorIdParam}))`
      : `TRUE`;

  // Candidate 1: table already has "timestamp" seconds
  const cand1 = {
    name: "user_trade_events.timestamp",
    sql: `
      SELECT
        t.id,
        t.timestamp,
        t.type,
        t.game_id,
        t.side,
        t.gross_amount_dec,
        t.net_amount_dec,
        t.fee_dec,
        t.price_bps,
        t.shares_dec,
        t.tx_hash,
        t.log_index,
        t.league
      FROM user_trade_events t
      WHERE ${whereUser}
        AND ${whereLeague}
        AND ${whereTime}
        AND ${whereCursor}
      ORDER BY t.timestamp DESC, t.id DESC
      LIMIT ${limitParam}
    `,
    params: paramsBase,
  };

  // Candidate 2: same table but "ts" column
  const cand2 = {
    name: "user_trade_events.ts",
    sql: `
      SELECT
        t.id,
        t.ts AS timestamp,
        t.type,
        t.game_id,
        t.side,
        t.gross_amount_dec,
        t.net_amount_dec,
        t.fee_dec,
        t.price_bps,
        t.shares_dec,
        t.tx_hash,
        t.log_index,
        t.league
      FROM user_trade_events t
      WHERE LOWER(t.user) = ${userParam}
        AND ${whereLeague.replaceAll("t.", "t.")}
        AND ${(timeMinParam || timeMaxParam) ? whereTime.replaceAll("t.timestamp", "t.ts") : "TRUE"}
        AND ${(cursorTsParam && cursorIdParam) ? whereCursor.replaceAll("t.timestamp", "t.ts") : "TRUE"}
      ORDER BY t.ts DESC, t.id DESC
      LIMIT ${limitParam}
    `,
    params: paramsBase,
  };

  // Candidate 3: different table name
  const cand3 = {
    name: "cached_trade_events",
    sql: `
      SELECT
        t.id,
        COALESCE(t.timestamp, t.ts, t.block_time) AS timestamp,
        t.type,
        COALESCE(t.game_id, t.gameid) AS game_id,
        t.side,
        COALESCE(t.gross_amount_dec, t.gross_amount, t.gross, 0) AS gross_amount_dec,
        COALESCE(t.net_amount_dec, t.net_amount, t.net, 0) AS net_amount_dec,
        COALESCE(t.fee_dec, t.fee, 0) AS fee_dec,
        t.price_bps,
        COALESCE(t.shares_dec, t.shares) AS shares_dec,
        t.tx_hash,
        t.log_index,
        t.league
      FROM cached_trade_events t
      WHERE LOWER(t.user) = ${userParam}
        AND ${whereLeague.replaceAll("t.", "t.")}
        AND ${
          (timeMinParam || timeMaxParam)
            ? `(${timeMinParam ? `COALESCE(t.timestamp,t.ts,t.block_time) >= ${timeMinParam}` : "TRUE"} AND ${
                timeMaxParam ? `COALESCE(t.timestamp,t.ts,t.block_time) <= ${timeMaxParam}` : "TRUE"
              })`
            : "TRUE"
        }
        AND ${
          cursorTsParam && cursorIdParam
            ? `(
                (COALESCE(t.timestamp,t.ts,t.block_time) < ${cursorTsParam})
                OR (COALESCE(t.timestamp,t.ts,t.block_time) = ${cursorTsParam} AND t.id < ${cursorIdParam})
              )`
            : "TRUE"
        }
      ORDER BY COALESCE(t.timestamp,t.ts,t.block_time) DESC, t.id DESC
      LIMIT ${limitParam}
    `,
    params: paramsBase,
  };

  const ledgerRes = await queryFirstWorking<any>([cand1, cand2, cand3]);

  let rows: any[] = [];
  let ledgerDebug: any = {};

  if (ledgerRes.ok) {
    rows = ledgerRes.rows;
    ledgerDebug.ledgerSource = ledgerRes.used;
  } else {
    ledgerDebug.ledgerSource = null;
    ledgerDebug.ledgerAttempted = ledgerRes.attempted;
    ledgerDebug.ledgerFirstError = ledgerRes.firstError;
    // return empty (don't hard crash your UI)
    return {
      trades: [],
      nextCursor: null,
      gameMetaById: {},
      debug: ledgerDebug,
    };
  }

  // Normalize to TradeEvent shape
  const trades: TradeEvent[] = rows.map((r) => {
    const timestamp = asNum(r.timestamp ?? r.ts ?? r.block_time, 0);
    return {
      id: asStr(r.id, `${timestamp}:${asStr(r.tx_hash, "noid")}:${asNum(r.log_index, 0)}`),
      timestamp,
      type: asTradeType(r.type),
      gameId: asStr(r.game_id ?? r.gameId ?? r.gameid, ""),
      side: asSide(r.side),
      grossAmountDec: asNum(r.gross_amount_dec ?? r.gross_amount ?? r.gross, 0),
      netAmountDec: asNum(r.net_amount_dec ?? r.net_amount ?? r.net, 0),
      feeDec: r.fee_dec != null || r.fee != null ? asNum(r.fee_dec ?? r.fee, 0) : undefined,
      priceBps: r.price_bps == null ? null : asNum(r.price_bps, 0),
      sharesDec: r.shares_dec != null || r.shares != null ? asNum(r.shares_dec ?? r.shares, 0) : null,
      txHash: r.tx_hash ? String(r.tx_hash) : undefined,
      logIndex: r.log_index != null ? asNum(r.log_index, 0) : undefined,
    };
  }).filter((t) => !!t.gameId);

  // next cursor: if we returned exactly limit rows, set cursor from last row
  let nextCursor: string | null = null;
  if (trades.length === args.limit && trades.length > 0) {
    const last = trades[trades.length - 1];
    nextCursor = encodeCursor(last.timestamp, last.id);
  }

  // Build gameMeta map
  const gameIds = Array.from(new Set(trades.map((t) => t.gameId))).filter(Boolean);
  const gameMetaById: Record<string, GameMeta> = {};

  if (gameIds.length === 0) {
    return { trades, nextCursor, gameMetaById, debug: ledgerDebug };
  }

  // Candidate games tables (update once you confirm real name)
  const gamesCand1 = {
    name: "games",
    sql: `
      SELECT
        g.game_id,
        g.league,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name,
        g.lock_time,
        g.is_final,
        g.winner_side
      FROM games g
      WHERE g.game_id = ANY($1::text[])
    `,
    params: [gameIds],
  };

  const gamesCand2 = {
    name: "games_cache",
    sql: `
      SELECT
        g.game_id,
        g.league,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name,
        g.lock_time,
        g.is_final,
        g.winner_side
      FROM games_cache g
      WHERE g.game_id = ANY($1::text[])
    `,
    params: [gameIds],
  };

  const gamesCand3 = {
    name: "cached_games",
    sql: `
      SELECT
        g.game_id,
        g.league,
        g.team_a_code,
        g.team_b_code,
        g.team_a_name,
        g.team_b_name,
        g.lock_time,
        g.is_final,
        g.winner_side
      FROM cached_games g
      WHERE g.game_id = ANY($1::text[])
    `,
    params: [gameIds],
  };

  const gamesRes = await queryFirstWorking<any>([gamesCand1, gamesCand2, gamesCand3]);
  if (gamesRes.ok) {
    ledgerDebug.gamesSource = gamesRes.used;
    for (const r of gamesRes.rows) {
      const gameId = asStr(r.game_id, "");
      if (!gameId) continue;
      gameMetaById[gameId] = {
        gameId,
        league: asStr(r.league, "—"),
        teamACode: r.team_a_code ?? undefined,
        teamBCode: r.team_b_code ?? undefined,
        teamAName: r.team_a_name ?? undefined,
        teamBName: r.team_b_name ?? undefined,
        lockTime: asNum(r.lock_time, 0),
        isFinal: !!r.is_final,
        winnerSide: r.winner_side ? asSide(r.winner_side) : null,
      };
    }
  } else {
    ledgerDebug.gamesSource = null;
    ledgerDebug.gamesAttempted = gamesRes.attempted;
    ledgerDebug.gamesFirstError = gamesRes.firstError;
  }

  return { trades, nextCursor, gameMetaById, debug: ledgerDebug };
}

/* ===================== Public API ===================== */

export async function buildProfilePortfolio(req: Request) {
  const userLower = String((req as any).params?.address || "")
    .toLowerCase()
    .trim();

  const league = clampLeague((req as any).query?.league);
  const range = clampRange((req as any).query?.range);

  const limit = Math.min(200, Math.max(1, Number((req as any).query?.limit ?? 50) || 50));
  const cursor = (req as any).query?.cursor ? String((req as any).query?.cursor) : null;

  // Anchor makes D30/D90 stable
  const anchorTs =
    range === "ALL"
      ? null
      : (req as any).query?.anchorTs
        ? Number((req as any).query?.anchorTs)
        : Math.floor(Date.now() / 1000);

  const { trades, nextCursor, gameMetaById, debug } = await loadUserLedger({
    userLower,
    league,
    range,
    anchorTs,
    cursor,
    limit,
  });

  const { pos } = derivePositions(trades, gameMetaById);

  const openPositions = Array.from(pos.values())
    .filter((p) => (p.netPositionDec ?? 0) > 1e-9)
    .map((p) => {
      const g = gameMetaById[p.gameId];
      return {
        gameId: p.gameId,
        league: g?.league ?? p.league ?? "—",
        teamACode: g?.teamACode,
        teamBCode: g?.teamBCode,
        teamAName: g?.teamAName,
        teamBName: g?.teamBName,
        lockTime: g?.lockTime ?? 0,
        isFinal: !!g?.isFinal,
        winnerSide: g?.winnerSide ?? null,

        side: p.side,
        netPositionDec: p.netPositionDec,
        avgEntryBps: p.avgEntryBps,
        lastPriceBps: null, // optional: you can fill from a prices table later
        costBasisOpenDec: p.costBasisOpenDec,
      };
    });

  const tradesCount = trades.length;
  const tradedGross = trades
    .filter((t) => t.type === "BUY")
    .reduce((s, t) => s + (t.grossAmountDec || 0), 0);

  const soldGross = trades
    .filter((t) => t.type === "SELL")
    .reduce((s, t) => s + (t.grossAmountDec || 0), 0);

  const wonFinal = trades
    .filter((t) => t.type === "CLAIM")
    .reduce((s, t) => s + (t.netAmountDec || 0), 0);

  const realizedSellPnl = trades
    .filter((t) => t.type === "SELL")
    .reduce((s, t) => s + (t.realizedPnlDec || 0), 0);

  const pnlNet = realizedSellPnl + wonFinal;

  const denom = tradedGross > 1e-9 ? tradedGross : 0;
  const roiNet = denom > 0 ? pnlNet / denom - 1 : null;

  // simple most-bet league heuristic (by BUY gross)
  const buyByLeague: Record<string, number> = {};
  for (const t of trades) {
    if (t.type !== "BUY") continue;
    const lg = (gameMetaById[t.gameId]?.league || "—").toUpperCase();
    buyByLeague[lg] = (buyByLeague[lg] || 0) + (t.grossAmountDec || 0);
  }
  const mostBetLeague =
    Object.entries(buyByLeague).sort((a, b) => b[1] - a[1])[0]?.[0] ?? null;

  return {
    ok: true,
    user: userLower,
    league,
    range,
    anchorTs,

    stats: {
      tradesCount,
      tradedGross,
      soldGross,
      wonFinal,
      pnlNet,
      roiNet,
      mostBetLeague,
    },

    openPositions,
    trades,

    page: { limit, nextCursor },

    // ✅ keep debug for now; you can remove once stable
    debug,
  };
}
