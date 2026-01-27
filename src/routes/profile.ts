// src/services/profile.ts
import type { Request } from "express";

/**
 * Profile portfolio builder (single source of truth)
 * - returns full ledger page (BUY/SELL/CLAIM)
 * - derives open positions (avg-cost)
 * - computes headline stats
 *
 * NOTE: loadUserLedger() is still a stub until you wire it to your DB/subgraph.
 */

type League = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";
type Range = "ALL" | "D90" | "D30";
type TradeType = "BUY" | "SELL" | "CLAIM";

export type TradeEvent = {
  id: string;
  timestamp: number; // seconds

  type: TradeType;
  gameId: string;
  side: "A" | "B";

  grossAmountDec: number; // BUY: grossIn, SELL: grossOut, CLAIM: claimGross (optional)
  netAmountDec: number; // BUY: netStake, SELL: netOut, CLAIM: netClaim
  feeDec?: number;

  priceBps: number | null; // bps at execution time
  sharesDec?: number | null; // shares delta for BUY/SELL if available

  // SELL analytics (filled by derivePositions)
  realizedPnlDec?: number;
  costBasisClosedDec?: number;

  txHash?: string;
  logIndex?: number;
};

type GameMeta = {
  gameId: string;
  league: string;
  teamACode?: string;
  teamBCode?: string;
  teamAName?: string;
  teamBName?: string;
  lockTime: number; // seconds
  isFinal: boolean;
  winnerSide?: "A" | "B" | null;
};

type PositionKey = string; // `${gameId}:${side}`

type PositionState = {
  gameId: string;
  side: "A" | "B";
  league: string;

  // shares-like qty remaining (if you provide sharesDec)
  netPositionDec: number;
  costBasisOpenDec: number; // remaining open cost basis (in USDC)
  avgEntryBps: number | null; // weighted avg entry for remaining open shares
};

function clampLeague(v: any): League {
  const s = String(v ?? "ALL").toUpperCase().trim();
  return (["ALL", "MLB", "NFL", "NBA", "NHL", "EPL", "UCL"] as const).includes(
    s as any
  )
    ? (s as League)
    : "ALL";
}

function clampRange(v: any): Range {
  const s = String(v ?? "ALL").toUpperCase().trim();
  return (["ALL", "D90", "D30"] as const).includes(s as any)
    ? (s as Range)
    : "ALL";
}

function safeNum(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

function safeSide(v: any): "A" | "B" {
  const s = String(v ?? "").toUpperCase();
  return s === "A" ? "A" : "B";
}

function stableCursorFromTrade(t: TradeEvent): string {
  // Cursor is "timestamp:id" so pagination is deterministic even with same timestamps.
  const ts = Math.floor(safeNum(t.timestamp));
  return `${ts}:${String(t.id || "")}`;
}

function parseCursor(cursor: string | null | undefined): { ts: number; id: string } | null {
  if (!cursor) return null;
  const s = String(cursor);
  const i = s.indexOf(":");
  if (i <= 0) return null;
  const ts = Number(s.slice(0, i));
  const id = s.slice(i + 1);
  if (!Number.isFinite(ts)) return null;
  return { ts, id };
}

/**
 * Load a deterministic page of the user's ledger.
 *
 * IMPORTANT REQUIREMENTS:
 * - MUST include BUY + SELL + CLAIM
 * - MUST NOT default-filter to "today"
 * - MUST return a deterministic ordering:
 *     newest-first on (timestamp DESC, id DESC) for paging,
 *     while derivePositions() internally re-sorts ASC for accounting.
 * - MUST filter by league and by time range (using anchorTs for D30/D90).
 *
 * Cursor paging contract:
 * - cursor is "timestamp:id" (both from the *last* item of previous page)
 * - next page returns items strictly older than cursor (ts,id)
 */
async function loadUserLedger(args: {
  userLower: string;
  league: League;
  range: Range;
  anchorTs?: number | null; // seconds
  cursor?: string | null; // "timestamp:id"
  limit: number;
}): Promise<{
  trades: TradeEvent[];
  nextCursor: string | null;
  gameMetaById: Record<string, GameMeta>;
}> {
  // TODO: IMPLEMENT USING YOUR DATA SOURCE.
  // Placeholder:
  return { trades: [], nextCursor: null, gameMetaById: {} };
}

/**
 * Accounting:
 * - avg-cost per (game,side) using sharesDec as quantity.
 * - if sharesDec is missing, we fall back to cash-basis:
 *     - BUY increases costBasisOpenDec
 *     - SELL does NOT reliably reduce position qty (so openPositions will be less meaningful)
 *
 * NOTE: We mutate SELL trades to attach realizedPnlDec / costBasisClosedDec.
 */
function derivePositions(trades: TradeEvent[], gameMetaById: Record<string, GameMeta>) {
  const pos = new Map<PositionKey, PositionState>();
  const realizedByKey = new Map<
    PositionKey,
    { realizedPnlDec: number; costBasisClosedDec: number }
  >();

  const ensure = (t: TradeEvent): PositionState => {
    const g = gameMetaById[t.gameId];
    const league = String(g?.league || "—").toUpperCase();
    const key = `${t.gameId}:${t.side}`;
    const cur = pos.get(key);
    if (cur) return cur;

    const fresh: PositionState = {
      gameId: t.gameId,
      side: safeSide(t.side),
      league,
      netPositionDec: 0,
      costBasisOpenDec: 0,
      avgEntryBps: null,
    };
    pos.set(key, fresh);
    realizedByKey.set(key, { realizedPnlDec: 0, costBasisClosedDec: 0 });
    return fresh;
  };

  // For correct accounting: chronological ASC
  const asc = [...trades].sort(
    (a, b) =>
      safeNum(a.timestamp) - safeNum(b.timestamp) ||
      String(a.id || "").localeCompare(String(b.id || ""))
  );

  for (const t of asc) {
    const p = ensure(t);
    const key = `${t.gameId}:${t.side}`;
    const r = realizedByKey.get(key)!;

    const type = String(t.type || "BUY").toUpperCase() as TradeType;

    if (type === "BUY") {
      const addQty = safeNum(t.sharesDec ?? 0);
      const addCost = safeNum(t.netAmountDec ?? 0);

      if (addQty > 0) {
        const prevQty = p.netPositionDec;
        const prevCost = p.costBasisOpenDec;
        const nextQty = prevQty + addQty;
        const nextCost = prevCost + addCost;

        p.netPositionDec = nextQty;
        p.costBasisOpenDec = nextCost;

        if (t.priceBps != null) {
          const prevBps = p.avgEntryBps ?? t.priceBps;
          p.avgEntryBps = Math.round(
            ((prevBps * prevQty) + (t.priceBps * addQty)) / Math.max(1e-9, nextQty)
          );
        }
      } else {
        // cash-basis fallback if shares missing
        p.costBasisOpenDec += addCost;
      }
    }

    if (type === "SELL") {
      const sellQty = safeNum(t.sharesDec ?? 0);
      const proceeds = safeNum(t.netAmountDec ?? 0);

      const openQty = p.netPositionDec;
      const openCost = p.costBasisOpenDec;

      if (sellQty > 0 && openQty > 0) {
        const closeQty = Math.min(sellQty, openQty);
        const costClosed = (openCost * closeQty) / Math.max(1e-9, openQty);
        const pnl = proceeds - costClosed;

        r.realizedPnlDec += pnl;
        r.costBasisClosedDec += costClosed;

        p.netPositionDec = openQty - closeQty;
        p.costBasisOpenDec = openCost - costClosed;

        // annotate the SELL trade row (useful for UI)
        t.realizedPnlDec = pnl;
        t.costBasisClosedDec = costClosed;
      } else {
        // Ensure SELL rows still have stable fields, even if shares missing
        t.realizedPnlDec = t.realizedPnlDec ?? 0;
        t.costBasisClosedDec = t.costBasisClosedDec ?? 0;
      }
    }

    // CLAIM: no position qty change here by default
  }

  return { pos, realizedByKey };
}

export async function buildProfilePortfolio(req: Request) {
  const userLower = String(req.params.address || "").toLowerCase().trim();
  if (!userLower) {
    return {
      ok: false,
      error: "missing_address",
    };
  }

  const league = clampLeague((req.query as any).league);
  const range = clampRange((req.query as any).range);

  const limit = Math.min(200, Math.max(1, Number((req.query as any).limit ?? 50) || 50));
  const cursor = (req.query as any).cursor ? String((req.query as any).cursor) : null;

  // Anchor makes D30/D90 stable across paging/refresh (matches your leaderboard behavior)
  const anchorTs =
    range === "ALL"
      ? null
      : (req.query as any).anchorTs
      ? Number((req.query as any).anchorTs)
      : Math.floor(Date.now() / 1000);

  const { trades, nextCursor, gameMetaById } = await loadUserLedger({
    userLower,
    league,
    range,
    anchorTs,
    cursor,
    limit,
  });

  // Ensure the returned trades are sane and stable
  const cleaned: TradeEvent[] = (trades ?? [])
    .filter(Boolean)
    .map((t) => ({
      ...t,
      id: String(t.id),
      timestamp: Math.floor(safeNum(t.timestamp)),
      type: (String(t.type || "BUY").toUpperCase() as TradeType) || "BUY",
      gameId: String(t.gameId),
      side: safeSide(t.side),

      grossAmountDec: safeNum(t.grossAmountDec),
      netAmountDec: safeNum(t.netAmountDec),
      feeDec: t.feeDec === undefined ? undefined : safeNum(t.feeDec),

      priceBps: t.priceBps === null || t.priceBps === undefined ? null : Math.round(safeNum(t.priceBps)),
      sharesDec: t.sharesDec === null || t.sharesDec === undefined ? null : safeNum(t.sharesDec),

      realizedPnlDec: t.realizedPnlDec === undefined ? undefined : safeNum(t.realizedPnlDec),
      costBasisClosedDec: t.costBasisClosedDec === undefined ? undefined : safeNum(t.costBasisClosedDec),

      txHash: t.txHash ? String(t.txHash) : undefined,
      logIndex: t.logIndex === undefined ? undefined : Math.floor(safeNum(t.logIndex)),
    }));

  // Positions + sell PnL attach
  const { pos } = derivePositions(cleaned, gameMetaById || {});

  const openPositions = Array.from(pos.values())
    .filter((p) => safeNum(p.netPositionDec) > 1e-9)
    .map((p) => {
      const g = (gameMetaById || {})[p.gameId];
      return {
        gameId: p.gameId,
        league: (g?.league ?? p.league ?? "—").toUpperCase(),
        teamACode: g?.teamACode,
        teamBCode: g?.teamBCode,
        teamAName: g?.teamAName,
        teamBName: g?.teamBName,
        lockTime: safeNum(g?.lockTime ?? 0),
        isFinal: !!g?.isFinal,
        winnerSide: g?.winnerSide ?? null,

        side: p.side,
        netPositionDec: safeNum(p.netPositionDec),
        avgEntryBps: p.avgEntryBps ?? null,
        lastPriceBps: null as number | null, // optional (wire later)
        costBasisOpenDec: safeNum(p.costBasisOpenDec),
      };
    })
    // newest games first (by lockTime if present)
    .sort((a, b) => safeNum(b.lockTime) - safeNum(a.lockTime));

  // Stats (align later to match your exact product definitions if needed)
  const tradesCount = cleaned.length;
  const tradedGross = cleaned
    .filter((t) => t.type === "BUY")
    .reduce((s, t) => s + safeNum(t.grossAmountDec), 0);

  const soldGross = cleaned
    .filter((t) => t.type === "SELL")
    .reduce((s, t) => s + safeNum(t.grossAmountDec), 0);

  const wonFinal = cleaned
    .filter((t) => t.type === "CLAIM")
    .reduce((s, t) => s + safeNum(t.netAmountDec), 0);

  const realizedSellPnl = cleaned
    .filter((t) => t.type === "SELL")
    .reduce((s, t) => s + safeNum(t.realizedPnlDec ?? 0), 0);

  const pnlNet = realizedSellPnl + wonFinal;

  const denom = tradedGross > 1e-9 ? tradedGross : 0;
  const roiNet = denom > 0 ? pnlNet / denom - 1 : null;

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
      mostBetLeague: null as string | null,
    },

    openPositions,

    trades: cleaned,

    page: {
      limit,
      nextCursor: nextCursor ?? (cleaned.length ? stableCursorFromTrade(cleaned[cleaned.length - 1]) : null),
    },
  };
}
