import type { Request } from "express";

type League = "ALL"|"MLB"|"NFL"|"NBA"|"NHL"|"EPL"|"UCL";
type Range = "ALL"|"D90"|"D30";

type TradeType = "BUY"|"SELL"|"CLAIM";

export type TradeEvent = {
  id: string;
  timestamp: number;

  type: TradeType;
  gameId: string;
  side: "A"|"B";

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

type GameMeta = {
  gameId: string;
  league: string;
  teamACode?: string;
  teamBCode?: string;
  teamAName?: string;
  teamBName?: string;
  lockTime: number;
  isFinal: boolean;
  winnerSide?: "A"|"B"|null;
};

type PositionKey = string; // `${gameId}:${side}`

type PositionState = {
  gameId: string;
  side: "A"|"B";
  league: string;

  netPositionDec: number;
  costBasisOpenDec: number;     // remaining cost basis
  avgEntryBps: number | null;   // weighted avg for open shares
};

function clampLeague(v: any): League {
  const s = String(v ?? "ALL").toUpperCase();
  return (["ALL","MLB","NFL","NBA","NHL","EPL","UCL"] as const).includes(s as any) ? (s as League) : "ALL";
}
function clampRange(v: any): Range {
  const s = String(v ?? "ALL").toUpperCase();
  return (["ALL","D90","D30"] as const).includes(s as any) ? (s as Range) : "ALL";
}

/**
 * You will implement this to read all user events from your DB/subgraph.
 * It MUST return a complete ledger in a deterministic order.
 *
 * Requirements:
 * - includes BUY + SELL + CLAIM
 * - includes priceBps at time of trade
 * - includes txHash/logIndex when possible
 * - filters by league/range/anchorTs
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
}> {
  // TODO: IMPLEMENT USING YOUR DATA SOURCE.
  // Placeholder:
  return { trades: [], nextCursor: null, gameMetaById: {} };
}

/**
 * FIFO / average-cost accounting for positions.
 * This version uses a simple weighted-average cost basis per (game,side).
 * If you want strict FIFO lots, we can do that too — but avg-cost is simpler and stable.
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

  // IMPORTANT: process chronological ASC for correct accounting
  const asc = [...trades].sort((a,b)=> (a.timestamp - b.timestamp) || (String(a.id).localeCompare(String(b.id))));

  for (const t of asc) {
    const p = ensure(t);
    const key = `${t.gameId}:${t.side}`;
    const r = realizedByKey.get(key)!;

    if (t.type === "BUY") {
      const addQty = Number(t.sharesDec ?? 0) || 0;
      const addCost = Number(t.netAmountDec ?? 0) || 0;

      // If you don’t have sharesDec, you can treat "shares" as 1 unit and just do cash-basis,
      // but you asked for bps-at-buy/sell and shares, so ideally store shares.
      // With no shares, avgEntryBps will be less meaningful.
      if (addQty > 0) {
        const prevQty = p.netPositionDec;
        const prevCost = p.costBasisOpenDec;
        const nextQty = prevQty + addQty;
        const nextCost = prevCost + addCost;

        p.netPositionDec = nextQty;
        p.costBasisOpenDec = nextCost;

        // weighted avg entry bps for remaining open position
        if (t.priceBps != null) {
          const prevBps = p.avgEntryBps ?? t.priceBps;
          // weight by shares
          p.avgEntryBps = Math.round(((prevBps * prevQty) + (t.priceBps * addQty)) / Math.max(1e-9, nextQty));
        }
      } else {
        // fallback if shares not present: treat netAmount as “position units”
        p.costBasisOpenDec += addCost;
      }
    }

    if (t.type === "SELL") {
      const sellQty = Number(t.sharesDec ?? 0) || 0;
      const proceeds = Number(t.netAmountDec ?? 0) || 0;

      // determine cost basis closed (avg-cost)
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

        // attach analytics to the trade (optional; you can also compute server-side output)
        t.realizedPnlDec = pnl;
        t.costBasisClosedDec = costClosed;
      }
    }

    // CLAIM typically applies to finalized outcome, doesn’t reduce “shares” unless your system does.
    // Many systems treat claim as settlement of the game; openPositions should be 0 by then anyway.
  }

  return { pos, realizedByKey };
}

export async function buildProfilePortfolio(req: Request) {
  const userLower = String(req.params.address || "").toLowerCase().trim();
  const league = clampLeague(req.query.league);
  const range = clampRange(req.query.range);

  const limit = Math.min(200, Math.max(1, Number(req.query.limit ?? 50) || 50));
  const cursor = req.query.cursor ? String(req.query.cursor) : null;

  // Anchor makes D30/D90 stable (matches your leaderboard behavior)
  const anchorTs = range === "ALL" ? null : (req.query.anchorTs ? Number(req.query.anchorTs) : Math.floor(Date.now()/1000));

  const { trades, nextCursor, gameMetaById } = await loadUserLedger({
    userLower,
    league,
    range,
    anchorTs,
    cursor,
    limit,
  });

  // Compute positions + realized sell pnl
  const { pos, realizedByKey } = derivePositions(trades, gameMetaById);

  const openPositions = Array.from(pos.values())
    .filter(p => (p.netPositionDec ?? 0) > 1e-9)
    .map(p => {
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
        lastPriceBps: null, // optional
        costBasisOpenDec: p.costBasisOpenDec,
      };
    });

  // Stats (you can align this exactly with your leaderboard definitions)
  const tradesCount = trades.length;
  const tradedGross = trades.filter(t=>t.type==="BUY").reduce((s,t)=>s+(t.grossAmountDec||0),0);
  const soldGross = trades.filter(t=>t.type==="SELL").reduce((s,t)=>s+(t.grossAmountDec||0),0);
  const wonFinal = trades.filter(t=>t.type==="CLAIM").reduce((s,t)=>s+(t.netAmountDec||0),0);

  const realizedSellPnl = trades
    .filter(t=>t.type==="SELL")
    .reduce((s,t)=> s + (t.realizedPnlDec || 0), 0);

  const pnlNet = realizedSellPnl + wonFinal; // if you want subtract cost, do it in ROI definition

  // naive ROI (replace with your "final-only denom" logic if desired)
  const denom = tradedGross > 1e-9 ? tradedGross : 0;
  const roiNet = denom > 0 ? (pnlNet / denom) - 1 : null;

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
      mostBetLeague: null,
    },

    openPositions,

    trades, // already contains SELL analytics + priceBps

    page: { limit, nextCursor },
  };
}
