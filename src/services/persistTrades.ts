// src/services/persistTrades.ts
import { pool } from "../db";

/**
 * We normalize TIEs here so your DB never stores contradictory outcomes like:
 *   winner_team_code = 'TIE' but winner_side = 'A'
 *
 * Your desired encoding:
 *   winner_side = 'C'  // means tie / neither A nor B
 *
 * We enforce:
 * - If winnerTeamCode is "TIE" (or "DRAW"), winnerSide => "C"
 * - Else winnerSide must be "A" | "B" (or null)
 */

type TradeType = "BUY" | "SELL" | "CLAIM";
type Side = "A" | "B" | "C";

type PersistTradeRow = {
  id: string;
  user: string;
  type: TradeType;
  side: Side;
  timestamp: number;
  txHash: string | null;

  spotPriceBps: number | null;
  avgPriceBps: number | null;

  grossInDec: string;
  grossOutDec: string;
  feeDec: string;
  netStakeDec: string;
  netOutDec: string;

  costBasisClosedDec: string;
  realizedPnlDec: string;

  gameId: string;
  league: string | null;
};

type PersistGameRow = {
  gameId: string;
  league: string | null;
  lockTime: number | null;
  isFinal: boolean | null;

  // IMPORTANT: winnerSide can now be 'A' | 'B' | 'C' | null
  winnerSide: string | null;
  winnerTeamCode: string | null;

  teamACode: string | null;
  teamBCode: string | null;
  teamAName: string | null;
  teamBName: string | null;
};

function toStr(v: any, fallback = "0"): string {
  if (v == null) return fallback;
  const s = String(v);
  return s === "" ? fallback : s;
}

function toNumOrNull(v: any): number | null {
  if (v == null || v === "") return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function toInt(v: any): number {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : 0;
}

function toBoolOrNull(v: any): boolean | null {
  if (v == null) return null;
  return Boolean(v);
}

function toTradeType(v: any): TradeType {
  const s = String(v || "").toUpperCase().trim();
  if (s === "SELL") return "SELL";
  if (s === "CLAIM") return "CLAIM";
  return "BUY";
}

function toSide(v: any): Side {
  const s = String(v || "").toUpperCase().trim();
  if (s === "A") return "A";
  if (s === "B") return "B";
  if (s === "C") return "C";
  return "C"; // ✅ default to C so DB never gets NULL (CLAIM / unknown)
}
function normWinnerTeamCode(v: any): string | null {
  if (v == null) return null;
  const s = String(v).toUpperCase().trim();
  return s ? s : null;
}

/**
 * Normalize winner_side + winner_team_code into a consistent encoding.
 *
 * Desired behavior:
 * - If winner_team_code indicates a tie => winner_side = 'C'
 * - Else winner_side = 'A' | 'B' (or null)
 */
function normalizeWinnerSide(opts: {
  winnerSide: any;
  winnerTeamCode: any;
  isFinal: any;
}): { winnerSide: string | null; winnerTeamCode: string | null } {
  const isFinal = opts.isFinal == null ? null : Boolean(opts.isFinal);

  const wtc = normWinnerTeamCode(opts.winnerTeamCode);

  // If it's not final yet, don't force anything
  if (!isFinal) {
    const wsRaw = opts.winnerSide == null ? null : String(opts.winnerSide).toUpperCase().trim();
    const ws =
      wsRaw === "A" || wsRaw === "B" || wsRaw === "C"
        ? wsRaw
        : null;

    return { winnerSide: ws, winnerTeamCode: wtc };
  }

  // Final game:
  // If team code says tie/draw => force side 'C'
  if (wtc === "TIE" || wtc === "DRAW") {
    return { winnerSide: "C", winnerTeamCode: "TIE" };
  }

  // Otherwise use winnerSide if valid
  const wsRaw = opts.winnerSide == null ? null : String(opts.winnerSide).toUpperCase().trim();
  const ws = wsRaw === "A" || wsRaw === "B" ? wsRaw : null;

  return { winnerSide: ws, winnerTeamCode: wtc };
}

export async function upsertUserTradesAndGames(opts: {
  user: string;
  tradeRows: any[];
}) {
  const user = String(opts.user || "").toLowerCase();

  // ---- map trade rows -> DB shape
  const trades: PersistTradeRow[] = (opts.tradeRows || [])
    .map((r: any) => {
      const g = r?.game ?? {};

const tType: TradeType = toTradeType(r?.type);
const tSide: Side = tType === "CLAIM" ? "C" : toSide(r?.side);

      const gameId = String(g?.id || r?.gameId || "");
      const league =
        g?.league != null ? String(g.league) : r?.league != null ? String(r.league) : null;

      const spotPriceBps = toNumOrNull(
        r?.spotPriceBps ?? r?.priceBps ?? r?.spot_price_bps ?? r?.spotPrice
      );
      const avgPriceBps = toNumOrNull(
        r?.avgPriceBps ?? r?.priceBps ?? r?.avg_price_bps ?? r?.avgPrice
      );

      const grossInDec = toStr(r?.grossInDec ?? r?.grossAmount ?? r?.gross_in_dec, "0");
      const grossOutDec = toStr(r?.grossOutDec ?? r?.gross_out_dec, "0");
      const feeDec = toStr(r?.feeDec ?? r?.fee ?? r?.fee_dec, "0");
      const netStakeDec = toStr(r?.netStakeDec ?? r?.amountDec ?? r?.net_stake_dec, "0");
      const netOutDec = toStr(r?.netOutDec ?? r?.net_out_dec, "0");

      const costBasisClosedDec = toStr(r?.costBasisClosedDec ?? r?.cost_basis_closed_dec, "0");
      const realizedPnlDec = toStr(r?.realizedPnlDec ?? r?.realized_pnl_dec, "0");

      return {
        id: String(r?.id || ""),
        user,
        type: tType,
        side: tSide,
        timestamp: toInt(r?.timestamp),
        txHash: r?.txHash ? String(r.txHash) : null,

        spotPriceBps,
        avgPriceBps,

        grossInDec,
        grossOutDec,
        feeDec,
        netStakeDec,
        netOutDec,

        costBasisClosedDec,
        realizedPnlDec,

        gameId,
        league,
      };
    })
    .filter((t) => t.id && t.gameId && t.timestamp > 0);

  // ---- map games (dedupe by gameId)
  const gamesById = new Map<string, PersistGameRow>();
  for (const r of opts.tradeRows || []) {
    const g = r?.game ?? {};
    const gameId = String(g?.id || "");
    if (!gameId) continue;

    if (!gamesById.has(gameId)) {
      const league = g?.league ?? null;
      const lockTime = g?.lockTime == null ? null : toInt(g.lockTime);
      const isFinal = toBoolOrNull(g?.isFinal);

      const normalized = normalizeWinnerSide({
        winnerSide: g?.winnerSide ?? null,
        winnerTeamCode: g?.winnerTeamCode ?? null,
        isFinal,
      });

      gamesById.set(gameId, {
        gameId,
        league,
        lockTime,
        isFinal,

        // ✅ normalized tie encoding
        winnerSide: normalized.winnerSide,
        winnerTeamCode: normalized.winnerTeamCode,

        teamACode: g?.teamACode ?? null,
        teamBCode: g?.teamBCode ?? null,
        teamAName: g?.teamAName ?? null,
        teamBName: g?.teamBName ?? null,
      });
    }
  }
  const games = [...gamesById.values()];

  if (!trades.length && !games.length) return { tradesUpserted: 0, gamesUpserted: 0 };

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    // Upsert games
    if (games.length) {
      const values: any[] = [];
      const chunks: string[] = [];
      games.forEach((g, i) => {
        const base = i * 10;
        chunks.push(
          `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10})`
        );
        values.push(
          g.gameId,
          g.league,
          g.lockTime,
          g.isFinal,
          g.winnerSide,     // now can be 'C'
          g.winnerTeamCode, // 'TIE'
          g.teamACode,
          g.teamBCode,
          g.teamAName,
          g.teamBName
        );
      });

      await client.query(
        `
        INSERT INTO public.games
          (game_id, league, lock_time, is_final, winner_side, winner_team_code, team_a_code, team_b_code, team_a_name, team_b_name)
        VALUES ${chunks.join(",")}
        ON CONFLICT (game_id) DO UPDATE SET
          league = EXCLUDED.league,
          lock_time = EXCLUDED.lock_time,
          is_final = EXCLUDED.is_final,
          winner_side = EXCLUDED.winner_side,
          winner_team_code = EXCLUDED.winner_team_code,
          team_a_code = EXCLUDED.team_a_code,
          team_b_code = EXCLUDED.team_b_code,
          team_a_name = EXCLUDED.team_a_name,
          team_b_name = EXCLUDED.team_b_name
        `,
        values
      );
    }

    // Upsert trade ledger (BUY/SELL/CLAIM)
    if (trades.length) {
      const values: any[] = [];
      const chunks: string[] = [];
      trades.forEach((t, i) => {
        const base = i * 17;
        chunks.push(
          `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15},$${base + 16},$${base + 17})`
        );
        values.push(
          t.id,
          t.user,
          t.type,
          t.side,
          t.timestamp,
          t.txHash,
          t.spotPriceBps,
          t.avgPriceBps,
          t.grossInDec,
          t.grossOutDec,
          t.feeDec,
          t.netStakeDec,
          t.netOutDec,
          t.costBasisClosedDec,
          t.realizedPnlDec,
          t.gameId,
          t.league
        );
      });

      await client.query(
        `
        INSERT INTO public.user_trade_events
          (id, user_address, type, side, timestamp, tx_hash,
           spot_price_bps, avg_price_bps,
           gross_in_dec, gross_out_dec, fee_dec, net_stake_dec, net_out_dec,
           cost_basis_closed_dec, realized_pnl_dec,
           game_id, league)
        VALUES ${chunks.join(",")}
        ON CONFLICT (id) DO UPDATE SET
          user_address = EXCLUDED.user_address,
          type = EXCLUDED.type,
          side = EXCLUDED.side,
          timestamp = EXCLUDED.timestamp,
          tx_hash = EXCLUDED.tx_hash,
          spot_price_bps = EXCLUDED.spot_price_bps,
          avg_price_bps = EXCLUDED.avg_price_bps,
          gross_in_dec = EXCLUDED.gross_in_dec,
          gross_out_dec = EXCLUDED.gross_out_dec,
          fee_dec = EXCLUDED.fee_dec,
          net_stake_dec = EXCLUDED.net_stake_dec,
          net_out_dec = EXCLUDED.net_out_dec,
          cost_basis_closed_dec = EXCLUDED.cost_basis_closed_dec,
          realized_pnl_dec = EXCLUDED.realized_pnl_dec,
          game_id = EXCLUDED.game_id,
          league = EXCLUDED.league
        `,
        values
      );
    }

    await client.query("COMMIT");
    return { tradesUpserted: trades.length, gamesUpserted: games.length };
  } catch (e) {
    await client.query("ROLLBACK");
    throw e;
  } finally {
    client.release();
  }
}
