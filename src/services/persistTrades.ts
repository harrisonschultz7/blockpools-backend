// src/services/persistTrades.ts
import { pool } from "../db";

/**
 * Goal:
 * - Persist BUY/SELL/CLAIM rows into public.user_trade_events
 * - Persist game metadata into public.games
 * - Ensure CLAIM rows always have side='C'
 * - Ensure BUY/SELL rows NEVER persist with side='C' or null (drop them if malformed)
 * - Normalize tie encoding for games:
 *     winner_team_code in (TIE, DRAW) => winner_side='C' and winner_team_code='TIE'
 *
 * This avoids both:
 *  - NOT NULL errors on side
 *  - CHECK constraint violations on side (common when CLAIM has null side)
 *  - Silent corruption (BUY/SELL accidentally stored with side='C')
 */

type TradeType = "BUY" | "SELL" | "CLAIM";
type Side = "A" | "B" | "C"; // DB encoding: C = tie/claim bucket

type PersistTradeRow = {
  id: string;
  user: string;
  type: TradeType;
  side: Side; // never null by the time we insert
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

// BUY/SELL side parser (A/B only). We do NOT default to C here.
function toABSide(v: any): "A" | "B" | null {
  const s = String(v || "").toUpperCase().trim();
  if (s === "A") return "A";
  if (s === "B") return "B";
  return null;
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
 * - If winner_team_code indicates a tie => winner_side = 'C' and winner_team_code='TIE'
 * - Else winner_side = 'A' | 'B' (or null)
 *
 * Note: If the game is not final, do not force anything (store what we have if valid).
 */
function normalizeWinnerSide(opts: {
  winnerSide: any;
  winnerTeamCode: any;
  isFinal: any;
}): { winnerSide: string | null; winnerTeamCode: string | null } {
  const isFinal = opts.isFinal == null ? null : Boolean(opts.isFinal);
  const wtc = normWinnerTeamCode(opts.winnerTeamCode);

  if (!isFinal) {
    const wsRaw =
      opts.winnerSide == null ? null : String(opts.winnerSide).toUpperCase().trim();
    const ws = wsRaw === "A" || wsRaw === "B" || wsRaw === "C" ? wsRaw : null;
    return { winnerSide: ws, winnerTeamCode: wtc };
  }

  // Final game: tie/draw => C + TIE
  if (wtc === "TIE" || wtc === "DRAW") {
    return { winnerSide: "C", winnerTeamCode: "TIE" };
  }

  // Otherwise winnerSide must be A/B to be valid
  const wsRaw =
    opts.winnerSide == null ? null : String(opts.winnerSide).toUpperCase().trim();
  const ws = wsRaw === "A" || wsRaw === "B" ? wsRaw : null;

  return { winnerSide: ws, winnerTeamCode: wtc };
}

export async function upsertUserTradesAndGames(opts: { user: string; tradeRows: any[] }) {
  const user = String(opts.user || "").toLowerCase();

  // ---- map trade rows -> DB shape (strict rules)
  // - CLAIM => side='C' always
  // - BUY/SELL => side must be 'A' or 'B' else drop the row
  const trades: PersistTradeRow[] = (opts.tradeRows || [])
    .map((r: any) => {
      const g = r?.game ?? {};

      const tType: TradeType = toTradeType(r?.type);

      let side: Side | null = null;
      if (tType === "CLAIM") {
        side = "C";
      } else {
        const ab = toABSide(r?.side);
        side = ab; // A/B or null
      }

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
        // TEMP: side can be null here, we filter below; cast after filtering
        side: (side as any) as Side,
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
    .filter((t: any) => {
      if (!t.id || !t.gameId || t.timestamp <= 0) return false;
      if (t.type === "CLAIM") return true;
      // BUY/SELL must be A/B only
      return t.side === "A" || t.side === "B";
    })
    // now the type is truly PersistTradeRow (side guaranteed non-null)
    .map((t: any) => t as PersistTradeRow);

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
          g.winnerSide, // can be 'C'
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
