// src/services/persistTrades.ts
import { pool } from "../db";

type PersistTradeRow = {
  id: string;
  user: string;
  type: "BUY" | "SELL";
  side: "A" | "B";
  timestamp: number;
  txHash: string | null;

  // amounts / prices (keep as strings if your DB columns are numeric)
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
  winnerSide: string | null;
  winnerTeamCode: string | null;
  teamACode: string | null;
  teamBCode: string | null;
  teamAName: string | null;
  teamBName: string | null;
};

export async function upsertUserTradesAndGames(opts: {
  user: string;
  tradeRows: any[];
}) {
  const user = opts.user.toLowerCase();

  // ---- map trade rows -> DB shape
  const trades: PersistTradeRow[] = (opts.tradeRows || [])
    .map((r: any) => {
      const g = r?.game ?? {};
      return {
        id: String(r?.id || ""),
        user,
        type: (r?.type === "SELL" ? "SELL" : "BUY"),
        side: (r?.side === "B" ? "B" : "A"),
        timestamp: Number(r?.timestamp || 0),
        txHash: r?.txHash ? String(r.txHash) : null,

        spotPriceBps: r?.spotPriceBps == null ? null : Number(r.spotPriceBps),
        avgPriceBps: r?.avgPriceBps == null ? null : Number(r.avgPriceBps),

        grossInDec: String(r?.grossInDec ?? "0"),
        grossOutDec: String(r?.grossOutDec ?? "0"),
        feeDec: String(r?.feeDec ?? "0"),
        netStakeDec: String(r?.netStakeDec ?? "0"),
        netOutDec: String(r?.netOutDec ?? "0"),
        costBasisClosedDec: String(r?.costBasisClosedDec ?? "0"),
        realizedPnlDec: String(r?.realizedPnlDec ?? "0"),

        gameId: String(g?.id || r?.gameId || ""),
        league: g?.league ? String(g.league) : null,
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
      gamesById.set(gameId, {
        gameId,
        league: g?.league ?? null,
        lockTime: g?.lockTime == null ? null : Number(g.lockTime),
        isFinal: g?.isFinal == null ? null : Boolean(g.isFinal),
        winnerSide: g?.winnerSide ?? null,
        winnerTeamCode: g?.winnerTeamCode ?? null,
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

    // Upsert games (optional but recommended for filters/labels)
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
          g.winnerSide,
          g.winnerTeamCode,
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

    // Upsert trade ledger
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
