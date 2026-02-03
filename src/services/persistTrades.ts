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
 * IMPORTANT FIX:
 * - On games upsert, NEVER overwrite existing non-null fields with NULL.
 *   (Use COALESCE(EXCLUDED.col, public.games.col))
 *
 * PROP SUPPORT:
 * - Persist market_type/topic/market_question/market_short into public.games when present
 *
 * NEW (OPTIONAL):
 * - Accept gameMetaById keyed by gameId/address. If provided, we merge it to fill missing fields.
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

  // winnerSide can be 'A' | 'B' | 'C' | null
  winnerSide: string | null;
  winnerTeamCode: string | null;

  teamACode: string | null;
  teamBCode: string | null;
  teamAName: string | null;
  teamBName: string | null;

  // ✅ PROP metadata
  marketType: string | null; // "PROP" | "GAME" (stored as text)
  topic: string | null;
  marketQuestion: string | null;
  marketShort: string | null;
};

export type GameMetaInput = Partial<{
  league: string;
  lockTime: number;

  teamACode: string;
  teamBCode: string;
  teamAName: string;
  teamBName: string;

  marketType: string; // "PROP" | "GAME"
  topic: string;
  marketQuestion: string;
  marketShort: string;
}>;

/* =========================
   Small helpers
========================= */

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

function cleanTeamCode(v: any): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  if (!s) return null;
  return s.toUpperCase();
}

function cleanTeamName(v: any): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

function cleanText(v: any): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s : null;
}

function cleanUpper(v: any): string | null {
  const s = cleanText(v);
  return s ? s.toUpperCase() : null;
}

/**
 * Pick the first non-empty string among keys on an object.
 * Lets us support multiple shapes coming from different ingesters/subgraph versions.
 */
function pickStr(obj: any, keys: string[]): string | null {
  for (const k of keys) {
    const v = obj?.[k];
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return null;
}

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

  if (wtc === "TIE" || wtc === "DRAW") {
    return { winnerSide: "C", winnerTeamCode: "TIE" };
  }

  const wsRaw =
    opts.winnerSide == null ? null : String(opts.winnerSide).toUpperCase().trim();
  const ws = wsRaw === "A" || wsRaw === "B" ? wsRaw : null;

  return { winnerSide: ws, winnerTeamCode: wtc };
}

function mergeGameMeta(base: PersistGameRow, meta?: GameMetaInput): PersistGameRow {
  if (!meta) return base;

  return {
    ...base,
    league: base.league ?? (meta.league ? String(meta.league) : null),
    lockTime: base.lockTime ?? (meta.lockTime != null ? toInt(meta.lockTime) : null),

    teamACode: base.teamACode ?? (meta.teamACode ? cleanTeamCode(meta.teamACode) : null),
    teamBCode: base.teamBCode ?? (meta.teamBCode ? cleanTeamCode(meta.teamBCode) : null),
    teamAName: base.teamAName ?? (meta.teamAName ? cleanTeamName(meta.teamAName) : null),
    teamBName: base.teamBName ?? (meta.teamBName ? cleanTeamName(meta.teamBName) : null),

    marketType: base.marketType ?? (meta.marketType ? cleanUpper(meta.marketType) : null),
    topic: base.topic ?? (meta.topic ? cleanText(meta.topic) : null),
    marketQuestion: base.marketQuestion ?? (meta.marketQuestion ? cleanText(meta.marketQuestion) : null),
    marketShort: base.marketShort ?? (meta.marketShort ? cleanText(meta.marketShort) : null),
  };
}

export async function upsertUserTradesAndGames(opts: {
  user: string;
  tradeRows: any[];
  // Optional: if a caller wants to provide meta directly instead of injecting into row.game
  gameMetaById?: Record<string, GameMetaInput | undefined>;
}) {
  const user = String(opts.user || "").toLowerCase();
  const gameMetaById = opts.gameMetaById || {};

  // ---- map trade rows -> DB shape (strict rules)
  const trades: PersistTradeRow[] = (opts.tradeRows || [])
    .map((r: any) => {
      const g = r?.game ?? {};

      const tType: TradeType = toTradeType(r?.type);

      let side: Side | null = null;
      if (tType === "CLAIM") side = "C";
      else side = toABSide(r?.side);

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
      return t.side === "A" || t.side === "B";
    })
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

      const teamACodeRaw =
        pickStr(g, ["teamACode", "team_a_code", "teamA_code", "homeCode", "team0Code"]) ?? null;
      const teamBCodeRaw =
        pickStr(g, ["teamBCode", "team_b_code", "teamB_code", "awayCode", "team1Code"]) ?? null;

      const teamANameRaw =
        pickStr(g, ["teamAName", "team_a_name", "homeName", "team0Name"]) ?? null;
      const teamBNameRaw =
        pickStr(g, ["teamBName", "team_b_name", "awayName", "team1Name"]) ?? null;

      // ✅ PROP fields (support multiple shapes)
      const marketTypeRaw = pickStr(g, ["marketType", "market_type", "type", "market"]) ?? null;
      const topicRaw = pickStr(g, ["topic", "marketTopic", "market_topic"]) ?? null;
      const marketQuestionRaw =
        pickStr(g, ["marketQuestion", "market_question", "question"]) ?? null;
      const marketShortRaw = pickStr(g, ["marketShort", "market_short", "short"]) ?? null;

      let row: PersistGameRow = {
        gameId,
        league,
        lockTime,
        isFinal,

        winnerSide: normalized.winnerSide,
        winnerTeamCode: normalized.winnerTeamCode,

        teamACode: cleanTeamCode(teamACodeRaw),
        teamBCode: cleanTeamCode(teamBCodeRaw),
        teamAName: cleanTeamName(teamANameRaw),
        teamBName: cleanTeamName(teamBNameRaw),

        marketType: cleanUpper(marketTypeRaw),
        topic: cleanText(topicRaw),
        marketQuestion: cleanText(marketQuestionRaw),
        marketShort: cleanText(marketShortRaw),
      };

      // Optional merge from caller-provided meta map
      const meta = gameMetaById[gameId];
      row = mergeGameMeta(row, meta);

      gamesById.set(gameId, row);
    }
  }

  // Also seed games if we have meta for a gameId referenced in trades but no row.game
  for (const t of trades) {
    const gameId = String(t.gameId || "");
    if (!gameId || gamesById.has(gameId)) continue;

    const meta = gameMetaById[gameId];
    if (!meta) continue;

    const seed: PersistGameRow = mergeGameMeta(
      {
        gameId,
        league: t.league ?? null,
        lockTime: null,
        isFinal: null,
        winnerSide: null,
        winnerTeamCode: null,
        teamACode: null,
        teamBCode: null,
        teamAName: null,
        teamBName: null,
        marketType: null,
        topic: null,
        marketQuestion: null,
        marketShort: null,
      },
      meta
    );

    gamesById.set(gameId, seed);
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
        const base = i * 14;
        chunks.push(
          `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13},$${base + 14})`
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
          g.teamBName,
          g.marketType,
          g.topic,
          g.marketQuestion,
          g.marketShort
        );
      });

      await client.query(
        `
        INSERT INTO public.games
          (game_id, league, lock_time, is_final, winner_side, winner_team_code,
           team_a_code, team_b_code, team_a_name, team_b_name,
           market_type, topic, market_question, market_short)
        VALUES ${chunks.join(",")}
        ON CONFLICT (game_id) DO UPDATE SET
          league = COALESCE(EXCLUDED.league, public.games.league),
          lock_time = COALESCE(EXCLUDED.lock_time, public.games.lock_time),
          is_final = COALESCE(EXCLUDED.is_final, public.games.is_final),

          winner_side = COALESCE(EXCLUDED.winner_side, public.games.winner_side),
          winner_team_code = COALESCE(EXCLUDED.winner_team_code, public.games.winner_team_code),

          team_a_code = COALESCE(EXCLUDED.team_a_code, public.games.team_a_code),
          team_b_code = COALESCE(EXCLUDED.team_b_code, public.games.team_b_code),
          team_a_name = COALESCE(EXCLUDED.team_a_name, public.games.team_a_name),
          team_b_name = COALESCE(EXCLUDED.team_b_name, public.games.team_b_name),

          market_type = COALESCE(EXCLUDED.market_type, public.games.market_type),
          topic = COALESCE(EXCLUDED.topic, public.games.topic),
          market_question = COALESCE(EXCLUDED.market_question, public.games.market_question),
          market_short = COALESCE(EXCLUDED.market_short, public.games.market_short)
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
