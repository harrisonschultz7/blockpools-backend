// src/services/persistTrades.ts
import { pool } from "../db";

type TradeType = "BUY" | "SELL" | "CLAIM";

// Keep side only for legacy semantics (CLAIM bucket / old UI)
// IMPORTANT: 'C' is RESERVED for CLAIM. Do NOT use 'C' to represent DRAW.
type Side = "A" | "B" | "C";

type PersistTradeRow = {
  id: string;
  user: string;
  type: TradeType;

  // Legacy side (optional for BUY/SELL, forced 'C' for CLAIM)
  side: Side | null;

  // ✅ Canonical outcome identifiers (MULTI + BINARY + 3-way)
  outcomeIndex: number | null;
  outcomeCode: string | null;

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

  winnerSide: string | null; // legacy (A/B only)
  winnerTeamCode: string | null; // legacy-ish, still useful

  // ✅ multi fields
  marketType: string | null; // "BINARY" | "MULTI" | "PROP" etc
  outcomesCount: number | null;
  resolutionType: string | null; // "UNRESOLVED" | "RESOLVED" ...
  winningOutcomeIndex: number | null;

  teamACode: string | null;
  teamBCode: string | null;
  teamAName: string | null;
  teamBName: string | null;

  // props (keep)
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

  marketType: string;
  outcomesCount: number;
  resolutionType: string;
  winningOutcomeIndex: number;

  topic: string;
  marketQuestion: string;
  marketShort: string;
}>;

/* =========================
   Helpers
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

function toABSide(v: any): "A" | "B" | null {
  const s = String(v || "").toUpperCase().trim();
  if (s === "A") return "A";
  if (s === "B") return "B";
  return null;
}

function cleanTeamCode(v: any): string | null {
  if (v == null) return null;
  const s = String(v).trim();
  return s ? s.toUpperCase() : null;
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

function pickStr(obj: any, keys: string[]): string | null {
  for (const k of keys) {
    const v = obj?.[k];
    if (v == null) continue;
    const s = String(v).trim();
    if (s) return s;
  }
  return null;
}

function pickNum(obj: any, keys: string[]): number | null {
  for (const k of keys) {
    const v = obj?.[k];
    if (v == null || v === "") continue;
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

/**
 * Canonical outcomeCode normalization.
 * - If outcomeCode is present -> uppercase it (team code / "DRAW" / etc)
 * - If missing AND outcomeIndex indicates a draw (commonly 2) -> set "DRAW"
 *   (Adjust if your protocol uses a different index for draw.)
 */
function normalizeOutcomeCode(outcomeIndex: number | null, rawOutcomeCode: any): string | null {
  const cleaned = cleanTeamCode(rawOutcomeCode);
  if (cleaned) return cleaned;

  // ✅ common 3-way convention: index 2 = DRAW
  if (outcomeIndex === 2) return "DRAW";

  return null;
}

function mergeGameMeta(base: PersistGameRow, meta?: GameMetaInput): PersistGameRow {
  if (!meta) return base;

  return {
    ...base,
    league: base.league ?? (meta.league ? String(meta.league) : null),
    lockTime: base.lockTime ?? (meta.lockTime != null ? toInt(meta.lockTime) : null),

    teamACode: base.teamACode ?? (meta.teamACode ? cleanTeamCode(meta.teamACode) : null),
    teamBCode: base.teamBCode ?? (meta.teamBCode ? cleanTeamCode(meta.teamBCode) : null),
    teamAName: base.teamAName ?? (meta.teamAName ? cleanText(meta.teamAName) : null),
    teamBName: base.teamBName ?? (meta.teamBName ? cleanText(meta.teamBName) : null),

    marketType: base.marketType ?? (meta.marketType ? cleanUpper(meta.marketType) : null),
    outcomesCount: base.outcomesCount ?? (meta.outcomesCount != null ? toInt(meta.outcomesCount) : null),
    resolutionType: base.resolutionType ?? (meta.resolutionType ? cleanUpper(meta.resolutionType) : null),
    winningOutcomeIndex:
      base.winningOutcomeIndex ?? (meta.winningOutcomeIndex != null ? toInt(meta.winningOutcomeIndex) : null),

    topic: base.topic ?? (meta.topic ? cleanText(meta.topic) : null),
    marketQuestion: base.marketQuestion ?? (meta.marketQuestion ? cleanText(meta.marketQuestion) : null),
    marketShort: base.marketShort ?? (meta.marketShort ? cleanText(meta.marketShort) : null),
  };
}

export async function upsertUserTradesAndGames(opts: {
  user: string;
  tradeRows: any[];
  gameMetaById?: Record<string, GameMetaInput | undefined>;
}) {
  const user = String(opts.user || "").toLowerCase();
  const gameMetaById = opts.gameMetaById || {};

  /* =========================
     Map trade rows -> DB shape
  ========================= */

  const trades: PersistTradeRow[] = (opts.tradeRows || [])
    .map((r: any) => {
      const g = r?.game ?? {};

      const tType: TradeType = toTradeType(r?.type);

      const rawSide = String(r?.side ?? "").toUpperCase().trim();

      // legacy side:
      // - CLAIM rows are always side='C'
      // - BUY/SELL keep side ONLY for old binary semantics; MULTI/3-way should be null and rely on outcomeIndex/outcomeCode.
      let side: Side | null = null;
      if (tType === "CLAIM") side = "C";
      else side = toABSide(rawSide); // NOTE: does NOT return 'C'

      const outcomeIndexRaw = toNumOrNull(r?.outcomeIndex ?? r?.outcome_index);

      // ✅ IMPORTANT: some historical 3-way pipelines emit DRAW as side='C' on BUY/SELL (legacy)
      // If it's NOT a CLAIM and outcomeIndex is missing, infer draw outcomeIndex=2.
      const inferredOutcomeIndex =
        tType !== "CLAIM" && rawSide === "C" && outcomeIndexRaw == null ? 2 : outcomeIndexRaw;

      const outcomeIndex = inferredOutcomeIndex == null ? null : Math.trunc(inferredOutcomeIndex);

      const outcomeCode = normalizeOutcomeCode(outcomeIndex, r?.outcomeCode ?? r?.outcome_code);

      const gameId = String(g?.id || r?.gameId || r?.game_id || "");
      const league = g?.league != null ? String(g.league) : r?.league != null ? String(r.league) : null;

      const spotPriceBps = toNumOrNull(r?.spotPriceBps ?? r?.spot_price_bps ?? r?.spotPrice);
      const avgPriceBps = toNumOrNull(r?.avgPriceBps ?? r?.avg_price_bps ?? r?.avgPrice);

      const grossInDec = toStr(r?.grossInDec ?? r?.gross_in_dec ?? r?.grossAmount, "0");
      const grossOutDec = toStr(r?.grossOutDec ?? r?.gross_out_dec, "0");
      const feeDec = toStr(r?.feeDec ?? r?.fee_dec ?? r?.fee, "0");
      const netStakeDec = toStr(r?.netStakeDec ?? r?.net_stake_dec ?? r?.amountDec, "0");
      const netOutDec = toStr(r?.netOutDec ?? r?.net_out_dec, "0");

      const costBasisClosedDec = toStr(r?.costBasisClosedDec ?? r?.cost_basis_closed_dec, "0");
      const realizedPnlDec = toStr(r?.realizedPnlDec ?? r?.realized_pnl_dec, "0");

      return {
        id: String(r?.id || ""),
        user,
        type: tType,
        side,

        outcomeIndex,
        outcomeCode,

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
    .filter((t: PersistTradeRow) => {
      if (!t.id || !t.gameId || t.timestamp <= 0) return false;

      // ✅ CLAIM rows are allowed without outcomeIndex/outcomeCode.
      if (t.type === "CLAIM") return true;

      // ✅ BUY/SELL must have outcomeIndex (canonical). outcomeCode is recommended but not required.
      if (t.outcomeIndex == null) return false;

      return true;
    });

  /* =========================
     Map games (dedupe by gameId)
  ========================= */

  const gamesById = new Map<string, PersistGameRow>();

  for (const r of opts.tradeRows || []) {
    const g = r?.game ?? {};
    const gameId = String(g?.id || "");
    if (!gameId) continue;

    if (gamesById.has(gameId)) continue;

    const league = g?.league ?? null;
    const lockTime = g?.lockTime == null ? null : toInt(g.lockTime);
    const isFinal = toBoolOrNull(g?.isFinal);

    const marketType = cleanUpper(pickStr(g, ["marketType", "market_type", "type"]) ?? null);
    const outcomesCount = pickNum(g, ["outcomesCount", "outcomes_count"]) ?? null;
    const resolutionType = cleanUpper(pickStr(g, ["resolutionType", "resolution_type"]) ?? null);
    const winningOutcomeIndex = pickNum(g, ["winningOutcomeIndex", "winning_outcome_index"]) ?? null;

    // legacy team fields still exist for BINARY and are useful for UI fallbacks
    const teamACode = cleanTeamCode(pickStr(g, ["teamACode", "team_a_code"]) ?? null);
    const teamBCode = cleanTeamCode(pickStr(g, ["teamBCode", "team_b_code"]) ?? null);
    const teamAName = cleanText(pickStr(g, ["teamAName", "team_a_name"]) ?? null);
    const teamBName = cleanText(pickStr(g, ["teamBName", "team_b_name"]) ?? null);

    // ✅ Legacy winnerSide is A/B ONLY. Do NOT accept 'C' here.
    const winnerSideRaw = pickStr(g, ["winnerSide", "winner_side"]) ?? null;
    const winnerSideCandidate = winnerSideRaw ? String(winnerSideRaw).toUpperCase().trim() : null;
    const winnerSide = winnerSideCandidate === "A" || winnerSideCandidate === "B" ? winnerSideCandidate : null;

    const winnerTeamCode = cleanTeamCode(pickStr(g, ["winnerTeamCode", "winner_team_code"]) ?? null);

    const topic = cleanText(pickStr(g, ["topic", "marketTopic", "market_topic"]) ?? null);
    const marketQuestion = cleanText(pickStr(g, ["marketQuestion", "market_question", "question"]) ?? null);
    const marketShort = cleanText(pickStr(g, ["marketShort", "market_short", "short"]) ?? null);

    let row: PersistGameRow = {
      gameId,
      league,
      lockTime,
      isFinal,

      winnerSide,
      winnerTeamCode,

      marketType,
      outcomesCount: outcomesCount == null ? null : Math.trunc(outcomesCount),
      resolutionType,
      winningOutcomeIndex: winningOutcomeIndex == null ? null : Math.trunc(winningOutcomeIndex),

      teamACode,
      teamBCode,
      teamAName,
      teamBName,

      topic,
      marketQuestion,
      marketShort,
    };

    row = mergeGameMeta(row, gameMetaById[gameId]);
    gamesById.set(gameId, row);
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

      // 18 columns
      games.forEach((g, i) => {
        const base = i * 18;
        chunks.push(
          `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15},$${base + 16},$${base + 17},$${base + 18})`
        );
        values.push(
          g.gameId,
          g.league,
          g.lockTime,
          g.isFinal,
          g.winnerSide,
          g.winnerTeamCode,

          g.marketType,
          g.outcomesCount,
          g.resolutionType,
          g.winningOutcomeIndex,

          g.teamACode,
          g.teamBCode,
          g.teamAName,
          g.teamBName,

          g.topic,
          g.marketQuestion,
          g.marketShort,

          null // reserved placeholder
        );
      });

      await client.query(
        `
        INSERT INTO public.games
          (game_id, league, lock_time, is_final, winner_side, winner_team_code,
           market_type, outcomes_count, resolution_type, winning_outcome_index,
           team_a_code, team_b_code, team_a_name, team_b_name,
           topic, market_question, market_short,
           _reserved)
        VALUES ${chunks.join(",")}
        ON CONFLICT (game_id) DO UPDATE SET
          league = COALESCE(EXCLUDED.league, public.games.league),
          lock_time = COALESCE(EXCLUDED.lock_time, public.games.lock_time),
          is_final = COALESCE(EXCLUDED.is_final, public.games.is_final),

          winner_side = COALESCE(EXCLUDED.winner_side, public.games.winner_side),
          winner_team_code = COALESCE(EXCLUDED.winner_team_code, public.games.winner_team_code),

          market_type = COALESCE(EXCLUDED.market_type, public.games.market_type),
          outcomes_count = COALESCE(EXCLUDED.outcomes_count, public.games.outcomes_count),
          resolution_type = COALESCE(EXCLUDED.resolution_type, public.games.resolution_type),
          winning_outcome_index = COALESCE(EXCLUDED.winning_outcome_index, public.games.winning_outcome_index),

          team_a_code = COALESCE(EXCLUDED.team_a_code, public.games.team_a_code),
          team_b_code = COALESCE(EXCLUDED.team_b_code, public.games.team_b_code),
          team_a_name = COALESCE(EXCLUDED.team_a_name, public.games.team_a_name),
          team_b_name = COALESCE(EXCLUDED.team_b_name, public.games.team_b_name),

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

      // 19 columns
      trades.forEach((t, i) => {
        const base = i * 19;
        chunks.push(
          `($${base + 1},$${base + 2},$${base + 3},$${base + 4},$${base + 5},$${base + 6},$${base + 7},$${base + 8},$${base + 9},$${base + 10},$${base + 11},$${base + 12},$${base + 13},$${base + 14},$${base + 15},$${base + 16},$${base + 17},$${base + 18},$${base + 19})`
        );
        values.push(
          t.id,
          t.user,
          t.type,
          t.side,
          t.outcomeIndex,
          t.outcomeCode,
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
          (id, user_address, type, side, outcome_index, outcome_code, timestamp, tx_hash,
           spot_price_bps, avg_price_bps,
           gross_in_dec, gross_out_dec, fee_dec, net_stake_dec, net_out_dec,
           cost_basis_closed_dec, realized_pnl_dec,
           game_id, league)
        VALUES ${chunks.join(",")}
        ON CONFLICT (id) DO UPDATE SET
          user_address = EXCLUDED.user_address,
          type = EXCLUDED.type,
          side = EXCLUDED.side,
          outcome_index = EXCLUDED.outcome_index,
          outcome_code = EXCLUDED.outcome_code,
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
