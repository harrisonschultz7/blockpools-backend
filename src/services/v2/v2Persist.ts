// src/services/v2/v2Persist.ts
//
// Persistence for the v2 (order-book) exchange. Two responsibilities:
//   1. Durable order + fill records in the `v2` schema (Open Orders tab survives
//      matcher restarts; monitoring has a source of truth).
//   2. Map every settled fill leg into public.user_trade_events with the SAME
//      shape an AMM trade uses, so the leaderboard/profile read v2 with zero
//      re-wiring (see v2MarketResolver for the mapping rationale).
//
// The matcher is a trusted backend service; it POSTs order/fill/cancel lifecycle
// events here (see routes/v2Routes.ts). All writes are idempotent so retries and
// re-deliveries never double-count.

import { JsonRpcProvider, Contract } from "ethers";
import { pool } from "../../db";
import { upsertUserTradesAndGames } from "../persistTrades";
import { resolveV2Leg, resolveV2Market, V2MarketEntry } from "./v2MarketResolver";

const SCALE = 1_000_000n; // 1e6 == $1.00/share and 1e6 == 1 share

const V2_VAULT_ADDRESS =
  process.env.V2_VAULT_ADDRESS || "0x14BD1fd3911C22173FeaEcBfD670D09c1143A594";
// Read-only RPC for sharesOf at resolution. Falls back to the public Arbitrum
// endpoint (same default as refreshGamesAndScores) so no extra env is required.
const V2_RPC_URL =
  process.env.ARBITRUM_RPC_URL || process.env.RPC_URL || "https://arb1.arbitrum.io/rpc";

let _vault: Contract | null = null;
function vault(): Contract | null {
  if (_vault) return _vault;
  if (!V2_RPC_URL) return null;
  const provider = new JsonRpcProvider(V2_RPC_URL);
  _vault = new Contract(
    V2_VAULT_ADDRESS,
    ["function sharesOf(bytes32 marketId, uint8 outcome, address user) view returns (uint256)"],
    provider
  );
  return _vault;
}

// Market-maker / operator wallets whose fills seed liquidity — excluded from the
// leaderboard the same way the AMM house is (they still land in v2.fills for
// monitoring). Comma-separated, lowercased.
const MM_WALLETS = new Set(
  String(process.env.V2_MM_WALLETS || process.env.V2_OPERATOR_WALLET || "")
    .split(",")
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);

function low(s: any): string {
  return String(s ?? "").toLowerCase();
}
function bi(v: any): bigint {
  try {
    return BigInt(String(v ?? "0").split(".")[0] || "0");
  } catch {
    return 0n;
  }
}
/** micro-USD (1e6 == $1) bigint -> decimal string like "12.500000". */
function microToDec(micro: bigint): string {
  const neg = micro < 0n;
  const a = neg ? -micro : micro;
  const whole = a / SCALE;
  const frac = (a % SCALE).toString().padStart(6, "0");
  return `${neg ? "-" : ""}${whole}.${frac}`;
}
/** 1e6 price units -> integer basis points (1e6 == 10000 bps). */
function priceToBps(price1e6: bigint): number {
  return Number(price1e6 / 100n);
}

export type V2OrderInput = {
  hash: string;
  maker: string;
  marketId: string;
  outcome: number; // 0 | 1
  side: number; // 0 = BUY, 1 = SELL
  shares: string | number | bigint; // 6dp raw units
  price: string | number | bigint; // 1e6 units
  feeRateBps?: number;
  expiry?: number | string;
  salt: string | number | bigint;
  signature: string;
  // Matcher's authoritative post-cross state:
  remaining?: string | number | bigint; // defaults to shares
  filledShares?: string | number | bigint; // defaults to shares - remaining
  status?: string; // open|partial|filled|cancelled|expired
};

/**
 * Upsert one order (idempotent). The matcher sends this on accept and again each
 * time the order's remaining changes (rested, or filled as a counter), so
 * v2.orders always reflects current resting depth.
 */
export async function recordV2Order(o: V2OrderInput): Promise<void> {
  const shares = bi(o.shares);
  const remaining = o.remaining != null ? bi(o.remaining) : shares;
  const filled = o.filledShares != null ? bi(o.filledShares) : shares - remaining;
  const status =
    o.status ||
    (remaining <= 0n ? "filled" : remaining < shares ? "partial" : "open");

  const m: V2MarketEntry | null = resolveV2Market(o.marketId);
  const leg = m?.outcomes[Number(o.outcome)] || null;

  await pool.query(
    `
    INSERT INTO v2.orders
      (order_hash, maker, market_id, outcome, side, shares, price, fee_rate_bps,
       expiry, salt, signature, status, filled_shares, remaining,
       game_id, parent_game_id, parent_outcome_index, outcome_code, market_type, league,
       created_at, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20, now(), now())
    ON CONFLICT (order_hash) DO UPDATE SET
      status = EXCLUDED.status,
      filled_shares = EXCLUDED.filled_shares,
      remaining = EXCLUDED.remaining,
      updated_at = now()
    `,
    [
      low(o.hash),
      low(o.maker),
      low(o.marketId),
      Number(o.outcome),
      Number(o.side),
      shares.toString(),
      bi(o.price).toString(),
      Number(o.feeRateBps || 0),
      Number(o.expiry || 0),
      String(o.salt),
      o.signature,
      status,
      filled.toString(),
      remaining.toString(),
      m?.gameId ?? null,
      m?.parentGameId ?? null,
      m?.parentOutcomeIndex ?? null,
      leg?.outcomeCode ?? null,
      m?.marketType ?? null,
      m?.league ?? null,
    ]
  );
}

export async function cancelV2Order(hash: string): Promise<void> {
  await pool.query(
    `UPDATE v2.orders SET status = 'cancelled', updated_at = now() WHERE order_hash = $1`,
    [low(hash)]
  );
}

type LegInput = {
  hash?: string;
  maker: string; // wallet
  marketId: string;
  outcome: number;
  side: number; // 0 BUY, 1 SELL
  price: string | number | bigint; // the order's own limit price (1e6)
};

export type V2FillInput = {
  txHash: string;
  matchType: "complementary" | "mint" | "merge";
  fill: string | number | bigint; // 6dp shares filled
  makerPrice: string | number | bigint; // b (resting) order price, settlement ref (1e6)
  a: LegInput; // taker / incoming
  b: LegInput; // resting / maker counter
  blockNumber?: number | null;
};

/**
 * Effective per-share price (1e6 units) a leg trades at, given the match type.
 *  - complementary: both legs settle at the maker price P.
 *  - mint / merge:  the resting (maker) leg is P; the taker leg funds the $1
 *                   remainder, so it is (SCALE - P).
 */
function legPrice(matchType: string, isTaker: boolean, makerPrice: bigint): bigint {
  if (matchType === "complementary") return makerPrice;
  return isTaker ? SCALE - makerPrice : makerPrice;
}

/**
 * Per-leg economics for one settled fill leg. Pure + exported for testing.
 *  - price:     effective per-share price (1e6 units) this leg trades at
 *  - amountDec: USD notional decimal string (stake for BUY, proceeds for SELL)
 *  - bps:       price in basis points (1e6 == 10000 bps)
 */
export function legEconomics(
  matchType: string,
  isTaker: boolean,
  makerPrice1e6: bigint | string | number,
  fill1e6: bigint | string | number
): { price: bigint; microUsd: bigint; amountDec: string; bps: number } {
  const P = bi(makerPrice1e6);
  const F = bi(fill1e6);
  const price = legPrice(matchType, isTaker, P);
  const microUsd = (price * F) / SCALE;
  return { price, microUsd, amountDec: microToDec(microUsd), bps: priceToBps(price) };
}

/**
 * Build the user_trade_events row for one leg. Returns a discriminated result so
 * the caller can tell "skipped because MM" (fine) apart from "skipped because
 * the market didn't resolve" (a data-loss bug — usually a stale/missing
 * games.json; see GAMES_JSON_PATH) which must be logged loudly.
 */
function buildStatsRow(
  txHash: string,
  suffix: "a" | "b",
  legInput: LegInput,
  matchType: string,
  fill: bigint,
  makerPrice: bigint
): { ok: true; user: string; row: any; gameMeta: any } | { ok: false; reason: "mm" | "unmapped" } {
  const wallet = low(legInput.maker);
  if (!wallet || MM_WALLETS.has(wallet)) return { ok: false, reason: "mm" }; // house — off leaderboard

  const resolved = resolveV2Leg(legInput.marketId, legInput.outcome);
  if (!resolved) return { ok: false, reason: "unmapped" };
  const { entry, leg } = resolved;

  const { amountDec, bps } = legEconomics(matchType, suffix === "a", makerPrice, fill);
  const isBuy = Number(legInput.side) === 0;

  const row: any = {
    id: `v2-${low(txHash)}-${suffix}`,
    type: isBuy ? "BUY" : "SELL",
    side: leg.side,
    outcomeIndex: leg.outcomeIndex,
    outcomeCode: leg.outcomeCode,
    timestamp: Math.floor(Date.now() / 1000),
    txHash,
    spotPriceBps: bps,
    avgPriceBps: bps,
    grossInDec: isBuy ? amountDec : "0",
    grossOutDec: isBuy ? "0" : amountDec,
    feeDec: "0",
    netStakeDec: isBuy ? amountDec : "0",
    netOutDec: isBuy ? "0" : amountDec,
    costBasisClosedDec: "0",
    realizedPnlDec: "0",
    game: {
      id: entry.gameId,
      league: entry.league,
      marketType: entry.marketType,
      outcomesCount: entry.outcomesCount,
      teamAName: entry.teamAName,
      teamBName: entry.teamBName,
      marketQuestion: entry.marketQuestion,
    },
    __source: "v2-fill",
  };

  return {
    ok: true,
    user: wallet,
    row,
    gameMeta: {
      league: entry.league,
      marketType: entry.marketType,
      outcomesCount: entry.outcomesCount,
      teamAName: entry.teamAName,
      teamBName: entry.teamBName,
      marketQuestion: entry.marketQuestion,
    },
  };
}

/**
 * Record one settled fill: insert into v2.fills and map each non-MM leg into
 * user_trade_events. Idempotent (v2.fills PK = tx_hash; trade ids are
 * v2-<tx>-a/b with ON CONFLICT DO UPDATE).
 */
export async function recordV2Fill(f: V2FillInput): Promise<{ fillId: string; statsRows: number }> {
  const txHash = low(f.txHash);
  const fill = bi(f.fill);
  const makerPrice = bi(f.makerPrice);
  const market = resolveV2Market(f.a.marketId);

  // 1) v2.fills (durable settlement record; keeps MM legs too, for monitoring).
  await pool.query(
    `
    INSERT INTO v2.fills
      (id, tx_hash, market_id, match_type, fill_shares, maker_price,
       a_hash, b_hash, a_maker, b_maker, a_side, b_side, a_outcome, b_outcome,
       game_id, league, block_number, created_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17, now())
    ON CONFLICT (id) DO NOTHING
    `,
    [
      txHash,
      txHash,
      low(f.a.marketId),
      f.matchType,
      fill.toString(),
      makerPrice.toString(),
      f.a.hash ? low(f.a.hash) : null,
      f.b.hash ? low(f.b.hash) : null,
      low(f.a.maker),
      low(f.b.maker),
      Number(f.a.side),
      Number(f.b.side),
      Number(f.a.outcome),
      Number(f.b.outcome),
      market?.parentGameId ?? null,
      market?.league ?? null,
      f.blockNumber ?? null,
    ]
  );

  // 2) Map each leg into user_trade_events (skips MM; loudly flags unmapped).
  const built = [
    { leg: buildStatsRow(txHash, "a", f.a, f.matchType, fill, makerPrice), marketId: f.a.marketId },
    { leg: buildStatsRow(txHash, "b", f.b, f.matchType, fill, makerPrice), marketId: f.b.marketId },
  ];

  let statsRows = 0;
  for (const { leg, marketId } of built) {
    if (leg.ok === false) {
      if (leg.reason === "unmapped") {
        // A real user's trade we could NOT put on the leaderboard — almost always
        // a stale/missing games.json on the backend (set GAMES_JSON_PATH).
        console.warn(
          `[v2/fill] UNMAPPED market ${marketId} (tx ${txHash}) — leg NOT written to ` +
            `user_trade_events. Check GAMES_JSON_PATH points at the live games.json.`
        );
      }
      continue;
    }
    await upsertUserTradesAndGames({
      user: leg.user,
      tradeRows: [leg.row],
      gameMetaById: { [leg.row.game.id]: leg.gameMeta },
    });
    statsRows++;
  }

  return { fillId: txHash, statsRows };
}

export type OpenOrderRow = {
  order_hash: string;
  maker: string;
  market_id: string;
  outcome: number;
  side: number;
  shares: string;
  price: string;
  fee_rate_bps: number;
  expiry: string;
  salt: string;
  signature: string;
  status: string;
  filled_shares: string;
  remaining: string;
  game_id: string | null;
  parent_game_id: string | null;
  parent_outcome_index: number | null;
  outcome_code: string | null;
  market_type: string | null;
  league: string | null;
};

/**
 * Open (resting) orders — used by the matcher to rehydrate its book on boot and
 * by the frontend Open Orders tab (durable across restarts). Filter by market
 * and/or maker.
 */
export async function getOpenV2Orders(opts: {
  marketId?: string;
  maker?: string;
  limit?: number;
}): Promise<OpenOrderRow[]> {
  const where: string[] = ["status IN ('open','partial')", "remaining > 0"];
  const params: any[] = [];
  if (opts.marketId) {
    params.push(low(opts.marketId));
    where.push(`market_id = $${params.length}`);
  }
  if (opts.maker) {
    params.push(low(opts.maker));
    where.push(`maker = $${params.length}`);
  }
  params.push(Math.min(Math.max(Number(opts.limit || 1000), 1), 5000));
  const { rows } = await pool.query(
    `SELECT order_hash, maker, market_id, outcome, side, shares, price, fee_rate_bps,
            expiry, salt, signature, status, filled_shares, remaining,
            game_id, parent_game_id, parent_outcome_index, outcome_code, market_type, league
       FROM v2.orders
      WHERE ${where.join(" AND ")}
      ORDER BY created_at ASC
      LIMIT $${params.length}`,
    params
  );
  return rows as OpenOrderRow[];
}

/**
 * Write realized-PnL CLAIM rows when a v2 market resolves. Payout per holder is
 * read on-chain (authoritative): winning shares × $1, or for an even void the
 * average of both outcomes' shares. Idempotent (id = v2claim-<market>-<holder>).
 * Also marks the market's open orders resolved so the Open Orders tab clears.
 */
export async function recordV2Resolution(opts: {
  marketId: string;
  winningOutcome: number | null; // 0|1, or null for even void [1,1]
  gameId?: string;
}): Promise<{ claims: number; holders: number }> {
  const marketId = low(opts.marketId);
  const entry = resolveV2Market(marketId);
  const gameId = entry?.gameId || (opts.gameId ? String(opts.gameId) : null);
  const league = entry?.league ?? null;

  // Mark resting orders resolved (Open Orders tab clears; the matcher clears its
  // in-memory book separately via /clear-market + the resolution sweeper).
  await pool.query(
    `UPDATE v2.orders SET status='resolved', updated_at=now()
      WHERE market_id=$1 AND status IN ('open','partial')`,
    [marketId]
  );

  const v = vault();
  if (!v || !gameId) return { claims: 0, holders: 0 };

  // Every non-MM wallet that ever traded this market.
  const { rows } = await pool.query(
    `SELECT DISTINCT m AS holder FROM (
        SELECT a_maker AS m FROM v2.fills WHERE market_id=$1
        UNION SELECT b_maker AS m FROM v2.fills WHERE market_id=$1
     ) h WHERE m IS NOT NULL`,
    [marketId]
  );
  const holders = rows.map((r: any) => low(r.holder)).filter((h: string) => h && !MM_WALLETS.has(h));

  const win = opts.winningOutcome;
  let claims = 0;
  for (const holder of holders) {
    let payoutMicro = 0n;
    try {
      if (win === 0 || win === 1) {
        payoutMicro = BigInt(await v.sharesOf(marketId, win, holder)); // winning shares × $1
      } else {
        // even void [1,1]: refund = (sharesA + sharesB) / 2
        const a = BigInt(await v.sharesOf(marketId, 0, holder));
        const b = BigInt(await v.sharesOf(marketId, 1, holder));
        payoutMicro = (a + b) / 2n;
      }
    } catch (e: any) {
      console.warn(`[v2/resolve] sharesOf failed for ${holder}: ${e?.message || e}`);
      continue;
    }
    if (payoutMicro <= 0n) continue;

    const amountDec = microToDec(payoutMicro);
    const row = {
      id: `v2claim-${marketId}-${holder}`,
      type: "CLAIM",
      side: "C",
      timestamp: Math.floor(Date.now() / 1000),
      txHash: null,
      grossInDec: "0",
      grossOutDec: amountDec,
      feeDec: "0",
      netStakeDec: "0",
      netOutDec: amountDec,
      costBasisClosedDec: "0",
      realizedPnlDec: "0",
      game: { id: gameId, league },
      __source: "v2-resolve",
    };
    await upsertUserTradesAndGames({ user: holder, tradeRows: [row] });
    claims++;
  }
  return { claims, holders: holders.length };
}
