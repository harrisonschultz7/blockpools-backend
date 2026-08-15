// src/services/promotions/placeFreeBet.ts
//
// Funding wallet places a single free bet on behalf of a redemption-holding
// user. The user never owns the shares — the funding wallet does. That's what
// makes the bet structurally non-sellable and non-withdrawable for the user
// pre-settlement.
//
// Idempotent on retry: if a bet_funded ledger entry already exists for this
// redemption, we return the redemption's current state instead of placing
// again.

import { Contract } from "@ethersproject/contracts";
import { BigNumber } from "@ethersproject/bignumber";
import { parseUnits, formatUnits } from "@ethersproject/units";

import { pool } from "../../db";
import {
  USDC_DECIMALS,
  PROMO_TX_CONFIRMATIONS,
  PROMO_BUY_GAS_LIMIT,
} from "../../config/promo";
import { getFundingWallet } from "./findFundingWallet";
import { hasBetFundedEntry, writeLedgerEntry } from "./promotionFunding";
import { triggerFundingWalletAttributionRefresh } from "./handlePromoTradeAttribution";
import { upsertUserTradesAndGames } from "../persistTrades";
import { resolveV2Market, V2MarketEntry } from "../v2/v2MarketResolver";
import {
  quoteV2MarketBuy,
  ensureExchangeApproval,
  placeV2Order,
  v2MarketState,
} from "./v2FreeBet";

// Minimal pool ABI covering both pool variants in this codebase:
//   - Multi-outcome (gamePoolMulti): buy(uint8 outcome, uint256, uint256)
//   - Binary (gamePool):              buyTeamA(uint256, uint256) / buyTeamB(...)
// We branch on games.market_type to pick the right method.
const POOL_ABI = [
  // multi
  "function buy(uint8 outcome, uint256 grossAmount, uint256 minSharesOut)",
  "function currentPriceBps(uint8 outcome) view returns (uint256)",
  "function isResolved() view returns (bool)",
  // binary
  "function buyTeamA(uint256 grossAmount, uint256 minSharesOut)",
  "function buyTeamB(uint256 grossAmount, uint256 minSharesOut)",
  // shared
  "function isLocked() view returns (bool)",
];

const ERC20_APPROVE_ABI = [
  "function allowance(address owner, address spender) view returns (uint256)",
  "function approve(address spender, uint256 amount) returns (bool)",
];

export type PlaceFreeBetError =
  | "REDEMPTION_NOT_FOUND"
  | "REDEMPTION_NOT_ELIGIBLE"
  | "REDEMPTION_EXPIRED"
  | "ADDRESS_MISMATCH"
  | "POOL_INELIGIBLE"
  | "POOL_LOCKED_OR_FINAL"
  | "PRICE_OUT_OF_BAND"
  | "FUNDING_WALLET_MISMATCH"
  | "INSUFFICIENT_FUNDING_BALANCE"
  | "INSUFFICIENT_BOOK_LIQUIDITY"
  | "ON_CHAIN_TX_FAILED";

export class PlaceFreeBetException extends Error {
  code: PlaceFreeBetError;
  detail?: any;
  constructor(code: PlaceFreeBetError, detail?: any) {
    super(code);
    this.code = code;
    this.detail = detail;
  }
}

export type PlaceFreeBetInput = {
  redemptionId: string;
  poolAddress: string;
  outcomeIndex: number;
  userAddress: string;
  // When present AND it resolves to a known v2 market, the bet is placed on the
  // order book (funding wallet signs an EIP-712 order to the matcher) instead of
  // an AMM pool. `poolAddress` is then ignored for on-chain purposes — the
  // canonical game id is derived from the marketId via v2MarketResolver.
  marketId?: string;
};

export type PlaceFreeBetResult = {
  redemptionId: string;
  txHash: string;
  poolAddress: string;
  outcomeIndex: number;
  creditUsdc: string;
  status: "placed";
  alreadyPlaced?: boolean;
};

export async function placeFreeBet(
  input: PlaceFreeBetInput
): Promise<PlaceFreeBetResult> {
  const redemptionId = String(input.redemptionId);
  const poolAddress = String(input.poolAddress).toLowerCase();
  const outcomeIndex = Number(input.outcomeIndex);
  const userAddress = String(input.userAddress).toLowerCase();

  // v2 detection: if a marketId was supplied and it maps to a known v2 market,
  // this is an order-book bet. `gameKey` is the identifier we store in
  // promo_redemptions.pool_address AND look up in public.games — for v2 that's
  // the game id (e.g. "MLB-…"), the SAME key recordV2Resolution writes
  // is_final/winning_outcome_index under, so settlement lines up. For AMM it's
  // the pool contract address, unchanged.
  const marketId = input.marketId ? String(input.marketId) : "";
  const v2Entry: V2MarketEntry | null = marketId ? resolveV2Market(marketId) : null;
  // Preserve the CANONICAL case of a v2 gameId (v2 game_ids are UPPERCASE, e.g.
  // "NFL-GB-PIT-…"). Lowercasing it made the direct-write create a DUPLICATE
  // lowercase games row (metadata-less) beside the canonical one — ON CONFLICT
  // (game_id) is case-sensitive — which fanned the betslip out into two cards.
  // AMM pool addresses stay lowercased (case-insensitive hex).
  const gameKey = v2Entry ? String(v2Entry.gameId) : poolAddress.toLowerCase();

  // Idempotency short-circuit: if the ledger already has a bet_funded row for
  // this redemption, this is a retry — don't re-place.
  if (await hasBetFundedEntry(redemptionId)) {
    const r = await pool.query(
      `SELECT pool_address, outcome_index, tx_hash, credit_usdc
         FROM public.promo_redemptions WHERE id = $1`,
      [redemptionId]
    );
    const row = r.rows[0];
    if (row?.tx_hash) {
      return {
        redemptionId,
        txHash: row.tx_hash,
        poolAddress: String(row.pool_address || "").toLowerCase(),
        outcomeIndex: Number(row.outcome_index ?? outcomeIndex),
        creditUsdc: String(row.credit_usdc),
        status: "placed",
        alreadyPlaced: true,
      };
    }
  }

  // Load redemption + promotion guardrails.
  const q = await pool.query(
    `
    SELECT
      r.id,
      r.user_address,
      r.status,
      r.expires_at,
      r.credit_usdc,
      p.id                           AS promotion_id,
      p.eligible_leagues,
      p.eligible_pool_addresses,
      p.min_odds_bps,
      p.max_odds_bps,
      p.credit_usdc                  AS promo_credit_usdc,
      p.funding_wallet_address       AS promo_funding_wallet
    FROM public.promo_redemptions r
    JOIN public.promotions p ON p.id = r.promotion_id
    WHERE r.id = $1
    `,
    [redemptionId]
  );
  const red = q.rows[0];
  if (!red) throw new PlaceFreeBetException("REDEMPTION_NOT_FOUND");
  if (red.status !== "eligible") {
    throw new PlaceFreeBetException("REDEMPTION_NOT_ELIGIBLE", { current: red.status });
  }
  if (red.expires_at && new Date(red.expires_at).getTime() < Date.now()) {
    throw new PlaceFreeBetException("REDEMPTION_EXPIRED");
  }
  if (String(red.user_address).toLowerCase() !== userAddress) {
    throw new PlaceFreeBetException("ADDRESS_MISMATCH");
  }

  // Pool must be in the promo's allow-list (either by league or by explicit
  // pool address). If both eligibility lists are null the campaign accepts
  // any pool — we still require the pool to exist in `games`.
  const gameQ = await pool.query(
    `SELECT game_id, league, is_final, lock_time, market_type
       FROM public.games WHERE lower(game_id) = lower($1)`,
    [gameKey]
  );
  let game = gameQ.rows[0];
  if (!game) {
    if (v2Entry) {
      // v2 markets share one Exchange/Vault and don't get a public.games row
      // until the first trade or resolution is persisted — so a free bet can be
      // the FIRST activity on a market. Synthesize the game context from the
      // resolver; lock/resolution is gated on-chain (vault.markets) in the v2
      // branch below, not from this (absent) row. The direct-write later creates
      // the row via upsertUserTradesAndGames.
      game = {
        game_id: gameKey,
        league: v2Entry.league,
        is_final: false,
        lock_time: null,
        market_type: v2Entry.marketType,
      };
    } else {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", { reason: "game_not_found" });
    }
  }
  const marketType = String(game.market_type || "").toUpperCase();

  const eligibleLeagues: string[] | null = red.eligible_leagues;
  const eligiblePools: string[] | null = red.eligible_pool_addresses;
  if (eligibleLeagues && eligibleLeagues.length) {
    const leagueOk = eligibleLeagues
      .map((s) => String(s).toUpperCase())
      .includes(String(game.league || "").toUpperCase());
    if (!leagueOk) {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", { reason: "league_not_allowed" });
    }
  } else if (eligiblePools && eligiblePools.length) {
    const poolOk = eligiblePools
      .map((s) => String(s).toLowerCase())
      .includes(gameKey.toLowerCase());
    if (!poolOk) {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", { reason: "pool_not_allowed" });
    }
  }

  if (game.is_final) {
    throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", { reason: "is_final" });
  }
  // AMM pools lock at kickoff. v2 order-book markets trade live through the game,
  // so we DON'T block on lock_time for v2 — the authoritative gate is the vault's
  // on-chain `resolved` flag, checked in the v2 placement branch below.
  if (!v2Entry && game.lock_time != null) {
    const lockMs = Number(game.lock_time) * 1000;
    if (lockMs <= Date.now()) {
      throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", { reason: "lock_time_passed" });
    }
  }

  // Wire up funding wallet + on-chain reads.
  const { wallet, usdc } = getFundingWallet();

  // Defense in depth: refuse to place if the env-configured wallet doesn't
  // match the campaign's funding_wallet_address. Catches cross-environment
  // misconfig (test wallet env in prod, etc.).
  const promoFundingWallet = String(red.promo_funding_wallet || "").toLowerCase();
  if (promoFundingWallet && promoFundingWallet !== wallet.address.toLowerCase()) {
    throw new PlaceFreeBetException("FUNDING_WALLET_MISMATCH", {
      promoFundingWallet,
      configured: wallet.address.toLowerCase(),
    });
  }

  const creditUsdc = String(red.credit_usdc ?? red.promo_credit_usdc);
  const grossAmount = parseUnits(creditUsdc, USDC_DECIMALS);

  // Pre-check funding wallet balance so we fail loudly before sending any tx.
  const balance: BigNumber = await usdc.balanceOf(wallet.address);
  if (balance.lt(grossAmount)) {
    throw new PlaceFreeBetException("INSUFFICIENT_FUNDING_BALANCE", {
      have: formatUnits(balance, USDC_DECIMALS),
      need: creditUsdc,
    });
  }

  // Odds guardrail (bps) — enforced identically for AMM and v2. min/max may be
  // null (that side unbounded).
  const hasOddsBand = red.min_odds_bps != null || red.max_odds_bps != null;
  const enforceOddsBand = (bps: number) => {
    if (red.min_odds_bps != null && bps < Number(red.min_odds_bps)) {
      throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", { priceBps: bps, min: red.min_odds_bps });
    }
    if (red.max_odds_bps != null && bps > Number(red.max_odds_bps)) {
      throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", { priceBps: bps, max: red.max_odds_bps });
    }
  };

  let txHash: string;
  let priceBps: number | null = null;
  // v2 placement metadata, persisted on the 'placed' event so settleFreeBet can
  // redeem/settle from the exact per-redemption share count without re-reading
  // the book. Null for AMM bets.
  let v2Meta:
    | { marketId: string; shares: string; spentMicro: string; avgPriceBps: number }
    | null = null;

  if (v2Entry) {
    // ── v2 (order-book) placement ──────────────────────────────────────────
    // Fund the bet by signing a crossing BUY as the funding wallet and posting
    // it to the matcher; the funding wallet ends up holding the outcome shares
    // (same custody as the AMM path — the user never owns the stake).
    if (outcomeIndex !== 0 && outcomeIndex !== 1) {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", {
        reason: "v2_market_is_binary_outcome_0_or_1",
        outcomeIndex,
      });
    }

    // On-chain gate — the authoritative lock/resolution check for v2 (there's no
    // per-game DB row). Market must exist on the vault and not be resolved.
    const mkt = await v2MarketState(marketId);
    if (!mkt.exists) {
      throw new PlaceFreeBetException("POOL_INELIGIBLE", {
        reason: "v2_market_not_on_chain",
        marketId,
      });
    }
    if (mkt.resolved) {
      throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", { reason: "v2_market_resolved" });
    }

    await ensureExchangeApproval(grossAmount);

    // Quote the market buy; require enough resting liquidity to fill the whole
    // credit. If the book is thin (e.g. the seed bot isn't quoting this market
    // yet) refuse and leave the redemption 'eligible' for a clean retry rather
    // than resting a partially-fillable promo order.
    const quote = await quoteV2MarketBuy(marketId, outcomeIndex, grossAmount.toString());
    if (quote.insufficient || BigNumber.from(quote.shares).lte(0)) {
      throw new PlaceFreeBetException("INSUFFICIENT_BOOK_LIQUIDITY", {
        need: creditUsdc,
        availableMicro: quote.availableAmount,
      });
    }

    // avgPrice is 1e6-scaled ($1 == 1e6 == 10000 bps) → bps = avgPrice / 100.
    const avgBps = Math.round(Number(quote.avgPrice) / 100);
    if (hasOddsBand) enforceOddsBand(avgBps);
    priceBps = avgBps;

    let place;
    try {
      place = await placeV2Order({
        marketId,
        outcome: outcomeIndex,
        shares: quote.shares,
        worstPriceScaled: quote.worstPrice,
      });
    } catch (err: any) {
      throw new PlaceFreeBetException("ON_CHAIN_TX_FAILED", {
        detail: err?.message ?? String(err),
      });
    }
    if (!place.orderHash || BigNumber.from(place.filledShares).lte(0)) {
      // Nothing crossed (the book moved between quote and submit) — the whole
      // order was cancelled, no funds moved, safe to retry.
      throw new PlaceFreeBetException("INSUFFICIENT_BOOK_LIQUIDITY", {
        reason: "order_did_not_cross",
        need: creditUsdc,
      });
    }
    txHash = place.orderHash; // matcher order hash doubles as the tx identifier
    // Actual USDC spent ≈ filledShares × avgPrice (both 1e6-scaled).
    const spentMicro = BigNumber.from(place.filledShares)
      .mul(BigNumber.from(quote.avgPrice))
      .div(BigNumber.from(1_000_000));
    v2Meta = {
      marketId,
      shares: place.filledShares,
      spentMicro: spentMicro.toString(),
      avgPriceBps: avgBps,
    };
  } else {
    // ── AMM (game pool) placement — unchanged ──────────────────────────────
    const poolContract = new Contract(poolAddress, POOL_ABI, wallet);

    // Defense in depth: also confirm the contract itself isn't locked/resolved.
    try {
      const [locked, resolved] = await Promise.all([
        poolContract.isLocked(),
        poolContract.isResolved(),
      ]);
      if (locked || resolved) {
        throw new PlaceFreeBetException("POOL_LOCKED_OR_FINAL", {
          reason: "contract_state",
          locked,
          resolved,
        });
      }
    } catch (err: any) {
      if (err instanceof PlaceFreeBetException) throw err;
      // Best-effort — old pools may not implement these getters.
      console.warn("[placeFreeBet] pool state read failed; relying on DB gate", err?.message);
    }

    // Binary pools don't expose currentPriceBps — skip the read entirely if
    // we're on a binary pool, or if no bounds are set on the campaign.
    const supportsPriceRead = marketType !== "BINARY";
    if (supportsPriceRead) {
      try {
        const raw = await poolContract.currentPriceBps(outcomeIndex);
        priceBps = Number(raw.toString());
      } catch (err: any) {
        if (hasOddsBand) {
          throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", {
            reason: "price_read_failed",
            detail: err?.message,
          });
        }
        console.warn(
          "[placeFreeBet] currentPriceBps unavailable; skipping band check (no bounds on campaign)",
          err?.message
        );
      }
    } else if (hasOddsBand) {
      // Binary pool with odds band on the campaign — we can't enforce it.
      // Refuse rather than silently bypass.
      throw new PlaceFreeBetException("PRICE_OUT_OF_BAND", {
        reason: "binary_pool_does_not_support_odds_band",
        marketType,
      });
    }

    if (priceBps != null) enforceOddsBand(priceBps);

    // Ensure USDC allowance for the pool. Approve generously (max uint256) once
    // per pool to avoid one-extra-tx-per-bet overhead. If allowance already
    // covers grossAmount, skip.
    const usdcWithApprove = new Contract(usdc.address, ERC20_APPROVE_ABI, wallet);
    const currentAllowance: BigNumber = await usdcWithApprove.allowance(
      wallet.address,
      poolAddress
    );
    if (currentAllowance.lt(grossAmount)) {
      const MAX_UINT256 = BigNumber.from(
        "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
      );
      const approveTx = await usdcWithApprove.approve(poolAddress, MAX_UINT256);
      await approveTx.wait(PROMO_TX_CONFIRMATIONS);
    }

    // Place the buy. minSharesOut = 0 for now — we already gated on price band
    // (when applicable) above.
    //   - BINARY  → buyTeamA(amount, minSharesOut) for outcome 0,
    //               buyTeamB(amount, minSharesOut) for outcome 1.
    //   - else    → buy(uint8 outcome, amount, minSharesOut) (multi).
    try {
      let tx;
      if (marketType === "BINARY") {
        if (outcomeIndex === 0) {
          tx = await poolContract.buyTeamA(grossAmount, 0, {
            gasLimit: PROMO_BUY_GAS_LIMIT,
          });
        } else if (outcomeIndex === 1) {
          tx = await poolContract.buyTeamB(grossAmount, 0, {
            gasLimit: PROMO_BUY_GAS_LIMIT,
          });
        } else {
          throw new PlaceFreeBetException("POOL_INELIGIBLE", {
            reason: "binary_pool_only_supports_outcome_0_or_1",
            outcomeIndex,
          });
        }
      } else {
        tx = await poolContract.buy(outcomeIndex, grossAmount, 0, {
          gasLimit: PROMO_BUY_GAS_LIMIT,
        });
      }
      const receipt = await tx.wait(PROMO_TX_CONFIRMATIONS);
      if (!receipt || receipt.status !== 1) {
        throw new PlaceFreeBetException("ON_CHAIN_TX_FAILED", { txHash: tx.hash });
      }
      txHash = tx.hash;
    } catch (err: any) {
      if (err instanceof PlaceFreeBetException) throw err;
      throw new PlaceFreeBetException("ON_CHAIN_TX_FAILED", {
        detail: err?.message ?? String(err),
      });
    }
  }

  // Persist DB transitions in one transaction so a redemption can't be marked
  // 'placed' without the matching ledger entry.
  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    await client.query(
      `
      UPDATE public.promo_redemptions
         SET status        = 'placed',
             pool_address  = $1,
             outcome_index = $2,
             placed_at     = now(),
             tx_hash       = $3
       WHERE id = $4
         AND status = 'eligible'
      `,
      [gameKey, outcomeIndex, txHash, redemptionId]
    );

    await client.query(
      `INSERT INTO public.promo_eligibility_events
         (redemption_id, event_type, event_data)
       VALUES ($1, 'placed', $2::jsonb)`,
      [
        redemptionId,
        JSON.stringify({
          poolAddress: gameKey,
          outcomeIndex,
          txHash,
          priceBps,
          creditUsdc,
          // v2 order-book placement facts — read back by settleFreeBet to redeem
          // the exact per-redemption share count at the vault (absent for AMM).
          ...(v2Meta
            ? {
                v2: true,
                marketId: v2Meta.marketId,
                shares: v2Meta.shares, // 6dp share units held by the funding wallet
                spentMicro: v2Meta.spentMicro,
              }
            : {}),
        }),
      ]
    );

    await writeLedgerEntry(
      {
        promotionId: red.promotion_id,
        redemptionId,
        direction: "bet_funded",
        amountUsdc: creditUsdc,
        txHash,
      },
      client
    );

    await client.query("COMMIT");
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch {}
    // The on-chain bet succeeded but the DB write failed. Reconciliation
    // (reconcilePromoFunding) will surface this. Don't throw a 5xx that hides
    // the tx hash from the caller — log loudly and rethrow.
    console.error(
      "[placeFreeBet] DB persist failed AFTER successful on-chain buy",
      { redemptionId, txHash, err }
    );
    throw err;
  } finally {
    client.release();
  }

  // Direct-write the BUY row to user_trade_events NOW, independent of the
  // subgraph, so the promo bet shows in stats immediately even while the
  // subgraph lags or is stalled. Without this the row only ever arrived via the
  // subgraph pull below — which silently produces nothing whenever the subgraph
  // is behind. Mirrors the frontend recordBuyDirect path for user buys.
  //
  // - user = funding wallet (it placed the buy on-chain), so refreshUserTradesPage's
  //   `buy-direct-%` reconcile collapses this row by txHash once the subgraph
  //   delivers the authoritative funding-wallet trade — no double-count.
  // - persistTrades' promo pre-insert hook (applyBeneficiaryToFundingWalletTrade)
  //   finds the now-'placed' redemption by (pool, txHash) and stamps
  //   beneficiary_address + promo_redemption_id, so the trade attributes to the
  //   user via effective_user_address — no extra wiring needed here.
  // Amounts are the credit (gross≈net for a free bet); the subgraph reconcile
  // later replaces this with exact fee/shares values. Never blocks the response.
  // For v2 the "stake" is the actual USDC that crossed (gross≈net); for AMM it's
  // the credit. net_stake_dec/avg_price_bps here also serve as settleFreeBet's
  // fallback per-redemption share source if the placement event is ever missing.
  const stakeDec = v2Meta ? formatUnits(v2Meta.spentMicro, USDC_DECIMALS) : creditUsdc;
  try {
    await upsertUserTradesAndGames({
      user: wallet.address.toLowerCase(),
      tradeRows: [
        {
          id: `buy-direct-${txHash.toLowerCase()}`,
          type: "BUY",
          side: null,
          outcomeIndex,
          outcomeCode: null,
          timestamp: Math.floor(Date.now() / 1000),
          txHash,
          spotPriceBps: priceBps,
          avgPriceBps: priceBps,
          grossInDec: stakeDec,
          grossOutDec: "0",
          feeDec: "0",
          netStakeDec: stakeDec,
          netOutDec: "0",
          costBasisClosedDec: "0",
          realizedPnlDec: "0",
          game: { id: gameKey, league: game.league ?? null },
          __source: "promo-direct",
        },
      ],
    });
  } catch (err) {
    console.error(
      "[placeFreeBet] direct-write of promo BUY failed (non-blocking)",
      { redemptionId, txHash, err }
    );
  }

  // Fire-and-forget: pull the funding wallet's freshly-confirmed BUY from the
  // subgraph and persist it. Stamps beneficiary_address + promo_redemption_id
  // on the trade row via persistTrades' pre-insert hook so the bet lands in
  // user-facing stats immediately. Retries with backoff to absorb subgraph
  // indexing lag. Never blocks placeFreeBet's response — the tx hash is
  // already returned to the caller.
  //
  // v2 has NO subgraph (fills are matcher-persisted), so skip it there — the
  // direct-write above is the authoritative stats row for a v2 free bet.
  if (!v2Entry) {
    triggerFundingWalletAttributionRefresh(txHash, redemptionId).catch((err) => {
      console.error(
        "[placeFreeBet] background attribution refresh threw (non-blocking)",
        { redemptionId, txHash, err }
      );
    });
  }

  return {
    redemptionId,
    txHash,
    poolAddress: gameKey,
    outcomeIndex,
    creditUsdc,
    status: "placed",
  };
}
