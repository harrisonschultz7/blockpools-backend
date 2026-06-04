// src/routes/promotionsRouter.ts
//
// HTTP surface for the new promo framework. Mounted at /api/promotions in
// server.ts (after PROMO_FRAMEWORK_ENABLED gate). Distinct from the legacy
// /api/promo router so the two systems don't collide.

import { Router, Request, Response } from "express";

import { pool } from "../db";
import { PROMO_FRAMEWORK_ENABLED } from "../config/promo";
import { redeemPromoCode } from "../services/promotions/redeemPromoCode";
import {
  placeFreeBet,
  PlaceFreeBetException,
} from "../services/promotions/placeFreeBet";

const router = Router();

const ADDR_RE = /^0x[a-f0-9]{40}$/i;

function notReady(_req: Request, res: Response) {
  return res.status(503).json({ error: "Promo framework disabled" });
}

// Every endpoint is gated on the feature flag at request time so a flip in
// env can take effect on the next process restart without redeploying.
router.use((req, res, next) => {
  if (!PROMO_FRAMEWORK_ENABLED) return notReady(req, res);
  next();
});

// ── POST /api/promotions/redeem ──────────────────────────────────────────────
//
// Body: { code, userAddress, referrerAddress? }
//
// Returns { redemptionId, status, expiresAt, creditUsdc, promotionType }.
router.post("/redeem", async (req: Request, res: Response) => {
  const { code, userAddress, referrerAddress } = (req.body || {}) as {
    code?: string;
    userAddress?: string;
    referrerAddress?: string;
  };
  if (!code) return res.status(400).json({ error: "Missing code" });
  if (!userAddress || !ADDR_RE.test(userAddress)) {
    return res.status(400).json({ error: "Invalid userAddress" });
  }

  try {
    const result = await redeemPromoCode({
      code,
      userAddress,
      referrerAddress: referrerAddress || null,
    });
    // Audit trail — surface every successful redeem in the journal so we can
    // grep by code / address / redemptionId later. Cheap one-liner.
    console.log(
      `[promotionsRouter/redeem] ok code=${code} user=${userAddress} redemptionId=${(result as any)?.redemptionId} status=${(result as any)?.status}`
    );
    return res.status(200).json({ ok: true, ...result });
  } catch (err: any) {
    if (err?.code) {
      const code = String(err.code);
      const map: Record<string, number> = {
        PROMO_NOT_FOUND: 404,
        PROMO_INACTIVE: 400,
        PROMO_EXPIRED: 400,
        PROMO_NOT_STARTED: 400,
        PROMO_EXHAUSTED: 409,
        ALREADY_REDEEMED: 409,
        INVALID_ADDRESS: 400,
        NOT_NEW_USER: 403,
        REFERRER_REQUIRED: 400,
        ALREADY_HAS_REFERRAL_BONUS: 409,
      };
      const status = map[code] ?? 400;
      console.warn(`[promotionsRouter/redeem] fail code=${code} user=${userAddress}`);
      return res.status(status).json({ error: code });
    }
    console.error("[promotionsRouter/redeem]", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

// ── GET /api/promotions/active ───────────────────────────────────────────────
//
// Returns claimable campaigns: active=true, within window, not exhausted, and
// not legacy direct-credit type. Used by the frontend "promo wall".
router.get("/active", async (_req: Request, res: Response) => {
  try {
    const r = await pool.query(`
      SELECT
        p.id,
        p.code,
        p.type,
        p.name,
        p.description,
        p.credit_usdc,
        p.unlock_condition,
        p.unlock_min_trade_usdc,
        p.placement_window_hours,
        p.eligible_leagues,
        p.eligible_pool_addresses,
        p.min_odds_bps,
        p.max_odds_bps,
        p.max_claims_total,
        p.max_claims_per_user,
        p.total_claimed,
        p.is_repeatable,
        p.starts_at,
        p.expires_at
      FROM public.promotions p
      WHERE p.active = true
        AND (p.starts_at  IS NULL OR p.starts_at  <= now())
        AND (p.expires_at IS NULL OR p.expires_at >  now())
        AND p.type <> 'legacy_direct_credit'
      ORDER BY p.created_at DESC NULLS LAST
    `);

    const rows = r.rows.filter((p: any) => {
      if (p.max_claims_total == null) return true;
      return Number(p.total_claimed) < Number(p.max_claims_total);
    });

    return res.json({ promotions: rows });
  } catch (err) {
    console.error("[promotionsRouter/active]", err);
    return res.status(500).json({ error: "DB error" });
  }
});

// ── GET /api/promotions/me?address=0x... ─────────────────────────────────────
//
// Active and historical redemptions for a wallet, joined with promotion
// metadata. Used for combined balance display + free-bet position cards.
router.get("/me", async (req: Request, res: Response) => {
  const address = String(req.query.address || "").toLowerCase().trim();
  if (!ADDR_RE.test(address)) {
    return res.status(400).json({ error: "Invalid address" });
  }

  try {
    const r = await pool.query(
      `
      SELECT
        r.id,
        r.promotion_id,
        r.user_address,
        r.referrer_address,
        r.status,
        r.credit_usdc,
        r.pool_address,
        r.outcome_index,
        r.tx_hash,
        r.payout_tx_hash,
        r.payout_amount_usdc,
        r.treasury_recovered_usdc,
        r.claimed_at,
        r.qualified_at,
        r.placed_at,
        r.settled_at,
        r.expires_at,
        p.code,
        p.name           AS promotion_name,
        p.description    AS promotion_description,
        p.type           AS promotion_type,
        p.placement_window_hours,
        p.eligible_leagues,
        p.eligible_pool_addresses,
        p.min_odds_bps,
        p.max_odds_bps
      FROM public.promo_redemptions r
      JOIN public.promotions p ON p.id = r.promotion_id
      WHERE lower(r.user_address) = $1
      ORDER BY r.claimed_at DESC NULLS LAST
      `,
      [address]
    );
    return res.json({ redemptions: r.rows });
  } catch (err) {
    console.error("[promotionsRouter/me]", err);
    return res.status(500).json({ error: "DB error" });
  }
});

// ── GET /api/promotions/me/progress?address=0x... ────────────────────────────
//
// Compact progress report for ONE wallet against their current
// pending_qualification redemption (typically the bet-to-unlock signup
// bonus). Used by the profile page to render an inline progress bar.
//
// Response shape:
//   {
//     hasPending: boolean,
//     redemption?: {
//       id, code, promotionName, creditUsdc, claimedAt, expiresAt,
//       unlockMinTradeUsdc, accumulatedUsdc, remainingUsdc, percent
//     }
//   }
//
// "Accumulated" is computed using the SAME aggregation as
// evaluatePromoEligibility (sum BUY net_stake − sum SELL cost_basis_closed,
// per game+outcome, ONLY where the game is_final=true and the trade is not
// a free-bet trade). We take the MAX held-net-stake across all settled
// (game,outcome) buckets — i.e. the user's best single settled position
// that could unlock the promo. This matches what the qualifier looks for.
router.get("/me/progress", async (req: Request, res: Response) => {
  const address = String(req.query.address || "").toLowerCase().trim();
  if (!ADDR_RE.test(address)) {
    return res.status(400).json({ error: "Invalid address" });
  }

  try {
    // Find the most recent pending_qualification redemption for the user.
    const redQ = await pool.query(
      `
      SELECT
        r.id,
        r.claimed_at,
        r.expires_at,
        r.credit_usdc,
        p.code,
        p.name                  AS promotion_name,
        p.unlock_condition,
        p.unlock_min_trade_usdc
      FROM public.promo_redemptions r
      JOIN public.promotions p ON p.id = r.promotion_id
      WHERE lower(r.user_address) = $1
        AND r.status = 'pending_qualification'
      ORDER BY r.claimed_at DESC NULLS LAST
      LIMIT 1
      `,
      [address]
    );

    if (!redQ.rows.length) {
      return res.json({ hasPending: false });
    }
    const red = redQ.rows[0];
    const minTrade = Number(red.unlock_min_trade_usdc ?? 0);

    // Compute held net stake per (game, outcome) twice:
    //   - SETTLED bucket (g.is_final = true)   → counts toward unlock
    //   - PENDING bucket (g.is_final = false)  → in-flight; shown as striped
    //
    // We pick the MAX held position from each bucket. Mirrors the exact
    // qualification rule used by evaluatePromoEligibility for the settled
    // bucket; the pending bucket is purely UI signal so users know their
    // in-flight bet is being tracked.
    const progQ = await pool.query(
      `
      WITH per_outcome AS (
        SELECT
          e.game_id,
          e.outcome_index,
          g.is_final,
          SUM(CASE WHEN e.type = 'BUY'
                   THEN COALESCE(e.net_stake_dec, 0)
                   ELSE 0 END)::numeric AS bought,
          SUM(CASE WHEN e.type = 'SELL'
                   THEN COALESCE(e.cost_basis_closed_dec, 0)
                   ELSE 0 END)::numeric AS sold
        FROM public.user_trade_events e
        JOIN public.games g ON lower(g.game_id) = lower(e.game_id)
        WHERE lower(e.user_address) = $1
          AND e.beneficiary_address IS NULL
          AND e.inserted_at >= $2
        GROUP BY e.game_id, e.outcome_index, g.is_final
      )
      SELECT
        COALESCE(MAX(CASE WHEN is_final = true  THEN bought - sold END), 0)::numeric AS best_settled,
        COALESCE(MAX(CASE WHEN is_final = false THEN bought - sold END), 0)::numeric AS best_pending
      FROM per_outcome
      `,
      [address, red.claimed_at]
    );
    const accumulated = Math.max(0, Number(progQ.rows[0]?.best_settled ?? 0));
    const pending = Math.max(0, Number(progQ.rows[0]?.best_pending ?? 0));
    const remaining = Math.max(0, minTrade - accumulated);
    const percent =
      minTrade > 0 ? Math.min(100, Math.round((accumulated / minTrade) * 100)) : 0;
    // Pending fill is shown ON TOP of the settled fill, but never exceeds the
    // remaining unfilled portion of the bar — so settled + pending stripes
    // visually max out at 100%.
    const pendingPercent =
      minTrade > 0
        ? Math.max(
            0,
            Math.min(100 - percent, Math.round((pending / minTrade) * 100))
          )
        : 0;

    return res.json({
      hasPending: true,
      redemption: {
        id: red.id,
        code: red.code,
        promotionName: red.promotion_name,
        creditUsdc: Number(red.credit_usdc),
        claimedAt: red.claimed_at,
        expiresAt: red.expires_at,
        unlockMinTradeUsdc: minTrade,
        accumulatedUsdc: accumulated,
        pendingUsdc: pending,
        remainingUsdc: remaining,
        percent,
        pendingPercent,
      },
    });
  } catch (err) {
    console.error("[promotionsRouter/me/progress]", err);
    return res.status(500).json({ error: "DB error" });
  }
});

// ── GET /api/promotions/me/activity?address=0x... ────────────────────────────
//
// Focused free-bet activity history (claim → place → settle) for a wallet.
router.get("/me/activity", async (req: Request, res: Response) => {
  const address = String(req.query.address || "").toLowerCase().trim();
  if (!ADDR_RE.test(address)) {
    return res.status(400).json({ error: "Invalid address" });
  }

  try {
    const r = await pool.query(
      `
      SELECT
        e.id,
        e.redemption_id,
        e.event_type,
        e.event_data,
        e.created_at,
        r.user_address,
        p.code,
        p.name AS promotion_name,
        p.type AS promotion_type
      FROM public.promo_eligibility_events e
      JOIN public.promo_redemptions r ON r.id = e.redemption_id
      JOIN public.promotions p ON p.id = r.promotion_id
      WHERE lower(r.user_address) = $1
      ORDER BY e.created_at DESC
      LIMIT 200
      `,
      [address]
    );
    return res.json({ events: r.rows });
  } catch (err) {
    console.error("[promotionsRouter/me/activity]", err);
    return res.status(500).json({ error: "DB error" });
  }
});

// ── GET /api/promotions/positions?address=0x... ──────────────────────────────
//
// Open promo positions for the right-rail betslip. A position is "open" while
// the redemption is in 'placed' state — i.e. funding wallet has bought shares
// but the game hasn't settled yet. After settlement, the synthetic CLAIM row
// in user_trade_events represents the same position in regular trade history,
// so we drop it from this list.
//
// The funding wallet holds the on-chain shares, so the user's normal
// betsCache (which reads on-chain holdings for the connected address) doesn't
// see these positions. This endpoint gives the frontend everything it needs
// to render a read-only promo card.
router.get("/positions", async (req: Request, res: Response) => {
  const address = String(req.query.address || "").toLowerCase().trim();
  if (!ADDR_RE.test(address)) {
    return res.status(400).json({ error: "Invalid address" });
  }

  try {
    const r = await pool.query(
      `
      SELECT
        r.id                AS redemption_id,
        r.status            AS redemption_status,
        r.credit_usdc,
        r.pool_address,
        r.outcome_index     AS redemption_outcome_index,
        r.placed_at,
        r.expires_at,
        e.id                AS trade_id,
        e.tx_hash,
        e.timestamp         AS trade_timestamp,
        e.gross_in_dec,
        e.net_stake_dec,
        e.avg_price_bps,
        e.spot_price_bps,
        e.outcome_index     AS trade_outcome_index,
        e.outcome_code,
        g.game_id,
        g.league,
        g.team_a_name,
        g.team_b_name,
        g.team_a_code,
        g.team_b_code,
        g.market_type,
        g.lock_time,
        g.is_final,
        g.winning_outcome_index,
        g.market_question,
        g.market_short
      FROM public.promo_redemptions r
      JOIN public.user_trade_events e
        ON e.promo_redemption_id = r.id
       AND e.type = 'BUY'
      LEFT JOIN public.games g
        ON lower(g.game_id) = lower(r.pool_address)
      WHERE lower(r.user_address) = $1
        AND r.status = 'placed'
      ORDER BY e.timestamp DESC
      `,
      [address]
    );
    return res.json({ positions: r.rows });
  } catch (err) {
    console.error("[promotionsRouter/positions]", err);
    return res.status(500).json({ error: "DB error" });
  }
});

// ── POST /api/promotions/place-bet ───────────────────────────────────────────
//
// Body: { redemptionId, poolAddress, outcomeIndex, userAddress }
//
// Funding wallet places the bet on-chain and DB transitions to 'placed'.
router.post("/place-bet", async (req: Request, res: Response) => {
  const { redemptionId, poolAddress, outcomeIndex, userAddress } = (req.body ||
    {}) as {
    redemptionId?: string;
    poolAddress?: string;
    outcomeIndex?: number | string;
    userAddress?: string;
  };

  if (!redemptionId) return res.status(400).json({ error: "Missing redemptionId" });
  if (!poolAddress || !ADDR_RE.test(poolAddress)) {
    return res.status(400).json({ error: "Invalid poolAddress" });
  }
  if (!userAddress || !ADDR_RE.test(userAddress)) {
    return res.status(400).json({ error: "Invalid userAddress" });
  }
  const oi = Number(outcomeIndex);
  if (!Number.isFinite(oi) || oi < 0 || oi > 255) {
    return res.status(400).json({ error: "Invalid outcomeIndex" });
  }

  try {
    const result = await placeFreeBet({
      redemptionId,
      poolAddress,
      outcomeIndex: Math.trunc(oi),
      userAddress,
    });
    // Audit trail for successful placements — useful for tracing why a
    // bonus trade did/didn't reach the chain when a user reports issues.
    console.log(
      `[promotionsRouter/place-bet] ok redemptionId=${redemptionId} user=${userAddress} pool=${poolAddress} outcome=${oi} txHash=${(result as any)?.txHash}`
    );
    return res.status(200).json({ ok: true, ...result });
  } catch (err: any) {
    if (err instanceof PlaceFreeBetException) {
      const map: Record<string, number> = {
        REDEMPTION_NOT_FOUND: 404,
        REDEMPTION_NOT_ELIGIBLE: 409,
        REDEMPTION_EXPIRED: 410,
        ADDRESS_MISMATCH: 403,
        POOL_INELIGIBLE: 400,
        POOL_LOCKED_OR_FINAL: 400,
        PRICE_OUT_OF_BAND: 400,
        INSUFFICIENT_FUNDING_BALANCE: 503,
        ON_CHAIN_TX_FAILED: 502,
      };
      const status = map[err.code] ?? 400;
      console.warn(
        `[promotionsRouter/place-bet] fail code=${err.code} redemptionId=${redemptionId} user=${userAddress} pool=${poolAddress} outcome=${oi}`
      );
      return res.status(status).json({ error: err.code, detail: err.detail });
    }
    console.error("[promotionsRouter/place-bet]", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

export default router;
