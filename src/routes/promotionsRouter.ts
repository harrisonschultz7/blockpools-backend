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
      };
      const status = map[code] ?? 400;
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
      return res.status(status).json({ error: err.code, detail: err.detail });
    }
    console.error("[promotionsRouter/place-bet]", err);
    return res.status(500).json({ error: "Internal error" });
  }
});

export default router;
