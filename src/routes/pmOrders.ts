// src/routes/pmOrders.ts
//
// The signed-in user's resting Polymarket limit orders placed through BlockPools.
//
//   GET  /api/pm/orders              -> { orders: [...] }   live orders
//   POST /api/pm/orders              -> record one          { id, marketId, outcomeIndex, side, price, shares }
//   POST /api/pm/orders/:id/close    -> mark closed         { reason }
//
// WHY THIS EXISTS. Polymarket will not report these orders back to us:
// listOpenOrders returns nothing for Deposit-Wallet orders and fetchOrder's
// response fails the SDK schema. The app therefore records each limit order at
// placement; this table makes that record the same on phone and desktop (it was
// one browser's localStorage). Keyed by the Privy user id, identical across
// devices. The client still checks each order with the venue when it can and
// closes it here once it fills or is cancelled.

import { Router, Response } from "express";
import { pool } from "../db";
import { authPrivyOptionalWallet, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

/** Safety net: an order nobody ever resolved stops showing after this long. */
const MAX_AGE_DAYS = 21;
const CLOSE_REASONS = new Set(["cancelled", "filled", "edited", "expired", "venue_closed"]);

const isOrderId = (x: unknown) => /^0x[0-9a-fA-F]{8,128}$/.test(String(x || ""));
const isCondition = (x: unknown) => /^0x[0-9a-fA-F]{64}$/.test(String(x || ""));

function toRow(r: any) {
  return {
    id: r.id,
    marketId: r.market_id,
    outcomeIndex: Number(r.outcome_index),
    side: r.side,
    price: Number(r.price),
    shares: Number(r.shares),
    createdAt: r.created_at,
  };
}

router.get("/", authPrivyOptionalWallet, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
    const { rows } = await pool.query(
      `SELECT id, market_id, outcome_index, side, price, shares, created_at
         FROM pm_limit_orders
        WHERE user_id = $1 AND status = 'live'
          AND created_at > now() - ($2 || ' days')::interval
        ORDER BY created_at DESC
        LIMIT 100`,
      [req.user.id, String(MAX_AGE_DAYS)],
    );
    res.set("Cache-Control", "no-store");
    return res.json({ orders: rows.map(toRow) });
  } catch (e) {
    console.error("[GET /api/pm/orders] error", e);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/", authPrivyOptionalWallet, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
    const b = req.body || {};
    const id = String(b.id || "");
    const marketId = String(b.marketId || "").toLowerCase();
    const outcomeIndex = Number(b.outcomeIndex);
    const side = String(b.side || "").toLowerCase();
    const price = Number(b.price);
    const shares = Number(b.shares);
    if (!isOrderId(id)) return res.status(400).json({ error: "invalid id" });
    if (!isCondition(marketId)) return res.status(400).json({ error: "invalid marketId" });
    if (outcomeIndex !== 0 && outcomeIndex !== 1) return res.status(400).json({ error: "invalid outcomeIndex" });
    if (side !== "buy" && side !== "sell") return res.status(400).json({ error: "invalid side" });
    if (!(price > 0 && price < 1)) return res.status(400).json({ error: "invalid price" });
    if (!(shares > 0)) return res.status(400).json({ error: "invalid shares" });

    // Upsert, but never across users: an id already owned by someone else is
    // left untouched (the WHERE on the conflict update), and reported.
    const { rowCount } = await pool.query(
      `INSERT INTO pm_limit_orders (id, user_id, owner_address, market_id, outcome_index, side, price, shares)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
       ON CONFLICT (id) DO UPDATE SET
         market_id = EXCLUDED.market_id, outcome_index = EXCLUDED.outcome_index,
         side = EXCLUDED.side, price = EXCLUDED.price, shares = EXCLUDED.shares,
         updated_at = now()
       WHERE pm_limit_orders.user_id = EXCLUDED.user_id`,
      [id, req.user.id, req.user.primaryAddress || null, marketId, outcomeIndex, side, price, shares],
    );
    if (!rowCount) return res.status(409).json({ error: "order belongs to another account" });
    return res.json({ ok: true });
  } catch (e) {
    console.error("[POST /api/pm/orders] error", e);
    return res.status(500).json({ error: "Internal server error" });
  }
});

router.post("/:id/close", authPrivyOptionalWallet, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
    const id = String(req.params.id || "");
    if (!isOrderId(id)) return res.status(400).json({ error: "invalid id" });
    const reason = CLOSE_REASONS.has(String((req.body || {}).reason)) ? String(req.body.reason) : "cancelled";
    await pool.query(
      `UPDATE pm_limit_orders
          SET status = 'closed', closed_reason = $3, closed_at = now(), updated_at = now()
        WHERE id = $1 AND user_id = $2 AND status = 'live'`,
      [id, req.user.id, reason],
    );
    return res.json({ ok: true });
  } catch (e) {
    console.error("[POST /api/pm/orders/:id/close] error", e);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
