// src/routes/pmOrders.ts
//
// The signed-in user's resting Polymarket limit orders placed through BlockPools.
//
//   GET  /api/pm/orders              -> { orders: [...] }   live orders
//   POST /api/pm/orders              -> record one          { id, marketId, outcomeIndex, side, price, shares }
//   POST /api/pm/orders/:id/close    -> mark closed         { reason }
//
// KEEP AFTER KICKOFF. Polymarket clears every resting order on a sports market
// at the official start. A POST may ask for carryOver (with tokenId,
// signerAddress, gameStartAt); it is granted only if Privy confirms the user's
// signer wallet has our session signer, and the reply says whether it was. The
// re-placement itself is trading-bot/run/limit-carryover.js.
//
// WHY THIS EXISTS. Polymarket will not report these orders back to us:
// listOpenOrders returns nothing for Deposit-Wallet orders and fetchOrder's
// response fails the SDK schema. The app therefore records each limit order at
// placement; this table makes that record the same on phone and desktop (it was
// one browser's localStorage). Keyed by the Privy user id, identical across
// devices. The client still checks each order with the venue when it can and
// closes it here once it fills or is cancelled.

import { Router, Response } from "express";
import { PrivyClient } from "@privy-io/server-auth";
import { pool } from "../db";
import { authPrivyOptionalWallet, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

/** Safety net: an order nobody ever resolved stops showing after this long. */
const MAX_AGE_DAYS = 21;
const CLOSE_REASONS = new Set(["cancelled", "filled", "edited", "expired", "venue_closed", "game_start"]);

const isOrderId = (x: unknown) => /^0x[0-9a-fA-F]{8,128}$/.test(String(x || ""));
const isCondition = (x: unknown) => /^0x[0-9a-fA-F]{64}$/.test(String(x || ""));
const isAddress = (x: unknown) => /^0x[0-9a-fA-F]{40}$/.test(String(x || ""));
const isTokenId = (x: unknown) => /^\d{10,90}$/.test(String(x || ""));

// Off until trading-bot/run/limit-carryover.js runs somewhere that can reach the
// CLOB: granting carry-over is a promise to re-place the order.
const CARRY_ENABLED = /^(1|true|yes|on)$/i.test(String(process.env.LIMIT_CARRYOVER_ENABLED || ""));

const privy = new PrivyClient(
  (process.env.PRIVY_APP_ID || "").trim(),
  (process.env.PRIVY_APP_SECRET || "").trim(),
);

/**
 * Has this user granted our session signer on THIS wallet? Asked of Privy, never
 * taken from the client: carry-over means we sign an order for them later,
 * unattended, so the flag must only ever be set from Privy's own answer.
 * Errors count as no.
 */
async function hasDelegated(did: string, address: string): Promise<boolean> {
  try {
    const user = await privy.getUserById(did);
    return (user?.linkedAccounts || []).some((a: any) =>
      a.type === "wallet" && a.delegated === true &&
      String(a.address || "").toLowerCase() === address.toLowerCase());
  } catch (e) {
    console.warn("[pm orders] delegation check failed", (e as any)?.message || e);
    return false;
  }
}

function toRow(r: any) {
  return {
    id: r.id,
    marketId: r.market_id,
    outcomeIndex: Number(r.outcome_index),
    side: r.side,
    price: Number(r.price),
    shares: Number(r.shares),
    createdAt: r.created_at,
    carryOver: r.carry_over === true,
    carryStatus: r.carry_status ?? null,
    gameStartAt: r.game_start_at ?? null,
  };
}

router.get("/", authPrivyOptionalWallet, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
    const { rows } = await pool.query(
      `SELECT id, market_id, outcome_index, side, price, shares, created_at,
              carry_over, carry_status, game_start_at
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

    // Optional carry-over. Every field must be sound and the game still ahead,
    // and Privy must confirm the delegation -- otherwise it is recorded as a
    // plain order and the reply says carryOver:false so the app can warn.
    const tokenId = isTokenId(b.tokenId) ? String(b.tokenId) : null;
    const signer = isAddress(b.signerAddress) ? String(b.signerAddress).toLowerCase() : null;
    const startSec = Number(b.gameStartAt);
    const gameStartAt = Number.isFinite(startSec) && startSec > 1e9 ? new Date(startSec * 1000) : null;
    // Game identity + display metadata, so a later resting fill can be written
    // to the trade table under the same game as the app's own rows
    // (src/workers/pmFillSyncWorker.ts). Bounded; never trusted for money.
    const gameId = typeof b.gameId === "string" && b.gameId.length <= 120 ? b.gameId.toLowerCase() : null;
    const m = b.meta && typeof b.meta === "object" ? b.meta : null;
    const str = (x: unknown) => (typeof x === "string" && x.length <= 200 ? x : null);
    const meta = m ? {
      league: str(m.league), lockTime: Number(m.lockTime) > 0 ? Math.trunc(Number(m.lockTime)) : null,
      teamACode: str(m.teamACode), teamBCode: str(m.teamBCode),
      teamAName: str(m.teamAName), teamBName: str(m.teamBName),
      marketType: str(m.marketType), marketQuestion: str(m.marketQuestion),
      // Which flow placed it, and for a Queue ticket which model it follows:
      // the copy worker uses these to execute that model's stop-loss for
      // ticket holders who granted the signer (trading-bot copy/fanout.js
      // ticketStops). "copy" is reserved for the worker itself.
      source: m.source === "queue" ? "queue" : null,
      botId: typeof m.botId === "string" && /^[a-z0-9-]{1,40}$/.test(m.botId) ? m.botId : null,
    } : null;
    let carryOver = false;
    if (CARRY_ENABLED && b.carryOver === true && tokenId && signer && gameStartAt && gameStartAt.getTime() > Date.now()) {
      carryOver = await hasDelegated(req.user.id, signer);
    }

    // Upsert, but never across users: an id already owned by someone else is
    // left untouched (the WHERE on the conflict update), and reported.
    const { rowCount } = await pool.query(
      `INSERT INTO pm_limit_orders (id, user_id, owner_address, market_id, outcome_index, side, price, shares,
                                    token_id, signer_address, game_start_at, carry_over, carry_status, game_id, meta)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, CASE WHEN $12 THEN 'pending' END, $13, $14)
       ON CONFLICT (id) DO UPDATE SET
         market_id = EXCLUDED.market_id, outcome_index = EXCLUDED.outcome_index,
         side = EXCLUDED.side, price = EXCLUDED.price, shares = EXCLUDED.shares,
         token_id = COALESCE(EXCLUDED.token_id, pm_limit_orders.token_id),
         signer_address = COALESCE(EXCLUDED.signer_address, pm_limit_orders.signer_address),
         game_start_at = COALESCE(EXCLUDED.game_start_at, pm_limit_orders.game_start_at),
         carry_over = pm_limit_orders.carry_over OR EXCLUDED.carry_over,
         carry_status = COALESCE(pm_limit_orders.carry_status, EXCLUDED.carry_status),
         game_id = COALESCE(EXCLUDED.game_id, pm_limit_orders.game_id),
         meta = COALESCE(EXCLUDED.meta, pm_limit_orders.meta),
         updated_at = now()
       WHERE pm_limit_orders.user_id = EXCLUDED.user_id`,
      [id, req.user.id, req.user.primaryAddress || null, marketId, outcomeIndex, side, price, shares,
       tokenId, signer, gameStartAt, carryOver, gameId, meta ? JSON.stringify(meta) : null],
    );
    if (!rowCount) return res.status(409).json({ error: "order belongs to another account" });
    return res.json({ ok: true, carryOver });
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
