// src/routes/notifications.ts
//
// Reads/marks the signed-in user's in-app notifications. Notifications are
// written server-side by the emitters in services/notifications/notify.ts.
// recipient_id == users.id == req.user.id (Privy DID).

import { Router, Response } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

const MAX_LIMIT = 50;
const DEFAULT_LIMIT = 30;

/**
 * GET /api/notifications?limit=30
 * Returns the recipient's most-recent notifications plus the unread count for
 * the badge.
 */
router.get("/notifications", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
    const recipientId = req.user.id;

    const rawLimit = Number(req.query.limit);
    const limit = Number.isFinite(rawLimit)
      ? Math.min(Math.max(Math.trunc(rawLimit), 1), MAX_LIMIT)
      : DEFAULT_LIMIT;

    const [list, count] = await Promise.all([
      pool.query(
        `SELECT id, type, actor_id, payload, read_at, created_at
           FROM public.notifications
          WHERE recipient_id = $1
          ORDER BY created_at DESC
          LIMIT $2`,
        [recipientId, limit]
      ),
      pool.query(
        `SELECT count(*)::int AS n
           FROM public.notifications
          WHERE recipient_id = $1 AND read_at IS NULL`,
        [recipientId]
      ),
    ]);

    return res.json({
      notifications: list.rows,
      unreadCount: count.rows[0]?.n ?? 0,
    });
  } catch (e: any) {
    console.error("[notifications] GET failed", e);
    return res.status(500).json({ error: e?.message || "Failed to load notifications" });
  }
});

/**
 * POST /api/notifications/mark-read
 * Body: { ids?: string[] }  — omit ids to mark ALL unread as read.
 * Returns the new unread count.
 */
router.post("/notifications/mark-read", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });
    const recipientId = req.user.id;

    const ids = Array.isArray(req.body?.ids)
      ? req.body.ids.map((x: any) => String(x)).filter(Boolean)
      : null;

    if (ids && ids.length) {
      await pool.query(
        `UPDATE public.notifications
            SET read_at = now()
          WHERE recipient_id = $1 AND read_at IS NULL AND id = ANY($2::uuid[])`,
        [recipientId, ids]
      );
    } else {
      await pool.query(
        `UPDATE public.notifications
            SET read_at = now()
          WHERE recipient_id = $1 AND read_at IS NULL`,
        [recipientId]
      );
    }

    const count = await pool.query(
      `SELECT count(*)::int AS n
         FROM public.notifications
        WHERE recipient_id = $1 AND read_at IS NULL`,
      [recipientId]
    );

    return res.json({ ok: true, unreadCount: count.rows[0]?.n ?? 0 });
  } catch (e: any) {
    console.error("[notifications] mark-read failed", e);
    return res.status(500).json({ error: e?.message || "Failed to mark read" });
  }
});

export default router;
