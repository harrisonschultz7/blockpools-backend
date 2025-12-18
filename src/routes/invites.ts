// src/routes/invites.ts
import { Router, Response } from "express";
import crypto from "crypto";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";
import { sendInviteEmail } from "../services/emailService";

const router = Router();

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v || !v.trim()) throw new Error(`Missing env var: ${name}`);
  return v.trim();
}

function isValidEmail(email: string) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

function sha256Hex(input: string) {
  return crypto.createHash("sha256").update(input).digest("hex");
}

/**
 * POST /api/invites/email
 * Body: { email: string }
 * Auth: Privy
 */
router.post("/invites/email", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const inviterUserId = req.user.id;

    const email = String(req.body?.email || "").trim().toLowerCase();
    if (!isValidEmail(email)) return res.status(400).json({ error: "Invalid email" });

    // Rate limit: max 5 invites per 24 hours per user
    const { rows: countRows } = await pool.query(
      `select count(*)::int as c
         from invites
        where inviter_user_id = $1
          and created_at > now() - interval '24 hours'`,
      [inviterUserId]
    );

    if ((countRows[0]?.c ?? 0) >= 5) {
      return res.status(429).json({ error: "Invite limit reached (24h)" });
    }

    const token = crypto.randomBytes(32).toString("hex");
    const tokenHash = sha256Hex(token);

    const appBaseUrl = requireEnv("APP_BASE_URL").replace(/\/+$/, "");
    // Use /app?invite=... (your current approach)
    const inviteUrl = `${appBaseUrl}/app?invite=${token}`;

    // Optional: show inviter label in email if you have a profiles table
    // Adjust to your schema (profiles/usernames/etc). If you don’t have it, leave undefined.
    let inviterLabel: string | undefined = undefined;
    try {
      const { rows: p } = await pool.query(
        `select coalesce(display_name, username) as label
           from profiles
          where user_id = $1
          limit 1`,
        [inviterUserId]
      );
      inviterLabel = p?.[0]?.label || undefined;
    } catch {
      // safe to ignore if profiles table doesn't exist / differs
    }

    // Insert first (audit trail). Mark 'sent' only if email succeeds.
    const insertResult = await pool.query(
      `insert into invites (inviter_user_id, invitee_email, token_hash, status)
       values ($1, $2, $3, 'pending')
       returning id`,
      [inviterUserId, email, tokenHash]
    );

    const inviteId = insertResult.rows[0]?.id;

    try {
      const sendResult = await sendInviteEmail({
        to: email,
        inviteUrl,
        inviterLabel,
      });

      await pool.query(
        `update invites
            set status = 'sent'
          where id = $1`,
        [inviteId]
      );

      return res.json({ ok: true, sendResult });
    } catch (emailErr: any) {
      await pool.query(
        `update invites
            set status = 'failed'
          where id = $1`,
        [inviteId]
      );
      return res.status(502).json({ error: emailErr?.message || "Email send failed" });
    }
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Failed to send invite" });
  }
});

/**
 * GET /api/invites/preview/:token
 * Optional (no auth): lets frontend show “Invited by …”.
 */
router.get("/invites/preview/:token", async (req, res) => {
  try {
    const token = String(req.params?.token || "").trim();
    if (!token || token.length < 20) return res.status(400).json({ error: "Invalid token" });

    const tokenHash = sha256Hex(token);

    // Keep response minimal; optionally join profiles to show inviter label
    const { rows } = await pool.query(
      `
      select
        i.status,
        i.created_at,
        i.inviter_user_id,
        coalesce(p.display_name, p.username) as inviter_label
      from invites i
      left join profiles p on p.user_id = i.inviter_user_id
      where i.token_hash = $1
      limit 1
      `,
      [tokenHash]
    );

    if (!rows[0]) return res.status(404).json({ error: "Invite not found" });

    return res.json({
      ok: true,
      invite: {
        status: rows[0].status,
        created_at: rows[0].created_at,
        inviter_user_id: rows[0].inviter_user_id,
        inviter_label: rows[0].inviter_label || null,
      },
    });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Failed" });
  }
});

/**
 * POST /api/invites/accept
 * Body: { token: string }
 * Auth required: call this immediately AFTER the invitee logs in.
 *
 * This is the critical piece for attribution: it links token_hash -> accepted_by_user_id.
 */
router.post("/invites/accept", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const token = String(req.body?.token || "").trim();
    if (!token || token.length < 20) return res.status(400).json({ error: "Invalid token" });

    const tokenHash = sha256Hex(token);
    const inviteeUserId = req.user.id;

    const { rows } = await pool.query(
      `
      update invites
         set status = case
                        when status in ('sent','pending') then 'accepted'
                        else status
                      end,
             accepted_at = coalesce(accepted_at, now()),
             accepted_by_user_id = coalesce(accepted_by_user_id, $2)
       where token_hash = $1
       returning inviter_user_id, status, accepted_at, accepted_by_user_id
      `,
      [tokenHash, inviteeUserId]
    );

    if (!rows[0]) return res.status(404).json({ error: "Invite not found" });

    return res.json({
      ok: true,
      inviterUserId: rows[0].inviter_user_id,
      status: rows[0].status,
      acceptedAt: rows[0].accepted_at,
      acceptedByUserId: rows[0].accepted_by_user_id,
    });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Failed to accept invite" });
  }
});

/**
 * POST /api/invites/redeem
 * Body: { token: string }
 * Auth required
 *
 * Keep this if you want a second “finalization” step (e.g., after profile creation).
 */
router.post("/invites/redeem", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const token = String(req.body?.token || "").trim();
    if (!token || token.length < 20) return res.status(400).json({ error: "Invalid token" });

    const tokenHash = sha256Hex(token);
    const inviteeUserId = req.user.id;

    const { rows } = await pool.query(
      `
      update invites
         set status = 'redeemed',
             redeemed_at = now(),
             redeemed_by_user_id = $2
       where token_hash = $1
         and status in ('sent','pending','accepted')
       returning inviter_user_id, status, redeemed_at, redeemed_by_user_id
      `,
      [tokenHash, inviteeUserId]
    );

    if (!rows[0]) {
      return res.status(409).json({ error: "Invite already redeemed or not found" });
    }

    return res.json({
      ok: true,
      inviterUserId: rows[0].inviter_user_id,
      status: rows[0].status,
      redeemedAt: rows[0].redeemed_at,
      redeemedByUserId: rows[0].redeemed_by_user_id,
    });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Failed to redeem invite" });
  }
});

export default router;
