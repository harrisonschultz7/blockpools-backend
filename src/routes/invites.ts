// src/routes/invites.ts
import { Router, Response } from "express";
import crypto from "crypto";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";
import { Resend } from "resend";

const router = Router();
const resend = new Resend(process.env.RESEND_API_KEY);

function requireEnv(name: string): string {
  const v = process.env[name];
  if (!v) throw new Error(`Missing env var: ${name}`);
  return v;
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
 * Auth: Privy (authPrivy middleware)
 */
router.post(
  "/invites/email",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const inviterUserId = req.user.id;

      const email = String(req.body?.email || "").trim().toLowerCase();
      if (!isValidEmail(email)) {
        return res.status(400).json({ error: "Invalid email" });
      }

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
const inviteUrl = `${appBaseUrl}/app?invite=${token}`;

      // Insert first (audit trail). Mark 'sent' only if email succeeds.
      const insertResult = await pool.query(
        `insert into invites (inviter_user_id, invitee_email, token_hash, status)
         values ($1, $2, $3, 'pending')
         returning id`,
        [inviterUserId, email, tokenHash]
      );

      const inviteId = insertResult.rows[0]?.id;
      const from = requireEnv("EMAIL_FROM_INVITES");

      try {
        const sendResult = await resend.emails.send({
          from,
          to: email,
          subject: "You’ve been invited to BlockPools",
          html: `
            <div style="font-family: ui-sans-serif, system-ui; line-height: 1.5;">
              <h2>You’ve been invited to BlockPools</h2>
              <p>Click below to accept your invite:</p>
              <p>
                <a href="${inviteUrl}"
                   style="display:inline-block;padding:10px 14px;border-radius:10px;text-decoration:none;border:1px solid #e6d7b5">
                  Accept invite
                </a>
              </p>
              <p style="color:#666;font-size:12px">
                If you weren’t expecting this, you can ignore this email.
              </p>
            </div>
          `,
        });

        await pool.query(
          `update invites
             set status = 'sent'
           where id = $1`,
          [inviteId]
        );

        // In prod you may not want to return inviteUrl
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
  }
);

/**
 * GET /api/invites/preview/:token
 * Optional (no auth): lets frontend show "Invited by ..." on the invite landing page.
 * Returns minimal info only.
 */
router.get("/invites/preview/:token", async (req, res) => {
  try {
    const token = String(req.params?.token || "").trim();
    if (!token || token.length < 20) return res.status(400).json({ error: "Invalid token" });

    const tokenHash = sha256Hex(token);

    // Adjust join to your users/profiles schema if you want to show inviter username.
    const { rows } = await pool.query(
      `
      select i.status, i.created_at, i.inviter_user_id
      from invites i
      where i.token_hash = $1
      limit 1
      `,
      [tokenHash]
    );

    if (!rows[0]) return res.status(404).json({ error: "Invite not found" });

    // Keep response minimal (don’t leak invitee email, etc.)
    return res.json({ ok: true, invite: rows[0] });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Failed" });
  }
});

/**
 * POST /api/invites/redeem
 * Body: { token: string }
 * Auth required: the invitee must be logged in so you can attribute redemption to a user.
 */
router.post("/invites/redeem", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const token = String(req.body?.token || "").trim();
    if (!token || token.length < 20) return res.status(400).json({ error: "Invalid token" });

    const tokenHash = sha256Hex(token);
    const inviteeUserId = req.user.id;

    // Mark redeemed only once. If your table doesn't have these columns yet,
    // either add them, or simplify to just status='redeemed'.
    const { rows } = await pool.query(
      `
      update invites
         set status = 'redeemed',
             redeemed_at = now(),
             redeemed_by_user_id = $2
       where token_hash = $1
         and status in ('sent','pending')
       returning inviter_user_id, status
      `,
      [tokenHash, inviteeUserId]
    );

    if (!rows[0]) {
      return res.status(409).json({ error: "Invite already redeemed or not found" });
    }

    return res.json({ ok: true, inviterUserId: rows[0].inviter_user_id });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Failed to redeem invite" });
  }
});

export default router;
