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

      // Create token + store hash
      const token = crypto.randomBytes(32).toString("hex");
      const tokenHash = sha256Hex(token);

      const appBaseUrl = requireEnv("APP_BASE_URL").replace(/\/+$/, "");
      const inviteUrl = `${appBaseUrl}/invite/${token}`;

      // Store invite row first (so we have an audit trail even if email fails)
      await pool.query(
        `insert into invites (inviter_user_id, invitee_email, token_hash, status)
         values ($1, $2, $3, 'sent')`,
        [inviterUserId, email, tokenHash]
      );

      const from = requireEnv("EMAIL_FROM_INVITES");

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

      return res.json({ ok: true, inviteUrl, sendResult });
    } catch (e: any) {
      return res.status(500).json({ error: e?.message || "Failed to send invite" });
    }
  }
);

export default router;
