// src/routes/profile.ts

/**
 * IMPORTANT:
 * Do NOT default to localhost in production.
 * Set VITE_API_BASE_URL in your frontend env (Vercel/Netlify/etc):
 *   VITE_API_BASE_URL=https://api.blockpools.io
 */
import { Router, Response, NextFunction } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";
import multer from "multer";
import path from "path";
import { PrivyClient } from "@privy-io/server-auth";
import { Resend } from "resend";

import { buildProfilePortfolio } from "../services/profilePortfolio";

const router = Router();

// ── Resend client ────────────────────────────────────────────────────────────
const resend = new Resend(process.env.RESEND_API_KEY);

// Store avatar files under /uploads/avatars (relative to compiled server)
const upload = multer({
  dest: path.join(__dirname, "..", "..", "uploads", "avatars"),
});

const ENV_PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "")
  .trim()
  .replace(/\/+$/, "");

function getPublicBaseUrl(req?: any): string {
  if (ENV_PUBLIC_BASE_URL) return ENV_PUBLIC_BASE_URL;

  const xfProto = (req?.headers?.["x-forwarded-proto"] as string | undefined)
    ?.split(",")[0]
    ?.trim();
  const xfHost = (req?.headers?.["x-forwarded-host"] as string | undefined)
    ?.split(",")[0]
    ?.trim();

  const host = xfHost || req?.headers?.host;
  if (host) {
    const proto = xfProto || req?.protocol || "http";
    return `${proto}://${host}`.replace(/\/+$/, "");
  }

  const port = process.env.PORT || 3001;
  return `http://localhost:${port}`;
}

function normalizeAvatarUrl(
  avatarUrl: string | null | undefined,
  publicBaseUrl: string
): string | null {
  if (!avatarUrl) return null;
  if (!avatarUrl.includes("localhost")) return avatarUrl;

  try {
    const u = new URL(avatarUrl);
    if (u.pathname.startsWith("/uploads/")) {
      return `${publicBaseUrl}${u.pathname}`;
    }
    return avatarUrl;
  } catch {
    return avatarUrl.replace(/^http:\/\/localhost:\d+/, publicBaseUrl);
  }
}

function normalizeProfileRow(row: any, publicBaseUrl: string) {
  if (!row) return row;
  return {
    ...row,
    avatar_url: normalizeAvatarUrl(row.avatar_url, publicBaseUrl),
  };
}

// ── Shared welcome email helper ──────────────────────────────────────────────
// Sends the welcome email via Resend and marks welcome_email_sent = true.
// Safe to call from any route — errors are caught and logged, never re-thrown.
async function sendWelcomeEmail(
  userId: string,
  email: string,
  context: string
): Promise<void> {
  try {
    console.log(`[Welcome Email][${context}] Sending to: ${email} (userId: ${userId})`);
    console.log(`[Welcome Email][${context}] RESEND_API_KEY present: ${!!process.env.RESEND_API_KEY}`);

    await resend.emails.send({
      from: process.env.RESEND_FROM_EMAIL || "BlockPools <welcome@mail.blockpools.io>",
      to: email,
      subject: "Welcome to BlockPools",
      template: "2a86d254-f493-45d1-abda-706fd33f1479",
    } as any);

    await pool.query(
      `UPDATE users SET welcome_email_sent = true WHERE id = $1`,
      [userId]
    );

    console.log(`[Welcome Email][${context}] Sent and flagged OK for userId: ${userId}`);
  } catch (err: any) {
    console.error(`[Welcome Email][${context}] Failed:`, err?.message || err);
  }
}

// ── Privy optional auth (for public routes that benefit from knowing the viewer) ──
const PRIVY_APP_ID = (process.env.PRIVY_APP_ID || "").trim();
const PRIVY_APP_SECRET = (process.env.PRIVY_APP_SECRET || "").trim();

const privyOptionalClient =
  PRIVY_APP_ID && PRIVY_APP_SECRET
    ? new PrivyClient(PRIVY_APP_ID, PRIVY_APP_SECRET)
    : null;

async function authPrivyOptional(
  req: AuthedRequest,
  _res: Response,
  next: NextFunction
) {
  try {
    if (!privyOptionalClient) return next();

    const authHeader =
      (req.headers.authorization as string | undefined) ||
      (req.headers.Authorization as string | undefined);

    if (!authHeader) return next();

    const [scheme, token] = authHeader.split(" ");
    if (scheme !== "Bearer" || !token) return next();

    const { userId } = await privyOptionalClient.verifyAuthToken(token);
    const user = await privyOptionalClient.getUser(userId);

    const smartAddress = user.smartWallet?.address
      ? user.smartWallet.address.toLowerCase()
      : null;
    const eoaAddress = user.wallet?.address
      ? user.wallet.address.toLowerCase()
      : null;
    const primaryAddress = smartAddress ?? eoaAddress;

    if (!primaryAddress) return next();

    req.user = { id: user.id, primaryAddress, smartAddress, eoaAddress };
    return next();
  } catch {
    return next();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/profile/:address/portfolio
 */
router.get(
  "/:address(0x[a-fA-F0-9]{40})/portfolio",
  async (req: AuthedRequest, res: Response) => {
    try {
      const out = await buildProfilePortfolio(req);
      return res.json(out);
    } catch (err: any) {
      console.error("[GET /api/profile/:address/portfolio] error", err);
      return res.status(500).json({ ok: false, error: "internal_error" });
    }
  }
);

/**
 * GET /api/profile/me
 */
router.get("/me", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.email,
        u.welcome_email_sent,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount"
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Profile not found" });
    }

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[GET /api/profile/me] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile/sync-email
 *
 * Called fire-and-forget on every login from the frontend (TopAccountBar +
 * TopAccountBarMobile). Persists the Privy email for the first time if the
 * column is currently null, then sends a welcome email if one hasn't been
 * sent yet. Idempotent — safe to call on every login.
 *
 * This is the primary path that catches:
 *  - Users who signed up before email collection was in place
 *  - Users who skipped the profile modal
 *  - Any user whose email was saved by a previous code path but whose
 *    welcome_email_sent flag was never set
 */
router.post("/sync-email", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;
    const { email } = req.body || {};

    // Wallet-only users (no Privy email) — nothing to do
    if (!email || typeof email !== "string") {
      return res.json({ ok: true, saved: false });
    }

    // Write email only if the column is currently null/empty
    const writeResult = await pool.query(
      `UPDATE users
         SET email = $1, updated_at = NOW()
       WHERE id = $2
         AND (email IS NULL OR email = '')
       RETURNING email, welcome_email_sent`,
      [email, userId]
    );

    const wrote = writeResult.rows.length > 0;
    console.log(`[sync-email] userId: ${userId}, wrote new email: ${wrote}`);

    if (wrote) {
      // Email just saved for the first time — send welcome if not already sent
      const row = writeResult.rows[0];
      if (!row.welcome_email_sent) {
        await sendWelcomeEmail(userId, email, "sync-email/first-save");
      }
    } else {
      // Email was already stored — check whether welcome email still needs sending
      // (handles the case where email existed from a previous path but the flag
      //  was never set, e.g. before welcome_email_sent column was added)
      const check = await pool.query(
        `SELECT email, welcome_email_sent FROM users WHERE id = $1 LIMIT 1`,
        [userId]
      );
      const existing = check.rows[0];
      if (existing?.email && !existing.welcome_email_sent) {
        console.log(
          `[sync-email] Email already set but welcome not sent — sending catchup for userId: ${userId}`
        );
        await sendWelcomeEmail(userId, existing.email, "sync-email/catchup");
      }
    }

    return res.json({ ok: true, saved: wrote });
  } catch (err) {
    console.error("[POST /api/profile/sync-email] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile
 *
 * Creates or updates the current user's profile (username, socials, etc.).
 * On first-time profile creation (username was previously null/empty):
 *   - Saves email to DB
 *   - Fires welcome email if not already sent
 */
router.post("/", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;
    console.log(`[POST /api/profile] HIT — userId: ${userId}`);

    const primaryAddress = req.user.primaryAddress.toLowerCase();
    const eoaAddress = req.user.eoaAddress
      ? req.user.eoaAddress.toLowerCase()
      : null;

    const { username, display_name, x_handle, instagram_handle, avatar_url, email } =
      req.body || {};

    console.log(`[POST /api/profile] body — username: ${username}, email: ${email}`);

    if (!username || typeof username !== "string") {
      return res.status(400).json({ error: "username is required" });
    }

    const now = new Date().toISOString();

    // Determine whether this is a first-time profile creation.
    // We check for a missing/empty username rather than row absence because
    // Privy may pre-create the user row before the profile modal is submitted.
    const existingUser = await pool.query(
      `SELECT id, email, username, welcome_email_sent FROM users WHERE id = $1 LIMIT 1`,
      [userId]
    );

    const isNewUser =
      existingUser.rows.length === 0 ||
      !existingUser.rows[0].username ||
      existingUser.rows[0].username.trim() === "";

    // Prefer the email from the request body; fall back to whatever is already
    // stored so we never accidentally null it out.
    const emailToSave =
      email ?? existingUser.rows[0]?.email ?? null;

    console.log(
      `[POST /api/profile] isNewUser: ${isNewUser}, emailToSave: ${emailToSave ?? "null"}`
    );

    const result = await pool.query(
      `
      INSERT INTO users (
        id,
        primary_address,
        eoa_address,
        username,
        display_name,
        x_handle,
        instagram_handle,
        avatar_url,
        email,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)
      ON CONFLICT (id) DO UPDATE SET
        primary_address  = EXCLUDED.primary_address,
        eoa_address      = EXCLUDED.eoa_address,
        username         = EXCLUDED.username,
        display_name     = EXCLUDED.display_name,
        x_handle         = EXCLUDED.x_handle,
        instagram_handle = EXCLUDED.instagram_handle,
        avatar_url       = EXCLUDED.avatar_url,
        email            = COALESCE(EXCLUDED.email, users.email),
        updated_at       = EXCLUDED.updated_at
      RETURNING
        id,
        primary_address,
        eoa_address,
        username,
        display_name,
        x_handle,
        instagram_handle,
        avatar_url,
        email,
        welcome_email_sent,
        created_at,
        updated_at
      `,
      [
        userId,
        primaryAddress,
        eoaAddress,
        username,
        display_name ?? null,
        x_handle ?? null,
        instagram_handle ?? null,
        avatar_url ?? null,
        emailToSave,
        now,
      ]
    );

    const savedProfile = result.rows[0];
    console.log(
      `[POST /api/profile] savedProfile.email: ${savedProfile.email}, welcome_email_sent: ${savedProfile.welcome_email_sent}`
    );

    // Send welcome email on first-time profile creation if not already sent
    if (isNewUser && savedProfile.email && !savedProfile.welcome_email_sent) {
      await sendWelcomeEmail(userId, savedProfile.email, "profile-upsert");
    } else {
      console.log(
        `[Welcome Email] Skipped — isNewUser: ${isNewUser}, email: ${savedProfile.email ?? "null"}, already_sent: ${savedProfile.welcome_email_sent}`
      );
    }

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(savedProfile, publicBaseUrl));
  } catch (err) {
    console.error("[POST /api/profile] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile/avatar
 */
router.post(
  "/avatar",
  authPrivy,
  upload.single("avatar"),
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });
      if (!req.file) return res.status(400).json({ error: "avatar file is required" });

      const userId = req.user.id;
      const now = new Date().toISOString();

      const publicBaseUrl = getPublicBaseUrl(req);
      const avatarUrl = `${publicBaseUrl}/uploads/avatars/${req.file.filename}`;

      const result = await pool.query(
        `
        UPDATE users
        SET avatar_url = $1,
            updated_at = $2
        WHERE id = $3
        RETURNING
          id,
          primary_address,
          eoa_address,
          username,
          display_name,
          x_handle,
          instagram_handle,
          avatar_url,
          created_at,
          updated_at
        `,
        [avatarUrl, now, userId]
      );

      if (result.rows.length === 0) {
        return res.status(404).json({ error: "Profile not found" });
      }

      return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
    } catch (err) {
      console.error("[POST /api/profile/avatar] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * POST /api/profile/by-addresses
 */
router.post("/by-addresses", async (req: AuthedRequest, res: Response) => {
  try {
    const addresses = Array.isArray(req.body?.addresses) ? req.body.addresses : [];
    const addrLower = Array.from(
      new Set(addresses.filter(Boolean).map((a: any) => String(a).toLowerCase()))
    );

    if (addrLower.length === 0) return res.json([]);

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.created_at,
        u.updated_at
      FROM users u
      WHERE u.primary_address = ANY($1::text[])
         OR u.eoa_address     = ANY($1::text[])
      `,
      [addrLower]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    const data = (result.rows || []).map((r: any) =>
      normalizeProfileRow(r, publicBaseUrl)
    );
    return res.json(data);
  } catch (err) {
    console.error("[POST /api/profile/by-addresses] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/by-id?profileId=...
 */
router.get("/by-id", authPrivyOptional, async (req: AuthedRequest, res: Response) => {
  try {
    const profileId = req.query.profileId as string;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const viewerId = req.user?.id ?? null;
    const params: any[] = [profileId];
    if (viewerId) params.push(viewerId);

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount"
        ${
          viewerId
            ? `,
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $2 AND following_id = u.id
        ) AS "is_followed_by_me"`
            : ""
        }
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      params
    );

    if (result.rows.length === 0)
      return res.status(404).json({ error: "Profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[GET /api/profile/by-id] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile/:profileId/follow
 */
router.post("/:profileId/follow", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const viewerId = req.user.id;
    const { profileId } = req.params;

    if (!profileId) return res.status(400).json({ error: "profileId is required" });
    if (viewerId === profileId)
      return res.status(400).json({ error: "Cannot follow your own profile" });

    const targetRes = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (targetRes.rows.length === 0)
      return res.status(404).json({ error: "Target profile not found" });

    await pool.query(
      `
      INSERT INTO user_follows (follower_id, following_id)
      VALUES ($1, $2)
      ON CONFLICT (follower_id, following_id) DO NOTHING
      `,
      [viewerId, profileId]
    );

    const { rows } = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount",
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $1 AND following_id = u.id
        ) AS "is_followed_by_me"
      FROM users u
      WHERE u.id = $2
      LIMIT 1
      `,
      [viewerId, profileId]
    );

    if (!rows.length) return res.status(404).json({ error: "Target profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[POST /api/profile/:profileId/follow] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * DELETE /api/profile/:profileId/follow
 */
router.delete("/:profileId/follow", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const viewerId = req.user.id;
    const { profileId } = req.params;

    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    await pool.query(
      `DELETE FROM user_follows WHERE follower_id = $1 AND following_id = $2`,
      [viewerId, profileId]
    );

    const { rows } = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount",
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $1 AND following_id = u.id
        ) AS "is_followed_by_me"
      FROM users u
      WHERE u.id = $2
      LIMIT 1
      `,
      [viewerId, profileId]
    );

    if (!rows.length) return res.status(404).json({ error: "Target profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[DELETE /api/profile/:profileId/follow] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/:profileId/follow-status
 */
router.get(
  "/:profileId/follow-status",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const viewerId = req.user.id;
      const { profileId } = req.params;

      if (!profileId) return res.status(400).json({ error: "profileId is required" });

      const check = await pool.query(
        `
        SELECT 1
        FROM user_follows
        WHERE follower_id = $1 AND following_id = $2
        LIMIT 1
        `,
        [viewerId, profileId]
      );

      return res.json({ is_followed_by_me: check.rows.length > 0 });
    } catch (err) {
      console.error("[GET /api/profile/:profileId/follow-status] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * GET /api/profile/:profileId/followers
 */
router.get("/:profileId/followers", async (req: AuthedRequest, res: Response) => {
  try {
    const { profileId } = req.params;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const target = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (target.rows.length === 0)
      return res.status(404).json({ error: "Profile not found" });

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.created_at,
        u.updated_at
      FROM user_follows f
      JOIN users u ON u.id = f.follower_id
      WHERE f.following_id = $1
      ORDER BY u.created_at DESC
      `,
      [profileId]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    const data = (result.rows || []).map((r: any) =>
      normalizeProfileRow(r, publicBaseUrl)
    );
    return res.json({ data });
  } catch (err) {
    console.error("[GET /api/profile/:profileId/followers] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/:profileId/following
 */
router.get("/:profileId/following", async (req: AuthedRequest, res: Response) => {
  try {
    const { profileId } = req.params;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const target = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (target.rows.length === 0)
      return res.status(404).json({ error: "Profile not found" });

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.created_at,
        u.updated_at
      FROM user_follows f
      JOIN users u ON u.id = f.following_id
      WHERE f.follower_id = $1
      ORDER BY u.created_at DESC
      `,
      [profileId]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    const data = (result.rows || []).map((r: any) =>
      normalizeProfileRow(r, publicBaseUrl)
    );
    return res.json({ data });
  } catch (err) {
    console.error("[GET /api/profile/:profileId/following] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;