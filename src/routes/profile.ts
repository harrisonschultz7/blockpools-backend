import { Router, Response } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";
import multer from "multer";
import path from "path";

const router = Router();

// Store avatar files under /uploads/avatars (relative to compiled server)
const upload = multer({
  dest: path.join(__dirname, "..", "..", "uploads", "avatars"),
});

const PUBLIC_BASE_URL =
  process.env.PUBLIC_BASE_URL ||
  `http://localhost:${process.env.PORT || 3001}`;

/**
 * GET /api/profile/me
 * Returns current user's profile, or 404 if not created yet.
 */
router.get("/me", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) {
      return res.status(401).json({ error: "Not authenticated" });
    }

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
        u.created_at,
        u.updated_at,
        -- how many users follow ME
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        -- how many users I follow
        (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount"
      FROM users u
      WHERE u.id = $1
      `,
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Profile not found" });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    console.error("[GET /api/profile/me] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile
 * Creates or updates current user's profile.
 */
router.post("/", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) {
      return res.status(401).json({ error: "Not authenticated" });
    }

    const userId = req.user.id;

    // From authPrivy: smart wallet preferred as primary
    const primaryAddress = req.user.primaryAddress.toLowerCase();
    const eoaAddress = req.user.eoaAddress
      ? req.user.eoaAddress.toLowerCase()
      : null;

    const {
      username,
      display_name,
      x_handle,
      instagram_handle,
      avatar_url,
    } = req.body;

    if (!username || typeof username !== "string") {
      return res.status(400).json({ error: "username is required" });
    }

    const now = new Date().toISOString();

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
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)
      ON CONFLICT (id) DO UPDATE SET
        primary_address   = EXCLUDED.primary_address,
        eoa_address       = EXCLUDED.eoa_address,
        username          = EXCLUDED.username,
        display_name      = EXCLUDED.display_name,
        x_handle          = EXCLUDED.x_handle,
        instagram_handle  = EXCLUDED.instagram_handle,
        avatar_url        = EXCLUDED.avatar_url,
        updated_at        = EXCLUDED.updated_at
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
      [
        userId,
        primaryAddress,
        eoaAddress,
        username,
        display_name ?? null,
        x_handle ?? null,
        instagram_handle ?? null,
        avatar_url ?? null,
        now,
      ]
    );

    return res.json(result.rows[0]);
  } catch (err: any) {
    console.error("[POST /api/profile] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile/avatar
 * Upload a new avatar image for the current user.
 * Expects multipart/form-data with field "avatar".
 */
router.post(
  "/avatar",
  authPrivy,
  upload.single("avatar"),
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      if (!req.file) {
        return res.status(400).json({ error: "avatar file is required" });
      }

      const userId = req.user.id;
      const now = new Date().toISOString();

      // Public URL for the uploaded file
      const avatarUrl = `${PUBLIC_BASE_URL}/uploads/avatars/${req.file.filename}`;

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

      return res.json(result.rows[0]);
    } catch (err) {
      console.error("[POST /api/profile/avatar] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * POST /api/profile/by-addresses
 * Public lookup: { addresses: string[] } -> Profile[]
 * Used by leaderboard + public profile pages.
 */
router.post(
  "/by-addresses",
  async (req: AuthedRequest, res: Response) => {
    try {
      let addresses: string[] = req.body?.addresses || [];
      if (!Array.isArray(addresses)) {
        return res.status(400).json({ error: "addresses must be an array" });
      }

      // normalize -> lowercase + dedupe
      const addrLower = Array.from(
        new Set(
          addresses
            .filter(Boolean)
            .map((a) => String(a).toLowerCase())
        )
      );

      if (addrLower.length === 0) {
        return res.json([]);
      }

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
          -- followers of THIS user
          (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
          -- users THIS user is following
          (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount"
        FROM users u
        WHERE LOWER(u.primary_address) = ANY($1::text[])
           OR LOWER(u.eoa_address)    = ANY($1::text[])
        `,
        [addrLower]
      );

      return res.json(result.rows || []);
    } catch (err) {
      console.error("[POST /api/profile/by-addresses] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * POST /api/profile/:profileId/follow
 * Follow another user by their profile id (Privy DID).
 */
router.post(
  "/:profileId/follow",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      const viewerId = req.user.id; // did:privy:...
      const { profileId } = req.params; // target user id

      if (!profileId) {
        return res.status(400).json({ error: "profileId is required" });
      }

      if (viewerId === profileId) {
        return res
          .status(400)
          .json({ error: "Cannot follow your own profile" });
      }

      // Make sure target user exists
      const targetRes = await pool.query(
        `
        SELECT id
        FROM users
        WHERE id = $1
        `,
        [profileId]
      );

      if (targetRes.rows.length === 0) {
        return res.status(404).json({ error: "Target profile not found" });
      }

      // Insert into user_follows join table
      await pool.query(
        `
        INSERT INTO user_follows (follower_id, following_id)
        VALUES ($1, $2)
        ON CONFLICT (follower_id, following_id) DO NOTHING
        `,
        [viewerId, profileId]
      );

      // Return updated target profile with counts + is_followed_by_me
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
          (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount",
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

      if (!rows.length) {
        return res.status(404).json({ error: "Target profile not found" });
      }

      return res.json(rows[0]);
    } catch (err) {
      console.error("[POST /api/profile/:profileId/follow] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * DELETE /api/profile/:profileId/follow
 * Unfollow another user by their profile id (Privy DID).
 */
router.delete(
  "/:profileId/follow",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      const viewerId = req.user.id;
      const { profileId } = req.params;

      if (!profileId) {
        return res.status(400).json({ error: "profileId is required" });
      }

      await pool.query(
        `
        DELETE FROM user_follows
        WHERE follower_id = $1
          AND following_id = $2
        `,
        [viewerId, profileId]
      );

      // Return updated target profile with counts + is_followed_by_me
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
          (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount",
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

      if (!rows.length) {
        return res.status(404).json({ error: "Target profile not found" });
      }

      return res.json(rows[0]);
    } catch (err) {
      console.error("[DELETE /api/profile/:profileId/follow] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * GET /api/profile/:profileId/follow-status
 * Returns whether the current viewer follows this profile.
 */
router.get(
  "/:profileId/follow-status",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      const viewerId = req.user.id;
      const { profileId } = req.params;

      if (!profileId) {
        return res.status(400).json({ error: "profileId is required" });
      }

      const check = await pool.query(
        `
        SELECT 1
        FROM user_follows
        WHERE follower_id = $1
          AND following_id = $2
        LIMIT 1
        `,
        [viewerId, profileId]
      );

      const isFollowed = check.rows.length > 0;

      return res.json({ is_followed_by_me: isFollowed });
    } catch (err) {
      console.error("[GET /api/profile/:profileId/follow-status] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * GET /api/profile/:profileId/followers
 * Returns the list of profiles that follow this user.
 */
router.get(
  "/:profileId/followers",
  async (req: AuthedRequest, res: Response) => {
    try {
      const { profileId } = req.params;

      if (!profileId) {
        return res.status(400).json({ error: "profileId is required" });
      }

      // Ensure target user exists (optional but nice for 404 clarity)
      const target = await pool.query(
        `
        SELECT id
        FROM users
        WHERE id = $1
        `,
        [profileId]
      );

      if (target.rows.length === 0) {
        return res.status(404).json({ error: "Profile not found" });
      }

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
        JOIN users u
          ON u.id = f.follower_id
        WHERE f.following_id = $1
        ORDER BY u.created_at DESC
        `,
        [profileId]
      );

      // Wrap in { data: [...] } to match frontend expectation
      return res.json({ data: result.rows || [] });
    } catch (err) {
      console.error("[GET /api/profile/:profileId/followers] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * GET /api/profile/:profileId/following
 * Returns the list of profiles that this user is following.
 */
router.get(
  "/:profileId/following",
  async (req: AuthedRequest, res: Response) => {
    try {
      const { profileId } = req.params;

      if (!profileId) {
        return res.status(400).json({ error: "profileId is required" });
      }

      // Ensure target user exists
      const target = await pool.query(
        `
        SELECT id
        FROM users
        WHERE id = $1
        `,
        [profileId]
      );

      if (target.rows.length === 0) {
        return res.status(404).json({ error: "Profile not found" });
      }

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
        JOIN users u
          ON u.id = f.following_id
        WHERE f.follower_id = $1
        ORDER BY u.created_at DESC
        `,
        [profileId]
      );

      return res.json({ data: result.rows || [] });
    } catch (err) {
      console.error("[GET /api/profile/:profileId/following] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);


export default router;
