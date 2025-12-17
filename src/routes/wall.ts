// src/routes/wall.ts
import { Router, Response } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

/**
 * Prefer an explicit PUBLIC_BASE_URL in production.
 * Example: PUBLIC_BASE_URL=https://api.blockpools.io
 */
const ENV_PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "")
  .trim()
  .replace(/\/+$/, "");

/**
 * Build a public base URL for the current request.
 * - If PUBLIC_BASE_URL env exists, always use it.
 * - Otherwise, attempt to infer from proxy headers.
 * - Fallback to localhost.
 */
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

function normalizeWallRow(row: any, publicBaseUrl: string) {
  if (!row) return row;
  return {
    ...row,
    author_avatar_url: normalizeAvatarUrl(row.author_avatar_url, publicBaseUrl),
  };
}

function normalizeCommentRow(row: any, publicBaseUrl: string) {
  if (!row) return row;
  return {
    ...row,
    author_avatar_url: normalizeAvatarUrl(row.author_avatar_url, publicBaseUrl),
  };
}

/**
 * GET /api/wall/feed
 *
 * Returns posts authored by:
 *  - the logged-in user
 *  - plus all users they follow
 *
 * Shape:
 *   { data: WallPost[], nextCursor, hasMore }
 */
router.get(
  "/wall/feed",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const viewerUserId = req.user.id; // users.id (did:privy:...)
      const limit = Math.min(Number(req.query.limit) || 50, 100);
      const cursor = req.query.cursor as string | undefined;
      const cursorDate = cursor ? new Date(cursor) : null;

      const publicBaseUrl = getPublicBaseUrl(req);

      // 1) Look up who the viewer follows
      let followingIds: string[] = [];
      try {
        const { rows: followRows } = await pool.query(
          `
          SELECT following_id
          FROM user_follows
          WHERE follower_id = $1;
          `,
          [viewerUserId]
        );

        followingIds = (followRows || [])
          .map((r: any) => r.following_id)
          .filter((v: any) => typeof v === "string" && v.trim().length > 0);
      } catch (followErr) {
        console.error(
          "[GET /wall/feed] follow query failed, treating as no follows",
          followErr
        );
        followingIds = [];
      }

      // Always include the viewer themself so they see their own posts
      const authorIds: string[] = Array.from(
        new Set<string>([viewerUserId, ...followingIds])
      );

      if (authorIds.length === 0) {
        return res.json({ data: [], nextCursor: null, hasMore: false });
      }

      // 2) Fetch posts authored by myself + people I follow
      const baseParams: any[] = [viewerUserId, authorIds];
      let cursorClause = "";
      if (cursorDate) {
        cursorClause = "AND p.created_at < $3";
        baseParams.push(cursorDate.toISOString());
      }

      const limitParamIndex = baseParams.length + 1;
      baseParams.push(limit + 1); // +1 to detect hasMore

      const postsSql = `
        SELECT
          p.id,
          p.content,
          p.created_at,
          p.updated_at,
          p.is_deleted,
          p.profile_id,
          p.author_profile_id,

          -- author identity (MUST be present)
          au.id               AS author_id,
          au.username         AS author_username,
          au.display_name     AS author_display_name,
          au.avatar_url       AS author_avatar_url,
          au.primary_address  AS author_primary_address,
          au.eoa_address      AS author_eoa_address,

          -- likes
          COALESCE(l.like_count, 0)::int AS like_count,
          CASE WHEN my_like.profile_id IS NULL THEN false ELSE true END AS liked_by_me
        FROM wall_posts p
        JOIN users au ON au.id = p.author_profile_id
        LEFT JOIN (
          SELECT post_id, COUNT(*) AS like_count
          FROM wall_post_likes
          GROUP BY post_id
        ) l ON l.post_id = p.id
        LEFT JOIN wall_post_likes my_like
          ON my_like.post_id = p.id
         AND my_like.profile_id = $1  -- viewer
        WHERE p.author_profile_id = ANY($2)
          AND p.is_deleted = FALSE
          ${cursorClause}
        ORDER BY p.created_at DESC
        LIMIT $${limitParamIndex};
      `;

      const { rows: postRowsRaw } = await pool.query(postsSql, baseParams);
      const postRows = postRowsRaw || [];

      const hasMore = postRows.length > limit;
      const posts = hasMore ? postRows.slice(0, limit) : postRows;
      const nextCursor = hasMore ? posts[posts.length - 1]?.created_at ?? null : null;

      // 3) Load comments for this batch of posts
      const postIds = posts.map((p: any) => p.id);
      let commentsByPost: Record<string, any[]> = {};

      if (postIds.length > 0) {
        try {
          const { rows: commentRowsRaw } = await pool.query(
            `
            SELECT
              c.id,
              c.post_id,
              c.author_profile_id,
              c.content,
              c.created_at,
              c.updated_at,
              c.is_deleted,

              -- comment author identity
              cu.id              AS author_id,
              cu.username        AS author_username,
              cu.display_name    AS author_display_name,
              cu.avatar_url      AS author_avatar_url,
              cu.primary_address AS author_primary_address,
              cu.eoa_address     AS author_eoa_address
            FROM wall_comments c
            JOIN users cu ON cu.id = c.author_profile_id
            WHERE c.post_id = ANY($1)
              AND c.is_deleted = FALSE
            ORDER BY c.created_at ASC;
            `,
            [postIds]
          );

          const commentRows = (commentRowsRaw || []).map((r: any) =>
            normalizeCommentRow(r, publicBaseUrl)
          );

          commentsByPost = commentRows.reduce((acc: any, row: any) => {
            if (!acc[row.post_id]) acc[row.post_id] = [];
            acc[row.post_id].push(row);
            return acc;
          }, {});
        } catch (commentsErr) {
          console.error("[GET /wall/feed] comments query error", commentsErr);
          commentsByPost = {};
        }
      }

      const payload = posts.map((p: any) => {
        const normalized = normalizeWallRow(p, publicBaseUrl);
        return {
          ...normalized,
          comments: commentsByPost[p.id] || [],
        };
      });

      return res.json({ data: payload, nextCursor, hasMore });
    } catch (err) {
      console.error("[GET /wall/feed] error", err);
      return res.status(500).json({ error: "Failed to load feed" });
    }
  }
);

/**
 * GET /api/profiles/:profileId/wall
 */
router.get(
  "/profiles/:profileId/wall",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      const { profileId } = req.params;
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const viewerUserId = req.user.id;
      const limit = Math.min(Number(req.query.limit) || 20, 50);
      const cursor = req.query.cursor as string | undefined;
      const cursorDate = cursor ? new Date(cursor) : null;

      const publicBaseUrl = getPublicBaseUrl(req);

      const baseParams: any[] = [viewerUserId, profileId];
      let cursorClause = "";
      if (cursorDate) {
        cursorClause = "AND p.created_at < $3";
        baseParams.push(cursorDate.toISOString());
      }

      const limitParamIndex = baseParams.length + 1;
      baseParams.push(limit + 1);

      const postsSql = `
        SELECT
          p.id,
          p.content,
          p.created_at,
          p.updated_at,
          p.is_deleted,
          p.profile_id,
          p.author_profile_id,

          -- author identity
          au.id               AS author_id,
          au.username         AS author_username,
          au.display_name     AS author_display_name,
          au.avatar_url       AS author_avatar_url,
          au.primary_address  AS author_primary_address,
          au.eoa_address      AS author_eoa_address,

          -- likes
          COALESCE(l.like_count, 0)::int AS like_count,
          CASE WHEN my_like.profile_id IS NULL THEN false ELSE true END AS liked_by_me
        FROM wall_posts p
        JOIN users au ON au.id = p.author_profile_id
        LEFT JOIN (
          SELECT post_id, COUNT(*) AS like_count
          FROM wall_post_likes
          GROUP BY post_id
        ) l ON l.post_id = p.id
        LEFT JOIN wall_post_likes my_like
          ON my_like.post_id = p.id
         AND my_like.profile_id = $1  -- viewer
        WHERE p.profile_id = $2
          AND p.is_deleted = FALSE
          ${cursorClause}
        ORDER BY p.created_at DESC
        LIMIT $${limitParamIndex};
      `;

      const { rows: postRowsRaw } = await pool.query(postsSql, baseParams);

      const postRows = postRowsRaw || [];
      const hasMore = postRows.length > limit;
      const posts = hasMore ? postRows.slice(0, limit) : postRows;
      const nextCursor = hasMore ? posts[posts.length - 1]?.created_at ?? null : null;

      // Load comments for this batch of posts
      const postIds = posts.map((p: any) => p.id);
      let commentsByPost: Record<string, any[]> = {};

      if (postIds.length > 0) {
        try {
          const { rows: commentRowsRaw } = await pool.query(
            `
            SELECT
              c.id,
              c.post_id,
              c.author_profile_id,
              c.content,
              c.created_at,
              c.updated_at,
              c.is_deleted,

              cu.id              AS author_id,
              cu.username        AS author_username,
              cu.display_name    AS author_display_name,
              cu.avatar_url      AS author_avatar_url,
              cu.primary_address AS author_primary_address,
              cu.eoa_address     AS author_eoa_address
            FROM wall_comments c
            JOIN users cu ON cu.id = c.author_profile_id
            WHERE c.post_id = ANY($1)
              AND c.is_deleted = FALSE
            ORDER BY c.created_at ASC;
            `,
            [postIds]
          );

          const commentRows = (commentRowsRaw || []).map((r: any) =>
            normalizeCommentRow(r, publicBaseUrl)
          );

          commentsByPost = commentRows.reduce((acc: any, row: any) => {
            if (!acc[row.post_id]) acc[row.post_id] = [];
            acc[row.post_id].push(row);
            return acc;
          }, {});
        } catch (commentsErr) {
          console.error(
            "[GET /profiles/:profileId/wall] comments query error",
            commentsErr
          );
          commentsByPost = {};
        }
      }

      const payload = posts.map((p: any) => {
        const normalized = normalizeWallRow(p, publicBaseUrl);
        return {
          ...normalized,
          comments: commentsByPost[p.id] || [],
        };
      });

      return res.json({ data: payload, nextCursor, hasMore });
    } catch (err) {
      console.error("[GET /profiles/:profileId/wall] error", err);
      return res.status(500).json({ error: "Failed to load wall" });
    }
  }
);

/**
 * POST /api/profiles/:profileId/wall  (create a post on someone's wall)
 */
router.post(
  "/profiles/:profileId/wall",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const { profileId } = req.params;
      const authorUserId = req.user.id;
      const { content } = req.body;

      if (!content || typeof content !== "string" || !content.trim()) {
        return res.status(400).json({ error: "Content is required" });
      }

      const publicBaseUrl = getPublicBaseUrl(req);

      const { rows } = await pool.query(
        `
        WITH inserted AS (
          INSERT INTO wall_posts (profile_id, author_profile_id, content)
          VALUES ($1, $2, $3)
          RETURNING *
        )
        SELECT
          i.id,
          i.profile_id,
          i.author_profile_id,
          i.content,
          i.created_at,
          i.updated_at,
          i.is_deleted,

          u.id              AS author_id,
          u.username        AS author_username,
          u.display_name    AS author_display_name,
          u.avatar_url      AS author_avatar_url,
          u.primary_address AS author_primary_address,
          u.eoa_address     AS author_eoa_address,

          0::int  AS like_count,
          false   AS liked_by_me
        FROM inserted i
        JOIN users u ON u.id = i.author_profile_id;
        `,
        [profileId, authorUserId, content.trim()]
      );

      const row = normalizeWallRow(rows?.[0], publicBaseUrl);
      return res.status(201).json(row);
    } catch (err) {
      console.error("[POST /profiles/:profileId/wall] error", err);
      return res.status(500).json({ error: "Failed to create post" });
    }
  }
);

/**
 * POST /api/wall-posts/:postId/comments
 */
router.post(
  "/wall-posts/:postId/comments",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const { postId } = req.params;
      const authorUserId = req.user.id;
      const { content } = req.body;

      if (!content || typeof content !== "string" || !content.trim()) {
        return res.status(400).json({ error: "Content is required" });
      }

      const publicBaseUrl = getPublicBaseUrl(req);

      const { rows } = await pool.query(
        `
        WITH inserted AS (
          INSERT INTO wall_comments (post_id, author_profile_id, content)
          VALUES ($1, $2, $3)
          RETURNING *
        )
        SELECT
          i.id,
          i.post_id,
          i.author_profile_id,
          i.content,
          i.created_at,
          i.updated_at,
          i.is_deleted,

          u.id              AS author_id,
          u.username        AS author_username,
          u.display_name    AS author_display_name,
          u.avatar_url      AS author_avatar_url,
          u.primary_address AS author_primary_address,
          u.eoa_address     AS author_eoa_address
        FROM inserted i
        JOIN users u ON u.id = i.author_profile_id;
        `,
        [postId, authorUserId, content.trim()]
      );

      const row = normalizeCommentRow(rows?.[0], publicBaseUrl);
      return res.status(201).json(row);
    } catch (err) {
      console.error("[POST /wall-posts/:postId/comments] error", err);
      return res.status(500).json({ error: "Failed to create comment" });
    }
  }
);

// POST /api/wall-posts/:postId/likes  (like)
router.post(
  "/wall-posts/:postId/likes",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const { postId } = req.params;
      const userId = req.user.id;

      await pool.query(
        `
        INSERT INTO wall_post_likes (post_id, profile_id)
        VALUES ($1, $2)
        ON CONFLICT (post_id, profile_id) DO NOTHING;
        `,
        [postId, userId]
      );

      return res.status(204).end();
    } catch (err) {
      console.error("[POST /wall-posts/:postId/likes] error", err);
      return res.status(500).json({ error: "Failed to like post" });
    }
  }
);

// DELETE /api/wall-posts/:postId/likes (unlike)
router.delete(
  "/wall-posts/:postId/likes",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const { postId } = req.params;
      const userId = req.user.id;

      await pool.query(
        `
        DELETE FROM wall_post_likes
        WHERE post_id = $1 AND profile_id = $2;
        `,
        [postId, userId]
      );

      return res.status(204).end();
    } catch (err) {
      console.error("[DELETE /wall-posts/:postId/likes] error", err);
      return res.status(500).json({ error: "Failed to unlike post" });
    }
  }
);

export default router;
