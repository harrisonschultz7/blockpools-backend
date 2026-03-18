// src/routes/leagueChat.ts
import { Router, Response } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

const VALID_LEAGUES = new Set(["UCL", "NBA", "NHL", "EPL", "MLB", "NFL"]);

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

function normalizeRow(row: any, publicBaseUrl: string) {
  if (!row) return row;
  return {
    ...row,
    author_avatar_url: normalizeAvatarUrl(row.author_avatar_url, publicBaseUrl),
  };
}

/**
 * GET /api/league-chat/:league/feed
 * Returns paginated posts for a league channel.
 */
router.get(
  "/league-chat/:league/feed",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const league = String(req.params.league || "").toUpperCase();
      if (!VALID_LEAGUES.has(league)) {
        return res.status(400).json({ error: "Invalid league" });
      }

      const viewerUserId = req.user.id;
      const limit = Math.min(Number(req.query.limit) || 30, 100);
      const cursor = req.query.cursor as string | undefined;
      const cursorDate = cursor ? new Date(cursor) : null;
      const publicBaseUrl = getPublicBaseUrl(req);

      const baseParams: any[] = [viewerUserId, league];
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
          p.league,
          p.content,
          p.created_at,
          p.updated_at,
          p.is_deleted,

          au.id               AS author_id,
          au.username         AS author_username,
          au.display_name     AS author_display_name,
          au.avatar_url       AS author_avatar_url,
          au.primary_address  AS author_primary_address,

          COALESCE(l.like_count, 0)::int AS like_count,
          CASE WHEN my_like.profile_id IS NULL THEN false ELSE true END AS liked_by_me
        FROM league_chat_posts p
        JOIN users au ON au.id = p.author_id
        LEFT JOIN (
          SELECT post_id, COUNT(*) AS like_count
          FROM league_chat_likes
          GROUP BY post_id
        ) l ON l.post_id = p.id
        LEFT JOIN league_chat_likes my_like
          ON my_like.post_id = p.id
         AND my_like.profile_id = $1
        WHERE p.league = $2
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

      const postIds = posts.map((p: any) => p.id);
      let commentsByPost: Record<string, any[]> = {};

      if (postIds.length > 0) {
        try {
          const { rows: commentRowsRaw } = await pool.query(
            `
            SELECT
              c.id,
              c.post_id,
              c.content,
              c.created_at,
              c.is_deleted,

              cu.id              AS author_id,
              cu.username        AS author_username,
              cu.display_name    AS author_display_name,
              cu.avatar_url      AS author_avatar_url,
              cu.primary_address AS author_primary_address
            FROM league_chat_comments c
            JOIN users cu ON cu.id = c.author_id
            WHERE c.post_id = ANY($1)
              AND c.is_deleted = FALSE
            ORDER BY c.created_at ASC;
            `,
            [postIds]
          );

          const commentRows = (commentRowsRaw || []).map((r: any) =>
            normalizeRow(r, publicBaseUrl)
          );

          commentsByPost = commentRows.reduce((acc: any, row: any) => {
            if (!acc[row.post_id]) acc[row.post_id] = [];
            acc[row.post_id].push(row);
            return acc;
          }, {});
        } catch (err) {
          console.error("[GET /league-chat/:league/feed] comments error", err);
          commentsByPost = {};
        }
      }

      const payload = posts.map((p: any) => {
        const normalized = normalizeRow(p, publicBaseUrl);
        return { ...normalized, comments: commentsByPost[p.id] || [] };
      });

      return res.json({ data: payload, nextCursor, hasMore });
    } catch (err) {
      console.error("[GET /league-chat/:league/feed] error", err);
      return res.status(500).json({ error: "Failed to load league chat" });
    }
  }
);

/**
 * POST /api/league-chat/:league/posts
 * Create a new post in a league channel.
 */
router.post(
  "/league-chat/:league/posts",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const league = String(req.params.league || "").toUpperCase();
      if (!VALID_LEAGUES.has(league)) {
        return res.status(400).json({ error: "Invalid league" });
      }

      const authorUserId = req.user.id;
      const { content } = req.body;

      if (!content || typeof content !== "string" || !content.trim()) {
        return res.status(400).json({ error: "Content is required" });
      }

      if (content.trim().length > 500) {
        return res.status(400).json({ error: "Message too long (max 500 chars)" });
      }

      const publicBaseUrl = getPublicBaseUrl(req);

      const { rows } = await pool.query(
        `
        WITH inserted AS (
          INSERT INTO league_chat_posts (league, author_id, content)
          VALUES ($1, $2, $3)
          RETURNING *
        )
        SELECT
          i.id,
          i.league,
          i.content,
          i.created_at,
          i.updated_at,
          i.is_deleted,

          u.id              AS author_id,
          u.username        AS author_username,
          u.display_name    AS author_display_name,
          u.avatar_url      AS author_avatar_url,
          u.primary_address AS author_primary_address,

          0::int  AS like_count,
          false   AS liked_by_me
        FROM inserted i
        JOIN users u ON u.id = i.author_id;
        `,
        [league, authorUserId, content.trim()]
      );

      const row = normalizeRow(rows?.[0], publicBaseUrl);
      return res.status(201).json({ ...row, comments: [] });
    } catch (err) {
      console.error("[POST /league-chat/:league/posts] error", err);
      return res.status(500).json({ error: "Failed to create post" });
    }
  }
);

/**
 * POST /api/league-chat/posts/:postId/comments
 */
router.post(
  "/league-chat/posts/:postId/comments",
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

      if (content.trim().length > 300) {
        return res.status(400).json({ error: "Comment too long (max 300 chars)" });
      }

      const publicBaseUrl = getPublicBaseUrl(req);

      const { rows } = await pool.query(
        `
        WITH inserted AS (
          INSERT INTO league_chat_comments (post_id, author_id, content)
          VALUES ($1, $2, $3)
          RETURNING *
        )
        SELECT
          i.id,
          i.post_id,
          i.content,
          i.created_at,
          i.is_deleted,

          u.id              AS author_id,
          u.username        AS author_username,
          u.display_name    AS author_display_name,
          u.avatar_url      AS author_avatar_url,
          u.primary_address AS author_primary_address
        FROM inserted i
        JOIN users u ON u.id = i.author_id;
        `,
        [postId, authorUserId, content.trim()]
      );

      const row = normalizeRow(rows?.[0], publicBaseUrl);
      return res.status(201).json(row);
    } catch (err) {
      console.error("[POST /league-chat/posts/:postId/comments] error", err);
      return res.status(500).json({ error: "Failed to create comment" });
    }
  }
);

/**
 * POST /api/league-chat/posts/:postId/likes
 */
router.post(
  "/league-chat/posts/:postId/likes",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const { postId } = req.params;
      const userId = req.user.id;

      await pool.query(
        `
        INSERT INTO league_chat_likes (post_id, profile_id)
        VALUES ($1, $2)
        ON CONFLICT (post_id, profile_id) DO NOTHING;
        `,
        [postId, userId]
      );

      return res.status(204).end();
    } catch (err) {
      console.error("[POST /league-chat/posts/:postId/likes] error", err);
      return res.status(500).json({ error: "Failed to like post" });
    }
  }
);

/**
 * DELETE /api/league-chat/posts/:postId/likes
 */
router.delete(
  "/league-chat/posts/:postId/likes",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const { postId } = req.params;
      const userId = req.user.id;

      await pool.query(
        `
        DELETE FROM league_chat_likes
        WHERE post_id = $1 AND profile_id = $2;
        `,
        [postId, userId]
      );

      return res.status(204).end();
    } catch (err) {
      console.error("[DELETE /league-chat/posts/:postId/likes] error", err);
      return res.status(500).json({ error: "Failed to unlike post" });
    }
  }
);

export default router;