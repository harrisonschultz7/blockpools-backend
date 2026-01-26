// src/routes/profile.ts
import { Router, Response, NextFunction } from "express";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";
import multer from "multer";
import path from "path";
import { PrivyClient } from "@privy-io/server-auth";

// ✅ Metrics: reuse the same SELL-aware “recent trades” logic used by leaderboard.
// NOTE: adjust the import name if your masterMetrics export differs.
// The route below will hard-fail fast with a clear error if the function is missing.
import * as masterMetrics from "../services/metrics/masterMetrics";

const router = Router();

// Store avatar files under /uploads/avatars (relative to compiled server)
const upload = multer({
  dest: path.join(__dirname, "..", "..", "uploads", "avatars"),
});

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

/**
 * Normalize avatar_url values that were previously stored with localhost
 * to the real public host.
 */
function normalizeAvatarUrl(
  avatarUrl: string | null | undefined,
  publicBaseUrl: string
): string | null {
  if (!avatarUrl) return null;

  // If it's already correct or is some external CDN, leave it.
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

/**
 * Optional auth: if Authorization Bearer is present and valid, populate req.user.
 * If missing/invalid, continue as anonymous.
 */
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

    req.user = {
      id: user.id,
      primaryAddress,
      smartAddress,
      eoaAddress,
    };

    return next();
  } catch {
    // silent fail — treat as anonymous
    return next();
  }
}

/* =======================================================================================
   ✅ NEW: Cached Profile Trade History (SELL-aware) so the whole profile page can be cached
   ---------------------------------------------------------------------------------------
   Frontend should call:
     GET /api/profile/address/:address/trades?league=ALL&range=ALL&limit=50&anchorTs=...

   This endpoint is designed to mirror leaderboard “recent trades” semantics:
   - Includes BUY and SELL rows
   - Includes realizedPnlDec / costBasisClosedDec / netPositionDec when available
   - Includes claimByGame mapping for Won line items
   - Safe for CDN caching (short TTL + stale-while-revalidate)
======================================================================================= */

type ApiTimeRange = "ALL" | "D90" | "D30";
type ApiLeague = "ALL" | "MLB" | "NFL" | "NBA" | "NHL" | "EPL" | "UCL";

const VALID_RANGES = new Set<ApiTimeRange>(["ALL", "D90", "D30"]);
const VALID_LEAGUES = new Set<ApiLeague>([
  "ALL",
  "MLB",
  "NFL",
  "NBA",
  "NHL",
  "EPL",
  "UCL",
]);

function normRange(v: any): ApiTimeRange {
  const s = String(v ?? "ALL").toUpperCase().trim();
  return (VALID_RANGES.has(s as ApiTimeRange) ? s : "ALL") as ApiTimeRange;
}

function normLeague(v: any): ApiLeague {
  const s = String(v ?? "ALL").toUpperCase().trim();
  return (VALID_LEAGUES.has(s as ApiLeague) ? s : "ALL") as ApiLeague;
}

function clampInt(n: any, def: number, min: number, max: number): number {
  const x = Number(n);
  if (!Number.isFinite(x)) return def;
  return Math.max(min, Math.min(max, Math.floor(x)));
}

// Small in-process cache (works even without Redis). CDN headers still applied.
const RECENT_CACHE_TTL_MS = 30_000; // 30s in-memory
const recentCache = new Map<
  string,
  { at: number; payload: any }
>();

function setCacheHeaders(res: Response) {
  // CDN-friendly; tweak if you’re fronting with Cloudflare/Vercel/etc.
  res.setHeader(
    "Cache-Control",
    "public, max-age=15, s-maxage=60, stale-while-revalidate=300"
  );
}

router.get(
  "/address/:address/trades",
  authPrivyOptional,
  async (req: AuthedRequest, res: Response) => {
    try {
      const address = String(req.params.address || "").toLowerCase().trim();
      if (!address || !address.startsWith("0x")) {
        return res.status(400).json({ ok: false, error: "address is required" });
      }

      const league = normLeague(req.query.league);
      const range = normRange(req.query.range);
      const limit = clampInt(req.query.limit, 50, 1, 200);

      // Keep your “anchored range” behavior consistent with leaderboard
      const anchorTsRaw = req.query.anchorTs ?? req.query.anchor;
      const anchorTs =
        anchorTsRaw === undefined || anchorTsRaw === null || anchorTsRaw === ""
          ? undefined
          : clampInt(anchorTsRaw, Math.floor(Date.now() / 1000), 0, 10_000_000_000);

      const cacheKey = `recent:${address}:${league}:${range}:${limit}:${anchorTs ?? "na"}`;
      const now = Date.now();

      const cached = recentCache.get(cacheKey);
      if (cached && now - cached.at < RECENT_CACHE_TTL_MS) {
        setCacheHeaders(res);
        return res.json(cached.payload);
      }

      // Resolve the correct function from masterMetrics (avoid guessing your exact export name)
      const fn =
        (masterMetrics as any).getUserRecent ||
        (masterMetrics as any).getUserRecentTrades ||
        (masterMetrics as any).userRecent ||
        null;

      if (typeof fn !== "function") {
        return res.status(500).json({
          ok: false,
          error:
            "metrics function missing: expected masterMetrics.getUserRecent (or compatible export)",
        });
      }

      // IMPORTANT: Your leaderboard route already uses this same logic.
      // We are simply exposing it under /profile so the profile page can be cached.
      const resp = await fn({
        address,
        user: address, // allow either parameter name depending on your implementation
        league,
        range,
        limit,
        anchorTs,
      });

      const payload = {
        ok: true,
        user: address,
        league,
        range,
        limit,
        anchorTs: anchorTs ?? null,
        // standardize to your frontend expectations
        recent: resp?.recent ?? resp?.rows ?? resp?.data ?? [],
        rows: resp?.rows ?? resp?.recent ?? resp?.data ?? [],
        claimByGame: resp?.claimByGame ?? resp?.claimsByGame ?? {},
        asOf: resp?.asOf ?? resp?.anchorTs ?? null,
      };

      recentCache.set(cacheKey, { at: now, payload });
      setCacheHeaders(res);
      return res.json(payload);
    } catch (err: any) {
      console.error("[GET /api/profile/address/:address/trades] error", err);
      return res.status(500).json({
        ok: false,
        error: "Internal server error",
      });
    }
  }
);

/**
 * GET /api/profile/me
 * Returns current user's profile, or 404 if not created yet.
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
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount"
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
 * POST /api/profile
 * Creates or updates current user's profile.
 */
router.post("/", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;

    // From authPrivy: smart wallet preferred as primary
    const primaryAddress = req.user.primaryAddress.toLowerCase();
    const eoaAddress = req.user.eoaAddress ? req.user.eoaAddress.toLowerCase() : null;

    const { username, display_name, x_handle, instagram_handle, avatar_url } =
      req.body || {};

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

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
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
 * Public lookup: { addresses: string[] } -> Profile[]
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
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount"
      FROM users u
      WHERE LOWER(u.primary_address) = ANY($1::text[])
         OR LOWER(u.eoa_address)    = ANY($1::text[])
      `,
      [addrLower]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(
      (result.rows || []).map((r: any) => normalizeProfileRow(r, publicBaseUrl))
    );
  } catch (err) {
    console.error("[POST /api/profile/by-addresses] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * ✅ Recommended: GET /api/profile/by-id?profileId=<users.id>
 * Public lookup by users.id (Privy DID) using query param (avoids path encoding issues).
 * If Authorization bearer is present+valid, includes is_followed_by_me.
 */
router.get("/by-id", authPrivyOptional, async (req: AuthedRequest, res: Response) => {
  try {
    const profileId = typeof req.query.profileId === "string" ? req.query.profileId : "";
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const viewerId = req.user?.id;
    const params = viewerId ? [profileId, viewerId] : [profileId];

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
        (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount"
        ${viewerId ? `,
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $2 AND following_id = u.id
        ) AS "is_followed_by_me"` : ""}
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      params
    );

    if (result.rows.length === 0) return res.status(404).json({ error: "Profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[GET /api/profile/by-id] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/:profileId
 * Backwards compatibility endpoint.
 * Note: DIDs in a path are brittle; prefer /by-id.
 */
router.get("/:profileId", async (req: AuthedRequest, res: Response) => {
  try {
    const { profileId } = req.params;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const viewerId = req.user?.id;
    const params = viewerId ? [profileId, viewerId] : [profileId];

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
        (SELECT COUNT(*) FROM user_follows WHERE follower_id = u.id) AS "followingCount"
        ${viewerId ? `,
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $2 AND following_id = u.id
        ) AS "is_followed_by_me"` : ""}
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      params
    );

    if (result.rows.length === 0) return res.status(404).json({ error: "Profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[GET /api/profile/:profileId] error", err);
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
      `
      DELETE FROM user_follows
      WHERE follower_id = $1 AND following_id = $2
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
router.get("/:profileId/follow-status", authPrivy, async (req: AuthedRequest, res: Response) => {
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
});

/**
 * GET /api/profile/:profileId/followers
 */
router.get("/:profileId/followers", async (req: AuthedRequest, res: Response) => {
  try {
    const { profileId } = req.params;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const target = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (target.rows.length === 0) return res.status(404).json({ error: "Profile not found" });

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
    const data = (result.rows || []).map((r: any) => normalizeProfileRow(r, publicBaseUrl));
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
    if (target.rows.length === 0) return res.status(404).json({ error: "Profile not found" });

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
    const data = (result.rows || []).map((r: any) => normalizeProfileRow(r, publicBaseUrl));
    return res.json({ data });
  } catch (err) {
    console.error("[GET /api/profile/:profileId/following] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
