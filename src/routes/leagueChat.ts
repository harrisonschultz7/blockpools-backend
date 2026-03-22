// src/routes/leagueChat.ts
import { Router, Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";
import { PrivyClient } from "@privy-io/server-auth";
import { pool } from "../db"; // ✅ Added for canonical live ROI query

const router = Router();

const supabase = createClient(
  process.env.SUPABASE_URL!,
  process.env.SUPABASE_SERVICE_ROLE_KEY!
);

const privy = new PrivyClient(
  process.env.PRIVY_APP_ID!,
  process.env.PRIVY_APP_SECRET!
);

const VALID_LEAGUES = ["UCL", "NBA", "NHL", "EPL", "MLB", "NFL"] as const;
const VALID_CHANNELS = ["public", "expert"] as const;
const EXPERT_ROI_THRESHOLD = 10; // percent (i.e. 10 = +10%)
const EXPERT_MIN_TRADES = 3;     // minimum settled BUY trades in window

// ── Auth helper ──────────────────────────────────────────────────────────────

async function getVerifiedUser(
  authHeader: string | undefined
): Promise<{ userId: string; address: string } | null> {
  if (!authHeader?.startsWith("Bearer ")) return null;
  try {
    const token = authHeader.slice(7);
    const claims = await privy.verifyAuthToken(token);
    const { data } = await supabase
      .from("users")
      .select("id, primary_address")
      .eq("id", claims.userId)
      .single();
    if (!data?.primary_address) return null;
    return { userId: data.id, address: data.primary_address.toLowerCase() };
  } catch {
    return null;
  }
}

// ── Canonical live ROI helper ────────────────────────────────────────────────
//
// Matches the profile page formula exactly (masterMetrics.ts / tradeAggRoutes.ts):
//   Total Traded = SUM(gross_in_dec) WHERE type = 'BUY'
//   Total Return = SUM(net_out_dec)  WHERE type IN ('SELL', 'CLAIM')
//   ROI (%) = (TotalReturn / TotalTraded - 1) * 100
//   trades_30d = COUNT of settled BUY rows (is_final = true) in the 30-day window
//
// Window: event timestamp-based (NOT game lock_time), last 30 days.
// League filter applied to games.league.

async function computeLiveRoi(
  address: string,
  league: string
): Promise<{ roi_30d: number | null; trades_30d: number; is_expert: boolean }> {
  const windowSec = Math.floor(Date.now() / 1000) - 30 * 86400;

  const sql = `
    SELECT
      COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0)           AS total_traded,
      COALESCE(SUM(e.net_out_dec::numeric)  FILTER (WHERE e.type IN ('SELL','CLAIM')), 0) AS total_return,
      COUNT(*)                              FILTER (
        WHERE e.type = 'BUY' AND g.is_final = true
      )::int                                                                              AS trades_settled
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE LOWER(e.user_address) = $1
      AND e.timestamp >= $2
      AND g.league = $3
  `;

  const { rows } = await pool.query(sql, [
    address.toLowerCase(),
    windowSec,
    league.toUpperCase(),
  ]);

  const row = rows[0];
  if (!row) return { roi_30d: null, trades_30d: 0, is_expert: false };

  const totalTraded = Number(row.total_traded) || 0;
  const totalReturn = Number(row.total_return) || 0;
  const tradesSettled = Number(row.trades_settled) || 0;

  const roi_30d = totalTraded > 0
    ? (totalReturn / totalTraded - 1) * 100
    : null;

  const is_expert =
    roi_30d !== null &&
    roi_30d >= EXPERT_ROI_THRESHOLD &&
    tradesSettled >= EXPERT_MIN_TRADES;

  return { roi_30d, trades_30d: tradesSettled, is_expert };
}

// ── Bulk ROI for post enrichment ─────────────────────────────────────────────
// Same canonical formula, batched across multiple author addresses for efficiency.

async function computeLiveRoiBulk(
  addresses: string[],
  league: string
): Promise<Map<string, { roi_30d: number | null; trades_30d: number; is_expert: boolean }>> {
  if (!addresses.length) return new Map();

  const windowSec = Math.floor(Date.now() / 1000) - 30 * 86400;

  const sql = `
    SELECT
      LOWER(e.user_address)                                                               AS user_address,
      COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0)           AS total_traded,
      COALESCE(SUM(e.net_out_dec::numeric)  FILTER (WHERE e.type IN ('SELL','CLAIM')), 0) AS total_return,
      COUNT(*)                              FILTER (
        WHERE e.type = 'BUY' AND g.is_final = true
      )::int                                                                              AS trades_settled
    FROM public.user_trade_events e
    JOIN public.games g ON g.game_id = e.game_id
    WHERE LOWER(e.user_address) = ANY($1::text[])
      AND e.timestamp >= $2
      AND g.league = $3
    GROUP BY LOWER(e.user_address)
  `;

  const { rows } = await pool.query(sql, [
    addresses.map((a) => a.toLowerCase()),
    windowSec,
    league.toUpperCase(),
  ]);

  const result = new Map<string, { roi_30d: number | null; trades_30d: number; is_expert: boolean }>();

  for (const row of rows) {
    const totalTraded = Number(row.total_traded) || 0;
    const totalReturn = Number(row.total_return) || 0;
    const tradesSettled = Number(row.trades_settled) || 0;

    const roi_30d = totalTraded > 0
      ? (totalReturn / totalTraded - 1) * 100
      : null;

    const is_expert =
      roi_30d !== null &&
      roi_30d >= EXPERT_ROI_THRESHOLD &&
      tradesSettled >= EXPERT_MIN_TRADES;

    result.set(row.user_address.toLowerCase(), { roi_30d, trades_30d: tradesSettled, is_expert });
  }

  return result;
}

// ── GET /api/league-chat/:league/posts ──────────────────────────────────────

router.get("/:league/posts", async (req: Request, res: Response) => {
  const league = (req.params.league || "").toUpperCase();
  if (!VALID_LEAGUES.includes(league as any))
    return res.status(400).json({ error: "Invalid league" });

  const channel = (req.query.channel as string) || "expert";
  if (!VALID_CHANNELS.includes(channel as any))
    return res.status(400).json({ error: "Invalid channel" });

  const limit = Math.min(Number(req.query.limit) || 25, 50);
  const cursor = req.query.cursor as string | undefined;

  const auth = await getVerifiedUser(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  let query = supabase
    .from("league_chat_posts")
    .select(`
      id, league, channel, content, created_at, updated_at,
      author:users!league_chat_posts_author_id_fkey(
        id, primary_address, username, display_name, avatar_url
      ),
      like_count:league_chat_likes(count),
      comments:league_chat_comments(
        id, content, created_at,
        author:users!league_chat_comments_author_id_fkey(
          id, primary_address, username, display_name, avatar_url
        )
      )
    `)
    .eq("league", league)
    .eq("channel", channel)
    .eq("is_deleted", false)
    .order("created_at", { ascending: false })
    .limit(limit + 1);

  if (cursor) query = query.lt("created_at", cursor);

  const { data: posts, error } = await query;
  if (error) return res.status(500).json({ error: error.message });

  const hasMore = posts!.length > limit;
  const items = hasMore ? posts!.slice(0, limit) : posts!;

  // ✅ Enrich authors with LIVE canonical ROI (same formula as profile page)
  const authorAddresses = [
    ...new Set(items.map((p: any) => p.author?.primary_address?.toLowerCase()).filter(Boolean)),
  ] as string[];

  const roiMap = await computeLiveRoiBulk(authorAddresses, league);

  // Liked-by-me
  const postIds = items.map((p: any) => p.id);
  const { data: myLikes } = await supabase
    .from("league_chat_likes")
    .select("post_id")
    .eq("user_id", auth.userId)
    .in("post_id", postIds);
  const likedSet = new Set((myLikes || []).map((l: any) => l.post_id));

  const enriched = items.map((post: any) => {
    const authorAddr = post.author?.primary_address?.toLowerCase();
    const roi = roiMap.get(authorAddr) ?? { roi_30d: null, trades_30d: 0, is_expert: false };
    return {
      id: post.id,
      league: post.league,
      channel: post.channel,
      content: post.content,
      created_at: post.created_at,
      author_id: post.author?.id,
      author_username: post.author?.username,
      author_display_name: post.author?.display_name,
      author_primary_address: post.author?.primary_address,
      author_avatar_url: post.author?.avatar_url,
      author_roi_30d: roi.roi_30d,
      author_trades_30d: roi.trades_30d,
      author_is_expert: roi.is_expert,
      like_count: post.like_count?.[0]?.count ?? 0,
      liked_by_me: likedSet.has(post.id),
      comments: (post.comments || []).map((c: any) => ({
        id: c.id,
        content: c.content,
        created_at: c.created_at,
        author_id: c.author?.id,
        author_username: c.author?.username,
        author_display_name: c.author?.display_name,
        author_primary_address: c.author?.primary_address,
        author_avatar_url: c.author?.avatar_url,
      })),
    };
  });

  return res.json({ posts: enriched, hasMore });
});

// ── POST /api/league-chat/:league/posts ─────────────────────────────────────

router.post("/:league/posts", async (req: Request, res: Response) => {
  const league = (req.params.league || "").toUpperCase();
  if (!VALID_LEAGUES.includes(league as any))
    return res.status(400).json({ error: "Invalid league" });

  const auth = await getVerifiedUser(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  const { content, channel = "expert" } = req.body;
  if (!content?.trim()) return res.status(400).json({ error: "Content required" });
  if (!VALID_CHANNELS.includes(channel as any))
    return res.status(400).json({ error: "Invalid channel" });
  if (content.length > 500) return res.status(400).json({ error: "Too long" });

  // ✅ Expert gate — live canonical ROI, same formula as profile page
  if (channel === "expert") {
    const roi = await computeLiveRoi(auth.address, league);
    const qualified = roi.is_expert;

    if (!qualified) {
      return res.status(403).json({
        error: `Expert channel requires ≥10% ${league} ROI over the last 30 days with at least ${EXPERT_MIN_TRADES} settled trades.`,
        code: "EXPERT_GATE",
        roi_30d: roi.roi_30d,
        trades_30d: roi.trades_30d,
        threshold: EXPERT_ROI_THRESHOLD,
        league,
      });
    }
  }

  const { data: post, error } = await supabase
    .from("league_chat_posts")
    .insert({ league, channel, author_id: auth.userId, content: content.trim() })
    .select("id, created_at")
    .single();

  if (error) return res.status(500).json({ error: error.message });

  return res.status(201).json({ post });
});

// ── GET /api/league-chat/roi/:address ───────────────────────────────────────
// Returns live canonical ROI for the given address + league (or ALL).
// NOW matches profile page math exactly.

router.get("/roi/:address", async (req: Request, res: Response) => {
  const auth = await getVerifiedUser(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  const address = req.params.address.toLowerCase();
  const league = (req.query.league as string || "ALL").toUpperCase();

  if (league === "ALL") {
    // For ALL leagues, aggregate across all valid leagues
    const windowSec = Math.floor(Date.now() / 1000) - 30 * 86400;

    const sql = `
      SELECT
        COALESCE(SUM(e.gross_in_dec::numeric) FILTER (WHERE e.type = 'BUY'), 0)           AS total_traded,
        COALESCE(SUM(e.net_out_dec::numeric)  FILTER (WHERE e.type IN ('SELL','CLAIM')), 0) AS total_return,
        COUNT(*)                              FILTER (
          WHERE e.type = 'BUY' AND g.is_final = true
        )::int                                                                              AS trades_settled
      FROM public.user_trade_events e
      JOIN public.games g ON g.game_id = e.game_id
      WHERE LOWER(e.user_address) = $1
        AND e.timestamp >= $2
        AND g.league = ANY($3::text[])
    `;

    const { rows } = await pool.query(sql, [
      address,
      windowSec,
      Array.from(VALID_LEAGUES),
    ]);

    const row = rows[0];
    const totalTraded = Number(row?.total_traded) || 0;
    const totalReturn = Number(row?.total_return) || 0;
    const tradesSettled = Number(row?.trades_settled) || 0;

    const roi_30d = totalTraded > 0 ? (totalReturn / totalTraded - 1) * 100 : null;
    const is_expert =
      roi_30d !== null &&
      roi_30d >= EXPERT_ROI_THRESHOLD &&
      tradesSettled >= EXPERT_MIN_TRADES;

    return res.json({ roi_30d, trades_30d: tradesSettled, is_expert, league: "ALL" });
  }

  if (!VALID_LEAGUES.includes(league as any))
    return res.status(400).json({ error: "Invalid league" });

  const roi = await computeLiveRoi(address, league);
  return res.json({ ...roi, league });
});

// ── POST /api/league-chat/posts/:postId/comments ─────────────────────────────

router.post("/posts/:postId/comments", async (req: Request, res: Response) => {
  const auth = await getVerifiedUser(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  const { content } = req.body;
  if (!content?.trim()) return res.status(400).json({ error: "Content required" });
  if (content.length > 300) return res.status(400).json({ error: "Too long" });

  const { data: comment, error } = await supabase
    .from("league_chat_comments")
    .insert({ post_id: req.params.postId, author_id: auth.userId, content: content.trim() })
    .select("id, created_at")
    .single();

  if (error) return res.status(500).json({ error: error.message });

  return res.status(201).json({ comment });
});

// ── POST /api/league-chat/posts/:postId/likes ────────────────────────────────

router.post("/posts/:postId/likes", async (req: Request, res: Response) => {
  const auth = await getVerifiedUser(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  await supabase
    .from("league_chat_likes")
    .upsert({ post_id: req.params.postId, user_id: auth.userId }, { onConflict: "post_id,user_id" });

  return res.json({ ok: true });
});

// ── DELETE /api/league-chat/posts/:postId/likes ──────────────────────────────

router.delete("/posts/:postId/likes", async (req: Request, res: Response) => {
  const auth = await getVerifiedUser(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  await supabase
    .from("league_chat_likes")
    .delete()
    .eq("post_id", req.params.postId)
    .eq("user_id", auth.userId);

  return res.json({ ok: true });
});

// ── POST /api/league-chat/refresh-roi ───────────────────────────────────────
// This endpoint is now a no-op since ROI is computed live, but kept for
// backwards compatibility with any cron jobs hitting it.

router.post("/refresh-roi", async (req: Request, res: Response) => {
  const token = req.headers.authorization?.replace("Bearer ", "");
  if (token !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  // ROI is now computed live from user_trade_events — no snapshot to refresh.
  return res.json({ ok: true, message: "ROI is now computed live; no snapshot refresh needed.", refreshed_at: new Date().toISOString() });
});

export default router;