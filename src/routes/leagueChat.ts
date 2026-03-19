// src/routes/leagueChat.ts  — additions / replacements for expert channel support
// ─────────────────────────────────────────────────────────────────────────────
// Key changes vs original:
//   • GET  /api/league-chat/:league/posts?channel=public|expert
//   • POST /api/league-chat/:league/posts  — enforces expert gate for channel=expert
//   • GET  /api/league-chat/roi/:address   — lightweight ROI lookup for a single user
//   • Cron endpoint: POST /api/league-chat/refresh-roi  (service-role only)
//
// The existing comment / like routes are unchanged — channel is inherited from
// the parent post so no extra gating is needed there.
// ─────────────────────────────────────────────────────────────────────────────

import { Router, Request, Response } from "express";
import { createClient } from "@supabase/supabase-js";
import { PrivyClient } from "@privy-io/server-auth";

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
const EXPERT_ROI_THRESHOLD = 10; // percent
const EXPERT_MIN_TRADES = 3;     // must have at least 3 settled trades

// ── Auth helper ──────────────────────────────────────────────────────────────

async function getVerifiedUserId(
  authHeader: string | undefined
): Promise<{ userId: string; address: string } | null> {
  if (!authHeader?.startsWith("Bearer ")) return null;
  try {
    const token = authHeader.slice(7);
    const claims = await privy.verifyAuthToken(token);
    // Fetch primary_address from users table
    const { data } = await supabase
      .from("users")
      .select("id, primary_address")
      .eq("id", claims.userId)
      .single();
    if (!data) return null;
    return { userId: data.id, address: data.primary_address };
  } catch {
    return null;
  }
}

// ── ROI helper ───────────────────────────────────────────────────────────────

async function getUserRoi(address: string): Promise<{
  roi_30d: number;
  trades_30d: number;
  is_expert: boolean;
} | null> {
  const { data } = await supabase
    .from("user_roi_snapshots")
    .select("roi_30d, trades_30d, is_expert")
    .eq("user_address", address)
    .single();
  return data ?? null;
}

// ── GET /api/league-chat/:league/posts ──────────────────────────────────────
// Query params:
//   channel  = 'public' | 'expert'   (default: 'public')
//   cursor   = ISO timestamp for pagination
//   limit    = number (max 50)

router.get("/:league/posts", async (req: Request, res: Response) => {
  const league = (req.params.league || "").toUpperCase();
  if (!VALID_LEAGUES.includes(league as any))
    return res.status(400).json({ error: "Invalid league" });

  const channel = (req.query.channel as string) || "public";
  if (!VALID_CHANNELS.includes(channel as any))
    return res.status(400).json({ error: "Invalid channel" });

  const limit = Math.min(Number(req.query.limit) || 25, 50);
  const cursor = req.query.cursor as string | undefined;

  // Verify auth (expert channel still requires auth to read)
  const auth = await getVerifiedUserId(req.headers.authorization);
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

  if (cursor) {
    query = query.lt("created_at", cursor);
  }

  const { data: posts, error } = await query;
  if (error) return res.status(500).json({ error: error.message });

  const hasMore = posts!.length > limit;
  const items = hasMore ? posts!.slice(0, limit) : posts!;

  // Enrich with ROI for each unique author
  const authorAddresses = [
    ...new Set(items.map((p: any) => p.author?.primary_address).filter(Boolean)),
  ] as string[];

  const { data: roiRows } = await supabase
    .from("user_roi_snapshots")
    .select("user_address, roi_30d, trades_30d, is_expert")
    .in("user_address", authorAddresses);

  const roiMap = Object.fromEntries(
    (roiRows || []).map((r: any) => [r.user_address, r])
  );

  // Check which posts the current user has liked
  const postIds = items.map((p: any) => p.id);
  const { data: myLikes } = await supabase
    .from("league_chat_likes")
    .select("post_id")
    .eq("user_id", auth.userId)
    .in("post_id", postIds);
  const likedSet = new Set((myLikes || []).map((l: any) => l.post_id));

  const enriched = items.map((post: any) => {
    const authorAddr = post.author?.primary_address;
    const roi = roiMap[authorAddr] ?? { roi_30d: null, trades_30d: 0, is_expert: false };
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
      // ROI badge fields
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

  const auth = await getVerifiedUserId(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  const { content, channel = "public" } = req.body;
  if (!content?.trim()) return res.status(400).json({ error: "Content required" });
  if (!VALID_CHANNELS.includes(channel as any))
    return res.status(400).json({ error: "Invalid channel" });
  if (content.length > 500) return res.status(400).json({ error: "Too long" });

  // ── Expert gate ─────────────────────────────────────────────────────────
  if (channel === "expert") {
    const roi = await getUserRoi(auth.address);
    const qualified =
      roi &&
      roi.roi_30d >= EXPERT_ROI_THRESHOLD &&
      roi.trades_30d >= EXPERT_MIN_TRADES;

    if (!qualified) {
      return res.status(403).json({
        error: "Expert channel requires ≥10% ROI over the last 30 days with at least 3 settled trades.",
        code: "EXPERT_GATE",
        roi_30d: roi?.roi_30d ?? null,
        trades_30d: roi?.trades_30d ?? 0,
        threshold: EXPERT_ROI_THRESHOLD,
      });
    }
  }

  const { data: post, error } = await supabase
    .from("league_chat_posts")
    .insert({
      league,
      channel,
      author_id: auth.userId,
      content: content.trim(),
    })
    .select("id, created_at")
    .single();

  if (error) return res.status(500).json({ error: error.message });

  return res.status(201).json({ post });
});

// ── GET /api/league-chat/roi/:address ───────────────────────────────────────
// Lightweight endpoint the frontend can hit to get the current user's ROI
// (used to decide whether to show the "post in Expert" option).

router.get("/roi/:address", async (req: Request, res: Response) => {
  const auth = await getVerifiedUserId(req.headers.authorization);
  if (!auth) return res.status(401).json({ error: "Unauthorized" });

  // Users can only look up their own ROI (or any address if you want it public)
  const address = req.params.address.toLowerCase();

  const roi = await getUserRoi(address);
  if (!roi) return res.json({ roi_30d: null, trades_30d: 0, is_expert: false });

  return res.json(roi);
});

// ── POST /api/league-chat/refresh-roi ───────────────────────────────────────
// Called by a cron job (or Supabase Edge Function scheduled trigger).
// Secured by a shared secret passed as Bearer token.

router.post("/refresh-roi", async (req: Request, res: Response) => {
  const token = req.headers.authorization?.replace("Bearer ", "");
  if (token !== process.env.CRON_SECRET) {
    return res.status(401).json({ error: "Unauthorized" });
  }

  const { error } = await supabase.rpc("refresh_roi_snapshots");
  if (error) return res.status(500).json({ error: error.message });

  return res.json({ ok: true, refreshed_at: new Date().toISOString() });
});

export default router;