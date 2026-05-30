// src/routes/analyticsAdmin.ts
//
// Read-only aggregation API behind the in-house analytics dashboard.
// All endpoints are GET, gated by the same x-admin-key convention as
// adminSweeps, and run parameterized SQL over the analytics_* views.
//
// Mount in server.ts:
//   import analyticsAdminRouter from "./routes/analyticsAdmin";
//   app.use("/api/analytics/admin", analyticsAdminRouter);
//
// Common query params (all optional):
//   days=30        window size (1..365), default 30
//   connected=1|0  filter to connected-wallet vs anonymous sessions
//   device=mobile|tablet|desktop

import { Router, Request, Response } from "express";
import { pool } from "../db";

const router = Router();

function requireAdminKey(req: Request): boolean {
  const expected = process.env.ADMIN_API_KEY;
  if (!expected) throw new Error("ADMIN_API_KEY is not set in environment");
  const got = req.header("x-admin-key");
  return !!got && got === expected;
}

// ── param helpers ────────────────────────────────────────────────────────────
function sinceFromDays(req: Request): string {
  let days = Number(req.query.days);
  if (!Number.isFinite(days)) days = 30;
  days = Math.min(Math.max(Math.trunc(days), 1), 365);
  return new Date(Date.now() - days * 86_400_000).toISOString();
}

function connectedParam(req: Request): boolean | null {
  const v = String(req.query.connected ?? "").toLowerCase();
  if (v === "1" || v === "true") return true;
  if (v === "0" || v === "false") return false;
  return null;
}

function deviceParam(req: Request): string | null {
  const v = String(req.query.device ?? "").toLowerCase();
  return ["mobile", "tablet", "desktop"].includes(v) ? v : null;
}

// Wraps a handler with auth + error handling (admin read routes never throw raw).
function guarded(
  fn: (req: Request, res: Response) => Promise<unknown>,
): (req: Request, res: Response) => void {
  return (req, res) => {
    let authed = false;
    try {
      authed = requireAdminKey(req);
    } catch (err: any) {
      return res.status(500).json({ error: String(err?.message || err) });
    }
    if (!authed) return res.status(401).json({ error: "Unauthorized" });
    fn(req, res).catch((err) => {
      console.error("[analytics/admin] query failed", err);
      res.status(500).json({ error: "Query failed" });
    });
  };
}

// ── 1) Overview totals ───────────────────────────────────────────────────────
router.get(
  "/overview",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const [totals, byDevice, byLocale, byPage] = await Promise.all([
      pool.query(
        `select
           count(*) filter (where event_type='page_view') as page_views,
           count(*) filter (where event_type='click')     as clicks,
           count(distinct session_id)                      as sessions,
           count(distinct wallet_address)                  as wallets
         from public.analytics_events_enriched
         where event_ts >= $1`,
        [since],
      ),
      pool.query(
        `select coalesce(device,'unknown') as device, count(distinct session_id) as sessions
         from public.analytics_events_enriched where event_ts >= $1 group by 1 order by 2 desc`,
        [since],
      ),
      pool.query(
        `select coalesce(locale,'unknown') as locale, count(distinct session_id) as sessions
         from public.analytics_events_enriched where event_ts >= $1 group by 1 order by 2 desc`,
        [since],
      ),
      pool.query(
        `select page_category,
                count(*) filter (where event_type='page_view') as views,
                count(distinct session_id) as sessions
         from public.analytics_events_enriched where event_ts >= $1 group by 1 order by 2 desc`,
        [since],
      ),
    ]);
    res.json({
      since,
      totals: totals.rows[0],
      by_device: byDevice.rows,
      by_locale: byLocale.rows,
      by_page_category: byPage.rows,
    });
  }),
);

// ── 2) Acquisition funnel ────────────────────────────────────────────────────
router.get(
  "/funnel",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const connected = connectedParam(req);
    const device = deviceParam(req);
    const { rows } = await pool.query(
      `select
         count(*)                                    as sessions,
         count(*) filter (where reached_marketplace) as reached_marketplace,
         count(*) filter (where reached_market)      as reached_market,
         count(*) filter (where used_league_filter)  as used_league_filter,
         count(*) filter (where trade_intent)        as opened_trade_panel,
         count(*) filter (where clicked_auth)        as clicked_auth,
         count(*) filter (where connected)           as connected_wallet
       from public.analytics_session_summary
       where started_at >= $1
         and ($2::boolean is null or connected = $2)
         and ($3::text is null or device = $3)`,
      [since, connected, device],
    );
    res.json({ since, funnel: rows[0] });
  }),
);

// ── 3) Page-to-page transitions (Sankey edges) ───────────────────────────────
router.get(
  "/transitions",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const connected = connectedParam(req);
    const device = deviceParam(req);
    const { rows } = await pool.query(
      `with seq as (
         select e.session_id, ss.connected, ss.device, e.page_category, e.event_ts, e.id,
                lead(e.page_category) over (
                  partition by e.session_id order by e.event_ts, e.id
                ) as next_category
         from public.analytics_events_enriched e
         join public.analytics_session_summary ss on ss.session_id = e.session_id
         where e.event_type = 'page_view' and e.event_ts >= $1
       )
       select page_category as from_category, next_category as to_category, count(*)::int as n
       from seq
       where next_category is not null and next_category <> page_category
         and ($2::boolean is null or connected = $2)
         and ($3::text is null or device = $3)
       group by 1, 2
       order by n desc`,
      [since, connected, device],
    );
    res.json({ since, edges: rows });
  }),
);

// ── 4) Pages: views, sessions, avg engaged time ──────────────────────────────
router.get(
  "/pages",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const { rows } = await pool.query(
      `select page_category, page_path,
              count(*) filter (where event_type='page_view') as views,
              count(distinct session_id) as sessions,
              round(avg(engaged_ms) filter (where engaged_ms is not null) / 1000.0, 1) as avg_sec
       from public.analytics_events_enriched
       where event_ts >= $1
       group by 1, 2
       order by views desc
       limit 100`,
      [since],
    );
    res.json({ since, pages: rows });
  }),
);

// ── 5) Top clicked buttons (by action category + name) ───────────────────────
router.get(
  "/top-buttons",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const connected = connectedParam(req);
    const device = deviceParam(req);
    const { rows } = await pool.query(
      `select action_category, name,
              count(*)::int as clicks,
              count(distinct session_id)::int as sessions
       from public.analytics_events_enriched
       where event_type='click' and event_ts >= $1
         and ($2::boolean is null or (wallet_address is not null) = $2)
         and ($3::text is null or device = $3)
       group by 1, 2
       order by clicks desc
       limit 100`,
      [since, connected, device],
    );
    res.json({ since, buttons: rows });
  }),
);

// ── 6) Top markets (by views) ────────────────────────────────────────────────
router.get(
  "/top-markets",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const { rows } = await pool.query(
      `select market_address,
              count(*) filter (where event_type='page_view') as views,
              count(distinct session_id) as sessions,
              round(avg(engaged_ms) filter (where engaged_ms is not null) / 1000.0, 1) as avg_sec
       from public.analytics_events_enriched
       where market_address is not null and event_ts >= $1
       group by 1
       order by views desc
       limit 100`,
      [since],
    );
    res.json({ since, markets: rows });
  }),
);

// ── 7) Heatmap: click coordinates for one page ───────────────────────────────
router.get(
  "/heatmap",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const device = deviceParam(req);
    const page = String(req.query.page ?? "").slice(0, 300);
    if (!page) return res.status(400).json({ error: "page param required" });
    const { rows } = await pool.query(
      `select x, y, vw, vh, name, action_category
       from public.analytics_clicks
       where page_path = $1 and event_ts >= $2
         and ($3::text is null or device = $3)
       limit 5000`,
      [page, since, device],
    );
    res.json({ since, page, points: rows });
  }),
);

// ── 8) Sessions list (drill-down index) ──────────────────────────────────────
router.get(
  "/sessions",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const connected = connectedParam(req);
    const device = deviceParam(req);
    const { rows } = await pool.query(
      `select session_id, connected, wallet_address, device, locale,
              started_at, last_seen_at, round(session_seconds)::int as session_seconds,
              page_views, clicks, markets_viewed,
              reached_market, trade_intent, clicked_auth
       from public.analytics_session_summary
       where started_at >= $1
         and ($2::boolean is null or connected = $2)
         and ($3::text is null or device = $3)
       order by started_at desc
       limit 200`,
      [since, connected, device],
    );
    res.json({ since, sessions: rows });
  }),
);

// ── 9) Single session trail (drill-down detail) ──────────────────────────────
router.get(
  "/session/:id",
  guarded(async (req, res) => {
    const id = String(req.params.id).slice(0, 100);
    const { rows } = await pool.query(
      `select event_ts, event_type, action_category, page_category, page_path,
              name, market_address, engaged_ms, metadata
       from public.analytics_events_enriched
       where session_id = $1
       order by event_ts asc, id asc
       limit 1000`,
      [id],
    );
    res.json({ session_id: id, events: rows });
  }),
);

export default router;
