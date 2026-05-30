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

// ── 9) User journeys: step-indexed path analysis (origin → … → drop-off) ─────
//
// Per session we build the ordered sequence of "milestone" steps (each page
// category + the key in-page actions), collapse consecutive repeats, cap at
// maxSteps, and append a terminal node: 'Exit' if the journey actually ended,
// 'More…' if it was truncated. We then aggregate every session into a
// step-indexed graph (node key = "<step> <label>") so the same label at
// different depths is a distinct node — that gives the left-to-right flow with
// explicit drop-off at each stage. Also returns the most common full paths.
router.get(
  "/journey",
  guarded(async (req, res) => {
    const since = sinceFromDays(req);
    const connected = connectedParam(req);
    const device = deviceParam(req);
    let maxSteps = Number(req.query.maxSteps);
    if (!Number.isFinite(maxSteps)) maxSteps = 5;
    maxSteps = Math.min(Math.max(Math.trunc(maxSteps), 2), 8);

    const { rows } = await pool.query(
      `select session_id, step_label
       from (
         select e.session_id, e.event_ts, e.id,
           case
             when e.event_type = 'page_view' then
               case e.page_category
                 when 'landing'      then 'Landing'
                 when 'marketplace'  then 'Marketplace'
                 when 'market_detail' then 'Market'
                 when 'leaderboard'  then 'Leaderboard'
                 when 'profile'      then 'Profile'
                 when 'positions'    then 'Positions'
                 when 'groups'       then 'Groups'
                 else 'Other page'
               end
             when e.action_category = 'market_open'   then 'Open market'
             when e.action_category = 'league_filter' then 'League filter'
             when e.action_category = 'trade_intent'  then 'Trade panel'
             when e.action_category = 'auth'          then 'Sign-in'
             else null
           end as step_label
         from public.analytics_events_enriched e
         join public.analytics_session_summary ss on ss.session_id = e.session_id
         where e.event_ts >= $1
           and ($2::boolean is null or ss.connected = $2)
           and ($3::text is null or e.device = $3)
       ) t
       where step_label is not null
       order by session_id, event_ts, id`,
      [since, connected, device],
    );

    // Group rows into ordered per-session paths, collapsing consecutive repeats.
    const sessions: string[][] = [];
    let curId: string | null = null;
    let cur: string[] = [];
    for (const r of rows as { session_id: string; step_label: string }[]) {
      if (r.session_id !== curId) {
        if (cur.length) sessions.push(cur);
        cur = [];
        curId = r.session_id;
      }
      if (cur[cur.length - 1] !== r.step_label) cur.push(r.step_label);
    }
    if (cur.length) sessions.push(cur);

    const totalSessions = sessions.length;
    const keyOf = (step: number, label: string) => `${step} ${label}`;

    const nodeMap = new Map<
      string,
      { key: string; step: number; label: string; count: number }
    >();
    const linkMap = new Map<
      string,
      { source: string; target: string; value: number }
    >();
    const pathMap = new Map<string, { steps: string[]; count: number }>();

    for (const path of sessions) {
      const real = path.slice(0, maxSteps);
      const terminal = path.length > maxSteps ? "More…" : "Exit";
      const full = [...real, terminal];

      full.forEach((label, i) => {
        const k = keyOf(i, label);
        const nd = nodeMap.get(k) || { key: k, step: i, label, count: 0 };
        nd.count++;
        nodeMap.set(k, nd);
      });
      for (let i = 0; i < full.length - 1; i++) {
        const s = keyOf(i, full[i]);
        const t = keyOf(i + 1, full[i + 1]);
        const lk = `${s}${t}`;
        const e = linkMap.get(lk) || { source: s, target: t, value: 0 };
        e.value++;
        linkMap.set(lk, e);
      }
      const ps = full.join(" → ");
      const pe = pathMap.get(ps) || { steps: full, count: 0 };
      pe.count++;
      pathMap.set(ps, pe);
    }

    const pct = (c: number) =>
      totalSessions ? Math.round((c / totalSessions) * 1000) / 10 : 0;

    res.json({
      since,
      totalSessions,
      maxSteps,
      nodes: [...nodeMap.values()].map((nd) => ({ ...nd, pct: pct(nd.count) })),
      links: [...linkMap.values()],
      topPaths: [...pathMap.values()]
        .sort((a, b) => b.count - a.count)
        .slice(0, 12)
        .map((p) => ({ steps: p.steps, count: p.count, pct: pct(p.count) })),
    });
  }),
);

// ── 10) Single session trail (drill-down detail) ─────────────────────────────
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
