-- src/db/migrations/2026-05-31_analytics_visitor_and_admin_exclude.sql
--
-- 1) Adds persistent visitor_id (localStorage on the client) so we can count
--    true unique visitors and detect returning visitors across sessions.
-- 2) Recreates the analytics_* views so the base enriched view EXCLUDES the
--    /admin dashboard's own traffic (so your usage doesn't show up as sessions),
--    and exposes visitor_id.
--
-- Because the dependent views are rebuilt off analytics_events_enriched, we drop
-- the chain with CASCADE and recreate all of them in order. Re-runnable.
--
-- Apply: paste into the Supabase SQL editor and run (after the earlier
-- analytics migrations).

-- ── 1) Column ────────────────────────────────────────────────────────────────
ALTER TABLE public.analytics_events
  ADD COLUMN IF NOT EXISTS visitor_id text;

CREATE INDEX IF NOT EXISTS analytics_events_visitor_idx
  ON public.analytics_events (visitor_id);

-- ── 2) Rebuild the view chain ────────────────────────────────────────────────
DROP VIEW IF EXISTS public.analytics_events_enriched CASCADE;

-- Enriched event stream (now excludes /admin self-traffic; visitor_id via e.*).
CREATE VIEW public.analytics_events_enriched AS
SELECT
  e.*,
  COALESCE(e.occurred_at, e.created_at)                       AS event_ts,
  CASE
    WHEN e.page_path = '/'                      THEN 'landing'
    WHEN e.page_path IN ('/app', '/m')          THEN 'marketplace'
    WHEN e.page_path LIKE '/markets/%'          THEN 'market_detail'
    WHEN e.page_path = '/leaderboard'           THEN 'leaderboard'
    WHEN e.page_path LIKE '/profile%'           THEN 'profile'
    WHEN e.page_path = '/positions'             THEN 'positions'
    WHEN e.page_path LIKE '/groups%'            THEN 'groups'
    ELSE 'other'
  END                                                         AS page_category,
  CASE
    WHEN e.page_path LIKE '/markets/%'
      THEN lower(split_part(e.page_path, '/', 3))
    ELSE NULL
  END                                                         AS market_address,
  CASE
    WHEN e.event_type = 'page_view' THEN 'page_view'
    WHEN e.metadata->>'href' IN ('/app','/m','/leaderboard','/profile','/positions','/groups')
      THEN 'navigation'
    WHEN e.name ~* '^(home|mlb|nba|nhl|nfl|epl|ucl|copa|mundial|champions|wnba|ncaa)'
      THEN 'league_filter'
    WHEN e.name ILIKE 'Buy %' OR e.name ~ '\$0\.' OR e.name IN ('+5','+10','+25','+100','Max','Máx')
      THEN 'trade_intent'
    WHEN e.name ILIKE '%sign in%' OR e.name IN ('Google','Apple','Email','Continuar','Continue')
      THEN 'auth'
    WHEN e.name IN ('Later','Más tarde','Close','Cerrar','close modal','Dismiss','X')
      THEN 'dismiss'
    WHEN (e.metadata->>'tag') = 'div' AND (e.metadata->>'role') = 'button' AND e.name ~ '\$0\.'
      THEN 'market_open'
    ELSE 'other_click'
  END                                                         AS action_category,
  CASE
    WHEN e.event_type = 'page_view'
      THEN LEAST(e.duration_ms, 600000)
    ELSE NULL
  END                                                         AS engaged_ms
FROM public.analytics_events e
WHERE e.session_id NOT IN ('x', 'vps-test')
  AND e.page_path NOT LIKE '/admin%';   -- exclude the analytics dashboard itself

-- Per-session rollup (now carries visitor_id).
CREATE VIEW public.analytics_session_summary AS
SELECT
  session_id,
  max(visitor_id)                                            AS visitor_id,
  bool_or(wallet_address IS NOT NULL)                        AS connected,
  max(wallet_address)                                        AS wallet_address,
  max(device)                                                AS device,
  max(locale)                                                AS locale,
  min(event_ts)                                              AS started_at,
  max(event_ts)                                              AS last_seen_at,
  extract(epoch FROM (max(event_ts) - min(event_ts)))        AS session_seconds,
  count(*) FILTER (WHERE event_type = 'page_view')           AS page_views,
  count(*) FILTER (WHERE event_type = 'click')               AS clicks,
  count(DISTINCT page_path)                                  AS distinct_pages,
  count(DISTINCT market_address) FILTER (WHERE market_address IS NOT NULL)
                                                             AS markets_viewed,
  bool_or(page_category = 'marketplace')                     AS reached_marketplace,
  bool_or(page_category = 'market_detail')                   AS reached_market,
  bool_or(action_category = 'trade_intent')                 AS trade_intent,
  bool_or(action_category = 'auth')                         AS clicked_auth,
  bool_or(action_category = 'league_filter')                AS used_league_filter
FROM public.analytics_events_enriched
GROUP BY session_id;

-- Page-to-page transitions (Sankey edges).
CREATE VIEW public.analytics_transitions AS
WITH seq AS (
  SELECT
    e.session_id,
    ss.connected,
    ss.device,
    e.page_category,
    e.event_ts,
    e.id,
    lead(e.page_category) OVER (
      PARTITION BY e.session_id ORDER BY e.event_ts, e.id
    ) AS next_category
  FROM public.analytics_events_enriched e
  JOIN public.analytics_session_summary ss ON ss.session_id = e.session_id
  WHERE e.event_type = 'page_view'
)
SELECT
  page_category AS from_category,
  next_category AS to_category,
  connected,
  device,
  count(*)      AS n
FROM seq
WHERE next_category IS NOT NULL
  AND next_category <> page_category
GROUP BY 1, 2, 3, 4;

-- Acquisition funnel (single row).
CREATE VIEW public.analytics_funnel AS
SELECT
  count(*)                                          AS sessions,
  count(*) FILTER (WHERE reached_marketplace)       AS reached_marketplace,
  count(*) FILTER (WHERE reached_market)            AS reached_market,
  count(*) FILTER (WHERE used_league_filter)        AS used_league_filter,
  count(*) FILTER (WHERE trade_intent)              AS opened_trade_panel,
  count(*) FILTER (WHERE clicked_auth)              AS clicked_auth,
  count(*) FILTER (WHERE connected)                 AS connected_wallet
FROM public.analytics_session_summary;

-- Click coordinates for heatmaps.
CREATE VIEW public.analytics_clicks AS
SELECT
  id,
  event_ts,
  session_id,
  wallet_address,
  device,
  locale,
  page_path,
  page_category,
  action_category,
  name,
  (metadata->>'x')::numeric            AS x,
  (metadata->>'y')::numeric            AS y,
  (metadata->>'vw')::numeric           AS vw,
  (metadata->>'vh')::numeric           AS vh,
  metadata->>'selector'                AS selector,
  metadata->>'href'                    AS href
FROM public.analytics_events_enriched
WHERE event_type = 'click'
  AND metadata ? 'x';
