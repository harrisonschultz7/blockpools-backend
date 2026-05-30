-- src/db/migrations/2026-05-30_analytics_views.sql
--
-- Read-model views that power the in-house analytics dashboard. No data is
-- copied — these are live views over analytics_events. Re-runnable.
--
--   analytics_events_enriched : every event + derived page_category,
--                               action_category, market_address, event_ts
--                               (true client time), engaged_ms (capped).
--   analytics_session_summary : one row per session — segment (new vs
--                               connected), device, locale, funnel flags.
--   analytics_transitions     : page -> next-page edges (the Sankey/flow).
--   analytics_funnel          : single-row acquisition funnel counts.
--   analytics_clicks          : click coords for heatmaps.
--
-- Apply AFTER 2026-05-30_analytics_events_enrich.sql (needs occurred_at etc.).

-- ─────────────────────────────────────────────────────────────────────────────
-- 1) Enriched event stream
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW public.analytics_events_enriched AS
SELECT
  e.*,
  -- True event time; fall back to insert time for rows logged before occurred_at.
  COALESCE(e.occurred_at, e.created_at)                       AS event_ts,
  -- Page bucket from the path.
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
  -- The on-chain market address, when on a market detail page.
  CASE
    WHEN e.page_path LIKE '/markets/%'
      THEN lower(split_part(e.page_path, '/', 3))
    ELSE NULL
  END                                                         AS market_address,
  -- Coarse action taxonomy for clicks (heuristic, locale-tolerant).
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
  -- Time-on-page with outliers capped at 10 min (background tabs inflate raw
  -- duration because the timer keeps running while hidden).
  CASE
    WHEN e.event_type = 'page_view'
      THEN LEAST(e.duration_ms, 600000)
    ELSE NULL
  END                                                         AS engaged_ms
FROM public.analytics_events e
-- Drop the manual smoke-test rows.
WHERE e.session_id NOT IN ('x', 'vps-test');

-- ─────────────────────────────────────────────────────────────────────────────
-- 2) Per-session rollup — the segmentation backbone
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW public.analytics_session_summary AS
SELECT
  session_id,
  bool_or(wallet_address IS NOT NULL)                         AS connected,
  max(wallet_address)                                         AS wallet_address,
  max(device)                                                 AS device,
  max(locale)                                                 AS locale,
  min(event_ts)                                               AS started_at,
  max(event_ts)                                               AS last_seen_at,
  extract(epoch FROM (max(event_ts) - min(event_ts)))         AS session_seconds,
  count(*) FILTER (WHERE event_type = 'page_view')            AS page_views,
  count(*) FILTER (WHERE event_type = 'click')                AS clicks,
  count(DISTINCT page_path)                                   AS distinct_pages,
  count(DISTINCT market_address) FILTER (WHERE market_address IS NOT NULL)
                                                              AS markets_viewed,
  -- Funnel flags
  bool_or(page_category = 'marketplace')                      AS reached_marketplace,
  bool_or(page_category = 'market_detail')                    AS reached_market,
  bool_or(action_category = 'trade_intent')                  AS trade_intent,
  bool_or(action_category = 'auth')                          AS clicked_auth,
  bool_or(action_category = 'league_filter')                 AS used_league_filter
FROM public.analytics_events_enriched
GROUP BY session_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3) Page-to-page transitions — Sankey / flow edges
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW public.analytics_transitions AS
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
  page_category                AS from_category,
  next_category                AS to_category,
  connected,
  device,
  count(*)                     AS n
FROM seq
WHERE next_category IS NOT NULL
  AND next_category <> page_category
GROUP BY 1, 2, 3, 4;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4) Acquisition funnel (single row)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW public.analytics_funnel AS
SELECT
  count(*)                                          AS sessions,
  count(*) FILTER (WHERE reached_marketplace)       AS reached_marketplace,
  count(*) FILTER (WHERE reached_market)            AS reached_market,
  count(*) FILTER (WHERE used_league_filter)        AS used_league_filter,
  count(*) FILTER (WHERE trade_intent)              AS opened_trade_panel,
  count(*) FILTER (WHERE clicked_auth)              AS clicked_auth,
  count(*) FILTER (WHERE connected)                 AS connected_wallet
FROM public.analytics_session_summary;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5) Click coordinates for heatmaps
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW public.analytics_clicks AS
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
