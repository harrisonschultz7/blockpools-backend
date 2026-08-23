-- src/db/migrations/2026-05-31_analytics_exclude_wallets.sql
--
-- Authoritative definition of analytics_events_enriched. Combines:
--   * bilingual (EN/ES) + id-aware action_category
--   * /admin self-traffic exclusion + smoke-test session exclusion
--   * exclusion of internal/test wallets — ENTIRE sessions that ever connected
--     one of these wallets are dropped (including their anonymous pre-connect
--     events), so they never appear anywhere in the dashboard.
--
-- Pure CREATE OR REPLACE (column set unchanged) — no cascade. Re-runnable.
-- To add/remove an excluded wallet later, edit the IN (...) list (lowercase) and
-- re-run this block.

CREATE OR REPLACE VIEW public.analytics_events_enriched AS
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

    WHEN e.name = 'place_trade'     THEN 'trade_intent'
    WHEN e.name = 'sign_in'         THEN 'auth'
    WHEN e.name = 'deposit_onramp'  THEN 'deposit'
    WHEN e.name = 'withdraw_submit' THEN 'withdraw'
    WHEN e.name = 'redeem_promo'    THEN 'promo'

    WHEN e.metadata->>'href' IN ('/app','/m','/leaderboard','/profile','/positions','/groups')
      THEN 'navigation'

    WHEN e.name ~* '^(home|inicio|mlb|nba|nhl|nfl|epl|ucl|copa|mundial|champions|wnba|ncaa)'
      THEN 'league_filter'

    WHEN e.name ILIKE 'Buy %' OR e.name ILIKE 'Comprar%' OR e.name ~ '\$0\.'
         OR e.name IN ('+5','+10','+25','+100','Max','Máx')
      THEN 'trade_intent'

    WHEN e.name ILIKE '%sign in%' OR e.name ILIKE '%iniciar sesi%'
         OR e.name IN ('Google','Apple','Email','Continue','Continuar','Acceder','Conectar','Conectarse','Regístrate')
      THEN 'auth'

    WHEN e.name ILIKE 'Deposit%' OR e.name ILIKE 'Depositar%' OR e.name ILIKE 'Fund%'
      THEN 'deposit'

    WHEN e.name ILIKE 'Withdraw%' OR e.name ILIKE 'Retirar%'
      THEN 'withdraw'

    WHEN e.name ILIKE 'Promo%' OR e.name ILIKE 'Reclamar%' OR e.name ILIKE 'Redeem%'
      THEN 'promo'

    WHEN e.name IN ('Later','Más tarde','Close','Cerrar','Cancelar','close modal','Dismiss','X')
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
  AND e.page_path NOT LIKE '/admin%'
  AND e.session_id NOT IN (
    SELECT session_id
    FROM public.analytics_events
    WHERE lower(wallet_address) IN (
      '0xafcc1c34125535a391e01dde1b08651293f11b91',
      '0x0ffb8c2dfe3ecac06ccb1c77e462c45ef29fa1fe',
      '0xee7de6e38db71587d8d89ecc6f3ff43d7c538a14',
      '0x39ef251f74586c244829e1b19ecd344c2e4452ce'
    )
  );
