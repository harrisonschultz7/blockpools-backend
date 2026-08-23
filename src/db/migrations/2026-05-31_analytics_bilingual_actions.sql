-- src/db/migrations/2026-05-31_analytics_bilingual_actions.sql
--
-- Makes action_category language-agnostic (EN + ES) and id-aware. Previously
-- the classifier was English-only, so Spanish buy clicks ("Comprar COL"),
-- deposits ("Depositar"), etc. fell through to 'other_click' and didn't count
-- in the funnel/journey. This treats both languages — and the stable
-- data-analytics-id button names — the same.
--
-- Pure view replacement: column set is unchanged, so CREATE OR REPLACE works
-- without dropping dependents. Re-runnable. Run in the Supabase SQL editor.

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

    -- Stable analytics ids (language-independent) win first.
    WHEN e.name = 'place_trade'     THEN 'trade_intent'
    WHEN e.name = 'sign_in'         THEN 'auth'
    WHEN e.name = 'deposit_onramp'  THEN 'deposit'
    WHEN e.name = 'withdraw_submit' THEN 'withdraw'
    WHEN e.name = 'redeem_promo'    THEN 'promo'

    -- Navigation by destination href (already language-independent).
    WHEN e.metadata->>'href' IN ('/app','/m','/leaderboard','/profile','/positions','/groups')
      THEN 'navigation'

    -- League filter pills (league codes are the same across locales).
    WHEN e.name ~* '^(home|inicio|mlb|nba|nhl|nfl|epl|ucl|copa|mundial|champions|wnba|ncaa)'
      THEN 'league_filter'

    -- Trade intent: EN "Buy ...", ES "Comprar ...", outcome price chips ($0.x),
    -- and bet-amount steppers.
    WHEN e.name ILIKE 'Buy %' OR e.name ILIKE 'Comprar%' OR e.name ~ '\$0\.'
         OR e.name IN ('+5','+10','+25','+100','Max','Máx')
      THEN 'trade_intent'

    -- Auth: EN/ES sign-in + social providers.
    WHEN e.name ILIKE '%sign in%' OR e.name ILIKE '%iniciar sesi%'
         OR e.name IN ('Google','Apple','Email','Continue','Continuar','Acceder','Conectar','Conectarse','Regístrate')
      THEN 'auth'

    -- Deposit / fund wallet (EN + ES).
    WHEN e.name ILIKE 'Deposit%' OR e.name ILIKE 'Depositar%' OR e.name ILIKE 'Fund%'
      THEN 'deposit'

    -- Withdraw / cash out (EN + ES).
    WHEN e.name ILIKE 'Withdraw%' OR e.name ILIKE 'Retirar%'
      THEN 'withdraw'

    -- Promo / redeem (EN + ES).
    WHEN e.name ILIKE 'Promo%' OR e.name ILIKE 'Reclamar%' OR e.name ILIKE 'Redeem%'
      THEN 'promo'

    -- Dismiss / close modals (EN + ES).
    WHEN e.name IN ('Later','Más tarde','Close','Cerrar','Cancelar','close modal','Dismiss','X')
      THEN 'dismiss'

    -- Opening a market tile (price-bearing card).
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
  AND e.page_path NOT LIKE '/admin%';
