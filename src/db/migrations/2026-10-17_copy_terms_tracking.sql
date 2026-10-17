-- Applied to Supabase 2026-10-17 (migration copy_terms_tracking).
-- Who signed up for which model, and proof they agreed to the terms:
--   copy.subscriptions.terms_version / terms_accepted_at -- stamped by
--     /api/copy/subscribe in the same transaction as the subscription (and the
--     append-only copy.consents row). The fan-out refuses any subscription
--     without terms_accepted_at.
--   copy.settings('current_terms_version') -- written by the API on boot.
--   copy.subscriber_overview -- one row per subscription: user, model, status,
--     allocation, P&L, wallet permission, agreed_to_terms, on_current_terms.
ALTER TABLE copy.subscriptions
  ADD COLUMN IF NOT EXISTS terms_version     text,
  ADD COLUMN IF NOT EXISTS terms_accepted_at timestamptz;
UPDATE copy.subscriptions s
   SET terms_version = c.terms_version, terms_accepted_at = c.accepted_at
  FROM (SELECT DISTINCT ON (privy_did, bot_id) privy_did, bot_id, terms_version, accepted_at
          FROM copy.consents ORDER BY privy_did, bot_id, accepted_at DESC) c
 WHERE c.privy_did = s.privy_did AND c.bot_id = s.bot_id;
CREATE TABLE IF NOT EXISTS copy.settings (
  key text PRIMARY KEY, value text NOT NULL, updated_at timestamptz NOT NULL DEFAULT now()
);
INSERT INTO copy.settings (key, value) VALUES ('current_terms_version', '2026-10-16.1')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now();
CREATE OR REPLACE VIEW copy.subscriber_overview AS
SELECT s.id AS subscription_id, u.username, s.privy_did,
       lower(s.wallet_address) AS signer_wallet, lower(u.trading_address) AS polymarket_wallet,
       s.bot_id, s.status, s.basis_usd AS allocated_usd, s.realized_pnl,
       s.created_at AS first_subscribed_at, s.updated_at,
       s.delegated_at IS NOT NULL AS wallet_permission_granted,
       s.terms_accepted_at IS NOT NULL AS agreed_to_terms,
       s.terms_version, s.terms_accepted_at,
       (s.terms_version = cfg.value) AS on_current_terms, cfg.value AS current_terms_version,
       (SELECT count(*) FROM copy.consents c WHERE c.privy_did = s.privy_did AND c.bot_id = s.bot_id) AS consent_records,
       (SELECT c.country FROM copy.consents c WHERE c.privy_did = s.privy_did AND c.bot_id = s.bot_id
         ORDER BY c.accepted_at DESC LIMIT 1) AS country_at_consent
  FROM copy.subscriptions s
  LEFT JOIN public.users u ON u.id = s.privy_did
  CROSS JOIN (SELECT value FROM copy.settings WHERE key = 'current_terms_version') cfg;
REVOKE ALL ON copy.subscriber_overview FROM anon, authenticated;
REVOKE ALL ON copy.settings FROM anon, authenticated;
