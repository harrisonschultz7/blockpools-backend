-- Applied to Supabase 2026-10-16 (migration pm_wallet_transfers).
-- Polymarket wallet pUSD / USDC.e transfers (deposits, withdrawals and trade
-- money), scanned from Polygon logs by src/services/pmWalletValue.ts, for the
-- profile's portfolio-value graph.
CREATE TABLE IF NOT EXISTS public.pm_wallet_transfers (
  wallet text NOT NULL, tx_hash text NOT NULL, log_index integer NOT NULL,
  block bigint NOT NULL, ts bigint NOT NULL, token text NOT NULL,
  direction text NOT NULL CHECK (direction IN ('in','out')), amount numeric NOT NULL,
  PRIMARY KEY (wallet, tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS pm_wallet_transfers_wallet_ts ON public.pm_wallet_transfers (wallet, ts);
CREATE TABLE IF NOT EXISTS public.pm_wallet_scan (
  wallet text PRIMARY KEY, from_block bigint NOT NULL, next_block bigint NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.pm_wallet_transfers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pm_wallet_scan ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.pm_wallet_transfers FROM anon, authenticated;
REVOKE ALL ON public.pm_wallet_scan FROM anon, authenticated;
