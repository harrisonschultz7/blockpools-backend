-- Applied to Supabase 2026-10-15 (migration notifications_allow_model_trade).
-- notify.ts emits type 'model_trade' (model-alert cron, shipped 2026-10-12) but the
-- CHECK constraint was never widened, so every model alert insert was rejected.
ALTER TABLE public.notifications DROP CONSTRAINT notifications_type_check;
ALTER TABLE public.notifications ADD CONSTRAINT notifications_type_check
  CHECK (type = ANY (ARRAY['follow','trade','promo_credit','referral','model_trade']));
