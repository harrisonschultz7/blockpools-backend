-- 2026-11-10 Referral links + referral credit report.
--
-- Two ways to refer a friend, both recorded in public.invites:
--   kind='email'  the existing single-use emailed invite (POST /api/invites/email)
--   kind='link'   a reusable personal link /invite/<users.referral_code>. Each
--                 friend who signs up through it gets their own invites row,
--                 inserted already 'redeemed' (invitee_email is null).
-- A referral = an invites row with status='redeemed' and redeemed_by_user_id set.
-- The backend lets each user be referred once and only while their account is new.
--
-- Credit: referral_credits() totals each referred friend's own BUY volume since
-- they were referred and applies a rate (default 1%). Paid out manually.
--   select * from public.referral_credits();                      -- all time
--   select * from public.referral_credits('2026-11-10');          -- since promo start
--   select * from public.referral_credits('2026-11-01', '2026-12-01');

alter table public.invites alter column invitee_email drop not null;

alter table public.invites
  add column if not exists kind text not null default 'email';

do $$ begin
  alter table public.invites
    add constraint invites_kind_check check (kind in ('email', 'link'));
exception when duplicate_object then null; end $$;

create index if not exists invites_redeemed_by_idx
  on public.invites (redeemed_by_user_id) where status = 'redeemed';

alter table public.users add column if not exists referral_code text;
create unique index if not exists users_referral_code_key
  on public.users (referral_code) where referral_code is not null;

create or replace function public.referral_credits(
  p_since timestamptz default '-infinity',
  p_until timestamptz default 'infinity',
  p_rate  numeric     default 0.01
)
returns table (
  referrer_user_id  text,
  referrer_username text,
  referrer_email    text,
  referrer_address  text,
  referee_user_id   text,
  referee_username  text,
  referee_email     text,
  referee_address   text,
  source            text,
  referred_at       timestamptz,
  buy_trades        bigint,
  buy_volume_usd    numeric,
  credit_usd        numeric
)
language sql
stable
as $$
  -- First redemption wins if a user was (historically) referred more than once.
  with refs as (
    select distinct on (i.redeemed_by_user_id)
           i.inviter_user_id, i.redeemed_by_user_id, i.kind, i.redeemed_at
      from public.invites i
     where i.status = 'redeemed'
       and i.redeemed_by_user_id is not null
       and i.redeemed_by_user_id <> i.inviter_user_id
     order by i.redeemed_by_user_id, i.redeemed_at asc, i.id asc
  )
  select r.inviter_user_id,
         ua.username,
         ua.email,
         lower(ua.primary_address),
         r.redeemed_by_user_id,
         ub.username,
         ub.email,
         lower(ub.primary_address),
         r.kind,
         r.redeemed_at,
         count(t.id),
         coalesce(sum(t.gross_in_dec), 0),
         round(coalesce(sum(t.gross_in_dec), 0) * p_rate, 2)
    from refs r
    left join public.users ua on ua.id = r.inviter_user_id
    left join public.users ub on ub.id = r.redeemed_by_user_id
    left join public.user_trade_events t
           on t.user_address = lower(ub.primary_address)
          and t.type = 'BUY'
          and t.promo_redemption_id is null          -- free bets don't earn credit
          and to_timestamp(t.timestamp) >= greatest(r.redeemed_at, p_since)
          and to_timestamp(t.timestamp) <  p_until
   where lower(coalesce(ua.primary_address, '')) is distinct from lower(ub.primary_address)
   group by r.inviter_user_id, ua.username, ua.email, ua.primary_address,
            r.redeemed_by_user_id, ub.username, ub.email, ub.primary_address,
            r.kind, r.redeemed_at
   order by 13 desc, r.redeemed_at desc
$$;

-- Emails and trading volume: service role only.
revoke all on function public.referral_credits(timestamptz, timestamptz, numeric) from public, anon, authenticated;
