-- 2026-11-11 Referral promo: count only real trading, and track weekly payouts.
--
-- Promo: a referrer earns 1% of each referred friend's QUALIFYING buys, for
-- friends referred and buys made from 2026-11-10 through 2026-12-11 (ET).
--
-- A buy qualifies when the position was either
--   - held to settlement: shares still held once the market settled
--     (games.is_final, or the friend claimed it, or 24h past kickoff), or
--   - sold at a real move: average sell price at least p_min_move (default 10%)
--     away from the average buy price, up or down.
-- A quick buy-and-sell round trip doesn't count. Holding BOTH sides of the same
-- market to settlement counts only the net one-sided amount (a hedge isn't a bet).
-- Positions still open are reported as pending and count once they settle.
--
-- Weekly payout:
--   select * from public.referral_payouts_due();          -- who is owed what
--   insert into public.referral_payouts (referrer_user_id, amount_usd, note)
--     values ('did:privy:…', 1.23, 'week of 2026-11-17');  -- after paying
--   select * from public.referral_credits();               -- per-friend detail

create table if not exists public.referral_payouts (
  id               bigserial primary key,
  referrer_user_id text        not null,
  amount_usd       numeric     not null check (amount_usd > 0),
  paid_at          timestamptz not null default now(),
  note             text
);
create index if not exists referral_payouts_referrer_idx on public.referral_payouts (referrer_user_id);
alter table public.referral_payouts enable row level security;
revoke all on public.referral_payouts from anon, authenticated;

drop function if exists public.referral_credits(timestamptz, timestamptz, numeric);

create or replace function public.referral_credits(
  p_start    timestamptz default '2026-11-10 05:00:00+00',  -- 2026-11-10 00:00 ET
  p_end      timestamptz default '2026-12-12 05:00:00+00',  -- end of 2026-12-11 ET
  p_rate     numeric     default 0.01,
  p_min_move numeric     default 0.10
)
returns table (
  referrer_user_id   text,
  referrer_username  text,
  referrer_email     text,
  referrer_address   text,
  referee_user_id    text,
  referee_username   text,
  referee_email      text,
  referee_address    text,
  source             text,
  referred_at        timestamptz,
  buy_volume_usd     numeric,
  qualifying_usd     numeric,
  pending_usd        numeric,
  credit_usd         numeric,
  pending_credit_usd numeric
)
language sql
stable
as $$
  with refs as (
    -- First redemption wins if a user was (historically) referred twice.
    select distinct on (i.redeemed_by_user_id)
           i.inviter_user_id, i.redeemed_by_user_id, i.kind, i.redeemed_at
      from public.invites i
     where i.status = 'redeemed'
       and i.redeemed_by_user_id is not null
       and i.redeemed_by_user_id <> i.inviter_user_id
     order by i.redeemed_by_user_id, i.redeemed_at asc, i.id asc
  ),
  pairs as (
    select r.*, lower(ub.primary_address) as referee_addr
      from refs r
      join public.users ub on ub.id = r.redeemed_by_user_id
      left join public.users ua on ua.id = r.inviter_user_id
     where r.redeemed_at >= p_start and r.redeemed_at < p_end
       and lower(coalesce(ua.primary_address, '')) is distinct from lower(ub.primary_address)
  ),
  ev as (
    -- Own-money buys inside the promo window after the referral, and every
    -- later sell. Shares = USD / price.
    select p.redeemed_by_user_id as referee, t.game_id, t.outcome_index, t.type,
           case when t.type = 'BUY' then t.gross_in_dec else t.gross_out_dec end as usd,
           case when t.type = 'BUY' then t.gross_in_dec else t.gross_out_dec end
             / nullif(t.avg_price_bps, 0) * 10000 as shares
      from pairs p
      join public.user_trade_events t on t.user_address = p.referee_addr
     where t.promo_redemption_id is null
       and t.avg_price_bps > 0
       and to_timestamp(t.timestamp) >= p.redeemed_at
       and (   (t.type = 'BUY'  and to_timestamp(t.timestamp) < p_end)
            or  t.type = 'SELL')
  ),
  pos as (
    select referee, game_id, outcome_index,
           sum(usd)    filter (where type = 'BUY')  as buy_usd,
           sum(shares) filter (where type = 'BUY')  as buy_sh,
           sum(usd)    filter (where type = 'SELL') as sell_usd,
           sum(shares) filter (where type = 'SELL') as sell_sh
      from ev
     group by 1, 2, 3
    having sum(usd) filter (where type = 'BUY') > 0
  ),
  pos2 as (
    select p.*,
           least(coalesce(p.sell_sh, 0) / p.buy_sh, 1)                     as sold_frac,
           abs((p.sell_usd / nullif(p.sell_sh, 0)) / (p.buy_usd / p.buy_sh) - 1) as move,
           (   coalesce(g.is_final, false)
            or exists (select 1 from public.user_trade_events c
                        where c.type = 'CLAIM' and c.game_id = p.game_id
                          and c.user_address = (select pr.referee_addr from pairs pr where pr.redeemed_by_user_id = p.referee))
            or to_timestamp(coalesce(g.lock_time,
                 nullif(substring(p.game_id from '-pm-([0-9]{9,11})'), '')::bigint)) < now() - interval '24 hours'
           ) as settled
      from pos p
      left join public.games g on g.game_id = p.game_id
  ),
  pos3 as (
    select referee, game_id, outcome_index, buy_usd,
           case when settled then buy_usd * (1 - sold_frac) else 0 end              as held_q,
           case when not settled then buy_usd * (1 - sold_frac) else 0 end          as pending,
           case when sold_frac > 0 and move >= p_min_move then buy_usd * sold_frac else 0 end as sold_q
      from pos2
  ),
  per_game as (
    -- Held-to-settlement on several outcomes of one market only counts the net.
    select referee, game_id,
           sum(buy_usd)                                         as buy_usd,
           greatest(2 * max(held_q) - sum(held_q), 0) + sum(sold_q) as qual,
           sum(pending)                                         as pending
      from pos3
     group by 1, 2
  ),
  per_referee as (
    select referee, sum(buy_usd) as buy_usd, sum(qual) as qual, sum(pending) as pending
      from per_game group by 1
  )
  select p.inviter_user_id, ua.username, ua.email, lower(ua.primary_address),
         p.redeemed_by_user_id, ub.username, ub.email, p.referee_addr,
         p.kind, p.redeemed_at,
         round(coalesce(x.buy_usd, 0), 2),
         round(coalesce(x.qual, 0), 2),
         round(coalesce(x.pending, 0), 2),
         round(coalesce(x.qual, 0) * p_rate, 2),
         round(coalesce(x.pending, 0) * p_rate, 2)
    from pairs p
    left join per_referee x on x.referee = p.redeemed_by_user_id
    left join public.users ua on ua.id = p.inviter_user_id
    left join public.users ub on ub.id = p.redeemed_by_user_id
   order by 14 desc, p.redeemed_at desc
$$;

create or replace function public.referral_payouts_due(
  p_start    timestamptz default '2026-11-10 05:00:00+00',
  p_end      timestamptz default '2026-12-12 05:00:00+00',
  p_rate     numeric     default 0.01,
  p_min_move numeric     default 0.10
)
returns table (
  referrer_user_id   text,
  referrer_username  text,
  referrer_email     text,
  referrer_address   text,
  friends            bigint,
  qualifying_usd     numeric,
  earned_usd         numeric,
  paid_usd           numeric,
  owed_usd           numeric,
  pending_credit_usd numeric
)
language sql
stable
as $$
  with c as (
    select referrer_user_id, max(referrer_username) u, max(referrer_email) e, max(referrer_address) a,
           count(*) friends, sum(qualifying_usd) q, sum(credit_usd) earned, sum(pending_credit_usd) pend
      from public.referral_credits(p_start, p_end, p_rate, p_min_move)
     group by 1
  ),
  paid as (
    select referrer_user_id, sum(amount_usd) paid from public.referral_payouts group by 1
  )
  select c.referrer_user_id, c.u, c.e, c.a, c.friends, c.q, c.earned,
         coalesce(paid.paid, 0), greatest(c.earned - coalesce(paid.paid, 0), 0), c.pend
    from c left join paid using (referrer_user_id)
   order by 9 desc, 7 desc
$$;

-- Emails and trading volume: service role only.
revoke all on function public.referral_credits(timestamptz, timestamptz, numeric, numeric) from public, anon, authenticated;
revoke all on function public.referral_payouts_due(timestamptz, timestamptz, numeric, numeric) from public, anon, authenticated;
