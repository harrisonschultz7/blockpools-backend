-- 2026-11-17: "Funds received" notifications.
--
-- Every dollar that lands in a user's wallet from a market is recorded in
-- public.user_trade_events as a SELL (venue fill sync, direct sells, copy
-- exits and stops) or a CLAIM (settlement worker, manual claims). One trigger
-- there covers every path, instead of an emitter in each writer.
--
-- One notification per sale or settlement, not per fill: a single order often
-- fills in several pieces (ORL @ BOS 2026-11-16: five rows in one second). The
-- dedupe key buckets a SELL by game + outcome + 15-minute window (a CLAIM by
-- game + outcome), and every later fill refreshes the summed amount in place.
--
-- Only rows stamped within the last 6 hours notify, so a backfill never floods
-- anyone's bell. The function can never block the trade write: any error is
-- logged as a warning and the row is kept.

alter table public.notifications drop constraint if exists notifications_type_check;
alter table public.notifications add constraint notifications_type_check
  check (type = any (array['follow', 'trade', 'promo_credit', 'referral', 'model_trade', 'payout']));

create or replace function public.notify_payout() returns trigger
language plpgsql security definer set search_path = public as $$
declare
  uid text;
  bucket bigint;
  amt numeric;
  gross numeric;
  sh numeric;
  parts text[];
  matchup text;
begin
  if new.type not in ('SELL', 'CLAIM') then return new; end if;
  if coalesce(new."timestamp", 0) < extract(epoch from now()) - 6 * 3600 then return new; end if;

  select u.id into uid from public.users u
   where lower(new.user_address) in (lower(u.primary_address), lower(u.eoa_address),
                                     lower(u.deposit_address), lower(u.trading_address))
   limit 1;
  if uid is null then return new; end if;

  bucket := case when new.type = 'CLAIM' then 0 else new."timestamp" / 900 end;

  select sum(coalesce(e.net_out_dec, e.gross_out_dec)),
         sum(e.gross_out_dec),
         sum(case when e.avg_price_bps > 0 then e.gross_out_dec / (e.avg_price_bps / 10000.0) end)
    into amt, gross, sh
    from public.user_trade_events e
   where e.game_id = new.game_id
     and e.outcome_index is not distinct from new.outcome_index
     and e.type = new.type
     and lower(e.user_address) = lower(new.user_address)
     and (new.type = 'CLAIM' or e."timestamp" / 900 = bucket);
  if coalesce(amt, 0) < 0.01 then return new; end if;

  parts := string_to_array(split_part(new.game_id, '::', 1), '-');
  matchup := case when array_length(parts, 1) >= 3 then upper(parts[2]) || ' @ ' || upper(parts[3]) end;

  insert into public.notifications (recipient_id, type, actor_id, payload, dedupe_key)
  values (uid, 'payout', null,
          jsonb_build_object(
            'kind', case when new.type = 'CLAIM' then 'settled' else 'sold' end,
            'amountUsd', round(amt, 2),
            'gameId', new.game_id,
            'outcome', new.outcome_code,
            'league', new.league,
            'matchup', matchup,
            'pricePct', case when sh > 0 then round(gross / sh * 100) end,
            'shares', round(sh, 2)),
          'payout:' || new.type || ':' || lower(new.game_id) || ':' || coalesce(new.outcome_index::text, 'x') || ':' || bucket)
  on conflict (recipient_id, type, dedupe_key) where dedupe_key is not null
  do update set payload = excluded.payload;

  return new;
exception when others then
  raise warning 'notify_payout failed for %: %', new.id, sqlerrm;
  return new;
end $$;

drop trigger if exists user_trade_events_payout_trg on public.user_trade_events;
create trigger user_trade_events_payout_trg
  after insert on public.user_trade_events
  for each row when (new.type in ('SELL', 'CLAIM'))
  execute function public.notify_payout();
