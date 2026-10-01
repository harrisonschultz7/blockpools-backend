-- 009_copy_dry_run.sql
--
-- Marks rows the fan-out produced WITHOUT touching an exchange.
--
-- A simulated fill is indistinguishable from a real one once it is a row of
-- numbers, and a copied position nobody actually holds is the most dangerous
-- thing this schema can contain -- it would show in the betslip rail, in the
-- profile, and in any P&L built on top of them. So the flag lives on the row
-- itself rather than in a log somewhere, every surface can filter on it, and
-- the default is false so anything written before today stays real.

alter table copy.orders
  add column if not exists dry_run boolean not null default false;

alter table copy.positions
  add column if not exists dry_run boolean not null default false;

-- Simulated and real must never merge into one position. The existing unique
-- key is (subscription_id, token_id), so without this a dry run would add its
-- shares to a real holding.
drop index if exists copy.positions_sub_token_uniq;
alter table copy.positions drop constraint if exists positions_subscription_id_token_id_key;
create unique index if not exists positions_sub_token_dry_uniq
  on copy.positions (subscription_id, token_id, dry_run);

-- Everything simulated, newest first: the cleanup script's working set.
create index if not exists copy_orders_dry_idx
  on copy.orders (dry_run, created_at desc) where dry_run;
