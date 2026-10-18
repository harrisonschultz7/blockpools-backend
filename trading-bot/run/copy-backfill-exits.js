#!/usr/bin/env node
// trading-bot/run/copy-backfill-exits.js
//
// Puts the model's limit sell on every open copied position that has none.
//
// Two bugs left positions bare: exits read the wallet's shares from the lagging
// data-api seconds after the entry (read 0 -> skipped), and moneyline reprices
// had no condition id so the placed sell was never recorded (-> no carry-over
// through kickoff). Both are fixed in fanout.js; this repairs what they left.
//
// For each bare position it re-runs the copy of the bot's LATEST exit/reprice
// intent for that token, reusing that intent's copy.orders row. A position the
// model has no exit for (holding to settlement) is left alone.
//
//   node trading-bot/run/copy-backfill-exits.js          # list only
//   node trading-bot/run/copy-backfill-exits.js --apply  # place the sells

const { selectConfig } = require("../src/config");
selectConfig("config.argo-7.json");

const { q, close } = require("../src/db");
const log = require("../src/log");
const { copyExit } = require("../src/copy/fanout");

const APPLY = process.argv.includes("--apply");

(async () => {
  if (/^(1|true|yes|on)$/i.test(String(process.env.COPY_DRY_RUN || ""))) {
    log.warn("COPY_DRY_RUN is on -- nothing will reach the book");
  }
  const { rows } = await q(`
    select p.id as pos_id, p.token_id, p.shares, s.id as sub_id, s.bot_id,
           s.privy_did, s.wallet_address, s.basis_usd, s.multiplier,
           s.max_trade_usd, s.realized_pnl,
           i.id as intent_id, i.kind, i.limit_price, i.game_id, o.id as order_id
      from copy.positions p
      join copy.subscriptions s on s.id = p.subscription_id
      left join lateral (
        select * from bots.trade_intents
         where token_id = p.token_id and bot_id = s.bot_id and kind in ('exit','reprice')
         order by id desc limit 1
      ) i on true
      left join copy.orders o on o.subscription_id = s.id and o.intent_id = i.id
     where not p.dry_run and p.closed_at is null and p.shares > 0
       and not exists (
         select 1 from public.pm_limit_orders l
          where l.user_id = s.privy_did and l.token_id = p.token_id
            and l.side = 'sell' and l.status = 'live')
     order by p.id`);

  for (const r of rows) {
    const tag = `pos ${r.pos_id} sub ${r.sub_id} ${r.game_id || "?"} ${Number(r.shares).toFixed(2)} sh`;
    if (!r.intent_id) { log(`${tag}: model has no exit -- left alone`); continue; }
    if (!r.order_id) { log.warn(`${tag}: no copy.orders row for intent ${r.intent_id} -- skipped`); continue; }
    log(`${tag}: ${r.kind} @ ${Number(r.limit_price).toFixed(3)} (intent ${r.intent_id}, order ${r.order_id})`);
    if (!APPLY) continue;

    const { rows: [intent] } = await q(`select * from bots.trade_intents where id = $1`, [r.intent_id]);
    if (!intent.condition_id) {
      const e = await q(
        `select condition_id, line from bots.trade_intents
          where token_id = $1 and condition_id is not null order by id desc limit 1`,
        [intent.token_id]);
      intent.condition_id = e.rows[0]?.condition_id || null;
      if (intent.line == null) intent.line = e.rows[0]?.line ?? null;
    }
    const sub = {
      id: r.sub_id, privy_did: r.privy_did, wallet_address: r.wallet_address,
      basis_usd: r.basis_usd, multiplier: r.multiplier, max_trade_usd: r.max_trade_usd,
      realized_pnl: r.realized_pnl,
    };
    try {
      const ok = await copyExit(sub, intent, r.order_id);
      log(`${tag}: ${ok ? "PLACED" : "not placed (see copy.orders " + r.order_id + ")"}`);
    } catch (e) {
      log.err(`${tag}: ${e.message}`);
    }
  }
  if (!APPLY) log("list only -- re-run with --apply to place");
  await close();
})().catch(async (e) => { log.err(e.stack || e.message); await close(); process.exit(1); });
