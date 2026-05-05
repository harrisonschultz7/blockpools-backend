# Promo Framework — Integration Notes

The framework is delivered as a sidecar. All the new code lives in:

- `src/config/promo.ts`
- `src/services/promotions/*`
- `src/routes/promotionsRouter.ts`
- `src/scripts/expirePromoRedemptions.ts`
- `src/scripts/freebetSettlementBot.ts`
- `src/scripts/reconcilePromoFunding.ts`

While `PROMO_FRAMEWORK_ENABLED=false` (default), none of this code runs. The
single mount in `server.ts` returns 503 from every promo endpoint, and the two
fenced hooks in `persistTrades.ts` are no-ops.

To turn the framework on, follow the deploy steps below in order.

---

## Step 0 — Install ethers v5 (one-time)

The new code imports from `@ethersproject/*` v5 sub-packages, which are already
present in `node_modules` transitively. If a future `npm ci` ever drops them,
add this line to `package.json` `dependencies`:

```json
"ethers": "^5.7.2"
```

---

## Step 1 — Apply schema additions

Run `src/services/promotions/SCHEMA_ADDITIONS.sql` against Supabase. It's
idempotent. It:

1. Adds `promo_redemptions.outcome_index` (integer)
2. Adds `promo_redemptions.tx_hash` (text + lower() index)
3. Drops the `user_trade_events_attributed` view
4. Adds `user_trade_events.effective_user_address` as a STORED generated column
   (`COALESCE(beneficiary_address, user_address)`)
5. Indexes the generated column on `lower()`

The `beneficiary_address` and `promo_redemption_id` columns on
`user_trade_events` are already in place from the previous migration.

---

## Step 2 — Add env vars

`/etc/blockpools/backend.env`:

```
PROMO_FRAMEWORK_ENABLED=false   # leave false until Step 5
PROMO_FUNDING_WALLET_ADDRESS=0x...
PROMO_FUNDING_WALLET_PRIVATE_KEY=0x...
# Optional overrides:
# PROMO_RPC_URL=...
# PROMO_TX_CONFIRMATIONS=1
# PROMO_BUY_GAS_LIMIT=600000
# PROMO_CLAIM_GAS_LIMIT=400000
# PROMO_SETTLE_MAX_PER_RUN=50
# PROMO_SETTLE_CONCURRENCY=1
```

The configured wallet must match `promotions.funding_wallet_address` for any
campaign you create — `placeFreeBet` asserts this and returns
`FUNDING_WALLET_MISMATCH` if it doesn't.

Source via `set -a && source /etc/blockpools/backend.env && set +a`.

---

## Step 3 — Build, deploy with the flag still false

```bash
cd /opt/blockpools/backend && git pull && npm ci && npm run build && sudo systemctl restart blockpools-backend
```

The `server.ts` mount and the `persistTrades.ts` hooks are already in place
in the codebase, but inert while the flag is false. After this deploy:

- `GET /api/promotions/active` → 503 (proves the route is mounted but
  framework is dormant).
- Leaderboard / profile / ROI / expert status — verify all unchanged.
- Trade ingestion — verify trade row counts continue to match the subgraph.

---

## Step 4 — (Optional) Opt stats queries into attribution

This step makes free-bet wins/losses appear in user stats. With the
generated column `effective_user_address` in place, you opt in per-query by
changing `user_address` → `effective_user_address`. Files worth reviewing:

- `src/routes/leaderboard.ts`
- `src/routes/leagueChat.ts` — expert status query
- `src/routes/profile.ts`
- `src/routes/tradeAggRoutes.ts`
- `src/services/profilePortfolio.ts`
- `src/services/cacheRefresh.ts`

Until any of these are switched, free-bet trades will show under the funding
wallet's address in stats. That's safe — just not the final UX.

**Do not** change the `updatePromoProgress` SQL — that's the legacy promo
lock and intentionally keys on `user_address`.

---

## Step 5 — Flip the flag, smoke test

```bash
sed -i 's/PROMO_FRAMEWORK_ENABLED=false/PROMO_FRAMEWORK_ENABLED=true/' /etc/blockpools/backend.env
sudo systemctl restart blockpools-backend
```

Insert a test campaign:

```sql
INSERT INTO public.promotions
  (code, type, name, credit_usdc, unlock_condition,
   placement_window_hours, max_claims_total, max_claims_per_user,
   is_repeatable, active, eligible_leagues, funding_wallet_address)
VALUES
  ('TESTPROMO1', 'code_redemption', 'Smoke test', 5, 'none',
   24, 1, 1, false, false,
   ARRAY['NBA'],
   '0x...'  -- same as PROMO_FUNDING_WALLET_ADDRESS
);
```

Note `active=false` — keeps it invisible until you flip it. With your test
wallet only:

1. Toggle `active=true` for `TESTPROMO1`
2. `POST /api/promotions/redeem` with `{ code: "TESTPROMO1", userAddress }`
3. `GET /api/promotions/me?address=…` → confirm status `eligible`
4. `POST /api/promotions/place-bet` against an active NBA pool
5. After the pool finalizes, run `node dist/scripts/freebetSettlementBot.js`
6. Run `node dist/scripts/reconcilePromoFunding.js` — should print "OK"

---

## Step 6 — Schedule the crons

| script | cadence | notes |
|---|---|---|
| `dist/scripts/expirePromoRedemptions.js` | hourly | DB-only, fast, idempotent |
| `dist/scripts/freebetSettlementBot.js` | same as `bots/settlement-bot.ts`, run AFTER it | walks `placed → settled_*` |
| `dist/scripts/reconcilePromoFunding.js` | daily | logs discrepancies; exit code 2 if any |

---

## Step 7 — Open campaigns to real users

Insert / activate production campaigns. Frontend gates lift on next pageload.

---

## Removal path

1. `PROMO_FRAMEWORK_ENABLED=false`, restart backend → system goes dormant
   immediately. Existing redemptions stay in their last state, but no new
   placements, settlements, or attributions happen.
2. (Optional) revert the persistTrades hooks and the server.ts mount.
3. (Optional) drop `effective_user_address` generated column and the new
   `promo_redemptions.outcome_index` / `tx_hash` columns. The
   `beneficiary_address` and `promo_redemption_id` columns on
   `user_trade_events` were already there and don't need to be removed.

---

## Open issues you should resolve before going live

1. **CLAIM event attribution leak.** When the funding wallet calls
   `claimWinnings()` during settlement, the resulting on-chain `Claimed`
   event becomes a CLAIM trade for the funding wallet in
   `user_trade_events`. The pre-insert hook only handles BUYs. Worst case the
   funding wallet appears in stats with CLAIM rows. Quick fix when you
   actually opt stats queries into attribution: `AND type != 'CLAIM' OR
   beneficiary_address IS NOT NULL`. Tell me which queries to patch.

2. **Pool payout math.** `settleFreeBet` assumes 1 winning share = $1 USDC
   (reads `shares(funding_wallet, outcome)` and treats that as payout). Verify
   on a real settlement before opening to all users.

3. **`referee_signup` retroactive unlock.** A redemption claimed before the
   referee signs up will sit in `pending_qualification` forever — the
   evaluator runs from persistTrades and signups don't produce trades. Add a
   call to `evaluatePromoEligibility` from the user-creation flow when
   you're ready.

4. **`payout_tx_hash = ''` sentinel on losses.** The IS NULL lock holds, but
   the value is ugly. Easy switch later to a status-based lock if you prefer.

5. **`qualifying_trade_id` and `placed_position_id` columns are uuid in
   the schema, but `user_trade_events.id` is text** (e.g.
   `bet-bet-0x…-3`). Those FK columns are unused in code today; references go
   into `event_data` jsonb instead. Either change them to text or leave
   unused.
