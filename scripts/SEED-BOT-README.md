# Live order-book seeding bot (`seed-bot.js`)

Reads a fair price from **Polymarket** (public CLOB midpoint) and seeds a
capital-light **BID ladder** on both outcomes of a v2 market, sitting one tick
**behind any user order** so real user liquidity fills first. It re-prices as the
live line moves (with a deadband so a stable line causes no churn), caps exposure
per game, skews off a side when inventory piles up, and pauses via a **dead-man's
switch** if the feed or matcher drops. On/off is a Supabase flag.

It reuses the existing v2 rails end-to-end: EIP-712 signed orders → matcher
`POST /orders` / `POST /cancel`, same as [`mm-ladder.js`](./mm-ladder.js).

## Files

| File | What |
|---|---|
| `seed-bot.js` | the bot |
| `seed-bot.config.json` | risk profile (`live`/`pre`) + the game allowlist |
| `blockpools-seed-bot.service` | systemd unit (runs `--loop 1`) |
| `../src/db/migrations/2026-08-06_bot_control.sql` | the `bot_control` on/off table |
| control endpoint | `GET/POST /api/v2/seed-bot/control` in `../src/routes/v2Routes.ts` |

## One-time setup

1. **Dedicated wallet.** Create a fresh EOA for the bot (don't reuse the settler/
   operator key). Put it in the backend `.env` as `SEED_BOT_PRIVATE_KEY`.
2. **Fund + approve.** Send it USDC (start small — the live `maxExposureUsd` is
   $100/game by default), then:
   ```bash
   node scripts/seed-bot.js --approve
   ```
3. **Leaderboard exclusion.** Add the bot address to `V2_MM_WALLETS` in the
   backend `.env` so its fills don't pollute the leaderboard.
4. **Do NOT add it to `SWEEP_MAKERS`.** The kickoff+30m sweeper
   ([`clear-stale-books.js`](./clear-stale-books.js)) kills *stale* ladders; this
   bot re-prices to the live line, so the sweeper would fight it. This bot is its
   own risk manager.
5. **Migration.** Run `src/db/migrations/2026-08-06_bot_control.sql` in the
   Supabase SQL editor (creates `public.bot_control`, seeds `seed-bot` = OFF).
6. **Config the game.** Edit `seed-bot.config.json` → set the real `gameId` from
   `games.json` and confirm `polymarketSlug` resolves:
   `https://gamma-api.polymarket.com/markets?slug=<slug>`.

## Test it safely (dry run first)

```bash
node scripts/seed-bot.js --dry        # prints the ladder it WOULD post; signs nothing
```

Watch a few ticks, confirm the fair price + rung prices look right, then deploy:

```bash
sudo cp scripts/blockpools-seed-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now blockpools-seed-bot.service
journalctl -u blockpools-seed-bot.service -f
```

The daemon is now running but **seeding is still OFF** (the flag defaults false).

## The on/off switch

```bash
# turn seeding ON
curl -s -X POST http://127.0.0.1:8080/api/v2/seed-bot/control \
  -H 'content-type: application/json' -H "x-v2-secret: $V2_MATCHER_SECRET" \
  -d '{"bot":"seed-bot","enabled":true}'

# turn it OFF (bot cancels its orders + idles; service keeps running)
curl -s -X POST http://127.0.0.1:8080/api/v2/seed-bot/control \
  -H 'content-type: application/json' -H "x-v2-secret: $V2_MATCHER_SECRET" \
  -d '{"bot":"seed-bot","enabled":false}'
```

You can also flip the `enabled` column directly in the Supabase dashboard, or
wire an admin-UI button to the same POST. **Hard kill:** `systemctl stop
blockpools-seed-bot`.

## Risk model (what protects you)

- **`maxExposureUsd`** per game — the most USDC the ladder can spend (bids lock
  nothing until filled). Tighter for `live` than `pre`.
- **User-first** — the bot always rests behind the best user bid, so users absorb
  flow first and the house only takes overflow.
- **Inventory skew** — reads `vault.sharesOf`; once long a side past
  `inventoryCapShares` it stops bidding that side.
- **Cross-venue anchor** — quotes are set behind the Polymarket mid, so no one can
  round-trip house↔Polymarket against you.
- **Dead-man's switch** — stale/failed Polymarket feed, Polymarket market closed,
  matcher unreachable, on-chain market resolved, or control flag off ⇒ the bot
  cancels its orders and goes flat.

## Tuning (`seed-bot.config.json`)

`rungs` (ladder depth), `topSpreadCents` (how far behind fair the top bid sits),
`stepCents` (gap between rungs), `maxExposureUsd`, `cadenceSec` (1 = live,
3600 = hourly), `repriceDeadbandCents` (min move before re-quoting),
`inventoryCapShares`. Edits apply live — the config is re-read each tick.

## Not yet (phase 2 ideas)

- Kalshi as a second feed / cross-check (needs API-key auth).
- Polymarket CLOB **websocket** instead of polling (push prices; lower latency).
- Two-sided quoting (post asks to actively unwind inventory, not just stop buying).
- Auto-discovery of all of today's live MLB games (map each by team+date).
