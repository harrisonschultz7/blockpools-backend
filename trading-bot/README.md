# NFL Trading Bot (v1)

One bot. Medium risk. **NFL pre-game moneylines only. Paper fills, no money.**
Evaluates a full NFL **week as one slate** (all 16 games together), not a rolling
hours window.

Venue is Polymarket (via the builder integration); the V2 order book and seed
bot are not used. Runs on the VPS as its own systemd units, reusing the
backend's `node_modules` — no build step, plain CJS, same pattern as
`scripts/seed-bot.js`.

## The one idea

    p_fair = p_market + delta

The market price is the **anchor**, not a weighted factor. The Polymarket
midpoint is already an output of team strength, injuries, rest and matchup, so
blending it as one factor alongside those same inputs double-counts every one
of them — and *lowering* its blend weight does not fix that, it adds noise on
top of the double-count. The model forecasts only the **deviation**.

"How much may the bot disagree with the public" is therefore a **deviation cap**
(`model.deltaCap`, 0.12), not a blend weight.

This is not a theoretical concern. Run uncalibrated, the bot backed the market
favourite in **4 of 4** trades. With the residualisation fitted, the same slate
produced **2 trades, both underdogs**. Same factors, same weights — the
difference is stripping out what the price already knew.

## Factor weights (`config.json`, `model.weights`)

| factor | weight | what it is |
|---|---|---|
| momentum | 0.30 | opponent-adjusted, recency-weighted EPA/play |
| matchup | 0.25 | pace × defence interaction + style mismatch |
| injury | 0.25 | **baselined** against how the team actually played without the player |
| situational | 0.20 | rest, short week, primetime road, division, weather |

Weights are the *design intent*. What actually reaches `delta` is
`weight × scale × residual`, and `scale` is fitted — see Calibration.

### Why "baselined" injuries matter
A missing player is only worth what his replacement is *not*. The Rams without
Nacua are not "the Rams minus Nacua" — they are the Rams with whoever plays
instead, and that team has a record. The catch: in 2025 they played exactly
**one** game without him. `shrinkGamesWithout` pulls any such estimate back
toward league-average positional replacement, so one game barely moves the
number and eight games moves it most of the way. Without that shrinkage this
factor is a noise generator.

### Why matchup is multiplicative
Two fast offences produce more snaps, so defensive EPA-per-play gets applied
more times — defence mechanically matters more *in that game*. An additive
matchup rating cannot express that. See `features/matchup.js`.

## Calibration — run this before believing anything

    node trading-bot/run/calibrate.js          # full refit (~4 min)
    node trading-bot/run/calibrate.js --cache  # refit from cached samples

Fits, per factor, `beta` = how much of it the closing line already prices, and
damps `scale` toward zero when the residual does not predict outcomes.

**Current fit (300 games, 2025 + 2026 to date):**

| factor | corr w/ market | resid corr w/ outcome | σ | scale kept |
|---|---|---|---|---|
| momentum | 0.78 | −0.013 | −0.2 | 0.00 |
| matchup | 0.38 | +0.094 | **+1.6** | 0.94 |
| injury | 0.19 | −0.053 | −0.9 | 0.00 |
| situational | 0.16 | +0.018 | +0.3 | 0.18 |

Read this honestly: **no factor clears 2σ.** Only matchup is even suggestive.
Momentum is ~78% priced by the market already, which is what you would expect.
At n=300 the standard error on these correlations is 0.058, so everything here
is within noise of zero. The damping is what keeps an unproven factor from
sizing real bets.

This is the direct consequence of `data.startSeason = 2025`. Widening it is a
one-line config change and is the single highest-value experiment available;
`replacementBaselineStartSeason` is split out precisely so the injury baseline
can reach further back (what a backup QB costs you is stable across eras) while
momentum stays recent.

## Entry and exit

**Entry:** buy when the market price deviates from the model fair value by more
than `policy.minEdge` (2.5c). Fair value is computed per game as `p_fair`.

**Exit:** after each buy the bot rests a SELL limit at fair value. Buy at 0.72
against a fair of 0.80, rest a sell at 0.80, bank the markup if the market comes
to us. This is not a trade-off:

| | expected value | variance | capital |
|---|---|---|---|
| hold to settlement | +8c/share | 1.00 or 0.00 — huge | locked until kickoff |
| sell at fair | +8c/share | ~none | freed immediately |

Same EV, far less variance. And if the market never reaches the limit it simply
does not fill and the position rides to settlement exactly as it would have — so
the limit is a free option with no downside branch.

The resting price is **re-priced every tick** as `p_fair` moves (1c deadband). A
limit parked at Tuesday's fair is stale by Sunday: if the opposing QB is ruled
out and fair drifts to 0.86, a static 0.80 sells 6c too cheap. Same reasoning as
seed-bot.js re-pricing its ladder rather than leaving stale rungs.

Kickoff cancels any unfilled exit — the bot does not trade in play, so from there
the position rides to settlement. A position closed early is marked `exited`, which
is what stops `settleTrades()` paying out shares the bot no longer held.

Paper exits fill against the recorded best **bid**, walking the ladder — the mirror
of the entry walking the ask.

## Grade on CLV, not P&L

With a handful of trades a week, P&L is almost pure variance — a 55% edge and a
45% edge look identical over 30 bets. Every trade instead gets `clv_bps`: did
the price move our way by kickoff? Positive CLV with negative P&L is unlucky.
Negative CLV with positive P&L is lucky, and will be given back.

## Layout

    sql/                 schemas (sports.*, bots.*), server-only grants
    src/ingest/          nflverse, polymarket (depth recorder), weather
    src/features/        the four factors
    src/model/           forecast (residual), calibrate
    src/policy/          medium-tier sizing + skip gates
    src/exec/paper.js    depth-walking paper fills
    src/accounting/      NAV marking, settlement, CLV
    run/                 entry points
    systemd/             unit files

## Running

    node trading-bot/run/migrate.js           # idempotent
    node trading-bot/run/ingest-nflverse.js   # games, pbp, snaps, injuries
    node trading-bot/run/ingest-markets.js    # discover markets + one book snapshot
    node trading-bot/run/calibrate.js
    node trading-bot/run/tick.js --dry        # decide, record nothing
    node trading-bot/run/tick.js              # decide + record paper trades
    node trading-bot/run/daily.js             # refresh, settle, grade, mark NAV

`--window-hours N` widens the trading window for a dry run (refused without
`--dry`, so it can never loosen live trading).

### On the VPS

The unit files assume `/opt/blockpools/backend` and user `bp`. **Confirm against the
already-working backend unit before installing** -- they must match it exactly:

    systemctl cat blockpools-backend.service | grep -E "User|WorkingDirectory|ExecStart"

Then:

    sudo cp trading-bot/systemd/*.service trading-bot/systemd/*.timer /etc/systemd/system/
    sudo systemctl daemon-reload
    sudo systemctl enable --now blockpools-trading-bot-recorder.service
    sudo systemctl enable --now blockpools-trading-bot-tick.timer
    sudo systemctl enable --now blockpools-trading-bot-daily.timer

Verify the API route is actually serving before expecting the frontend tile:

    curl -s localhost:8080/api/bots | head -c 200

**The recorder is the one that must never stop.** Polymarket's CLOB serves
current book state only — historical depth cannot be bought, scraped or
reconstructed from any source at any price. Every hour it is down is an hour of
backtest fidelity permanently gone. It records far wider than it trades
(`ingest.recordWindowHours` 240 vs `policy.openWindowHoursBeforeKickoff` 72)
because recording is cheap and unrepeatable while trading early is neither.

## Local development

`DATABASE_URL` points at Supabase's direct host, which is IPv6-only — fine on
the VPS, unreachable from an IPv4-only machine. Set `TRADING_BOT_DATABASE_URL`
to a pooler connection string for local runs instead of editing the shared URL.

## Going live (later)

Swap `exec/paper.js` for a call into the builder path (`placePmMarketBuy`,
`requireBuilderCode`) and flip `config.mode`. Sizing, gating, accounting and
grading do not change — only the fill source. Note Polymarket enforces a
**5-share minimum order**, which is why cent-sized real orders were not an
option and paper fills against recorded depth were used instead.

## Known limitations

- **No in-play.** There is no live drive-level feed wired (Goalserve gives score
  and clock, not down/distance/possession), so the bot is pre-game only.
- **Within-week injury progression is unavailable historically.** nflverse
  republishes the injury file in place with no report date, so backfilled rows
  carry their ingest time. Progression is captured going forward only — see the
  backfill note in `features/injuries.js`.
- **Venue is keyed off the home team**, which is wrong for international games.
- **Weather is forecast-only by design.** nflverse `temp`/`wind` are observed
  after the fact; using them would be a look-ahead leak.
