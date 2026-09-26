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

---

# Argo-7 -- NFL game totals

Second bot. **Totals (over/under) only** -- no spreads, no moneylines. Shares
Adam-7's feature store, paper filler, exit engine and NAV accounting; a second
copy of the folder would have duplicated ~1500 lines and doubled every future fix.

Which bot is active is chosen by `TRADING_BOT_CONFIG=config.argo-7.json`, or by
`selectConfig()` at the top of a run script. Every Argo run script selects its own
config, so it cannot be started against the wrong one by accident.

## The thesis

The edge is not picking winners -- it is the **resting limit exit**. A sportsbook
makes you hold to settlement; here the bot names a price above fair and rides the
position through kickoff, taking the sale if an in-play swing reaches it and
settling normally if it does not. The resting order is a free option, never a stop.

Totals support this at size: **$20.5M of resting liquidity across 68 games**
(measured 2026-09-26), $300-440k on the single deepest line per game. And NFL
totals carry **`feeType: zero_fees`** while game moneylines carry
`sports_fees_v3` at rate 0.05 taker-only -- a real structural advantage for a
round-tripping strategy, and Polymarket's to withdraw at any time, which is why
`fee_rate` is stored per market rather than assumed.

## How a decision is made

Points first, probability second:

```
implied_mean = line + sigma * inverse_normal(p_market)   # invert the price
model_mean   = implied_mean + sum(weight_k * scale_k * points_k)
p_fair       = normal_cdf((model_mean - line) / sigma)   # back to a price
delta        = capped(p_fair - p_market)
```

The market is the **anchor**, never a weighted factor. A from-scratch projection
compared against the book would mostly be measuring the model's own error, since
both are built from the same public data.

`deltaCap` 0.16 means the model may disagree by about **5.5 points** at the fitted
sigma of 13.4 (4.3 at the 10.5 originally assumed), narrowing toward the price
extremes. That translation is the single best sanity check in the system: if the
model ever wants to move a total 8 points, it is broken.

One line per game -- the **deepest book**. Verified live: the deepest line is
always the one priced nearest 50/50, so "deepest" and "closest to a coin flip"
select the same market. Taking three lines would be the same bet three times.

The window is **72 hours**, not Adam-7's 192, because weather carries the top
weight and a wind forecast eight days out is close to worthless.

## Weights, and what the calibration did to them

The user set these, in this order of importance:

| factor | weight | the reasoning given |
|---|---|---|
| weather | 0.30 | top weight, but only above a threshold |
| scheme / play style | 0.29 | the matchup that others miss |
| rest | 0.15 | third |
| pace | 0.10 | small weight of its own, above what the anchor carries |
| injury | 0.08 | matters less to a total than to a side |
| situational | 0.08 | "doesn't matter in my opinion" |

**The calibration then overrode most of that.** `run/calibrate-totals.js` regresses
`actual_total - closing_line` on each factor's points, so the fitted beta reads
directly as *how much of this factor the market has not already priced*. On 318
games (2025 + 2026 weeks 1-3):

| factor | beta | t | scale applied | verdict |
|---|---|---|---|---|
| weather | +2.73 | 1.16 | 0.86 | survives, on only 45 non-zero games |
| situational | +8.27 | 2.25 | 1.25 | strongest t of the six |
| pace | +0.95 | 1.14 | 0.54 | survives, weakly |
| scheme | **-1.26** | -1.24 | **0** | wrong sign, zeroed |
| rest | **-1.45** | -1.11 | **0** | wrong sign, zeroed |
| injury | -- | -- | **0** | non-zero in <25 games, disabled |

So the shipped model is roughly **62% weather, 24% situational, 13% pace**, and the
user's stated #2 and #3 factors contribute nothing. A negative beta is clamped to
zero, never flipped -- a factor pointing the wrong way gets stopped, not reversed
off one season of evidence. Surviving betas are damped by `t^2/(t^2+1)`, so an
unreliable one is shrunk rather than traded at full size or dropped entirely.

Re-run the calibration as the season adds games. These conclusions are provisional
and several are one good month from changing.

## The scheme factor, and why it is mostly dormant

Built from nflverse `ftn_charting` -- the only free source that charts *how* a play
was run. Two axes per side: offence carries aggression (play action + motion +
shotgun) and tempo (no-huddle); defence carries pressure (blitz rate + pass
rushers) and front weight (defenders in the box). Four interaction terms.

**Only the interaction is used.** Main effects are fitted as controls and then
discarded: how good an offence is in the abstract is exactly what the price already
contains. The interaction -- whether *this* profile gains against *that* profile,
beyond what either is worth alone -- is the part a midpoint can plausibly miss.

**League-level, not team-level.** With two charted games per team there is no
sample for "the Eagles specifically against this scheme". The claim the model makes
is weaker than it sounds: offences that *look like* this one fare this way against
defences that *look like* that one.

**Zone versus man coverage is not available.** Not in FTN, not anywhere free. The
defensive axes are pressure and front weight, which is a pressure-scheme axis, not
a coverage axis. The original motivating example ("this offence is good against
zone, that defence plays a lot of zone") cannot be expressed at all.

Interaction significance by fit window, which is why `fitStartSeason` is 2022:

| fit start | n | max abs t | |
|---|---|---|---|
| 2025 | 587 | 0.61 | nothing |
| 2024 | 1142 | 1.38 | |
| 2023 | 1698 | 1.77 | |
| 2022 | 2253 | **2.28** | `tempo_pressure`, positive |

The surviving term says a high-tempo offence gains on a blitz-heavy defence, which
is sensible: no-huddle denies substitution and disguise. **Caveat that is not
resolved:** 4 terms x 4 windows is 16 tests, so a nominal 2.28 is suggestive rather
than established -- Bonferroni across the four terms wants 2.5. The monotone climb
with sample and the stable sign are the real evidence.

`fitStartSeason` (2022) governs only the league-wide coefficient. Team **traits**
still start at `schemePriorSeason` (2025), per the no-stale-matchups rule. The two
windows are separate because they describe different objects: a team's tendencies
go stale, whether tempo beats pressure does not. Set `fitStartSeason` back to 2025
to veto the wider window; the factor then goes dormant via `requireSignificantFit`.

## Running it

```bash
node trading-bot/run/migrate.js                  # adds sql/004 (idempotent)
node trading-bot/run/ingest-totals-markets.js    # discover totals markets
node trading-bot/run/ingest-ftn.js               # scheme tendencies
node trading-bot/run/fit-scheme.js               # -> src/model/scheme-fit.json
node trading-bot/run/calibrate-totals.js         # -> src/model/calibration-totals.json
node trading-bot/run/record-totals-books.js      # long-running depth recorder
node trading-bot/run/tick-totals.js --dry        # inspect a slate, write nothing
node trading-bot/test/factors.test.js            # no DB, no network
```

**Calibration is mandatory.** `forecastTotal()` returns `{skip: "not_calibrated"}`
when `calibration-totals.json` is absent, because there is no safe default: 1.0
would trade the market's own opinion back at it, and 0.0 would silently disable the
bot while looking operational.

systemd units: `blockpools-argo-tick.{service,timer}` (15 min),
`blockpools-argo-recorder.service` (continuous),
`blockpools-argo-markets.{service,timer}` (hourly). `run/daily.js` now loops every
enabled bot rather than needing one invocation per config.

## Bugs this build hit, kept as warnings

- **A missing config key produced NaN, and NaN passed every gate.**
  `replacementScaleEpa` was absent from Argo's config; the shared
  `playerBaseline()` computed `1 - drop/undefined`; injury points went NaN; p_fair
  went NaN; and the policy proposed trades on **14 of 15 games** with
  `edge NaNc size $NaN`. Nothing objected, because every gate is a comparison and
  every comparison against NaN is false. `forecastTotal()` now validates finiteness
  and names the offending factor, the policy re-checks, and
  `test/factors.test.js` asserts every shared key is present.
- **`Number(null)` is `0`, and `0` is finite.** A null rest day slipped past a
  `Number.isFinite` guard and was read as *zero days of rest* -- the most extreme
  short week possible -- pushing the total down 1.3 points. Caught by a test, not
  by reading the code. The same coercion exists in Adam-7's `situational.js` and
  was fixed there too.
- **Open-Meteo returns precipitation probability as a percent (0-100).**
  `nfl_weather.precip_prob` stores it raw (observed range 0-83). Reading it as a
  0-1 fraction made a 3% chance of rain worth -4 points and pinned the weather
  factor at its cap on a calm game. Adam-7 never consumed the column, so the
  ambiguity sat unnoticed.
- **Totals settlement fell through to the moneyline branch.** `side` is
  `over`/`under`, so `side = 'home'` is false and every totals trade was graded as
  `away_score > home_score` -- an away moneyline bet, with a plausible win rate and
  no error anywhere. Now branched on `market_type`.
- **Two tables existed only in the live database.**
  `nfl_team_game_stats.def_pass_epa`/`def_rush_epa`/`def_pass_rate` and the whole
  of `bots.limit_orders` had been created by hand and were never in `sql/`, so a
  fresh database could not reproduce the running one. Both are now declared in 004.

## Known limitations beyond Adam-7's

- **FTN charting lags.** On 2026-09-26 the 2026 file held weeks 1-2 complete and a
  single week-3 game, so scheme traits lean on the prior season well into October.
- **The coaching-change list is a hand-maintained skeleton and is unverified.**
  `data/coaching-changes.json` is currently empty, which means every team keeps its
  2025 scheme prior. A stale file fails in the worst direction: it keeps quoting a
  scheme the team no longer runs, with full confidence. `features/scheme.js` logs
  the list size on every run so an empty file is visible.
- **`beta_weather` is fitted optimistically.** There is no archive of what a
  forecast said three days before a 2025 game, so the calibrator substitutes
  *observed* nflverse temp and wind. Sound for asking what the line contained,
  and better information than the live bot will ever have.
- **Weather is one-directional**, so Argo-7 is a systematic under-buyer in bad
  weather -- which is also the one adjustment recreational money does make. A run
  of losing weather unders is a known property of this design.
- **Zero trades is the expected base rate.** On the 2026 week-3 slate the model
  landed within 0.3 points of the book on all 15 games and traded none.
