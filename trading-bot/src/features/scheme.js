// trading-bot/src/features/scheme.js
//
// PLAY-STYLE / SCHEME MATCHUP -- Argo-7's second-heaviest factor (0.29).
//
// The question it answers: does THIS offensive profile gain or lose against
// THIS defensive profile, beyond what either profile is worth on its own?
//
// ONLY THE INTERACTION IS USED. Main effects are fitted as controls and then
// thrown away at prediction time. That is not a simplification, it is the whole
// design: how good an offence is in the abstract is precisely what the market
// price already contains, so carrying main effects into the forecast would
// re-price team strength a second time and the residual model would be lying
// about being a residual model. The interaction is the part a midpoint can
// plausibly miss.
//
// LEAGUE-LEVEL, NOT TEAM-LEVEL. The coefficients describe the league, and the
// only team-specific inputs are the two trait values per side. With two charted
// games per team this season there is no sample for "the Eagles specifically
// against this scheme", so the claim the model actually makes is weaker than it
// sounds in conversation: offences that LOOK like this one fare this way against
// defences that LOOK like that one. Worth revisiting near week 10.
//
// THE COVERAGE GAP. Zone versus man is not in FTN or any other free source, so
// the defensive axes here are pressure and front weight -- a pressure-scheme
// axis, not a coverage axis. The original motivating example ("this offence is
// good against zone, that defence plays a lot of zone") cannot be expressed.
//
// Output is in POINTS on the game total, positive meaning more points.

const fs = require("fs");
const path = require("path");
const { cfg } = require("../config");
const { q } = require("../db");
const log = require("../log");

const FIT_PATH = path.join(__dirname, "../model/scheme-fit.json");
const COACHING_PATH = path.join(__dirname, "../../data/coaching-changes.json");

/** Fitted interaction coefficients, or nulls when run/fit-scheme.js has not run. */
function loadFit() {
  try {
    const f = JSON.parse(fs.readFileSync(FIT_PATH, "utf8"));
    return { ...f, fitted: true };
  } catch {
    // Zeroes, not guesses. An unfitted scheme factor contributes nothing and
    // says so, rather than inventing a coefficient and looking calibrated.
    return {
      fitted: false,
      n: 0,
      coef: { aggr_pressure: 0, aggr_box: 0, tempo_pressure: 0, tempo_box: 0 },
      league: null,
    };
  }
}

/** Teams whose prior-season scheme must be discarded, for a given season. */
function coachingChanges(season) {
  try {
    const all = JSON.parse(fs.readFileSync(COACHING_PATH, "utf8"));
    const entry = all[String(season)];
    if (!entry || !entry.teams) return { teams: {}, verified: false, count: 0 };
    return {
      teams: entry.teams,
      verified: entry._verified === true,
      count: Object.keys(entry.teams).length,
    };
  } catch (e) {
    log.warn(`coaching-changes.json unreadable (${e.message}) -- prior-season scheme kept for every team`);
    return { teams: {}, verified: false, count: 0 };
  }
}

// == Trait construction =====================================================
// Rates are pooled from RAW COUNTS across games, never averaged from per-game
// rates: a game contributing 6 neutral snaps must not carry the same weight as
// one contributing 53. Prior-season rows are pooled in at a discount, and
// dropped entirely for a team whose scheme discontinued.
//
// Each pooled rate is then shrunk toward the league rate on its own
// denominator, so a team with 40 neutral snaps on the season is pulled most of
// the way to average instead of being read as a strong tendency.

const RATE_SPECS = {
  // key              numerator      denominator
  motion:    ["motion_n",    "off_plays"],
  pa:        ["pa_n",        "off_dropbacks"],
  screen:    ["screen_n",    "off_dropbacks"],
  rpo:       ["rpo_n",       "off_plays"],
  noHuddle:  ["no_huddle_n", "off_plays"],
  shotgun:   ["shotgun_n",   "off_plays"],
  blitz:     ["blitz_n",     "def_dropbacks"],
  heavyBox:  ["heavy_box_n", "box_n"],
};
// Averages rather than rates: a sum over a count, same pooling logic.
const MEAN_SPECS = {
  rushers:   ["rushers_sum",   "rushers_n"],
  box:       ["box_sum",       "box_n"],
  backfield: ["backfield_sum", "backfield_n"],
};

/**
 * Pooled counts per team from every scheme row strictly BEFORE (season, week).
 *
 * Point-in-time by construction: the cutoff is a comparison on (season, week),
 * so a backtest at week 5 cannot see week 6. This is the same discipline the
 * rest of the feature store follows and the reason the whole thing is
 * append-only.
 */
async function loadSchemeProfiles(season, week, opts) {
  const sc = cfg().model.scheme;
  // TRAITS and the FIT look back different distances on purpose. A team's
  // tendencies go stale (data.schemePriorSeason, 2025); the league-wide
  // coefficient describing how tempo fares against pressure does not, so the
  // fitter passes a wider fromSeason. Same split as Adam-7's
  // replacementBaselineStartSeason, and for the same reason.
  const priorSeason = (opts && opts.fromSeason) || cfg().data.schemePriorSeason;
  const changes = coachingChanges(season);
  if (!changes.count) {
    log.warn(`scheme: coaching-changes.json lists 0 teams for ${season} -- ` +
             `every team keeps its ${priorSeason} scheme prior. Verify the file.`);
  }

  const { rows } = await q(
    `select team, season, week, counts
       from sports.nfl_team_scheme_game
      where counts is not null
        and season >= $1
        and (season < $2 or (season = $2 and week < $3))
      order by season, week`,
    [priorSeason, season, week],
  );

  const pooled = new Map();   // team -> { key: number, games, priorGames }
  for (const r of rows) {
    const isPrior = r.season < season;
    const change = changes.teams[r.team];
    // A team that changed its staff has no usable prior-season scheme. Dropping
    // the rows sends it to the league average via shrinkage, which is the
    // honest position: we do not know what they run yet.
    if (isPrior && change && sc.coachingChangeResetToLeagueAvg) continue;

    const w = isPrior ? sc.priorSeasonWeight : 1;
    const c = typeof r.counts === "string" ? JSON.parse(r.counts) : r.counts;
    if (!pooled.has(r.team)) pooled.set(r.team, { games: 0, priorGames: 0 });
    const p = pooled.get(r.team);
    for (const [k, v] of Object.entries(c)) {
      p[k] = (p[k] || 0) + (Number(v) || 0) * w;
    }
    p.games += w;
    if (isPrior) p.priorGames += 1;
  }
  return { pooled, changes, rowCount: rows.length };
}

/** League rate for one spec, from the pooled totals of every team. */
function leagueRates(pooled) {
  const out = {};
  const add = (specs) => {
    for (const [key, [n, d]] of Object.entries(specs)) {
      let num = 0, den = 0;
      for (const p of pooled.values()) { num += p[n] || 0; den += p[d] || 0; }
      out[key] = den > 0 ? num / den : null;
    }
  };
  add(RATE_SPECS);
  add(MEAN_SPECS);
  return out;
}

/**
 * One team's shrunk rates. shrinkDenominator is in the units of each rate's own
 * denominator (snaps, dropbacks, charted box rows), which is why it is expressed
 * as games times a typical per-game count rather than as a bare play count.
 */
function shrunkRates(p, league, sc) {
  const kPlays = sc.shrinkGames * sc.typicalNeutralPlaysPerGame;
  const kDrops = sc.shrinkGames * sc.typicalNeutralDropbacksPerGame;
  const out = {};
  const pull = (specs) => {
    for (const [key, [n, d]] of Object.entries(specs)) {
      const num = p[n] || 0;
      const den = p[d] || 0;
      const lg = league[key];
      if (lg === null) { out[key] = null; continue; }
      // Dropback-denominated rates shrink on the dropback scale; everything
      // else on the snap scale. Using one constant for both would over-shrink
      // the rates whose denominator is naturally three times smaller.
      const k = d.includes("dropback") || d === "rushers_n" ? kDrops : kPlays;
      out[key] = (num + k * lg) / (den + k);
    }
  };
  pull(RATE_SPECS);
  pull(MEAN_SPECS);
  return out;
}

/**
 * The four trait axes, as z-scores against the league spread.
 *
 * Two axes per side is the ceiling the fit sample supports: four interaction
 * terms on roughly 700 team-game observations. More axes would fit the 2025
 * season's noise and present it as scheme insight.
 */
function traitAxes(rates, spread) {
  const z = (key) => {
    const v = rates[key];
    const s = spread[key];
    if (v === null || v === undefined || !s || !s.sd) return 0;
    return (v - s.mean) / s.sd;
  };
  return {
    // Offence. Aggression bundles the three things an offence chooses to do
    // pre-snap that change the shape of a play; tempo is no-huddle usage.
    aggression: (z("pa") + z("motion") + z("shotgun")) / 3,
    tempo: z("noHuddle"),
    // Defence. Pressure is how often and how heavily it rushes; front weight is
    // how many bodies it keeps near the line. NOT a coverage axis.
    pressure: (z("blitz") + z("rushers")) / 2,
    frontWeight: (z("box") + z("heavyBox")) / 2,
  };
}

/** Mean and SD of each rate across teams -- the z-score denominator. */
function leagueSpread(pooled, league, sc) {
  const perTeam = [...pooled.values()].map((p) => shrunkRates(p, league, sc));
  const keys = [...Object.keys(RATE_SPECS), ...Object.keys(MEAN_SPECS)];
  const spread = {};
  for (const k of keys) {
    const xs = perTeam.map((r) => r[k]).filter((x) => x !== null && Number.isFinite(x));
    if (xs.length < 2) { spread[k] = { mean: 0, sd: 0 }; continue; }
    const mean = xs.reduce((s, x) => s + x, 0) / xs.length;
    const varc = xs.reduce((s, x) => s + (x - mean) ** 2, 0) / (xs.length - 1);
    spread[k] = { mean, sd: Math.sqrt(varc) };
  }
  return spread;
}

/** Build the full as-of context once per tick -- it is identical across games. */
async function loadSchemeContext(season, week, opts) {
  const sc = cfg().model.scheme;
  const { pooled, changes, rowCount } = await loadSchemeProfiles(season, week, opts);
  const league = leagueRates(pooled);
  const spread = leagueSpread(pooled, league, sc);
  const traits = new Map();
  for (const [team, p] of pooled) {
    traits.set(team, {
      axes: traitAxes(shrunkRates(p, league, sc), spread),
      games: p.games,
      priorGames: p.priorGames,
      offPlays: p.off_plays || 0,
      defPlays: p.def_plays || 0,
      schemeReset: !!changes.teams[team],
    });
  }
  return { traits, league, spread, changes, rowCount, fit: loadFit() };
}

/** The four interaction products for one offence-versus-defence pairing. */
function interactionTerms(off, def) {
  return {
    aggr_pressure: off.aggression * def.pressure,
    aggr_box: off.aggression * def.frontWeight,
    tempo_pressure: off.tempo * def.pressure,
    tempo_box: off.tempo * def.frontWeight,
  };
}

/**
 * Scheme signal for a GAME TOTAL, in points.
 *
 * A total needs the SUM of what both offences are expected to do, not the
 * differential a moneyline model wants -- so each offence is evaluated against
 * the opposing defence and the two EPA deltas are added. This is the structural
 * difference between Argo-7 and Adam-7, and getting it wrong would produce a
 * model that is blind to the games where both offences gain.
 *
 * EPA is already denominated in points, so the conversion to a total is just
 * plays: expected EPA/play delta times expected plays for that offence.
 */
function schemeSignal(game, ctx) {
  const sc = cfg().model.scheme;
  const plays = cfg().model.expectedPlaysPerTeam;
  const fit = ctx.fit;

  const home = ctx.traits.get(game.home_team);
  const away = ctx.traits.get(game.away_team);
  if (!home || !away) {
    return { points: 0, confidence: 0,
             detail: { reason: "no scheme profile", home: !!home, away: !!away } };
  }
  if (!fit.fitted) {
    return { points: 0, confidence: 0,
             detail: { reason: "scheme fit not run -- see run/fit-scheme.js" } };
  }
  // SIGNIFICANCE GATE. A fit can exist and still be noise. On the first real fit
  // (587 team-game observations, 2025 + 2026) the largest interaction t-statistic
  // was 0.61 and r2 was 0.008 -- nothing distinguishable from zero. Those betas
  // are small, but "small" is not "harmless": -0.018 EPA/play across 64 plays is
  // still 1.2 points on the total, which is half of what a 0.16 delta cap
  // permits. Left ungated, the second-heaviest factor would be betting sampling
  // noise with real money-sized conviction.
  if (sc.requireSignificantFit && !fit.significant) {
    return {
      points: 0,
      confidence: 0,
      detail: {
        reason: "scheme fit not significant -- factor disabled",
        maxAbsTInteraction: fit.maxAbsTInteraction ?? null,
        r2: fit.r2 ?? null,
        n: fit.n,
      },
    };
  }

  const sides = [
    { label: "home", off: home.axes, def: away.axes },
    { label: "away", off: away.axes, def: home.axes },
  ];

  let points = 0;
  const detail = { perSide: {}, coef: fit.coef, fitN: fit.n };
  for (const s of sides) {
    const terms = interactionTerms(s.off, s.def);
    const dEpa = Object.entries(terms)
      .reduce((sum, [k, v]) => sum + (fit.coef[k] || 0) * v, 0);
    const dPts = dEpa * plays;
    points += dPts;
    detail.perSide[s.label] = {
      terms: Object.fromEntries(Object.entries(terms).map(([k, v]) => [k, +v.toFixed(4)])),
      dEpaPerPlay: +dEpa.toFixed(5),
      dPoints: +dPts.toFixed(3),
    };
  }

  // Confidence on the thinner of the two teams' samples. A matchup is only as
  // readable as its least-charted participant.
  const gamesSeen = Math.min(home.games, away.games);
  const confidence = Math.max(0, Math.min(1, gamesSeen / sc.shrinkGames));

  const capped = Math.max(-sc.maxPoints, Math.min(sc.maxPoints, points));
  return {
    points: capped,
    confidence,
    detail: {
      ...detail,
      uncapped: +points.toFixed(3),
      homeAxes: home.axes, awayAxes: away.axes,
      homeGames: home.games, awayGames: away.games,
      schemeReset: { home: home.schemeReset, away: away.schemeReset },
      coachingListVerified: ctx.changes.verified,
    },
  };
}

module.exports = {
  loadSchemeContext, loadSchemeProfiles, schemeSignal, interactionTerms,
  leagueRates, leagueSpread, shrunkRates, traitAxes, loadFit, coachingChanges,
  RATE_SPECS, MEAN_SPECS, FIT_PATH,
};
