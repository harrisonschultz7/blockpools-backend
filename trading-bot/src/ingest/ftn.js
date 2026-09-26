// trading-bot/src/ingest/ftn.js
//
// SCHEME / PLAY-STYLE tendencies, from nflverse ftn_charting.
//
// FTN is the only free source that charts HOW a play was run: motion, play
// action, screens, RPOs, no-huddle, shotgun, defenders in the box, blitzers,
// pass rushers. It is the data behind Argo-7's second-heaviest factor.
//
// WHAT IT DOES NOT HAVE: coverage shell. Zone versus man is absent from FTN and
// from every other free source (it is PFF / Sports Info Solutions territory,
// both paid), so the scheme factor is built on pressure, tempo and personnel
// axes instead. The pressure axis is not a substitute for a coverage axis, and
// nothing downstream should be read as if it were.
//
// TWO JOINED FILES. FTN carries the charting but not who had the ball or what
// the scoreboard said; play-by-play carries both. They join on
// (nflverse_game_id, nflverse_play_id) -> (game_id, play_id). So this ingest
// streams pbp into a slim index first, then streams FTN against it.
//
// NEUTRAL SCRIPT ONLY. A trailing team runs more no-huddle and throws more --
// that is the deficit talking, not its scheme. Every rate here is measured
// inside config.model.scheme.neutral, and the surviving play count is stored so
// a thin game can be shrunk rather than trusted.
//
// FTN LAGS. Checked 2026-09-26: the 2026 file held weeks 1-2 complete and a
// single week-3 game. Current-season tendencies are therefore always a couple
// of weeks stale, which is why features/scheme.js blends a prior-season anchor.

const { cfg, ENV } = require("../config");
const { getText, streamCsv, num, str } = require("../http");
const { bulkInsert } = require("../db");
const log = require("../log");

const url = (tag, file) => `${ENV.NFLVERSE_BASE}/${tag}/${file}`;

// Only the columns actually used, projected at parse time -- a full-object
// parse of these files is what exhausted the heap on play-by-play.
const FTN_COLS = [
  "nflverse_game_id", "nflverse_play_id", "season", "week",
  "qb_location", "n_offense_backfield", "n_defense_box",
  "is_no_huddle", "is_motion", "is_play_action", "is_screen_pass", "is_rpo",
  "n_blitzers", "n_pass_rushers",
];

// From pbp: possession, game state, and the EPA that the interaction is fitted
// against. Deliberately a different list from nflverse.js PBP_COLS, which
// aggregates a different thing.
const PBP_JOIN_COLS = [
  "game_id", "play_id", "season", "week", "posteam", "defteam",
  "epa", "wp", "qtr", "score_differential", "pass", "rush", "aborted_play",
];

// FTN writes booleans as the strings TRUE/FALSE, not 0/1, so http.js bool()
// -- which goes through Number() -- returns null for every one of them. Reading
// these with the shared helper would silently zero the entire factor.
const flag = (v) => v === "TRUE" || v === "true" || v === "1";

// "0" is FTN's NOT-CHARTED sentinel, not a measurement: it appears in
// qb_location, starting_hash and n_defense_box together on the same ~24% of
// rows (kicks, and plays the charters skipped). qb_location is the cleanest
// gate, because n_offense_backfield genuinely can be 0 on an empty formation
// and filtering on that field would throw away real empty-set snaps.
const isCharted = (r) => str(r.qb_location) !== null && r.qb_location !== "0";

// A dropback is defined from FTN's own n_pass_rushers rather than from pbp's
// pass flag, so the blitz and play-action denominators come from the same file
// as their numerators. Mixing sources here produced rates above 1 in testing.
const isDropback = (r) => isCharted(r) && (num(r.n_pass_rushers) || 0) > 0;

// pbp play_id is written as an integer in some seasons and a float in others.
const joinKey = (gameId, playId) => `${gameId}|${Number(playId)}`;

/**
 * Slim (game_id, play_id) -> game-state index for one season of play-by-play.
 *
 * Scrimmage plays only, and only the fields the join needs. The source text is
 * scoped to this function so it can be collected before the FTN file is
 * fetched; holding both at once is what pushes this over the default heap.
 */
async function buildPbpIndex(season) {
  const text = await getText(url("pbp", `play_by_play_${season}.csv`));
  const index = new Map();
  streamCsv(text, PBP_JOIN_COLS, (p) => {
    const isPass = num(p.pass) === 1;
    const isRush = num(p.rush) === 1;
    if (!isPass && !isRush) return;
    if (num(p.aborted_play) === 1) return;
    const off = str(p.posteam);
    const def = str(p.defteam);
    const gid = str(p.game_id);
    if (!off || !def || !gid) return;
    index.set(joinKey(gid, p.play_id), {
      off, def,
      season: num(p.season),
      week: num(p.week),
      epa: num(p.epa),
      wp: num(p.wp),
      qtr: num(p.qtr),
      margin: num(p.score_differential),
    });
  });
  return index;
}

/** Is this play inside the neutral-script band? */
function isNeutral(state, n) {
  if (state.wp === null || state.qtr === null) return false;
  if (state.wp <= n.wpMin || state.wp >= n.wpMax) return false;
  if (state.qtr > n.maxQuarter) return false;
  // score_differential is missing on a handful of rows; treat that as neutral
  // rather than dropping the play, since qtr and wp have already gated it.
  if (state.margin !== null && Math.abs(state.margin) > n.maxAbsMargin) return false;
  return true;
}

/**
 * Accumulates scheme tendencies per (game, team).
 *
 * Each team collects TWO sets in the same game: offensive tendencies from plays
 * where it had the ball, defensive tendencies from plays where it did not. The
 * denominators differ per rate on purpose -- play action and blitz are measured
 * on dropbacks, motion and no-huddle on all charted snaps, box count only on
 * rows that actually carry one. A single shared denominator would quietly
 * deflate every rate by the uncharted share.
 */
function createSchemeAccumulator(neutral) {
  const games = new Map();   // game_id -> { meta, teams: Map<team, acc> }

  const blank = () => ({
    // offence
    offPlays: 0, offDropbacks: 0,
    motion: 0, pa: 0, screen: 0, rpo: 0, noHuddle: 0, shotgun: 0,
    backfield: [], offEpa: [],
    // defence
    defPlays: 0, defDropbacks: 0,
    blitz: 0, rushers: [], box: [], heavyBox: 0, boxRows: 0,
  });

  const teamAcc = (gid, meta, team) => {
    if (!games.has(gid)) games.set(gid, { meta, teams: new Map() });
    const g = games.get(gid);
    if (!g.teams.has(team)) g.teams.set(team, blank());
    return g.teams.get(team);
  };

  /** One FTN row joined to its pbp game state. */
  const push = (r, state) => {
    if (!isCharted(r)) return;
    if (!isNeutral(state, neutral)) return;
    const gid = str(r.nflverse_game_id);
    if (!gid) return;
    const meta = { season: state.season, week: state.week };
    const drop = isDropback(r);

    // -- the offence that ran the play ------------------------------------
    const o = teamAcc(gid, meta, state.off);
    o.offPlays++;
    if (drop) o.offDropbacks++;
    if (flag(r.is_motion)) o.motion++;
    if (flag(r.is_rpo)) o.rpo++;
    if (flag(r.is_no_huddle)) o.noHuddle++;
    // Pistol counts with shotgun: both are off-centre snaps, and separating
    // them at this sample size splits a thin cell for no gain.
    if (r.qb_location === "S" || r.qb_location === "P") o.shotgun++;
    if (drop) {
      if (flag(r.is_play_action)) o.pa++;
      if (flag(r.is_screen_pass)) o.screen++;
    }
    const backs = num(r.n_offense_backfield);
    if (backs !== null) o.backfield.push(backs);
    if (state.epa !== null) o.offEpa.push(state.epa);

    // -- the defence it ran against ---------------------------------------
    const d = teamAcc(gid, meta, state.def);
    d.defPlays++;
    const box = num(r.n_defense_box);
    if (box !== null && box > 0) {
      d.box.push(box);
      d.boxRows++;
      if (box >= 7) d.heavyBox++;
    }
    if (drop) {
      d.defDropbacks++;
      if ((num(r.n_blitzers) || 0) > 0) d.blitz++;
      const pr = num(r.n_pass_rushers);
      if (pr !== null && pr > 0) d.rushers.push(pr);
    }
  };

  const finish = (minPlays, minDropbacks) => {
    // Both gates are required. A caller that passes only one leaves the other
    // undefined, every `n >= undefined` comparison is false, and the result is
    // a full table of nulls that looks like a data problem rather than a bug --
    // which is exactly what happened the first time this was run.
    if (!Number.isFinite(minPlays) || !Number.isFinite(minDropbacks)) {
      throw new Error(`finish() needs both gates, got (${minPlays}, ${minDropbacks})`);
    }
    const sum = (xs) => xs.reduce((s, x) => s + x, 0);
    const mean = (xs) => (xs.length ? sum(xs) / xs.length : null);
    const rate = (n, d) => (d > 0 ? n / d : null);
    const out = [];

    for (const [gid, g] of games) {
      const teams = [...g.teams.keys()];
      if (teams.length !== 2) continue;              // partial or corrupt game
      for (const team of teams) {
        const a = g.teams.get(team);
        const opponent = teams.find((t) => t !== team);

        // The two sides are gated INDEPENDENTLY. In one game a team can hold
        // the ball for 39 neutral snaps and defend only 6, and a blitz rate
        // computed off two dropbacks is not a measurement -- it is 0 or 1. The
        // thin side is nulled rather than stored, so the feature layer can tell
        // "no reading" apart from "a reading of zero".
        const offOk = a.offPlays >= minPlays;
        const defOk = a.defPlays >= minPlays;
        const offDropOk = offOk && a.offDropbacks >= minDropbacks;
        const defDropOk = defOk && a.defDropbacks >= minDropbacks;
        if (!offOk && !defOk) continue;

        out.push({
          game_id: gid,
          team,
          opponent,
          season: g.meta.season,
          week: g.meta.week,

          off_neutral_plays: a.offPlays,
          off_motion_rate: offOk ? rate(a.motion, a.offPlays) : null,
          off_pa_rate: offDropOk ? rate(a.pa, a.offDropbacks) : null,
          off_screen_rate: offDropOk ? rate(a.screen, a.offDropbacks) : null,
          off_rpo_rate: offOk ? rate(a.rpo, a.offPlays) : null,
          off_no_huddle_rate: offOk ? rate(a.noHuddle, a.offPlays) : null,
          off_shotgun_rate: offOk ? rate(a.shotgun, a.offPlays) : null,
          off_backfield_avg: offOk ? mean(a.backfield) : null,
          off_epa_neutral: offOk ? mean(a.offEpa) : null,

          def_neutral_plays: a.defPlays,
          def_blitz_rate: defDropOk ? rate(a.blitz, a.defDropbacks) : null,
          def_rushers_avg: defDropOk ? mean(a.rushers) : null,
          def_box_avg: defOk ? mean(a.box) : null,
          def_heavy_box_rate: defOk ? rate(a.heavyBox, a.boxRows) : null,

          // Raw counts, so a team's season-to-date rate is sum(numerators) over
          // sum(denominators) rather than a mean of per-game rates. Gates are
          // NOT applied here: a thin game still contributes its handful of
          // snaps honestly once pooled, which is the whole reason to pool.
          counts: JSON.stringify({
            off_plays: a.offPlays, off_dropbacks: a.offDropbacks,
            motion_n: a.motion, pa_n: a.pa, screen_n: a.screen, rpo_n: a.rpo,
            no_huddle_n: a.noHuddle, shotgun_n: a.shotgun,
            backfield_sum: sum(a.backfield), backfield_n: a.backfield.length,
            epa_sum: sum(a.offEpa), epa_n: a.offEpa.length,
            def_plays: a.defPlays, def_dropbacks: a.defDropbacks,
            blitz_n: a.blitz,
            rushers_sum: sum(a.rushers), rushers_n: a.rushers.length,
            box_sum: sum(a.box), box_n: a.boxRows, heavy_box_n: a.heavyBox,
          }),
        });
      }
    }
    return out;
  };

  return { push, finish };
}

/** Ingest one season of FTN charting into sports.nfl_team_scheme_game. */
async function ingestFtnSeason(season) {
  const sc = cfg().model.scheme;
  const index = await buildPbpIndex(season);
  if (!index.size) { log.warn(`ftn ${season}: no pbp to join against`); return 0; }

  const acc = createSchemeAccumulator(sc.neutral);
  let charted = 0;
  let unjoined = 0;
  const text = await getText(url("ftn_charting", `ftn_charting_${season}.csv`));
  streamCsv(text, FTN_COLS, (r) => {
    const state = index.get(joinKey(r.nflverse_game_id, r.nflverse_play_id));
    // Unjoined rows are expected and benign: FTN charts kicks and other
    // non-scrimmage plays that the pbp index deliberately excludes. A LARGE
    // unjoined share is not benign -- it means the key drifted -- so it is
    // logged as a ratio rather than swallowed.
    if (!state) { unjoined++; return; }
    charted++;
    acc.push(r, state);
  });

  const rows = acc.finish(sc.minNeutralPlaysPerGame, sc.minNeutralDropbacksPerGame);
  const joinRate = charted / (charted + unjoined);
  if (joinRate < 0.4) {
    log.warn(`ftn ${season}: only ${(joinRate * 100).toFixed(0)}% of rows joined to pbp -- check the play_id key`);
  }
  if (!rows.length) { log.warn(`ftn ${season}: no team-game rows survived the neutral filter`); return 0; }

  const cols = Object.keys(rows[0]);
  const updates = cols.filter((c) => !["game_id", "team"].includes(c))
    .map((c) => `${c} = excluded.${c}`).join(", ");
  await bulkInsert("sports.nfl_team_scheme_game", cols, rows, {
    onConflict: `on conflict (game_id, team) do update set ${updates}, computed_at = now()`,
  });
  log(`ftn ${season}: ${charted} joined plays (${(joinRate * 100).toFixed(0)}%) -> ${rows.length} team-game rows`);
  return rows.length;
}

/** Current season plus the prior-season anchor the scheme factor blends in. */
async function ingestFtnAll() {
  const c = cfg();
  const now = new Date();
  const current = now.getMonth() >= 2 ? now.getFullYear() : now.getFullYear() - 1;
  const seasons = [...new Set([c.data.schemePriorSeason, c.data.startSeason, current])]
    .filter((s) => Number.isFinite(s) && s >= c.data.schemePriorSeason)
    .sort();
  let total = 0;
  for (const s of seasons) {
    try { total += await ingestFtnSeason(s); }
    catch (e) { log.warn(`ftn ${s}: ${e.message}`); }
  }
  return total;
}

module.exports = {
  ingestFtnSeason, ingestFtnAll, buildPbpIndex, createSchemeAccumulator,
  isNeutral, isCharted, isDropback, flag, joinKey, FTN_COLS, PBP_JOIN_COLS,
};
