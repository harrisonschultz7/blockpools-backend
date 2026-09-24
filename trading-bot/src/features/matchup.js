// trading-bot/src/features/matchup.js
//
// MATCHUP (weight 0.25): the pace x defence INTERACTION.
//
// This is the Lions/Bills case. When two fast offences meet, the game has more
// snaps in it, and defensive EPA-per-play is therefore applied more times.
// Defence mechanically matters more in that specific game -- not because the
// defences changed, but because the sample they face is larger. An additive
// "matchup rating" cannot express that; it needs a multiplicative term.
//
//   expected_plays = f(pace of both offences)
//   defensive weight = 1 + amplification * (expected_plays / league_avg - 1)
//
// Second term: style mismatch. A defence that is strong against the pass facing
// a pass-heavy offence is worth more than its overall rating implies.
//
// POINT-IN-TIME: reads only games kicked off before `asOf`.

const { cfg } = require("../config");
const { q } = require("../db");

/** Recency-weighted pace and style profile per team, as of a moment. */
async function loadProfiles(asOf) {
  const { startSeason } = cfg().data;
  const { halfLifeGames } = cfg().model.momentum;

  const { rows } = await q(
    `select s.team, s.sec_per_play, s.off_plays, s.plays_total, s.off_pass_rate,
            s.off_pass_epa, s.off_rush_epa, s.def_epa_per_play,
            s.def_pass_epa, s.def_rush_epa, g.kickoff
       from sports.nfl_team_game_stats s
       join sports.nfl_games g on g.game_id = s.game_id
      where g.kickoff < $1 and s.season >= $2 and s.sec_per_play is not null
      order by g.kickoff`,
    [asOf, startSeason],
  );

  const byTeam = new Map();
  for (const r of rows) {
    if (!byTeam.has(r.team)) byTeam.set(r.team, []);
    byTeam.get(r.team).push(r);
  }

  const out = new Map();
  for (const [team, games] of byTeam) {
    const n = games.length;
    let sw = 0;
    const acc = { secPerPlay: 0, offPlays: 0, passRate: 0, passEpa: 0, rushEpa: 0,
                  defPassEpa: 0, defRushEpa: 0 };
    games.forEach((g, i) => {
      const w = Math.pow(0.5, (n - 1 - i) / halfLifeGames);
      sw += w;
      acc.secPerPlay += w * Number(g.sec_per_play);
      acc.offPlays += w * Number(g.off_plays || 0);
      acc.passRate += w * Number(g.off_pass_rate || 0);
      acc.passEpa += w * Number(g.off_pass_epa || 0);
      acc.rushEpa += w * Number(g.off_rush_epa || 0);
      acc.defPassEpa += w * Number(g.def_pass_epa || 0);
      acc.defRushEpa += w * Number(g.def_rush_epa || 0);
    });
    if (sw <= 0) continue;
    out.set(team, {
      secPerPlay: acc.secPerPlay / sw,
      offPlays: acc.offPlays / sw,
      passRate: acc.passRate / sw,
      passEpa: acc.passEpa / sw,
      rushEpa: acc.rushEpa / sw,
      defPassEpa: acc.defPassEpa / sw,
      defRushEpa: acc.defRushEpa / sw,
      games: n,
    });
  }
  return out;
}

/**
 * Matchup signal for one game, in EPA/play units favouring the home team.
 *
 * Returns the defensive-weight multiplier alongside the signal so a decision
 * can be explained afterwards ("we liked the under-dog because this projected
 * as a 148-play game and their defence is the better one").
 */
function matchupSignal(profiles, ratings, homeTeam, awayTeam) {
  const c = cfg().model.matchup;
  const hp = profiles.get(homeTeam);
  const ap = profiles.get(awayTeam);
  const hr = ratings.get(homeTeam);
  const ar = ratings.get(awayTeam);
  if (!hp || !ap || !hr || !ar) {
    return { signal: 0, confidence: 0, detail: "missing profiles" };
  }

  // Faster offence = fewer seconds per snap = more snaps for both teams.
  const expectedPlays = hp.offPlays + ap.offPlays;
  const paceRatio = expectedPlays / c.leagueAvgPlays;
  const defWeight = 1 + c.paceAmplification * (paceRatio - 1);

  // Defensive edge: def rating is EPA ALLOWED, so the lower one is better.
  // Positive defEdge means the home defence is the stronger of the two.
  const defEdge = ar.def - hr.def;

  // Style mismatch: does a defence happen to be strong against the thing this
  // particular opponent does most? A defence specialised against the pass is
  // worth more than its overall rating implies when it draws a pass-heavy
  // offence, and less when it draws a run-first one.
  //   specialisation > 0  => better against the pass than the run
  //   passLean > 0        => opponent throws more than league average
  const LEAGUE_PASS_RATE = 0.56;
  const homeSpec = hp.defRushEpa - hp.defPassEpa;
  const awaySpec = ap.defRushEpa - ap.defPassEpa;
  const homeStyleEdge = homeSpec * (ap.passRate - LEAGUE_PASS_RATE);
  const awayStyleEdge = awaySpec * (hp.passRate - LEAGUE_PASS_RATE);
  const styleEdge = (homeStyleEdge - awayStyleEdge) * c.styleMismatchWeight;
  // The interaction: the defensive edge is scaled UP in a high-play game and
  // damped in a slow one. This term is the whole point of the factor.
  const signal = defEdge * defWeight + styleEdge;

  const confidence = Math.min(1, Math.min(hp.games, ap.games) / 8);
  return {
    signal,
    confidence,
    detail: {
      expectedPlays: +expectedPlays.toFixed(1),
      paceRatio: +paceRatio.toFixed(3),
      defWeight: +defWeight.toFixed(3),
      defEdge: +defEdge.toFixed(4),
      styleEdge: +styleEdge.toFixed(4),
      homeDefSpec: +homeSpec.toFixed(4),
      awayDefSpec: +awaySpec.toFixed(4),
      homeSecPerPlay: +hp.secPerPlay.toFixed(2),
      awaySecPerPlay: +ap.secPerPlay.toFixed(2),
    },
  };
}

module.exports = { loadProfiles, matchupSignal };
