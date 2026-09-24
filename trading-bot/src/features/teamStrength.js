// trading-bot/src/features/teamStrength.js
//
// MOMENTUM (weight 0.30): opponent-adjusted, recency-weighted EPA per play.
//
// Raw EPA/play is not team strength -- it is team strength plus schedule. A
// team that has played three bottom-five defences looks elite until it plays
// someone. So offence and defence ratings are solved jointly: a team's offence
// rating is its EPA above what its opponents' defences usually allow, and vice
// versa, iterated until stable. That is the cheap version of a ridge
// regression and it converges in a dozen passes at this data size.
//
// POINT-IN-TIME: only games that kicked off strictly before `asOf` are read.
// Every query here filters on kickoff, never on week, because a postponed or
// flexed game would otherwise leak a result the model could not have known.
//
// NOTE ON HOME FIELD: no home-field term anywhere. This is a residual model --
// the market price already contains home field, and adding it again would
// double-count the single best-known effect in the sport.

const { cfg } = require("../config");
const { q } = require("../db");

/**
 * Solve offence/defence ratings for every team, as of a moment in time.
 * Returns Map<team, { off, def, net, games }>, all in EPA/play units where
 * `off` higher is better and `def` LOWER is better (it is EPA allowed).
 */
async function solveRatings(asOf) {
  const c = cfg().model.momentum;
  const { startSeason } = cfg().data;

  const { rows } = await q(
    `select s.team, s.opponent, s.off_epa_per_play, s.def_epa_per_play, g.kickoff
       from sports.nfl_team_game_stats s
       join sports.nfl_games g on g.game_id = s.game_id
      where g.kickoff < $1
        and s.season >= $2
        and s.off_epa_per_play is not null
        and s.def_epa_per_play is not null
      order by g.kickoff`,
    [asOf, startSeason],
  );
  if (!rows.length) return new Map();

  // Recency weight: a game `halfLifeGames` back counts half as much. Indexed
  // per team so a team on a bye is not penalised for the calendar gap.
  const perTeam = new Map();
  for (const r of rows) {
    if (!perTeam.has(r.team)) perTeam.set(r.team, []);
    perTeam.get(r.team).push(r);
  }
  const weighted = [];
  for (const [team, games] of perTeam) {
    const n = games.length;
    games.forEach((g, i) => {
      const gamesAgo = n - 1 - i;                    // 0 = most recent
      weighted.push({
        team,
        opponent: g.opponent,
        off: Number(g.off_epa_per_play),
        def: Number(g.def_epa_per_play),
        w: Math.pow(0.5, gamesAgo / c.halfLifeGames),
      });
    });
  }

  const teams = [...perTeam.keys()];
  const off = new Map(teams.map((t) => [t, 0]));
  const def = new Map(teams.map((t) => [t, 0]));

  const wmean = (items, pick) => {
    let sw = 0, sx = 0;
    for (const it of items) { sw += it.w; sx += it.w * pick(it); }
    return sw > 0 ? sx / sw : 0;
  };

  // Alternate: offence is measured against opponents' defence ratings, then
  // defence against opponents' offence ratings, until both settle.
  for (let iter = 0; iter < c.ridgeIterations; iter++) {
    for (const t of teams) {
      const mine = weighted.filter((x) => x.team === t);
      off.set(t, wmean(mine, (x) => x.off - (def.get(x.opponent) || 0)));
    }
    for (const t of teams) {
      const mine = weighted.filter((x) => x.team === t);
      def.set(t, wmean(mine, (x) => x.def - (off.get(x.opponent) || 0)));
    }
  }

  // Shrink toward league average for teams with few games on record -- in
  // week 2 a single blowout should not make a team look like a juggernaut.
  const out = new Map();
  for (const t of teams) {
    const n = perTeam.get(t).length;
    const k = n / (n + c.shrinkGames);
    const o = off.get(t) * k;
    const d = def.get(t) * k;
    out.set(t, { off: o, def: d, net: o - d, games: n });
  }
  return out;
}

/**
 * Momentum signal for one game, in raw EPA/play differential (home - away).
 * Conversion to probability happens in the model layer, so the factor stays
 * in its natural unit and is comparable across weeks.
 */
function momentumSignal(ratings, homeTeam, awayTeam) {
  const h = ratings.get(homeTeam);
  const a = ratings.get(awayTeam);
  if (!h || !a) return { signal: 0, confidence: 0, detail: "missing ratings" };

  const signal = h.net - a.net;
  // Confidence rises with the thinner team's sample -- a rating built on two
  // games is a guess regardless of how extreme it looks.
  const minGames = Math.min(h.games, a.games);
  const confidence = Math.min(1, minGames / 8);
  return {
    signal,
    confidence,
    detail: {
      home: { team: homeTeam, ...round(h) },
      away: { team: awayTeam, ...round(a) },
    },
  };
}

const round = (r) => ({
  off: +r.off.toFixed(4), def: +r.def.toFixed(4),
  net: +r.net.toFixed(4), games: r.games,
});

module.exports = { solveRatings, momentumSignal };
