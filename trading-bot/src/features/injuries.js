// trading-bot/src/features/injuries.js
//
// INJURIES (weight 0.25): BASELINED, not raw player value.
//
// The whole point: a missing player is only worth what his replacement is NOT.
// The Rams without Nacua are not "the Rams minus Nacua's production" -- they
// are the Rams with whoever plays instead, and that team has a record.
//
//   penalty = playerImportance x statusMultiplier x (1 - replacementQuality)
//
// replacementQuality is measured, not assumed: how did this team actually
// perform in the games this player missed?
//
// THE TRAP THIS GUARDS AGAINST: in 2025 the Rams played exactly ONE game
// without Nacua. An unshrunk estimate would read that single game as proof
// and size up on it. Every measured quality is therefore pulled back toward
// league-average positional replacement by n / (n + shrinkGamesWithout), so
// one game moves the number barely at all and eight games moves it most of
// the way. Without this the factor is a noise generator.
//
// Name joining: snap_counts keys on pfr_player_id, injuries on gsis_id, and
// the two never share an id -- so the join is (team, normalised name).

const { cfg } = require("../config");
const { q } = require("../db");

// The injury report's position codes are not the config's. Map before lookup,
// or an offensive tackle ("T") silently falls through to the 0.08 default and
// a left tackle going down reads as a special-teamer.
const POS_ALIAS = {
  T: "OT", LT: "OT", RT: "OT", OL: "OG", G: "OG", LG: "OG", RG: "OG",
  NT: "DT", DL: "DT", OLB: "EDGE", ILB: "LB", MLB: "LB",
  FS: "S", SS: "S", DB: "CB", FB: "RB", WR: "WR", PK: "K", P: "K", LS: "K",
};
const canonPos = (p) => POS_ALIAS[String(p || "").toUpperCase()] || String(p || "").toUpperCase();

/** "Michael Penix Jr." and "Michael Penix" must collide. */
function normName(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/\b(jr|sr|ii|iii|iv|v)\.?\b/g, "")
    .replace(/[^a-z ]/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Latest injury report for a team as of a moment.
 *
 * The week's report may not exist yet -- the NFL posts Wednesday, so on a
 * Tuesday the newest data is last week's. Rather than return nothing (which
 * would read as "nobody is hurt", the most dangerous possible default), fall
 * back to the most recent posted week and report how stale it is so the caller
 * can discount it.
 */
async function currentReport(team, season, week, asOf) {
  const wk = await q(
    `select max(week) w from sports.nfl_injuries
      where team = $1 and season = $2 and week <= $3
        and (observed_at <= $4 or $4 < (select min(observed_at) from sports.nfl_injuries))`,
    [team, season, week, asOf],
  );
  const useWeek = wk.rows[0] && wk.rows[0].w;
  if (useWeek === null || useWeek === undefined) return { rows: [], week: null, staleWeeks: null };

  const { rows } = await q(
    `select distinct on (gsis_id, full_name)
            full_name, position, report_status, practice_status, observed_at
       from sports.nfl_injuries
      where team = $1 and season = $2 and week = $3
        and (observed_at <= $4 or $4 < (select min(observed_at) from sports.nfl_injuries))
      order by gsis_id, full_name, observed_at desc`,
    [team, season, useWeek, asOf],
  );
  return {
    rows: rows.filter((r) => ["Out", "Doubtful", "Questionable"].includes(r.report_status)),
    week: useWeek,
    staleWeeks: week - useWeek,
  };
}

/**
 * Snap share + the with/without baseline for one player.
 * Returns null when we have no snap history at all -- a player who has never
 * taken a snap cannot be a meaningful absence.
 */
async function playerBaseline(team, playerName, asOf) {
  const m = cfg().model.injury;
  const startSeason = cfg().data.replacementBaselineStartSeason;
  const key = normName(playerName);

  // Games the team played before asOf, and whether this player was on the field.
  const { rows } = await q(
    `select g.game_id,
            t.off_epa_per_play, t.def_epa_per_play,
            max(case when s.pfr_player_id is not null then 1 else 0 end) played,
            max(coalesce(s.offense_pct, 0)) off_pct,
            max(coalesce(s.defense_pct, 0)) def_pct
       from sports.nfl_games g
       join sports.nfl_team_game_stats t
         on t.game_id = g.game_id and t.team = $1
       left join sports.nfl_snap_counts s
         on s.game_id = g.game_id and s.team = $1
        and regexp_replace(
              regexp_replace(lower(s.player), '\m(jr|sr|ii|iii|iv|v)\.?\M', '', 'g'),
              '[^a-z ]', '', 'g') ~ ('^\s*' || $4 || '\s*$')
      where g.kickoff < $2 and g.season >= $3
      group by g.game_id, t.off_epa_per_play, t.def_epa_per_play, g.kickoff
      order by g.kickoff`,
    [team, asOf, startSeason, key],
  );
  if (!rows.length) return null;

  const withP = rows.filter((r) => r.played === 1);
  const withoutP = rows.filter((r) => r.played === 0);
  if (!withP.length) return null;                 // never played: no signal

  const net = (r) => Number(r.off_epa_per_play) - Number(r.def_epa_per_play);
  const avg = (xs) => xs.reduce((s, x) => s + net(x), 0) / xs.length;

  const snapShare = Math.max(
    ...withP.map((r) => Math.max(Number(r.off_pct) || 0, Number(r.def_pct) || 0)),
  );

  // Measured quality: 1.0 = the team was just as good without him.
  let quality = m.defaultReplacementQuality;
  if (withoutP.length) {
    const drop = avg(withP) - avg(withoutP);      // > 0 means worse without him
    const raw = 1 - drop / m.replacementScaleEpa;
    const clamped = Math.max(0, Math.min(1, raw));
    // Shrink toward the positional prior by sample size. One game barely moves it.
    const k = withoutP.length / (withoutP.length + m.shrinkGamesWithout);
    quality = m.defaultReplacementQuality + (clamped - m.defaultReplacementQuality) * k;
  }

  return {
    snapShare,
    gamesWith: withP.length,
    gamesWithout: withoutP.length,
    replacementQuality: quality,
    shrinkWeight: withoutP.length / (withoutP.length + m.shrinkGamesWithout),
  };
}

/** Total injury penalty for one team, in probability points (always >= 0). */
async function teamInjuryPenalty(team, season, week, asOf) {
  const m = cfg().model.injury;
  const report = await currentReport(team, season, week, asOf);
  const items = [];
  let penalty = 0;

  for (const r of report.rows) {
    const base = await playerBaseline(team, r.full_name, asOf);
    if (!base) continue;
    const posValue = m.positionValue[canonPos(r.position)] ?? m.positionValue.default;
    const statusMult = m.statusMultiplier[r.report_status] ?? 0;

    const importance = base.snapShare * posValue;
    const cost = importance * statusMult * (1 - base.replacementQuality);
    penalty += cost;

    if (cost > 0.001) {
      items.push({
        player: r.full_name,
        pos: canonPos(r.position),
        status: r.report_status,
        snapShare: +base.snapShare.toFixed(3),
        replacementQuality: +base.replacementQuality.toFixed(3),
        gamesWithout: base.gamesWithout,
        cost: +cost.toFixed(4),
      });
    }
  }

  items.sort((a, b) => b.cost - a.cost);
  return { penalty, items, reportWeek: report.week, staleWeeks: report.staleWeeks };
}

/**
 * Injury signal for a game, in probability points favouring the home team.
 * Positive = the away team is the more damaged side.
 */
async function injurySignal(game, asOf) {
  const m = cfg().model.injury;
  const [home, away] = await Promise.all([
    teamInjuryPenalty(game.home_team, game.season, game.week, asOf),
    teamInjuryPenalty(game.away_team, game.season, game.week, asOf),
  ]);

  const raw = away.penalty - home.penalty;
  const signal = Math.max(-m.maxProbSwing, Math.min(m.maxProbSwing, raw));

  // Confidence reflects how current the report is. A week-old "Out" still
  // carries real information -- most players ruled out stay out -- but it is
  // not the same as Friday's report, and a missing report is not good news.
  const stale = Math.max(home.staleWeeks ?? 9, away.staleWeeks ?? 9);
  const confidence = stale === 0 ? 1 : stale === 1 ? 0.6 : stale <= 2 ? 0.3 : 0.1;
  return {
    signal,
    confidence,
    detail: {
      homePenalty: +home.penalty.toFixed(4),
      awayPenalty: +away.penalty.toFixed(4),
      capped: Math.abs(raw) > m.maxProbSwing,
      reportWeek: home.reportWeek,
      staleWeeks: stale,
      home: home.items.slice(0, 5),
      away: away.items.slice(0, 5),
    },
  };
}

module.exports = { injurySignal, teamInjuryPenalty, playerBaseline, currentReport, normName, canonPos };
