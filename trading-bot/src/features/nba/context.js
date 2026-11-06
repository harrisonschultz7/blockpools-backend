// trading-bot/src/features/nba/context.js
//
// Everything Axis-3's factors read, loaded ONCE per tick for the teams being
// priced -- aggregated in SQL so a tick returns a few hundred rows rather than
// every box-score line of the season (Supabase egress is the binding limit; see
// the 2026-10 egress work).
//
//   teams   -> per team: recent results (margin, streak), schedule (rest, travel),
//              pace and 3-point rate from its box scores
//   players -> per rostered player: minutes, on-court +/-, with/without margins,
//              recent availability
//   injuries-> the current report for those teams
//   league  -> league-average pace and 3-point rate (the style baseline)

const { cfg } = require("../../config");
const { q } = require("../../db");

/** Seasons in play: this one and the last (early-season samples are tiny). */
function seasonFloor(asOf) {
  const d = new Date(asOf);
  // NBA seasons start in October; Jan-Sep belong to the season that began the
  // previous calendar year.
  const start = d.getUTCMonth() >= 8 ? d.getUTCFullYear() : d.getUTCFullYear() - 1;
  return start - 1;
}

async function loadNbaContext(teams, asOf) {
  const m = cfg().model;
  const floor = seasonFloor(asOf);
  const lookback = m.injury.lookbackGames;     // team games for player impact
  const recentN = m.injury.recentGames;        // "has he been playing lately"

  // ── Team game logs (results + where each game was) ─────────────────────────
  const { rows: games } = await q(
    `select t.team, g.game_id, g.kickoff, g.game_date::text as game_date, g.season, g.neutral_site,
            g.home_team, g.away_team, (g.home_team = t.team) as is_home,
            case when g.home_team = t.team then g.home_score - g.away_score
                 else g.away_score - g.home_score end as margin
       from unnest($1::text[]) t(team)
       join sports.nba_games g on t.team in (g.home_team, g.away_team)
      where g.home_score is not null and g.kickoff < $2 and g.season >= $3
        and g.season_type in (2, 5)
      order by t.team, g.kickoff desc`,
    [teams, asOf, floor]);

  // ── Players: one aggregated row each ───────────────────────────────────────
  // THE ROSTER is whoever played for the team in its last rotationGames this season
  // and has not since played for someone else, plus anyone on its injury report.
  // It used to be "in the team's last box score", on the belief that an injured
  // player still appears as a DNP. Often he does not: Duren and Isaiah Joe were not
  // in DET's box on 2026-11-04 at all, so an absent star dropped off the roster and
  // his return could never register.
  //
  // first_rn is the oldest team game he appears in, so games before he joined are
  // not read as games he missed.
  const { rows: players } = await q(
    `with tg as (
       select t.team, g.game_id, g.season,
              case when g.home_team = t.team then g.home_score - g.away_score
                   else g.away_score - g.home_score end as margin,
              row_number() over (partition by t.team order by g.kickoff desc) as rn
         from unnest($1::text[]) t(team)
         join sports.nba_games g on t.team in (g.home_team, g.away_team)
        where g.home_score is not null and g.kickoff < $2 and g.season >= $3
          and g.season_type in (2, 5)
     ), tgl as (select * from tg where rn <= $4),
     cur as (select max(season) as season from tg),
     pl as (
       select p.player_id, p.player, p.minutes, p.plus_minus, tgl.team, tgl.rn, tgl.margin, tgl.season,
              -- UNAVAILABLE, not merely unused: a coach's-decision DNP is a
              -- healthy bench player, and reading him as "out" makes him a star
              -- "returning" every time he is not on the report (2026-11-01:
              -- CLE's deep bench was worth +3.5 pts that way).
              (p.minutes = 0 and coalesce(p.dnp_reason, '') not ilike '%coach%') as unavailable,
              (p.minutes = 0 and coalesce(p.dnp_reason, '') ilike '%coach%') as coach_dnp
         from sports.nba_player_games p
         join tgl on tgl.game_id = p.game_id and tgl.team = p.team
     ),
     latest as (
       select distinct on (p.player_id) p.player_id, p.team
         from sports.nba_player_games p join sports.nba_games g on g.game_id = p.game_id
        where p.minutes > 0 and g.season >= $3
        order by p.player_id, g.kickoff desc
     ),
     roster as (
       select distinct pl.team, pl.player_id from pl
         join cur on pl.season = cur.season
         join latest l on l.player_id = pl.player_id and l.team = pl.team
        where pl.rn <= $6 and pl.minutes > 0
       union
       select i.team, i.player_id from sports.nba_injuries i where i.team = any($1)
     ),
     agg as (
     select r.team, r.player_id, max(pl.player) as player,
            count(*) filter (where pl.minutes > 0)                      as gp,
            coalesce(sum(pl.minutes)    filter (where pl.minutes > 0), 0) as min_sum,
            coalesce(sum(pl.plus_minus) filter (where pl.minutes > 0), 0) as pm_sum,
            max(pl.rn)                                                  as first_rn,
            count(*) filter (where pl.rn <= $5 and pl.minutes > 0)      as gp_recent,
            count(*) filter (where pl.rn <= $5 and pl.coach_dnp)        as coach_recent,
            count(*) filter (where pl.rn <= $5 and pl.unavailable)      as out_recent,
            avg(pl.margin) filter (where pl.minutes > 0)                as margin_with
       from roster r
       join pl on pl.player_id = r.player_id and pl.team = r.team
      group by r.team, r.player_id
     )
     -- THE TEAM WITHOUT HIM: every team game since he joined that he did not play,
     -- listed or not, except coach's decisions. It used to need a DNP row, but an
     -- injured player is often not in the box at all: Banchero, Holmgren and Edwards
     -- all read as "0 games without" and Moody's "without" was only last season's
     -- 9 listed games, so his one game back read as a 14-point swing.
     select a.*, wo.margin_without, wo.n_without
       from agg a
       left join lateral (
         select avg(t.margin) as margin_without, count(*) as n_without
           from tgl t
          where t.team = a.team and t.rn <= a.first_rn
            and not exists (
              select 1 from pl x
               where x.player_id = a.player_id and x.team = a.team and x.rn = t.rn
                 and (x.minutes > 0 or x.coach_dnp))
       ) wo on true`,
    [teams, asOf, floor, lookback, recentN, m.injury.rotationGames]);

  // How much of each player is IN his team's rating: the share of its rating weight
  // from games he played. Weighted exactly as ratings.js weights them. Only models
  // pricing from team ratings use it.
  const rc = m.ratings;
  const ratingShare = {};
  if (rc && (m.weights.matchup ?? 0) > 0) {
    const { rows: shares } = await q(
      `with cur as (select max(season) as season from sports.nba_games
                     where home_score is not null and kickoff < $2),
       tg as (
         select t.team, g.game_id,
                case when g.season = cur.season
                     then power(0.5, extract(epoch from ($2::timestamptz - g.kickoff)) / 86400 / $4::float)
                     else $5::float end as w
           from unnest($1::text[]) t(team)
           join sports.nba_games g on t.team in (g.home_team, g.away_team)
           cross join cur
          where g.home_score is not null and g.kickoff < $2 and g.season >= $3
            and g.season_type in (2, 5)
       ), total as (select team, sum(w) as w from tg group by team)
       select tg.team, p.player_id, sum(tg.w) / max(total.w) as share
         from tg
         join total on total.team = tg.team
         join sports.nba_player_games p on p.game_id = tg.game_id and p.team = tg.team
        where p.minutes > 0
        group by tg.team, p.player_id`,
      [teams, asOf, floor, rc.halfLifeDays, rc.priorSeasonWeight]);
    for (const r of shares) ratingShare[`${r.team}|${r.player_id}`] = Number(r.share);
    for (const p of players) p.rating_share = ratingShare[`${p.team}|${p.player_id}`] ?? 0;
  }

  // ── Team style: possessions and 3-point attempt rate, recent games ─────────
  const styleGames = m.style.games;
  const { rows: style } = await q(
    `with tg as (
       select t.team, g.game_id,
              row_number() over (partition by t.team order by g.kickoff desc) as rn
         from unnest($1::text[]) t(team)
         join sports.nba_games g on t.team in (g.home_team, g.away_team)
        where g.home_score is not null and g.kickoff < $2 and g.season >= $3
     ), box as (
       select tg.team, p.game_id,
              sum(p.fga) fga, sum(p.fg3a) fg3a, sum(p.fta) fta, sum(p.oreb) oreb, sum(p.tov) tov
         from tg join sports.nba_player_games p on p.game_id = tg.game_id and p.team = tg.team
        where tg.rn <= $4
        group by tg.team, p.game_id
     )
     select team, count(*) as n,
            avg(fga + 0.44 * fta - oreb + tov) as poss,
            sum(fg3a)::numeric / nullif(sum(fga), 0) as par3
       from box group by team`,
    [teams, asOf, floor, styleGames]);

  // League baseline over the same recent span (one row).
  const { rows: lg } = await q(
    `with box as (
       select p.game_id, p.team,
              sum(p.fga) fga, sum(p.fg3a) fg3a, sum(p.fta) fta, sum(p.oreb) oreb, sum(p.tov) tov
         from sports.nba_player_games p
         join sports.nba_games g on g.game_id = p.game_id
        where g.kickoff < $1 and g.kickoff > $1::timestamptz - interval '45 days'
        group by p.game_id, p.team
     )
     select count(*) as n, avg(fga + 0.44 * fta - oreb + tov) as poss,
            sum(fg3a)::numeric / nullif(sum(fga), 0) as par3
       from box`,
    [asOf]);

  const { rows: injuries } = await q(
    `select player_id, team, player, status, status_changed_at, observed_at, cleared_at
       from sports.nba_injuries where team = any($1)`,
    [teams]);
  const { rows: feed } = await q(`select max(observed_at) as at from sports.nba_injuries where cleared_at is null`);

  const byTeam = (rows) => rows.reduce((acc, r) => {
    (acc[r.team] ||= []).push(r);
    return acc;
  }, {});
  return {
    asOf,
    games: byTeam(games),
    players: byTeam(players),
    style: Object.fromEntries(style.map((s) => [s.team, { n: Number(s.n), poss: Number(s.poss), par3: Number(s.par3) }])),
    league: { n: Number(lg[0]?.n || 0), poss: Number(lg[0]?.poss || 0), par3: Number(lg[0]?.par3 || 0) },
    injuries: byTeam(injuries),
    injuryFeedAt: feed[0]?.at ? new Date(feed[0].at) : null,
  };
}

module.exports = { loadNbaContext, seasonFloor };
