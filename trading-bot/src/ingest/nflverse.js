// trading-bot/src/ingest/nflverse.js
//
// Pulls the four nflverse datasets the model needs and lands them in `sports`.
// All published as CSV, so there is no R dependency and no API key:
//
//   schedules/games.csv          -> sports.nfl_games
//     Also carries CLOSING moneyline/spread/total back to 2006, which is our
//     CLV ground truth. That was the one input I expected to cost money.
//   pbp/play_by_play_<season>    -> sports.nfl_team_game_stats  (EPA, pace)
//   snap_counts/<season>         -> sports.nfl_snap_counts      (injury baseline)
//   injuries/<season>            -> sports.nfl_injuries         (append-only)
//
// Everything is gated on config.data.startSeason.

const { cfg, ENV } = require("../config");
const { getText, parseCsv, streamCsv, num, str, bool } = require("../http");
const { bulkInsert, q } = require("../db");
const log = require("../log");

const url = (tag, file) => `${ENV.NFLVERSE_BASE}/${tag}/${file}`;

// ── Kickoff timestamps ──────────────────────────────────────────────────────
// nflverse gametime is US Eastern wall-clock. The season straddles the DST
// change in early November, so a fixed -4 or -5 offset is wrong for part of
// every season. Resolve the real offset per date via Intl instead.
function etOffsetHours(y, m, d) {
  const probe = new Date(Date.UTC(y, m - 1, d, 17, 0, 0));
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    timeZoneName: "shortOffset",
  });
  const part = fmt.formatToParts(probe).find((p) => p.type === "timeZoneName");
  const match = /GMT([+-]\d{1,2})/.exec(part ? part.value : "");
  return match ? Number(match[1]) : -5;
}

function kickoffUtc(gameday, gametime) {
  if (!gameday || !gametime) return null;
  const [y, m, d] = gameday.split("-").map(Number);
  const [hh, mm] = gametime.split(":").map(Number);
  if (![y, m, d, hh].every(Number.isFinite)) return null;
  const off = etOffsetHours(y, m, d);
  return new Date(Date.UTC(y, m - 1, d, hh - off, mm || 0, 0)).toISOString();
}

// ── schedules -> sports.nfl_games ───────────────────────────────────────────
async function ingestGames() {
  const { startSeason } = cfg().data;
  const rows = parseCsv(await getText(url("schedules", "games.csv")))
    .filter((r) => num(r.season) !== null && num(r.season) >= startSeason);

  const mapped = rows.map((r) => ({
    game_id: r.game_id,
    season: num(r.season),
    week: num(r.week),
    game_type: str(r.game_type),
    gameday: str(r.gameday),
    weekday: str(r.weekday),
    gametime: str(r.gametime),
    kickoff: kickoffUtc(str(r.gameday), str(r.gametime)),
    away_team: str(r.away_team),
    home_team: str(r.home_team),
    away_score: num(r.away_score),
    home_score: num(r.home_score),
    result: num(r.result),
    away_rest: num(r.away_rest),
    home_rest: num(r.home_rest),
    div_game: bool(r.div_game),
    roof: str(r.roof),
    surface: str(r.surface),
    temp: num(r.temp),
    wind: num(r.wind),
    away_moneyline: num(r.away_moneyline),
    home_moneyline: num(r.home_moneyline),
    spread_line: num(r.spread_line),
    total_line: num(r.total_line),
    away_qb_id: str(r.away_qb_id),
    home_qb_id: str(r.home_qb_id),
    stadium_id: str(r.stadium_id),
    stadium: str(r.stadium),
  })).filter((r) => r.game_id && r.home_team && r.away_team);

  const cols = Object.keys(mapped[0] || {});
  const updates = cols.filter((c) => c !== "game_id")
    .map((c) => `${c} = excluded.${c}`).join(", ");
  await bulkInsert("sports.nfl_games", cols, mapped, {
    onConflict: `on conflict (game_id) do update set ${updates}, updated_at = now()`,
  });

  // Freeze the closing line the first time a finished game shows odds. Once
  // stamped it is never rewritten -- that column is the CLV denominator and a
  // later nflverse correction must not silently move the goalposts.
  await q(`
    update sports.nfl_games
       set closing_captured_at = now()
     where closing_captured_at is null
       and home_moneyline is not null
       and result is not null
  `);

  log(`nflverse games: ${mapped.length} rows (season >= ${startSeason})`);
  return mapped.length;
}

module.exports = { ingestGames, kickoffUtc, etOffsetHours, url };

// ── play-by-play -> sports.nfl_team_game_stats ──────────────────────────────
// Aggregates each team's offensive EPA/play, success rate, style and PACE for
// every finished game. A team's DEFENSIVE numbers are simply its opponent's
// offensive numbers in the same game, so defence is derived, never parsed twice.
// Only these columns are read out of the ~400 the file carries. Projecting at
// parse time is what keeps a full season inside the default heap.
const PBP_COLS = [
  "game_id", "season", "week", "home_team", "away_team", "posteam", "defteam",
  "epa", "success", "pass", "rush", "aborted_play", "wp", "qtr", "down",
  "game_seconds_remaining", "fixed_drive", "home_score", "away_score",
];

/** Array form, kept for tests and small files. */
function aggregatePbp(plays) {
  const a = createPbpAccumulator();
  for (const p of plays) a.push(p);
  return a.finish();
}

function createPbpAccumulator() {
  const games = new Map(); // game_id -> { meta, teams: Map<team, acc> }

  const acc = () => ({
    epa: [], success: [], passEpa: [], rushEpa: [],
    neutralPass: [], plays: 0, paceDeltas: [],
    lastSecs: null, lastDrive: null,
  });

  const push = (p) => {
    const isPass = num(p.pass) === 1;
    const isRush = num(p.rush) === 1;
    if (!isPass && !isRush) return;
    if (num(p.aborted_play) === 1) return;
    const epa = num(p.epa);
    if (epa === null) return;
    const off = str(p.posteam);
    const def = str(p.defteam);
    const gid = str(p.game_id);
    if (!off || !def || !gid) return;

    if (!games.has(gid)) {
      games.set(gid, {
        meta: {
          season: num(p.season), week: num(p.week),
          home: str(p.home_team), away: str(p.away_team),
          home_score: num(p.home_score), away_score: num(p.away_score),
        },
        teams: new Map(),
      });
    }
    const g = games.get(gid);
    if (!g.teams.has(off)) g.teams.set(off, acc());
    const a = g.teams.get(off);

    a.plays++;
    a.epa.push(epa);
    const s = num(p.success);
    if (s !== null) a.success.push(s);
    if (isPass) a.passEpa.push(epa);
    if (isRush) a.rushEpa.push(epa);

    // Neutral-situation pass rate = style, stripped of score-chasing.
    const wp = num(p.wp), qtr = num(p.qtr), down = num(p.down);
    if (wp !== null && wp > 0.2 && wp < 0.8 && qtr !== null && qtr <= 3 &&
        (down === 1 || down === 2)) {
      a.neutralPass.push(isPass ? 1 : 0);
    }

    // PACE: seconds burned between consecutive snaps of the same drive. Gaps
    // outside 0-60s are clock stoppages (timeouts, quarter breaks, reviews) and
    // would otherwise swamp the average.
    const secs = num(p.game_seconds_remaining);
    const drive = num(p.fixed_drive);
    if (secs !== null && a.lastSecs !== null && drive === a.lastDrive) {
      const d = a.lastSecs - secs;
      if (d > 0 && d < 60) a.paceDeltas.push(d);
    }
    a.lastSecs = secs;
    a.lastDrive = drive;
  };

  const finish = () => {
  const mean = (xs) => (xs.length ? xs.reduce((s, x) => s + x, 0) / xs.length : null);
  const out = [];

  for (const [gid, g] of games) {
    const teams = [...g.teams.keys()];
    if (teams.length !== 2) continue;                // incomplete/corrupt game
    const totalPlays = teams.reduce((s, t) => s + g.teams.get(t).plays, 0);

    for (const team of teams) {
      const a = g.teams.get(team);
      const opponent = teams.find((t) => t !== team);
      const o = g.teams.get(opponent);
      const isHome = team === g.meta.home;
      out.push({
        game_id: gid,
        team,
        opponent,
        season: g.meta.season,
        week: g.meta.week,
        is_home: isHome,
        off_plays: a.plays,
        off_epa_per_play: mean(a.epa),
        off_success_rate: mean(a.success),
        off_pass_epa: mean(a.passEpa),
        off_rush_epa: mean(a.rushEpa),
        off_pass_rate: mean(a.neutralPass),
        // Defence is the opponent's offence, by definition.
        def_plays: o.plays,
        def_epa_per_play: mean(o.epa),
        def_success_rate: mean(o.success),
        // Pass/rush defensive splits are the opponent OFFENCE splits -- no
        // second parse needed, and they are what the style-mismatch term in
        // features/matchup.js needs to be meaningful.
        def_pass_epa: mean(o.passEpa),
        def_rush_epa: mean(o.rushEpa),
        def_pass_rate: mean(o.neutralPass),
        sec_per_play: mean(a.paceDeltas),
        plays_total: totalPlays,
        points_for: isHome ? g.meta.home_score : g.meta.away_score,
        points_against: isHome ? g.meta.away_score : g.meta.home_score,
      });
    }
  }
  return out;
  };

  return { push, finish };
}

async function ingestPbpSeason(season) {
  const text = await getText(url("pbp", `play_by_play_${season}.csv`));
  const agg = createPbpAccumulator();
  const playCount = streamCsv(text, PBP_COLS, (p) => agg.push(p));
  const rows = agg.finish();
  if (!rows.length) { log.warn(`pbp ${season}: no aggregable plays`); return 0; }

  const cols = Object.keys(rows[0]);
  const updates = cols.filter((c) => !["game_id", "team"].includes(c))
    .map((c) => `${c} = excluded.${c}`).join(", ");
  await bulkInsert("sports.nfl_team_game_stats", cols, rows, {
    onConflict: `on conflict (game_id, team) do update set ${updates}, computed_at = now()`,
  });
  log(`pbp ${season}: ${playCount} plays -> ${rows.length} team-game rows`);
  return rows.length;
}

// ── snap counts -> sports.nfl_snap_counts ───────────────────────────────────
// The injury baseline. Definitive on who took the field AND at what share --
// the injury report only says Active, which hides a 38%-of-snaps return.
async function ingestSnapCounts(season) {
  const rows = parseCsv(await getText(url("snap_counts", `snap_counts_${season}.csv`)))
    .map((r) => ({
      game_id: str(r.game_id),
      pfr_player_id: str(r.pfr_player_id),
      player: str(r.player),
      position: str(r.position),
      team: str(r.team),
      opponent: str(r.opponent),
      season: num(r.season),
      week: num(r.week),
      offense_snaps: num(r.offense_snaps),
      offense_pct: num(r.offense_pct),
      defense_snaps: num(r.defense_snaps),
      defense_pct: num(r.defense_pct),
      st_pct: num(r.st_pct),
    }))
    .filter((r) => r.game_id && r.pfr_player_id && r.team);
  if (!rows.length) return 0;

  const cols = Object.keys(rows[0]);
  const updates = cols.filter((c) => !["game_id", "pfr_player_id"].includes(c))
    .map((c) => `${c} = excluded.${c}`).join(", ");
  await bulkInsert("sports.nfl_snap_counts", cols, rows, {
    onConflict: `on conflict (game_id, pfr_player_id) do update set ${updates}`,
  });
  log(`snap_counts ${season}: ${rows.length} rows`);
  return rows.length;
}

// ── injuries -> sports.nfl_injuries (APPEND-ONLY) ──────────────────────────
// Insert-only with observed_at. The upstream CSV is rewritten in place and has
// no report date, so historically we only ever see its final state; by stamping
// each pull we rebuild the Wed -> Fri progression from today forward.
// Dedupe: skip inserting when the latest stored row for a player is unchanged,
// so a 6-hourly cron does not write four identical rows a day.
async function ingestInjuries(season) {
  const rows = parseCsv(await getText(url("injuries", `injuries_${season}.csv`)))
    .map((r) => ({
      season: num(r.season),
      week: num(r.week),
      team: str(r.team),
      gsis_id: str(r.gsis_id),
      full_name: str(r.full_name),
      position: str(r.position),
      report_status: str(r.report_status),
      report_primary_injury: str(r.report_primary_injury),
      practice_status: str(r.practice_status),
    }))
    .filter((r) => r.season && r.week && r.team);
  if (!rows.length) return 0;

  const prev = await q(
    `select distinct on (season, week, team, gsis_id)
            season, week, team, gsis_id, report_status, practice_status
       from sports.nfl_injuries
      where season = $1
      order by season, week, team, gsis_id, observed_at desc`,
    [season],
  );
  const seen = new Map();
  for (const r of prev.rows) {
    seen.set(`${r.season}|${r.week}|${r.team}|${r.gsis_id}`,
             `${r.report_status || ""}|${r.practice_status || ""}`);
  }

  const changed = rows.filter((r) => {
    const k = `${r.season}|${r.week}|${r.team}|${r.gsis_id}`;
    const v = `${r.report_status || ""}|${r.practice_status || ""}`;
    return seen.get(k) !== v;
  });
  if (!changed.length) { log(`injuries ${season}: no changes`); return 0; }

  await bulkInsert("sports.nfl_injuries", Object.keys(changed[0]), changed);
  log(`injuries ${season}: +${changed.length} changed rows (of ${rows.length})`);
  return changed.length;
}

// ── Orchestrator ────────────────────────────────────────────────────────────
async function ingestAll() {
  const { startSeason } = cfg().data;
  const thisSeason = currentSeason();
  await ingestGames();
  for (let s = startSeason; s <= thisSeason; s++) {
    await ingestPbpSeason(s);
    await ingestSnapCounts(s);
    await ingestInjuries(s);
  }
}

/** NFL season year: a January playoff game belongs to the prior season. */
function currentSeason(now = new Date()) {
  const y = now.getUTCFullYear();
  return now.getUTCMonth() < 2 ? y - 1 : y;  // Jan/Feb -> previous season
}

module.exports = {
  ingestGames, ingestPbpSeason, ingestSnapCounts, ingestInjuries, ingestAll,
  aggregatePbp, createPbpAccumulator, PBP_COLS, kickoffUtc, etOffsetHours, currentSeason, url,
};
