// trading-bot/src/ingest/nflversePlayers.js
//
// Weekly player box scores from nflverse -> sports.nfl_player_week. Powers
// Cash-7 (NFL QB passing yards): the QB's own recent form, the opponent's
// pass-defense ALLOWED (pass-only, derived from the opposing QBs' rows, so a
// bad run defense never leaks into a passing matchup), and each receiver's
// target share for the WR-injury factor. One row per player per finished game;
// upserts, so a re-run is safe and a finalised week overwrites its in-progress
// numbers.
//
// Source: <NFLVERSE_BASE>/stats_player/stats_player_week_<season>.csv -- the
// current nflverse path. The old player_stats_<season>.csv was retired upstream
// and now 404s, which is the whole reason this is its own module and not a tag
// swap inside nflverse.js.

const { cfg, ENV } = require("../config");
const { getText, parseCsv, num, str } = require("../http");
const { bulkInsert } = require("../db");
const log = require("../log");
const { currentSeason } = require("./nflverse");

const url = (tag, file) => `${ENV.NFLVERSE_BASE}/${tag}/${file}`;

// Only skill positions carry a passing/receiving line worth projecting; the
// file also ships ~80 defensive/ST columns and rows we never price, so they are
// dropped at parse time to keep the table the players a prop model looks at.
const KEEP_POS = new Set(["QB", "RB", "WR", "TE", "FB"]);

async function ingestPlayerWeekSeason(season) {
  let text;
  try {
    text = await getText(url("stats_player", `stats_player_week_${season}.csv`));
  } catch (e) {
    // A season with no file yet (future) or a transient upstream miss must not
    // abort the whole backfill loop.
    log.warn(`player_week ${season}: fetch failed (${e.message}) -- skipping`);
    return 0;
  }

  const rows = parseCsv(text)
    .filter((r) => KEEP_POS.has(str(r.position) || ""))
    .map((r) => ({
      game_id: str(r.game_id),
      player_id: str(r.player_id),               // gsis id, joins nfl_games qb ids
      player_display_name: str(r.player_display_name),
      position: str(r.position),
      team: str(r.team),
      opponent_team: str(r.opponent_team),
      season: num(r.season),
      week: num(r.week),
      season_type: str(r.season_type),
      // passing -- the QB form signal and the opponent pass-D-allowed signal
      completions: num(r.completions),
      attempts: num(r.attempts),
      passing_yards: num(r.passing_yards),
      passing_tds: num(r.passing_tds),
      passing_interceptions: num(r.passing_interceptions),
      sacks_suffered: num(r.sacks_suffered),
      passing_air_yards: num(r.passing_air_yards),
      passing_yards_after_catch: num(r.passing_yards_after_catch),
      passing_epa: num(r.passing_epa),
      passing_cpoe: num(r.passing_cpoe),
      // rushing -- QB scrambles trim pass volume; seed for the future RB bot
      carries: num(r.carries),
      rushing_yards: num(r.rushing_yards),
      rushing_tds: num(r.rushing_tds),
      // receiving + share -- the WR-injury weighting; seed for the WR/TE bot
      receptions: num(r.receptions),
      targets: num(r.targets),
      receiving_yards: num(r.receiving_yards),
      receiving_tds: num(r.receiving_tds),
      target_share: num(r.target_share),
      air_yards_share: num(r.air_yards_share),
      wopr: num(r.wopr),
    }))
    .filter((r) => r.game_id && r.player_id && r.season && r.week);

  if (!rows.length) { log.warn(`player_week ${season}: no rows`); return 0; }

  const cols = Object.keys(rows[0]);
  const updates = cols.filter((c) => !["game_id", "player_id"].includes(c))
    .map((c) => `${c} = excluded.${c}`).join(", ");
  await bulkInsert("sports.nfl_player_week", cols, rows, {
    onConflict: `on conflict (game_id, player_id) do update set ${updates}, updated_at = now()`,
  });
  log(`player_week ${season}: ${rows.length} rows`);
  return rows.length;
}

/** Backfill every season from config.data.startSeason to the current one. */
async function ingestPlayerWeekAll() {
  const { startSeason } = cfg().data;
  const thisSeason = currentSeason();
  let total = 0;
  for (let s = startSeason; s <= thisSeason; s++) {
    total += await ingestPlayerWeekSeason(s);
  }
  return total;
}

module.exports = { ingestPlayerWeekSeason, ingestPlayerWeekAll, url };
