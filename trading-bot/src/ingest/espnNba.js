// trading-bot/src/ingest/espnNba.js
//
// NBA DATA, from ESPN's public site API (no key). Axis-3's equivalent of
// ingest/nflverse.js -- the NBA has no open dataset like nflverse, so ESPN is
// the source for all three inputs:
//
//   scoreboard?dates=YYYYMMDD  -> sports.nba_games   (schedule, tip, finals, national TV)
//   summary?event=ID           -> sports.nba_player_games (box score: minutes, +/-, DNPs)
//   injuries                   -> sports.nba_injuries (the live report)
//
// Unofficial API: a shape change must fail LOUDLY, not ingest garbage. Every
// parser below throws on a missing core field rather than guessing, and the
// callers log and skip the one game rather than the whole run.
//
// Parsing is pure (parseScoreboard / parseBoxscore / parseInjuries) so it can be
// checked against live payloads without a database: run/ingest-nba.js --check.

const { getJson } = require("../http");
const { q, bulkInsert } = require("../db");
const log = require("../log");

const BASE = process.env.ESPN_NBA_URL || "https://site.api.espn.com/apis/site/v2/sports/basketball/nba";

// ESPN abbreviations -> Polymarket slug codes (upper-cased). Only the six that
// differ; the other 24 already agree. Polymarket's are canonical so a market
// maps to a game with no alias lookup on the hot path.
const ESPN_TO_PM = { GS: "GSW", NY: "NYK", WSH: "WAS", NO: "NOP", UTAH: "UTA", SA: "SAS" };
const toCode = (abbr) => {
  const a = String(abbr || "").toUpperCase();
  return ESPN_TO_PM[a] || a;
};

/** The 30 franchises, so a renamed/new abbreviation is caught, not ingested. */
const NBA_TEAMS = new Set([
  "ATL", "BOS", "BKN", "CHA", "CHI", "CLE", "DAL", "DEN", "DET", "GSW",
  "HOU", "IND", "LAC", "LAL", "MEM", "MIA", "MIL", "MIN", "NOP", "NYK",
  "OKC", "ORL", "PHI", "PHX", "POR", "SAC", "SAS", "TOR", "UTA", "WAS",
]);

/** National broadcasters. NBA TV is national but carries the low-profile games. */
const PRIME_NETWORKS = new Set(["ESPN", "ABC", "NBC", "PEACOCK", "PRIME VIDEO", "AMAZON PRIME VIDEO", "TNT", "TRUTV"]);

/** "2026-11-01" -> "20261101" */
const ymd = (d) => d.toISOString().slice(0, 10).replace(/-/g, "");

/** US-Eastern calendar date of an instant, as YYYY-MM-DD. */
function etDate(iso) {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "America/New_York", year: "numeric", month: "2-digit", day: "2-digit" })
    .format(new Date(iso));
}

// ── Parsers (pure) ──────────────────────────────────────────────────────────

/** One scoreboard payload -> nba_games rows. Preseason and All-Star are dropped. */
function parseScoreboard(json) {
  const out = [];
  for (const e of json?.events || []) {
    const c = e.competitions?.[0];
    if (!c) throw new Error(`event ${e.id}: no competition`);
    const seasonType = Number(e.season?.type ?? c.season?.type ?? NaN);
    const seasonYear = Number(e.season?.year ?? NaN);
    if (!Number.isFinite(seasonType) || !Number.isFinite(seasonYear)) {
      throw new Error(`event ${e.id}: no season on the payload`);
    }
    if (![2, 3, 5].includes(seasonType)) continue;  // regular, playoffs, play-in only

    const home = c.competitors?.find((t) => t.homeAway === "home");
    const away = c.competitors?.find((t) => t.homeAway === "away");
    if (!home || !away) throw new Error(`event ${e.id}: competitors missing`);
    const homeCode = toCode(home.team?.abbreviation);
    const awayCode = toCode(away.team?.abbreviation);
    // An exhibition vs an international club sneaks in as type 2 now and then.
    if (!NBA_TEAMS.has(homeCode) || !NBA_TEAMS.has(awayCode)) continue;

    const status = c.status?.type?.name || null;
    const final = c.status?.type?.completed === true && status === "STATUS_FINAL";
    const national = (c.geoBroadcasts || [])
      .filter((g) => g?.market?.type === "National")
      .map((g) => String(g?.media?.shortName || "").trim())
      .filter(Boolean);

    out.push({
      game_id: `NBA_${e.id}`,
      espn_id: String(e.id),
      // ESPN labels 2026-27 as 2027; we key seasons by their start year.
      season: seasonYear - 1,
      season_type: seasonType,
      game_date: etDate(e.date),
      kickoff: new Date(e.date).toISOString(),
      away_team: awayCode,
      home_team: homeCode,
      away_score: final ? Number(away.score) : null,
      home_score: final ? Number(home.score) : null,
      status,
      national_tv: national.length ? [...new Set(national)].join(",") : null,
      neutral_site: !!c.neutralSite,
    });
  }
  return out;
}

/** True when the broadcast list includes a primetime network (not NBA TV). */
function isPrimetime(nationalTv) {
  return String(nationalTv || "").split(",")
    .some((n) => PRIME_NETWORKS.has(n.trim().toUpperCase()));
}

const statNum = (v) => {
  const n = Number(String(v ?? "").replace(/^\+/, ""));
  return Number.isFinite(n) ? n : null;
};
const madeAtt = (v) => {
  const m = String(v ?? "").match(/^(\d+)-(\d+)$/);
  return m ? Number(m[2]) : null;           // attempts
};

/** One summary payload -> nba_player_games rows. */
function parseBoxscore(gameId, json) {
  const teams = json?.boxscore?.players;
  if (!Array.isArray(teams) || teams.length !== 2) throw new Error(`${gameId}: no box score`);
  const rows = [];
  for (const t of teams) {
    const team = toCode(t.team?.abbreviation);
    const block = t.statistics?.[0];
    const keys = block?.keys || block?.names;
    if (!Array.isArray(keys)) throw new Error(`${gameId}: box score has no stat keys`);
    const ix = (k) => keys.indexOf(k);
    const iMin = ix("minutes"), iPts = ix("points"), iPm = ix("plusMinus");
    const iFg = ix("fieldGoalsMade-fieldGoalsAttempted");
    const i3 = ix("threePointFieldGoalsMade-threePointFieldGoalsAttempted");
    const iFt = ix("freeThrowsMade-freeThrowsAttempted");
    const iOr = ix("offensiveRebounds"), iTo = ix("turnovers");
    if (iMin < 0 || iPm < 0) throw new Error(`${gameId}: minutes/plusMinus missing from box score`);

    for (const a of block.athletes || []) {
      const id = a?.athlete?.id;
      if (!id) continue;
      const s = Array.isArray(a.stats) ? a.stats : [];
      const dnp = !!a.didNotPlay || s.length === 0;
      rows.push({
        game_id: gameId,
        player_id: String(id),
        team,
        player: a.athlete.displayName || null,
        position: a.athlete.position?.abbreviation || null,
        starter: !!a.starter,
        dnp,
        dnp_reason: dnp ? (a.reason || null) : null,
        minutes: dnp ? 0 : statNum(s[iMin]),
        points: dnp ? 0 : statNum(s[iPts]),
        plus_minus: dnp ? null : statNum(s[iPm]),
        fga: dnp ? 0 : madeAtt(s[iFg]),
        fg3a: dnp ? 0 : madeAtt(s[i3]),
        fta: dnp ? 0 : madeAtt(s[iFt]),
        oreb: dnp ? 0 : statNum(s[iOr]),
        tov: dnp ? 0 : statNum(s[iTo]),
      });
    }
  }
  return rows;
}

/** League injury report -> nba_injuries rows (current state). */
function parseInjuries(json) {
  const out = [];
  const teamCodeByName = new Map();
  for (const t of json?.injuries || []) {
    for (const i of t.injuries || []) {
      // The athlete object carries no id field; the player-card link does.
      const href = (i.athlete?.links || []).map((l) => l.href || "").find((h) => /\/id\/\d+/.test(h)) || "";
      const pid = (href.match(/\/id\/(\d+)/) || [])[1];
      const abbr = toCode(i.athlete?.team?.abbreviation || t.abbreviation || teamCodeByName.get(t.displayName));
      if (!pid || !i.status) continue;
      // The headline status is two-valued ("Out" / "Day-To-Day") and loose: 11 of
      // 89 "Out" rows on 2026-11-01 were really game-time decisions. The fantasy
      // status underneath is the precise one -- OUT, OFS (out for season), GTD.
      const fine = String(i.details?.fantasyStatus?.abbreviation || "").toUpperCase();
      out.push({
        player_id: pid,
        team: abbr || null,
        teamName: t.displayName || null,
        player: i.athlete?.displayName || null,
        status: fine || String(i.status),
        short_comment: i.shortComment || null,
        espn_updated_at: i.date ? new Date(i.date).toISOString() : null,
      });
    }
  }
  return out;
}

// ── Fetch + store ───────────────────────────────────────────────────────────

const fetchScoreboard = (date) => getJson(`${BASE}/scoreboard?dates=${ymd(date)}&limit=50`, { timeoutMs: 20_000 });
const fetchSummary = (espnId) => getJson(`${BASE}/summary?event=${espnId}`, { timeoutMs: 20_000 });
const fetchInjuries = () => getJson(`${BASE}/injuries`, { timeoutMs: 20_000 });

/** Upsert the schedule for every date in [from, to]. Scores only land when FINAL. */
async function syncSchedule(from, to) {
  let n = 0;
  for (let d = new Date(from); d <= to; d = new Date(d.getTime() + 86_400_000)) {
    let rows;
    try { rows = parseScoreboard(await fetchScoreboard(d)); }
    catch (e) { log.warn(`nba scoreboard ${ymd(d)}: ${e.message}`); continue; }
    if (!rows.length) continue;
    const cols = Object.keys(rows[0]);
    const updates = cols.filter((c) => c !== "game_id").map((c) => `${c} = excluded.${c}`).join(", ");
    await bulkInsert("sports.nba_games", cols, rows, {
      onConflict: `on conflict (game_id) do update set ${updates}, updated_at = now()`,
    });
    n += rows.length;
  }
  return n;
}

/** Box scores for FINAL games that do not have one yet (oldest first, capped). */
async function syncBoxscores(limit = 40) {
  const { rows: games } = await q(
    `select g.game_id, g.espn_id from sports.nba_games g
      where g.home_score is not null
        and not exists (select 1 from sports.nba_player_games p where p.game_id = g.game_id)
      order by g.kickoff limit $1`, [limit]);
  let n = 0;
  for (const g of games) {
    try {
      const rows = parseBoxscore(g.game_id, await fetchSummary(g.espn_id));
      if (!rows.length) continue;
      await bulkInsert("sports.nba_player_games", Object.keys(rows[0]), rows, {
        onConflict: "on conflict (game_id, player_id) do nothing",
      });
      n++;
    } catch (e) { log.warn(`nba box score ${g.game_id}: ${e.message}`); }
  }
  if (n) log(`nba: ${n} box score(s) stored`);
  return n;
}

/**
 * Replace the injury report. status_changed_at only moves when the STATUS does --
 * a rewritten comment ("still out, re-evaluated Friday") is not news.
 */
async function syncInjuries() {
  const rows = parseInjuries(await fetchInjuries()).filter((r) => r.team && NBA_TEAMS.has(r.team));
  // Sanity: an empty or tiny report mid-season is a feed failure, not a healthy
  // league. Keep yesterday's rather than clearing everyone to play.
  if (rows.length < 5) throw new Error(`injury report has only ${rows.length} rows -- refusing to replace`);
  const unique = [...new Map(rows.map((r) => [r.player_id, r])).values()];
  // When did a status we have never seen change? In steady state a new row IS
  // new news, and ESPN's stamp is a fair time for it. On the FIRST load it is
  // not: ESPN's stamp is when the COMMENT was last edited (a month-long back
  // injury re-dated this morning), so the whole league would read as breaking.
  // Backdate the first load a day -- everything on it is priced already.
  const { rows: seeded } = await q(`select exists (select 1 from sports.nba_injuries) as any`);
  const firstLoad = !seeded[0].any;
  const dayAgo = new Date(Date.now() - 86_400_000).toISOString();
  await bulkInsert("sports.nba_injuries",
    ["player_id", "team", "player", "status", "short_comment", "espn_updated_at", "status_changed_at"],
    unique.map(({ teamName, ...r }) => ({
      ...r, status_changed_at: firstLoad ? dayAgo : (r.espn_updated_at || new Date().toISOString()),
    })), {
      onConflict: `on conflict (player_id) do update set
        team = excluded.team, player = excluded.player, short_comment = excluded.short_comment,
        espn_updated_at = excluded.espn_updated_at, observed_at = now(),
        -- Back on the report after being cleared is news too, even at the same status.
        status_changed_at = case when sports.nba_injuries.status is distinct from excluded.status
                                   or sports.nba_injuries.cleared_at is not null
                                 then now() else sports.nba_injuries.status_changed_at end,
        status = excluded.status, cleared_at = null`,
    });
  // Off the report = cleared. The row STAYS, stamped: it is the only evidence that
  // a player is back, and the stamp is when the market could first know. An old
  // tombstone is no longer news, so those are purged.
  const ids = unique.map((r) => r.player_id);
  const { rowCount } = await q(
    `update sports.nba_injuries set cleared_at = now()
      where cleared_at is null and not (player_id = any($1))`, [ids]);
  await q(`delete from sports.nba_injuries where cleared_at < now() - interval '30 days'`);
  return { listed: unique.length, cleared: rowCount };
}

module.exports = {
  parseScoreboard, parseBoxscore, parseInjuries, isPrimetime, toCode, etDate,
  fetchScoreboard, fetchSummary, fetchInjuries,
  syncSchedule, syncBoxscores, syncInjuries, NBA_TEAMS,
};
