// trading-bot/src/ingest/polymarketPassing.js
//
// QB PASSING-YARDS prop discovery + the passing DEPTH RECORDER. This is the
// Cash-7 liquidity probe: before trusting a prop model, prove the central-line
// books actually get tradeable depth, because Polymarket's CLOB serves current
// state only and an unrecorded hour of depth can never be bought back.
//
// Cloned from polymarketTotals.js, with three deliberate differences that come
// straight from probing the live Gamma API (2026-10-13):
//
//   1. FILTER. sports_market_types=passing_yards returns the per-game O/U ladder
//      (slug nfl-<away>-<home>-<date>-pyd-<player>-<line>). The sportsMarketType
//      FIELD is empty on props, same as totals; the query PARAM still filters.
//
//   2. liquidityNum IS USELESS HERE -- it is 0 on every prop, even ones that have
//      traded. So lines cannot be ranked by it the way totals are. The central
//      line is chosen by the ladder centre ((min+max)/2, the market's implied
//      median), and the recorder captures the band around it so the real central
//      line can be read off the recorded book once depth arrives.
//
//   3. EMPTY BOOKS ARE PLACEHOLDERS. An unseeded prop quotes bid 0.01 / ask 0.99
//      (mid 0.50 -- a fake coin flip). normaliseBook still records it; the spread
//      column makes the placeholder obvious in analysis, and the whole point of
//      the probe is to see WHEN (if ever) that spread collapses to a real market.
//
// FEES: props carry feeType sports_fees_nfl_cfb_oct26 at rate 0.03 (3%) -- NOT
// the totals' zero_fees. Recorded per market, never assumed: a 3% round-trip
// headwind is central to the go/no-go and is Polymarket's to change.

const { cfg, ENV } = require("../config");
const { getJson } = require("../http");
const { q, bulkInsert } = require("../db");
const { writeBooks, dueMarkets } = require("./bookWriter");
const log = require("../log");
const { toNflverse, parsePmTime, parseJsonField, normaliseBook, fetchBook } = require("./polymarket");

// nfl-<away>-<home>-<yyyy>-<mm>-<dd>-pyd-<player-slug>-<line>, 324.5 -> 324pt5.
// Player slug may contain hyphens; the line is the trailing number, so a greedy
// player capture plus an anchored numeric tail splits them correctly.
const PASSING_SLUG =
  /^nfl-([a-z]{2,4})-([a-z]{2,4})-(\d{4})-(\d{2})-(\d{2})-pyd-(.+)-(\d+)(pt5)?$/;

/**
 * Every open NFL passing-yards prop. order=id for a UNIQUE sort key -- paging on
 * gameStartTime returns overlapping pages (a game's whole ladder shares one
 * kickoff), the same trap that hid moneyline and totals markets.
 */
async function fetchNflPassing({ maxPages = 25 } = {}) {
  const { pmNflTagId } = cfg().ingest;
  const seen = new Map();
  for (let page = 0; page < maxPages; page++) {
    const u =
      ENV.GAMMA_API_URL + "/markets?tag_id=" + pmNflTagId + "&closed=false" +
      "&sports_market_types=passing_yards&limit=100&offset=" + page * 100 +
      "&order=id&ascending=true";
    const batch = await getJson(u);
    if (!Array.isArray(batch) || !batch.length) break;
    for (const m of batch) if (m && m.conditionId) seen.set(m.conditionId, m);
    if (batch.length < 100) break;
  }
  return [...seen.values()];
}

/** Per-game passing props only -- drops season-long "most passing yards" futures. */
function selectGamePassing(markets) {
  return markets.filter((m) => m && PASSING_SLUG.test(m.slug || ""));
}

/**
 * The starters-and-skill roster for a game, as lower(name) -> gsis player_id.
 * Prefer the QB row and the most recent week when a name repeats. Lets a prop
 * ("Tyson Bagent: Passing Yards O/U 324.5") carry a real player_id, which is how
 * the model later joins the market to the QB's form and opponent pass-D.
 */
async function loadRoster(game_id, home, away) {
  const { rows } = await q(
    `select distinct on (lower(player_display_name))
            player_display_name, player_id
       from sports.nfl_player_week
      where team in ($1, $2)
        and player_display_name is not null
        and season = (select season from sports.nfl_games where game_id = $3)
      order by lower(player_display_name), (position = 'QB') desc, week desc`,
    [home, away, game_id],
  );
  const map = new Map();
  for (const r of rows) map.set(r.player_display_name.toLowerCase(), r.player_id);
  return map;
}

/**
 * Resolve passing props to nflverse game_ids and gsis player_ids and upsert them
 * into sports.pm_passing_markets. Game match is team codes + a kickoff window,
 * never the slug date (slug is UTC, nflverse gameday is US-Eastern).
 */
async function mapPassingToGames(markets) {
  const gameIdCache = new Map();
  const rosterCache = new Map();
  const rows = [];

  for (const m of markets) {
    const sm = PASSING_SLUG.exec(m.slug || "");
    if (!sm) continue;
    const away = toNflverse(sm[1]);
    const home = toNflverse(sm[2]);
    const kickoff = parsePmTime(m.gameStartTime);

    // The `line` FIELD is authoritative -- never re-derive it from the slug.
    const line = Number(m.line);
    if (!Number.isFinite(line)) { log.warn(`passing market ${m.slug} has no usable line`); continue; }

    // Outcomes are ["Over","Under"], same order as the tokens. Asserted, because
    // a silent inversion would price the opposite of what the model decided.
    const outcomes = parseJsonField(m.outcomes, []);
    if (outcomes.length === 2 && String(outcomes[0]).toLowerCase() !== "over") {
      log.warn(`passing market ${m.slug} outcome order is ${JSON.stringify(outcomes)} -- skipped`);
      continue;
    }

    let game_id = null;
    let confidence = "unmapped";
    const cacheKey = kickoff ? `${home}|${away}|${kickoff.toISOString()}` : null;
    if (cacheKey && gameIdCache.has(cacheKey)) {
      game_id = gameIdCache.get(cacheKey);
      confidence = game_id ? "exact" : "unmapped";
    } else if (kickoff) {
      const r = await q(
        `select game_id from sports.nfl_games
          where home_team = $1 and away_team = $2
            and kickoff between $3::timestamptz - interval '36 hours'
                            and $3::timestamptz + interval '36 hours'
          order by abs(extract(epoch from (kickoff - $3::timestamptz))) limit 1`,
        [home, away, kickoff.toISOString()],
      );
      game_id = r.rows[0] ? r.rows[0].game_id : null;
      confidence = game_id ? "exact" : "unmapped";
      gameIdCache.set(cacheKey, game_id);
    }

    const player_name = (m.groupItemTitle || m.question || "").split(":")[0].trim() || null;
    let player_id = null;
    if (game_id && player_name) {
      if (!rosterCache.has(game_id)) rosterCache.set(game_id, await loadRoster(game_id, home, away));
      player_id = rosterCache.get(game_id).get(player_name.toLowerCase()) || null;
    }

    const tokens = parseJsonField(m.clobTokenIds, []);
    const fee = m.feeSchedule || {};
    rows.push({
      condition_id: m.conditionId,
      slug: m.slug,
      question: m.question,
      game_id,
      player_id,
      player_name,
      line,
      over_token_id: tokens[0] || null,
      under_token_id: tokens[1] || null,
      liquidity_num: Number(m.liquidityNum) || 0,
      volume_num: Number(m.volumeNum) || 0,
      tick_size: Number(m.orderPriceMinTickSize) || null,
      min_order_usd: Number(m.orderMinSize) || null,
      fee_type: m.feeType || null,
      fee_rate: Number.isFinite(Number(fee.rate)) ? Number(fee.rate) : null,
      end_date: m.endDate || null,
      closed: !!m.closed,
      map_confidence: confidence,
    });
  }

  const unique = [...new Map(rows.map((r) => [r.condition_id, r])).values()];
  if (unique.length) {
    const cols = Object.keys(unique[0]);
    const updates = cols.filter((c) => c !== "condition_id")
      .map((c) => `${c} = excluded.${c}`).join(", ");
    await bulkInsert("sports.pm_passing_markets", cols, unique, {
      onConflict: `on conflict (condition_id) do update set ${updates}, last_seen = now()`,
    });
  }
  const mappedG = unique.filter((r) => r.game_id).length;
  const mappedP = unique.filter((r) => r.player_id).length;
  const players = new Set(unique.filter((r) => r.player_name).map((r) => r.player_name)).size;
  log(`pm passing: ${unique.length} markets, ${mappedG} game-mapped, ${mappedP} player-mapped, ${players} players`);
  return unique;
}

/**
 * The lines to record per player: the N nearest the ladder centre ((min+max)/2,
 * the market's implied median). liquidityNum is 0 on every prop, so it cannot
 * pick the line the way totals do; recording the central BAND instead means the
 * real central line can be identified from the recorded book once a two-sided
 * market appears, rather than being guessed from an empty placeholder now.
 */
async function passingLinesToRecord() {
  const c = cfg();
  const perPlayer = c.ingest.passingLinesRecordedPerPlayer || 3;
  const hours = c.ingest.passingRecordWindowHours || c.ingest.recordWindowHours || 30;

  const { rows } = await q(
    `with games as (
       select game_id, kickoff
         from sports.nfl_games
        where home_score is null
          and kickoff between now() - interval '6 hours'
                          and now() + ($2 || ' hours')::interval
     ), laddered as (
       select m.*, g.kickoff,
              (min(m.line) over w + max(m.line) over w) / 2.0 as ctr_line
         from sports.pm_passing_markets m
         join games g on g.game_id = m.game_id
        where m.closed = false and m.over_token_id is not null
       window w as (partition by m.game_id, coalesce(m.player_id, m.player_name))
     ), ranked as (
       select *, row_number() over (
                 partition by game_id, coalesce(player_id, player_name)
                 order by abs(line - ctr_line), line) as rnk
         from laddered
     )
     select * from ranked where rnk <= $1
      order by kickoff, player_name, line`,
    [perPlayer, String(hours)],
  );
  return rows;
}

/**
 * Record one snapshot of every tracked passing book. Window extends past kickoff
 * because the price at kickoff is the closing line a trade would be graded
 * against. Probe phase records only (no fills) -- the question is whether depth
 * ever shows up, not whether a resting order fills against it.
 */
async function recordPassingBooks() {
  const { bookLevelsStored } = cfg().ingest;
  const markets = await passingLinesToRecord();

  const { due, deferred } = dueMarkets(markets);
  if (deferred) log(`  ${deferred} market(s) not due for a poll yet`);

  const out = [];
  for (const m of due) {
    for (const side of ["over", "under"]) {
      const tokenId = side === "over" ? m.over_token_id : m.under_token_id;
      if (!tokenId) continue;
      try {
        const snap = normaliseBook(await fetchBook(tokenId), bookLevelsStored);
        if (snap.mid === null) continue;            // truly empty book, nothing to log
        out.push({ condition_id: m.condition_id, token_id: tokenId,
                   game_id: m.game_id, side, ...snap });
      } catch (e) {
        log.warn(`passing book fetch failed ${m.game_id} ${m.player_name}@${m.line}/${side}: ${e.message}`);
      }
    }
  }

  const players = new Set(markets.map((m) => m.player_name)).size;
  const res = await writeBooks(out, `passing books (${markets.length} lines / ${players} players)`);
  return res.written;
}

module.exports = {
  fetchNflPassing, selectGamePassing, mapPassingToGames, loadRoster,
  passingLinesToRecord, recordPassingBooks, PASSING_SLUG,
};
