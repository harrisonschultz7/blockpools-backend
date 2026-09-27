// trading-bot/src/ingest/polymarketTotals.js
//
// Totals market discovery + the totals DEPTH RECORDER.
//
// Same priority argument as the moneyline recorder: Polymarket's CLOB serves
// current book state only, historical depth cannot be bought or reconstructed
// from anywhere, and every hour this is not running is an hour of backtest
// fidelity permanently gone.
//
// THE SCALE PROBLEM, which is what makes this file different. There are ~2028
// open NFL totals markets -- 25 to 42 separate binaries per game, one per line.
// Recording all of them would be 4056 book calls per poll, every minute. So only
// the top few lines per game by liquidity are recorded. The bot trades the
// deepest; the neighbours are kept so a later version can compare lines without
// having lost the history it would need.
//
// VERIFIED LIVE (2026-09-26): the deepest line on a game is always the one priced
// nearest 50/50 -- bid/ask between 0.47 and 0.53 on every game checked. So
// "deepest book" and "closest to a coin flip" are the same market in practice,
// and the two selection rules do not have to be reconciled.
//
// FEES ARE READ, NOT ASSUMED. Also verified live: NFL totals carry feeType
// 'zero_fees' (rate 0) while game moneylines carry 'sports_fees_v3' (rate 0.05,
// taker-only). Zero taker fee is a real structural advantage for a strategy whose
// whole thesis is round-tripping out of a position before settlement -- and it is
// Polymarket's to withdraw whenever they like. Storing it per market means the day
// that changes, the paper ledger notices instead of quietly overstating returns.

const { cfg, ENV } = require("../config");
const { getJson } = require("../http");
const { q, bulkInsert } = require("../db");
const { writeBooks, dueMarkets } = require("./bookWriter");
const log = require("../log");
const { toNflverse, parsePmTime, parseJsonField, normaliseBook, fetchBook } = require("./polymarket");

// nfl-<away>-<home>-<yyyy>-<mm>-<dd>-total-<line>, where 44.5 is written 44pt5.
const TOTALS_SLUG =
  /^nfl-([a-z]{2,4})-([a-z]{2,4})-(\d{4})-(\d{2})-(\d{2})-total-(\d+)(pt5)?$/;

/**
 * Every open NFL totals market.
 *
 * order=id because it is a UNIQUE sort key. Paging on gameStartTime silently
 * returns overlapping pages -- rows share a start time -- which on the moneyline
 * side produced 295 duplicate conditionIds and hid all but one of 75 real markets.
 * The same trap applies here and harder, since dozens of lines on one game share
 * a kickoff exactly.
 */
async function fetchNflTotals({ maxPages = 25 } = {}) {
  const { pmNflTagId } = cfg().ingest;
  const seen = new Map();
  for (let page = 0; page < maxPages; page++) {
    const u =
      ENV.GAMMA_API_URL + "/markets?tag_id=" + pmNflTagId + "&closed=false" +
      "&sports_market_types=totals&limit=100&offset=" + page * 100 +
      "&order=id&ascending=true";
    const batch = await getJson(u);
    if (!Array.isArray(batch) || !batch.length) break;
    for (const m of batch) if (m && m.conditionId) seen.set(m.conditionId, m);
    if (batch.length < 100) break;
  }
  return [...seen.values()];
}

/** Single-game totals only -- drops season-long and other non-game questions. */
function selectGameTotals(markets) {
  return markets.filter((m) => m && TOTALS_SLUG.test(m.slug || ""));
}

/**
 * Resolve totals markets to nflverse game_ids.
 *
 * Team codes plus a kickoff window, never the slug date: the slug carries the UTC
 * date while nflverse gameday is US-Eastern, so every Sunday-night and Monday game
 * would be off by one.
 */
async function mapTotalsToGames(markets) {
  const gameIdCache = new Map();
  const rows = [];

  for (const m of markets) {
    const sm = TOTALS_SLUG.exec(m.slug);
    if (!sm) continue;
    const away = toNflverse(sm[1]);
    const home = toNflverse(sm[2]);
    const kickoff = parsePmTime(m.gameStartTime);

    // The `line` FIELD is authoritative -- do not re-derive it from the slug.
    // 44pt5 parses fine, but a future 3-decimal or negative form would not, and
    // a silently mis-parsed line is a bet on the wrong question.
    const line = Number(m.line);
    if (!Number.isFinite(line)) { log.warn(`totals market ${m.slug} has no usable line`); continue; }

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

    const tokens = parseJsonField(m.clobTokenIds, []);
    const outcomes = parseJsonField(m.outcomes, []);
    // Verified live: outcomes are ["Over","Under"] in that order, matching the
    // token order. Asserted rather than assumed, because silently inverting these
    // would make the bot buy the exact opposite of what it decided.
    if (outcomes.length === 2 && String(outcomes[0]).toLowerCase() !== "over") {
      log.warn(`totals market ${m.slug} outcome order is ${JSON.stringify(outcomes)} -- skipped`);
      continue;
    }

    const fee = m.feeSchedule || {};
    rows.push({
      condition_id: m.conditionId,
      slug: m.slug,
      question: m.question,
      game_id,
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

  // Postgres refuses an ON CONFLICT DO UPDATE that touches the same row twice.
  const unique = [...new Map(rows.map((r) => [r.condition_id, r])).values()];
  if (unique.length) {
    const cols = Object.keys(unique[0]);
    const updates = cols.filter((c) => c !== "condition_id")
      .map((c) => `${c} = excluded.${c}`).join(", ");
    await bulkInsert("sports.pm_totals_markets", cols, unique, {
      onConflict: `on conflict (condition_id) do update set ${updates}, last_seen = now()`,
    });
  }
  const mapped = unique.filter((r) => r.game_id).length;
  const games = new Set(unique.filter((r) => r.game_id).map((r) => r.game_id)).size;
  log(`pm totals: ${unique.length} markets, ${mapped} mapped across ${games} games`);
  return unique;
}

/**
 * The lines to record for each game: the top N by liquidity, PLUS every line a
 * bot actually holds.
 *
 * The held-line union is not a nicety. Recording only the deepest line is fine
 * for forming a view -- that is the line the policy trades -- but depth MOVES,
 * and it moves most violently once a game is in play. NE @ JAX was opened on the
 * 46.5 because 46.5 was the deepest book; ninety minutes into the game the flow
 * had rotated to the 44.5 and our line was fourth by liquidity, so the top-1
 * lateral quietly stopped polling the one token with money riding on it. The
 * resting sell then sat unfillable against a book frozen at kickoff while the
 * real bid ran from 0.65 clean through our 0.67 limit to 0.74.
 *
 * So liquidity rank decides what we WATCH; an open position decides what we must
 * keep watching regardless of rank. Recorder-only -- the policy picks its line
 * through deepestLineForGame(), which is unaffected by this union.
 */
async function linesToRecord() {
  const c = cfg();
  const perGame = c.ingest.totalsLinesRecordedPerGame;
  const hours = c.ingest.recordWindowHours || c.policy.openWindowHoursBeforeKickoff;

  // Two sources unioned in one round trip: `ranked` is the liquidity view of the
  // games in the window, `held` is every line with shares against it. distinct on
  // (condition_id) collapses the overlap -- normally the held line IS the deepest,
  // and then this query returns exactly what it did before.
  const { rows } = await q(
    `with ranked as (
       select t.*, g.kickoff
         from sports.nfl_games g
         join lateral (
           select * from sports.pm_totals_markets m
            where m.game_id = g.game_id
              and m.closed = false
              and m.over_token_id is not null
            order by m.liquidity_num desc nulls last
            limit $1
         ) t on true
        where g.kickoff between now() - interval '6 hours'
                            and now() + ($2 || ' hours')::interval
     ), held as (
       select m.*, g.kickoff
         from bots.positions p
         join sports.pm_totals_markets m
           on m.game_id = p.game_id
          and p.token_id in (m.over_token_id, m.under_token_id)
         join sports.nfl_games g on g.game_id = m.game_id
        where p.shares > 0
          and p.market_type = 'totals'
          and m.closed = false
          and m.over_token_id is not null
          and g.home_score is null
          and g.kickoff > now() - interval '12 hours'
     ), merged as (
       select distinct on (condition_id) *
         from (select * from ranked union all select * from held) u
        order by condition_id, liquidity_num desc nulls last
     )
     select * from merged order by kickoff, liquidity_num desc nulls last`,
    [perGame, String(hours)],
  );
  return rows;
}

/**
 * Record one snapshot of every tracked totals book.
 *
 * The window deliberately extends past kickoff, for two reasons. The price at
 * kickoff is the closing line every trade is graded against, so it has to be
 * captured; and Argo-7's resting sells RIDE THROUGH KICKOFF, so an in-play game
 * with a live order still needs fresh depth or the paper filler has nothing to
 * walk and the order would sit unfillable while the price ran through it.
 */
async function recordTotalsBooks() {
  const { bookLevelsStored } = cfg().ingest;
  const markets = await linesToRecord();

  // Far-out games are sampled far less often -- see dueMarkets(). Skipping the
  // FETCH as well as the write is the point: it saves the HTTP call too.
  const { due, deferred } = dueMarkets(markets);
  if (deferred) log(`  ${deferred} market(s) not due for a poll yet`);

  const out = [];
  for (const m of due) {
    for (const side of ["over", "under"]) {
      const tokenId = side === "over" ? m.over_token_id : m.under_token_id;
      if (!tokenId) continue;
      try {
        const snap = normaliseBook(await fetchBook(tokenId), bookLevelsStored);
        if (snap.mid === null) continue;            // empty book, nothing to log
        out.push({ condition_id: m.condition_id, token_id: tokenId,
                   game_id: m.game_id, side, ...snap });
      } catch (e) {
        log.warn(`totals book fetch failed ${m.game_id}@${m.line}/${side}: ${e.message}`);
      }
    }
  }

  const games = new Set(markets.map((m) => m.game_id)).size;
  const res = await writeBooks(out, `totals books (${markets.length} lines / ${games} games)`);
  return res.written;
}

/**
 * The single line Argo-7 will trade on a game: the deepest book.
 *
 * Deliberately ONE line, not several. Taking three lines on the same game is the
 * same bet three times -- they are correlated almost perfectly -- while looking
 * like diversification in the trade log and consuming three times the exposure cap.
 */
async function deepestLineForGame(gameId) {
  const { rows } = await q(
    `select * from sports.pm_totals_markets
      where game_id = $1 and closed = false and over_token_id is not null
      order by liquidity_num desc nulls last limit 1`,
    [gameId],
  );
  return rows[0] || null;
}

/** Latest recorded book for one totals market side. */
async function latestTotalsBook(conditionId, side) {
  const { rows } = await q(
    `select * from sports.odds_history
      where condition_id = $1 and side = $2
      order by ts desc limit 1`,
    [conditionId, side],
  );
  return rows[0] || null;
}

module.exports = {
  fetchNflTotals, selectGameTotals, mapTotalsToGames, linesToRecord,
  recordTotalsBooks, deepestLineForGame, latestTotalsBook, TOTALS_SLUG,
};
