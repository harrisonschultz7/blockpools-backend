// trading-bot/src/ingest/polymarketNba.js
//
// NBA game moneylines on Polymarket: discovery, mapping to sports.nba_games, and
// the depth recorder. The NFL module's book handling is reused unchanged
// (normaliseBook / fetchBook / writeBooks' change-only writes); only what is
// league-specific lives here.
//
// NBA markets are stored in sports.pm_markets beside the NFL ones (migration
// 010 dropped that table's NFL-only foreign key), so the exit manager and
// settle-from-resolution find them with no extra joins.

const { cfg, ENV } = require("../config");
const { getJson } = require("../http");
const { q, bulkInsert } = require("../db");
const { writeBooks, dueMarkets } = require("./bookWriter");
const { normaliseBook, fetchBook, parsePmTime, parseJsonField } = require("./polymarket");
const log = require("../log");

// nba-bos-orl-2026-11-01: away-home, then the US-Eastern date. Outcomes come
// in the same away, home order.
const NBA_SLUG = /^nba-([a-z]{2,4})-([a-z]{2,4})-(\d{4})-(\d{2})-(\d{2})$/;

/** Every open NBA moneyline (futures share the type; the slug filter drops them). */
async function fetchNbaMarkets({ maxPages = 10 } = {}) {
  const tag = cfg().ingest.pmNbaTagId;
  const seen = new Map();
  for (let page = 0; page < maxPages; page++) {
    // order=id: a UNIQUE sort key -- paging on start time returns overlapping
    // pages (see ingest/polymarket.js fetchNflMarkets).
    const batch = await getJson(
      `${ENV.GAMMA_API_URL}/markets?tag_id=${tag}&closed=false&sports_market_types=moneyline` +
      `&limit=100&offset=${page * 100}&order=id&ascending=true`);
    if (!Array.isArray(batch) || !batch.length) break;
    for (const m of batch) if (m?.conditionId && NBA_SLUG.test(m.slug || "")) seen.set(m.conditionId, m);
    if (batch.length < 100) break;
  }
  return [...seen.values()];
}

/**
 * Map markets to games on team codes + tip-off within 6h. The slug codes ARE
 * our codes (nba_games stores Polymarket's), so no alias table is needed -- a
 * mismatch is logged, never guessed.
 */
async function mapNbaMarkets(markets) {
  const rows = [];
  for (const m of markets) {
    const sm = NBA_SLUG.exec(m.slug);
    const away = sm[1].toUpperCase();
    const home = sm[2].toUpperCase();
    const tip = parsePmTime(m.gameStartTime);
    let game_id = null;
    if (tip) {
      const r = await q(
        `select game_id from sports.nba_games
          where home_team = $1 and away_team = $2
            and kickoff between $3::timestamptz - interval '6 hours' and $3::timestamptz + interval '6 hours'
          order by abs(extract(epoch from (kickoff - $3::timestamptz))) limit 1`,
        [home, away, tip.toISOString()]);
      game_id = r.rows[0]?.game_id || null;
    }
    // Polymarket lists games ~2 weeks out; the schedule is only synced
    // ingest.scheduleDaysAhead ahead, so a far game is unmapped by design and maps
    // itself once it comes inside that horizon. Only a NEAR one is a real miss.
    const horizon = Date.now() + (cfg().ingest.scheduleDaysAhead + 0.5) * 86_400_000;
    if (!game_id && tip && tip.getTime() <= horizon) log.warn(`unmapped NBA market ${m.slug} (${away} @ ${home})`);
    const tokens = parseJsonField(m.clobTokenIds, []);
    const outcomes = parseJsonField(m.outcomes, []);
    rows.push({
      condition_id: m.conditionId, slug: m.slug, question: m.question, game_id,
      away_token_id: tokens[0] || null, home_token_id: tokens[1] || null,
      away_outcome: outcomes[0] || null, home_outcome: outcomes[1] || null,
      end_date: m.endDate || null, closed: !!m.closed,
      map_confidence: game_id ? "exact" : "unmapped",
    });
  }
  if (rows.length) {
    const cols = Object.keys(rows[0]);
    const updates = cols.filter((c) => c !== "condition_id").map((c) => `${c} = excluded.${c}`).join(", ");
    await bulkInsert("sports.pm_markets", cols, rows, {
      onConflict: `on conflict (condition_id) do update set ${updates}, last_seen = now()`,
    });
  }
  log(`nba pm markets: ${rows.length} moneylines, ${rows.filter((r) => r.game_id).length} mapped`);
  return rows;
}

/**
 * One snapshot of every NBA book worth recording: games inside the record
 * window, plus any game an NBA bot still has a resting order on (exits ride
 * through tip-off, and an unrecorded in-play book can never fill them). One
 * recorder serves Axis-3 and Bain-3.
 */
async function recordNbaBooks() {
  const c = cfg();
  const { rows: markets } = await q(
    `select distinct m.condition_id, m.game_id, m.home_token_id, m.away_token_id, g.kickoff
       from sports.pm_markets m
       join sports.nba_games g on g.game_id = m.game_id
       left join bots.limit_orders o
              on o.game_id = m.game_id and o.status = 'open'
      where m.closed = false and m.home_token_id is not null and g.home_score is null
        and (g.kickoff between now() - interval '4 hours' and now() + ($1 || ' hours')::interval
             or (o.id is not null and g.kickoff > now() - interval '8 hours'))
      order by g.kickoff`,
    [String(c.ingest.recordWindowHours)]);

  const { due } = dueMarkets(markets);
  const out = [];
  for (const m of due) {
    for (const side of ["home", "away"]) {
      const tokenId = side === "home" ? m.home_token_id : m.away_token_id;
      try {
        const snap = normaliseBook(await fetchBook(tokenId), c.ingest.bookLevelsStored);
        if (snap.mid === null) continue;
        out.push({ condition_id: m.condition_id, token_id: tokenId, game_id: m.game_id, side, ...snap });
      } catch (e) { log.warn(`nba book ${m.game_id}/${side}: ${e.message}`); }
    }
  }
  const res = await writeBooks(out, `nba books (${markets.length} markets)`);
  return res.written;
}

module.exports = { fetchNbaMarkets, mapNbaMarkets, recordNbaBooks, NBA_SLUG };
