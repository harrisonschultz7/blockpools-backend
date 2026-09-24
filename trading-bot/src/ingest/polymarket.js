// trading-bot/src/ingest/polymarket.js
//
// Market discovery + THE DEPTH RECORDER.
//
// Priority note: Polymarket's CLOB serves current book state only. Historical
// depth cannot be bought, scraped or reconstructed after the fact from any
// source. Every hour this job is not running is an hour of backtest fidelity
// permanently gone -- which is why it ships before any model code.

const { cfg, ENV } = require("../config");
const { getJson } = require("../http");
const { q, bulkInsert } = require("../db");
const log = require("../log");

const GAME_SLUG = /^nfl-([a-z]{2,4})-([a-z]{2,4})-(\d{4})-(\d{2})-(\d{2})$/;

// Polymarket slug codes -> nflverse team codes. Only codes that actually differ
// need listing; everything else upper-cases cleanly.
const TEAM_ALIAS = {
  lar: "LA", la: "LA", ram: "LA",
  lac: "LAC", sd: "LAC",
  lv: "LV", lvr: "LV", oak: "LV",
  jac: "JAX", jax: "JAX",
  wsh: "WAS", was: "WAS", wft: "WAS",
  gnb: "GB", gb: "GB",
  kan: "KC", kc: "KC",
  nwe: "NE", ne: "NE",
  nor: "NO", no: "NO",
  sfo: "SF", sf: "SF",
  tam: "TB", tb: "TB",
  ari: "ARI", crd: "ARI",
  bal: "BAL", rav: "BAL",
  ten: "TEN", oti: "TEN",
  ind: "IND", clt: "IND",
  hou: "HOU", htx: "HOU",
};
const toNflverse = (code) =>
  TEAM_ALIAS[String(code).toLowerCase()] || String(code).toUpperCase();

/**
 * Gamma returns "2026-09-25 00:15:00+00": a space separator and a two-digit
 * offset. Date.parse rejects both -- `new Date("...T00:15:00+00")` is an
 * Invalid Date, which silently made every market unmappable.
 */
function parsePmTime(v) {
  if (!v) return null;
  const iso = String(v).replace(" ", "T").replace(/([+-]\d{2})$/, "$1:00");
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? null : d;
}

/**
 * Fetch every open NFL game moneyline.
 *
 * Two things this has to get right:
 *  - sports_market_types=moneyline filters server-side. Tag 450 alone returns
 *    ~1200 markets that are mostly props, futures and celebrity questions.
 *  - order=id is a UNIQUE sort key. Paging on order=gameStartTime silently
 *    returns overlapping pages (rows share a start time, futures have none),
 *    which produced 295 duplicate conditionIds and hid all but one of the 75
 *    real game markets.
 */
async function fetchNflMarkets({ maxPages = 12 } = {}) {
  const { pmNflTagId } = cfg().ingest;
  const seen = new Map();
  for (let page = 0; page < maxPages; page++) {
    const u =
      ENV.GAMMA_API_URL + "/markets?tag_id=" + pmNflTagId + "&closed=false" +
      "&sports_market_types=moneyline&limit=100&offset=" + page * 100 +
      "&order=id&ascending=true";
    const batch = await getJson(u);
    if (!Array.isArray(batch) || !batch.length) break;
    for (const m of batch) if (m && m.conditionId) seen.set(m.conditionId, m);
    if (batch.length < 100) break;
  }
  return [...seen.values()];
}

/** Single-game markets only -- drops futures, which share the moneyline type. */
function selectMoneylines(markets) {
  return markets.filter((m) => m && GAME_SLUG.test(m.slug || ""));
}

function parseJsonField(v, fallback) {
  if (Array.isArray(v)) return v;
  try { return JSON.parse(v); } catch { return fallback; }
}

/**
 * Resolve Polymarket markets to nflverse game_ids.
 *
 * Matching on the slug date alone is wrong: the slug carries the UTC date while
 * nflverse `gameday` is the US-Eastern date, so every Sunday-night and Monday
 * game is off by one. Match on team codes plus a kickoff window instead.
 */
async function mapMarketsToGames(markets) {
  const rows = [];
  for (const m of markets) {
    const sm = GAME_SLUG.exec(m.slug);
    if (!sm) continue;
    const away = toNflverse(sm[1]);
    const home = toNflverse(sm[2]);
    const kickoff = parsePmTime(m.gameStartTime);

    let game_id = null;
    let confidence = "unmapped";
    if (kickoff) {
      const r = await q(
        `select game_id
           from sports.nfl_games
          where home_team = $1 and away_team = $2
            and kickoff between $3::timestamptz - interval '36 hours'
                            and $3::timestamptz + interval '36 hours'
          order by abs(extract(epoch from (kickoff - $3::timestamptz)))
          limit 1`,
        [home, away, kickoff.toISOString()],
      );
      if (r.rows[0]) { game_id = r.rows[0].game_id; confidence = "exact"; }
    }
    if (!game_id) {
      log.warn(`unmapped PM market ${m.slug} (${away} @ ${home}) -- check TEAM_ALIAS`);
    }

    const tokens = parseJsonField(m.clobTokenIds, []);
    const outcomes = parseJsonField(m.outcomes, []);
    // Slug order is away-home, and Gamma lists outcomes in that same order.
    rows.push({
      condition_id: m.conditionId,
      slug: m.slug,
      question: m.question,
      game_id,
      away_token_id: tokens[0] || null,
      home_token_id: tokens[1] || null,
      away_outcome: outcomes[0] || null,
      home_outcome: outcomes[1] || null,
      end_date: m.endDate || null,
      closed: !!m.closed,
      map_confidence: confidence,
    });
  }

  // Postgres refuses an ON CONFLICT DO UPDATE that touches the same row twice,
  // so the batch must be unique on condition_id before it is sent.
  const unique = [...new Map(rows.map((r) => [r.condition_id, r])).values()];

  if (unique.length) {
    const cols = Object.keys(unique[0]);
    const updates = cols.filter((c) => c !== "condition_id")
      .map((c) => `${c} = excluded.${c}`).join(", ");
    await bulkInsert("sports.pm_markets", cols, unique, {
      onConflict: `on conflict (condition_id) do update set ${updates}, last_seen = now()`,
    });
  }
  const mapped = unique.filter((r) => r.game_id).length;
  log(`pm markets: ${unique.length} moneylines, ${mapped} mapped to games`);
  return unique;
}

// == Book snapshots =========================================================
// Normalises one CLOB book into the row shape odds_history stores.
// Verified against the live API: bids and asks are BOTH sorted ascending by
// price, so the best of each is the LAST element. Reading index 0 would hand
// the model the worst price in the book.
// Persisted BEST-FIRST, because the paper filler walks them in order.
function normaliseBook(book, levelsToStore) {
  const rawBids = Array.isArray(book.bids) ? book.bids : [];
  const rawAsks = Array.isArray(book.asks) ? book.asks : [];

  const clean = (arr) => arr
    .map((l) => [Number(l.price), Number(l.size)])
    .filter(([p, s]) => Number.isFinite(p) && Number.isFinite(s) && s > 0);

  const bids = clean(rawBids).sort((a, b) => b[0] - a[0]);  // best = highest
  const asks = clean(rawAsks).sort((a, b) => a[0] - b[0]);  // best = lowest

  const bestBid = bids.length ? bids[0][0] : null;
  const bestAsk = asks.length ? asks[0][0] : null;
  const mid = bestBid !== null && bestAsk !== null ? (bestBid + bestAsk) / 2
            : bestBid !== null ? bestBid
            : bestAsk !== null ? bestAsk
            : null;

  const depth = (levels, best, isBid) =>
    best === null ? null
      : levels.filter(([p]) => (isBid ? p >= best - 0.01 : p <= best + 0.01))
              .reduce((s, [p, sz]) => s + p * sz, 0);

  return {
    mid,
    best_bid: bestBid,
    best_ask: bestAsk,
    spread: bestBid !== null && bestAsk !== null ? bestAsk - bestBid : null,
    bid_depth_usd: depth(bids, bestBid, true),
    ask_depth_usd: depth(asks, bestAsk, false),
    bids: JSON.stringify(bids.slice(0, levelsToStore)),
    asks: JSON.stringify(asks.slice(0, levelsToStore)),
  };
}

async function fetchBook(tokenId) {
  return getJson(ENV.CLOB_API_URL + "/book?token_id=" + tokenId,
                 { retries: 1, timeoutMs: 15000 });
}

/**
 * Record one snapshot of every tradeable pre-game NFL book.
 *
 * The window extends a little PAST kickoff on purpose: the bot never trades
 * in-play (no live drive-level feed is wired), but the price at kickoff is the
 * closing line every trade is graded against, so it must be captured.
 */
async function recordBooks() {
  const c = cfg();
  const { bookLevelsStored, recordWindowHours } = c.ingest;
  // Record far wider than we trade: an unrecorded book is gone forever, while
  // an early recording costs one HTTP call.
  const hoursBefore = recordWindowHours || c.policy.openWindowHoursBeforeKickoff;

  const { rows: markets } = await q(
    `select m.condition_id, m.game_id, m.home_token_id, m.away_token_id, g.kickoff
       from sports.pm_markets m
       join sports.nfl_games g on g.game_id = m.game_id
      where m.closed = false
        and m.home_token_id is not null
        and g.kickoff between now() - interval '6 hours'
                          and now() + ($1 || ' hours')::interval
      order by g.kickoff`,
    [String(hoursBefore)],
  );

  const out = [];
  for (const m of markets) {
    for (const side of ["home", "away"]) {
      const tokenId = side === "home" ? m.home_token_id : m.away_token_id;
      if (!tokenId) continue;
      try {
        const snap = normaliseBook(await fetchBook(tokenId), bookLevelsStored);
        if (snap.mid === null) continue;          // empty book, nothing to log
        out.push({ condition_id: m.condition_id, token_id: tokenId,
                   game_id: m.game_id, side, ...snap });
      } catch (e) {
        log.warn(`book fetch failed ${m.game_id}/${side}: ${e.message}`);
      }
    }
  }

  if (out.length) {
    await bulkInsert(
      "sports.odds_history",
      ["condition_id", "token_id", "game_id", "side", "mid", "best_bid",
       "best_ask", "spread", "bid_depth_usd", "ask_depth_usd", "bids", "asks"],
      out,
    );
  }
  log(`books: ${out.length} snapshots across ${markets.length} markets`);
  return out.length;
}

/** Latest recorded book for one game side -- what the bot prices against. */
async function latestBook(gameId, side) {
  const r = await q(
    `select * from sports.odds_history
      where game_id = $1 and side = $2
      order by ts desc limit 1`,
    [gameId, side],
  );
  return r.rows[0] || null;
}

module.exports = {
  fetchNflMarkets, selectMoneylines, mapMarketsToGames,
  normaliseBook, fetchBook, recordBooks, latestBook,
  toNflverse, parsePmTime, parseJsonField, GAME_SLUG, TEAM_ALIAS,
};
