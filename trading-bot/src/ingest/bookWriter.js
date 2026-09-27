// trading-bot/src/ingest/bookWriter.js
//
// WRITE A BOOK SNAPSHOT ONLY WHEN THE BOOK CHANGED.
//
// The depth recorder polled every token every 60s and inserted unconditionally.
// Measured on the live database: 195,662 rows and 129 MB in a single day once
// the totals recorder joined the moneyline one, against 34 rows the day before,
// and 66% of consecutive snapshots were byte-identical to the one before them.
// sports.odds_history reached 448 MB of a 782 MB database -- 57% of everything,
// on a 500 MB plan.
//
// Dedup here is LOSSLESS for every consumer, because they all ask the same
// question: "the book as at time T", answered by the newest row at or before T.
// A row that would have been identical to its predecessor adds nothing to that
// answer. Nothing reconstructs a per-minute series from this table.
//
// THE ONE THING DEDUP BREAKS, and why heartbeatSeconds exists: the policy rejects
// a stale book (staleBookSeconds, 900). With pure dedup, a book that is quiet but
// perfectly current would have an old ts and be refused -- the bot would stop
// trading precisely the calm markets it is happiest in. So an unchanged book is
// still written once every heartbeatSeconds, which bounds ts age without
// restoring the per-minute firehose.
//
// At a 60s poll and a 600s heartbeat, a quiet token costs 1 row per 10 minutes
// instead of 10 -- a 90% cut -- while an active one is recorded on every change,
// which is the data that actually matters.

const { cfg } = require("../config");
const { q, bulkInsert } = require("../db");
const log = require("../log");

const COLS = [
  "condition_id", "token_id", "game_id", "side", "mid", "best_bid",
  "best_ask", "spread", "bid_depth_usd", "ask_depth_usd", "bids", "asks",
];

/**
 * The newest stored snapshot for each of these tokens, in one round trip.
 *
 * LATERAL over unnest, not `where token_id = any($1)`. The any() form let the
 * planner fall back to a scan of a 450 MB table and blew the statement timeout
 * on the moneyline recorder, which tracks ~150 tokens against the totals
 * recorder's 32. Driving from the token list forces one index seek per token on
 * odds_hist_token_ts_idx (token_id, ts desc), which is the access pattern the
 * index was built for.
 */
async function latestByToken(tokenIds) {
  if (!tokenIds.length) return new Map();
  const { rows } = await q(
    `select t.token_id, o.ts, o.mid, o.best_bid, o.best_ask,
            o.bid_depth_usd, o.ask_depth_usd,
            o.bids::text AS bids, o.asks::text AS asks
       from unnest($1::text[]) AS t(token_id)
       join lateral (
         select * from sports.odds_history o
          where o.token_id = t.token_id
          order by o.ts desc
          limit 1
       ) o on true`,
    [tokenIds],
  );
  return new Map(rows.map((r) => [r.token_id, r]));
}

/** Numeric compare that treats 0.5 and "0.50000" as the same price. */
const sameNum = (a, b) => {
  if (a === null || a === undefined) return b === null || b === undefined;
  if (b === null || b === undefined) return false;
  return Math.abs(Number(a) - Number(b)) < 1e-9;
};

/**
 * Compare two ladders by VALUE, never as strings.
 *
 * The stored side comes back from Postgres as jsonb canonical text -- "[[0.5,
 * 100], [0.49, 200]]", with spaces -- while a fresh snapshot is JSON.stringify
 * output with none. A string compare therefore never matches and the dedup
 * silently does nothing: the first run of this suppressed 0 of 32 snapshots that
 * had not moved in ten seconds, which is what gave it away.
 */
function sameLadder(a, b) {
  const parse = (v) => {
    if (Array.isArray(v)) return v;
    try { return JSON.parse(v || "[]"); } catch { return null; }
  };
  const x = parse(a);
  const y = parse(b);
  if (!x || !y || x.length !== y.length) return false;
  for (let i = 0; i < x.length; i++) {
    const [px, sx] = x[i] || [];
    const [py, sy] = y[i] || [];
    if (!sameNum(px, py) || !sameNum(sx, sy)) return false;
  }
  return true;
}

/**
 * Split fresh snapshots into those worth storing and those that are noise.
 *
 * The comparison includes the FULL ladder, not just top-of-book. A change at
 * level 4 is real information to the paper filler, which walks levels until the
 * order is filled -- treating it as unchanged would quietly degrade fill
 * fidelity, which is the one thing this whole table exists to protect.
 */
function selectChanged(snapshots, previous, heartbeatSeconds, now = Date.now()) {
  const keep = [];
  let unchanged = 0;
  let heartbeat = 0;

  for (const s of snapshots) {
    const p = previous.get(s.token_id);
    if (!p) { keep.push(s); continue; }

    const same =
      sameNum(s.mid, p.mid) &&
      sameNum(s.best_bid, p.best_bid) &&
      sameNum(s.best_ask, p.best_ask) &&
      sameNum(s.bid_depth_usd, p.bid_depth_usd) &&
      sameNum(s.ask_depth_usd, p.ask_depth_usd) &&
      sameLadder(s.bids, p.bids) &&
      sameLadder(s.asks, p.asks);

    if (!same) { keep.push(s); continue; }

    const ageSec = (now - new Date(p.ts).getTime()) / 1000;
    if (ageSec >= heartbeatSeconds) { keep.push(s); heartbeat++; continue; }
    unchanged++;
  }
  return { keep, unchanged, heartbeat };
}

/**
 * Persist a batch of snapshots, skipping the ones that changed nothing.
 * Returns what was written and what was suppressed, so the saving is visible in
 * the logs rather than being an invisible behaviour change.
 */
async function writeBooks(snapshots, label = "books") {
  if (!snapshots.length) { log(`${label}: nothing to record`); return { written: 0, skipped: 0 }; }

  const heartbeatSeconds = cfg().ingest.bookHeartbeatSeconds ?? 600;
  const previous = await latestByToken([...new Set(snapshots.map((s) => s.token_id))]);
  const { keep, unchanged, heartbeat } = selectChanged(snapshots, previous, heartbeatSeconds);

  if (keep.length) await bulkInsert("sports.odds_history", COLS, keep);

  const pct = snapshots.length ? Math.round((100 * unchanged) / snapshots.length) : 0;
  log(`${label}: ${keep.length} written (${heartbeat} heartbeat), ` +
      `${unchanged} unchanged and skipped (${pct}%)`);
  return { written: keep.length, skipped: unchanged };
}

module.exports = { writeBooks, selectChanged, latestByToken, sameLadder, COLS };
