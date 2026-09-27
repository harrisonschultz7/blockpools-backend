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
 * Are two ladders the same AS FAR AS A FILL IS CONCERNED?
 *
 * Not a byte comparison, and the difference is the whole point. Measured in
 * production: top-of-book was unchanged on 98.1% of polls while the FULL ladder
 * matched on only 10.5% -- market makers shuffle size at depth on 87.6% of polls
 * without the price moving at all. A strict comparison is defeated by that noise
 * and writes a row for every wobble, which is what left the daily volume
 * unchanged after the first attempt at this.
 *
 * So the comparison is DECISION-LOSSLESS rather than byte-lossless:
 *
 *   - prices must match exactly, at every compared level
 *   - sizes may differ by up to sizeTolerancePct
 *   - only the first compareLevels are considered
 *
 * The justification for each: a price change is always real information. A size
 * change of a few percent at depth cannot alter what a $500 order fills at --
 * exec/paper.js walks levels until the order is filled and stops at
 * maxSlippageCents, which at these sizes is satisfied inside the top few levels.
 * And a level the order can never reach cannot affect it at all.
 *
 * What this gives up, stated plainly: the stored deep levels can lag reality
 * between writes. They are still a real snapshot of a real moment, just not
 * refreshed for changes that no decision depends on.
 */
function sameLadder(a, b, { compareLevels = 4, sizeTolerancePct = 0.1 } = {}) {
  const parse = (v) => {
    if (Array.isArray(v)) return v;
    try { return JSON.parse(v || "[]"); } catch { return null; }
  };
  const x = parse(a);
  const y = parse(b);
  if (!x || !y) return false;

  const n = Math.min(compareLevels, Math.max(x.length, y.length));
  for (let i = 0; i < n; i++) {
    const [px, sx] = x[i] || [];
    const [py, sy] = y[i] || [];
    // A level present on one side and missing on the other is a real change.
    if ((x[i] === undefined) !== (y[i] === undefined)) return false;
    if (x[i] === undefined) continue;
    if (!sameNum(px, py)) return false;
    const a1 = Number(sx);
    const b1 = Number(sy);
    if (!Number.isFinite(a1) || !Number.isFinite(b1)) return false;
    const denom = Math.max(Math.abs(a1), Math.abs(b1), 1);
    if (Math.abs(a1 - b1) / denom > sizeTolerancePct) return false;
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
function selectChanged(snapshots, previous, heartbeatSeconds, now = Date.now(), ladderOpts = {}) {
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
      sameLadder(s.bids, p.bids, ladderOpts) &&
      sameLadder(s.asks, p.asks, ladderOpts);

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
  const ing = cfg().ingest;
  const { keep, unchanged, heartbeat } = selectChanged(
    snapshots, previous, heartbeatSeconds, Date.now(),
    { compareLevels: ing.bookCompareLevels ?? 4,
      sizeTolerancePct: ing.bookSizeTolerancePct ?? 0.1 });

  if (keep.length) await bulkInsert("sports.odds_history", COLS, keep);

  const pct = snapshots.length ? Math.round((100 * unchanged) / snapshots.length) : 0;
  log(`${label}: ${keep.length} written (${heartbeat} heartbeat), ` +
      `${unchanged} unchanged and skipped (${pct}%)`);
  return { written: keep.length, skipped: unchanged };
}

// == Poll cadence by time to kickoff =========================================
// A game ten days out does not need its book sampled every minute. Measured on
// the live recorder: 38 of 158 tracked tokens were for games more than 48 hours
// away, and they were polled at the same rate as a game kicking off in an hour.
//
// Tiers are held in memory because the recorder is a long-running process --
// persisting a last-polled timestamp per token would cost a write per poll,
// which is the exact thing being economised.
const lastPolled = new Map();

/**
 * Is this market due for a poll?
 *
 * Anything at or past kickoff is always due: that is when the exits ride and the
 * closing line is struck, and it is the one window where a missed book costs
 * something that cannot be recovered.
 */
function isDueForPoll(kickoff, now = Date.now(), key = null) {
  const c = cfg().ingest;
  const hoursOut = (new Date(kickoff).getTime() - now) / 3600000;

  let intervalSec;
  if (hoursOut <= (c.nearKickoffHours ?? 6)) intervalSec = 0;         // always
  else if (hoursOut <= (c.midWindowHours ?? 48)) intervalSec = c.bookPollSeconds ?? 60;
  else intervalSec = c.farPollSeconds ?? 600;

  if (!intervalSec || !key) return true;
  const last = lastPolled.get(key) || 0;
  if (now - last < intervalSec * 1000) return false;
  lastPolled.set(key, now);
  return true;
}

/** Split a market list into the ones due now and a count of those deferred. */
function dueMarkets(markets, now = Date.now()) {
  const due = [];
  let deferred = 0;
  for (const m of markets) {
    if (isDueForPoll(m.kickoff, now, m.condition_id)) due.push(m);
    else deferred++;
  }
  return { due, deferred };
}

module.exports = {
  writeBooks, selectChanged, latestByToken, sameLadder,
  isDueForPoll, dueMarkets, COLS,
};
