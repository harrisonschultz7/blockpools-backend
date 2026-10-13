// ---------------------------------------------------------------------------
// model trade alert worker
//
// When a BlockPools model (Adam-7 / Argo-7) acts, alert every user who FOLLOWS
// that model (public.model_follows). Alerts only -- no copy/execution, so no
// Polymarket geoblock. Three event kinds:
//   buy   -- opened a position            (bots.trades.opened_at)
//   limit -- set its resting limit sell   (bots.limit_orders, action='SELL')
//   sell  -- the position was sold/exited  (bots.trades.exit_at)
//
// Idempotent: each event fans out once (deduped by `mtrade:<event>:<id>`), and
// only to followers who were already following when the event happened. Polls a
// short recent window and leans on the dedupe, so no cursor table is needed.
// ---------------------------------------------------------------------------
import { pg } from "../db/pg";
import { notifyModelTradeToFollowers } from "../services/notifications/notify";

const INTERVAL_MS = Number(process.env.MODEL_ALERT_INTERVAL_MS || 5 * 60_000);
const WINDOW = process.env.MODEL_ALERT_WINDOW || "6 hours";

function positionLabel(row: any): string {
  const side = String(row.side || "").toLowerCase();
  if (String(row.market_type || "") === "totals") {
    const ou = side === "over" ? "Over" : "Under";
    return row.line == null ? ou : `${ou} ${Number(row.line)}`;
  }
  // moneyline: the team backed (away_team/home_team are short codes)
  return side === "home" ? String(row.home_team || "") : String(row.away_team || "");
}

const pct = (v: any) => (v == null ? null : Math.round(Number(v) * 100));

let running = false;

export async function runModelAlertsOnce(): Promise<void> {
  if (running) return;
  running = true;
  try {
    // Only models with at least one follower are worth scanning.
    const { rows: followed } = await pg.query(
      `SELECT DISTINCT bot_id FROM public.model_follows`,
    );
    if (!followed.length) return;
    const botIds = followed.map((r: any) => r.bot_id);

    // 1) BUYS (opens) + 3) SELLS (exits) — both live on bots.trades.
    const { rows: trades } = await pg.query(
      `SELECT t.id, t.bot_id, b.name AS bot_name, t.side, t.market_type, t.line,
              t.fill_price, t.exit_price,
              extract(epoch FROM t.opened_at)::bigint AS opened_sec,
              extract(epoch FROM t.exit_at)::bigint   AS exit_sec,
              (t.exit_at IS NOT NULL) AS exited,
              g.away_team, g.home_team
         FROM bots.trades t
         JOIN bots.bot b ON b.id = t.bot_id
         JOIN sports.nfl_games g ON g.game_id = t.game_id
        WHERE t.bot_id = ANY($1)
          AND (t.opened_at > now() - ($2)::interval
               OR (t.exit_at IS NOT NULL AND t.exit_at > now() - ($2)::interval))
        ORDER BY t.opened_at ASC`,
      [botIds, WINDOW],
    );

    for (const r of trades) {
      const base = {
        botId: r.bot_id,
        botName: r.bot_name || r.bot_id,
        position: positionLabel(r),
        matchup: `${r.away_team} @ ${r.home_team}`,
        league: "NFL",
      };
      // BUY (dedupe + follower-time gating make a re-emit a no-op, so no window
      // guard needed even when this row was selected only for its recent exit).
      await notifyModelTradeToFollowers({
        ...base, event: "buy", entityId: `t${r.id}`,
        tradeAtSec: Number(r.opened_sec), pricePct: pct(r.fill_price),
      });
      // SELL — the exit filled inside the window.
      if (r.exited && r.exit_sec != null) {
        await notifyModelTradeToFollowers({
          ...base, event: "sell", entityId: `t${r.id}`,
          tradeAtSec: Number(r.exit_sec), pricePct: pct(r.exit_price),
        });
      }
    }

    // 2) LIMIT SELLS set — the resting sell the bot places after a buy. One row
    //    per position (reprices update it in place, so created_at = placement).
    const { rows: limits } = await pg.query(
      `SELECT o.id, o.bot_id, b.name AS bot_name, o.side, o.market_type, o.line,
              coalesce(o.original_price, o.limit_price) AS px,
              extract(epoch FROM o.created_at)::bigint AS set_sec,
              g.away_team, g.home_team
         FROM bots.limit_orders o
         JOIN bots.bot b ON b.id = o.bot_id
         JOIN sports.nfl_games g ON g.game_id = o.game_id
        WHERE o.bot_id = ANY($1) AND o.action = 'SELL'
          AND o.created_at > now() - ($2)::interval`,
      [botIds, WINDOW],
    );
    for (const r of limits) {
      await notifyModelTradeToFollowers({
        botId: r.bot_id,
        botName: r.bot_name || r.bot_id,
        position: positionLabel(r),
        matchup: `${r.away_team} @ ${r.home_team}`,
        league: "NFL",
        event: "limit",
        entityId: `o${r.id}`,
        tradeAtSec: Number(r.set_sec),
        pricePct: pct(r.px),
      });
    }
  } catch (e: any) {
    console.error(`[model-alert] run error: ${e?.message || e}`);
  } finally {
    running = false;
  }
}

export function startModelAlertCron(): void {
  if (String(process.env.MODEL_ALERTS_ENABLED || "true").toLowerCase() === "false") {
    console.log("[model-alert] disabled (MODEL_ALERTS_ENABLED=false)");
    return;
  }
  console.log(`[model-alert] cron enabled — every ${Math.round(INTERVAL_MS / 60000)}m`);
  setTimeout(() => void runModelAlertsOnce(), 45_000);
  setInterval(() => void runModelAlertsOnce(), INTERVAL_MS);
}
