// ---------------------------------------------------------------------------
// model trade alert worker
//
// When a BlockPools model (Adam-7 / Argo-7) opens a trade, alert every user who
// FOLLOWS that model (public.model_follows). Alerts only -- no copy/execution,
// so none of the Polymarket geoblock applies. Mirrors the user-follow trade
// alerts (notifyTradeToFollowers), but sourced from bots.trades.
//
// Idempotent: each trade fans out once (notifications deduped by `mtrade:<id>`),
// and only to followers who were already following when the trade opened. Polls
// a short recent window and leans on the dedupe, so no cursor table is needed.
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

    const { rows } = await pg.query(
      `SELECT t.id, t.bot_id, b.name AS bot_name, t.side, t.market_type, t.line,
              t.fill_price, extract(epoch FROM t.opened_at)::bigint AS opened_sec,
              g.away_team, g.home_team
         FROM bots.trades t
         JOIN bots.bot b ON b.id = t.bot_id
         JOIN sports.nfl_games g ON g.game_id = t.game_id
        WHERE t.bot_id = ANY($1)
          AND t.opened_at > now() - ($2)::interval
        ORDER BY t.opened_at ASC`,
      [botIds, WINDOW],
    );

    for (const r of rows) {
      const pricePct =
        r.fill_price == null ? null : Math.round(Number(r.fill_price) * 100);
      await notifyModelTradeToFollowers({
        botId: r.bot_id,
        botName: r.bot_name || r.bot_id,
        tradeId: r.id,
        tradeAtSec: Number(r.opened_sec),
        position: positionLabel(r),
        side: "BUY", // models buy to open
        pricePct,
        matchup: `${r.away_team} @ ${r.home_team}`,
        league: "NFL",
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
