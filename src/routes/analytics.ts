// src/routes/analytics.ts
//
// First-party click & navigation analytics ingest.
//
//   POST /api/analytics/track   body: { events: Evt[] }
//
// Accepts a batch of click / page_view events from the frontend and inserts
// them into public.analytics_events via the service-role client. Analytics is
// best-effort: this route NEVER returns 5xx to the client — a failed insert is
// logged and swallowed so it can never break or block the UI.
//
// Mount in server.ts:
//   import analyticsRouter from "./routes/analytics";
//   app.use("/api/analytics", analyticsRouter);

import { Router, Request, Response } from "express";
import { supabaseAdmin } from "../services/groups/supabaseAdmin";

const router = Router();

// Cap per request so a malformed/abusive client can't push a huge batch.
const MAX_EVENTS = 50;

// Convert a client epoch-ms timestamp to an ISO string, guarding against
// missing/garbage values and absurd clocks (before 2020 or far in the future).
function occurredAtIso(ms: unknown): string | null {
  if (!Number.isFinite(ms as number)) return null;
  const n = Number(ms);
  const MIN = 1577836800000; // 2020-01-01
  const MAX = Date.now() + 60_000; // allow 60s clock skew
  if (n < MIN || n > MAX) return null;
  return new Date(n).toISOString();
}

router.post("/track", async (req: Request, res: Response) => {
  try {
    const events = Array.isArray(req.body?.events) ? req.body.events : [];
    if (events.length === 0) return res.status(204).end();

    const rows = events.slice(0, MAX_EVENTS).map((e: any) => ({
      session_id: String(e.sessionId ?? ""),
      visitor_id: e.visitorId ? String(e.visitorId).slice(0, 64) : null,
      wallet_address: e.walletAddress
        ? String(e.walletAddress).toLowerCase()
        : null,
      event_type: e.eventType === "click" ? "click" : "page_view",
      name: String(e.name ?? "").slice(0, 200),
      page_path: String(e.pagePath ?? "").slice(0, 300),
      duration_ms: Number.isFinite(e.durationMs) ? Math.round(e.durationMs) : null,
      metadata: e.metadata ?? null,
      // Real client event time (epoch ms) — preserves true ordering/timing that
      // created_at (set at batch-insert time) collapses. Reject implausible
      // values (> ~now, or before 2020) so a bad client clock can't poison data.
      occurred_at: occurredAtIso(e.occurredAt),
      locale: e.locale ? String(e.locale).slice(0, 20) : null,
      device: e.device ? String(e.device).slice(0, 20) : null,
    }));

    const { error } = await supabaseAdmin().from("analytics_events").insert(rows);
    if (error) throw error;

    res.status(204).end();
  } catch (err) {
    console.error("[analytics] track failed", err);
    // Analytics must never break the UI — always succeed from the client's view.
    res.status(204).end();
  }
});

export default router;
