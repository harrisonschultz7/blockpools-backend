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

router.post("/track", async (req: Request, res: Response) => {
  try {
    const events = Array.isArray(req.body?.events) ? req.body.events : [];
    if (events.length === 0) return res.status(204).end();

    const rows = events.slice(0, MAX_EVENTS).map((e: any) => ({
      session_id: String(e.sessionId ?? ""),
      wallet_address: e.walletAddress
        ? String(e.walletAddress).toLowerCase()
        : null,
      event_type: e.eventType === "click" ? "click" : "page_view",
      name: String(e.name ?? "").slice(0, 200),
      page_path: String(e.pagePath ?? "").slice(0, 300),
      duration_ms: Number.isFinite(e.durationMs) ? Math.round(e.durationMs) : null,
      metadata: e.metadata ?? null,
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
