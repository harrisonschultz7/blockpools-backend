// src/server.ts
import "dotenv/config";

import express from "express";
import cors from "cors";
import path from "path";

import { cacheRoutes } from "./routes/cacheRoutes";
import { pingDb } from "./db/pg";

// Existing routes (keep these exactly as your backend uses today)
import profileRouter from "./routes/profile";
import wallRouter from "./routes/wall";
import invitesRouter from "./routes/invites";
import emailTestRouter from "./routes/emailTest";
import adminSweepsRouter from "./routes/adminSweeps";

// ✅ Leaderboard routes (backend-computed + cached metrics views)
import leaderboardRouter from "./routes/leaderboard";

// ✅ Groups leaderboard routes (backend-computed group metrics)
import groupsMetricsRouter from "./routes/groupsMetrics";

// ✅ Trade agg route (NEW)
import tradeAggRoutes from "./routes/tradeAggRoutes";

const PORT = Number(process.env.PORT || 3001);

// Behind Nginx/Cloudflare, this ensures req.protocol/host are derived from forwarded headers.
const TRUST_PROXY = Number(process.env.TRUST_PROXY || 1);

// CORS allowlist (comma-separated). If empty, allow all (dev convenience).
const CORS_ORIGINS = (process.env.CORS_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

export function makeServer() {
  const app = express();

  app.set("trust proxy", TRUST_PROXY);

  app.use(
    cors({
      origin: (origin, cb) => {
        // Allow non-browser requests (curl, server-to-server)
        if (!origin) return cb(null, true);

        // If env not set, default allow all (dev)
        if (!CORS_ORIGINS.length) return cb(null, true);

        // Otherwise require origin in allowlist
        if (CORS_ORIGINS.includes(origin)) return cb(null, true);

        return cb(new Error(`CORS blocked for origin: ${origin}`));
      },
      credentials: true,
    })
  );

  app.use(express.json({ limit: "1mb" }));

  // Serve uploaded files (avatars, etc.) from /uploads
  app.use("/uploads", express.static(path.join(__dirname, "..", "uploads")));

  // Existing health endpoint (kept)
  app.get("/health", (_req, res) => {
    res.json({ ok: true });
  });

  // Health endpoint with DB ping (used for cache readiness checks)
  app.get("/healthz", async (_req, res) => {
    const dbOk = await pingDb().catch(() => false);
    res.json({ ok: true, dbOk, ts: new Date().toISOString() });
  });

  // ✅ Cache API (Subgraph -> Postgres snapshots)
  //   GET /cache/meta
  //   GET /cache/leaderboard?...
  app.use("/cache", cacheRoutes);

  // Existing backend routes (unchanged)
  app.use("/api/profile", profileRouter);

  // ✅ NEW: Trade agg (query-based)
  //   GET /api/profile/trade-agg?user=0x...&page=1&pageSize=10&league=ALL&range=ALL
  app.use("/api/profile/trade-agg", tradeAggRoutes);

  app.use("/api", wallRouter);
  app.use("/api", invitesRouter);
  app.use("/api", emailTestRouter);
  app.use("/api/admin", adminSweepsRouter);

  // ✅ User leaderboard API (backend-computed + cached metrics)
  // Endpoints:
  //   GET /api/leaderboard/users
  //   GET /api/leaderboard/users/:address/recent
  app.use("/api", leaderboardRouter);

  // ✅ Groups leaderboard API (backend-computed group metrics)
  // Endpoints:
  //   GET /api/groups/leaderboard?range=D30&league=ALL
  app.use("/api", groupsMetricsRouter);

  // 404 (optional but helpful for debugging)
  app.use((_req, res) => {
    res.status(404).json({ error: "Not found" });
  });

  // Basic error handler (useful for CORS / route errors)
  app.use((err: any, _req: any, res: any, _next: any) => {
    console.error("[server] error", err);
    res.status(500).json({
      error: "Internal server error",
      detail: String(err?.message || err),
    });
  });

  return app;
}

if (require.main === module) {
  const app = makeServer();
  app.listen(PORT, () => {
    console.log(`BlockPools backend listening on port ${PORT}`);
  });
}
