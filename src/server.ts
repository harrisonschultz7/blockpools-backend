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

// ✅ New leaderboard routes (backend-cached metrics views)
import leaderboardRouter from "./routes/leaderboard";

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

  // New health endpoint with DB ping (used for cache readiness checks)
  app.get("/healthz", async (_req, res) => {
    const dbOk = await pingDb().catch(() => false);
    res.json({ ok: true, dbOk, ts: new Date().toISOString() });
  });

  // ✅ Cache API (Subgraph -> Postgres snapshots)
  app.use("/cache", cacheRoutes);

  // Existing backend routes (unchanged)
  app.use("/api/profile", profileRouter);
  app.use("/api", wallRouter);
  app.use("/api", invitesRouter);
  app.use("/api", emailTestRouter);
  app.use("/api/admin", adminSweepsRouter);

  // ✅ Leaderboard API (backend-computed + cached metrics)
  // Endpoints:
  //   GET /api/leaderboard/users
  //   GET /api/leaderboard/users/:address/recent
  app.use("/api", leaderboardRouter);

  // Basic error handler (useful for CORS / route errors)
  app.use((err: any, _req: any, res: any, _next: any) => {
    console.error("[server] error", err);
    res
      .status(500)
      .json({
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
