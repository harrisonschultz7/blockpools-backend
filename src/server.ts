// src/server.ts
import "dotenv/config";
import express from "express";
import cors from "cors";
import path from "path";

import profileRouter from "./routes/profile";
import wallRouter from "./routes/wall";
import invitesRouter from "./routes/invites";
import emailTestRouter from "./routes/emailTest";
import adminSweepsRouter from "./routes/adminSweeps";

const PORT = Number(process.env.PORT || 3001);

const app = express();

/**
 * IMPORTANT:
 * Behind Nginx/Cloudflare you want trust proxy enabled so:
 * - req.protocol is derived from X-Forwarded-Proto
 * - req.get("host") reflects forwarded host when configured
 * This is used by getPublicBaseUrl() logic in profile routes.
 */
app.set("trust proxy", 1);

/**
 * CORS
 * Use CORS_ORIGINS if set (comma-separated), otherwise allow all (dev convenience).
 * Example:
 *   CORS_ORIGINS=https://www.blockpools.io,https://blockpools.io,http://localhost:5173
 */
const CORS_ORIGINS = (process.env.CORS_ORIGINS || "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

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

// JSON body parsing
app.use(express.json({ limit: "1mb" }));

// Serve uploaded files (avatars, etc.) from /uploads
app.use("/uploads", express.static(path.join(__dirname, "..", "uploads")));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// Profile routes
app.use("/api/profile", profileRouter);

// Wall routes: /api/wall/feed, /api/profiles/:profileId/wall, etc.
app.use("/api", wallRouter);

// Invites route: POST /api/invites
app.use("/api", invitesRouter);

// Email test route: POST /api/email/test
app.use("/api", emailTestRouter);

// Admin routes (internal)
app.use("/api/admin", adminSweepsRouter);

// Basic error handler (nice for debugging CORS / route errors)
app.use((err: any, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  console.error("[server] error", err);
  res.status(500).json({ error: "Internal server error", detail: String(err?.message || err) });
});

app.listen(PORT, () => {
  console.log(`BlockPools backend listening on port ${PORT}`);
});
