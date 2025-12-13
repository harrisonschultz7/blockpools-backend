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


const PORT = process.env.PORT || 3001;

const app = express();

app.use(
  cors({
    origin: "*", // tighten later if you want
  })
);

app.use(express.json());

// Serve uploaded files (avatars, etc.) from /uploads
app.use("/uploads", express.static(path.join(__dirname, "..", "uploads")));

app.get("/health", (_req, res) => {
  res.json({ ok: true });
});

// Profile routes
app.use("/api/profile", profileRouter);

// Wall routes: /api/profiles/:profileId/wall, etc.
app.use("/api", wallRouter);

// Invites route: POST /api/invites
app.use("/api", invitesRouter);

// Email test route: POST /api/email/test
app.use("/api", emailTestRouter);

// Admin routes (internal)
app.use("/api/admin", adminSweepsRouter);

app.listen(PORT, () => {
  console.log(`BlockPools backend listening on port ${PORT}`);
});

