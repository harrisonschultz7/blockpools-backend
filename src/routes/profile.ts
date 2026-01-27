// src/routes/profile.ts
import { Router } from "express";
import { buildProfilePortfolio } from "../services/profile";

const router = Router();

/**
 * Mounted in server.ts at:
 *   app.use("/api/profile", profileRouter);
 *
 * Final URL:
 *   GET /api/profile/:address/portfolio
 */
router.get("/:address/portfolio", async (req, res) => {
  try {
    const out = await buildProfilePortfolio(req);

    if (!out || (out as any).ok === false) {
      return res.status(400).json(out ?? { ok: false, error: "bad_request" });
    }

    return res.json(out);
  } catch (err: any) {
    console.error("[routes/profile] error", err);
    return res.status(500).json({
      ok: false,
      error: "internal_error",
      detail: String(err?.message || err),
    });
  }
});

export default router;
