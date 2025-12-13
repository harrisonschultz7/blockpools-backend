// src/routes/emailTest.ts
import { Router } from "express";
import { sendTestEmail } from "../services/emailService";

const router = Router();

/**
 * POST /api/email/test
 * Body: { to: "you@example.com" }
 *
 * Temporary dev-only endpoint to validate Resend sending.
 * Remove or lock down before production.
 */
router.post("/email/test", async (req, res) => {
  try {
    const to = String(req.body?.to || "").trim();
    if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(to)) {
      return res.status(400).json({ error: "Invalid 'to' email" });
    }

    const result = await sendTestEmail(to);
    return res.json({ ok: true, result });
  } catch (e: any) {
    return res.status(500).json({ ok: false, error: e?.message || "Failed" });
  }
});

export default router;
