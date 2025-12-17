// src/routes/emailTest.ts
import { Router, Response } from "express";
import { sendTestEmail } from "../services/emailService";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

/**
 * POST /api/email/test
 * Body: { to: string }
 * Protected so random people can't spam your Resend account.
 */
router.post("/test", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    const to = (req.body?.to || "").toString().trim();
    if (!to) return res.status(400).json({ error: "Missing 'to' in body" });

    const result = await sendTestEmail({ to });
    return res.json({ ok: true, result });
  } catch (e: any) {
    return res.status(500).json({ error: e?.message || "Email test failed" });
  }
});

export default router;
