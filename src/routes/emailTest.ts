// src/routes/emailTest.ts
import { Router } from "express";
import { sendTestEmail } from "../services/emailService";

const router = Router();

router.post("/email/test", async (req, res) => {
  try {
    // Require admin key in prod
    const adminKey = process.env.ADMIN_API_KEY;
    if (process.env.NODE_ENV === "production") {
      const provided = String(req.headers["x-admin-key"] || "");
      if (!adminKey || provided !== adminKey) {
        return res.status(403).json({ error: "Forbidden" });
      }
    }

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
