// src/routes/profile.ts

/**
 * IMPORTANT:
 * Do NOT default to localhost in production.
 * Set VITE_API_BASE_URL in your frontend env (Vercel/Netlify/etc):
 *   VITE_API_BASE_URL=https://api.blockpools.io
 */
import { Router, Response, NextFunction } from "express";
import { pool } from "../db";
import {
  authPrivy,
  authPrivyOptionalWallet,
  AuthedRequest,
} from "../middleware/authPrivy";
import multer from "multer";
import path from "path";
import { PrivyClient } from "@privy-io/server-auth";
import { Resend } from "resend";
import { notifyFollow } from "../services/notifications/notify";

import { buildProfilePortfolio } from "../services/profilePortfolio";
import { PROMO_FRAMEWORK_ENABLED } from "../config/promo";
import { createReferralRedemptions } from "../services/promotions/createReferralRedemptions";

const router = Router();

// ── Resend client ────────────────────────────────────────────────────────────
const resend = new Resend(process.env.RESEND_API_KEY);

// Store avatar files under /uploads/avatars (relative to compiled server)
const upload = multer({
  dest: path.join(__dirname, "..", "..", "uploads", "avatars"),
});

const ENV_PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "")
  .trim()
  .replace(/\/+$/, "");

function getPublicBaseUrl(req?: any): string {
  if (ENV_PUBLIC_BASE_URL) return ENV_PUBLIC_BASE_URL;

  const xfProto = (req?.headers?.["x-forwarded-proto"] as string | undefined)
    ?.split(",")[0]
    ?.trim();
  const xfHost = (req?.headers?.["x-forwarded-host"] as string | undefined)
    ?.split(",")[0]
    ?.trim();

  const host = xfHost || req?.headers?.host;
  if (host) {
    const proto = xfProto || req?.protocol || "http";
    return `${proto}://${host}`.replace(/\/+$/, "");
  }

  const port = process.env.PORT || 3001;
  return `http://localhost:${port}`;
}

function normalizeAvatarUrl(
  avatarUrl: string | null | undefined,
  publicBaseUrl: string
): string | null {
  if (!avatarUrl) return null;
  if (!avatarUrl.includes("localhost")) return avatarUrl;

  try {
    const u = new URL(avatarUrl);
    if (u.pathname.startsWith("/uploads/")) {
      return `${publicBaseUrl}${u.pathname}`;
    }
    return avatarUrl;
  } catch {
    return avatarUrl.replace(/^http:\/\/localhost:\d+/, publicBaseUrl);
  }
}

function normalizeProfileRow(row: any, publicBaseUrl: string) {
  if (!row) return row;
  return {
    ...row,
    avatar_url: normalizeAvatarUrl(row.avatar_url, publicBaseUrl),
  };
}

// ── Welcome-email language selection ─────────────────────────────────────────
// Two email languages are supported: Spanish (default, LatAm audience) and
// English (Hong Kong / international expansion). Language is derived from the
// browser Accept-Language header at first login, with the stored
// preferred_locale tag as a fallback, then Spanish as the ultimate default.
type WelcomeLang = "es" | "en";

// Resend template ids. Each language keeps its template id hardcoded as the
// default so sends work with no env changes; the env vars override if you want
// to swap templates without a deploy. If English ever resolves empty, the send
// degrades gracefully to the Spanish template (see sendWelcomeEmail).
const WELCOME_TEMPLATE_ES =
  (process.env.RESEND_WELCOME_TEMPLATE_ES || "").trim() ||
  "2a86d254-f493-45d1-abda-706fd33f1479";
const WELCOME_TEMPLATE_EN =
  (process.env.RESEND_WELCOME_TEMPLATE_EN || "").trim() ||
  "120a6317-8e49-4388-a8be-290ecb9abf8e";

const WELCOME_SUBJECT: Record<WelcomeLang, string> = {
  es: "Bienvenido a BlockPools",
  en: "Welcome to BlockPools",
};

// Primary subtag of the first entry in an Accept-Language header, lowercased
// (e.g. "es-419,es;q=0.9,en;q=0.8" -> "es-419"). Returns null if absent.
function normalizeLocaleTag(header?: string | null): string | null {
  if (!header) return null;
  const first = header.split(",")[0]?.split(";")[0]?.trim().toLowerCase();
  return first || null;
}

// Map a raw locale tag to one of the two supported email languages. Spanish
// tags -> es; everything else routes to English (HK/international is
// English-facing). Returns null for an empty/unknown tag so callers can fall
// back to the next signal.
function langFromLocaleTag(tag?: string | null): WelcomeLang | null {
  if (!tag) return null;
  if (tag.startsWith("es")) return "es";
  return "en";
}

// Resolve the welcome-email language for a user from their request, and persist
// the first-seen locale tag onto the user row (first-touch wins — never clobber
// an existing value). Best-effort: any DB error here is non-fatal.
async function resolveWelcomeLang(
  req: AuthedRequest,
  userId: string
): Promise<WelcomeLang> {
  const headerTag = normalizeLocaleTag(
    req.headers["accept-language"] as string | undefined
  );

  if (headerTag) {
    await pool
      .query(
        `UPDATE users SET preferred_locale = COALESCE(preferred_locale, $1) WHERE id = $2`,
        [headerTag, userId]
      )
      .catch((e) =>
        console.error(
          "[Welcome Email] preferred_locale persist failed (non-fatal)",
          e?.message || e
        )
      );
  }

  // Prefer the live header; fall back to any stored tag; default Spanish.
  const fromHeader = langFromLocaleTag(headerTag);
  if (fromHeader) return fromHeader;

  try {
    const { rows } = await pool.query(
      `SELECT preferred_locale FROM users WHERE id = $1 LIMIT 1`,
      [userId]
    );
    const stored = langFromLocaleTag(rows[0]?.preferred_locale);
    if (stored) return stored;
  } catch {
    /* non-fatal — fall through to default */
  }
  return "es";
}

// ── Shared welcome email helper ──────────────────────────────────────────────
// Sends the welcome email via Resend (SDK v6.8.0 template object syntax) in the
// caller-provided language (defaults to Spanish to preserve prior behavior).
// NOTE: Does NOT set welcome_email_sent — callers must claim the flag
// atomically BEFORE calling this function to prevent race conditions.
// If Resend fails, callers should roll back the flag.
async function sendWelcomeEmail(
  userId: string,
  email: string,
  context: string,
  lang: WelcomeLang = "es"
): Promise<boolean> {
  try {
    // Choose template by language. If English is requested but its template id
    // isn't configured yet, degrade gracefully to the Spanish template rather
    // than failing the send (a missing env var must not break onboarding).
    let templateId = lang === "en" ? WELCOME_TEMPLATE_EN : WELCOME_TEMPLATE_ES;
    let effectiveLang: WelcomeLang = lang;
    if (lang === "en" && !templateId) {
      console.warn(
        `[Welcome Email][${context}] RESEND_WELCOME_TEMPLATE_EN not set — falling back to Spanish template for ${email}`
      );
      templateId = WELCOME_TEMPLATE_ES;
      effectiveLang = "es";
    }

    console.log(`[Welcome Email][${context}] Sending to: ${email} (userId: ${userId}, lang: ${effectiveLang})`);
    console.log(`[Welcome Email][${context}] RESEND_API_KEY present: ${!!process.env.RESEND_API_KEY}`);

    const emailResult = await resend.emails.send({
      from: process.env.RESEND_FROM_EMAIL || "BlockPools <welcome@mail.blockpools.io>",
      to: email,
      subject: WELCOME_SUBJECT[effectiveLang],
      template: {
        id: templateId,
      },
    } as any);

    console.log(`[Welcome Email][${context}] Resend result:`, JSON.stringify(emailResult));

    if ((emailResult as any)?.error || !(emailResult as any)?.data?.id) {
      console.error(
        `[Welcome Email][${context}] Resend returned no id — rolling back flag for userId: ${userId}`
      );
      // Roll back the flag so the next login retries
      await pool.query(
        `UPDATE users SET welcome_email_sent = false WHERE id = $1`,
        [userId]
      );
      return false;
    }

    console.log(`[Welcome Email][${context}] Sent OK for userId: ${userId}`);
    return true;
  } catch (err: any) {
    console.error(`[Welcome Email][${context}] Failed:`, err?.message || err);
    // Roll back the flag so the next login retries
    await pool.query(
      `UPDATE users SET welcome_email_sent = false WHERE id = $1`,
      [userId]
    ).catch(() => {});
    return false;
  }
}

// ── Privy optional auth (for public routes that benefit from knowing the viewer) ──
const PRIVY_APP_ID = (process.env.PRIVY_APP_ID || "").trim();
const PRIVY_APP_SECRET = (process.env.PRIVY_APP_SECRET || "").trim();

const privyOptionalClient =
  PRIVY_APP_ID && PRIVY_APP_SECRET
    ? new PrivyClient(PRIVY_APP_ID, PRIVY_APP_SECRET)
    : null;

async function authPrivyOptional(
  req: AuthedRequest,
  _res: Response,
  next: NextFunction
) {
  try {
    if (!privyOptionalClient) return next();

    const authHeader =
      (req.headers.authorization as string | undefined) ||
      (req.headers.Authorization as string | undefined);

    if (!authHeader) return next();

    const [scheme, token] = authHeader.split(" ");
    if (scheme !== "Bearer" || !token) return next();

    const { userId } = await privyOptionalClient.verifyAuthToken(token);
    const user = await privyOptionalClient.getUser(userId);

    const smartAddress = user.smartWallet?.address
      ? user.smartWallet.address.toLowerCase()
      : null;
    const eoaAddress = user.wallet?.address
      ? user.wallet.address.toLowerCase()
      : null;
    const primaryAddress = smartAddress ?? eoaAddress;

    if (!primaryAddress) return next();

    req.user = { id: user.id, primaryAddress, smartAddress, eoaAddress };
    return next();
  } catch {
    return next();
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// ROUTES
// ─────────────────────────────────────────────────────────────────────────────

/**
 * GET /api/profile/:address/portfolio
 */
router.get(
  "/:address(0x[a-fA-F0-9]{40})/portfolio",
  async (req: AuthedRequest, res: Response) => {
    try {
      const out = await buildProfilePortfolio(req);
      return res.json(out);
    } catch (err: any) {
      console.error("[GET /api/profile/:address/portfolio] error", err);
      return res.status(500).json({ ok: false, error: "internal_error" });
    }
  }
);

/**
 * GET /api/profile/me
 */
router.get("/me", authPrivyOptionalWallet, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.email,
        u.bio,
        u.favorite_team,
        u.welcome_email_sent,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount"
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      [userId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "Profile not found" });
    }

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[GET /api/profile/me] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/username-available?username=foo
 *
 * Lightweight pre-check so the wizard can tell a user a handle is taken when
 * they leave the identity slide — instead of failing at the end with a 409.
 * Public (no auth): it only reveals existence of a username, nothing sensitive.
 * Matches the column exactly so it mirrors the unique constraint that would
 * actually reject the insert.
 */
router.get("/username-available", async (req: AuthedRequest, res: Response) => {
  try {
    const raw = String(req.query.username ?? "").trim();
    if (
      raw.length < 3 ||
      raw.length > 20 ||
      !/^[a-zA-Z0-9_]+$/.test(raw)
    ) {
      return res.status(400).json({ available: false, error: "Invalid username" });
    }
    const { rows } = await pool.query(
      `SELECT 1 FROM users WHERE username = $1 LIMIT 1`,
      [raw]
    );
    return res.json({ available: rows.length === 0 });
  } catch (err) {
    console.error("[GET /api/profile/username-available] error", err);
    // Don't block the user on a check failure — the 409 at save still guards.
    return res.json({ available: true });
  }
});

/**
 * POST /api/profile/sync-email
 *
 * Called fire-and-forget on every login from the frontend (TopAccountBar +
 * TopAccountBarMobile). Persists the Privy email for the first time if the
 * column is currently null, then sends a welcome email if one hasn't been
 * sent yet. Idempotent — safe to call on every login.
 *
 * This is the primary path that catches:
 *  - Users who signed up before email collection was in place
 *  - Users who skipped the profile modal
 *  - Any user whose email was saved by a previous code path but whose
 *    welcome_email_sent flag was never set
 */
router.post("/sync-email", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;
    const { email } = req.body || {};

    // Wallet-only users (no Privy email) — nothing to do
    if (!email || typeof email !== "string") {
      return res.json({ ok: true, saved: false });
    }

    // ── Path A: email not yet stored — write it and atomically claim the
    // welcome_email_sent flag in one UPDATE so concurrent requests can't
    // both win the race.
    const writeResult = await pool.query(
      `UPDATE users
         SET email = $1, updated_at = NOW(), welcome_email_sent = true
       WHERE id = $2
         AND (email IS NULL OR email = '')
         AND welcome_email_sent = false
       RETURNING email`,
      [email, userId]
    );

    const wrote = writeResult.rows.length > 0;
    console.log(`[sync-email] userId: ${userId}, wrote new email: ${wrote}`);

    // Resolve language from the browser Accept-Language header (and persist the
    // locale tag) once — reused for both the first-save and catchup sends.
    const lang = await resolveWelcomeLang(req, userId);

    if (wrote) {
      await sendWelcomeEmail(userId, email, "sync-email/first-save", lang);
    } else {
      // ── Path B: email already stored — atomically claim the flag to send
      // a catchup welcome email (covers users whose email existed before the
      // welcome_email_sent column was added).
      const catchupResult = await pool.query(
        `UPDATE users
           SET welcome_email_sent = true
         WHERE id = $1
           AND email IS NOT NULL
           AND email != ''
           AND welcome_email_sent = false
         RETURNING email`,
        [userId]
      );
      const catchupRow = catchupResult.rows[0];
      if (catchupRow?.email) {
        console.log(
          `[sync-email] Catchup — atomically claimed flag for userId: ${userId}`
        );
        await sendWelcomeEmail(userId, catchupRow.email, "sync-email/catchup", lang);
      }
    }

    return res.json({ ok: true, saved: wrote });
  } catch (err) {
    console.error("[POST /api/profile/sync-email] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile
 *
 * Creates or updates the current user's profile (username, socials, etc.).
 * On first-time profile creation (username was previously null/empty):
 *   - Saves email to DB
 *   - Fires welcome email if not already sent
 */
router.post("/", authPrivyOptionalWallet, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const userId = req.user.id;
    console.log(`[POST /api/profile] HIT — userId: ${userId}`);

    // May be "" when the smart wallet is still provisioning — store NULL and let
    // it backfill on a later request rather than blocking profile creation.
    const primaryAddress = (req.user.primaryAddress || "").toLowerCase() || null;
    const eoaAddress = req.user.eoaAddress
      ? req.user.eoaAddress.toLowerCase()
      : null;

    const {
      username,
      display_name,
      x_handle,
      instagram_handle,
      avatar_url,
      email,
      bio,
      favorite_team,
    } = req.body || {};

    console.log(`[POST /api/profile] body — username: ${username}, email: ${email}`);

    if (!username || typeof username !== "string") {
      return res.status(400).json({ error: "username is required" });
    }

    // ── Normalize the two new personalization fields ────────────────────────
    // Treat `undefined` (field not in body) as "don't touch" by passing the
    // existing column value back into the UPSERT via COALESCE. Treat empty
    // string / null as an explicit clear so the user can wipe their bio or
    // un-pick their favorite team without a separate endpoint.
    const bioInput: string | null | undefined =
      typeof bio === "string"
        ? // 280-char cap mirrors the frontend. Normalize empty → NULL so the
          // "no bio yet" prompt on the profile page can use a single check.
          (bio.trim().slice(0, 280) || null)
        : bio === null
        ? null
        : undefined;

    // favorite_team comes through as "<sport>:<CODE>" or null. We don't try
    // to validate the team here (sport lists evolve over time) — the frontend
    // picker is the source of truth, and unknown values just render no pill.
    const favoriteTeamInput: string | null | undefined =
      typeof favorite_team === "string"
        ? favorite_team.trim().slice(0, 32) || null
        : favorite_team === null
        ? null
        : undefined;

    const now = new Date().toISOString();

    // Determine whether this is a first-time profile creation.
    // We check for a missing/empty username rather than row absence because
    // Privy may pre-create the user row before the profile modal is submitted.
    const existingUser = await pool.query(
      `SELECT id, email, username, welcome_email_sent FROM users WHERE id = $1 LIMIT 1`,
      [userId]
    );

    const isNewUser =
      existingUser.rows.length === 0 ||
      !existingUser.rows[0].username ||
      existingUser.rows[0].username.trim() === "";

    // Prefer the email from the request body; fall back to whatever is already
    // stored so we never accidentally null it out.
    const emailToSave = email ?? existingUser.rows[0]?.email ?? null;

    console.log(
      `[POST /api/profile] isNewUser: ${isNewUser}, emailToSave: ${emailToSave ?? "null"}`
    );

    // For bio / favorite_team we use a sentinel-aware param: `null` means
    // "the caller passed null/empty → clear", and `undefined` means "field
    // absent from the request body → keep whatever's in the DB". We can't
    // express "leave column alone" via EXCLUDED.* directly, so the UPDATE
    // branch falls back to users.bio / users.favorite_team when the new
    // value is NULL *and* the caller's intent was "don't touch".
    //
    // We encode the caller's intent in two extra params:
    //   $11 = bio value (string or null)
    //   $12 = bio touched? (true if caller sent the field at all)
    //   $13 = favorite_team value (string or null)
    //   $14 = favorite_team touched? (true if caller sent the field at all)
    const bioTouched = bioInput !== undefined;
    const favoriteTeamTouched = favoriteTeamInput !== undefined;

    const result = await pool.query(
      `
      INSERT INTO users (
        id,
        primary_address,
        eoa_address,
        username,
        display_name,
        x_handle,
        instagram_handle,
        avatar_url,
        email,
        bio,
        favorite_team,
        created_at,
        updated_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $11, $13, $10, $10)
      ON CONFLICT (id) DO UPDATE SET
        -- Don't wipe an address we already have if this save happened before the
        -- smart wallet finished provisioning (primary_address arrives as NULL).
        primary_address  = COALESCE(EXCLUDED.primary_address, users.primary_address),
        eoa_address      = COALESCE(EXCLUDED.eoa_address, users.eoa_address),
        username         = EXCLUDED.username,
        display_name     = EXCLUDED.display_name,
        x_handle         = EXCLUDED.x_handle,
        instagram_handle = EXCLUDED.instagram_handle,
        avatar_url       = EXCLUDED.avatar_url,
        email            = COALESCE(EXCLUDED.email, users.email),
        bio              = CASE WHEN $12 THEN $11 ELSE users.bio END,
        favorite_team    = CASE WHEN $14 THEN $13 ELSE users.favorite_team END,
        updated_at       = EXCLUDED.updated_at
      RETURNING
        id,
        primary_address,
        eoa_address,
        username,
        display_name,
        x_handle,
        instagram_handle,
        avatar_url,
        email,
        bio,
        favorite_team,
        welcome_email_sent,
        created_at,
        updated_at
      `,
      [
        userId,                                  // $1
        primaryAddress,                          // $2
        eoaAddress,                              // $3
        username,                                // $4
        display_name ?? null,                    // $5
        x_handle ?? null,                        // $6
        instagram_handle ?? null,                // $7
        avatar_url ?? null,                      // $8
        emailToSave,                             // $9
        now,                                     // $10
        bioInput ?? null,                        // $11
        bioTouched,                              // $12
        favoriteTeamInput ?? null,               // $13
        favoriteTeamTouched,                     // $14
      ]
    );

    const savedProfile = result.rows[0];
    console.log(
      `[POST /api/profile] savedProfile.email: ${savedProfile.email}, welcome_email_sent: ${savedProfile.welcome_email_sent}`
    );

    // Referral promo: a brand-new account's smart wallet is only known to the
    // backend once this upsert records primary_address — which frequently
    // happens in a SECOND /api/profile call, after the invite was already
    // redeemed. createReferralRedemptions needs BOTH wallets, so it would have
    // been skipped at redeem time (referee wallet still null). Re-trigger it
    // here the moment the wallet lands, for any redeemed invite where this user
    // is the referee — so the pair forms on first signup, with no second link
    // click and no waiting for the hourly backfill cron. Idempotent + guarded;
    // fire-and-forget so it can never affect the profile response.
    if (PROMO_FRAMEWORK_ENABLED && savedProfile.primary_address) {
      pool
        .query(
          `SELECT id FROM public.invites
            WHERE COALESCE(redeemed_by_user_id, accepted_by_user_id) = $1`,
          [userId]
        )
        .then((inv) => {
          for (const r of inv.rows) {
            createReferralRedemptions(r.id).catch(() => {});
          }
        })
        .catch((e) =>
          console.error(
            "[POST /api/profile] referral pair trigger failed (non-blocking)",
            e
          )
        );
    }

    // Send welcome email on first-time profile creation — atomically claim
    // the flag first so concurrent requests can't both trigger a send.
    //
    // IMPORTANT: this is a best-effort SIDE EFFECT. The profile has already been
    // committed above, so a failure here (email provider down, bad API key,
    // network blip) must NEVER fail the request — otherwise a brand-new user
    // sees "Internal server error" even though their profile saved fine. We
    // therefore swallow any error and still return the saved profile.
    try {
      if (isNewUser && savedProfile.email && !savedProfile.welcome_email_sent) {
        const claim = await pool.query(
          `UPDATE users SET welcome_email_sent = true
           WHERE id = $1 AND welcome_email_sent = false
           RETURNING id`,
          [userId]
        );
        if (claim.rows.length > 0) {
          const lang = await resolveWelcomeLang(req, userId);
          await sendWelcomeEmail(userId, savedProfile.email, "profile-upsert", lang);
        } else {
          console.log(`[Welcome Email] Flag already claimed — skipping for userId: ${userId}`);
        }
      } else {
        console.log(
          `[Welcome Email] Skipped — isNewUser: ${isNewUser}, email: ${savedProfile.email ?? "null"}, already_sent: ${savedProfile.welcome_email_sent}`
        );
      }
    } catch (emailErr) {
      // Non-fatal: log and continue. If the send failed we also roll the flag
      // back so a later attempt can retry the welcome email.
      console.error("[POST /api/profile] welcome email failed (non-fatal)", emailErr);
      try {
        await pool.query(
          `UPDATE users SET welcome_email_sent = false WHERE id = $1`,
          [userId]
        );
      } catch {
        /* ignore rollback failure */
      }
    }

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(savedProfile, publicBaseUrl));
  } catch (err: any) {
    console.error("[POST /api/profile] error", err);
    // A taken username is a unique-constraint violation (pg code 23505 on
    // users_username_key). The upsert only resolves conflicts on `id`, so a
    // *different* user picking an existing handle lands here. Return a clear
    // 409 the wizard can show instead of a scary "Internal server error".
    if (
      err?.code === "23505" &&
      String(err?.constraint || "").toLowerCase().includes("username")
    ) {
      return res
        .status(409)
        .json({ error: "That username is already taken. Please choose another." });
    }
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile/avatar
 */
router.post(
  "/avatar",
  authPrivy,
  upload.single("avatar"),
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });
      if (!req.file) return res.status(400).json({ error: "avatar file is required" });

      const userId = req.user.id;
      const now = new Date().toISOString();

      const publicBaseUrl = getPublicBaseUrl(req);
      const avatarUrl = `${publicBaseUrl}/uploads/avatars/${req.file.filename}`;

      const result = await pool.query(
        `
        UPDATE users
        SET avatar_url = $1,
            updated_at = $2
        WHERE id = $3
        RETURNING
          id,
          primary_address,
          eoa_address,
          username,
          display_name,
          x_handle,
          instagram_handle,
          avatar_url,
          bio,
          favorite_team,
          created_at,
          updated_at
        `,
        [avatarUrl, now, userId]
      );

      if (result.rows.length === 0) {
        return res.status(404).json({ error: "Profile not found" });
      }

      return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
    } catch (err) {
      console.error("[POST /api/profile/avatar] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * POST /api/profile/by-addresses
 */
router.post("/by-addresses", async (req: AuthedRequest, res: Response) => {
  try {
    const addresses = Array.isArray(req.body?.addresses) ? req.body.addresses : [];
    const addrLower = Array.from(
      new Set(addresses.filter(Boolean).map((a: any) => String(a).toLowerCase()))
    );

    if (addrLower.length === 0) return res.json([]);

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.bio,
        u.favorite_team,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount"
      FROM users u
      WHERE u.primary_address = ANY($1::text[])
         OR u.eoa_address     = ANY($1::text[])
      `,
      [addrLower]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    const data = (result.rows || []).map((r: any) =>
      normalizeProfileRow(r, publicBaseUrl)
    );
    return res.json(data);
  } catch (err) {
    console.error("[POST /api/profile/by-addresses] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/by-id?profileId=...
 */
router.get("/by-id", authPrivyOptional, async (req: AuthedRequest, res: Response) => {
  try {
    const profileId = req.query.profileId as string;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const viewerId = req.user?.id ?? null;
    const params: any[] = [profileId];
    if (viewerId) params.push(viewerId);

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.bio,
        u.favorite_team,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount"
        ${
          viewerId
            ? `,
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $2 AND following_id = u.id
        ) AS "is_followed_by_me"`
            : ""
        }
      FROM users u
      WHERE u.id = $1
      LIMIT 1
      `,
      params
    );

    if (result.rows.length === 0)
      return res.status(404).json({ error: "Profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(result.rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[GET /api/profile/by-id] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * POST /api/profile/:profileId/follow
 */
router.post("/:profileId/follow", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const viewerId = req.user.id;
    const { profileId } = req.params;

    if (!profileId) return res.status(400).json({ error: "profileId is required" });
    if (viewerId === profileId)
      return res.status(400).json({ error: "Cannot follow your own profile" });

    const targetRes = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (targetRes.rows.length === 0)
      return res.status(404).json({ error: "Target profile not found" });

    const followIns = await pool.query(
      `
      INSERT INTO user_follows (follower_id, following_id)
      VALUES ($1, $2)
      ON CONFLICT (follower_id, following_id) DO NOTHING
      `,
      [viewerId, profileId]
    );

    // Notify the followed user — only when a NEW follow row was created, so
    // re-POSTing an existing follow doesn't re-fire. Fire-and-forget.
    if ((followIns.rowCount ?? 0) > 0) {
      notifyFollow({ followerId: viewerId, followingId: profileId }).catch(() => {});
    }

    const { rows } = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.bio,
        u.favorite_team,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount",
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $1 AND following_id = u.id
        ) AS "is_followed_by_me"
      FROM users u
      WHERE u.id = $2
      LIMIT 1
      `,
      [viewerId, profileId]
    );

    if (!rows.length) return res.status(404).json({ error: "Target profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[POST /api/profile/:profileId/follow] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * DELETE /api/profile/:profileId/follow
 */
router.delete("/:profileId/follow", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    if (!req.user) return res.status(401).json({ error: "Not authenticated" });

    const viewerId = req.user.id;
    const { profileId } = req.params;

    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    await pool.query(
      `DELETE FROM user_follows WHERE follower_id = $1 AND following_id = $2`,
      [viewerId, profileId]
    );

    const { rows } = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.bio,
        u.favorite_team,
        u.created_at,
        u.updated_at,
        (SELECT COUNT(*) FROM user_follows WHERE following_id = u.id) AS "followersCount",
        (SELECT COUNT(*) FROM user_follows WHERE follower_id  = u.id) AS "followingCount",
        EXISTS (
          SELECT 1 FROM user_follows
          WHERE follower_id = $1 AND following_id = u.id
        ) AS "is_followed_by_me"
      FROM users u
      WHERE u.id = $2
      LIMIT 1
      `,
      [viewerId, profileId]
    );

    if (!rows.length) return res.status(404).json({ error: "Target profile not found" });

    const publicBaseUrl = getPublicBaseUrl(req);
    return res.json(normalizeProfileRow(rows[0], publicBaseUrl));
  } catch (err) {
    console.error("[DELETE /api/profile/:profileId/follow] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/:profileId/follow-status
 */
router.get(
  "/:profileId/follow-status",
  authPrivy,
  async (req: AuthedRequest, res: Response) => {
    try {
      if (!req.user) return res.status(401).json({ error: "Not authenticated" });

      const viewerId = req.user.id;
      const { profileId } = req.params;

      if (!profileId) return res.status(400).json({ error: "profileId is required" });

      const check = await pool.query(
        `
        SELECT 1
        FROM user_follows
        WHERE follower_id = $1 AND following_id = $2
        LIMIT 1
        `,
        [viewerId, profileId]
      );

      return res.json({ is_followed_by_me: check.rows.length > 0 });
    } catch (err) {
      console.error("[GET /api/profile/:profileId/follow-status] error", err);
      return res.status(500).json({ error: "Internal server error" });
    }
  }
);

/**
 * GET /api/profile/:profileId/followers
 */
router.get("/:profileId/followers", async (req: AuthedRequest, res: Response) => {
  try {
    const { profileId } = req.params;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const target = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (target.rows.length === 0)
      return res.status(404).json({ error: "Profile not found" });

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.bio,
        u.favorite_team,
        u.created_at,
        u.updated_at
      FROM user_follows f
      JOIN users u ON u.id = f.follower_id
      WHERE f.following_id = $1
      ORDER BY u.created_at DESC
      `,
      [profileId]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    const data = (result.rows || []).map((r: any) =>
      normalizeProfileRow(r, publicBaseUrl)
    );
    return res.json({ data });
  } catch (err) {
    console.error("[GET /api/profile/:profileId/followers] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

/**
 * GET /api/profile/:profileId/following
 */
router.get("/:profileId/following", async (req: AuthedRequest, res: Response) => {
  try {
    const { profileId } = req.params;
    if (!profileId) return res.status(400).json({ error: "profileId is required" });

    const target = await pool.query(`SELECT id FROM users WHERE id = $1`, [profileId]);
    if (target.rows.length === 0)
      return res.status(404).json({ error: "Profile not found" });

    const result = await pool.query(
      `
      SELECT
        u.id,
        u.primary_address,
        u.eoa_address,
        u.username,
        u.display_name,
        u.x_handle,
        u.instagram_handle,
        u.avatar_url,
        u.bio,
        u.favorite_team,
        u.created_at,
        u.updated_at
      FROM user_follows f
      JOIN users u ON u.id = f.following_id
      WHERE f.follower_id = $1
      ORDER BY u.created_at DESC
      `,
      [profileId]
    );

    const publicBaseUrl = getPublicBaseUrl(req);
    const data = (result.rows || []).map((r: any) =>
      normalizeProfileRow(r, publicBaseUrl)
    );
    return res.json({ data });
  } catch (err) {
    console.error("[GET /api/profile/:profileId/following] error", err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;