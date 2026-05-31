// src/middleware/authPrivy.ts
import type { Request, Response, NextFunction } from "express";
import { PrivyClient } from "@privy-io/server-auth";
import { pool } from "../db";

const APP_ID = (process.env.PRIVY_APP_ID || "").trim();
const APP_SECRET = (process.env.PRIVY_APP_SECRET || "").trim();

if (!APP_ID || !APP_SECRET) {
  // Fail fast on boot in production; this avoids mysterious 401s later.
  // If you prefer a softer failure in dev, you can downgrade this to console.warn.
  throw new Error("Missing PRIVY_APP_ID or PRIVY_APP_SECRET in environment");
}

const privyClient = new PrivyClient(APP_ID, APP_SECRET);

export interface AuthedUser {
  id: string; // Privy DID (did:privy:...)

  // Primary on-chain identity for BlockPools → prefer the smart wallet
  primaryAddress: string;

  smartAddress?: string | null;
  eoaAddress?: string | null;
}

export interface AuthedRequest extends Request {
  user?: AuthedUser;
}

/**
 * Core auth routine.
 *
 * @param requireWallet When true (default), a Privy user with no smart/EOA
 *   wallet is rejected with 400. When false, the request is allowed through
 *   with `req.user.primaryAddress = ""` — used by the profile onboarding routes
 *   so a brand-new user can set up their profile (which is keyed by the Privy
 *   DID, not a wallet) while the smart wallet is still being provisioned in the
 *   background. The address is backfilled automatically on the next request
 *   once it appears.
 */
async function runAuth(
  req: AuthedRequest,
  res: Response,
  next: NextFunction,
  requireWallet: boolean
) {
  try {
    const authHeader =
      (req.headers.authorization as string | undefined) ||
      (req.headers.Authorization as string | undefined);

    if (!authHeader) {
      return res.status(401).json({ error: "Missing Authorization header" });
    }

    const [scheme, token] = authHeader.split(" ");
    if (scheme !== "Bearer" || !token) {
      return res.status(401).json({ error: "Invalid Authorization header" });
    }

    // ✅ Verify token with Privy
    const { userId } = await privyClient.verifyAuthToken(token);
    const privyUser = await privyClient.getUser(userId);

    // Prefer smart-wallet
    const smartAddress = privyUser.smartWallet?.address
      ? privyUser.smartWallet.address.toLowerCase()
      : null;

    const eoaAddress = privyUser.wallet?.address
      ? privyUser.wallet.address.toLowerCase()
      : null;

    const primaryAddress = smartAddress ?? eoaAddress;
    if (!primaryAddress && requireWallet) {
      return res
        .status(400)
        .json({ error: "No wallet or smart wallet address on Privy user" });
    }

    // Attach to request first (even if DB write fails). When the wallet isn't
    // ready yet (wallet-optional path), primaryAddress is "" — consumers that
    // need a real address should treat the empty string as "not yet".
    req.user = {
      id: privyUser.id, // Privy DID (did:privy:...)
      primaryAddress: primaryAddress ?? "",
      smartAddress,
      eoaAddress,
    };

    // ✅ Upsert/refresh addresses on every authenticated request.
    // Important: DO update primary_address (smart wallet can appear later).
    try {
      const now = new Date().toISOString();

      await pool.query(
        `
        INSERT INTO users (id, primary_address, eoa_address, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $4)
        ON CONFLICT (id) DO UPDATE SET
          -- COALESCE so the wallet-optional path (primary_address NULL while the
          -- smart wallet provisions) never wipes an address we already have.
          primary_address = COALESCE(EXCLUDED.primary_address, users.primary_address),
          eoa_address     = COALESCE(EXCLUDED.eoa_address, users.eoa_address),
          updated_at      = EXCLUDED.updated_at
        `,
        [
          privyUser.id,
          primaryAddress || null,
          eoaAddress || null,
          now,
        ]
      );
    } catch (dbErr) {
      // Do not block auth if DB write fails; log for visibility.
      console.error("[authPrivy] failed to upsert user addresses", dbErr);
    }

    return next();
  } catch (err) {
    console.error("[authPrivy] error verifying token", err);
    return res.status(401).json({ error: "Invalid or expired auth token" });
  }
}

/**
 * Standard auth — requires a provisioned wallet. Use everywhere except the
 * profile onboarding routes.
 */
export function authPrivy(
  req: AuthedRequest,
  res: Response,
  next: NextFunction
) {
  return runAuth(req, res, next, true);
}

/**
 * Wallet-optional auth — for profile onboarding (GET /me, POST /). Lets a
 * just-signed-in user load and save their profile before the smart wallet has
 * finished provisioning; the address backfills on the next authenticated
 * request once it exists.
 */
export function authPrivyOptionalWallet(
  req: AuthedRequest,
  res: Response,
  next: NextFunction
) {
  return runAuth(req, res, next, false);
}
