// src/middleware/authPrivy.ts
import type { Request, Response, NextFunction } from "express";
import { PrivyClient } from "@privy-io/server-auth";
import { pool } from "../db";

const APP_ID = process.env.PRIVY_APP_ID!;
const APP_SECRET = process.env.PRIVY_APP_SECRET!;

const privyClient = new PrivyClient(APP_ID, APP_SECRET);

export interface AuthedUser {
  id: string;

  // Primary on-chain identity for BlockPools → prefer the smart wallet
  primaryAddress: string;

  smartAddress?: string | null;
  eoaAddress?: string | null;
}

export interface AuthedRequest extends Request {
  user?: AuthedUser;
}

/**
 * Auth middleware:
 * - Verifies Privy auth token
 * - Attaches req.user with DID + addresses
 * - Ensures users row always has primary_address / eoa_address populated
 *   so Wall + other identity-dependent features are consistent (leaderboard-style).
 */
export async function authPrivy(
  req: AuthedRequest,
  res: Response,
  next: NextFunction
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
    const user = await privyClient.getUser(userId);

    // Prefer smart-wallet
    const smartAddress = user.smartWallet?.address
      ? user.smartWallet.address.toLowerCase()
      : null;

    const eoaAddress = user.wallet?.address ? user.wallet.address.toLowerCase() : null;

    const primaryAddress = smartAddress ?? eoaAddress;
    if (!primaryAddress) {
      return res
        .status(400)
        .json({ error: "No wallet or smart wallet address on Privy user" });
    }

    // Attach to request first (even if DB write fails)
    req.user = {
      id: user.id, // Privy DID (did:privy:...)
      primaryAddress,
      smartAddress,
      eoaAddress,
    };

    // ✅ CRITICAL: Upsert addresses on every authenticated request
    // This prevents "Wall author missing primary_address" when users never ran POST /api/profile.
    try {
      const now = new Date().toISOString();

      await pool.query(
        `
        INSERT INTO users (id, primary_address, eoa_address, created_at, updated_at)
        VALUES ($1, NULLIF($2, ''), $3, $4, $4)
        ON CONFLICT (id) DO UPDATE SET
          -- Only fill if currently NULL so we don't overwrite existing values
          primary_address = COALESCE(users.primary_address, EXCLUDED.primary_address),
          eoa_address     = COALESCE(users.eoa_address, EXCLUDED.eoa_address),
          updated_at      = EXCLUDED.updated_at
        `,
        [user.id, primaryAddress, eoaAddress, now]
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
