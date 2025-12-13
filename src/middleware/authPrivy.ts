// src/middleware/authPrivy.ts
import type { Request, Response, NextFunction } from "express";
import { PrivyClient } from "@privy-io/server-auth";

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
    const eoaAddress = user.wallet?.address
      ? user.wallet.address.toLowerCase()
      : null;

    const primaryAddress = smartAddress ?? eoaAddress;
    if (!primaryAddress) {
      return res
        .status(400)
        .json({ error: "No wallet or smart wallet address on Privy user" });
    }

    req.user = {
      id: user.id,
      primaryAddress,
      smartAddress,
      eoaAddress,
    };

    next();
  } catch (err) {
    console.error("[authPrivy] error verifying token", err);
    return res.status(401).json({ error: "Invalid or expired auth token" });
  }
}
