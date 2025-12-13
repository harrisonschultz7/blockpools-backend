// src/services/authService.ts
import { PrivyClient } from '@privy-io/server-auth';
import pool from '../db'; // 👈 if your db helper file has a different path/name, adjust this import

const APP_ID = process.env.PRIVY_APP_ID!;
const APP_SECRET = process.env.PRIVY_APP_SECRET!;

const privyClient = new PrivyClient(APP_ID, APP_SECRET);

/**
 * Verify a Privy auth token, derive the primary wallet address,
 * upsert into `users` table if needed, and return users.id.
 */
export async function verifyPrivyTokenAndGetUserId(
  token: string
): Promise<string | null> {
  try {
    // 1) Verify token with Privy
    const { userId } = await privyClient.verifyAuthToken(token);
    const privyUser = await privyClient.getUser(userId);

    // 2) Derive addresses
    const smartAddress = privyUser.smartWallet?.address
      ? privyUser.smartWallet.address.toLowerCase()
      : null;

    const eoaAddress = privyUser.wallet?.address
      ? privyUser.wallet.address.toLowerCase()
      : null;

    const primaryAddress = smartAddress ?? eoaAddress;
    if (!primaryAddress) {
      console.warn(
        '[authService] Privy user has no wallet/smart wallet address',
        privyUser.id
      );
      return null;
    }

    // 3) Upsert into users table (you already created this table in SQL)
    const result = await pool.query<
      { id: string }
    >(
      `
      INSERT INTO users (id, primary_address, eoa_address, created_at, updated_at)
      VALUES ($1, $2, $3, NOW(), NOW())
      ON CONFLICT (id) DO UPDATE
      SET primary_address = EXCLUDED.primary_address,
          eoa_address     = COALESCE(EXCLUDED.eoa_address, users.eoa_address),
          updated_at      = NOW()
      RETURNING id
      `,
      [privyUser.id, primaryAddress, eoaAddress]
    );

    const row = result.rows[0];
    if (!row) return null;

    return row.id; // this is users.id
  } catch (err) {
    console.error('[authService.verifyPrivyTokenAndGetUserId] error', err);
    return null;
  }
}
