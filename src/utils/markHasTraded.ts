// src/utils/markHasTraded.ts

import { pool } from "../db";

/**
 * Sets has_traded = TRUE and followup_email_sent = TRUE for the user
 * matching the given wallet address (smart wallet or EOA).
 *
 * Safe to call multiple times — no-ops if already set.
 * Setting followup_email_sent = TRUE ensures they never get the
 * re-engagement nudge email after they've already traded.
 */
export async function markUserHasTraded(userAddress: string): Promise<void> {
  if (!userAddress) return;

  const addr = userAddress.toLowerCase();

  await pool.query(
    `UPDATE users
     SET
       has_traded          = TRUE,
       followup_email_sent = TRUE
     WHERE
       primary_address = $1
       OR eoa_address  = $1`,
    [addr]
  );
}