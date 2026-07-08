// src/utils/markHasTraded.ts

import { pool } from "../db";
import { resolveAttribution } from "../services/attribution";
import { sendFirstTradeEvent } from "../services/metaCapi";

/**
 * Sets has_traded = TRUE and followup_email_sent = TRUE for the user
 * matching the given wallet address (smart wallet or EOA).
 *
 * Safe to call multiple times — no-ops if already set.
 * Setting followup_email_sent = TRUE ensures they never get the
 * re-engagement nudge email after they've already traded.
 *
 * FirstTrade side-effect: the UPDATE is guarded with
 * `has_traded IS DISTINCT FROM TRUE`, so it only affects a row on the
 * false->true transition. That is exactly the funded/value moment, so we use it
 * to (1) resolve first-touch ad attribution and (2) fire the server-side Meta
 * FirstTrade Conversions API event — once per user. Both side-effects are
 * best-effort and never throw; a failure here never blocks the trade.
 */
export async function markUserHasTraded(userAddress: string): Promise<void> {
  if (!userAddress) return;

  const addr = userAddress.toLowerCase();

  const { rows } = await pool.query(
    `UPDATE users
     SET
       has_traded          = TRUE,
       followup_email_sent = TRUE
     WHERE
       (primary_address = $1 OR eoa_address = $1)
       AND has_traded IS DISTINCT FROM TRUE
     RETURNING primary_address, email`,
    [addr]
  );

  // No row returned => already traded (or unknown wallet). Nothing more to do.
  if (rows.length === 0) return;

  const primaryAddress: string = (rows[0].primary_address || addr).toLowerCase();
  const email: string | null = rows[0].email || null;

  // First-trade side effects — isolated so neither can break the trade path.
  try {
    // Resolve first-touch attribution now so the ad id is present on the row
    // and available for the CAPI event. Idempotent (fills nulls only).
    const attribution = await resolveAttribution(primaryAddress);

    // Best-effort funded value = the user's earliest recorded trade gross.
    let valueUsd: number | null = null;
    try {
      const v = await pool.query(
        `SELECT gross_in_dec
         FROM public.user_trade_events
         WHERE lower(user_address) = $1
         ORDER BY inserted_at ASC
         LIMIT 1`,
        [primaryAddress]
      );
      const raw = v.rows[0]?.gross_in_dec;
      if (raw != null && Number.isFinite(Number(raw))) valueUsd = Number(raw);
    } catch {
      /* value is optional for CAPI */
    }

    await sendFirstTradeEvent({
      address: primaryAddress,
      email,
      valueUsd,
      adId: attribution?.utm_content ?? null,
    });
  } catch (err) {
    console.error("[markHasTraded] FirstTrade side-effect failed", err);
  }
}
