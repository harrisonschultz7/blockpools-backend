// src/services/metaCapi.ts
//
// Meta (Facebook) Conversions API — server-side conversion events.
//
// BlockPools is non-custodial, so there is no on-chain "deposit" event. The
// funded/value moment is a user's FIRST real trade. We send that server-side as
// a `FirstTrade` event so Meta can (a) optimise delivery toward funded users
// and (b) seed a lookalike of people who have actually traded.
//
// Match quality: we send hashed email (em) + hashed wallet (external_id), which
// is enough for Meta to match against the same users your existing Pixel already
// saw for Lead / CompleteRegistration (same Pixel/Dataset). When available we
// also attach fbp/fbc/client_user_agent/client_ip for stronger matching.
//
// DISABLED BY DEFAULT. Nothing is sent unless META_CAPI_ENABLED is truthy AND
// META_PIXEL_ID + META_CAPI_ACCESS_TOKEN are set. This lets the code deploy
// safely before the CAPI token exists. Every path swallows its own errors — a
// CAPI failure must never affect a trade.
//
// Env:
//   META_CAPI_ENABLED         "true"/"1" to actually send (default off)
//   META_PIXEL_ID             Pixel / Dataset id (Events Manager > Data sources)
//   META_CAPI_ACCESS_TOKEN    System-user Conversions API token
//   META_CAPI_TEST_EVENT_CODE optional — routes events to Test Events tab only
//   META_GRAPH_VERSION        optional — Graph API version (default v21.0)

import { createHash } from "crypto";
import { pool } from "../db";

const GRAPH_VERSION = (process.env.META_GRAPH_VERSION || "v21.0").trim();
const PIXEL_ID = (process.env.META_PIXEL_ID || "").trim();
const ACCESS_TOKEN = (process.env.META_CAPI_ACCESS_TOKEN || "").trim();
const TEST_EVENT_CODE = (process.env.META_CAPI_TEST_EVENT_CODE || "").trim();
const ENABLED =
  /^(1|true|yes)$/i.test((process.env.META_CAPI_ENABLED || "").trim()) &&
  !!PIXEL_ID &&
  !!ACCESS_TOKEN;

const SEND_TIMEOUT_MS = 8000;

/** True when the integration is fully configured and turned on. */
export function metaCapiEnabled(): boolean {
  return ENABLED;
}

function sha256(input: string): string {
  return createHash("sha256").update(input).digest("hex");
}

/** Meta requires em/external_id normalised (trim + lowercase) then SHA-256. */
function hashEmail(email: string): string | null {
  const norm = email.trim().toLowerCase();
  if (!norm || !norm.includes("@")) return null;
  return sha256(norm);
}

function hashExternalId(wallet: string): string {
  return sha256(wallet.trim().toLowerCase());
}

export interface FirstTradeInput {
  /** Canonical user wallet (users.primary_address). Used as external_id + event dedup key. */
  address: string;
  /** User email if known — hashed for match. Optional. */
  email?: string | null;
  /** First-trade gross USD (the funded value), if known. */
  valueUsd?: number | null;
  /** Meta ad id from persisted attribution (attributed_utm_content), if known. */
  adId?: string | null;
}

/**
 * Fire the FirstTrade Conversions API event for a user. Fire-and-forget: awaiting
 * is optional and it never throws. No-ops when the integration is disabled.
 *
 * event_id is deterministic (`firsttrade-<wallet>`) so if this ever fires twice
 * (e.g. a retry or a race) Meta deduplicates it to a single conversion.
 */
export async function sendFirstTradeEvent(input: FirstTradeInput): Promise<void> {
  if (!ENABLED) return;
  const addr = (input.address || "").toLowerCase();
  if (!addr) return;

  try {
    // Best-effort enrichment: latest analytics row for this wallet gives us the
    // browser signals (ua, fbp, fbc/fbclid) captured at their sessions.
    const signals = await latestBrowserSignals(addr);

    const userData: Record<string, unknown> = {
      external_id: [hashExternalId(addr)],
    };
    const em = input.email ? hashEmail(input.email) : null;
    if (em) (userData.em as unknown) = [em];
    if (signals.ua) userData.client_user_agent = signals.ua;
    if (signals.ip) userData.client_ip_address = signals.ip;
    if (signals.fbp) userData.fbp = signals.fbp;
    if (signals.fbc) userData.fbc = signals.fbc;

    const customData: Record<string, unknown> = { currency: "USD" };
    if (typeof input.valueUsd === "number" && Number.isFinite(input.valueUsd)) {
      customData.value = Number(input.valueUsd.toFixed(2));
    }
    if (input.adId) customData.ad_id = input.adId;

    const body: Record<string, unknown> = {
      data: [
        {
          event_name: "FirstTrade",
          event_time: Math.floor(Date.now() / 1000),
          action_source: "website",
          event_id: `firsttrade-${addr}`,
          user_data: userData,
          custom_data: customData,
        },
      ],
    };
    if (TEST_EVENT_CODE) body.test_event_code = TEST_EVENT_CODE;

    const url = `https://graph.facebook.com/${GRAPH_VERSION}/${PIXEL_ID}/events?access_token=${encodeURIComponent(
      ACCESS_TOKEN
    )}`;

    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), SEND_TIMEOUT_MS);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
        signal: ctrl.signal,
      });
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        console.error(
          `[metaCapi] FirstTrade send failed HTTP ${res.status}: ${txt.slice(0, 300)}`
        );
      } else {
        console.log(`[metaCapi] FirstTrade sent for ${addr}`);
      }
    } finally {
      clearTimeout(t);
    }
  } catch (err) {
    console.error("[metaCapi] sendFirstTradeEvent error", err);
  }
}

interface BrowserSignals {
  ua: string | null;
  ip: string | null;
  fbp: string | null;
  fbc: string | null;
}

/**
 * Pull browser match signals from the most recent analytics entry that carried
 * an acquisition context for this wallet. `fbc` is derived from a stored fbclid
 * per Meta's format (fb.1.<ms>.<fbclid>) when a raw fbc wasn't captured.
 */
async function latestBrowserSignals(addr: string): Promise<BrowserSignals> {
  const empty: BrowserSignals = { ua: null, ip: null, fbp: null, fbc: null };
  try {
    const { rows } = await pool.query(
      `
      SELECT metadata->'entry' AS entry,
             extract(epoch from created_at) * 1000 AS created_ms
      FROM public.analytics_events
      WHERE lower(wallet_address) = $1
        AND metadata ? 'entry'
      ORDER BY created_at DESC
      LIMIT 1
      `,
      [addr]
    );
    if (rows.length === 0 || !rows[0].entry) return empty;
    const entry = rows[0].entry as Record<string, any>;
    const createdMs = Math.floor(Number(rows[0].created_ms) || Date.now());

    let fbc: string | null = entry.fbc || null;
    if (!fbc && entry.fbclid) {
      fbc = `fb.1.${createdMs}.${String(entry.fbclid)}`;
    }
    return {
      ua: typeof entry.ua === "string" ? entry.ua : null,
      ip: null, // not currently captured in the entry object
      fbp: typeof entry._fbp === "string" ? entry._fbp : entry.fbp || null,
      fbc,
    };
  } catch {
    return empty;
  }
}
