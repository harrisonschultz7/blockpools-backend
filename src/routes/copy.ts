// src/routes/copy.ts
//
// COPY TRADING, phase 1: managing a subscription. Nothing here places an order.
//
// The sleeve model: a subscriber allocates capital TO A STRATEGY, not to a
// wallet. One Polymarket wallet underneath, one sleeve per bot on top, and the
// bot's position as a fraction of ITS portfolio is mirrored against the sleeve's
// value rather than the wallet's balance.
//
// Two things this route is strict about, because both are load-bearing later:
//
//   DELEGATION is verified against Privy on every write, never trusted from the
//   client. A subscription whose delegated_at is null must never be signed for,
//   so the one place that flag is set has to be the one place it is checked.
//
//   CONSENT is appended, never updated. A subscriber who pauses, edits and
//   resumes consented once, to a particular version, at a particular moment.

import { Router, Response } from "express";
import { PrivyClient } from "@privy-io/server-auth";
import { pool } from "../db";
import { authPrivy, AuthedRequest } from "../middleware/authPrivy";

const router = Router();

const privy = new PrivyClient(
  (process.env.PRIVY_APP_ID || "").trim(),
  (process.env.PRIVY_APP_SECRET || "").trim(),
);

/**
 * The version of the copy-trading terms a subscriber is accepting.
 *
 * Bump this when the disclosures change. Existing rows keep the version they
 * agreed to, which is the entire point of recording it -- and a bump is how you
 * find who needs to re-consent.
 */
export const TERMS_VERSION = "2026-09-29.1";

/** Sleeve size is the one number a subscriber must not be able to fat-finger. */
const MIN_BASIS_USD = 25;
const MAX_BASIS_USD = 100_000;

/**
 * Does this user have a wallet Privy will sign with on our behalf?
 *
 * Asked of Privy directly rather than cached. Delegation is revocable from the
 * client at any time, and a stale "yes" here is the difference between not
 * trading for someone and trading for someone who withdrew permission.
 */
async function delegatedWallet(did: string): Promise<string | null> {
  const user = await privy.getUserById(did);
  const wallets = (user?.linkedAccounts || []).filter(
    (a: any) => a.type === "wallet" && a.delegated === true && a.address,
  );
  return wallets.length ? String((wallets[0] as any).address) : null;
}

function num(v: unknown): number | null {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

/** Everything the Copy modal needs before it can show a subscribe step. */
router.get("/status", authPrivy, async (req: AuthedRequest, res: Response) => {
  try {
    const did = req.user!.id;
    let delegated: string | null = null;
    try {
      delegated = await delegatedWallet(did);
    } catch {
      // Privy being unreachable must not 500 the page. Reported as
      // not-delegated, which fails closed: the modal asks for delegation again
      // rather than a subscribe button appearing that cannot be honoured.
      delegated = null;
    }

    const { rows } = await pool.query(
      `select s.bot_id, s.status, s.basis_usd, s.multiplier, s.max_trade_usd,
              s.realized_pnl, s.delegated_at, s.wallet_address,
              b.name as bot_name, b.risk_tier
         from copy.subscriptions s
         join bots.bot b on b.id = s.bot_id
        where s.privy_did = $1
        order by s.created_at`,
      [did],
    );

    res.json({
      termsVersion: TERMS_VERSION,
      minBasisUsd: MIN_BASIS_USD,
      maxBasisUsd: MAX_BASIS_USD,
      delegated: Boolean(delegated),
      delegatedWallet: delegated,
      subscriptions: rows.map((r) => ({
        botId: r.bot_id,
        botName: r.bot_name,
        riskTier: r.risk_tier,
        status: r.status,
        basisUsd: Number(r.basis_usd),
        multiplier: Number(r.multiplier),
        maxTradeUsd: r.max_trade_usd === null ? null : Number(r.max_trade_usd),
        realizedPnl: Number(r.realized_pnl),
        // A subscription can exist with delegation revoked. It is inert until
        // the subscriber re-delegates, and saying so is better than showing it
        // as active and quietly never trading.
        delegated: r.delegated_at !== null,
        walletAddress: r.wallet_address,
      })),
    });
  } catch (e: any) {
    console.error("[copy] status failed:", e);
    res.status(500).json({ error: "copy_status_unavailable" });
  }
});

/**
 * Start copying a bot, or change the sleeve on one already copied.
 *
 * Upsert rather than separate create/update: a subscriber who revoked and came
 * back is the same sleeve, and its realized_pnl is part of its history.
 */
router.post("/subscribe", authPrivy, async (req: AuthedRequest, res: Response) => {
  const did = req.user!.id;
  const botId = String(req.body?.botId || "").trim();
  const basisUsd = num(req.body?.basisUsd);
  const multiplier = num(req.body?.multiplier) ?? 1;
  const maxTradeUsd = req.body?.maxTradeUsd == null ? null : num(req.body.maxTradeUsd);
  const termsVersion = String(req.body?.termsVersion || "");

  if (!botId) return res.status(400).json({ error: "bot_required" });
  if (basisUsd === null || basisUsd < MIN_BASIS_USD || basisUsd > MAX_BASIS_USD) {
    return res.status(400).json({ error: "basis_out_of_range", minBasisUsd: MIN_BASIS_USD, maxBasisUsd: MAX_BASIS_USD });
  }
  if (multiplier <= 0 || multiplier > 3) {
    return res.status(400).json({ error: "multiplier_out_of_range" });
  }
  // The client must echo the version it displayed. If it is stale the terms
  // changed while the modal was open, and the subscriber has not read what they
  // would be agreeing to.
  if (termsVersion !== TERMS_VERSION) {
    return res.status(409).json({ error: "terms_version_stale", termsVersion: TERMS_VERSION });
  }

  try {
    const bot = await pool.query(`select id from bots.bot where id = $1 and enabled`, [botId]);
    if (!bot.rows.length) return res.status(404).json({ error: "bot_not_found" });

    const wallet = await delegatedWallet(did);
    if (!wallet) return res.status(412).json({ error: "delegation_required" });

    const client = await pool.connect();
    try {
      await client.query("begin");
      await client.query(
        `insert into copy.subscriptions
           (privy_did, wallet_address, bot_id, status, basis_usd, multiplier,
            max_trade_usd, delegated_at)
         values ($1,$2,$3,'active',$4,$5,$6, now())
         on conflict (privy_did, bot_id) do update set
           wallet_address = excluded.wallet_address,
           status         = 'active',
           basis_usd      = excluded.basis_usd,
           multiplier     = excluded.multiplier,
           max_trade_usd  = excluded.max_trade_usd,
           delegated_at   = now(),
           updated_at     = now()`,
        [did, wallet, botId, basisUsd, multiplier, maxTradeUsd],
      );
      // Appended every time, including on a sleeve edit. Cheap, and it means the
      // record answers "what had they agreed to when they set this size".
      await client.query(
        `insert into copy.consents (privy_did, bot_id, terms_version, ip, user_agent)
         values ($1,$2,$3,$4,$5)`,
        [did, botId, TERMS_VERSION,
         String(req.headers["x-forwarded-for"] || req.ip || "").slice(0, 200),
         String(req.headers["user-agent"] || "").slice(0, 400)],
      );
      await client.query("commit");
    } catch (e) {
      await client.query("rollback").catch(() => {});
      throw e;
    } finally {
      client.release();
    }

    res.json({ ok: true, botId, basisUsd, multiplier, maxTradeUsd, walletAddress: wallet });
  } catch (e: any) {
    console.error("[copy] subscribe failed:", e);
    res.status(500).json({ error: "copy_subscribe_failed" });
  }
});

/**
 * Stop copying. Open positions STAY -- they become the subscriber's to manage by
 * hand, and nothing here sells anything on their behalf.
 *
 * Status, not a delete. The row carries realized_pnl and the consent trail, and
 * a subscriber who comes back should return to their own sleeve rather than a
 * fresh one that has forgotten what it made.
 */
router.post("/unsubscribe", authPrivy, async (req: AuthedRequest, res: Response) => {
  const did = req.user!.id;
  const botId = String(req.body?.botId || "").trim();
  if (!botId) return res.status(400).json({ error: "bot_required" });

  try {
    const { rowCount } = await pool.query(
      `update copy.subscriptions
          set status = 'revoked', updated_at = now()
        where privy_did = $1 and bot_id = $2 and status <> 'revoked'`,
      [did, botId],
    );
    res.json({ ok: true, changed: rowCount });
  } catch (e: any) {
    console.error("[copy] unsubscribe failed:", e);
    res.status(500).json({ error: "copy_unsubscribe_failed" });
  }
});

/** Pause without revoking: keeps the sleeve, stops new entries. */
router.post("/pause", authPrivy, async (req: AuthedRequest, res: Response) => {
  const did = req.user!.id;
  const botId = String(req.body?.botId || "").trim();
  const paused = req.body?.paused !== false;
  if (!botId) return res.status(400).json({ error: "bot_required" });

  try {
    const { rowCount } = await pool.query(
      `update copy.subscriptions
          set status = $3, updated_at = now()
        where privy_did = $1 and bot_id = $2 and status <> 'revoked'`,
      [did, botId, paused ? "paused" : "active"],
    );
    res.json({ ok: true, changed: rowCount, status: paused ? "paused" : "active" });
  } catch (e: any) {
    console.error("[copy] pause failed:", e);
    res.status(500).json({ error: "copy_pause_failed" });
  }
});

export default router;
