// src/routes/promo.ts
// POST /api/promo/redeem
//
// Flow:
//  1. Authenticate user via Privy JWT
//  2. Validate promo code exists, is active, not expired, under max_uses
//  3. Check user hasn't already redeemed this code (DB unique constraint)
//  4. Send USDC from hot wallet → user's smart wallet via viem
//  5. Record redemption + set promo_locked on user atomically
//  6. Return tx hash + amount

import { Router, Request, Response } from 'express';
import { createClient } from '@supabase/supabase-js';
import {
  createWalletClient,
  createPublicClient,
  http,
  parseUnits,

} from 'viem';
import { privateKeyToAccount } from 'viem/accounts';
import { arbitrum } from 'viem/chains';
import { PrivyClient } from '@privy-io/server-auth';

// ─── Constants ────────────────────────────────────────────────────────────────
const USDC_ADDRESS = '0xaf88d065e77c8cC2239327C5EDb3A432268e5831'; // USDC on Arbitrum
const USDC_DECIMALS = 6;

// Minimum trade volume (in $) the user must do before withdrawing after a promo
const PROMO_TRADE_LOCK_MULTIPLIER = 1; // user must trade credit_usdc × this

const ERC20_TRANSFER_ABI = [
  {
    name: 'transfer',
    type: 'function',
    stateMutability: 'nonpayable',
    inputs: [
      { name: 'to', type: 'address' },
      { name: 'amount', type: 'uint256' },
    ],
    outputs: [{ name: '', type: 'bool' }],
  },
] as const;

// ─── Clients (initialised lazily so env vars are read at runtime) ─────────────
function getSupabase() {
  return createClient(
    process.env.SUPABASE_URL!,
    process.env.SUPABASE_SERVICE_ROLE_KEY!
  );
}

function getPrivy() {
  return new PrivyClient(
    process.env.PRIVY_APP_ID!,
    process.env.PRIVY_APP_SECRET!
  );
}

function getHotWalletClient() {
  const raw = process.env.PROMO_HOT_WALLET_PRIVATE_KEY!;
  if (!raw) throw new Error('PROMO_HOT_WALLET_PRIVATE_KEY not set');
  const key = raw.startsWith('0x') ? (raw as `0x${string}`) : (`0x${raw}` as `0x${string}`);
  const account = privateKeyToAccount(key);

  const walletClient = createWalletClient({
    account,
    chain: arbitrum,
    transport: http(process.env.ARBITRUM_RPC_URL || 'https://arb1.arbitrum.io/rpc'),
  });

  const publicClient = createPublicClient({
    chain: arbitrum,
    transport: http(process.env.ARBITRUM_RPC_URL || 'https://arb1.arbitrum.io/rpc'),
  });

  return { walletClient, publicClient, account };
}

// ─── Route ────────────────────────────────────────────────────────────────────
const router = Router();

router.post('/redeem', async (req: Request, res: Response) => {
  // 1. Auth
  const authHeader = req.headers.authorization ?? '';
  const token = authHeader.replace(/^Bearer\s+/i, '').trim();
  if (!token) return res.status(401).json({ error: 'Missing auth token' });

  let userAddress: string;
  try {
    const privy = getPrivy();
    const claims = await privy.verifyAuthToken(token);
    // We want the smart wallet address — stored as primary_address in users table
    // claims.userId is the Privy DID; we need to look up the address
    const supabase = getSupabase();
    const { data: userRow, error: userErr } = await supabase
      .from('users')
      .select('primary_address')
      .eq('id', claims.userId)
      .single();

    if (userErr || !userRow?.primary_address) {
      // Fallback: try to get address from Privy token subject
      // The subject in Privy JWTs is the DID, not the address — we'll use what we have
      return res.status(401).json({ error: 'User not found' });
    }
    userAddress = userRow.primary_address.toLowerCase();
  } catch (err) {
    console.error('[promo/redeem] auth error', err);
    return res.status(401).json({ error: 'Invalid auth token' });
  }

  // 2. Parse body
  const { code } = req.body as { code?: string };
  if (!code || typeof code !== 'string') {
    return res.status(400).json({ error: 'Missing promo code' });
  }
  const normalizedCode = code.trim().toUpperCase();

  const supabase = getSupabase();

  // 3. Look up the promo code
  const { data: promo, error: promoErr } = await supabase
    .from('promo_codes')
    .select('*')
    .eq('code', normalizedCode)
    .single();

  if (promoErr || !promo) {
    return res.status(404).json({ error: 'Promo code not found.' });
  }

  if (!promo.active) {
    return res.status(400).json({ error: 'This promo code is no longer active.' });
  }

  if (promo.expires_at && new Date(promo.expires_at) < new Date()) {
    return res.status(400).json({ error: 'This promo code has expired.' });
  }

  if (promo.max_uses !== null && promo.total_claimed >= promo.max_uses) {
    return res.status(400).json({ error: 'This promo code has reached its usage limit.' });
  }

  // 4. Check if user already redeemed this code
  const { data: existing } = await supabase
    .from('promo_redemptions')
    .select('id')
    .eq('promo_code_id', promo.id)
    .eq('user_address', userAddress)
    .maybeSingle();

  if (existing) {
    return res.status(409).json({ error: 'You have already redeemed this promo code.' });
  }

  // 5. Send USDC from hot wallet
  const amountUsdc = Number(promo.credit_usdc);
  let txHash: string;

  try {
    const { walletClient, publicClient } = getHotWalletClient();
    const amount = parseUnits(amountUsdc.toString(), USDC_DECIMALS);

    const hash = await walletClient.writeContract({
      address: USDC_ADDRESS as `0x${string}`,
      abi: ERC20_TRANSFER_ABI,
      functionName: 'transfer',
      args: [userAddress as `0x${string}`, amount],
    });

    // Wait for 1 confirmation
    await publicClient.waitForTransactionReceipt({ hash, confirmations: 1 });
    txHash = hash;
  } catch (err: any) {
    console.error('[promo/redeem] USDC send failed', err);
    return res.status(502).json({
      error: 'Failed to send USDC. Please try again.',
      detail: err?.shortMessage || err?.message,
    });
  }

  // 6. Record redemption + update user promo lock — do both in a transaction-like sequence
  //    If the DB write fails after the on-chain send, the tx hash is still returned so
  //    an admin can manually reconcile.
  const tradeRequired = amountUsdc * PROMO_TRADE_LOCK_MULTIPLIER;

  const { error: insertErr } = await supabase
    .from('promo_redemptions')
    .insert({
      promo_code_id: promo.id,
      user_address: userAddress,
      tx_hash: txHash,
      amount_usdc: amountUsdc,
    });

  if (insertErr) {
    // Unique constraint violation means race condition — code already redeemed
    if (insertErr.code === '23505') {
      return res.status(409).json({ error: 'You have already redeemed this promo code.' });
    }
    console.error('[promo/redeem] insert error', insertErr);
    // Don't block — still return success since USDC was sent
  }

  // Increment total_claimed on the promo code
  await supabase
    .from('promo_codes')
    .update({ total_claimed: promo.total_claimed + 1 })
    .eq('id', promo.id);

  // Set promo lock on user — they must trade `tradeRequired` USD before withdrawing
  await supabase
    .from('users')
    .update({
      promo_locked: true,
      promo_trade_required: tradeRequired,
    })
    .eq('primary_address', userAddress)
    .eq('promo_locked', false); // Only set once — don't reset if already locked

  return res.status(200).json({
    success: true,
    code: normalizedCode,
    amount_usdc: amountUsdc,
    tx_hash: txHash,
    trade_required: tradeRequired,
    message: `$${amountUsdc.toFixed(2)} USDC sent to your wallet!`,
  });
});

// ── GET /api/promo/lock-status?address=0x... ──────────────────────────────────
// Public-ish endpoint (no auth required — address is not sensitive here).
// Returns the promo lock fields needed by useWithdrawEligibility on the frontend.
router.get('/lock-status', async (req: Request, res: Response) => {
  const address = (req.query.address as string ?? '').toLowerCase().trim();
  if (!address || !/^0x[a-f0-9]{40}$/.test(address)) {
    return res.status(400).json({ error: 'Invalid address' });
  }

  const supabase = getSupabase();
  const { data, error } = await supabase
    .from('users')
    .select('promo_locked, promo_trade_required, promo_trade_accumulated')
    .eq('primary_address', address)
    .maybeSingle();

  if (error) {
    console.error('[promo/lock-status]', error);
    return res.status(500).json({ error: 'DB error' });
  }

  const promoTradeRequired = Number(data?.promo_trade_required ?? 0);
  let promoTradeAccumulated = Number(data?.promo_trade_accumulated ?? 0);
  let promoLocked = data?.promo_locked ?? false;
  let promoRedeemedAt: string | null = null;

  if (promoTradeRequired > 0) {
    const { data: redemptionRows, error: redemptionErr } = await supabase
      .from('promo_redemptions')
      .select('redeemed_at, inserted_at')
      .eq('user_address', address)
      .order('redeemed_at', { ascending: false })
      .limit(1);

    if (redemptionErr) {
      console.error('[promo/lock-status redemption]', redemptionErr);
    } else {
      promoRedeemedAt = redemptionRows?.[0]?.redeemed_at ?? redemptionRows?.[0]?.inserted_at ?? null;
    }
  }

  // Compute live net promo exposure so BUY then SELL reduces progress immediately:
  // net_open = SUM(BUY gross_in_dec) - SUM(SELL cost_basis_closed_dec), floored at 0.
  if (promoTradeRequired > 0) {
    let netQuery = supabase
      .from('user_trade_events')
      .select('type, gross_in_dec, cost_basis_closed_dec')
      .eq('user_address', address)
      .in('type', ['BUY', 'SELL'])
      .limit(3000);
    if (promoRedeemedAt) {
      netQuery = netQuery.gte('inserted_at', promoRedeemedAt);
    }
    const { data: netRows, error: netErr } = await netQuery;

    if (netErr) {
      console.error('[promo/lock-status net-open]', netErr);
    } else {
      let netOpen = 0;
      for (const r of netRows ?? []) {
        if (r?.type === 'BUY') netOpen += Number(r?.gross_in_dec ?? 0);
        if (r?.type === 'SELL') netOpen -= Number(r?.cost_basis_closed_dec ?? 0);
      }
      const computedAccumulated = Math.max(0, Math.min(promoTradeRequired, netOpen));
      promoTradeAccumulated = computedAccumulated;
      promoLocked = computedAccumulated < promoTradeRequired;
    }
  }

  // Additional anti-abuse gate:
  // even after a user reaches trade volume, keep withdrawals blocked while they
  // still have promo-related BUY activity on unresolved games.
  let promoGameFinalized = true;
  if (promoTradeRequired > 0) {
    let buyTradesQuery = supabase
      .from('user_trade_events')
      .select('game_id')
      .eq('user_address', address)
      .eq('type', 'BUY')
      .limit(300);
    if (promoRedeemedAt) {
      buyTradesQuery = buyTradesQuery.gte('inserted_at', promoRedeemedAt);
    }
    const { data: buyTrades, error: tradesError } = await buyTradesQuery;

    if (tradesError) {
      console.error('[promo/lock-status trades]', tradesError);
    } else if (buyTrades?.length) {
      const gameIds = Array.from(
        new Set(
          buyTrades
            .map((t: any) => String(t?.game_id ?? '').toLowerCase().trim())
            .filter(Boolean)
        )
      );

      if (gameIds.length) {
        const { data: games, error: gamesError } = await supabase
          .from('games')
          .select('game_id, is_final')
          .in('game_id', gameIds);

        if (gamesError) {
          console.error('[promo/lock-status games]', gamesError);
        } else {
          const isFinalByGameId = new Map(
            (games ?? []).map((g: any) => [
              String(g?.game_id ?? '').toLowerCase().trim(),
              Boolean(g?.is_final),
            ])
          );
          promoGameFinalized = gameIds.every((id) => isFinalByGameId.get(id) === true);
        }
      }
    }
  }

  const effectivePromoLocked = promoLocked || (promoTradeRequired > 0 && !promoGameFinalized);

  return res.json({
    promo_locked: effectivePromoLocked,
    promo_trade_required: promoTradeRequired,
    promo_trade_accumulated: promoTradeAccumulated,
    promo_game_finalized: promoGameFinalized,
  });
});

export default router;