import { Router, Request, Response } from "express";
import { pool } from "../db";

const router = Router();

function requireAdminKey(req: Request) {
  const expected = process.env.ADMIN_API_KEY;
  if (!expected) throw new Error("ADMIN_API_KEY is not set in environment");
  const got = req.header("x-admin-key");
  return got && got === expected;
}

// Prefer camelCase, but allow snake_case fallbacks.
// Returns null if missing (so DB can store null).
function pick(b: any, camel: string, snake: string) {
  const v = b?.[camel];
  if (v !== undefined && v !== null && v !== "") return v;
  const s = b?.[snake];
  if (s !== undefined && s !== null && s !== "") return s;
  return null;
}

router.post("/sweeps", async (req: Request, res: Response) => {
  try {
    if (!requireAdminKey(req)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const b = req.body ?? {};

    // Minimal required fields
    const chainId = Number(b.chainId);
    const contractAddress = String(b.contractAddress || "").toLowerCase();
    const txHash = String(b.txHash || "").toLowerCase();

    if (!Number.isFinite(chainId) || !contractAddress || !txHash) {
      return res.status(400).json({
        error: "Missing/invalid required fields: chainId, contractAddress, txHash",
      });
    }

    // ── Promo accounting snapshot ───────────────────────────────────────────
    // The promo hot wallet is just another bettor in this pool, so its activity
    // distorts amount_swept (winning free bets depress it, losing ones inflate
    // it). Aggregate promo_redemptions for this pool (keyed by pool_address =
    // contract_address) and snapshot it onto the row. The sweep only fires once
    // the game is resolved, so redemptions are typically settled by now; any
    // still in 'placed' are counted in promo_unsettled_count and the live
    // game_accounting view recomputes from promo_redemptions regardless.
    // Amounts are in whole USDC dollars (promo_redemptions' native unit).
    let promo = {
      bets_count: null as number | null,
      credit_staked: null as string | null,
      credit_won: null as string | null,
      credit_lost: null as string | null,
      payout_to_users: null as string | null,
      credit_recovered: null as string | null,
      unsettled_count: null as number | null,
    };
    try {
      const pr = await pool.query(
        `
        SELECT
          count(*)                  FILTER (WHERE status IN ('placed','settled_win','settled_loss')) AS bets_count,
          coalesce(sum(credit_usdc) FILTER (WHERE status IN ('placed','settled_win','settled_loss')), 0) AS credit_staked,
          coalesce(sum(credit_usdc) FILTER (WHERE status = 'settled_win'),  0) AS credit_won,
          coalesce(sum(credit_usdc) FILTER (WHERE status = 'settled_loss'), 0) AS credit_lost,
          coalesce(sum(payout_amount_usdc), 0)      AS payout_to_users,
          coalesce(sum(treasury_recovered_usdc), 0) AS credit_recovered,
          count(*)                  FILTER (WHERE status = 'placed') AS unsettled_count
        FROM public.promo_redemptions
        WHERE lower(pool_address) = $1
        `,
        [contractAddress]
      );
      const r = pr.rows[0];
      if (r && Number(r.bets_count) > 0) {
        promo = {
          bets_count: Number(r.bets_count),
          credit_staked: String(r.credit_staked),
          credit_won: String(r.credit_won),
          credit_lost: String(r.credit_lost),
          payout_to_users: String(r.payout_to_users),
          credit_recovered: String(r.credit_recovered),
          unsettled_count: Number(r.unsettled_count),
        };
      }
    } catch (e: any) {
      // Never fail the sweep write because the promo rollup hit a snag — the
      // live game_accounting view can still derive these. Log and continue.
      console.warn("[admin/sweeps] promo rollup failed; writing sweep without snapshot", e?.message ?? e);
    }

    // Helpful one-line debug for the new fields
    // (shows you immediately if the payload contains them)
    console.log("[admin/sweeps] incoming", {
      chainId,
      contractAddress,
      txHash: txHash.slice(0, 10) + "...",
      bettorsA: pick(b, "numBettorsTeamA", "bettors_team_a"),
      bettorsB: pick(b, "numBettorsTeamB", "bettors_team_b"),
      lockA: pick(b, "lockPriceTeamA_bps", "lock_price_team_a_bps"),
      lockB: pick(b, "lockPriceTeamB_bps", "lock_price_team_b_bps"),
      avgA: pick(b, "avgPriceTeamA_bps", "avg_price_team_a_bps"),
      avgB: pick(b, "avgPriceTeamB_bps", "avg_price_team_b_bps"),
    });

    const q = `
      insert into public.sweeps (
        chain_id, contract_address, tx_hash,
        locked_at, winning_team,

        contract_usdc_before, contract_usdc_after,
        treasury_usdc_before, treasury_usdc_after,

        amount_swept,
        pool_balance_before, pool_balance_after,
        liability_before, liability_after,
        expected_excess_before,

        stake_team_a_before, stake_team_b_before,
        total_shares_team_a_before, total_shares_team_b_before,
        stake_team_a_after, stake_team_b_after,
        total_shares_team_a_after, total_shares_team_b_after,

        total_volume_team_a_gross, total_volume_team_b_gross, total_fees_1pct,

        lp_funded_total, lp_funded_count,
        lp_balance_before, lp_balance_after,

        withdraw_count, withdraw_original_stake_total,
        withdraw_net_payout_total, withdraw_fees_total,

        virtual_liquidity, max_bet_per_tx,

        league, team_a_code, team_b_code, game_id,

        gas_used, effective_gas_price, gas_cost_native,

        bettors_team_a, bettors_team_b,
        lock_price_team_a_bps, lock_price_team_b_bps,
        avg_price_team_a_bps, avg_price_team_b_bps,

        promo_bets_count, promo_credit_staked_usdc,
        promo_credit_won_usdc, promo_credit_lost_usdc,
        promo_payout_to_users_usdc, promo_credit_recovered_usdc,
        promo_unsettled_count, promo_snapshot_at,

        gas_cost_usd, eth_usd_price,

        swept_at
      )
      values (
        $1, $2, $3,
        $4, $5,

        $6, $7,
        $8, $9,

        $10,
        $11, $12,
        $13, $14,
        $15,

        $16, $17,
        $18, $19,
        $20, $21,
        $22, $23,

        $24, $25, $26,

        $27, $28,
        $29, $30,

        $31, $32,
        $33, $34,

        $35, $36,

        $37, $38, $39, $40,

        $41, $42, $43,

        $44, $45,
        $46, $47,
        $48, $49,

        $50, $51,
        $52, $53,
        $54, $55,
        $56, $57,

        $58, $59,

        $60
      )
      on conflict (chain_id, contract_address, tx_hash)
      do update set
        locked_at = excluded.locked_at,
        winning_team = excluded.winning_team,

        contract_usdc_before = excluded.contract_usdc_before,
        contract_usdc_after = excluded.contract_usdc_after,
        treasury_usdc_before = excluded.treasury_usdc_before,
        treasury_usdc_after = excluded.treasury_usdc_after,

        amount_swept = excluded.amount_swept,
        pool_balance_before = excluded.pool_balance_before,
        pool_balance_after = excluded.pool_balance_after,
        liability_before = excluded.liability_before,
        liability_after = excluded.liability_after,
        expected_excess_before = excluded.expected_excess_before,

        stake_team_a_before = excluded.stake_team_a_before,
        stake_team_b_before = excluded.stake_team_b_before,
        total_shares_team_a_before = excluded.total_shares_team_a_before,
        total_shares_team_b_before = excluded.total_shares_team_b_before,
        stake_team_a_after = excluded.stake_team_a_after,
        stake_team_b_after = excluded.stake_team_b_after,
        total_shares_team_a_after = excluded.total_shares_team_a_after,
        total_shares_team_b_after = excluded.total_shares_team_b_after,

        total_volume_team_a_gross = excluded.total_volume_team_a_gross,
        total_volume_team_b_gross = excluded.total_volume_team_b_gross,
        total_fees_1pct = excluded.total_fees_1pct,

        lp_funded_total = excluded.lp_funded_total,
        lp_funded_count = excluded.lp_funded_count,
        lp_balance_before = excluded.lp_balance_before,
        lp_balance_after = excluded.lp_balance_after,

        withdraw_count = excluded.withdraw_count,
        withdraw_original_stake_total = excluded.withdraw_original_stake_total,
        withdraw_net_payout_total = excluded.withdraw_net_payout_total,
        withdraw_fees_total = excluded.withdraw_fees_total,

        virtual_liquidity = excluded.virtual_liquidity,
        max_bet_per_tx = excluded.max_bet_per_tx,

        league = excluded.league,
        team_a_code = excluded.team_a_code,
        team_b_code = excluded.team_b_code,
        game_id = excluded.game_id,

        gas_used = excluded.gas_used,
        effective_gas_price = excluded.effective_gas_price,
        gas_cost_native = excluded.gas_cost_native,

        bettors_team_a = excluded.bettors_team_a,
        bettors_team_b = excluded.bettors_team_b,
        lock_price_team_a_bps = excluded.lock_price_team_a_bps,
        lock_price_team_b_bps = excluded.lock_price_team_b_bps,
        avg_price_team_a_bps = excluded.avg_price_team_a_bps,
        avg_price_team_b_bps = excluded.avg_price_team_b_bps,

        promo_bets_count            = excluded.promo_bets_count,
        promo_credit_staked_usdc    = excluded.promo_credit_staked_usdc,
        promo_credit_won_usdc       = excluded.promo_credit_won_usdc,
        promo_credit_lost_usdc      = excluded.promo_credit_lost_usdc,
        promo_payout_to_users_usdc  = excluded.promo_payout_to_users_usdc,
        promo_credit_recovered_usdc = excluded.promo_credit_recovered_usdc,
        promo_unsettled_count       = excluded.promo_unsettled_count,
        promo_snapshot_at           = excluded.promo_snapshot_at,

        gas_cost_usd  = excluded.gas_cost_usd,
        eth_usd_price = excluded.eth_usd_price,

        swept_at = excluded.swept_at
      returning id
    `;

    const values = [
      chainId,
      contractAddress,
      txHash,

      pick(b, "lockedAt", "locked_at"),
      pick(b, "winningTeam", "winning_team"),

      pick(b, "contractUSDCBefore", "contract_usdc_before"),
      pick(b, "contractUSDCAfter", "contract_usdc_after"),
      pick(b, "treasuryUSDCBefore", "treasury_usdc_before"),
      pick(b, "treasuryUSDCAfter", "treasury_usdc_after"),

      pick(b, "amountSwept", "amount_swept"),
      pick(b, "poolBalanceBefore", "pool_balance_before"),
      pick(b, "poolBalanceAfter", "pool_balance_after"),
      pick(b, "liabilityBefore", "liability_before"),
      pick(b, "liabilityAfter", "liability_after"),
      pick(b, "expectedExcessBefore", "expected_excess_before"),

      pick(b, "stakeTeamA_before", "stake_team_a_before"),
      pick(b, "stakeTeamB_before", "stake_team_b_before"),
      pick(b, "totalSharesTeamA_before", "total_shares_team_a_before"),
      pick(b, "totalSharesTeamB_before", "total_shares_team_b_before"),
      pick(b, "stakeTeamA_after", "stake_team_a_after"),
      pick(b, "stakeTeamB_after", "stake_team_b_after"),
      pick(b, "totalSharesTeamA_after", "total_shares_team_a_after"),
      pick(b, "totalSharesTeamB_after", "total_shares_team_b_after"),

      pick(b, "totalVolumeTeamA_gross", "total_volume_team_a_gross"),
      pick(b, "totalVolumeTeamB_gross", "total_volume_team_b_gross"),
      pick(b, "totalFees_1pct", "total_fees_1pct"),

      pick(b, "lpFundedTotal", "lp_funded_total"),
      pick(b, "lpFundedCount", "lp_funded_count"),
      pick(b, "lpBalanceBefore", "lp_balance_before"),
      pick(b, "lpBalanceAfter", "lp_balance_after"),

      pick(b, "withdrawCount", "withdraw_count"),
      pick(b, "withdrawOriginalStakeTotal", "withdraw_original_stake_total"),
      pick(b, "withdrawNetPayoutTotal", "withdraw_net_payout_total"),
      pick(b, "withdrawFeesTotal", "withdraw_fees_total"),

      pick(b, "virtualLiquidity", "virtual_liquidity"),
      pick(b, "maxBetPerTx", "max_bet_per_tx"),

      pick(b, "league", "league"),
      pick(b, "teamACode", "team_a_code"),
      pick(b, "teamBCode", "team_b_code"),
      pick(b, "gameId", "game_id"),

      pick(b, "gasUsed", "gas_used"),
      pick(b, "effectiveGasPrice", "effective_gas_price"),
      pick(b, "gasCostNative", "gas_cost_native"),

      // New metrics
      pick(b, "numBettorsTeamA", "bettors_team_a"),
      pick(b, "numBettorsTeamB", "bettors_team_b"),
      pick(b, "lockPriceTeamA_bps", "lock_price_team_a_bps"),
      pick(b, "lockPriceTeamB_bps", "lock_price_team_b_bps"),
      pick(b, "avgPriceTeamA_bps", "avg_price_team_a_bps"),
      pick(b, "avgPriceTeamB_bps", "avg_price_team_b_bps"),

      // Promo accounting snapshot (dollars). All null when the pool had no
      // free bets. promo_snapshot_at is stamped only when we captured promo data.
      promo.bets_count,
      promo.credit_staked,
      promo.credit_won,
      promo.credit_lost,
      promo.payout_to_users,
      promo.credit_recovered,
      promo.unsettled_count,
      promo.bets_count !== null ? new Date() : null,

      // Gas valued in USD at sweep time (ETH/USD from Chainlink, posted by the
      // sweeper). Null when the price read failed — view falls back to native.
      pick(b, "gasCostUsd", "gas_cost_usd"),
      pick(b, "ethUsdPrice", "eth_usd_price"),

      b.sweptAt ? new Date(b.sweptAt) : new Date(),
    ];

    const result = await pool.query(q, values);
    const insertedId = result.rows?.[0]?.id ?? null;

    return res.status(200).json({ ok: true, id: insertedId });
  } catch (err: any) {
    console.error("POST /api/admin/sweeps error:", err?.message ?? err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
