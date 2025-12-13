import { Router, Request, Response } from "express";
import { pool } from "../db";

const router = Router();

function requireAdminKey(req: Request) {
  const expected = process.env.ADMIN_API_KEY;
  if (!expected) {
    throw new Error("ADMIN_API_KEY is not set in environment");
  }
  const got = req.header("x-admin-key");
  return got && got === expected;
}

/**
 * POST /api/admin/sweeps
 * Body: sweep report payload from the hardhat sweeper script
 */
router.post("/sweeps", async (req: Request, res: Response) => {
  try {
    if (!requireAdminKey(req)) {
      return res.status(401).json({ error: "Unauthorized" });
    }

    const b = req.body ?? {};

    // Minimal required fields (tighten/expand as desired)
    const chainId = Number(b.chainId);
    const contractAddress = String(b.contractAddress || "").toLowerCase();
    const txHash = String(b.txHash || "").toLowerCase();

    if (!Number.isFinite(chainId) || !contractAddress || !txHash) {
      return res.status(400).json({
        error: "Missing/invalid required fields: chainId, contractAddress, txHash",
      });
    }

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

        $44
      )
      on conflict (chain_id, contract_address, tx_hash)
      do nothing
      returning id
    `;

    const values = [
      chainId,
      contractAddress,
      txHash,

      b.lockedAt ?? null,
      b.winningTeam ?? null,

      b.contractUSDCBefore ?? null,
      b.contractUSDCAfter ?? null,
      b.treasuryUSDCBefore ?? null,
      b.treasuryUSDCAfter ?? null,

      b.amountSwept ?? null,
      b.poolBalanceBefore ?? null,
      b.poolBalanceAfter ?? null,
      b.liabilityBefore ?? null,
      b.liabilityAfter ?? null,
      b.expectedExcessBefore ?? null,

      b.stakeTeamA_before ?? null,
      b.stakeTeamB_before ?? null,
      b.totalSharesTeamA_before ?? null,
      b.totalSharesTeamB_before ?? null,
      b.stakeTeamA_after ?? null,
      b.stakeTeamB_after ?? null,
      b.totalSharesTeamA_after ?? null,
      b.totalSharesTeamB_after ?? null,

      b.totalVolumeTeamA_gross ?? null,
      b.totalVolumeTeamB_gross ?? null,
      b.totalFees_1pct ?? null,

      b.lpFundedTotal ?? null,
      b.lpFundedCount ?? null,
      b.lpBalanceBefore ?? null,
      b.lpBalanceAfter ?? null,

      b.withdrawCount ?? null,
      b.withdrawOriginalStakeTotal ?? null,
      b.withdrawNetPayoutTotal ?? null,
      b.withdrawFeesTotal ?? null,

      b.virtualLiquidity ?? null,
      b.maxBetPerTx ?? null,

      b.league ?? null,
      b.teamACode ?? null,
      b.teamBCode ?? null,
      b.gameId ?? null,

      b.gasUsed ?? null,
      b.effectiveGasPrice ?? null,
      b.gasCostNative ?? null,

      b.sweptAt ? new Date(b.sweptAt) : new Date(),
    ];

    const result = await pool.query(q, values);

    // If conflict "do nothing", no row returned. Still treat as OK/idempotent.
    const insertedId = result.rows?.[0]?.id ?? null;

    return res.status(200).json({ ok: true, id: insertedId });
  } catch (err: any) {
    console.error("POST /api/admin/sweeps error:", err?.message ?? err);
    return res.status(500).json({ error: "Internal server error" });
  }
});

export default router;
