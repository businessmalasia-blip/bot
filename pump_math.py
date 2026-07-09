"""Pump.fun bonding curve math.

The curve is constant-product with virtual reserves: it starts at
30 virtual SOL / 1.073B virtual tokens and completes when ~85 real SOL
have been deposited. Token price is v_sol / v_tok, so with
k = 30 * 1.073e9 and total supply of 1B:

    mcap_SOL = price * supply = v_sol^2 * 1e9 / k

Note the naive `curve_balance * SOL_price` is NOT the market cap: the
curve tops out at ~85 SOL, which is below common USD thresholds whenever
SOL trades low — that formula silently caps out and never triggers.
"""

VIRTUAL_SOL_BASE = 30.0
VIRTUAL_TOKEN_BASE = 1.073e9
TOTAL_SUPPLY = 1e9
K = VIRTUAL_SOL_BASE * VIRTUAL_TOKEN_BASE


def estimate_mcap_usd(curve_lamports: int | float, sol_price_usd: float) -> float:
    """Market cap in USD from the bonding curve's real SOL balance."""
    real_sol = curve_lamports / 1e9
    v_sol = VIRTUAL_SOL_BASE + real_sol
    mcap_sol = v_sol * v_sol * TOTAL_SUPPLY / K
    return mcap_sol * sol_price_usd
