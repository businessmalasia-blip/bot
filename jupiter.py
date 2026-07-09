import logging

import aiohttp

log = logging.getLogger(__name__)

# Price API V3; the old quote-api.jup.ag/v6/price was shut down on 2025-10-01.
# lite-api is the keyless free tier.
PRICE_URL = "https://lite-api.jup.ag/price/v3"

SOL_MINT = "So11111111111111111111111111111111111111112"


async def get_token_price(session: aiohttp.ClientSession, mint: str) -> dict | None:
    """Returns the V3 price entry for the mint ({"usdPrice", "liquidity", ...})
    or None if Jupiter doesn't know the token / request failed."""
    try:
        async with session.get(
            PRICE_URL, params={"ids": mint}, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            data = await resp.json(content_type=None)
        entry = data.get(mint) if isinstance(data, dict) else None
        return entry if isinstance(entry, dict) else None
    except Exception as e:
        log.debug("Jupiter price request failed for %s: %s", mint, e)
        return None
