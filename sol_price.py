import asyncio
import logging

import aiohttp
import redis.asyncio as redis

from config import SOL_PRICE_INTERVAL

log = logging.getLogger(__name__)

JUPITER_PRICE_URL = "https://quote-api.jup.ag/v6/price"
REDIS_KEY = "sol_price_usd"


async def fetch_sol_price(session: aiohttp.ClientSession) -> float | None:
    try:
        async with session.get(JUPITER_PRICE_URL, params={"ids": "SOL", "vsToken": "USDC"}) as resp:
            data = await resp.json()
        return float(data["data"]["SOL"]["price"])
    except Exception as e:
        log.warning("Failed to fetch SOL price: %s", e)
        return None


async def sol_price_loop(session: aiohttp.ClientSession, r: redis.Redis):
    while True:
        price = await fetch_sol_price(session)
        if price is not None:
            await r.set(REDIS_KEY, str(price), ex=10)
            log.debug("SOL price: $%.2f", price)
        await asyncio.sleep(SOL_PRICE_INTERVAL)


async def get_cached_sol_price(r: redis.Redis) -> float | None:
    val = await r.get(REDIS_KEY)
    if val is None:
        return None
    return float(val)
