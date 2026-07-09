import asyncio
import logging

import aiohttp
import redis.asyncio as redis

from config import SOL_PRICE_INTERVAL
from jupiter import get_token_price, SOL_MINT

log = logging.getLogger(__name__)

REDIS_KEY = "sol_price_usd"


async def fetch_sol_price(session: aiohttp.ClientSession) -> float | None:
    entry = await get_token_price(session, SOL_MINT)
    if entry is None:
        log.warning("Failed to fetch SOL price from Jupiter")
        return None
    return float(entry["usdPrice"])


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
