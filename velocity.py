import logging
import time

import redis.asyncio as redis

from config import VELOCITY_MIN_BUYERS, VELOCITY_WINDOW

log = logging.getLogger(__name__)

BUYS_KEY_TTL = 600


async def record_buy(r: redis.Redis, mint: str, buyer: str):
    """Called from the WS listener on every parsed buy of a tracked mint."""
    now = time.time()
    key = f"buys:{mint}"
    # score = timestamp; one entry per buyer keeps the set deduplicated,
    # the latest buy time wins which is what the window check needs
    await r.zadd(key, {buyer: now})
    await r.zremrangebyscore(key, 0, now - BUYS_KEY_TTL)
    await r.expire(key, BUYS_KEY_TTL)


async def unique_buyers_in_window(r: redis.Redis, mint: str, window: int = VELOCITY_WINDOW) -> int:
    now = time.time()
    return await r.zcount(f"buys:{mint}", now - window, now)


async def check_velocity(r: redis.Redis, mint: str) -> tuple[bool, int]:
    """Passes if enough distinct wallets bought within the last window."""
    buyers = await unique_buyers_in_window(r, mint)
    passed = buyers >= VELOCITY_MIN_BUYERS
    log.info("Velocity for %s: %d unique buyers / %ds, passed=%s", mint, buyers, VELOCITY_WINDOW, passed)
    return passed, buyers
