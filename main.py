import asyncio
import logging

import aiohttp
import redis.asyncio as redis

from config import REDIS_URL
from sol_price import sol_price_loop
from ws_listener import ws_listener

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)


async def main():
    log.info("Starting Pump.fun Token Screener")

    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    log.info("Redis connected")

    async with aiohttp.ClientSession() as session:
        price_task = asyncio.create_task(sol_price_loop(session, r))
        await asyncio.sleep(3)

        ws_task = asyncio.create_task(ws_listener(session, r))

        log.info("Bot is running")
        await asyncio.gather(price_task, ws_task)


if __name__ == "__main__":
    asyncio.run(main())
