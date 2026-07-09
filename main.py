import asyncio
import logging

import aiohttp
import redis.asyncio as redis

from config import REDIS_URL
from sol_price import sol_price_loop
from ws_listener import ws_listener
from alert import run_dispatcher, send_startup_message
import stats

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

    await stats.init_db()

    async with aiohttp.ClientSession() as session:
        price_task = asyncio.create_task(sol_price_loop(session, r))
        await asyncio.sleep(3)

        ws_task = asyncio.create_task(ws_listener(session, r))
        checkpoint_task = asyncio.create_task(stats.checkpoint_loop(session, r))
        dispatcher_task = asyncio.create_task(run_dispatcher())
        await send_startup_message()

        log.info("Bot is running")
        try:
            await asyncio.gather(price_task, ws_task, checkpoint_task, dispatcher_task)
        finally:
            await stats.close_db()


if __name__ == "__main__":
    asyncio.run(main())
