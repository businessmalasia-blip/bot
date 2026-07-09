import logging

from aiogram import Bot
from aiogram.enums import ParseMode

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

log = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_BOT_TOKEN)


async def send_alert(
    mint: str,
    name: str,
    symbol: str,
    human_percent: float,
    dev_status: str,
    msr: float | None,
    market_cap: float,
):
    msr_info = f" (MSR: {msr:.0f}%)" if msr is not None else ""
    text = (
        f"\U0001f525 <b>QUALITY TOKEN</b>\n"
        f"\U0001fa99 <b>{name}</b> ({symbol})\n"
        f"\U0001f4cb CA: <code>{mint}</code>\n"
        f'\U0001f517 <a href="https://photon-sol.tinyastro.io/token/{mint}">Photon</a>\n'
        f"\U0001f465 Humans: {human_percent:.0f}%\n"
        f"\U0001f468‍\U0001f4bb Dev: {dev_status}{msr_info}\n"
        f"\U0001f4b0 MC: ${market_cap:,.0f}"
    )

    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        log.info("Alert sent for %s", mint)
    except Exception as e:
        log.error("Failed to send alert for %s: %s", mint, e)
