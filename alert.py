import logging
import time

from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message

from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
import runtime_status
import stats

log = logging.getLogger(__name__)

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher()

SOCIAL_ICONS = {"twitter": "🐦", "telegram": "✈️", "website": "🌍", "discord": "💬"}


@dp.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.chat.id != TELEGRAM_CHAT_ID:
        return
    text = await stats.summary()
    await message.answer(text, parse_mode=ParseMode.HTML)


@dp.message(Command("status"))
async def cmd_status(message: Message):
    if message.chat.id != TELEGRAM_CHAT_ID:
        return

    t = runtime_status.totals
    last_event = runtime_status.last_ws_event_at
    if last_event is None:
        feed = "⏳ ещё не было событий"
    else:
        ago = int(time.time() - last_event)
        feed = f"🟢 живой ({ago} сек назад)" if ago < 120 else f"🔴 тишина {ago // 60} мин!"

    text = (
        f"🩺 <b>Статус бота</b>\n"
        f"⏱ Аптайм: {runtime_status.uptime_str()}\n"
        f"📡 Поток рынка: {feed}\n"
        f"📥 Событий получено: {t['ws_events']:,}\n"
        f"🛒 Покупок замечено: {t['buys_seen']:,}\n"
        f"🔍 Транзакций проверено: {t['txs_fetched']:,}\n"
        f"📈 Расчётов капы: {t['mcap_checks']:,}\n"
        f"🔔 Алертов отправлено: {t['alerts_sent']:,}"
    )
    await message.answer(text, parse_mode=ParseMode.HTML)


async def send_startup_message():
    try:
        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="✅ Бот запущен и следит за рынком.\n/status — проверить пульс, /stats — статистика алертов.",
        )
    except Exception as e:
        log.warning("Failed to send startup message: %s", e)


async def run_dispatcher():
    await dp.start_polling(bot, handle_signals=False)


def _format_socials(socials: dict) -> str:
    found = [f'{SOCIAL_ICONS[k]} <a href="{v}">{k}</a>' for k, v in socials.items() if k in SOCIAL_ICONS]
    return " ".join(found) if found else "нет"


async def send_alert(
    mint: str,
    name: str,
    symbol: str,
    human_percent: float,
    dev_status: str,
    msr: float | None,
    market_cap: float,
    velocity: int,
    bundle_txs: int,
    socials: dict,
):
    msr_info = f" (MSR: {msr:.0f}%)" if msr is not None else ""
    text = (
        f"\U0001f525 <b>QUALITY TOKEN</b>\n"
        f"\U0001fa99 <b>{name}</b> ({symbol})\n"
        f"\U0001f4cb CA: <code>{mint}</code>\n"
        f'\U0001f517 <a href="https://photon-sol.tinyastro.io/token/{mint}">Photon</a>'
        f' | <a href="https://gmgn.ai/sol/token/{mint}">GMGN</a>\n'
        f"\U0001f465 Humans: {human_percent:.0f}%\n"
        f"\U0001f468‍\U0001f4bb Dev: {dev_status}{msr_info}\n"
        f"⚡ Velocity: {velocity} buyers/min\n"
        f"\U0001f4e6 Bundle: clean ({bundle_txs} tx in creation slot)\n"
        f"\U0001f310 Socials: {_format_socials(socials)}\n"
        f"\U0001f4b0 MC: ${market_cap:,.0f}"
    )

    image_url = socials.get("_image")
    try:
        if image_url:
            try:
                await bot.send_photo(
                    chat_id=TELEGRAM_CHAT_ID,
                    photo=image_url,
                    caption=text,
                    parse_mode=ParseMode.HTML,
                )
                log.info("Alert with photo sent for %s", mint)
                runtime_status.bump("alerts_sent")
                return
            except Exception as e:
                # broken/slow image host must not eat the alert itself
                log.warning("send_photo failed for %s (%s), falling back to text", mint, e)

        await bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        log.info("Alert sent for %s", mint)
        runtime_status.bump("alerts_sent")
    except Exception as e:
        log.error("Failed to send alert for %s: %s", mint, e)
