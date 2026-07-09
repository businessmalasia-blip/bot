import asyncio
import logging

import aiohttp
import redis.asyncio as redis

from config import MCAP_ALERT_LOW, MCAP_ALERT_HIGH
from helius import get_account_info, get_asset
from filters import check_concentration, calculate_human_percent, check_dev
from sol_price import get_cached_sol_price
from alert import send_alert

log = logging.getLogger(__name__)


async def wait_for_mcap_range(
    session: aiohttp.ClientSession,
    r: redis.Redis,
    bonding_curve_address: str,
    timeout: float = 300,
) -> float | None:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        try:
            info = await get_account_info(session, bonding_curve_address)
            if info and info.get("value"):
                lamports = info["value"]["lamports"]
                sol_balance = lamports / 1e9
                sol_price = await get_cached_sol_price(r)
                if sol_price is None:
                    await asyncio.sleep(2)
                    continue
                mcap = sol_balance * sol_price
                if MCAP_ALERT_LOW <= mcap <= MCAP_ALERT_HIGH:
                    return mcap
                if mcap > MCAP_ALERT_HIGH:
                    log.info("MC $%.0f exceeded upper bound, skipping", mcap)
                    return None
        except Exception as e:
            log.warning("Error polling mcap: %s", e)
        await asyncio.sleep(2)

    log.info("Timeout waiting for mcap range on %s", bonding_curve_address)
    return None


async def analyze_token(
    session: aiohttp.ClientSession,
    r: redis.Redis,
    mint: str,
    bonding_curve_address: str,
):
    log.info("Analyzing token %s", mint)

    passed, holder_addresses = await check_concentration(session, mint)
    if not passed:
        log.info("Token %s failed concentration check", mint)
        return

    human_passed, human_pct = await calculate_human_percent(session, holder_addresses, r)
    if not human_passed:
        log.info("Token %s failed human check (%.1f%%)", mint, human_pct)
        return

    dev_result = await check_dev(session, mint, r)
    if dev_result["status"] == "Bad":
        log.info("Token %s has bad dev", mint)
        return

    log.info("Token %s passed all filters, waiting for mcap $%d-$%d", mint, MCAP_ALERT_LOW, MCAP_ALERT_HIGH)

    mcap = await wait_for_mcap_range(session, r, bonding_curve_address)
    if mcap is None:
        return

    name = mint[:8]
    symbol = "???"
    try:
        asset = await get_asset(session, mint)
        content = asset.get("content", {})
        metadata = content.get("metadata", {})
        name = metadata.get("name", name)
        symbol = metadata.get("symbol", symbol)
    except Exception as e:
        log.warning("Failed to get asset metadata for %s: %s", mint, e)

    await send_alert(
        mint=mint,
        name=name,
        symbol=symbol,
        human_percent=human_pct,
        dev_status=dev_result["status"],
        msr=dev_result.get("msr"),
        market_cap=mcap,
    )
