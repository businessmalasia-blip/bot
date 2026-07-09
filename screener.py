import asyncio
import logging

import aiohttp
import redis.asyncio as redis

from config import MCAP_ALERT_LOW, MCAP_WAIT_TIMEOUT, SOCIALS_REQUIRED
from helius import get_account_info
from filters import (
    check_concentration,
    calculate_human_percent,
    check_dev,
    check_bundle,
    check_socials,
    SOCIAL_KEYS,
)
from velocity import check_velocity
from sol_price import get_cached_sol_price
from alert import send_alert
import stats

log = logging.getLogger(__name__)


async def wait_for_mcap(
    session: aiohttp.ClientSession,
    r: redis.Redis,
    bonding_curve_address: str,
) -> float | None:
    """Dynamic window: fires at the FIRST crossing of MCAP_ALERT_LOW.
    No upper bound — a token that shoots past the mark between polls
    still produces an alert instead of being dropped.
    """
    deadline = asyncio.get_event_loop().time() + MCAP_WAIT_TIMEOUT
    while asyncio.get_event_loop().time() < deadline:
        try:
            info = await get_account_info(session, bonding_curve_address)
            if info and info.get("value"):
                lamports = info["value"]["lamports"]
                sol_price = await get_cached_sol_price(r)
                if sol_price is not None:
                    mcap = (lamports / 1e9) * sol_price
                    if mcap >= MCAP_ALERT_LOW:
                        return mcap
        except Exception as e:
            log.warning("Error polling mcap: %s", e)
        await asyncio.sleep(2)

    log.info("Timeout waiting for mcap on %s", bonding_curve_address)
    return None


async def analyze_token(
    session: aiohttp.ClientSession,
    r: redis.Redis,
    mint: str,
    bonding_curve_address: str,
    mcap_seen: float = 0.0,
):
    log.info("Analyzing token %s", mint)

    # every candidate goes into the dataset, including the rejected ones —
    # their 24h outcomes tell us which filters throw away future winners
    features: dict = {}

    async def reject(reason: str):
        log.info("Token %s rejected by %s", mint, reason)
        await stats.record_candidate(mint, bonding_curve_address, mcap_seen, features, reason)

    bundle_passed, bundle_txs = await check_bundle(session, mint)
    features["bundle_txs"] = bundle_txs
    if not bundle_passed:
        await reject("bundle")
        return

    passed, holder_addresses = await check_concentration(session, mint)
    if not passed:
        await reject("concentration")
        return

    human_passed, human_pct = await calculate_human_percent(session, holder_addresses, r)
    features["human_pct"] = human_pct
    if not human_passed:
        await reject("human")
        return

    dev_result = await check_dev(session, mint, r)
    features["dev_status"] = dev_result["status"]
    features["msr"] = dev_result.get("msr")
    if dev_result["status"] == "Bad":
        await reject("dev")
        return

    socials = await check_socials(session, mint)
    has_socials = any(k in socials for k in SOCIAL_KEYS)
    features["socials"] = ",".join(k for k in SOCIAL_KEYS if k in socials)
    if SOCIALS_REQUIRED and not has_socials:
        await reject("socials")
        return

    velocity_passed, buyers = await check_velocity(r, mint)
    features["velocity"] = buyers
    if not velocity_passed:
        await reject("velocity")
        return

    await stats.record_candidate(mint, bonding_curve_address, mcap_seen, features, None)

    log.info("Token %s passed all filters, waiting for mcap >= $%d", mint, MCAP_ALERT_LOW)

    mcap = await wait_for_mcap(session, r, bonding_curve_address)
    if mcap is None:
        return

    name = socials.get("_name") or mint[:8]
    symbol = socials.get("_symbol") or "???"

    await send_alert(
        mint=mint,
        name=name,
        symbol=symbol,
        human_percent=human_pct,
        dev_status=dev_result["status"],
        msr=dev_result.get("msr"),
        market_cap=mcap,
        velocity=buyers,
        bundle_txs=bundle_txs,
        socials=socials,
    )

    await stats.record_alert(
        mint=mint,
        bc_address=bonding_curve_address,
        name=name,
        symbol=symbol,
        mcap=mcap,
        human_pct=human_pct,
        dev_status=dev_result["status"],
        msr=dev_result.get("msr"),
        velocity=buyers,
        bundle_txs=bundle_txs,
        socials=[k for k in SOCIAL_KEYS if k in socials],
    )
