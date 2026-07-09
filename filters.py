import asyncio
import logging
import time

import aiohttp
import redis.asyncio as redis

from config import (
    CONCENTRATION_MAX_SINGLE,
    CONCENTRATION_MAX_TOP10,
    HUMAN_MIN_PERCENT,
    UNKNOWN_MAX_PERCENT,
    PUMP_PROGRAM,
)
from helius import get_token_accounts, get_signatures, get_transaction

log = logging.getLogger(__name__)

JUPITER_PRICE_URL = "https://quote-api.jup.ag/v6/price"


async def check_concentration(
    session: aiohttp.ClientSession, mint: str
) -> tuple[bool, list[str]]:
    accounts = await get_token_accounts(session, mint)
    if not accounts:
        log.warning("No holders found for %s", mint)
        return False, []

    holders: list[dict] = []
    for acc in accounts:
        amount = float(acc.get("amount", 0))
        owner = acc.get("owner", "")
        holders.append({"owner": owner, "amount": amount})

    total_supply = sum(h["amount"] for h in holders)
    if total_supply == 0:
        return False, []

    for h in holders:
        h["pct"] = (h["amount"] / total_supply) * 100

    bonding_curve_owner = None
    for h in holders:
        if h["pct"] > 50:
            bonding_curve_owner = h["owner"]
            break

    filtered = [h for h in holders if h["owner"] != bonding_curve_owner]
    filtered.sort(key=lambda x: x["amount"], reverse=True)

    for h in filtered:
        if h["pct"] > CONCENTRATION_MAX_SINGLE:
            log.info("Holder %s has %.2f%% — too concentrated", h["owner"], h["pct"])
            return False, []

    top10_sum = sum(h["pct"] for h in filtered[:10])
    if top10_sum > CONCENTRATION_MAX_TOP10:
        log.info("Top-10 hold %.2f%% — too concentrated", top10_sum)
        return False, []

    holder_addresses = [h["owner"] for h in filtered]
    return True, holder_addresses


async def calculate_human_percent(
    session: aiohttp.ClientSession,
    holders: list[str],
    r: redis.Redis,
) -> tuple[bool, float]:
    human_count = 0
    unknown_count = 0
    total = len(holders)
    if total == 0:
        return False, 0.0

    for address in holders:
        cached = await r.get(f"human:{address}")
        if cached is not None:
            val = cached.decode() if isinstance(cached, bytes) else cached
            if val == "1":
                human_count += 1
            else:
                unknown_count += 1
            continue

        try:
            sigs = await get_signatures(session, address, limit=1)
            if sigs and len(sigs) > 0:
                human_count += 1
                await r.set(f"human:{address}", "1", ex=3600)
            else:
                unknown_count += 1
                await r.set(f"human:{address}", "0", ex=300)
        except Exception as e:
            log.warning("Error checking address %s: %s", address, e)
            unknown_count += 1

    human_pct = (human_count / total) * 100
    unknown_pct = (unknown_count / total) * 100

    passed = human_pct >= HUMAN_MIN_PERCENT and unknown_pct <= UNKNOWN_MAX_PERCENT
    log.info("Human: %.1f%%, Unknown: %.1f%%, Passed: %s", human_pct, unknown_pct, passed)
    return passed, human_pct


async def check_dev(
    session: aiohttp.ClientSession,
    mint: str,
    r: redis.Redis,
) -> dict:
    sigs = await get_signatures(session, mint, limit=1)
    if not sigs:
        return {"status": "Unknown", "msr": None}

    tx = await get_transaction(session, sigs[0]["signature"])
    if tx is None:
        return {"status": "Unknown", "msr": None}

    fee_payer = tx["transaction"]["message"]["accountKeys"][0]
    if isinstance(fee_payer, dict):
        fee_payer = fee_payer["pubkey"]

    bad = await r.get(f"bad_dev:{fee_payer}")
    if bad is not None:
        log.info("Dev %s is known bad", fee_payer)
        return {"status": "Bad", "msr": None}

    dev_sigs = await get_signatures(session, fee_payer, limit=50)
    cutoff = time.time() - 30 * 86400
    recent_sigs = [s for s in dev_sigs if s.get("blockTime", 0) >= cutoff]

    pump_tokens: list[str] = []
    checked = 0
    for sig_info in recent_sigs:
        if checked >= 20:
            break
        try:
            tx_data = await get_transaction(session, sig_info["signature"])
            if tx_data is None:
                continue
            msg = tx_data["transaction"]["message"]
            account_keys = msg["accountKeys"]
            programs = set()
            for key in account_keys:
                pubkey = key["pubkey"] if isinstance(key, dict) else key
                programs.add(pubkey)

            if PUMP_PROGRAM in programs:
                inner_instructions = tx_data.get("meta", {}).get("innerInstructions", [])
                for inner in inner_instructions:
                    for ix in inner.get("instructions", []):
                        parsed = ix.get("parsed", {})
                        if isinstance(parsed, dict) and parsed.get("type") == "initializeMint":
                            token_mint = parsed.get("info", {}).get("mint")
                            if token_mint and token_mint != mint:
                                pump_tokens.append(token_mint)

                if not inner_instructions:
                    log_msgs = tx_data.get("meta", {}).get("logMessages", [])
                    for lm in log_msgs:
                        if "InitializeMint" in lm or "create" in lm.lower():
                            post_token_balances = tx_data.get("meta", {}).get("postTokenBalances", [])
                            for ptb in post_token_balances:
                                tm = ptb.get("mint")
                                if tm and tm != mint and tm not in pump_tokens:
                                    pump_tokens.append(tm)
                            break
            checked += 1
        except Exception as e:
            log.warning("Error checking dev tx %s: %s", sig_info["signature"], e)
            checked += 1

    total_tokens = len(pump_tokens)
    if total_tokens < 3:
        return {"status": "Unknown", "msr": None, "dev": fee_payer, "tokens": total_tokens}

    survived = 0
    for token_mint in pump_tokens[:20]:
        try:
            async with session.get(
                JUPITER_PRICE_URL,
                params={"ids": token_mint, "vsToken": "USDC"},
            ) as resp:
                price_data = await resp.json()

            token_data = price_data.get("data", {}).get(token_mint)
            if token_data is None:
                continue

            price = float(token_data.get("price", 0))
            volume = 0.0
            extra_info = token_data.get("extraInfo", {})
            if extra_info:
                volume = float(extra_info.get("lastSwappedPrice", {}).get("lastJupiterSellAt", 0) or 0)

            if price > 0:
                survived += 1
        except Exception as e:
            log.warning("Error checking token price %s: %s", token_mint, e)

    msr = (survived / total_tokens) * 100 if total_tokens > 0 else 0

    if msr >= 70:
        status = "Clean"
        await r.set(f"dev_msr:{fee_payer}", str(msr), ex=86400)
    else:
        status = "Bad"
        await r.set(f"bad_dev:{fee_payer}", "1", ex=86400)

    log.info("Dev %s: %d tokens, %d survived, MSR=%.1f%%, status=%s", fee_payer, total_tokens, survived, msr, status)
    return {"status": status, "msr": msr, "dev": fee_payer, "tokens": total_tokens}
