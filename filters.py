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
    BUNDLE_MAX_CREATION_TXS,
    HUMAN_CACHE_TTL,
    UNKNOWN_CACHE_TTL,
)
from helius import get_token_accounts, get_signatures, get_transaction, get_asset
from jupiter import get_token_price

log = logging.getLogger(__name__)

# a dev's past token counts as survived if it still has this much USD liquidity
SURVIVED_MIN_LIQUIDITY = 1000.0


async def check_concentration(
    session: aiohttp.ClientSession, mint: str, bonding_curve: str | None = None
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

    # exclude the bonding curve by address — the ">50%" heuristic stops
    # working mid-curve, where the curve's share drops below 50% and it
    # would be counted as a whale, false-rejecting every token
    def is_curve(h) -> bool:
        if bonding_curve is not None and h["owner"] == bonding_curve:
            return True
        return h["pct"] > 50

    filtered = [h for h in holders if not is_curve(h)]
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
                await r.set(f"human:{address}", "1", ex=HUMAN_CACHE_TTL)
            else:
                unknown_count += 1
                await r.set(f"human:{address}", "0", ex=UNKNOWN_CACHE_TTL)
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
        entry = await get_token_price(session, token_mint)
        if entry is None:
            continue
        price = float(entry.get("usdPrice", 0) or 0)
        liquidity = float(entry.get("liquidity", 0) or 0)
        if price > 0 and liquidity >= SURVIVED_MIN_LIQUIDITY:
            survived += 1

    msr = (survived / total_tokens) * 100 if total_tokens > 0 else 0

    if msr >= 70:
        status = "Clean"
        await r.set(f"dev_msr:{fee_payer}", str(msr), ex=86400)
    else:
        status = "Bad"
        await r.set(f"bad_dev:{fee_payer}", "1", ex=86400)

    log.info("Dev %s: %d tokens, %d survived, MSR=%.1f%%, status=%s", fee_payer, total_tokens, survived, msr, status)
    return {"status": status, "msr": msr, "dev": fee_payer, "tokens": total_tokens}


async def check_bundle(session: aiohttp.ClientSession, mint: str) -> tuple[bool, int | None]:
    """Sniper-bundle signature: several transactions land in the token's
    creation slot (or the one right after). Walks the signature history back
    to the oldest page to find the creation slot, then counts txs in it.

    Returns (passed, txs_in_creation_slots). CRITICAL: the count is only
    meaningful if pagination actually reached the token's first signature.
    Active tokens can have tens of thousands of txs; when the history is
    deeper than we can walk, verdict is "unknown" — (True, None), fail-open —
    because measuring a random old slot rejects every active token.
    """
    all_slots: list[int] = []
    before = None
    reached_creation = False
    for _ in range(5):  # up to 5000 signatures back
        page = await get_signatures(session, mint, limit=1000, before=before)
        if not page:
            reached_creation = True
            break
        all_slots.extend(s["slot"] for s in page if s.get("slot"))
        if len(page) < 1000:
            reached_creation = True
            break
        before = page[-1]["signature"]

    if not reached_creation:
        log.info("Bundle check for %s: history deeper than %d sigs, verdict unknown", mint, len(all_slots))
        return True, None

    if not all_slots:
        log.warning("Bundle check for %s: empty signature history, verdict unknown", mint)
        return True, None

    creation_slot = min(all_slots)
    creation_txs = sum(1 for s in all_slots if s <= creation_slot + 1)

    passed = creation_txs <= BUNDLE_MAX_CREATION_TXS
    log.info(
        "Bundle check for %s: %d txs in creation slot %d(+1), passed=%s",
        mint, creation_txs, creation_slot, passed,
    )
    return passed, creation_txs


SOCIAL_KEYS = ("twitter", "telegram", "website", "discord")


async def check_socials(session: aiohttp.ClientSession, mint: str) -> dict:
    """Collects social links from DAS getAsset: both the indexed content.links
    and the raw off-chain metadata JSON (pump.fun puts twitter/telegram there).

    Returns {"twitter": url, ...} with only the links that were found.
    Also returns name/symbol under "_name"/"_symbol" so the caller can reuse
    them for the alert without a second getAsset call.
    """
    socials: dict = {}
    try:
        asset = await get_asset(session, mint)
    except Exception as e:
        log.warning("getAsset failed for %s: %s", mint, e)
        return socials

    content = asset.get("content", {})
    metadata = content.get("metadata", {})
    socials["_name"] = metadata.get("name")
    socials["_symbol"] = metadata.get("symbol")

    links = content.get("links", {}) or {}
    for key in SOCIAL_KEYS:
        url = links.get(key)
        if url:
            socials[key] = url

    image = links.get("image")
    if not image:
        for f in content.get("files", []) or []:
            uri = f.get("uri") or f.get("cdn_uri")
            mime = f.get("mime", "")
            if uri and (mime.startswith("image") or not mime):
                image = f.get("cdn_uri") or uri
                break

    json_uri = content.get("json_uri")
    need_more = not image or any(k not in socials for k in SOCIAL_KEYS)
    if json_uri and need_more:
        try:
            async with session.get(json_uri, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                meta_json = await resp.json(content_type=None)
            if isinstance(meta_json, dict):
                for key in SOCIAL_KEYS:
                    url = meta_json.get(key)
                    if url and key not in socials:
                        socials[key] = url
                if not image:
                    image = meta_json.get("image")
        except Exception as e:
            log.debug("Failed to fetch metadata JSON for %s: %s", mint, e)

    if image:
        # Telegram can't fetch ipfs:// URIs directly — route through a gateway
        if image.startswith("ipfs://"):
            image = "https://ipfs.io/ipfs/" + image[len("ipfs://"):]
        socials["_image"] = image

    found = [k for k in SOCIAL_KEYS if k in socials]
    log.info("Socials for %s: %s", mint, found or "none")
    return socials
