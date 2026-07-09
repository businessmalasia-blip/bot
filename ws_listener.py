import asyncio
import json
import logging

import aiohttp
import redis.asyncio as redis
import websockets

from config import HELIUS_WSS, HELIUS_API_KEY, PUMP_PROGRAM, MCAP_THRESHOLD
from sol_price import get_cached_sol_price
from screener import analyze_token
from velocity import record_buy

log = logging.getLogger(__name__)

_active_mints: set[str] = set()


def _parse_pump_transaction(data: dict) -> tuple[str | None, str | None, float, str | None]:
    """Extract mint, bonding curve address, curve SOL balance and buyer (fee payer)."""
    if not isinstance(data, dict):
        return None, None, 0.0, None

    log_messages = data.get("meta", {}).get("logMessages", [])
    is_buy = False
    for msg in log_messages:
        if "Program log: Instruction: Buy" in msg:
            is_buy = True
            break

    if not is_buy:
        return None, None, 0.0, None

    account_keys = data.get("transaction", {}).get("message", {}).get("accountKeys", [])
    pre_balances = data.get("meta", {}).get("preBalances", [])
    post_balances = data.get("meta", {}).get("postBalances", [])

    if not account_keys or not pre_balances or not post_balances:
        return None, None, 0.0, None

    first_key = account_keys[0]
    buyer = first_key["pubkey"] if isinstance(first_key, dict) else first_key

    post_token_balances = data.get("meta", {}).get("postTokenBalances", [])
    mint = None
    for ptb in post_token_balances:
        m = ptb.get("mint")
        if m:
            mint = m
            break

    if mint is None:
        return None, None, 0.0, None

    max_gain = 0
    bc_index = -1
    for i in range(len(pre_balances)):
        gain = post_balances[i] - pre_balances[i]
        if gain > max_gain:
            max_gain = gain
            bc_index = i

    if bc_index < 0:
        return None, None, 0.0, None

    key = account_keys[bc_index]
    bc_address = key["pubkey"] if isinstance(key, dict) else key
    bc_balance_lamports = post_balances[bc_index]

    return mint, bc_address, bc_balance_lamports, buyer


async def _subscribe(ws):
    subscribe_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "transactionSubscribe",
        "params": [
            {"accountInclude": [PUMP_PROGRAM]},
            {
                "commitment": "confirmed",
                "encoding": "jsonParsed",
                "transactionDetails": "full",
                "maxSupportedTransactionVersion": 0,
            },
        ],
    }
    await ws.send(json.dumps(subscribe_msg))
    log.info("Subscribed to Pump.fun transactions")


async def ws_listener(http_session: aiohttp.ClientSession, r: redis.Redis):
    while True:
        try:
            async with websockets.connect(
                HELIUS_WSS,
                ping_interval=30,
                ping_timeout=10,
                max_size=10 * 1024 * 1024,
            ) as ws:
                await _subscribe(ws)

                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        continue

                    if "method" not in msg or msg["method"] != "transactionNotification":
                        continue

                    params = msg.get("params", {})
                    result = params.get("result", {})
                    tx_data = result.get("transaction", {})

                    mint, bc_address, bc_lamports, buyer = _parse_pump_transaction(tx_data)
                    if mint is None or bc_address is None:
                        continue

                    # velocity data accumulates for every token, so the stats
                    # are already warm by the time analysis kicks in
                    if buyer is not None:
                        try:
                            await record_buy(r, mint, buyer)
                        except Exception as e:
                            log.debug("record_buy failed for %s: %s", mint, e)

                    if mint in _active_mints:
                        continue

                    sol_price = await get_cached_sol_price(r)
                    if sol_price is None:
                        continue

                    mcap = (bc_lamports / 1e9) * sol_price
                    if mcap < MCAP_THRESHOLD:
                        continue

                    log.info("Token %s hit $%.0f mcap, starting analysis", mint, mcap)
                    _active_mints.add(mint)
                    asyncio.create_task(_run_analysis(http_session, r, mint, bc_address))

        except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
            log.warning("WebSocket disconnected: %s. Reconnecting in 5s...", e)
            await asyncio.sleep(5)
        except Exception as e:
            log.error("Unexpected WS error: %s. Reconnecting in 10s...", e)
            await asyncio.sleep(10)


async def _run_analysis(
    session: aiohttp.ClientSession,
    r: redis.Redis,
    mint: str,
    bc_address: str,
):
    try:
        await analyze_token(session, r, mint, bc_address)
    except Exception as e:
        log.error("Analysis failed for %s: %s", mint, e)
    finally:
        _active_mints.discard(mint)
