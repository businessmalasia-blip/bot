import asyncio
import json
import logging

import aiohttp
import redis.asyncio as redis
import websockets

from config import (
    HELIUS_WSS,
    PUMP_PROGRAM,
    MCAP_THRESHOLD,
    USE_ENHANCED_WS,
    TX_FETCH_RPS,
    ANALYZED_TTL,
)
from helius import get_transaction
from pump_math import estimate_mcap_usd
from sol_price import get_cached_sol_price
from screener import analyze_token
from velocity import record_buy
import runtime_status

log = logging.getLogger(__name__)

WSOL_MINT = "So11111111111111111111111111111111111111112"

_active_mints: set[str] = set()

# rolling counters for the once-a-minute heartbeat line
_hb = {
    "ws_events": 0, "buys_seen": 0, "txs_fetched": 0, "mcap_checks": 0,
    # diagnostics: why a fetched tx didn't reach the mcap check
    "fetch_empty": 0,   # getTransaction returned nothing (sig too fresh / pruned)
    "parse_fail": 0,    # fetched fine but didn't parse as a pump.fun buy
}


_hb_max_mcap = 0.0  # highest mcap seen this minute — shows how far samples reach


def _bump(key: str):
    _hb[key] += 1
    runtime_status.bump(key)


async def _heartbeat_loop(r: redis.Redis, sig_queue: asyncio.Queue | None):
    """Proof-of-life: one INFO line per minute with stream throughput.
    If buys_seen stays at 0 for several minutes, the market feed is stale."""
    global _hb_max_mcap
    while True:
        await asyncio.sleep(60)
        sol_price = await get_cached_sol_price(r)
        queue_info = f", queue={sig_queue.qsize()}" if sig_queue is not None else ""
        log.info(
            "heartbeat: ws_events=%d, buys_seen=%d, txs_fetched=%d, mcap_checks=%d, "
            "fetch_empty=%d, parse_fail=%d, max_mcap=$%.0f, analyzing=%d, SOL=$%s%s",
            _hb["ws_events"], _hb["buys_seen"], _hb["txs_fetched"], _hb["mcap_checks"],
            _hb["fetch_empty"], _hb["parse_fail"], _hb_max_mcap,
            len(_active_mints),
            f"{sol_price:.2f}" if sol_price is not None else "?",
            queue_info,
        )
        for k in _hb:
            _hb[k] = 0
        _hb_max_mcap = 0.0


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

    # the traded mint is the one whose token account is owned by the bonding
    # curve; "first mint in postTokenBalances" grabs USDC/WSOL on router txs
    post_token_balances = data.get("meta", {}).get("postTokenBalances", [])
    candidates = [
        p for p in post_token_balances
        if p.get("mint") and p["mint"] != WSOL_MINT
    ]
    mint = None
    for p in candidates:
        if p.get("owner") == bc_address:
            mint = p["mint"]
            break
    if mint is None and candidates:
        mint = candidates[0]["mint"]

    if mint is None:
        return None, None, 0.0, None

    return mint, bc_address, bc_balance_lamports, buyer


async def _handle_parsed_tx(
    http_session: aiohttp.ClientSession,
    r: redis.Redis,
    mint: str,
    bc_address: str,
    bc_lamports: float,
    buyer: str | None,
):
    """Common tail of both WS modes: velocity, mcap threshold, analysis kickoff."""
    # velocity data accumulates for every token, so the stats
    # are already warm by the time analysis kicks in
    if buyer is not None:
        try:
            await record_buy(r, mint, buyer)
        except Exception as e:
            log.debug("record_buy failed for %s: %s", mint, e)

    if mint in _active_mints:
        return

    sol_price = await get_cached_sol_price(r)
    if sol_price is None:
        return

    _bump("mcap_checks")
    mcap = estimate_mcap_usd(bc_lamports, sol_price)
    global _hb_max_mcap
    if mcap > _hb_max_mcap:
        _hb_max_mcap = mcap
    if mcap < MCAP_THRESHOLD:
        return

    # every buy above the threshold lands here — analyze each mint once
    # per ANALYZED_TTL, not on every buy
    if await r.exists(f"analyzed:{mint}"):
        return

    log.info("Token %s hit $%.0f mcap, starting analysis", mint, mcap)
    _active_mints.add(mint)
    asyncio.create_task(_run_analysis(http_session, r, mint, bc_address, mcap))


# ---------------------------------------------------------------------------
# Enhanced mode: transactionSubscribe (Helius Developer plan and above)
# ---------------------------------------------------------------------------

async def _subscribe_enhanced(ws):
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
    log.info("Subscribed to Pump.fun transactions (enhanced mode)")


async def _run_enhanced(ws, http_session: aiohttp.ClientSession, r: redis.Redis):
    await _subscribe_enhanced(ws)
    async for raw_msg in ws:
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            continue

        if msg.get("method") != "transactionNotification":
            continue

        _bump("ws_events")
        tx_data = msg.get("params", {}).get("result", {}).get("transaction", {})
        mint, bc_address, bc_lamports, buyer = _parse_pump_transaction(tx_data)
        if mint is None or bc_address is None:
            continue

        _bump("buys_seen")
        await _handle_parsed_tx(http_session, r, mint, bc_address, bc_lamports, buyer)


# ---------------------------------------------------------------------------
# Free-tier mode: logsSubscribe + sampled getTransaction fetches.
# logsSubscribe is available on every Helius plan but carries no balances,
# so buy signatures go into a bounded queue and a worker fetches them at
# TX_FETCH_RPS to stay inside the monthly credit budget. Buys arrive far
# faster than the budget allows — the queue keeps only the newest ones,
# which is fine: a token nearing the mcap threshold produces buys every
# few seconds, so sampling still catches it, just with a small delay.
# ---------------------------------------------------------------------------

async def _subscribe_logs(ws):
    subscribe_msg = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [PUMP_PROGRAM]},
            {"commitment": "confirmed"},
        ],
    }
    await ws.send(json.dumps(subscribe_msg))
    log.info("Subscribed to Pump.fun logs (free-tier mode)")


async def _run_logs(ws, sig_queue: asyncio.Queue):
    await _subscribe_logs(ws)
    async for raw_msg in ws:
        try:
            msg = json.loads(raw_msg)
        except json.JSONDecodeError:
            continue

        if msg.get("method") != "logsNotification":
            continue

        _bump("ws_events")
        value = msg.get("params", {}).get("result", {}).get("value", {})
        if value.get("err") is not None:
            continue

        logs = value.get("logs", [])
        if not any("Program log: Instruction: Buy" in line for line in logs):
            continue

        _bump("buys_seen")

        signature = value.get("signature")
        if not signature:
            continue

        if sig_queue.full():
            try:
                sig_queue.get_nowait()  # drop the oldest, keep the stream fresh
            except asyncio.QueueEmpty:
                pass
        sig_queue.put_nowait(signature)


async def _fetch_worker(
    http_session: aiohttp.ClientSession, r: redis.Redis, sig_queue: asyncio.Queue
):
    delay = 1.0 / TX_FETCH_RPS
    while True:
        signature = await sig_queue.get()
        try:
            tx_data = await get_transaction(http_session, signature)
            _bump("txs_fetched")
            if not tx_data:
                _hb["fetch_empty"] += 1
            else:
                mint, bc_address, bc_lamports, buyer = _parse_pump_transaction(tx_data)
                if mint is not None and bc_address is not None:
                    await _handle_parsed_tx(http_session, r, mint, bc_address, bc_lamports, buyer)
                else:
                    _hb["parse_fail"] += 1
        except Exception as e:
            log.warning("Failed to fetch tx %s: %s", signature, e)
        await asyncio.sleep(delay)


async def ws_listener(http_session: aiohttp.ClientSession, r: redis.Redis):
    sig_queue: asyncio.Queue = asyncio.Queue(maxsize=200)
    fetch_task = None
    if not USE_ENHANCED_WS:
        fetch_task = asyncio.create_task(_fetch_worker(http_session, r, sig_queue))
    heartbeat_task = asyncio.create_task(
        _heartbeat_loop(r, sig_queue if not USE_ENHANCED_WS else None)
    )

    try:
        while True:
            try:
                async with websockets.connect(
                    HELIUS_WSS,
                    ping_interval=30,
                    ping_timeout=10,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    if USE_ENHANCED_WS:
                        await _run_enhanced(ws, http_session, r)
                    else:
                        await _run_logs(ws, sig_queue)

            except (websockets.ConnectionClosed, ConnectionError, OSError) as e:
                log.warning("WebSocket disconnected: %s. Reconnecting in 5s...", e)
                await asyncio.sleep(5)
            except Exception as e:
                log.error("Unexpected WS error: %s. Reconnecting in 10s...", e)
                await asyncio.sleep(10)
    finally:
        heartbeat_task.cancel()
        if fetch_task is not None:
            fetch_task.cancel()


async def _run_analysis(
    session: aiohttp.ClientSession,
    r: redis.Redis,
    mint: str,
    bc_address: str,
    mcap_seen: float = 0.0,
):
    try:
        await r.set(f"analyzed:{mint}", "1", ex=ANALYZED_TTL)
        await analyze_token(session, r, mint, bc_address, mcap_seen)
    except Exception as e:
        log.error("Analysis failed for %s: %s", mint, e)
        # transient RPC failure — allow a retry on the token's next buy
        await r.delete(f"analyzed:{mint}")
    finally:
        _active_mints.discard(mint)
