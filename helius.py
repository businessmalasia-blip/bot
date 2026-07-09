import asyncio
import time
import aiohttp

from config import HELIUS_RPC, HELIUS_DELAY

_last_call = 0.0
_lock = asyncio.Lock()


async def _rate_limit():
    global _last_call
    async with _lock:
        now = time.monotonic()
        wait = HELIUS_DELAY - (now - _last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        _last_call = time.monotonic()


async def rpc_call(session: aiohttp.ClientSession, method: str, params: list | dict) -> dict:
    await _rate_limit()
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    async with session.post(HELIUS_RPC, json=payload) as resp:
        data = await resp.json()
    if "error" in data:
        raise RuntimeError(f"RPC error in {method}: {data['error']}")
    return data["result"]


async def get_token_accounts(session: aiohttp.ClientSession, mint: str, limit: int = 100) -> list:
    result = await rpc_call(session, "getTokenAccounts", [mint, {"limit": limit}])
    return result.get("token_accounts", [])


async def get_signatures(
    session: aiohttp.ClientSession, address: str, limit: int = 1, before: str | None = None
) -> list:
    opts: dict = {"limit": limit}
    if before is not None:
        opts["before"] = before
    return await rpc_call(session, "getSignaturesForAddress", [address, opts])


async def get_transaction(session: aiohttp.ClientSession, signature: str) -> dict:
    # commitment must match the WS subscription level: the default is
    # "finalized", which returns null for ~15s-fresh confirmed txs — and the
    # free-tier fetch queue is almost entirely txs younger than that
    return await rpc_call(
        session,
        "getTransaction",
        [signature, {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0,
            "commitment": "confirmed",
        }],
    )


async def get_account_info(session: aiohttp.ClientSession, address: str) -> dict:
    return await rpc_call(session, "getAccountInfo", [address, {"encoding": "jsonParsed"}])


async def get_asset(session: aiohttp.ClientSession, mint: str) -> dict:
    return await rpc_call(session, "getAsset", {"id": mint})
