import asyncio
import logging
import time

import aiohttp
import aiosqlite
import redis.asyncio as redis

from config import DB_PATH
from helius import get_account_info
from sol_price import get_cached_sol_price

log = logging.getLogger(__name__)

JUPITER_PRICE_URL = "https://quote-api.jup.ag/v6/price"

# (column suffix, seconds after alert)
CHECKPOINTS = [("1h", 3600), ("6h", 21600), ("24h", 86400)]

_db: aiosqlite.Connection | None = None

SCHEMA = """
CREATE TABLE IF NOT EXISTS alerts (
    mint TEXT PRIMARY KEY,
    bc_address TEXT NOT NULL,
    name TEXT,
    symbol TEXT,
    alerted_at REAL NOT NULL,
    mcap_at_alert REAL NOT NULL,
    human_pct REAL,
    dev_status TEXT,
    msr REAL,
    velocity INTEGER,
    bundle_txs INTEGER,
    socials TEXT,
    mcap_1h REAL,
    mcap_6h REAL,
    mcap_24h REAL,
    status_1h TEXT,
    status_6h TEXT,
    status_24h TEXT
);
"""


async def init_db():
    global _db
    _db = await aiosqlite.connect(DB_PATH)
    _db.row_factory = aiosqlite.Row
    await _db.execute(SCHEMA)
    await _db.commit()
    log.info("Stats DB ready at %s", DB_PATH)


async def close_db():
    if _db is not None:
        await _db.close()


async def record_alert(
    mint: str,
    bc_address: str,
    name: str,
    symbol: str,
    mcap: float,
    human_pct: float,
    dev_status: str,
    msr: float | None,
    velocity: int,
    bundle_txs: int,
    socials: list[str],
):
    await _db.execute(
        """INSERT OR REPLACE INTO alerts
           (mint, bc_address, name, symbol, alerted_at, mcap_at_alert,
            human_pct, dev_status, msr, velocity, bundle_txs, socials)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (mint, bc_address, name, symbol, time.time(), mcap,
         human_pct, dev_status, msr, velocity, bundle_txs, ",".join(socials)),
    )
    await _db.commit()
    log.info("Alert recorded for %s at $%.0f", mint, mcap)


async def _measure_token(
    session: aiohttp.ClientSession, r: redis.Redis, mint: str, bc_address: str
) -> tuple[float | None, str]:
    """Returns (current mcap or None, status).

    While the token sits on the bonding curve, mcap comes from the curve's SOL
    balance. A drained/closed curve means the token either graduated to a DEX
    (Jupiter then knows its price -> "graduated") or died ("dead").
    """
    try:
        info = await get_account_info(session, bc_address)
        value = info.get("value") if info else None
        lamports = value["lamports"] if value else 0
        # rent-exempt minimum is ~0.002 SOL; below ~0.01 SOL the curve is done
        if lamports > 10_000_000:
            sol_price = await get_cached_sol_price(r)
            if sol_price is None:
                return None, "pending"
            return (lamports / 1e9) * sol_price, "on_curve"
    except Exception as e:
        log.warning("getAccountInfo failed for %s: %s", bc_address, e)
        return None, "pending"

    try:
        async with session.get(
            JUPITER_PRICE_URL, params={"ids": mint, "vsToken": "USDC"}
        ) as resp:
            data = await resp.json()
        token_data = data.get("data", {}).get(mint)
        if token_data and float(token_data.get("price", 0)) > 0:
            return None, "graduated"
    except Exception as e:
        log.debug("Jupiter price check failed for %s: %s", mint, e)

    return 0.0, "dead"


async def checkpoint_loop(session: aiohttp.ClientSession, r: redis.Redis):
    """Every minute, fill in any due 1h/6h/24h checkpoints."""
    while True:
        try:
            now = time.time()
            for suffix, delay in CHECKPOINTS:
                cursor = await _db.execute(
                    f"""SELECT mint, bc_address FROM alerts
                        WHERE status_{suffix} IS NULL AND alerted_at <= ?""",
                    (now - delay,),
                )
                rows = await cursor.fetchall()
                for row in rows:
                    mcap, status = await _measure_token(session, r, row["mint"], row["bc_address"])
                    if status == "pending":
                        continue
                    await _db.execute(
                        f"UPDATE alerts SET mcap_{suffix} = ?, status_{suffix} = ? WHERE mint = ?",
                        (mcap, status, row["mint"]),
                    )
                    await _db.commit()
                    log.info("Checkpoint %s for %s: mcap=%s status=%s", suffix, row["mint"], mcap, status)
        except Exception as e:
            log.error("Checkpoint loop error: %s", e)
        await asyncio.sleep(60)


async def summary() -> str:
    """Aggregate stats for the /stats Telegram command."""
    cursor = await _db.execute("SELECT * FROM alerts")
    rows = await cursor.fetchall()
    total = len(rows)
    if total == 0:
        return "📊 Пока нет алертов."

    lines = [f"📊 <b>Статистика скринера</b>", f"Всего алертов: {total}"]

    for suffix, _ in CHECKPOINTS:
        checked = [r for r in rows if r[f"status_{suffix}"] is not None]
        if not checked:
            continue
        graduated = sum(1 for r in checked if r[f"status_{suffix}"] == "graduated")
        dead = sum(1 for r in checked if r[f"status_{suffix}"] == "dead")
        on_curve = [r for r in checked if r[f"status_{suffix}"] == "on_curve"]
        up13 = sum(
            1 for r in on_curve
            if r[f"mcap_{suffix}"] and r[f"mcap_{suffix}"] >= r["mcap_at_alert"] * 1.3
        ) + graduated
        up2 = sum(
            1 for r in on_curve
            if r[f"mcap_{suffix}"] and r[f"mcap_{suffix}"] >= r["mcap_at_alert"] * 2
        ) + graduated
        n = len(checked)
        lines.append(
            f"\n<b>Через {suffix}</b> (проверено {n}):\n"
            f"  🎓 Градуация: {graduated} ({graduated / n * 100:.0f}%)\n"
            f"  📈 ≥1.3x: {up13} ({up13 / n * 100:.0f}%)\n"
            f"  🚀 ≥2x: {up2} ({up2 / n * 100:.0f}%)\n"
            f"  💀 Умерло: {dead} ({dead / n * 100:.0f}%)"
        )

    return "\n".join(lines)
