"""Shared live counters: ws_listener writes, the Telegram /status command reads.
Lives in its own module to avoid an import cycle between ws_listener and alert."""

import time

started_at = time.time()

# cumulative since process start
totals = {"ws_events": 0, "buys_seen": 0, "txs_fetched": 0, "mcap_checks": 0, "alerts_sent": 0}

# timestamp of the last WebSocket event — freshness of the market feed
last_ws_event_at: float | None = None


def bump(key: str):
    global last_ws_event_at
    totals[key] += 1
    if key == "ws_events":
        last_ws_event_at = time.time()


def uptime_str() -> str:
    secs = int(time.time() - started_at)
    d, rem = divmod(secs, 86400)
    h, rem = divmod(rem, 3600)
    m, _ = divmod(rem, 60)
    parts = []
    if d:
        parts.append(f"{d}д")
    if h:
        parts.append(f"{h}ч")
    parts.append(f"{m}м")
    return " ".join(parts)
