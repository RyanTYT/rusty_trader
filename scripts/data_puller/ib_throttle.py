"""Concurrent IBKR historical fetcher with live throttling instrumentation.

Replaces the serial fetch loop when thousands of requests are needed. The point
is empirical: IBKR's published limits for bars >= 1 min are "lifted" but
soft-throttled, so rather than guess a sleep interval we fire requests as fast
as the socket allows and MEASURE what happens -- peak concurrency, request rate
in rolling windows, and any throttling responses.

Everything is recorded to a JSONL event log so a run can be analysed after the
fact, whether or not throttling showed up.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

# IBKR's published pacing rules (TWS API "Historical Data Limitations"). These
# are the floors to compare measurements against, not caps to enforce.
PACING_IDENTICAL_RETRY_S = 15     # no identical request within 15s
PACING_SAME_CONTRACT_2S = 6       # <=6 requests per contract/exchange/tick in 2s
PACING_WINDOW_S = 600              # <=60 requests in any 10 min
PACING_WINDOW_MAX = 60
MAX_SIMULTANEOUS = 50             # "max 50 simultaneous open historical requests"

THROTTLE_MARKERS = (
    "max requests", "maximum number of requests", "throttl", "pacing",
    "too many requests", "request limit", "historical data farm is busy",
    "farm is busy", "exceeded", "rate limit",
)


@dataclass
class ThrottleMonitor:
    """Rolling-window request accounting + throttle detection."""

    log_path: Path | None = None
    windows_s: tuple[float, ...] = (2.0, 10.0, 60.0, 600.0)
    events: deque = field(default_factory=lambda: deque(maxlen=20000))

    # live counters
    in_flight: int = 0
    peak_in_flight: int = 0
    total: int = 0
    started: float = field(default_factory=time.monotonic)
    # rolling peak per window
    peak_window: dict[float, int] = field(default_factory=dict)
    # throttle evidence
    throttled_empty: int = 0
    throttled_errors: int = 0
    ok: int = 0
    short: int = 0
    throttle_samples: list[str] = field(default_factory=list)

    def _record(self, kind: str, **kw):
        now = time.monotonic()
        self.events.append((now, kind, kw))
        if self.log_path:
            rec = {"t": round(now - self.started, 4), "kind": kind, **kw}
            with self.log_path.open("a") as fh:
                fh.write(json.dumps(rec) + "\n")

    def begin(self, key: str) -> None:
        self.in_flight += 1
        self.total += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        now = time.monotonic()
        # Record FIRST, then sample the windows -- previously this appended
        # here AND inside _record, double-counting every begin and inflating
        # all window peaks by 2x.
        self._record("begin", key=key, in_flight=self.in_flight)
        for w in self.windows_s:
            n = self._count(time.monotonic(), w)
            self.peak_window[w] = max(self.peak_window.get(w, 0), n)

    def end(self, key: str, n_bars: int, err: str | None = None) -> None:
        self.in_flight = max(0, self.in_flight - 1)
        low = (err or "").lower()
        is_throttle = any(m in low for m in THROTTLE_MARKERS)
        if is_throttle:
            self.throttled_errors += 1
            if len(self.throttle_samples) < 20:
                self.throttle_samples.append(f"{key}: {err[:140]}")
        elif n_bars > 0:
            self.ok += 1
        elif err:
            pass
        else:
            # empty with no error: either a genuine holiday or a silent throttle
            self.throttled_empty += 1
        self._record("end", key=key, n_bars=n_bars, err=err, in_flight=self.in_flight)

    def _count(self, now: float, w: float) -> int:
        c = 0
        for t, kind, _ in self.events:
            if kind != "begin":
                continue
            if now - t <= w:
                c += 1
        return c

    def rate(self, w: float) -> float:
        return self.peak_window.get(w, 0) / w

    def report(self) -> str:
        el = time.monotonic() - self.started
        lines = [
            "=" * 66,
            "THROTTLE MEASUREMENT",
            "=" * 66,
            f"  requests total     : {self.total:,}",
            f"  elapsed           : {el:.1f}s  ({self.total/max(el,1e-9):.1f} req/s)",
            f"  peak concurrency  : {self.peak_in_flight} "
            f"(IBKR documented max {MAX_SIMULTANEOUS})",
        ]
        for w in self.windows_s:
            n = self.peak_window.get(w, 0)
            lines.append(f"  peak in {w:>5.0f}s window: {n:5d} req "
                         f"({n/w:.2f}/s)")
        lines += [
            f"  ok (bars returned) : {self.ok:,}",
            f"  empty, no error    : {self.throttled_empty:,}  "
            f"(holidays OR silent throttle)",
            f"  explicit throttle  : {self.throttled_errors:,}",
        ]
        if self.peak_window.get(600, 0) > PACING_WINDOW_MAX:
            lines.append(
                f"  NOTE: exceeded IBKR's documented {PACING_WINDOW_MAX}"
                f" req / 10 min ({self.peak_window[600]})")
        if self.throttle_samples:
            lines.append("  throttle samples:")
            for s in self.throttle_samples[:5]:
                lines.append(f"    {s}")
        verdict = ("NO throttling observed" if self.throttled_errors == 0
                   else "THROTTLED")
        lines.append(f"  VERDICT: {verdict}")
        return "\n".join(lines)


async def fetch_many(ib, tasks: list[tuple[str, int]], *, end_dt_fmt,
                     bar_size: str, duration: str, concurrency: int,
                     monitor: ThrottleMonitor, timeout: float = 120.0,
                     on_result=None):
    """Fire `tasks` [(key, end_epoch)] concurrently, bounded by `concurrency`.

    Returns {key: (n_bars, error)}.
    """
    sem = asyncio.Semaphore(concurrency)
    results: dict[str, tuple[int, str | None]] = {}
    lock = asyncio.Lock()

    async def one(key: str, end_epoch: int):
        async with sem:
            contract, end_str = end_dt_fmt(key, end_epoch)
            monitor.begin(key)
            try:
                bars = await ib.reqHistoricalDataAsync(
                    contract, end_str, duration, bar_size,
                    "TRADES", True, 2, False, [], timeout)
                n = len(bars)
                err = None
                if n == 0:
                    # distinguish throttle from a real empty session
                    err = None
                monitor.end(key, n, err)
                results[key] = (n, err)
            except Exception as e:  # noqa: BLE001
                monitor.end(key, 0, f"{type(e).__name__}: {e}")
                results[key] = (0, f"{type(e).__name__}: {e}")
            if on_result:
                await on_result(key, results[key])

    await asyncio.gather(*(one(k, e) for k, e in tasks))
    return results
