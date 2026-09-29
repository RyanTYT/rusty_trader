"""Reference (streaming) rolling primitives — the Python twin of Rust
``trading/trading-app/src/strategy/helpers/rolling_fn.rs``.

These are the *specification*, not a menu. Each class mirrors its Rust
counterpart exactly:

* ``push(v) -> float | None`` — feed one value; returns the current value once
  the window is full, otherwise ``None`` (Rust returns ``Option``).
* ``replace_last(v)`` — correct the last pushed value and re-derive (live bar
  rewrite); ``None`` if nothing has been pushed.
* ``value() -> float | None`` — the current value without advancing.

They are deliberately small and O(1)-amortized where Rust is, so they can be
verified one-for-one against ``_vec.py`` (the vectorized twins) in ``verify.py``.
An LLM extending the library should write a NEW primitive as a similar class
and run ``verify.check_streaming_vs_vec`` rather than trusting it blindly.

The two polyadic/time-aware helpers take extra args:
* ``RollingBeta`` pairs ``(x, y)`` (market return, instrument return).
* ``RollingDayVwap`` takes a ``Bar`` (reset per session via ``ts_epoch_ms``).

``RollingAggregator`` turns a stream of fine ``Bar``s into a stream of completed
coarse ``Bar``s so every other helper can be layered on a different timeframe
by feeding it the aggregator's output.
"""

from __future__ import annotations

import math
from collections import Counter, deque

from stratlab.io import Bar, Timeframe, ts_ms_to_et_date


# ────────────────────────────────────────────────────────────────────────────
# Sum family
# ────────────────────────────────────────────────────────────────────────────
class RollingSum:
    def __init__(self, window: int):
        assert window > 0
        self.w = window
        self._q: deque[float] = deque()
        self._s = 0.0

    def push(self, v: float) -> float | None:
        self._q.append(v)
        self._s += v
        if len(self._q) > self.w:
            self._s -= self._q.popleft()
        return self.value()

    def replace_last(self, v: float) -> float | None:
        if not self._q:
            return None
        self._s -= self._q.pop()
        self._q.append(v)
        self._s += v
        return self.value()

    def value(self) -> float | None:
        return self._s if len(self._q) == self.w else None


class RollingMean:
    """Arithmetic mean over a trailing window (returns None until full)."""

    def __init__(self, window: int):
        self._sum = RollingSum(window)

    @property
    def w(self) -> int:
        return self._sum.w

    def push(self, v: float) -> float | None:
        s = self._sum.push(v)
        return s / self.w if s is not None else None

    def replace_last(self, v: float) -> float | None:
        s = self._sum.replace_last(v)
        return s / self.w if s is not None else None

    def value(self) -> float | None:
        s = self._sum.value()
        return s / self.w if s is not None else None


class RollingStd:
    """Sample standard deviation (ddof=1, divisor n-1) over a trailing window.

    Matches Rust ``RollingStd``: ``sqrt(max(sum(x^2) - n*mean^2, 0) / (n-1))``.
    """

    def __init__(self, window: int):
        assert window > 0
        self.w = window
        self._q: deque[float] = deque()
        self._s = 0.0
        self._s2 = 0.0

    def push(self, v: float) -> float | None:
        self._q.append(v)
        self._s += v
        self._s2 += v * v
        if len(self._q) > self.w:
            old = self._q.popleft()
            self._s -= old
            self._s2 -= old * old
        return self.value()

    def replace_last(self, v: float) -> float | None:
        if not self._q:
            return None
        old = self._q.pop()
        self._s -= old
        self._s2 -= old * old
        self._q.append(v)
        self._s += v
        self._s2 += v * v
        return self.value()

    def value(self) -> float | None:
        if len(self._q) < self.w:
            return None
        n = self.w
        mean = self._s / n
        var = (self._s2 - n * mean * mean) / (n - 1)
        return math.sqrt(max(var, 0.0))


class RollingZScore:
    """(last_x - rolling_mean) / rolling_std (sample). None until the window is
    full. The causal primitive — never use a static/full-sample std."""

    def __init__(self, window: int):
        assert window >= 2
        self.w = window
        self._sum = RollingSum(window)
        self._std = RollingStd(window)
        self._last = 0.0

    def push(self, v: float) -> float | None:
        self._sum.push(v)
        self._std.push(v)
        self._last = v
        return self.value()

    def replace_last(self, v: float) -> float | None:
        if not self._sum._q:
            return None
        self._sum.replace_last(v)
        self._std.replace_last(v)
        self._last = v
        return self.value()

    def value(self) -> float | None:
        s = self._sum.value()
        if s is None:
            return None
        mean = s / self.w
        std = self._std.value()
        if std is None or std == 0.0:
            return None
        return (self._last - mean) / std


# ────────────────────────────────────────────────────────────────────────────
# Max / Min (monotonic deques, O(1) amortized)
# ────────────────────────────────────────────────────────────────────────────
class RollingMax:
    def __init__(self, window: int):
        assert window > 0
        self.w = window
        self.idx = 0
        self._dq: deque[tuple[int, float]] = deque()  # (idx, val) decreasing

    def push(self, v: float) -> float | None:
        i = self.idx
        self.idx += 1
        while self._dq and self._dq[-1][1] <= v:
            self._dq.pop()
        self._dq.append((i, v))
        ws = i + 1 - self.w
        while self._dq and self._dq[0][0] < ws:
            self._dq.popleft()
        return self.value()

    def replace_last(self, v: float) -> float | None:
        if self.idx == 0:
            return None
        i = self.idx - 1
        while self._dq and self._dq[-1][0] == i:
            self._dq.pop()
        while self._dq and self._dq[0][0] == i:
            self._dq.popleft()
        while self._dq and self._dq[-1][1] <= v:
            self._dq.pop()
        self._dq.append((i, v))
        ws = i + 1 - self.w
        while self._dq and self._dq[0][0] < ws:
            self._dq.popleft()
        return self.value()

    def value(self) -> float | None:
        return self._dq[0][1] if self.idx >= self.w else None


class RollingMin:
    def __init__(self, window: int):
        assert window > 0
        self.w = window
        self.idx = 0
        self._dq: deque[tuple[int, float]] = deque()  # (idx, val) increasing

    def push(self, v: float) -> float | None:
        i = self.idx
        self.idx += 1
        while self._dq and self._dq[-1][1] >= v:
            self._dq.pop()
        self._dq.append((i, v))
        ws = i + 1 - self.w
        while self._dq and self._dq[0][0] < ws:
            self._dq.popleft()
        return self.value()

    def replace_last(self, v: float) -> float | None:
        if self.idx == 0:
            return None
        i = self.idx - 1
        while self._dq and self._dq[-1][0] == i:
            self._dq.pop()
        while self._dq and self._dq[0][0] == i:
            self._dq.popleft()
        while self._dq and self._dq[-1][1] >= v:
            self._dq.pop()
        self._dq.append((i, v))
        ws = i + 1 - self.w
        while self._dq and self._dq[0][0] < ws:
            self._dq.popleft()
        return self.value()

    def value(self) -> float | None:
        return self._dq[0][1] if self.idx >= self.w else None


# ────────────────────────────────────────────────────────────────────────────
# Rate of change
# ────────────────────────────────────────────────────────────────────────────
class RollingRoc:
    """ROC over `diff` bars: (last - first) / first. None until diff+1 bars."""

    def __init__(self, diff: int):
        assert diff > 0
        self.diff = diff
        self._q: deque[float] = deque()

    def push(self, v: float) -> float | None:
        self._q.append(v)
        if len(self._q) > self.diff + 1:
            self._q.popleft()
        return self.value()

    def replace_last(self, v: float) -> float | None:
        if not self._q:
            return None
        self._q.pop()
        self._q.append(v)
        return self.value()

    def value(self) -> float | None:
        if len(self._q) == self.diff + 1:
            f = self._q[0]
            return (self._q[-1] - f) / f if f != 0.0 else None
        return None


# ────────────────────────────────────────────────────────────────────────────
# EWMA (adjust=False, alpha = 2/(span+1)) — emits immediately
# ────────────────────────────────────────────────────────────────────────────
class EwmMean:
    def __init__(self, span: int):
        assert span > 0
        self.alpha = 2.0 / (span + 1.0)
        self.idx = 0
        self._prev: float | None = None
        self._prev_prev: float | None = None

    def push(self, v: float) -> float:
        self.idx += 1
        cur = v if self._prev is None else self.alpha * v + (1.0 - self.alpha) * self._prev
        self._prev_prev = self._prev
        self._prev = cur
        return cur

    def replace_last(self, v: float) -> float | None:
        if self.idx == 0:
            return None
        if self.idx == 1:
            self._prev = v
            return v
        pp = self._prev_prev
        assert pp is not None
        cur = self.alpha * v + (1.0 - self.alpha) * pp
        self._prev = cur
        return cur

    def value(self) -> float | None:
        return self._prev


# ────────────────────────────────────────────────────────────────────────────
# Rank percentile (exact: #(values <= last) / window — ties count fully)
# ────────────────────────────────────────────────────────────────────────────
class RollingRankPct:
    def __init__(self, window: int):
        assert window > 0
        self.w = window
        self._q: deque[float] = deque()
        self._counts: Counter = Counter()
        self._last = 0.0

    def push(self, x: float) -> float | None:
        self._q.append(x)
        self._counts[x] += 1
        if len(self._q) > self.w:
            old = self._q.popleft()
            self._counts[old] -= 1
            if self._counts[old] == 0:
                del self._counts[old]
        self._last = x
        return self.value()

    def replace_last(self, x: float) -> float | None:
        if not self._q:
            return None
        old = self._q.pop()
        self._counts[old] -= 1
        if self._counts[old] == 0:
            del self._counts[old]
        self._q.append(x)
        self._counts[x] += 1
        self._last = x
        return self.value()

    def value(self) -> float | None:
        if len(self._q) < self.w:
            return None
        leq = sum(c for k, c in self._counts.items() if k <= self._last)
        return leq / self.w


# ────────────────────────────────────────────────────────────────────────────
# Session VWAP (close-weighted, resets each trading day) + Beta + FracDiff
# ────────────────────────────────────────────────────────────────────────────
class RollingDayVwap:
    """Cumulative close-weighted session VWAP (Rust `RollingDayVwap` uses close,
    NOT typical price). Resets on each new ET date."""

    def __init__(self):
        self._date = None
        self._cum_pv = 0.0
        self._cum_v = 0.0

    def push(self, bar: Bar) -> float:
        d = ts_ms_to_et_date(bar.ts_epoch_ms)
        if self._date != d:
            self._date = d
            self._cum_pv = 0.0
            self._cum_v = 0.0
        self._cum_pv += bar.close * bar.volume
        self._cum_v += bar.volume
        return self._cum_pv / self._cum_v if self._cum_v > 0.0 else bar.close

    def value(self) -> float | None:
        return (self._cum_pv / self._cum_v) if self._cum_v > 0.0 else None


class RollingBeta:
    """Rolling OLS beta of y on x (running sums, O(1) per push). Degenerate
    window (no x variance) → 0.0, matching Rust `RollingBeta`."""

    def __init__(self, window: int):
        assert window > 1
        self.w = window
        self._qx: deque[float] = deque()
        self._qy: deque[float] = deque()
        self.sx = self.sy = self.sxy = self.sxx = 0.0

    def push(self, x: float, y: float) -> float | None:
        self._qx.append(x)
        self._qy.append(y)
        self.sx += x
        self.sy += y
        self.sxy += x * y
        self.sxx += x * x
        if len(self._qx) > self.w:
            ox = self._qx.popleft()
            oy = self._qy.popleft()
            self.sx -= ox
            self.sy -= oy
            self.sxy -= ox * oy
            self.sxx -= ox * ox
        return self.value()

    def value(self) -> float | None:
        n = self.w
        if len(self._qx) < self.w:
            return None
        denom = n * self.sxx - self.sx * self.sx
        if abs(denom) < 1e-12:
            return 0.0
        return (n * self.sxy - self.sx * self.sy) / denom


class RollingFracDiff:
    """Fractional differentiation (Lopez de Prado) — a stationary,
    memory-preserving feature. Weights ``w_k = prod_{i=1..k} (i-1-d)/i``."""

    def __init__(self, d: float, threshold: float = 1e-5):
        assert 0.0 < d < 1.0
        self.weights = self._compute_weights(d, threshold)
        self._q: deque[float] = deque()

    @staticmethod
    def _compute_weights(d: float, threshold: float) -> list[float]:
        weights = [1.0]
        w = 1.0
        for k in range(1, 1001):
            w *= ((k - 1.0) - d) / k
            if abs(w) < threshold:
                break
            weights.append(w)
        return weights

    def push(self, price: float) -> float | None:
        self._q.append(price)
        if len(self._q) > len(self.weights):
            self._q.popleft()
        return self.value()

    def value(self) -> float | None:
        if len(self._q) < len(self.weights):
            return None
        return sum(w * p for w, p in zip(self.weights, reversed(self._q)))


# ────────────────────────────────────────────────────────────────────────────
# RollingAggregator — fine bars → coarse bars (the multi-timeframe adapter)
# ────────────────────────────────────────────────────────────────────────────
class RollingAggregator:
    """Turns a stream of fine `Bar`s into a stream of completed coarse `Bar`s.

    Aggregates by CALENDAR boundary (see ``Timeframe``), NOT by a bar count: a
    bucket is emitted when the FIRST bar of the NEXT bucket is pushed. With
    ``Timeframe.minutes(15)`` the 9:45 bucket (9:45, 9:50, 9:55) is emitted when
    a 10:00-boundary bar arrives. The final still-open bucket is emitted by
    ``flush()``. This stays causal: a coarse bar is only "published" when its
    last constituent closes (the next boundary is seen).

    Aggregation: open=first open, high=max high, low=min low, close=last close,
    volume=sum, ts_epoch_ms=last source ts.
    """

    def __init__(self, timeframe: Timeframe):
        self.tf = timeframe
        self._cur_key: tuple | None = None
        self._buf: list[Bar] = []

    def push(self, bar: Bar) -> Bar | None:
        key = self.tf.bucket(bar.ts_epoch_ms)
        if self._cur_key is None:
            self._cur_key = key
            self._buf.append(bar)
            return None
        if key != self._cur_key:
            out = self._aggregate()
            self._cur_key = key
            self._buf = [bar]
            return out
        self._buf.append(bar)
        return None

    def replace_last(self, bar: Bar) -> None:
        """Correct the last source bar within the current open bucket (a live
        bar rewrite changes OHLCV, not the timestamp, so no re-bucketing)."""
        if self._buf:
            self._buf.pop()
            self._buf.append(bar)

    def flush(self) -> Bar | None:
        """Emit the final still-open bucket (call at end-of-stream)."""
        if not self._buf:
            return None
        out = self._aggregate()
        self._buf = []
        return out

    def _aggregate(self) -> Bar:
        open_ = self._buf[0].open
        high = max(b.high for b in self._buf)
        low = min(b.low for b in self._buf)
        close = self._buf[-1].close
        volume = sum(b.volume for b in self._buf)
        ts = self._buf[-1].ts_epoch_ms
        return Bar(ts_epoch_ms=ts, open=open_, high=high, low=low, close=close, volume=volume)


# The full Rust `rolling_fn.rs` surface, for discoverability / parity mapping.
__all__ = [
    "EwmMean",
    "RollingAggregator",
    "RollingBeta",
    "RollingDayVwap",
    "RollingFracDiff",
    "RollingMax",
    "RollingMean",
    "RollingMin",
    "RollingRankPct",
    "RollingRoc",
    "RollingStd",
    "RollingSum",
    "RollingZScore",
]