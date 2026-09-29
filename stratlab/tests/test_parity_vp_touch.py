"""VP-touch golden parity — metric level.

Validates that `stratlab.metrics.rust` reproduces the committed Rust
`backtest_results_vp_touch.json` (max drawdown / Sharpe / Sortino / scalars)
from its equity curve, to machine precision.

Full trade-level parity (exact trade count + equity from the transaction list)
is gated on a Rust transaction dump — see `stratlab.parity.full_parity_problems`.
"""

from __future__ import annotations

from stratlab.parity import (
    RustGolden, default_golden_path, metric_parity_problems,
)


def _load() -> RustGolden:
    return RustGolden.load(default_golden_path())


def test_metric_parity() -> None:
    g = _load()
    problems = metric_parity_problems(g, rtol=1e-6, atol=1e-6)
    assert not problems, "metric parity problems:\n" + "\n".join(problems)


def test_pinned_scalars() -> None:
    """Pin the golden so an accidental file swap can't silently re-bless itself."""
    g = _load()
    assert g.num_trades == 13202
    assert g.num_closed_trades == 6601
    assert g.num_winning_trades == 1612
    assert abs(g.final_equity - 49495.34266740918) < 1e-9
    assert abs(g.sharpe - -2.3690442216478593) < 1e-12
    assert abs(g.sortino - -3.2751061441648863) < 1e-12
    assert abs(g.max_drawdown_pct - 53.862804759591235) < 1e-12
    assert g.bar_interval_minutes == 5
    assert g.bars_per_year == 19656.0


if __name__ == "__main__":
    test_metric_parity()
    print("PASS  test_metric_parity")
    test_pinned_scalars()
    print("PASS  test_pinned_scalars")
    print("2 passed")