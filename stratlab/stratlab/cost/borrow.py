"""Stock-loan (borrow) cost for multi-day short holds.

Intraday books can ignore borrow; a book that holds shorts for days cannot.
Convention here is ACT/360 on calendar days held (entry date to exit date),
charged in bp of the short leg's notional::

    drag_bps = rate_bps_per_year * calendar_days_held / 360

The rate is an assumption, not a measurement: 200 bp/yr approximates
general-collateral large caps in normal regimes; hard-to-borrow names run far
higher. Sensitivity to the rate belongs in the study, next to the spread
multiples (``feature-panel-audit`` SS14: present cost at several levels).
"""

from __future__ import annotations

import datetime as _dt

import numpy as np
import pandas as pd

#: default borrow assumption (bp/yr) for liquid large-cap shorts
DEFAULT_BORROW_BPS_YEAR = 200.0
#: sensitivity ladder reported alongside every net number
BORROW_LADDER = (0.0, 100.0, 200.0, 500.0)


def calendar_days(entry, exit) -> int:
    """Calendar days from entry date to exit date (both ``datetime.date``)."""
    if isinstance(entry, pd.Timestamp):
        entry = entry.date()
    if isinstance(exit, pd.Timestamp):
        exit = exit.date()
    return max(int((exit - entry).days), 0)


def borrow_drag_bps(rate_bps_year: float, entry, exit) -> float:
    """Borrow drag in bp of short notional over the holding."""
    return float(rate_bps_year) * calendar_days(entry, exit) / 360.0


def borrow_table(entry, exit,
                 rates: tuple = BORROW_LADDER) -> dict[float, float]:
    """``{rate: drag_bps}`` for the sensitivity ladder."""
    return {float(r): borrow_drag_bps(float(r), entry, exit) for r in rates}
