"""Research-study scaffolding + labelling DSL.

A "study" is one folder under ``study/`` with the canonical four-section flow
(Features -> Signal -> Execution -> Validation). The DSL (`dsl.py`) provides the
mandatory label functions; `scaffold()` generates a runnable, correctly-labelled
``run.py`` (jupytext-percent, so it opens as a notebook with proper headers);
`validate()` gates the labelling.
"""

from __future__ import annotations

import os

from . import dsl, render
from .dsl import (
    SECTIONS,
    Study,
    alternatives,
    assumptions,
    execution_model,
    feature,
    h1,
    h2,
    h3,
    markdown_from_runpy,
    rationale,
    signal_rule,
    validate,
    validate_runpy,
    validation_check,
)

__all__ = [
    "SECTIONS",
    "Study",
    "alternatives",
    "assumptions",
    "dsl",
    "execution_model",
    "feature",
    "h1",
    "h2",
    "h3",
    "markdown_from_runpy",
    "rationale",
    "scaffold",
    "signal_rule",
    "validate",
    "validate_runpy",
    "validation_check",
]


# ────────────────────────────────────────────────────────────────────────────
# Scaffold: generate a correctly-labelled run.py skeleton
# ────────────────────────────────────────────────────────────────────────────
_RUNPY_TEMPLATE = '''# %% [markdown]
# # {title}
#
# **Hypothesis:** <one sentence — the falsifiable claim>.
#
# **Status:** hypothesis | in-progress | validated | rejected | promoted-to-production
#
# %% [markdown]
# ## 1. Features
#
# ### f01 — <feature name>
# **Why:** <why this feature = the economic/microstructure reason>.
# **Formula:** <exact definition; cite the causal horizon / source bar>.
# **Alternatives considered:**
# - …
# **Assumptions:**
# - …
#
# %% [markdown]
# ## 2. Signal
#
# ### s01 — <signal name>
# **Why:** <why these features combine into a decision, and why now>.
# **Rule:** <features -> target position / order, with thresholds>.
# **Alternatives considered:**
# - …
# **Assumptions:**
# - …
#
# %% [markdown]
# ## 3. Execution
#
# ### e01 — <execution model name>
# **Why:** <why this order type / timing / cost model>.
# **Execution:** <market vs limit; entry/exit timing; fill + commission model>.
# **Alternatives considered:**
# - …
# **Assumptions:**
# - …
#
# %% [markdown]
# ## 4. Validation
#
# ### v01 — <validation method>
# **Why:** <why this method guards against look-ahead / overfit>.
# **Method:** <H1/H2 split, walk-forward, cost-robustness, capacity…>.
# **Alternatives considered:**
# - …
# **Assumptions:**
# - …
#
# %% [markdown]
# ## Results
# <Paste the `stratlab` report / parity diff here, or leave it to `report()`.>
#
# %%
from __future__ import annotations
import sys
sys.path.insert(0, "../..")  # so `import stratlab` resolves from the repo root

from stratlab.io import load_default, Universe
from stratlab import primitives

# 1. Load raw_bars and derive features (causal only).
uni = Universe.from_parquet("../data/backtest-results/stats/raw_bars.parquet")
#   e.g. z = primitives.rolling_z(uni.grid_close, 20)

# 2. Signal: features -> target positions (shift(1) so the signal acts next bar).

# 3. Execution: run the thorough engine with the IBKR cost model.

# 4. Validation: H1/H2 + walk-forward + report.

# %%
# Optional: verify the primitives used here are causal + match the Rust spec.
from stratlab.primitives import verify
assert verify.run_all()
'''


def scaffold(slug: str, title: str | None = None, directory: str = "study") -> str:
    """Create ``<directory>/<slug>/run.py`` (correctly labelled) and return its
    path. Idempotent — leaves an existing file untouched."""
    folder = os.path.join(directory, slug)
    os.makedirs(folder, exist_ok=True)
    path = os.path.join(folder, "run.py")
    if os.path.exists(path):
        return path
    with open(path, "w") as f:
        f.write(_RUNPY_TEMPLATE.format(title=title or slug.replace("-", " ").title()))
    return path