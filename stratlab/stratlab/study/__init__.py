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
    "append_question",
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
    "validate_ipynb",
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


# ────────────────────────────────────────────────────────────────────────────
# Append a research question (the sanctioned way to grow a study)
# ────────────────────────────────────────────────────────────────────────────
def append_question(slug: str, question: str, directory: str = "study") -> str:
    """Append a labelled question block to ``<directory>/<slug>/run.py`` and
    return its path. The block is a 3-cell unit the LLM then fills in:

        markdown(question + hypothesis + why + alternatives + assumptions)
        -> code(experiment stub)
        -> markdown(finding + interpretation)

    Auto-numbers ``Q<n>`` by counting existing ``### Q`` headers. Raises
    ``FileNotFoundError`` with a remediation hint if run.py doesn't exist yet.
    """
    import re

    path = os.path.join(directory, slug, "run.py")
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"{path} does not exist — run `stratlab study new {slug}` first."
        )
    with open(path) as f:
        src = f.read()
    existing = re.findall(r"^### Q(\d+)\b", src, re.MULTILINE)
    n = max((int(x) for x in existing), default=0) + 1
    block = (
        "\n"
        f"# %% [markdown]\n"
        f"# ### Q{n} — {question}\n"
        f"#\n"
        f"# **Hypothesis:** <one sentence — the falsifiable claim>.\n"
        f"# **Why:** <why this question matters; what you expect to learn>.\n"
        f"# **Alternatives considered:**\n"
        f"# - …\n"
        f"# **Assumptions:**\n"
        f"# - …\n"
        f"#\n"
        f"# %%\n"
        f"# <experiment code — fill me in>\n"
        f"#\n"
        f"# %% [markdown]\n"
        f"# **Finding:** <paste the key number / result>.\n"
        f"# **Interpretation:** <what it means; does it support the hypothesis?>\n"
    )
    if not src.endswith("\n"):
        block = "\n" + block
    with open(path, "a") as f:
        f.write(block)
    return path


def validate_ipynb(path: str) -> list[str]:
    """Validate a rendered ``.ipynb`` notebook directly — extract its markdown
    cells and run the same ``validate()`` used on run.py. Lets you check the
    rendered output (what a human reviews) without round-tripping to run.py."""
    import json

    with open(path) as f:
        nb = json.load(f)
    md_parts: list[str] = []
    for cell in nb.get("cells", []):
        if cell.get("cell_type") == "markdown":
            src = cell.get("source", "")
            if isinstance(src, list):
                src = "".join(src)
            md_parts.append(src)
    return validate("\n\n".join(md_parts))
