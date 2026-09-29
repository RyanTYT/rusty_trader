"""Mandatory section-labelling DSL for a research study.

The point (per the project's research-traceability requirement): an LLM writing
a study's ``run.py`` MUST label every component with these functions (or the
exact strings they return), so the notebook always renders the canonical
four-section flow with consistent, greppable labels::

    1. Features  ->  2. Signal  ->  3. Execution  ->  4. Validation

Each component carries a stable id, a name, its definition (formula / rule /
model), a ``Why`` (rationale), the alternatives considered, and its assumptions.
``validate()`` enforces this structure so a study that skips the reasoning
fails loudly rather than silently losing it.
"""

from __future__ import annotations

from dataclasses import dataclass, field

# The canonical section order and their stable numbering.
SECTIONS = ["Features", "Signal", "Execution", "Validation"]
_SECTION_IDS = {s.lower(): i + 1 for i, s in enumerate(SECTIONS)}


# ────────────────────────────────────────────────────────────────────────────
# Header + prose helpers (return markdown)
# ────────────────────────────────────────────────────────────────────────────
def h1(title: str) -> str:  # top-level study title
    return f"# {title}"


def h2(name: str) -> str:
    """A canonical section header — auto-numbered for the four known sections."""
    n = _SECTION_IDS.get(name.strip().lower())
    return f"## {n}. {name}" if n else f"## {name}"


def h3(name: str) -> str:
    return f"### {name}"


def rationale(text: str) -> str:
    return f"**Why:** {text}"


def alternatives(items) -> str:
    if isinstance(items, str):
        items = [items]
    return "\n".join(["**Alternatives considered:**", *[f"- {i}" for i in items]])


def assumptions(items) -> str:
    if isinstance(items, str):
        items = [items]
    return "\n".join(["**Assumptions:**", *[f"- {i}" for i in items]])


# ────────────────────────────────────────────────────────────────────────────
# Structured component entries (feature / signal / execution / validation)
# ────────────────────────────────────────────────────────────────────────────
def _entry(fid: str, name: str, defn_label: str, definition: str,
           why: str, alt=(), asmp=()) -> str:
    parts = [f"### {fid} — {name}", "", rationale(why), "", f"**{defn_label}:** {definition}"]
    if alt:
        parts += ["", alternatives(alt)]
    if asmp:
        parts += ["", assumptions(asmp)]
    return "\n".join(parts)


def feature(fid: str, name: str, formula: str, why: str, alternatives=(), assumptions=()):
    """A feature: an input to the signal, with its formula and rationale."""
    return _entry(fid, name, "Formula", formula, why, alternatives, assumptions)


def signal_rule(fid: str, name: str, rule: str, why: str, alternatives=(), assumptions=()):
    """A signal rule: how features combine into a trade decision."""
    return _entry(fid, name, "Rule", rule, why, alternatives, assumptions)


def execution_model(fid: str, name: str, definition: str, why: str, alternatives=(), assumptions=()):
    """An execution choice: order type, entry/exit timing, fill + cost model."""
    return _entry(fid, name, "Execution", definition, why, alternatives, assumptions)


def validation_check(fid: str, name: str, method: str, why: str, alternatives=(), assumptions=()):
    """A validation step: H1/H2, walk-forward, cost-robustness, capacity, …"""
    return _entry(fid, name, "Method", method, why, alternatives, assumptions)


# ────────────────────────────────────────────────────────────────────────────
# Builder — assemble a study from cells, dump to markdown / jupytext .py
# ────────────────────────────────────────────────────────────────────────────
@dataclass
class Study:
    title: str
    cells: list[str] = field(default_factory=list)

    def add(self, md: str) -> Study:
        self.cells.append(md)
        return self

    def md(self, text: str) -> Study:
        return self.add(text)

    # convenience sugar mirroring the module-level helpers
    def h1(self, t: str) -> Study:
        return self.add(h1(t))

    def h2(self, name: str) -> Study:
        return self.add(h2(name))

    def feature(self, *a, **k) -> Study:
        return self.add(feature(*a, **k))

    def signal_rule(self, *a, **k) -> Study:
        return self.add(signal_rule(*a, **k))

    def execution_model(self, *a, **k) -> Study:
        return self.add(execution_model(*a, **k))

    def validation_check(self, *a, **k) -> Study:
        return self.add(validation_check(*a, **k))

    def to_markdown(self) -> str:
        return "\n\n".join(self.cells)

    def to_jupytext(self) -> str:
        """Render as a jupytext-percent script (markdown cells + code markers)."""
        out = ["# %% [markdown]", *(f"# {line}" if line else "#" for line in self.to_markdown().split("\n"))]
        return "\n".join(out)


# ────────────────────────────────────────────────────────────────────────────
# Validation — enforce the structure/labels
# ────────────────────────────────────────────────────────────────────────────
def validate(markdown: str) -> list[str]:
    """Return a list of problems (empty = valid). Checks the four canonical
    sections appear in order and every section carries a ``Why`` + assumptions."""
    problems: list[str] = []
    lines = markdown.splitlines()

    # locate the top-level sections (## N. Name)
    seen: list[str] = []
    current = None
    sections: dict[str, list[int]] = {}  # name -> line index
    for i, ln in enumerate(lines):
        s = ln.strip()
        if s.startswith("## "):
            body = s[3:].strip()
            # body is "N. Name" or "Name"
            name = body.split(". ", 1)[1] if ". " in body else body
            if name.lower() in _SECTION_IDS:
                sections[name] = sections.get(name, []) + [i]
                current = sections[name][-1]

    for want in SECTIONS:
        if want not in sections:
            problems.append(f"missing section: {want}")

    order = [s for s in SECTIONS if s in sections]
    if order != sorted(order, key=lambda s: _SECTION_IDS[s.lower()]):
        problems.append("sections out of order: " + " -> ".join(order) if order else "none")

    # every section must carry a Why + Assumptions
    def _has(sub: str, start: int, end: int) -> bool:
        return any(sub in ln for ln in lines[start:end])

    for name, idxs in sections.items():
        start = idxs[0]
        # end = next section's start, or EOF
        nxt = min((j for n2, js in sections.items() for j in js if j > start), default=None)
        end = nxt if nxt is not None else len(lines)
        if not _has("**Why:**", start, end):
            problems.append(f"section '{name}' missing a Why/rationale")
        if not _has("**Assumptions:**", start, end):
            problems.append(f"section '{name}' missing Assumptions")

    return problems


def markdown_from_runpy(src: str) -> str:
    """Extract the markdown cells from a jupytext-percent ``run.py`` source:
    ``# %% [markdown]`` cells, with the leading ``# `` comment prefix stripped,
    so the text can be fed to ``validate()``."""
    md_lines: list[str] = []
    for cell in src.split("# %%"):
        if "[markdown]" not in cell.split("\n", 1)[0]:
            continue
        for ln in cell.splitlines():
            if ln.startswith("# "):
                md_lines.append(ln[2:])
            elif ln == "#":
                md_lines.append("")
    return "\n".join(md_lines)


def validate_runpy(src: str) -> list[str]:
    """Validate a jupytext-format ``run.py`` source (extracts markdown first)."""
    return validate(markdown_from_runpy(src))


__all__ = [
    "SECTIONS",
    "Study",
    "alternatives",
    "assumptions",
    "execution_model",
    "feature",
    "h1",
    "h2",
    "h3",
    "markdown_from_runpy",
    "rationale",
    "signal_rule",
    "validate",
    "validate_runpy",
    "validation_check",
]