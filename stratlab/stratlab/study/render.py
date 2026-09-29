"""Render a jupytext-percent ``run.py`` into an ``.ipynb`` notebook (nbformat v4).

No hard dependencies — the notebook is written as plain JSON, so it opens in
Jupyter / VSCode without jupytext/nbformat installed. (jupytext is only needed
if you want to edit the ``.ipynb`` *as* a notebook and round-trip it back.)
"""

from __future__ import annotations

import json
import os


def parse_cells(src: str) -> list[tuple[str, list[str]]]:
    """Parse jupytext-percent source into ``[(cell_type, lines)]`` where
    cell_type is ``"markdown"`` or ``"code"``."""
    cells: list[tuple[str, list[str]]] = []
    current_type: str | None = None
    current_lines: list[str] = []

    for line in src.splitlines():
        if line.startswith("# %%"):
            if current_type is not None:
                cells.append((current_type, current_lines))
            header = line[4:].strip()
            current_type = "markdown" if header.startswith("[markdown]") else "code"
            current_lines = []
        else:
            if current_type is None:
                current_type = "code"
            current_lines.append(line)

    if current_type is not None:
        cells.append((current_type, current_lines))
    return cells


def _markdown_lines(lines: list[str]) -> list[str]:
    """Strip the leading jupytext comment marker from markdown cell lines."""
    out: list[str] = []
    for ln in lines:
        if ln == "#":
            out.append("\n")
        elif ln.startswith("# "):
            out.append(ln[2:] + "\n")
        else:
            out.append(ln + "\n")
    return out


def to_notebook(cells: list[tuple[str, list[str]]]) -> dict:
    nb_cells = []
    for ctype, lines in cells:
        if ctype == "markdown":
            nb_cells.append({"cell_type": "markdown", "metadata": {}, "source": _markdown_lines(lines)})
        else:
            nb_cells.append({
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [ln + "\n" for ln in lines],
            })
    return {
        "cells": nb_cells,
        "metadata": {
            "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def render_notebook(src: str) -> dict:
    """jupytext-percent source -> a notebook dict."""
    return to_notebook(parse_cells(src))


def write_ipynb(src: str, out_path: str) -> str:
    nb = render_notebook(src)
    with open(out_path, "w") as f:
        json.dump(nb, f, indent=2)
    return out_path


def render_runpy(path: str, out_path: str | None = None) -> str:
    """Read ``path`` (a jupytext-percent .py) and write the ``.ipynb`` sibling.

    Returns the output path. Default output is ``<path>`` with ``.ipynb``."""
    with open(path) as f:
        src = f.read()
    out = out_path or (os.path.splitext(path)[0] + ".ipynb")
    return write_ipynb(src, out)


__all__ = ["parse_cells", "to_notebook", "render_notebook", "write_ipynb", "render_runpy"]