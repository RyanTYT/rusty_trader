# Study-first routing — where new code goes

The default reflex for a research question ("sweep X", "is the signal
sensitive to Y", "decompose the Sharpe") must NOT be `touch
strategies/<name>.py`. That path is what filled `strategies/` with one-off
scripts. The sanctioned path is the study notebook: append a cell, not a
file.

## The decision table

| You're adding... | it goes in... | how |
|---|---|---|
| a research question / experiment / sweep / diagnostic | `studies/<slug>/run.py` (a cell) | `stratlab study append <slug> --question "..."` |
| a reusable model function (feature extraction, training, walk-forward) | `stratlab/models/<pkg>/` | new module under the right package |
| a new strategy | `stratlab/strategies/<name>.py` | `PrecomputedStrategy` subclass + `STUDY` markdown |
| a one-off diagnostic / analysis | `studies/<slug>/run.py` (a cell) | `stratlab study append` |

## The workflow for a research question

1. `stratlab study new <slug> --title "..."` — once per study; scaffold writes
   `studies/<slug>/run.py` with the four-section template (Features → Signal →
   Execution → Validation), each carrying Why + Alternatives + Assumptions.
2. `stratlab study append <slug> --question "<the question>"` — appends a
   3-cell block: markdown(question + hypothesis + why + alternatives +
   assumptions) → code(experiment stub) → markdown(finding + interpretation).
   Auto-numbers `Q<n>`.
3. Fill the code cell (the experiment). Fill the Why / finding / interpretation
   prompts too — they are the comments + context that make the notebook
   readable as a research narrative.
4. `stratlab study render <slug>/run.py` — regenerate the `.ipynb` from
   `run.py`.
5. `stratlab study validate <slug>/run.py` — gate the four-section labels
   (exit 1 if a section is missing its Why or Assumptions). Also works on the
   rendered `.ipynb`: `stratlab study validate <slug>/run.ipynb` — so you can
   validate the artifact a human reviews, not just the source.

## Rules

- **`run.py` is the source; `run.ipynb` is a rendered output.** Edit
  `run.py`, then render. The `.ipynb` is regenerated, not hand-edited — its
  diffs would be noise, and `validate` on `run.py` is the gate.
- **Never create a `.py` under `stratlab/strategies/` for a research
  question.** That folder holds `PrecomputedStrategy` subclasses only.
- **A reusable function that emerges from a study cell → promote to
  `stratlab/models/<pkg>/`** (like `models/vbo_xgb/`), then import it back
  into the cell. One-off code stays a cell.
- **Every research question carries a Why + Alternatives + Assumptions.** The
  `stratlab study append` block prompts for them; `validate` gates the four
  canonical sections. Don't leave the prompts blank — the comments are the
  point.

## What's already there (don't reinvent)

- `stratlab.study.dsl` — `feature` / `signal_rule` / `execution_model` /
  `validation_check` label functions; `validate()` enforces the four sections.
- `stratlab.study.scaffold` — writes the labelled `run.py` template.
- `stratlab.study.render` — renders `run.py` → `.ipynb` (plain JSON, no
  nbformat dependency).
- `stratlab.study.append_question` — appends a labelled question block.
- `stratlab.study.validate_ipynb` — validates the rendered `.ipynb` directly.

## Notebook narrative (the readability goal)

A study notebook should read as a research log, not a code dump:

1. Title + Hypothesis + Status.
2. The four canonical sections (Features → Signal → Execution → Validation),
   each with Why + Alternatives + Assumptions — the strategy *definition*.
3. A Research Log: each question is a markdown(question) → code(experiment) →
   markdown(finding + interpretation) block, in chronological order.
4. Results summary at the bottom.

`stratlab study append` builds block (3) correctly by construction — the
structural scaffolding (question / hypothesis / why / alternatives /
assumptions / finding / interpretation) is the LLM's fill-in-the-blank, so
comments and context are baked into the workflow rather than optional.
