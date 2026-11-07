# Study artifacts — the structure of `stratlab/study/`

`playbooks/study-artifacts.md` is THE spec for everything under `stratlab/study/`.
Follow it to the letter; no improvised layouts.

## The tree format

```
study/<program>/{dated_valid | dated_invalid_leak | current}/{probe | engine}/
    ├─ scripts live at the leaf root
    │      run_*    = engine-driving (calls the engine, produces numbers)
    │      probe_*  = analysis-only (plot, metrics, decomposition)
    ├─ output_artifacts/         .json .png .mp4      (finished results)
    ├─ intermediate_artifacts/   .parquet .csv .log .txt .pkl   (mid-rail)
    ├─ (self-contained units keep their own dir: _bak_*/ , reusable sub-dirs)
    ├─ FILE_AUDIT.md             tree root: renames + artifact→producer table + orphans
    └─ PRODUCERS.md              tree root: per-leaf provenance graph
```

## Classification rules (per-FILE, not per-folder)

- **Engine vs probe is decided by file content**, not location: engine receipts
  (`stratlab.engine`, `run_thorough`, `mod.backtest`, `cargo`,
  `local_backtest`, strategy imports); pure plot/metrics/decomposition ⇒
  probe. Borderline ⇒ engine (conservative), then re-verify by reading.
- **Era classes**: `current` = the newest active research wave; `dated_valid`
  = concluded, still-trusted results; `dated_invalid_leak` = superseded or
  contaminated work — kept, never deleted (it carries future context).
- **Artifact routing follows the artifact's type**: json/png/mp4 →
  `output_artifacts/`; parquet/csv/log/txt/pkl → `intermediate_artifacts/`.

## Producer prefixes (every artifact, no exceptions)

`{producer__}{origname.ext}` where producer is:
- the real producing script's stem (kept exact so linkage is mechanical);
- `local_backtest__` for rust-engine stdout dumps; `cfg__` for input envelopes;
- `orphan__` when no producing script can be evidenced.

Linkage is **evidence-based only**: a writer statement in the source
(`to_parquet` / `json.dump` / `open(...,"w")`) or a tag-key scheme. Never
guess.

## Hard rules

1. **COPY (never move) external producer scripts beside their artifacts.**
   Originals stay runnable in their own home.
2. **No central producers folder.** Every producer lives with its artifacts.
3. **NEVER DELETE anything** under `study/`. Orphans get `orphan__`; dead
   work goes in `dated_invalid_leak`.
4. **No leading `_` folders inside a program tree.** `_` prefixes are allowed
   at the study root only.
5. **One program = one study home.** Never create sibling folders for one
   program, never scatter one program's artifacts into another's tree.
6. **Report groupings BEFORE moving anything.** One message = inventory
   counts + proposed tree; execute after the user sees it.
7. **Never fabricate doc content.** FILE_AUDIT/PRODUCERS tables are
   regenerated programmatically from real inventory tables, not written
   from memory.

## The vbo umbrella (the one structural exception)

`study/vbo/` holds the `vbo_*` variant dirs with no class layer; variants
are per-era strategy homes (e.g. former `vbo_xgb` → `xgb_entry_filter`,
former `vbo_intx` → `interaction_grids`, the tc1 home). Each variant
follows the same leaf conventions above.

## When adding a NEW program

Scaffold the full skeleton from day one (`current/engine/`, `current/probe/`
with `output_artifacts/` + `intermediate_artifacts/`), name scripts
`run_*`/`probe_*`, prefix artifacts `{producer}__` as you create them, and
write FILE_AUDIT.md + PRODUCERS.md at the tree root as work lands — not as
a retroactive forensic exercise.

## Verification before declaring a move done (the five scans)

Ad-hoc `python3`/`find` scans, each must PASS:

(a) **no unprefixed artifacts** — every file in any `*_artifacts/` dir
    starts with `{producer}__`;
(b) **no unresolved producer stems** — every prefix stem is a script in
    the leaf, or `local_backtest` / `cfg` / `orphan`;
(c) **no class mismatches** — `run_*` scripts only in engine leaves,
    `probe_*` only in probe leaves;
(d) **no basename collisions** within any single directory;
(e) **tree count reconciled** — file count after the move equals the
    pre-move inventory.
