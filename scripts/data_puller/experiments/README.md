# EXPERIMENTS — RETIRED IB-API EXPLORATION (PINNED 2026-11-07)

Everything in this folder is a **retired experiment**, kept as a provenance
record of how the IB-API data pulling was figured out. Do not run these as
production pullers. The production puller is:

    scripts/data_puller/pull_bars.py   (single file, any IBKR bar size)

with the compose lane:

    docker compose -f scripts/data_puller/docker-compose.yml run --rm pull-1min
    (runs INSIDE the tws container's netstack — see "which side of the
     socket" note in pull_bars.py; host-side connects do NOT work on
     Docker Desktop/mac)

## What lives here

| file | what it was |
|---|---|
| `pull_5min_bars.py` | production 5-min full-history puller (2021→now, 196-asset module-level preset universe, 60-day chunks). Superseded by `pull_bars.py --tf "5 mins"`: the presets are gone; universe files now live in `../universes/*.json`. |
| `pull_1min_bars.py` | 1-min repair/extend puller built after the 4,154 truncated asset-days event (60-day requests of 1-min bars silently truncate — IBKR caps 1-min at a 1-day request window). Superseded by `pull_bars.py --tf "1 min" --mode repair` (same narrowed-window repair scan, generic tf). |
| `diag*.py`, `diag/` | diagnostic probes written while characterising IB pacing/truncation behaviour. Their measurement notes were folded into pull_bars' help texts and the step-size table. |

These scripts still import `ib_throttle` from the parent dir (sys.path patched
in-file
so they remain runnable for archaeology).
