# data_puller — IBKR bar-data acquisition layer

This is a standalone stack. The application stacks (trading, platform,
monitoring, tests) live in `compose/`; do not merge this one into them.

Production puller: **`pull_bars.py`** — ONE script, any IBKR bar size,
three modes (full/extend/repair), universe loaded from a JSON file.

## HOW TO RUN (the verified-correct lane)

Commands below run from the repo root. Add `--env-file .env` if you want the
root env file's `IMAGE_TAG`; otherwise the images default to `latest`.
On the dev VM, prefix raw `docker` commands with `sudo -n` when `docker info`
fails; `scripts/build.sh data` and `scripts/up.sh data` detect this themselves.

On Docker Desktop/mac the pull RUNS INSIDE THE GATEWAY'S NETSTACK. Host-side
connects to 127.0.0.1:4002 are a verified dead end (docker-proxy peer IP fails
the gateway trust check no matter what the config trusts). Do not debug it.

    # 0. build the images once on the dev VM (prod uses the prebuilt images):
    docker compose -f scripts/data_puller/docker-compose.yml \
        -f scripts/data_puller/compose.build.yml build

    # 1. up + wait for the entrypoint logs to reach this line:
    docker compose -f scripts/data_puller/docker-compose.yml up -d tws
    docker compose -f scripts/data_puller/docker-compose.yml logs -f tws
    #    wait for: "gateway ready -- running: sleep infinity"
    #    (nc succeeding is NOT readiness — the proxy binds the port before the
    #     gateway is logged in; only the logs line is)

    # 2a. NORMAL lane (its own container, shares tws netstack):
    docker compose -f scripts/data_puller/docker-compose.yml run --rm pull-1min
    #    defaults: TF='5 mins', MODE=extend, UNIVERSE=/app/universes/full_universe.json,
    #    OUTPUT=/data/backtest-results/stats/raw_5min_bars.parquet (= APP/data via mount)
    #    FIRST-EVER run: -e MODE=full   (extend refuses a missing output file)
    #    other env overrides: TF, START_DATE, END_DATE, CHUNK_DAYS, ASSETS, IB_CLIENT_ID...

    # 2b. MANUAL lane (exec inside the gateway container):
    docker compose -f scripts/data_puller/docker-compose.yml \
        cp scripts/data_puller/pull_bars.py tws:/app/
    docker compose -f scripts/data_puller/docker-compose.yml exec -w /app tws \
        python3 /app/pull_bars.py --mode full --assets SPY \
        --start 2026-11-04 --end 2026-11-07 --out /tmp/verify.parquet
    docker compose -f scripts/data_puller/docker-compose.yml \
        cp tws:/tmp/verify.parquet .

The authoritative, always-current version of this recipe + the traps it cost is
the ██-boxed "HOW TO RUN — READ THIS FIRST" section at the top of
`pull_bars.py` itself. If this README and the docstring ever disagree,
the docstring wins.

## Production files

| file | role |
|---|---|
| `pull_bars.py` | the puller (any `--tf`, modes full/extend/repair, staged chunks + duckdb merge + integrity report) |
| `ib_throttle.py` | ThrottleMonitor (per-request jsonl event log) + `MAX_SIMULTANEOUS` concurrency cap used by pull_bars |
| `Dockerfile.pull_1min` | puller image for FUTURE rebuilds (ENTRYPOINT already pull_bars.py; current compose lane = bind-mounts, no rebuild needed) |
| `universes/*.json` | universe files consumed via `--universe` (existing_41 / all_new / benchmarks / full_universe / excluded_no_contract) |
| `build_universes.py` | retired root file — merged into `universes/build_universes.py` |
| `test/` | 7 self-contained tests of pull_bars machinery (all green, no gateway needed) |

## The universe producer (single file)

`universes/build_universes.py` — PRODUCER ONLY, runs by hand, nothing at
runtime imports it. Holds the candidate pool (VERIFIED + PLAUSIBLE), the
design constraints/ban lists, the deterministic selection
(verified-first, sector ceilings, cap 150) and the legacy preset tables,
then writes the five JSONs. `python3 universes/build_universes.py` after
deliberately editing a table; output is deterministic and diffs are
auditable. The retired 3-file chain (universe_candidates /
universe_design / select_universe) was merged into it 2026-11-08 with
byte-identical JSON output verified before the old files were removed.

## Sidelined / utensil scripts (root)

| script | status |
|---|---|
| `fix_contracts.py` | utensil — diagnosing tickers that resolve to no IB contract (run when a universe file changes; pull_bars already skips+reports unresolvables) |
| `screen_universe.py` | retired to `experiments/` — screens candidates from bars, superseded by the curated `universes/build_universes.py` producer |
| `filter_universe.py` | obsolete — legacy final-merge filter for the `raw_bars_extended` era |
| `index_membership.py` | retired to `experiments/` — dormant one-off index-membership analysis; refresh source when ESMA/S&P/Nasdaq lists roll |
| `watch_pull.sh` | obsolete — live-progress watcher for the old `ibc-*` host-side runs |
| `experiments/` | RETIRED predecessor pullers + diag probes (see `experiments/README.md`) |
