# AGENTS.md — operational rules for agents working in this repo

## NEVER READ .env FILES — NO PERMISSION

**Rule: agents have NO PERMISSION to read any `.env` file (any directory, any
naming variant). Credentials, DB URLs, API keys and secrets live there and are
the user's private property. If a task genuinely requires a value from a `.env`
file, STOP and ASK THE USER, clearly flagging what you need and why — before
any read attempt.** This includes indirect reads (`cat`, `grep`, `source`,
python dotenv loading, shell expansion of the file, copying it, printing it
with any tool). Derive connection strings from the user's explicit
instructions instead.

## Running long scripts: use opencode's native BACKGROUND FUNCTION

**Rule: run long work with the shell tool's built-in background option
(`background: true`). opencode then returns control automatically and notifies
you when the command actually finishes — you continue other work in the
meantime.**

Correct — the command itself goes in the background, verbatim, no shell tricks:
```
shell(command="PYTHONPATH=. .venv/bin/python -u study/.../script.py",
      background=true)
```
Then **do not wait for the user and do not poll**. Keep working or end your
response. When the completion notification arrives, read the artefacts then.

**Never do this — do not detach the process yourself:**
```bash
nohup .venv/bin/python -u script.py > /tmp/log.txt 2>&1 &   # WRONG
```
and then any of:
```bash
sleep 400; cat /tmp/log.txt     # WRONG — blocking turn burn
cat /tmp/log.txt               # WRONG — that's polling, and usually too early
```

**Why:** `nohup ... &` returns instantly, so the tool call "completes" while the
job is still running. That forces you into a sleep-and-poll loop, which burns
turns, usually returns before the work is done, and leaves you guessing whether a
process is alive. opencode's background function is precisely the feature that
handles this: it tracks the process and returns a notification on real
completion. Use that mechanism instead of hand-rolling detachment.

Also: keep `-u` (unbuffered) on any Python whose stdout is redirected, otherwise
output is block-buffered and a log looks empty while the job is healthy.

## Repo-specific gotchas that have cost real time (verify these before trusting a number)

1. **Daily panel: never use `uni.resample(Timeframe.daily())` for indicators.**
   It returns **1583 rows for 1443 sessions** — the ~140 half-day sessions emit a
   duplicate bucket. Any rolling indicator built on it is silently corrupt (it
   produced 0 setups once). Build the daily series the way `connors_rsi2.py` does:
   `uni.grid_close.groupby(level=0).last()` (+ `.max()` for high, `.min()` for low,
   `.sum()` for volume).
2. **`.loc[date]` on a `(et_date, mso)` MultiIndex returns a 1-row DataFrame, not
   a Series.** Use `.loc[(date, mso)]` or `.xs(date, level=0)`. This silently
   yields empty/NaN per-day slices and Nones instead of numbers.
3. **Date-constant features (breadth, market indices) have no cross-sectional IC**
   by construction — they are one number per slice. Report them as slice-constant
   instead of feeding them to a per-name ranker.
4. **2-D boolean masking (`df[m]` with a 2-D mask) can return a mostly-NaN ragged
   array.** `.stack()` the mask once and build an explicit index of the selected
   (date, asset) pairs; index from that thereafter.
5. **`stratlab.metrics.rust` Sharpe/Sortino are computed on per-bar percentage
   returns**, so they are scale-free but window-dependent and **not additive**
   across subperiods. Sub-universe H1/H2 runs are also cold-start (fresh 200-day
   SMA200 warmup), so their trade counts do not partition the full-sample run —
   pad the window when you need a fair gate.
6. zsh does **not** word-split unquoted variables — pass CLI args literally.
7. **Every performance report quotes Sortino alongside Sharpe** (both annualised). `met()`/`pretty()` already carry it; keep it in every table, README bullet, and user-facing summary, even when the table gets wider. This is now a contract: `playbooks/reporting.md` — including its **capital-base rule: `starting_equity` stays at the 100K base unless the user explicitly specified another figure.**

## Rust port (trading/trading-app) — standing rules & §8 sleeve-autonomy architecture (2026-10-29)

- **Scope discipline:** verify file reality before every edit; anchor reports on exact
  insertion points; no scope creep beyond the weight-layer port without user word.
- **Test scope contract:** with `--features backtest`, unit suite =
  `cargo test --features backtest --lib` (baseline: 207 passed / 3 pre-existing user-owned
  ORB failures in `strategy::opening_range_breakout`); engine/parity scope =
  `cargo test --features backtest --bin backtest` (compiles, 0 unit tests). Never run
  smoke/integration legacy targets (pre-existing breakage, user-owned). Full engine parity
  runs happen on the SEPARATE data machine — local = unit/oracle tests only.
- **Runtime never reads parquet:** committed parquets/npz/npy in `data/blended5_fund/` are
  TEST ORACLES only; runtime bars come from the database.
- **§8 SLEEVE AUTONOMY = CURRENT ARCHITECTURE (P1a+P1b+P2+P3 complete 2026-10-28/29; design
  + file-by-file record in `stratlab/promoted/blended5_fund/PLAN.md` §8/§9):**
  - Live path = FOUR standalone sleeve executors `blended5_connors/macd/rsi/tc1`
    (`strategy/blended5_fund/sleeve.rs`, enum variant `Blended5Sleeve`): each is its OWN
    engine strategy with its OWN account; chain = head → weight gates → `FundGate` →
    `SharesGate`; sizing `sc = own equity / SE_SLEEVE`; `on_fill` → `apply_realised` on its
    OWN chain only (cross-sleeve broadcast is GONE from the live path).
  - `FundGate` (`gates/impls/fund_gate.rs`, P2): carries the sleeve's full book in SHARE
    SPACE (heads are delta-emitters — intent-only scaling never reaches held positions;
    the v1 intent-space gate exploded connors 10k→737k with cash −3.9M); fire
    classification is an exact port of legacy FundBook steps 4-8 (scale =
    (cap·eq−fixed)/priced, fixed≡0 in both engines); caps EOD 1.95 / INTRA 4.0 bind at the
    FUND level: peer gross comes from the slot-start `FundRegistry` snapshot
    (`state.rs`; vintage = SLOT-START, deterministic, sleeve-order independent; written by
    both replay runners per slot BEFORE publish_close); NaN/absent registry → fail-open
    pass-through + warn-once. Counters `take_counters()/log_counters(tag)` wired into
    local_backtest run ends.
  - Engine (P1b): `run_multi_route` = multi-strategy single pass (`replay.rs`
    `run_multi_with_warm_up/run_multi_with_bars`, per-strategy `InMemoryState` seeded 10k
    each + residual = capital − Σ seeds ≥ 0; per-strategy order_id counters start 0 =
    solo parity; per-slot rollup equity = residual + Σ). Attribution = engine `order_ref`
    (= strategy name) — NO delta-decomposition, NO merge semantics. Strategies never see
    commissions/margin (rust engine models NO margin interest; py-only concept).
  - Conservation invariant: residual + Σ sleeve equities = initial + realized PnL
    (asserted; P1b multi: 10,000 + 138,245.59 = 148,245.59 ✓).
  - **P2 baseline: 148,245.59 (SE 50k = 4×10k seeds + 10k residual, own-equity sizing
    family — NOT comparable to the legacy fund-equity-sizing pins below).** Solo pins
    (SE 10k, FundGate): connors-wtx 34,016.83612599067/1,853 byte-exact vs P1a; macd
    39,169.07552342104/2,491 byte-exact; rsi 34,322.28907056672/6,373 (1 ulp); tc1
    27,316.51/4,871 (Δ−1.27 floor-association class). Multi: connors/macd/rsi byte-exact
    vs solos (caps never fire), tc1 30,737.38 (+3,420.90 = the ONLY sleeve whose cap
    fires; multi cap basis = residual+Σ — DESIGNED fund-level cap semantics).
    Discriminator: legacy FundBook solo connors-plain SE10k (19,546.65/1,846/DD 22.9%) ==
    FundGate plain solo BYTE-IDENTICAL → FundGate == FundBook semantics proven.
  - **Engine nondeterminism (accepted, user declined canonicalization 2026-10-29):**
    within-timestamp fill ORDER varies per process (reconcile targets_map HashMap
    RandomState); results are order-invariant EXCEPT order-sensitive consumers — severity
    FIFO pairing (1-cell class, e.g. 2022-05-02 QCOM) and equity snapshot summation
    (1-ulp class). Do NOT chase sub-ulp/1-cell diffs across runs.
  - **P3 flag-retain CLOSED (legacy engine DELETED 2026-11-06, user ruling):** the
    legacy `fund.rs` FundBook/Blended5Fund composite (broadcast on_fill, ghost sync,
    unalloc park, sub_cur, fund-equity sizing) was REMOVED after the user asked for
    fund.rs to keep only the shared live items (USD_FX/SE_SLEEVE/EOD_CAP/INTRA_CAP,
    U173_VENUES + ibkr_primary_exchange, SleeveKind, bar_frame_key/realised_fill_from/
    warmup_feed_for/WARMUP_BAR_LIMIT, `minutes_since_open`). The `BLENDED5_LEGACY_FUND=1`
    escape hatch is GONE ("blended5_fund" now a reserved error name); the
    `StrategyEnum::Blended5Fund` variant and the `blended5_fund::Blended5Fund|FUND_NAME|USD_FX`
    re-export were removed. Legacy-engine pins (2a/2b, P6, parity golden,
    FundGate==FundBook discriminator) are HISTORICAL ONLY — parity reference lives in
    git history. Test-suite baseline 2026-11-06: **243 passed / 3 pre-existing ORB
    failures** (legacy fund unit tests deleted with the engine).
- **vbo_stack rename closed** (2026-10-26): `intx` → `vbo_stack` across
  `strategies/enhancements/tests`; runtime sleeve `name()` literal stays `"intx"` (data-file
  provenance); golden oracle test `intx_builder_rebuilds_committed_book_and_sizes` green.
- **Weight layer = concrete gates, NOT a generic gate** (user ruling): fold-map weight layers
  are feature-specific `GateLogic` implementations named for their fit inputs, under
  `src/strategy_wrappers/gates/impls/` (NOT `impl/` — keyword; user final: concise folder
  name `impls`). Current instances: `SpyRealizedVolGate` (unary quartile fold on SPY 10-day
  realized vol), `SpyGapIntradayRetGate` (pair fold on overnight-gap>75bp × SPY intraday ret
  at entry). Shared machinery in `gates/impls/mod.rs`: `FwdRetSample{date,sym,x,y}` /
  `FwdRetSample2{xa,xb}` (X = feature values, Y = realized forward return t5d bp),
  `fit_purge_ok` (entry < ds AND entry+10 calendar days < ds; ds = min fold-year entry date),
  `quantiles3` (linear interp = np.nanquantile), `bucket_code` (# edges passed, x>=e, digitize
  right=False), `unary_bucket_weights` (mean Y per bucket, n>=30 else 1.0, normalized by max
  positive bucket mean, clip [0.05, 1.0], no positive max → all 1.0). Pair gate: per-factor
  fit-quartile edges, 4×4 cell table (valid iff mean finite ∧ n>=30), same clip.
  Fold members = entry year ∧ finite label; non-finite feature member → explicit 1.0.
  Underfit folds (fit n<60) → EMPTY map (absent cells = 1.0 at consumption).
- **TC-1 tag-on gates (2026-10-27, post-leak-audit promotion)**: the live chain in
  `enhancements/vbo_stack.rs::enhanced()` is TimeFilter (kernel dead-window, mso ∈ (45, 90])
  → `EntryMomentumFilterGate` (A3_r_last3 binary FILTER: bucket open w=1 iff n≥30 ∧ bucket
  mean > fit-pool mean, else w=0 ⇒ entry dropped; fit < 200 → keep-all) →
  `FirstHourVolShareGate` (B1_firstHourVolShare staircase; fit < 200 → all-1.0;
  `adv_zero_fill` flag default true = python `Vd` zero-fill of already-tracked symbols'
  absent sessions) → D8×D1 pair tail (`SpyGapIntradayRetGateStream`, fit < 60, NO chain
  clip — `reclip=False`: every layer clips inside its own fit-time weight construction and
  the product is consumed unclipped; a chain clip would resurrect A3 hard drops w=0 →
  0.05; the pair's legacy `clip` field stays reserved for the vbo_stack_tests A2 pin).
  tc1 = w_A3 × w_B1 × X_D8D1 with NO dial/SPY-daily-momentum layers — `DistRangeDialGate`
  and `SpyRealizedVolGate` are RETIRED from the live chain (kept only in the legacy A2
  batch builder + tests). Promoted spec = study/vbo/interaction_grids/engine/FINDINGS.md "TC1 PROMOTED
  (post-leak-audit)"; tc1/stacktc1 are the same product (marginal_gates.py tc branch).
- **Fold pool semantic pin (A2 oracle-green, 2026-10-26; updated D2-A 2026-10-27):** the A2
  gates (`SpyRealizedVolGate`, D8×D1 pair) use the KEPT ∧ LABELLED census pool (dead
  excluded; 2,805 = 3,485 − 670 dead − 10 unlabelled) — oracle-pinned, do NOT change. The
  TC-1 tag-on gates (A3 `EntryMomentumFilterGate`, B1 `FirstHourVolShareGate`) instead
  MIRROR the python tag-on ladders' kept-census semantics (user ruling D2-A, 2026-10-27):
  edges come from LABELLED purged rows only, but bucket counts/means are taken over the
  census INCLUDING unlabelled entries (retained as `census_lost`, y=NaN, via the gates'
  `realize_labels` failure path; `fit_face(samples, lost, ds)`); poison: A3 → bucket
  closes (w=0, entry dropped), B1 → bucket weight 1.0 (python np.mean NaN semantics).
  vbo_stack A2 fold product is ORACLE-GREEN cell-for-cell
  vs `expected_stack_book_map41.csv` (1e-9); legacy npz path = `build_vbo_stack(rows)` /
  `opts(rows, stack_folds=false)` (default, keeps the b_intx committed oracle); A2 stack
  product = `opts(rows, true)`.
- **Persistence contract (DONE 2026-10-27, P4 item 5 closed):** superseded by the streaming
  ruling — gates are fold-streaming (census pools + labels accumulate as bars arrive; chain
  replay via `install_warmup` seeds state from warm bars; face refits lazily at the first
  entry intent of each fold year). No file-map persistence exists or is needed; the old
  read-file-on-init design was v2-era and was never wired.
- **MACD live severity layer (2026-10-28, fixed + verified):** fused MACD map =
  prod_clip(quint entry-day, sev, 0.4/2.0); quint cells materialize at the day_last flush
  (RSI-validated identical machinery); sev cells = fresh-entry day_last `score_only`
  (stream preview, no double-feed) + `on_realised` close-order appends keyed
  (pair.entry_date, sym) with fragments last-write-wins. Causal running-median fill
  (`MIN_PAST_FOR_FILL = 10`; below → cell appended NaN): **stream stats MUST be skipna
  (`nan_mean`/`nan_pstd`, band priors with a NaN arm cell skipped) — appending NaN with
  propagating stats poisons the arm forever (all weights → WQ[0]=0.6; this exact bug cost
  −84k: live compose 119,238.71 → fixed 213,841.68 vs py 200,856.09).** Identical-input
  oracle: `data/blended5_fund/macd_sevrow_dump.csv` + `macd_sev_wof_dump.csv` (battery
  plain-run rows pre-fillna + battery batch map via study/vbo/interaction_grids/engine/dump_macd_sevo.py) —
  `offline_compose_batch_idiom_matches_battery_wof_on_identical_rows` = 0 diffs on 2,087
  cells (machinery byte-exact); `offline_compose_running_median_mode_diff_report` = 272
  diffs / max step 0.5 (declared causal-fill cost). py golden provenance: the probe macd
  map is the FROZEN battery artifact `macd_sev_wof.parquet` (composed once from a topk-K4
  long-only 100k plain replay in the lag-0 era; the probe does `read_parquet` at warm-up,
  never re-composes). Lag-0 `run_thorough` byte-ref REPRODUCED the battery plain pin
  exactly (254,743.7457 / n=3837; current-env default lag-1 gives 233,537 / 3,834 on the
  same script — era difference, NOT engine rot), and on those battery rows the recomposed
  battery-idiom map == the frozen artifact (2,088/2,088 keys, 0 value diffs; the dump
  script's in-memory lineage check has a date-object-vs-str dtype bug and prints a false
  "zero overlap" — the SAVED CSVs match). Engine parity proven by the
  D1 seam (frozen py map into rust): 203,298.59 vs py 200,856.09 (±2.4k over 5 years).
  Tooling seams (env): MACD_PLAIN=1 (weights→All1), MACD_SEV_DUMP=path (rows CSV,
  WRITE-SIDE only), RUST_WMAP_DUMP_MACD; local_backtest cuts replay at config.end.
- **Severity map has NO data-loading paths (user ruling 2026-10-28, enforced):**
  `MACD_SEV_SEED` (pass-2 stream seed) and `MACD_FUSED_SEED` (frozen-map injection)
  DELETED along with their loaders (`read_seed_rows`/`read_fused_cells`), the
  `fused_seed` field, and the `refresh_fused` override branch in
  severity_quintile_gate.rs — the live severity map is composed ONLY in-process
  from its own traded data; the two-pass compose protocol is retired (live
  single-run compose = shipped regime, the 213,841.68 run). Remaining env seams
  are mode-only (MACD_PLAIN, RUST_WMODE_MACD) or write-only dumps
  (MACD_SEV_DUMP, RUST_WMAP_DUMP_*). Battery-map CSVs under
  data/blended5_fund/ load only in `#[cfg(test)]` oracles. Suite 195/3.
- **BLENDED5 connors sleeve = PLAIN by battery design (B9), two class flavors exist:**
  `ConnorsTrexSleeve` (connors-trendxsev-v4) carries trend×severity weights as a MERGED
  GRID `sigc_w` (frozen battery maps `data/connors_trend_wof.parquet` ×
  `data/connors_sev_wof.parquet`, cell-replace no-propagation; ARCH-1 asserts vs
  `connors_sigc_w.parquet`) — but the FUND leg constructs `weighted=False`
  (strategy.py:196) = plain signal w=1.0 (battery ConnorsFund subclasses PLAIN
  ConnorsRSI2; standalone weighted sleeve book = SE10k/2500 anchor pnl 29,706.06 /
  n=1,055 / SR 1.3340). Rust mirrors the plain leg (chain Source→EqScale→Shares, no
  layer; parity py probe 105,973.61/1,853 fills vs rust 105,972.5544 = ±38 ±1-share
  rounding fills, Δ −$1.06). A weighted connors fund leg is a NEW decision requiring
  its own parity regime + load-vs-compose ruling.
- **CONNORS WEIGHTED LEG PROMOTED (py + rust, 2026-10-28):** python promoted spec =
  `strategy.py:203` `ConnorsTrexSleeve(..., weighted=True)` — sigc_w merge (frozen
  battery maps `connors_trend_wof × sev_wof`, trend = prev-day TD=C/SMA200−1 pandas
  rank(pct=True) cohort bands WQ, sev = causal z-stream over the plain book's own
  trades, fusion prod_clip(0.4,2.0) intersection, NO propagation). Evidence (committed
  intx book, SE 50k): map41 lag1 plain 986,560.20/24,095 vs connw 1,342,958.29/24,181
  (Δ +356,398 / Shr 1.368→1.471 / Sort 1.95→2.101); u173mkt plain 1,823,029.89/25,897
  vs connw 2,564,565.92/25,994 (Δ +741,536 / Shr 1.575→1.660 / Sort 2.351→2.506).
  Probe-cell goldens (stacktc1 book): connw solo 169,184.32/1,855; full 1,322,992.90/
  24,537; u173mkt full 2,464,855.79/25,821; oracle dump =
  `study/vbo/interaction_grids/engine/dump_connors_trendxsevo.py` →
  `trading-app/data/blended5_fund/connors_trendxsevo.csv` (TEST ORACLE ONLY, n=1055).
  Rust port = LIVE-COMPOSE gate `ConnorsTrendSevGate`
  (`strategy_wrappers/gates/impls/connors_trendxsev.rs`): ConnorsTail daily tails
  (O/C/H + ATR ewm 1/14 min14 + SMA200 rolling), avg_rank_band (pandas pct semantics,
  ties=average, NO ≥2-names guard), SeverityStream(100) fed INTENT-DRIVEN
  (score-then-append in score_cohort; **on_realised = no-op PIN — under the LEGACY fund,
  receive_realised BROADCAST every fill to every sleeve's gate, so fill-driven feeding
  would ingest macd/rsi/intx trades into the connors z-history and score months early
  (verified: polluted run marginal +8,278 vs clean +156,520); under §8 the live sleeve
  routes ONLY its own fills, but the no-op PIN STAYS — belt-and-braces now, not
  load-bearing**; the legacy broadcast exposure in severity_quintile_gate.rs is likewise
  LEGACY-ONLY since P3 — the live MACD sleeve ingests only its own fills via its own
  chain's on_realised). CarryWeights lock=false
  cell-replace; seams mode-only `RUST_WMODE_CONNORS=plain|wtx` (DEFAULT wtx since
  parity green; "plain" = retired plain flavour, bare() retained verbatim) +
  write-only RUST_WMAP_DUMP_CONNORS. Rust ladder: 1a plain solo byte-exact
  105,972.5544/1853; 1b wtx solo 178,439.02/1,854 vs py 169,184.32 (+9,254.70
  declared: own-book stream vs frozen battery maps, cold-start, causal running-median
  fill; 240/332 shared fused cells exact); 2a full plain 934,224.40/24,905 (Δ vs py
  −52,336 = pre-existing rust-vs-py stream divergences); 2b full wtx 1,090,743.93/
  24,949 / Shr 1.3811 / Sort 1.9824 / mdd 46.63% — **LEGACY-ENGINE cells (single
  consolidated fund account, fund-equity sizing; §8 multi baseline is 148,245.59)**
  (rust-side connw marginal +156,520 / +0.028 Shr / +0.038 Sort / mdd −0.49pp;
  gap vs py −252,214 = pre-existing plain divergence compounded + own-book stream).
  Suite 207/3 (was 195/3; +12 gate tests).
- **GAP INVESTIGATION CLOSED (2026-10-28/29; full record in PLAN.md §1):** rust−py
  −252,214 (legacy 2b vs py promotedconnw lag1) = −115,904 frozen-vs-causal connors map
  semantics (battery-u173 z-history + partial coverage 348/937 vs own-map41-stream
  804/937; NOT look-ahead — frozen map proven 100% causal, rust compose cell-exact 837/837
  vs py causal) + −136,310 pure engine divergence (≈0 through 2025, accrued Apr–Sep 2026;
  Aug −137.8k same-map whipsaw = py winddown book did BETTER) + −59,863 expiry artifact
  (py b_intx.parquet dies 2026-06-30, macd_sev_wof.parquet dies 2026-06-26 → py winddown).
  **rust == py lag1 timing PROVEN** (byte-exact first fill 1609943400000 DE 28 @
  279.157913) — "set fill_lag_bars=0 to reproduce committed results" is WRONG: lag0
  plain 1,104,777 vs committed 986,560 (+118k), lag0 connw 1,397,678 vs 1,342,958 (+55k).
  py causal connw = 1,227,054.26/24,391 (the honest live-compose comparator).
- **P6 pin stale→SUPERSEDED:** the 2026-10-26 fund parity pin (fills 25,554 /
  end-eq 1,006,114.17, A2 golden) predates the TC-1 live-chain rewrite + the MACD
  severity fixes + the connw promotion; the 2a/2b cell numbers above are LEGACY-engine
  baselines (BLENDED5_LEGACY_FUND=1); the §8 multi baseline 148,245.59 is current.