# rusty_trader

A personal live-trading bot written in Rust against Interactive Brokers' (IBKR) API. It ingests real-time market data, executes a strategy over consolidated bars, reconciles state against the broker, and self-heals through IBKR's nightly restarts and connection drops. It is also a **learning vehicle** for low-latency systems programming, async/sync interop, and database design.

> **Scope honesty.** This runs against a paper-trading account at personal scale. It is not production financial infrastructure and makes no claims about fitness for live capital. Treat it as a serious engineering artifact and a case study, not a trading product.

## Table of contents

- [What this is](#what-this-is)
- [Architecture at a glance](#architecture-at-a-glance)
- [Why sync ibapi](#why-sync-ibapi)
- [No locks on the hot path](#no-locks-on-the-hot-path)
- [The data plane](#the-data-plane)
- [Subsystem tour](#subsystem-tour)
  - [Persistence](#persistence)
  - [Execution](#execution)
  - [Strategy](#strategy)
  - [Scheduling & pure helpers](#scheduling--pure-helpers)
  - [Lifecycle & self-healing](#lifecycle--self-healing)
- [How to run](#how-to-run)
- [Extending it](#extending-it)
- [Known rough edges & debt](#known-rough-edges--debt)
- [My journey](#my-journey)

## What this is

A single Rust binary (`trading-app/`, edition 2024, ~20K LOC) that:

- **Connects to IB Gateway** (the Java gateway fronting TWS) via the `ibapi` crate, using one client for order updates and another for market data and contract lookups.
- **Subscribes to real-time 5-second bars** for a configured set of contracts, fans them out to a persistence consumer and a strategy consumer, and aggregates them into 1-minute (FX) or 5-minute (stock) bars.
- **Runs a strategy** (a reference "noise" strategy on QQQ plus placeholder `Manual`/`Unknown` strategies) whose sync `on_bar_update` produces either orders to emit immediately or target positions to reconcile against the broker.
- **Manages orders** through the full lifecycle: optimistic DB rows before IBKR confirms, FX hedge attachment for multi-currency buys, a local redb store for orders blocked on FX settlement, and a four-handler event pipeline for order updates.
- **Self-heals**: an outer restart loop re-boots the entire IBKR stack on connection instability, APAC resets, broken pipes, or broker maintenance windows, driven by a state machine that parses IBKR error codes out of the log stream.

Stack: `sqlx`/PostgreSQL with TimescaleDB hypertables, `redb` (order store), `moka` (caches), `tokio` (multi-threaded runtime), `axum` (HTTP server), `nyse-holiday-cal`, a forked `rust-ibapi-fork` (branch `ibapi-rtyt`), and a `spmc-ring` git submodule.

## Architecture at a glance

The defining design decision is that **`ibapi` is compiled with its `sync` feature**: every IBKR call blocks. This forces a split-world architecture in which blocking IBKR I/O runs on raw `std::thread`s while async DB/HTTP work runs on a tokio multi-threaded runtime, and the two are bridged by captured `tokio::runtime::Handle`s.

```
                ┌──────────────────────────────────────────────┐
                │   tokio multi-threaded runtime (main.rs)     │
                │   • sqlx DB queries                          │
                │   • yfinance fallback                        │
                │   • run_program select! loop                 │
                │   • order-update async receiver              │
                │   • axum HTTP (own separate runtime)         │
                └──────────────────────────────────────────────┘
                        ▲  Handle.spawn / Handle.block_on
                        │  (the bridge: OS threads dispatch async
                        │   work onto runtime workers, then park)
   ┌────────────────────┴───────────────────────────────────────┐
   │  Raw std::threads — blocking ibapi lives here              │
   │                                                            │
   │   producer thread (1/contract) ── try_push ──┐             │
   │                                              ▼             │
   │                                  ┌────────────────────┐    │
   │                                  │  SpmcRingBuffer    │    │
   │                                  └────────────────────┘    │
   │                                    │ independent           │
   │                          ┌─────────┴─────────┐             │
   │                          ▼                   ▼             │
   │                  DB consumer         strategy thread       │
   │                  (persists bars)     (hot spin → on_bar_   │
   │                                       update → block_on)   │
   │                                                            │
   │   order_update_stream thread ── mpsc ──► async receiver    │
   │   ephemeral: sync_timeout, place_order, cancel_orders      │
   └────────────────────────────────────────────────────────────┘
```

Roughly seven thread categories coexist: tokio workers, a separate axum runtime, per-contract producer threads, a grouped DB-consumer thread, per-strategy heartbeat threads, the order-update stream thread, and ephemeral one-shot threads for bounded sync calls. Each is named (`qqq_stock_prod`, `noise_strat`, `order_update_stream`, …) so `top -H` / `perf` output maps directly to logical roles.

## Why sync ibapi

**Historical note.** When this project started, `ibapi` had no async API. The sync path was the only option. What follows is the _retained_ reasoning — why I decided to stick with the sync architecture.

**The trade, precisely stated.** Sync is not faster than async per call — both hit the same IBKR socket with the same wire latency. The difference is a _threading-model_ difference. For this bot, sync is efficient because (a) the concurrency is small and fixed (~5–15 threads), so per-thread overhead is negligible, and (b) the latency-critical path hot-spins on a dedicated OS thread anyway.

**Easier to reason about, concretely:**

- **No async colour propagation.** If `on_bar_update` were `async`, every function it calls that touches I/O would have to be async too, and `.await` + `Send + 'static` bounds would ripple through the entire strategy layer. Sync keeps `on_bar_update` a plain function; only the single DB seam pays the async tax (via `Handle::block_on`).
- **Borrowing across I/O.** Sync code holds `&self`, `&mut VecDeque<Bar>`, `&Contract` across a blocking call naturally. Async would force every value held across an `.await` to be `Send + 'static`, pushing toward `Arc` and owned data.
- **Real backtraces.** A panic on a strategy thread gives a stack trace through the code → ibapi → the syscall. An async task panic gives a waker chain that's much harder to read.
- **Deterministic control flow.** No scheduler deciding when a future resumes; the thread runs your code linearly.

**Benefits the sync architecture provides this codebase:**

1. **Simple, explicit shutdown.** `std::thread` + an `is_alive: Arc<AtomicBool>` checked in the loop is really simple and easy to reason about where the code may be stalling. Async task cancellation (`JoinHandle::abort`, `CancellationToken`) is subtler — abort is async, tasks may be mid-`.await`. The `async_drop` + `is_alive` pattern stays clean precisely because the sync threads exit at the next loop iteration.
2. **Predictable profiling topology.** Named threads with known roles make `top -H`, `perf`, and the `hotpath` instrumentation directly interpretable — you can see exactly which logical work is burning CPU. Async worker pools obscure which task is using which worker.

**The costs:**

- **The `Handle::block_on` bridge has a sharp precondition.** `block_on` panics if the calling thread is already inside a tokio context. One of the harder issues to realise and fix as instead of immediately panicking, this can also manifest as stalls (Read My Blog to find out more).
- **Doesn't scale to high fanout.** Thread-per-operation would explode with thousands of concurrent streams. i.e. This architecture is more so for one with many many cores for many many strategies or someone like me who wants to try a few strategies from time to time.
- **No work-stealing.** A stuck producer can't have its work taken over by another thread, the way tokio workers steal tasks - though using std::thread::sleep on some architectures signals the CPU to deprioritise the thread (which may be comparable?).

## No locks on the hot path

The trading hot path — from a 5-second bar arriving at a producer thread, through the SPMC ring, the DB consumer, the strategy heartbeat, the order engine, and the order-update stream — uses no `Mutex`es or `RwLock`s. This is a deliberate design choice:

- **Per-thread-owned mutable state** — each producer / consumer / strategy thread owns its mutable state outright (the `VecDeque<Bar>` aggregator, the rolling-stats structs, the order deque); nothing else can touch it.
- **Immutable shared handles** — shared state across threads is `Arc<T>` of _immutable_ handles (a `PgPool`, a `tokio::runtime::Handle`, a `Contract`), never `Arc<Mutex<T>>`.
- **Lock-free atomics for liveness** — `AtomicBool` `is_alive` flags checked in every thread's loop; an `AtomicUsize` + `compare_exchange` singleton guard on the order-update stream.
- **Message passing for coordination** — `mpsc` channels (the order-update stream's sync-thread → async-receiver bridge; the batch-insert flush triggers; the server's state-feed channel).
- **Single-owner transitions** — teardown via `Arc::into_inner` (asserts one strong ref) and `Arc::get_mut` spin-waits, not locks.

The few `Mutex`es that do exist sit in control-plane code, all off the trading path and all held briefly:

| Site                                  | Lock                                                 | Why                                                                             |
| ------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------------------------------- |
| `logger.rs` connection state machine  | `std::sync::Mutex<ConnectionState>`                  | Log-stream-as-control-plane state; touched on connection-error log events only. |
| `server/server.rs` HTTP state handle  | `tokio::sync::Mutex<Option<Weak<ApplicationState>>>` | The server's `Weak` to the trading state, upgraded per HTTP request.            |
| `batch_operations.rs` shutdown signal | `std::sync::Mutex<Option<oneshot::Sender>>`          | The batch-insert background task's shutdown oneshot, touched once at teardown.  |

None are reachable from a bar-update or order-submission context. The payoff is that reasoning about the hot path needs no lock-order analysis: **there is no deadlock surface on the trading path because there is nothing to deadlock on.**

## The data plane

```
   IBKR TWS
     │  5s bars (sync realtime_bars)
     ▼
 ┌─────────────────┐   try_push (50×, else drop bar)
 │ Producer thread │ ──────────────────┐
 └─────────────────┘                   ▼
                       ┌──────────────────────────────┐
                       │  SpmcRingBuffer<Bar, 128, 10>│
                       │  lock-free, cache-aligned    │
                       └──────────────────────────────┘
                          │  independent consumer heads
              ┌───────────┴────────────┐
              ▼                        ▼
      DB consumer thread        Strategy thread
      aggregate_bars            sort_consumers → spin-pop
      → create_or_update(DB)    aggregate_bars → dispatch_bar
      → live_prices moka cache  → strategy.on_bar_update (sync)
                                → order_engine.handle_bar_update_outcome
```

- **`spmc-ring`** is a lock-free single-producer-multi-consumer ring (`CacheAligned<AtomicUsize>` heads/tail, power-of-two capacity, `UnsafeCell<Option<T>>` slots). The producer caches the slowest consumer head and only scans all heads when the cached check says full; the consumer caches the tail and only reloads on the empty fast-path — both minimize cross-core cache invalidation. `try_push` failure after 50 retries drops the bar (data-loss over backpressure, deliberately, to keep the producer from stalling ibapi's subscription).
- **Hot spin.** Consumers spin-poll `try_pop()` within a 200ms window around each 5s boundary with `SPIN_BACKOFF=ZERO` (true `std::hint::spin_loop()`). Burns a full core per consumer thread; minimizes bar-to-order latency. `align_and_prime_schedule` anchors the steady-state loop to the **producer's actual bar timestamps**, not wall clock, so the consumer tracks ibapi's cadence rather than drifting.
- **Bar aggregation.** A pure `aggregate_bars` fn drains a `VecDeque<Bar>` into OHLCV bars of `bar_time_width` seconds (300 for stocks, 60 for FX), detecting boundary crossings by comparing the bucket index of the latest bar vs. the next expected bar.
- **FX bid/ask join.** Forex arrives as separate bid and ask streams. `sort_consumers` sorts by `(is_fx, symbol, what_to_show_rank)` with `Bid=0 < Ask=1`, guaranteeing bid is immediately before ask, so the join logic fuses them via O(1) `idx±1` indexing instead of a hash lookup.

## Subsystem tour

### Database & Database Methods

A three-tier design that gets ~98 method bodies (7 operations × ~14 tables) from one template.

- **Proc-macros derive companion key-set structs** from a model's field types. `Option`-ness _is_ the schema metadata: `ExtractFullKeys` (all fields, `Option<T>` unwrapped — the insert shape), `ExtractPrimaryKeys` (non-`Option` fields only — the WHERE-clause identity), `ExtractPrimaryKeysWoTime` (non-`Option` minus `time`/`day`), `ExtractUpdateKeys` (only `Option<T>` fields, kept as `Option` — nullable SET targets). `DeriveInsertable` generates column-name lists and `bind_*` methods; its `opt_column_names()` is runtime-dynamic, so `update()` emits SQL with only the columns actually set — partial updates for free.
- **A generic `CRUD<FK,PK,UK>` + blanket impl** provides all seven CRUD operations for any key-set triple that implements `Insertable`. Two delegation macros layer further: one for single-table wrappers, one for enum dispatch.
- **Interface enums** (`OpenOrdersCRUD`, `CurrentPositionsCRUD`, `TransactionsCRUD`, `TargetPositionsCRUD`, `HistoricalDataCRUD`) dispatch by `AssetType` via a `from(asset_type, pool)` constructor that collapses 7 asset-type variants into 2 storage shapes (futures/CFD/forex/cash share the stock schema). Variant mismatch returns `Err` — a runtime guard for an invariant the type system can't express.
- **Two query tiers coexist.** Generic `CRUDTrait` (runtime, untyped) for simple CRUD; hand-written `sqlx::query_as!` for joins, aggregations, and conditional math (e.g. `update_positions_additive` preserves `avg_price` on opposite-sign fills via a `SIGN` guard — a domain-correct behavior the generic `update` couldn't express).
- **Bulk insert** via a separate high-throughput tier: temp table → binary `COPY IN` → dedup (reverse iteration + `HashSet` = last-write-wins) → `INSERT ... ON CONFLICT DO UPDATE`. A background batching channel flushes on size/time/close triggers, with RAII flush on `Drop`.
- **TimescaleDB.** Intraday bars live in a `historical_data` hypertable; `daily_ohlcv` is a continuous aggregate refreshed on demand. `logs.logs` is a hypertable with 3-day retention.
- **Staged-commission triggers.** A DB-level solution to the executions-vs-commissions event-ordering race: a `staged_commissions` table with a `BEFORE INSERT` trigger that tries to apply to matching transactions and only stages if no match, plus `AFTER INSERT` triggers on the transaction tables. Elegant.

### Execution

`OrderEngine` is deliberately thin — just a `PgPool` and a `tokio::Handle`; `Client` and `OrderStore` are threaded in as method args, so it owns no IBKR lifecycle.

- **Optimistic order rows.** `place_order` fetches `next_order_id()` synchronously, spawns an OS thread to `submit_order` and write a DB row keyed by `perm_id = -1` (a sentinel), and returns the `order_id` _before_ IBKR confirms. The real `OpenOrder::submitted` event later deletes the `-1` placeholder and inserts the real row. Crash-recovery-safe; the small race window is mitigated by defensive double-deletes on cancel.
- **Positional parent attachment.** `references_parent_order: i32` is a _positional index_ into the input `Vec` (`-1` = none, `>=0` = parent's position). O(1) parent-ID resolution, no second pass — brittle to reordering, documented in the struct field.
- **Reconciliation.** `handle_bar_update_outcome` dispatches `BarUpdateOutcome`: `EmitOrders` → submit immediately (fast, no DB read); `PendingDbQuery` → for each asset type, load target-vs-current-vs-open-orders (from both Postgres _and_ the redb store), compute FX attachments, then `on_new_qty_diff_for_strat` which cancels-all / cancels-and-replaces / places-the-delta.
- **FX attachments are pure.** `get_required_fx_attachments` is an associated function (no `&self`) that consumes three `HashMap`s by value and returns `FxAttachments` by value — no I/O. Greedy shortfall satisfaction: for each buy-contract shortfall, iterate `remaining_proceeds` consuming `min(proceeds, shortfall)`, constructing an FX `ForexPair` on `IDEALPRO`. Attachment wiring sets `transmit=false` on the sell and `transmit=true` on the last FX child, producing an IBKR parent-children chain.
- **redb `OrderStore`.** An embedded ACID KV store (postcard-serialized) backing orders that have been computed but can't yet be submitted because their FX precondition hasn't settled. Role: keeps IBKR-unknown orders out of the `OpenOrders` table (which would violate its "rows = live IBKR orders" invariant) while surviving crashes. `LocalOpenOrder.is_from_db` merges DB + redb + broker as three sources of truth for reconciliation.
- **Order-update stream.** A three-thread fan-out — sync subscriber thread (`next_timeout(5s)`) → per-event OS thread calling `blocking_send` into a 1024-cap tokio mpsc → async receiver dispatching to four handlers (`open_order`, `order_status`, `execution`, `commission_report`). A static `AtomicUsize` + `compare_exchange` enforces singleton. `order_status::submitted` is deliberately read-only because `OpenOrder::submitted` is the more reliable write signal.
- **Startup reconciliation.** `SyncerEngine` + `SyncOps` runs three syncs: executions (reuses the _live_ handler over a 10s-bounded subscription — single code path), open orders, and positions (per-currency FX via `account_updates` `CashBalance` with a tolerance; stock/option via `positions_multi`).

### Strategy

- **`StrategyExecutor` trait** carries a heavy super-trait bound (`Ord + PartialOrd + Eq + PartialEq + Clone + Send + Sync`) — a relic of my older design
  - **`strategy_enum!` macro** generates the enum, a `Hash` impl that hashes by `get_name()` (identity = name string; usable as a `HashMap` key), and a full `StrategyExecutor` delegation impl via `match`. Adding a strategy is one line in the macro invocation. **Why a macro-enum instead of `dyn StrategyExecutor`?** `Ord + Eq + Clone` aren't object-safe in a usable way; `async_trait` with `dyn` would heap-box every future and virtualize every call. The enum gives static dispatch and a concrete (non-`Pin<Box>`) future. Trade: closed set, recompile to add.
- **`BarUpdateOutcome`** is a two-path contract strategy → execution. `EmitOrders(Vec<OrderIBKR>)` (fast path — currently reserved, no strategy emits it); `PendingDbQuery(Vec<AssetType>)` ("I wrote target positions; reconcile me").
- **Sync `on_bar_update` + `Handle::block_on`.** The strategy heartbeat runs on a dedicated OS thread (spawned by `hook_strategy`). The sync `on_bar_update` can spawn async DB tasks via `tokio_handle.spawn` join them with `self.tokio_handle.block_on(async { tokio::join!(...) })`.
- **Rolling statistics.** Nine pure rolling-stat structs (`RollingMax/Min/Sum/Std/ZScore/RankPct/EwmMean/Roc/Mean`), `Decimal`-internal for lossless finance math with an f64 façade. `EwmMean` implements pandas' `adjust=False` EWMA and keeps a `prev_prev` snapshot enabling O(1) `replace_last` — recompute the forming bar without replaying the series. `RollingRankPct` uses a `BTreeMap<OrderedFloat, usize>` multiset for sub-linear percentile queries.
- **`proportional_integer_reduce`** solves the indivisible-shares problem: real-valued proportional scaling always under-spends (sum of floors ≤ floor of the sum); this function helps to reduce the number of shares to just below the limit via a greedy method.

### Scheduling & pure helpers

A clean three-tier model, separated by purity:

- **Tier 1 (pure) — broker availability.** `IbkrRegion` / `IbkrStateService`: is the IBKR backend up? Knows only the weekly maintenance window (Fri 23:00 → Sat 03:00 ET). Pure chrono arithmetic; no I/O, no client.
- **Tier 2 (orchestrator) — `run_program`.** Consumes Tier 1 to gate the outer loop; owns the `tokio::select!` over sleep-to-broker-down / connection alerts / SIGTERM / SIGINT.
- **Tier 3 (per-contract) — `IbkrContractScheduler`.** Parses IBKR `liquid_hours`/`trading_hours` strings into a `BTreeMap<NaiveDate, Option<TradingHours>>`, imputes missing days (the FX-Saturday gap case), and `is_trading` checks _yesterday's_ schedule first to catch sessions spanning midnight. The producer sleeps until `get_next_earliest_available_data` when not trading.
- **`HashContract`**: custom `Hash` on a _normalized subset_ of contract fields (`primary_exchange.trim()`, symbol, currency, security_type, option quartet with `OrderedFloat(strike)`); bare `impl Eq` backed by derived raw `PartialEq`. **This is not a `Hash`/`Eq` invariant violation** — the contract only requires `a == b → hash(a) == hash(b)`, and hashing a subset of the eq-compared fields can only coarsen the hash space. The trim is the _safe direction_ of normalization: it future-proofs a loosened `eq` without breaking the invariant today.
- **`sync_timeout`** — pure std-only timeout for sync ibapi calls: spawn OS thread + `mpsc` + `recv_timeout`. Bounds blocking calls without holding a tokio worker. (Cost: the worker thread is leaked on timeout — Rust threads can't be cancelled.)

### Lifecycle & self-healing

- **`run_program` is the outer restart loop.** `AppReturnState` is a typed algebra of every way an iteration can end (broker-down, init errors, four flavors of unstable connection, SIGINT/SIGTERM). Only the two signal variants are terminal; **every other state `continue`s the loop → fresh `with_gateway_retry` → fresh `init_app`**. The app re-boots its entire IBKR stack on connection instability, APAC reset, broken pipe, or broker maintenance.
- **Log-stream-as-control-plane.** `IbConnectionLayer` is a `tracing` `Layer` that inspects every ERROR/WARN event's message for IBKR error codes `1100`/`1102` (stale), `111` (refused), `Broken pipe (os error 32)`, and `timed out waiting for next bar`. It runs a state machine that consults pure datetime predicates (`is_autorestart`, `is_apac_reset_now`, `is_any_open`) to classify the outage and emit `ConnectionAlert`s to `run_program`'s `select!`. The connection state machine is driven entirely by parsing log strings — powerful but fragile (an ibapi string reformat silently breaks detection).
- **Structural `IBGateway` lifecycle safety.** `IBGateway` is intentionally non-constructible externally — only `with_gateway`/`with_gateway_retry` own both halves, making "obtain an `IBGateway` and forget to shut it down" structurally impossible. It spawns the gateway in a process group so descendants die together; failure paths kill+reap the child before returning `Err` (no orphaned processes, no stuck port 4002). `Drop` is a safety net that only SIGKILLs — never `block_on`s (that was the original core bug; the comment calls it out).
- **`test_internals` test seam.** Source functions stay `pub(crate)`; a `#[cfg(any(test, feature = "test-utils"))]` module re-exports `pub` wrappers. A self-dev-dependency (`trading-app = { path = ".", features = ["test-utils"] }`) makes `tests/` binaries auto-build with the feature. Zero prod-code impact; full test access to the pure functions that matter.

## How to run

### Prerequisites

- **Rust** (edition 2024 — Rust 1.85+).
- **PostgreSQL** with **TimescaleDB** enabled.
- **IB Gateway** managed via **IBC** at `<IBC_INSTALL_PATH>` (redact your own path; currently hardcoded).
- The **sync `ibapi` fork**: `rust-ibapi-fork`, branch `ibapi-rtyt` (referenced in `trading-app/Cargo.toml`).
- The **`spmc-ring` submodule** — `git submodule update --init` after clone.

### Environment

The binary reads these env vars (only `DATABASE_URL` is in `.env.example`; the rest are required but undocumented — tracked in debt):

| Var                | Purpose                                                                                |
| ------------------ | -------------------------------------------------------------------------------------- |
| `DATABASE_URL`     | Postgres connection string.                                                            |
| `ORDERS_FILE_PATH` | Path to the redb `OrderStore` file (e.g. `/data/orders/orders.redb`). Panics if unset. |
| `TRADING_TYPE`     | Selects live (`4001`) vs paper (`4002`) IB Gateway port.                               |
| `SERVER_URL`       | Axum bind address (default `0.0.0.0:8000`).                                            |

The paper-trading account id and the IBC install path are currently hardcoded in source (to `<PAPER_ACCOUNT_ID>` / `<IBC_INSTALL_PATH>` placeholders) — see [Known rough edges & debt](#known-rough-edges--debt).

### Build & test

```sh
cargo check --lib                       # type-check the library

SQLX_OFFLINE=true cargo test --test unit_tests          # 275 passing, 3 ignored, no DB/IBKR
DATABASE_URL=… cargo test --test integration_tests       # needs Postgres
cargo test --test smoke_tests -- --ignored              # needs a live IB Gateway
```

Three test binaries: `unit_tests` (pure-logic, offline, the bulk of coverage), `integration_tests` (per-table CRUD + advanced DB ops + bulk insert; needs Postgres), `smoke_tests` (live IB Gateway + Postgres; `#[ignore]`d by default).

## Extending it

### Add a strategy

1. Implement `StrategyExecutor` for a new struct (the trait requires `Ord`/`Eq`/`Clone`/`Send`/`Sync` — hand-write `Ord` as `priority.cmp().then(name.cmp())`).
2. Add one line to the `strategy_enum!` invocation in `strategy/strategy.rs`: `NewVariant(NewStruct)`.
3. Construct it in `init_strategies` (`init_app.rs`) with its `DataSubscription` list and a cloned `tokio::runtime::Handle`.

That's the entire wiring — the macro generates the enum variant, the `Hash`-by-name, and the delegation impl. Strategies are dispatched statically (no `dyn`).

### Add a table

1. Define the model struct in `database/models.rs` with the four key-set derives (`ExtractFullKeys + ExtractPrimaryKeys + ExtractUpdateKeys + DeriveInsertable`) and `FromRow`.
2. Create the table in a new migration under `trading-app/migrations/`.
3. Add a per-table CRUD leaf in `database/models_crud/` (a thin wrapper around `CRUD<…FullKeys, …PrimaryKeys, …UpdateKeys>` + the `implement_all_crud_methods!` macro + a `new(pool)` with the table name).
4. If it follows the stock/option duality, add a variant to the relevant interface enum (`OpenOrdersCRUD`, etc.) and its parallel key enums.

### Use the test seam

Add `#[cfg(any(test, feature = "test-utils"))]` to keep source items `pub(crate)` in prod but reachable from `tests/` via `trading_app::test_internals::…`. The `test-utils` cargo feature + self-dev-dependency auto-enables it for `cargo test`.

## Known rough edges & debt

- **Orphaned/dead files** — `strategy/threshold_rebalancing.rs` (not declared in `mod.rs`; would fail to compile against the current trait; a fossil from a pre-refactor era; its DB table still exists), `database/models_crud/frac_mom_weekly_pos.rs` (references a non-existent macro), `market_data/traits/available_funds.rs` (references fields that no longer exist on `Consolidator`), `market_data/strategy_scheduler.rs` (entirely commented out). ~270 lines of commented legacy in `broker_scheduler.rs`; ~200 lines of abandoned view/trigger DDL in `init.sql`.
- **Forex `read_last_vwap` SQL** references non-existent `volume`/`stock` columns on `historical_forex_data` — copy-paste from the stock branch; would fail at runtime if exercised (not in tests).
- **`IbkrState::async_drop` consolidator spin-loop** is a busy-wait with no sleep (the comment acknowledges an infinite loop) — fine in stable operation, hangs teardown if a clone leaks so this MUST be tracked closely.
- **`sync_timeout` leaks threads on timeout** — Rust threads can't be cancelled; a hung `contract_details` lives indefinitely.
- **Hardcoded deployment specifics** — paper account id, IBC install path, and SGD base currency are in source, not env. Should be `PAPER_ACCOUNT_ID`, `IBC_INSTALL_PATH`, and a configurable base currency. (The literals in this README are redacted to placeholders.)
- **Magic numbers** — `$1000` FX reconciliation tolerance, 15min/60s memoiser TTLs, `MAX_SUB_TRY_TIMES=50`, `HOT_WINDOW=200ms`. Tuned-in-practice constants that could be parameterized; not currently justified by anything, feel free to modify.
- **`order_ref` overloaded as `"{strategy}:{price}"`** for backed-up orders — a stringly-typed contract across two files with `.expect()` on parse.
- **FX calendar** uses a minimal 3-holiday set (New Year's, Christmas, Good Friday via a hand-rolled Computus). `nyse-holiday-cal` (already a dependency for stock calendars) might have been a better foundation; this code was mostly AI-written and is low-stakes.
- **Stalling for permissions**: if one has no permission to get prices or the contract is invalid - for e.g. you don't have permission to get the price of option contracts - get_current_price can hang for up to around 30~45s. Multiply that by a few option contracts and using PendingDbQuery as the bar update outcome with this can very easily exceed the 1 minute threshold set by the FOREX contract subscribed to by unknown/manual/any strategy you build that takes 1 minute bars.
  - In a nutshell, if you encounter "Consumer Stall" error logs, you should first check hotpath-console to see which thread is blocking, and consequently which function in that thread is blocking, then read the logs as well; typically, some consumer thread IS stalling for some reason, typically having to do with get_current_price or an IBKR client side sync function.

## My journey

For the design memoir — the reasoning behind the architecture, the dead ends, and what I learned building this — see **[My journey →](YOUR_JOURNEY_URL)** on my personal page.

---

_Built by `RyanTYT`. Rust · IBKR · PostgreSQL/TimescaleDB · tokio · redb · moka._
