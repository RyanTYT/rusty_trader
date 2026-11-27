//! Tests for `src/init_app.rs`.
//!
//! Covered here (deterministic, no IB socket, no Postgres connection):
//!   - `StrategyParameters` construction + the `strategy_parameters` test
//!     constructor (`src/lib.rs:174-182`) and its `subscribed_contracts`
//!     plumbing, including `DataSubscription` identity (Hash/Eq) because the
//!     per-contract maps below de-duplicate on exactly that key
//!     (`src/market_data/handler.rs:51-117`).
//!   - The strategy roster's `get_strategy_details` values
//!     (`StrategyDetails::{name,is_fx_strategy,hooks_execution}`, plus the
//!     implicit private `priority == 1` checked via derived `PartialEq`) and
//!     the `StrategyDetails` ordering used for contract de-confliction
//!     (`src/execution/syncer.rs:74-78`). `Manual`/`Unknown` are constructed
//!     directly; the four sleeve executors are covered by a deliberate
//!     `#[ignore]` because `SleeveExecutor::build` panics in debug builds at
//!     `src/strategy_wrappers/gates/impls/mpsc.rs:154` (known bug).
//!   - The strategy-name → metric-label helper
//!     (`StrategyEnum::metric_label`, `src/strategy/strategy.rs:345`), the
//!     live roster's only pure strategy-name parser/selector.
//!   - `pg_connect_options` (`src/init_app.rs:43-49`): `DATABASE_URL`
//!     parsing, the `MEMORY_LIMIT` → `trading_app.env` GUC forwarding, its
//!     `small` fallback, and the panic on an unparseable URL.
//!
//! Deliberately NOT covered — needs a live IB client and/or Postgres:
//!   - `init_strategies` (`src/init_app.rs:114-330`): `DbDirect::connect`
//!     (`:121`, `:315`) and `strategy.get_contracts(client)`
//!     (`:188-227`, live contracts). The deterministic outputs of that
//!     function that do not need the client (names/details/funds) are
//!     re-derived here by constructing the same strategies directly.
//!   - `connect_to_client_with_retry` (`src/init_app.rs:333-360`): opens a
//!     TWS socket.
//!   - `init_app` (`src/init_app.rs:362-620`): connects to IB (`:376`),
//!     opens `OrderStore` (`:391-392`), writes to Postgres, syncs orders and
//!     funds strategies.
//!   - The contract→strategy map itself (`SyncerEngine::new`,
//!     `src/execution/syncer.rs:57-95`): the map is a private field with no
//!     accessor, so only its key identity (`DataSubscription`/`HashContract`)
//!     and its comparator (`StrategyDetails::Ord`) are testable externally.
//!     `SyncerEngine::new` also takes a `PgPool`, but a lazy pool is
//!     side-effect free; the blocker is field visibility, not the pool.

mod config;
mod strategy_parameters;

use sqlx::postgres::PgPoolOptions;

/// A pool that never opens a connection: `connect_lazy` only stores the
/// connect options. `max_lifetime`/`idle_timeout` are disabled so sqlx does
/// not spawn its maintenance task (which needs a Tokio runtime; this keeps
/// the non-async tests runtime-free).
fn lazy_pool() -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(1)
        .max_lifetime(None)
        .idle_timeout(None)
        .connect_lazy("postgres://test:test@127.0.0.1:1/never_connected")
        .expect("lazy pool URL must parse")
}
