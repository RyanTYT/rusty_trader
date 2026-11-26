//! Tests for `src/market_data/consumer/db_consumer.rs`.
//!
//! This module has **no pure, callable surface** — every public item either
//! spawns an OS thread that talks to Postgres or needs a live `ibapi::Client`.
//! Only the branch inputs it feeds into the historical-data tables are asserted
//! here.
//!
//! Blockers (path:line):
//!   - `begin_db_consumer_thread_singular`
//!     (src/market_data/consumer/db_consumer.rs:65) — signature takes `PgPool`,
//!     `Arc<IbkrContractScheduler>`, `IbkrBarConsumer`, `Cache` and a
//!     `tokio::runtime::Handle`; the spawned closure (:83) calls
//!     `align_and_prime_schedule` (:87-89) whose scheduler requires
//!     `Arc<Client>` (src/schedule/contract_scheduler.rs:60).
//!   - `begin_db_consumer_thread_grouped`
//!     (src/market_data/consumer/db_consumer.rs:182) — same dependencies; it
//!     also panics on an empty `consumers` vec at :209-218.
//!   - `MarketDataDbConsumer` (:33) — fields `is_alive` / `thread_handle` are
//!     private and there is no constructor, so `Drop` (:58) / `async_drop`
//!     (:39) shutdown flags are not observable.
//!   - the write path `DbDirect::connect` + `historical_data_crud
//!     .create_or_update(..)` (:129-146 and :312-327) — needs a real `PgPool`.
//!   - granularity selection is inline rather than a helper: `Normal => 60`,
//!     anything else => 300 at :106-109 and :283-286. It is asserted indirectly
//!     through `IbkrBarConsumer::get_bar_type`
//!     (tests/market_data/consumer/strategy_consumer.rs).
//!   - `sleep_until_system_time` (:376) — private.
//!
//! What *is* pure and asserted below: `AssetType::from_str(&SecurityType)`
//! (:113-114 / :290-292) — the switch that decides which historical table
//! (stock / option / forex) a bar is written to. The remaining per-bar
//! transforms, `HistoricalDataPrimaryKeys::from_contract` (:115-119) and
//! `HistoricalDataUpdateKeys::from_bar` (:120-124), are fed directly by
//! `aggregate_bars` output covered in `consumer/helper.rs`.

use ibapi::contracts::SecurityType;
use trading_app::database::models::AssetType;

#[test]
fn asset_type_from_security_type_table() {
    let cases: &[(SecurityType, AssetType)] = &[
        (SecurityType::Stock, AssetType::Stock),
        (SecurityType::Future, AssetType::Future),
        (SecurityType::ForexPair, AssetType::ForexPair),
        (SecurityType::CFD, AssetType::CFD),
        (SecurityType::Option, AssetType::Option),
    ];

    for (security_type, expected) in cases {
        assert_eq!(
            AssetType::from_str(security_type),
            *expected,
            "AssetType::from_str({security_type:?})"
        );
    }
}

#[test]
fn unsupported_security_types_fall_back_to_unknown() {
    // db_consumer only ever sees subscription contracts, but the fallback at
    // src/database/models.rs:100-105 must not classify an unsupported type as
    // a writable historical table.
    for security_type in [
        SecurityType::Index,
        SecurityType::Bond,
        SecurityType::Warrant,
        SecurityType::Spread,
        SecurityType::ContinuousFuture,
    ] {
        assert_eq!(
            AssetType::from_str(&security_type),
            AssetType::Unknown,
            "expected Unknown for {security_type:?}"
        );
    }
}

#[test]
fn asset_type_from_security_type_is_pure_and_repeatable() {
    for security_type in [
        SecurityType::Stock,
        SecurityType::Option,
        SecurityType::ForexPair,
        SecurityType::Future,
        SecurityType::CFD,
    ] {
        let first = AssetType::from_str(&security_type);
        let second = AssetType::from_str(&security_type);
        assert_eq!(first, second, "no hidden state for {security_type:?}");
    }
}
