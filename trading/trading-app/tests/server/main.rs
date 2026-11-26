//! Tests for the axum HTTP layer in `src/server/server.rs`.
//!
//! Everything here is deterministic and offline: no IB socket, no Postgres, no
//! external network. The only sockets used are loopback listeners on
//! OS-assigned ephemeral ports.
//!
//! Module map:
//!   - `types`  — serde/JSON contract of `CurrencyVal`, `Currencies`,
//!                `StrategyValue` and `StrategyQuery`.
//!   - `query`  — real `axum::extract::Query` parsing of `StrategyQuery` and
//!                `Currencies` (percent-decoding, repeated/unknown/missing
//!                params, invalid encodings).
//!   - `server` — `init_server`/`ServerHandle` lifecycle (bind, local_addr,
//!                health, drop/shutdown port release, synchronous bind errors)
//!                plus the exact state-independent responses of every route.
//!
//! Not reachable without IB/Postgres (see the report in `server.rs`):
//!   - happy paths of `/contract/price`, `/contracts/stock`,
//!     `/exchange_rate`, `/strategy/capital` — all four call
//!     `extract_application_state` (`src/server/server.rs:77`) and then need a
//!     live `Consolidator`/IB `Client`.
//!   - `check_health`'s "connected to IBKR" branch
//!     (`src/server/server.rs:393`) needs `client.is_connected() == true`.

mod query;
mod server;
mod types;
