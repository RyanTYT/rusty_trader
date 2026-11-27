//! StrategyGate — higher-order strategy transforms (wrapper chain), v2.
//!
//! Rust mirror of `stratlab/stratlab/strategy_wrappers/base.py` (v2 revision,
//! DESIGN SETTLED 2026-10-23, user-approved) — RESTRUCTURED 2026-10-25:
//!
//! - `payload.rs` — `BarFrame` (app-native bars), `Payload`, `RealisedFill`.
//! - `intent.rs` — `Intent` + `shares_from_intents` (battery-exact, unchanged).
//! - `gate.rs` — `GateLogic` trait, `strategy_gate_enum!`, the ONE `GateEnum`
//!   registration + `BasicStrategy` head wiring.
//! - `strategy.rs` — `BasicStrategy` base contract + `ClockCtx` (push-model
//!   context; the deleted `GateCtx` trait's replacement: the fund pushes
//!   date/mso/scale, gates never pull).
//! - `chain.rs` — `run_chain` free function: the forward driver; fill
//!   feedback enters EXCLUSIVELY via the fund's `on_fill` →
//!   `GateLogic::on_realised` (no inferred fills, no memo).
//! - `gates/` — concrete gates grouped: `shares`, `carry`,
//!   `builders` (streaming map builders, P3).
//!
//! The engine-facing fund gate builds one chain per sleeve and drives it via
//! `run_chain`; `GateEnum` NEVER implements StrategyExecutor.
//!
//! ## Contract
//!
//! - **Pure transforms.** [`GateLogic::on_bar_update`](crate::strategy_wrappers::gate::GateLogic::on_bar_update) takes the upstream
//!   emission (`None` at the head) and returns either intents or shares; it
//!   performs no I/O and never reads global state.
//! - **One truncation.** Gates size via [`Intent`](crate::strategy_wrappers::intent::Intent)s and the chain tail
//!   (`SharesGate`) performs the single share truncation; [`run_chain`](crate::strategy_wrappers::chain::run_chain)
//!   rejects a chain that emits intents at the tail.
//! - **Engine-cleared fills only.** Feedback enters exclusively through the
//!   fund's `on_fill` → [`GateLogic::on_realised`](crate::strategy_wrappers::gate::GateLogic::on_realised); nothing infers fills from
//!   position deltas.
//! - **One nesting point.** [`GateEnum::Chain`](crate::strategy_wrappers::gate::GateEnum::Chain) is the only place a gate calls
//!   another gate, so individual gates stay reusable and never route each
//!   other.
//! - **The enum is not a strategy.** `GateEnum` is driven by the fund; it
//!   never implements `StrategyExecutor` itself.

pub mod chain;
pub mod gate;
pub mod gates;
pub mod intent;
pub mod payload;
pub mod strategy;


pub use chain::run_chain;
pub use gate::{basic, GateEnum, GateLogic};
pub use intent::{Intent, IntentMap, shares_from_intents};
pub use payload::{BarFrame, Payload, RealisedFill};
pub use strategy::{BasicStrategy, ClockCtx};
