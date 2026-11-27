//! Shared submodules of the gate set (v2 restructure — grouped under `gates/`).
//!
//! - `helpers`: general weight-derivation PRIMITIVES shared across gate
//!   implementations (battery-exact: quintile bands + `WQ`, fused
//!   prod-clip + `PROD_CLIP`, streaming severity, fold-refit machinery).
//! - `impls`: gate IMPLEMENTATIONS (all `GateLogic`-impl files live there —
//!   per-bar applications + feature-specific fold layers).

pub mod impls;

