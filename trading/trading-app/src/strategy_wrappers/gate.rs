//! The gate contract + enum (v2 restructure — ONE file holds the trait, the
//! macro and the single `GateEnum` registration).
//!
//! What a Gate is: a PURE transform in a chain tree. `on_bar_update(up, bars,
//! ctx)` with `up: None` = produce (head), `Some(up)` = transform the upstream
//! emission. `on_realised` receives engine-cleared fills (routed by the fund's
//! `on_fill`, NEVER inferred) — default no-op.
//!
//! What the enum is: `GateEnum` is a TREE (`Chain(head, tail)`, both boxed).
//! The nested calling is injected in ONE place — the `Chain` arm below — so
//! gates never route each other and stay reusable/composable. Chains are
//! built receiver-first: `head.chain(w1).chain(w2)` evaluates head → w1 → w2.
//!
//! Execution: `chain::run_chain` drives a whole chain per bar; the tail MUST
//! be `SharesGate` (type-level safety net at the runner).
//!
//! Registration: one variant line per gate in the `strategy_gate_enum!`
//! invocation below (mirrors `strategy_enum!` in `strategy/strategy.rs`);
//! box heavy structs AT the call site.

pub use super::gates::impls::shares::SharesGate;
pub use super::payload::{BarFrame, Payload, RealisedFill};
pub use super::strategy::{BasicStrategy, ClockCtx};
pub use super::intent::{Intent, IntentMap};

/// Pure-logic gate (Python `Gate`). Chained only through `GateEnum::chain`.
pub trait GateLogic {
    /// `up == None` = "produce an emission" (chain head); `Some` = transform.
    fn on_bar_update(
        &mut self,
        up: Option<&Payload>,
        bars: &BarFrame,
        ctx: &ClockCtx,
    ) -> Result<Payload, String>;

    /// State update from engine-cleared fills. Default: no-op.
    /// Gates must not consume the events (same fills forwarded to every gate).
    fn on_realised(&mut self, _fills: &[RealisedFill], _ctx: &ClockCtx) {}
}

/// Generates `GateEnum` + `chain()` + `GateLogic` dispatch + per-variant
/// `From` impls. Registration = ONE variant line.
#[macro_export]
macro_rules! strategy_gate_enum {
    ($($variant:ident($type:ty)),* $(,)?) => {
        pub enum GateEnum {
            /// (head-applying-first, tail) — BOTH boxed (recursive enum; also
            /// mirrors the settled v2 receiver-first design note). The nested
            /// calling lives ONLY here — the Chain arm of the two `GateLogic`
            /// match blocks below.
            Chain(Box<GateEnum>, Box<GateEnum>),
            $($variant($type)),*
        }

        impl GateEnum {
            /// Append w AFTER self ("transform by me first, then chain down").
            /// The last-chained gate is the outermost layer applied LAST.
            pub fn chain(self, w: impl Into<GateEnum>) -> GateEnum {
                GateEnum::Chain(Box::new(self), Box::new(w.into()))
            }
        }

        impl crate::strategy_wrappers::GateLogic for GateEnum {
            fn on_bar_update(
                &mut self,
                up: Option<&crate::strategy_wrappers::payload::Payload>,
                bars: &crate::strategy_wrappers::payload::BarFrame,
                ctx: &crate::strategy_wrappers::strategy::ClockCtx,
            ) -> Result<crate::strategy_wrappers::payload::Payload, String> {
                match self {
                    // nested calling injected HERE, once
                    GateEnum::Chain(h, t) => {
                        let mid = crate::strategy_wrappers::GateLogic::on_bar_update(&mut **h, up, bars, ctx)?;
                        crate::strategy_wrappers::GateLogic::on_bar_update(&mut **t, Some(&mid), bars, ctx)
                    }
                    $(GateEnum::$variant(g) => crate::strategy_wrappers::GateLogic::on_bar_update(g, up, bars, ctx),)*
                }
            }

            fn on_realised(&mut self, fills: &[crate::strategy_wrappers::payload::RealisedFill], ctx: &crate::strategy_wrappers::strategy::ClockCtx) {
                match self {
                    GateEnum::Chain(h, t) => {
                        crate::strategy_wrappers::GateLogic::on_realised(&mut **h, fills, ctx); // head-first, never consumed
                        crate::strategy_wrappers::GateLogic::on_realised(&mut **t, fills, ctx);
                    }
                    $(GateEnum::$variant(g) => crate::strategy_wrappers::GateLogic::on_realised(g, fills, ctx),)*
                }
            }
        }

        $(impl From<$type> for GateEnum {
            fn from(g: $type) -> Self {
                GateEnum::$variant(g)
            }
        })*
    };
}

strategy_gate_enum! {
    // Head: a concrete `BasicStrategy` (used AS the head — no adapter layer).
    // Heads ignore `up` and PRODUCE via `BasicStrategy::on_row`.
    BasicStrategy(Box<dyn BasicStrategy>),
    // P3 streaming builders: sleeves OWN their layer struct in their own
    // enhancement file (no cross-theme centralization); this slot is the
    // registration point only. Boxed dyn — the enum never introspects it.
    Dyn(Box<dyn GateLogic + Send + Sync>),
    // Tail conversion, single battery truncation — chains MUST end here.
    Shares(SharesGate),
    // §8 P2: the fund-level cap gate — ONE instance per sleeve, chained
    // after the sleeve's weight gates and before its Shares tail (sleeve.rs).
    FundGate(crate::strategy_wrappers::gates::impls::fund_gate::FundGate),
    // §4 hybrid: resting-limit persistence + print-gated TTL + OCA-aware
    // cancels — ONE instance per sleeve, chained AFTER FundGate, BEFORE the
    // Shares tail (sleeve.rs `with_order_gate`). Env-gated off by default
    // (BLENDED5_HYB); off-path is a byte-identical pass-through.
    OrderGate(crate::strategy_wrappers::gates::impls::order_gate::OrderGate),
}

/// Convenience constructor for the `BasicStrategy` head variant.
pub fn basic(s: impl BasicStrategy + 'static) -> GateEnum {
    GateEnum::BasicStrategy(Box::new(s))
}

/// Introspection: a mutable handle to the chain HEAD as `dyn Any`, when the
/// head is a boxed `BasicStrategy`. Sleeves use this to downcast to their own
/// strategy type and install warm-up-recomputed grids without adding trait
/// methods. Recurses through `GateEnum::Chain`; exhausts at other variants.
impl GateEnum {
    pub fn head_any_mut(&mut self) -> Option<&mut dyn std::any::Any> {
        match self {
            GateEnum::Chain(h, _) => h.head_any_mut(),
            GateEnum::BasicStrategy(b) => Some(b.as_mut() as &mut dyn std::any::Any),
            _ => None,
        }
    }
}


/// Head contract: heads ignore the upstream emission (`up`) — they PRODUCE.
impl GateLogic for dyn BasicStrategy {
    fn on_bar_update(
        &mut self,
        _up: Option<&Payload>,
        bars: &BarFrame,
        ctx: &ClockCtx,
    ) -> Result<Payload, String> {
        BasicStrategy::on_row(self, bars, ctx).map(Payload::Intents)
    }

    fn on_realised(&mut self, fills: &[RealisedFill], ctx: &ClockCtx) {
        BasicStrategy::on_realised(self, fills, ctx)
    }
}

/// Blanket forward for boxed gate registrations (`Dyn(Box<..>)` etc.) —
/// `Box<T>` delegates to `T` for every `GateLogic` method.
impl<T: GateLogic + ?Sized> GateLogic for Box<T> {
    fn on_bar_update(
        &mut self,
        up: Option<&Payload>,
        bars: &BarFrame,
        ctx: &ClockCtx,
    ) -> Result<Payload, String> {
        GateLogic::on_bar_update(self.as_mut(), up, bars, ctx)
    }

    fn on_realised(&mut self, fills: &[RealisedFill], ctx: &ClockCtx) {
        GateLogic::on_realised(self.as_mut(), fills, ctx)
    }
}
