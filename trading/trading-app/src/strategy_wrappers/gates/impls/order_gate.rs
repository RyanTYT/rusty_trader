//! OrderGate — §4 hybrid fill machinery (python `thorough.py`
//! `_process_pending_limits` + `_reconcile_targets` cancel taxonomy,
//! ported gate-side; HYBRID_ROLLOUT.md Section 4).
//!
//! Placement: post-FundGate (sees the trimmed per-share BOOK), pre-SharesGate
//! (`sleeve.rs::with_order_gate`). Every intent arriving here is a per-share
//! book count or a flat — `FundGate`'s completer path (`pooled_intents`)
//! emits nothing else.
//!
//! State model (the held-book convention): the gate keeps ONE working
//! resting entry per (owner, sym) — the ABSOLUTE book target the engine's
//! working LMT converges to. The netting rule is "absence = persist":
//!
//! - target UNCHANGED while resting → emit NOTHING for the sym (the sheet
//!   lacks the entry → `sheet_to_orders` computes delta vs the engine
//!   position only for syms it sees → no re-emit; the resting LMT keeps
//!   working toward the stored target).
//! - target CHANGED (increase/decrease/flip) → cancel the working LMT (via
//!   the `ogcancel-` marker the EmitOrders seam honors) and cross the FULL
//!   target at market. Documented deviation from python's re-place: a
//!   changed target crosses instead of re-resting (re-rests fresh next slot
//!   if the target then stays stable).
//! - TTL expiry → python `limit_then_market`: cancel + cross at market.
//! - FLAT upstream → cancel + pass the flat through stamped with the
//!   cancel marker (the sell-to-flat order doubles as the cancel carrier).
//!
//! Anchors: fresh placements stamp `decision-bar close ± 5bp` (python
//! `limit_k_bp=5`) by INTENT side; `sheet_to_orders` rests the order as LMT
//! only when the ORDER delta side matches the anchor side (reduces/flips
//! cross at market — python: exits never rest).
//!
//! SHARED state (thread-local, `fund_gate::FUND_LEDGER` pattern): in pooled
//! mode the emitting chain ROTATES (only the completing sleeve emits), so
//! the resting map must be module-level, keyed (owner, sym). The engine is
//! single-thread-class (all sleeves flush sequentially on one replay
//! thread) — safe. Known §4 scope note: anchors are stamped off the
//! COMPLETER's frame, so only the completing sleeve's own-universe entries
//! rest; foreign syms (absent from that frame) cross at market.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::strategy_wrappers::gate::GateLogic;
use crate::strategy_wrappers::intent::{Intent, IntentMap};
use crate::strategy_wrappers::payload::{
    BarFrame, OG_CANCEL_PREFIX, OG_PREFIX, Payload, RealisedFill,
};
use crate::strategy_wrappers::strategy::ClockCtx;

/// Passive-limit offset off the decision-bar close (python `limit_k_bp=5`).
pub const OG_LIMIT_K_BP: f64 = 5.0;

/// One working resting entry: the ABSOLUTE book target the engine's LMT
/// converges to, the anchor it rests at, the print-gated TTL, and the emit
/// descriptor (fill-join + engine cancel key).
#[derive(Debug, Clone)]
pub struct RestingEntry {
    pub target: i64,
    pub limit_px: f64,
    pub ttl: u8,
    pub oca_group: String,
}

thread_local! {
    /// (owner, sym) → working resting entry. Shared across ALL OrderGate
    /// instances (single replay thread; completer-rotation-safe).
    static RESTING: RefCell<HashMap<(String, String), RestingEntry>> =
        RefCell::new(HashMap::new());
}

#[derive(Debug, Clone)]
pub struct OrderGate {
    /// Owning sleeve's engine strategy name — resolves `strategy: None`.
    own: String,
    /// Fresh placements get this TTL (python `limit_ttl=3`).
    ttl: u8,
    /// Monotonic emit-descriptor counter (unique per process).
    seq: u64,
    /// Hybrid flag (construction-time run param `RunParams.hyb`, formerly
    /// the `BLENDED5_HYB` env seam). OFF = byte-identical
    /// pass-through: legacy PendingDbQuery/emit arms never see limit
    /// intents and no resting state is recorded.
    active: bool,
}

impl OrderGate {
    pub fn new(own: String, ttl: u8, active: bool) -> Self {
        Self { own, ttl, seq: 0, active }
    }

    fn next_oca(&mut self) -> String {
        self.seq += 1;
        format!("{OG_PREFIX}{}-{}", self.own, self.seq)
    }

    /// Decision-bar close ± k_bp, passive side by INTENT direction. `None`
    /// when the sym has no printable bar (→ market pass-through, nothing
    /// recorded — python skip-no-print).
    fn anchor_for(side: i32, bars: &BarFrame, sym: &str) -> Option<f64> {
        let px = bars
            .get(sym)
            .map(|b| b.get_price())
            .filter(|p| p.is_finite() && *p > 0.0)?;
        let k = OG_LIMIT_K_BP / 10_000.0;
        // PASSIVE side (python k5bp geometry): a BUY rests BELOW the close
        // (fills only if price dips — price improvement), a SELL rests ABOVE
        // (fills only if price rallies). Engine fills AT the limit when the
        // passive edge is touched, so the resting fill price is favourable
        // by construction. (Buy-at-close+k would be marketable-above: every
        // fill executes at a 5bp premium over where the market traded —
        // the inverted-anchor bug that cost the first hybrid parity run.)
        Some(if side > 0 { px * (1.0 - k) } else { px * (1.0 + k) })
    }

    /// The per-share book target this intent converts to downstream
    /// (`SharesGate` truncation association: (n_base * w) * sc, trunc).
    fn book_target(it: &Intent) -> i64 {
        (it.n_base * it.w * it.sc).trunc() as i64 * it.side as i64
    }

    /// Drop every entry sharing `oca_group` (OCA cascade, gate-side).
    fn cancel_oca(oca_group: &str) {
        if oca_group.is_empty() {
            return;
        }
        RESTING.with(|m| {
            m.borrow_mut().retain(|_, e| e.oca_group != oca_group);
        });
    }
}

impl GateLogic for OrderGate {
    fn on_bar_update(
        &mut self,
        up: Option<&Payload>,
        bars: &BarFrame,
        _ctx: &ClockCtx,
    ) -> Result<Payload, String> {
        let items = match up {
            Some(Payload::Intents(v)) => v,
            Some(Payload::Shares(_)) => {
                return Err("OrderGate consumes Intents, not Shares".into())
            }
            None => return Ok(Payload::Intents(Vec::new())),
        };
        if !self.active {
            return Ok(Payload::Intents(items.clone()));
        }
        let mut out: IntentMap = Vec::with_capacity(items.len());
        for (sym, it_ref) in items.iter() {
            let sym = sym.clone();
            let it = it_ref.clone();
            let owner = it.strategy.clone().unwrap_or_else(|| self.own.clone());
            let key = (owner.clone(), sym.clone());

            // ── 1) FLAT upstream → cancel the working resting (+OCA); the
            //       flat rides through stamped with the cancel marker so
            //       the sell-to-flat order doubles as the engine cancel. ──
            if it.side == 0 {
                if let Some(e) = RESTING.with(|m| m.borrow_mut().remove(&key)) {
                    let marker = format!("{OG_CANCEL_PREFIX}{}", e.oca_group);
                    Self::cancel_oca(&e.oca_group);
                    out.push((sym.clone(), Intent { oca_group: Some(marker), ..it }));
                } else {
                    out.push((sym.clone(), it));
                }
                continue;
            }
            // Non-per-share intents cannot be anchored/netted gate-side —
            // pass through untouched (post-FundGate streams are per-share;
            // this arm is defensive).
            if !it.per_share {
                out.push((sym.clone(), it));
                continue;
            }
            let t = Self::book_target(&it);

            // ── 2) print-gated TTL aging on the working entry (own syms age
            //       only when they printed; foreign syms may be absent from
            //       the completer's frame — age per completing slot, same
            //       daily bar rate across universes). ──
            RESTING.with(|m| {
                if let Some(e) = m.borrow_mut().get_mut(&key) {
                    if bars.contains_key(sym.as_str()) || owner != self.own {
                        e.ttl = e.ttl.saturating_sub(1);
                    }
                }
            });
            let entry = RESTING.with(|m| m.borrow_mut().remove(&key));

            match entry {
                None => {
                    // ── 3) fresh placement — stamp the decision-bar anchor
                    //       and record. Unprintable → market, nothing
                    //       recorded. ──
                    match Self::anchor_for(it.side, bars, &sym) {
                        Some(px) => {
                            let oca = self.next_oca();
                            RESTING.with(|m| {
                                m.borrow_mut().insert(
                                    key,
                                    RestingEntry {
                                        target: t,
                                        limit_px: px,
                                        ttl: self.ttl,
                                        oca_group: oca.clone(),
                                    },
                                );
                            });
                            out.push((
                                sym.clone(),
                                Intent { limit_px: Some(px), oca_group: Some(oca), ..it },
                            ));
                        }
                        None => out.push((sym.clone(), it)),
                    }
                }
                Some(e) => {
                    if e.ttl == 0 {
                        // ── 4) TTL expiry = python limit_then_market:
                        //       cancel the resting LMT and cross the FULL
                        //       target at market (limit stripped). ──
                        let marker = format!("{OG_CANCEL_PREFIX}{}", e.oca_group);
                        Self::cancel_oca(&e.oca_group);
                        out.push((
                            sym.clone(),
                            Intent { limit_px: None, oca_group: Some(marker), ..it },
                        ));
                    } else if t == e.target {
                        // ── 5) idempotent persist — emit NOTHING for this
                        //       sym; the resting LMT keeps working. Put the
                        //       (aged) entry back. ──
                        RESTING.with(|m| {
                            m.borrow_mut().insert(key, e);
                        });
                    } else {
                        // ── 6) target changed (increase/decrease/flip) →
                        //       cancel the working LMT and cross at market.
                        //       Re-rests fresh next slot if the target then
                        //       stays stable (documented deviation from
                        //       python's re-place). ──
                        let marker = format!("{OG_CANCEL_PREFIX}{}", e.oca_group);
                        Self::cancel_oca(&e.oca_group);
                        out.push((
                            sym.clone(),
                            Intent { limit_px: None, oca_group: Some(marker), ..it },
                        ));
                    }
                }
            }
        }
        Ok(Payload::Intents(out))
    }

    /// Fill join: a fill carrying one of OUR descriptors drops that entry
    /// (all-or-nothing fills — no partial-fill remainder) and OCA-collapses
    /// its siblings. GateEnum broadcasts EVERY sleeve's fills here
    /// (head-first, never consumed) — only `og-`-prefixed groups are ours.
    fn on_realised(&mut self, fills: &[RealisedFill], _ctx: &ClockCtx) {
        for f in fills {
            if !f.oca_group.starts_with(OG_PREFIX) {
                continue;
            }
            Self::cancel_oca(&f.oca_group);
        }
    }
}

#[cfg(test)]
mod order_gate_tests {
    use super::*;
    use crate::database::models::HistoricalStockDataFullKeys;
    use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;
    use chrono::{DateTime, NaiveDate, Utc};
    use rust_decimal::Decimal;

    fn frame(closes: &[(&str, f64)]) -> BarFrame {
        closes
            .iter()
            .map(|(sym, c)| {
                (
                    sym.to_string(),
                    HistoricalDataFullKeys::Stock(HistoricalStockDataFullKeys {
                        stock: sym.to_string(),
                        primary_exchange: "SMART".into(),
                        currency: "USD".into(),
                        time: DateTime::<Utc>::from_timestamp(1_800_000_000, 0).unwrap(),
                        open: *c,
                        high: *c,
                        low: *c,
                        close: *c,
                        volume: Decimal::ZERO,
                    }),
                )
            })
            .collect()
    }

    fn ctx() -> ClockCtx {
        ClockCtx {
            et_date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            mso: 100,
            scale: 1.0,
            day_last: false,
            close_window: false,
            own_eq_sgd: 0.0,
        }
    }

    /// Book-count intent: per-share |q| shares at w=sc=1 (the post-FundGate
    /// emission shape).
    fn book(sym_side: i32, qty: f64) -> Intent {
        Intent::per_share(sym_side, qty, 1.0)
    }

    fn entry(target: i64, side: i32) -> Intent {
        Intent::per_share(side, target.abs() as f64, 1.0)
    }

    fn run(
        g: &mut OrderGate,
        items: IntentMap,
        bars: &BarFrame,
    ) -> Result<Payload, String> {
        GateLogic::on_bar_update(g, Some(&Payload::Intents(items)), bars, &ctx())
    }

    fn intents_of(p: &Payload) -> IntentMap {
        match p {
            Payload::Intents(v) => v.clone(),
            _ => panic!("expected Intents"),
        }
    }

    fn resting_get(owner: &str, sym: &str) -> Option<RestingEntry> {
        RESTING.with(|m| {
            m.borrow()
                .get(&(owner.to_string(), sym.to_string()))
                .cloned()
        })
    }

    #[test]
    fn off_path_is_byte_passthrough() {
        let mut g = OrderGate::new("synth_a".into(), 3, false);
        let bars = frame(&[("AAPL", 100.0)]);
        let items = vec![("AAPL".into(), entry(20, 1))];
        let out = intents_of(&run(&mut g, items.clone(), &bars).unwrap());
        assert_eq!(out, items);
        assert!(resting_get("synth_a", "AAPL").is_none());
    }

    #[test]
    fn fresh_placement_stamps_anchor_and_records() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap(),
        );
        let (_, it) = &out[0];
        let px = it.limit_px.expect("anchor stamped");
        // passive side: BUY rests BELOW the close (entry(side=1) → close×(1−k))
        assert!((px - 100.0 * (1.0 - 5.0 / 10_000.0)).abs() < 1e-9);
        assert!(it.oca_group.as_deref().unwrap().starts_with(OG_PREFIX));
        let e = resting_get("synth_a", "AAPL").expect("entry recorded");
        assert_eq!(e.target, 20);
        assert_eq!(e.ttl, 3);
        assert_eq!(e.oca_group, it.oca_group.clone().unwrap());
    }

    #[test]
    fn idempotent_target_emits_nothing() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap();
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap(),
        );
        assert!(out.is_empty(), "unchanged target → absence = persist");
        assert!(resting_get("synth_a", "AAPL").is_some());
    }

    #[test]
    fn target_change_cancels_and_crosses_at_market() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap();
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), entry(35, 1))], &bars).unwrap(),
        );
        let (_, it) = &out[0];
        assert!(it.limit_px.is_none(), "changed target crosses at market");
        assert!(it
            .oca_group
            .as_deref()
            .unwrap()
            .starts_with(OG_CANCEL_PREFIX));
        assert!(resting_get("synth_a", "AAPL").is_none(), "entry removed");
    }

    #[test]
    fn ttl_expiry_flips_to_market() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap();
        // 3 print slots after placement: ages 3→2→1, still resting.
        for _ in 0..2 {
            let out = intents_of(
                &run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap(),
            );
            assert!(out.is_empty());
        }
        assert!(resting_get("synth_a", "AAPL").is_some());
        // 3rd print → ttl 0 → expiry: market cross with the cancel marker.
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap(),
        );
        let (_, it) = &out[0];
        assert!(it.limit_px.is_none());
        assert!(it
            .oca_group
            .as_deref()
            .unwrap()
            .starts_with(OG_CANCEL_PREFIX));
        assert!(resting_get("synth_a", "AAPL").is_none());
    }

    #[test]
    fn no_print_does_not_age_own_entry() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap();
        // AAPL absent from this frame → no print → no aging.
        let empty = frame(&[]);
        for _ in 0..5 {
            let out = intents_of(
                &run(&mut g, vec![("AAPL".into(), entry(20, 1))], &empty).unwrap(),
            );
            assert!(out.is_empty());
        }
        let e = resting_get("synth_a", "AAPL").expect("still resting");
        assert_eq!(e.ttl, 3, "unprinted slots never age the TTL");
    }

    #[test]
    fn flat_upstream_cancels_with_marker() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap();
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), Intent::flat())], &bars).unwrap(),
        );
        let (_, it) = &out[0];
        assert_eq!(it.side, 0);
        assert!(it
            .oca_group
            .as_deref()
            .unwrap()
            .starts_with(OG_CANCEL_PREFIX));
        assert!(resting_get("synth_a", "AAPL").is_none());
    }

    #[test]
    fn fill_join_drops_entry_by_oca_group() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap(),
        );
        let grp = out[0].1.oca_group.clone().unwrap();
        let fills = vec![RealisedFill {
            sym: "AAPL".into(),
            qty: 20,
            px: 100.05,
            date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            order_id: 1,
            oca_group: grp,
        }];
        GateLogic::on_realised(&mut g, &fills, &ctx());
        assert!(resting_get("synth_a", "AAPL").is_none());
    }

    #[test]
    fn foreign_fills_are_ignored() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        run(&mut g, vec![("AAPL".into(), entry(20, 1))], &bars).unwrap();
        let fills = vec![RealisedFill {
            sym: "AAPL".into(),
            qty: 20,
            px: 100.05,
            date: NaiveDate::from_ymd_opt(2026, 1, 5).unwrap(),
            order_id: 1,
            oca_group: String::new(),
        }];
        GateLogic::on_realised(&mut g, &fills, &ctx());
        assert!(resting_get("synth_a", "AAPL").is_some());
    }

    #[test]
    fn foreign_owner_keys_separately() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        let foreign = entry(20, 1);
        let foreign = Intent { strategy: Some("synth_b".into()), ..foreign };
        run(&mut g, vec![("AAPL".into(), foreign)], &bars).unwrap();
        assert!(resting_get("synth_a", "AAPL").is_none());
        assert!(resting_get("synth_b", "AAPL").is_some());
    }

    #[test]
    fn non_per_share_passes_through() {
        let mut g = OrderGate::new("synth_a".into(), 3, true);
        let bars = frame(&[("AAPL", 100.0)]);
        let dollars = Intent::long(2000.0, 1.0);
        let out = intents_of(
            &run(&mut g, vec![("AAPL".into(), dollars)], &bars).unwrap(),
        );
        assert_eq!(out[0].1.per_share, false);
        assert!(out[0].1.limit_px.is_none());
        assert!(resting_get("synth_a", "AAPL").is_none());
    }
}
