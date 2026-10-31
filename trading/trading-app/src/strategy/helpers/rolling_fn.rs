use chrono::NaiveDate;
use chrono_tz::America::New_York;
use ordered_float::OrderedFloat;
use rust_decimal::{
    Decimal, MathematicalOps, dec,
    prelude::{FromPrimitive, ToPrimitive},
};
use std::collections::{BTreeMap, VecDeque};

use crate::database::models_crud::historical_data::historical_data::HistoricalDataFullKeys;

pub trait RollingFn {
    fn new(window: usize) -> Self;
    fn push(&mut self, value: f64) -> Option<f64>;
    fn push_dec(&mut self, value: Decimal) -> Option<Decimal>;
    fn replace_last(&mut self, value: f64) -> Option<f64>;
    fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal>;
}

#[derive(Debug, Clone)]
pub struct RollingMax {
    window: usize,
    idx: usize,
    deq: VecDeque<(usize, Decimal)>, // (index, value), decreasing
}

impl RollingMax {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            idx: 0,
            deq: VecDeque::new(),
        }
    }

    /// Push a new value.
    /// Returns Some(max) once the window is full, otherwise None.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        self.push_dec(
            Decimal::from_f64(value)
                .expect("Expected to be able to read value pushed to RollingMax as Decimal"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent max value as f64")
        })
    }

    /// Push a new value.
    /// Returns Some(max) once the window is full, otherwise None.
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        let i = self.idx;
        self.idx += 1;

        // Remove smaller values from the back
        while let Some(&(_, v)) = self.deq.back() {
            if v <= value {
                self.deq.pop_back();
            } else {
                break;
            }
        }

        self.deq.push_back((i, value));

        // Remove out-of-window values from the front
        let window_start = (i + 1).saturating_sub(self.window);
        while let Some(&(j, _)) = self.deq.front() {
            if j < window_start {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        self.max_dec()
    }

    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        self.replace_last_dec(
            Decimal::from_f64(value)
                .expect("Expected to be able to read value pushed to RollingMax as Decimal"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent max value as Decimal")
        })
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(max) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        if self.idx == 0 {
            return None;
        }

        let i = self.idx - 1;

        // 1) Remove any existing entries for the last index (usually at the back, but handle safely)
        while let Some(&(j, _)) = self.deq.back() {
            if j == i {
                self.deq.pop_back();
            } else {
                break;
            }
        }
        // Extremely rare but for safety: if somehow last index is at the front (shouldn't happen),
        // remove it as well.
        while let Some(&(j, _)) = self.deq.front() {
            if j == i {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        // let value = Decimal::from_f64(value_f64)
        //     .expect("Expected to be able to read value pushed to RollingMax as Decimal");
        // 2) Reinsert (i, value) using the same monotonic rule as push()
        while let Some(&(_, v)) = self.deq.back() {
            if v <= value {
                self.deq.pop_back();
            } else {
                break;
            }
        }
        self.deq.push_back((i, value));

        // 3) Enforce window bounds (unchanged, but keep consistent)
        let window_start = (i + 1).saturating_sub(self.window);
        while let Some(&(j, _)) = self.deq.front() {
            if j < window_start {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        self.max_dec()
    }

    pub fn max_dec(&self) -> Option<Decimal> {
        if self.idx >= self.window {
            Some(self.deq.front().unwrap().1)
        } else {
            None
        }
    }

    pub fn max(&self) -> Option<f64> {
        self.max_dec().map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent max value as Decimal")
        })
    }
}

#[derive(Debug, Clone)]
pub struct RollingMin {
    window: usize,
    idx: usize,
    deq: VecDeque<(usize, Decimal)>, // (index, value), increasing
}

impl RollingMin {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            idx: 0,
            deq: VecDeque::new(),
        }
    }

    /// Push a new value.
    /// Returns Some(min) once the window is full, otherwise None.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        self.push_dec(
            Decimal::from_f64(value)
                .expect("Expected to be able to read value pushed to RollingMax as Decimal"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent max value as f64")
        })
    }

    /// Push a new value.
    /// Returns Some(min) once the window is full, otherwise None.
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        let i = self.idx;
        self.idx += 1;

        // Remove smaller values from the back
        while let Some(&(_, v)) = self.deq.back() {
            if v >= value {
                self.deq.pop_back();
            } else {
                break;
            }
        }

        self.deq.push_back((i, value));

        // Remove out-of-window values from the front
        let window_start = (i + 1).saturating_sub(self.window);
        while let Some(&(j, _)) = self.deq.front() {
            if j < window_start {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        self.min_dec()
    }

    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        self.replace_last_dec(
            Decimal::from_f64(value)
                .expect("Expected to be able to read value pushed to RollingMax as Decimal"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent max value as Decimal")
        })
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(min) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        if self.idx == 0 {
            return None;
        }

        let i = self.idx - 1;

        // 1) Remove any existing entries for the last index (usually at the back, but handle safely)
        while let Some(&(j, _)) = self.deq.back() {
            if j == i {
                self.deq.pop_back();
            } else {
                break;
            }
        }
        // Extremely rare but for safety: if somehow last index is at the front (shouldn't happen),
        // remove it as well.
        while let Some(&(j, _)) = self.deq.front() {
            if j == i {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        // let value = Decimal::from_f64(value_f64)
        //     .expect("Expected to be able to read value pushed to RollingMax as Decimal");
        // 2) Reinsert (i, value) using the same monotonic rule as push()
        while let Some(&(_, v)) = self.deq.back() {
            if v >= value {
                self.deq.pop_back();
            } else {
                break;
            }
        }
        self.deq.push_back((i, value));

        // 3) Enforce window bounds (unchanged, but keep consistent)
        let window_start = (i + 1).saturating_sub(self.window);
        while let Some(&(j, _)) = self.deq.front() {
            if j < window_start {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        self.min_dec()
    }

    pub fn min_dec(&self) -> Option<Decimal> {
        if self.idx >= self.window {
            Some(self.deq.front().unwrap().1)
        } else {
            None
        }
    }

    pub fn min(&self) -> Option<f64> {
        self.min_dec().map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent max value as Decimal")
        })
    }
}

#[derive(Debug, Clone)]
pub struct RollingSum {
    window: usize,
    rolling_sum: Decimal,
    deq: VecDeque<Decimal>, // (value), increasing
}

impl RollingSum {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            rolling_sum: dec!(0.0),
            deq: VecDeque::new(),
        }
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        self.deq.push_back(value);
        self.rolling_sum += value;
        if self.deq.len() > self.window {
            self.rolling_sum -= self.deq.pop_front().unwrap();
        }
        self.rolling_sum_dec()
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push(&mut self, value_f64: f64) -> Option<f64> {
        self.push_dec(
            Decimal::from_f64(value_f64)
                .expect("Expected to be able to represent value in rust decimal for rolling_sum"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent rolling_sum as f64")
        })
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(in) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        if self.deq.is_empty() {
            return None;
        }

        let prev_val = self.deq.pop_back().unwrap();
        self.rolling_sum -= prev_val;
        self.rolling_sum += value;
        self.deq.push_back(value);

        self.rolling_sum_dec()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(in) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        self.replace_last_dec(Decimal::from_f64(value).expect(
            "Expected to be able to represent value passed to replace_last\
                    in rust decimal for rolling_sum",
        ))
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent rolling_sum as f64")
        })
    }

    pub fn rolling_sum(&self) -> Option<f64> {
        if self.deq.len() == self.window {
            Some(
                self.rolling_sum
                    .to_f64()
                    .expect("Expected to be able to represent rolling_sum in f64"),
            )
        } else {
            None
        }
    }

    pub fn rolling_sum_dec(&self) -> Option<Decimal> {
        if self.deq.len() == self.window {
            Some(self.rolling_sum)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingMean {
    window: usize,
    min_periods: usize,
    /// If true, a NaN push (or NaN replace_last) PANICS (legacy behavior —
    /// NaN input is a caller bug in live strategies). If false, the NaN
    /// occupies the window position as an absent slot (pandas semantics).
    panic_on_nan: bool,
    /// If true, a mean QUERY with observed < min_periods PANICS instead of
    /// silently returning None (silent None propagates NaN downstream).
    panic_on_min_periods: bool,
    rolling_sum: Decimal,
    deq: VecDeque<Option<Decimal>>, // None = NaN/absent position (pandas window semantics)
}

impl RollingMean {
    /// Legacy constructor: min_periods = window, panic_on_nan = true,
    /// panic_on_min_periods = false (identical legacy behavior — NaN pushes
    /// panic; None until the window is full; full no-NaN mean exactly as
    /// the old window-denominator form).
    pub fn new(window: usize) -> Self {
        Self::with_flags(window, window, true, false)
    }

    /// pandas-style constructor: `rolling(window, min_periods)` — no panics:
    /// NaN pushes occupy window POSITIONS but are never observations; the
    /// mean returns None until min_periods observations are present.
    pub fn with_min_periods(window: usize, min_periods: usize) -> Self {
        Self::with_flags(window, min_periods, false, false)
    }

    /// Explicit flags constructor. `panic_on_nan`: NaN input panics (live
    /// strictness) instead of taking the window slot. `panic_on_min_periods`:
    /// a query below min_periods observations panics instead of returning
    /// None (surfacing silent-None propagation).
    pub fn with_flags(
        window: usize,
        min_periods: usize,
        panic_on_nan: bool,
        panic_on_min_periods: bool,
    ) -> Self {
        assert!(window > 0);
        assert!(min_periods >= 1 && min_periods <= window);
        Self {
            window,
            min_periods,
            panic_on_nan,
            panic_on_min_periods,
            rolling_sum: dec!(0.0),
            deq: VecDeque::new(),
        }
    }

    fn push_slot(&mut self, slot: Option<Decimal>) -> Option<Decimal> {
        self.deq.push_back(slot);
        if let Some(v) = slot {
            self.rolling_sum += v;
        }
        if self.deq.len() > self.window {
            if let Some(Some(v)) = self.deq.pop_front() {
                self.rolling_sum -= v;
            }
        }
        self.rolling_mean_dec()
    }

    /// Push a new value. With panic_on_nan (legacy default), a NaN PANICS
    /// (caller bug); otherwise it occupies a window position as an absent
    /// slot. Returns avg once observed >= min_periods (panics instead if
    /// panic_on_min_periods), otherwise None.
    pub fn push(&mut self, value_f64: f64) -> Option<f64> {
        if value_f64.is_nan() {
            if self.panic_on_nan {
                // Preserve the legacy panic site/message byte-identically.
                Decimal::from_f64(value_f64).expect(
                    "Expected to be able to represent value in rust decimal for rolling_sum",
                );
            }
            self.push_slot(None)
        } else {
            self.push_dec(Decimal::from_f64(value_f64).expect(
                "Expected to be able to represent value in rust decimal for rolling_sum",
            ))
        }
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent rolling_sum as f64")
        })
    }

    /// Push a new value (Decimal path — never absent; see `push` for NaN).
    /// Returns avg once observed >= min_periods, otherwise None.
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        self.push_slot(Some(value))
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(avg) once observed >= min_periods, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        self.replace_last_slot(Some(value))
    }

    /// Replace the last pushed slot; `None` quashes the last position
    /// (NaN-reinterpretation of the trailing point).
    pub fn replace_last_slot(&mut self, value: Option<Decimal>) -> Option<Decimal> {
        match self.deq.pop_back() {
            None => return None, // nothing pushed yet
            Some(Some(prev_val)) => {
                self.rolling_sum -= prev_val;
            }
            Some(None) => {}
        }
        if let Some(v) = value {
            self.rolling_sum += v;
        }
        self.deq.push_back(value);

        self.rolling_mean_dec()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(avg) once observed >= min_periods, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        let slot = if value.is_nan() {
            if self.panic_on_nan {
                // Preserve the legacy panic site/message byte-identically.
                Decimal::from_f64(value).expect(
                    "Expected to be able to represent value passed to replace_last\
                        in rust decimal for rolling_sum",
                );
            }
            None
        } else {
            Some(Decimal::from_f64(value).expect(
                "Expected to be able to represent value passed to replace_last\
                        in rust decimal for rolling_sum",
            ))
        };
        self.replace_last_slot(slot)
            .map(|dec_val| {
                dec_val
                    .to_f64()
                    .expect("Expected to be able to represent rolling_sum as f64")
            })
    }

    pub fn rolling_mean(&self) -> Option<f64> {
        self.rolling_mean_dec()
            .map(|dec_val| {
                dec_val
                    .to_f64()
                    .expect("Expected to be able to represent rolling_sum in f64")
            })
    }

    /// Mean over the OBSERVED (non-NaN) cells, gated at min_periods observed.
    /// If observed < min_periods: panics when panic_on_min_periods, else
    /// returns None. Legacy paths (min_periods == window, no NaN pushes) are
    /// byte-identical to the old window-denominator form.
    pub fn rolling_mean_dec(&self) -> Option<Decimal> {
        let observed = self.deq.iter().filter(|s| s.is_some()).count();
        if observed < self.min_periods {
            if self.panic_on_min_periods {
                panic!(
                    "RollingMean query below min_periods: observed {} < min_periods {} \
                     (panic_on_min_periods)",
                    observed, self.min_periods
                );
            }
            return None;
        }
        if observed == 0 {
            return None;
        }
        Some(
            self.rolling_sum
                / Decimal::from_usize(observed).expect(
                    "Expected observed count of RollingMean to be representable as a Decimal",
                ),
        )
    }
}

#[derive(Debug, Clone)]
pub struct RollingStd {
    window: usize,
    rolling_sum: RollingSum,
    rolling_sum_sq: RollingSum,
}

impl RollingStd {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            rolling_sum: RollingSum::new(window),
            rolling_sum_sq: RollingSum::new(window),
        }
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        let rolling_sum_val = self.rolling_sum.push_dec(value);
        self.rolling_sum_sq.push_dec(value * value);
        if rolling_sum_val.is_none() {
            return None;
        }
        // println!("{:?}", self.rolling_sum.rolling_sum());

        self.rolling_std_dec()
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        self.push_dec(Decimal::from_f64(value).expect(
            "Expected to be able to read value\
                passed to RollingStd.push as Decimal",
        ))
        .map(|dec_val| {
            dec_val.to_f64().expect(
                "Expected to be able to represent\
                rolling_std as f64",
            )
        })
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(std) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        let rolling_sum_val = self.rolling_sum.replace_last_dec(value);
        self.rolling_sum_sq.replace_last_dec(value * value);
        if rolling_sum_val.is_none() {
            return None;
        }

        self.rolling_std_dec()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(std) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        self.replace_last_dec(Decimal::from_f64(value).expect(
            "Expected value passed to RollingStd.replace_last\
                to be able to be converted to Decimal",
        ))
        .map(|dec_val| {
            dec_val.to_f64().expect(
                "Expected to be able to represent\
                rolling_std in f64",
            )
        })
    }

    pub fn rolling_std_dec(&self) -> Option<Decimal> {
        if self.rolling_sum.deq.len() < self.window {
            return None;
        }

        let window_dec = Decimal::from_usize(self.window)
            .expect("Expected to be able to convert window usize to decimal");
        let rolling_mean = self
            .rolling_sum
            .rolling_sum_dec()
            .expect("Expected enough data for rolling_sum in rolling_std")
            / window_dec;
        let rolling_sum_sq_val = self
            .rolling_sum_sq
            .rolling_sum_dec()
            .expect("Expected enough data for rolling_sum_sq in rolling_std");

        Some(
            ((rolling_sum_sq_val - window_dec * rolling_mean * rolling_mean).max(dec!(0.0))
                / (window_dec - dec!(1.0)))
            .sqrt()
            .expect("Expected to be able to take sqrt of decimal for rolling_std"),
        )
    }

    pub fn rolling_std(&self) -> Option<f64> {
        if self.rolling_sum.deq.len() < self.window {
            return None;
        }

        Some(
            self.rolling_std_dec()
                .unwrap()
                .to_f64()
                .expect("Expected to be able to represent rolling_std as f64"),
        )
    }
}

#[derive(Debug, Clone)]
pub struct RollingZScore {
    window: usize,
    rolling_sum: RollingSum,
    rolling_std: RollingStd,
    last_x: Decimal,
}

impl RollingZScore {
    pub fn new(window: usize) -> Self {
        assert!(window >= 2);
        Self {
            window,
            rolling_sum: RollingSum::new(window),
            rolling_std: RollingStd::new(window),
            last_x: dec!(0.0),
        }
    }

    /// Push new value, return z-score of *latest* value
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        self.rolling_sum.push_dec(value);
        self.rolling_std.push_dec(value);
        self.last_x = value;

        self.z_score_dec()
    }

    /// Push new value, return z-score of *latest* value
    pub fn push(&mut self, value: f64) -> Option<f64> {
        self.push_dec(
            Decimal::from_f64(value)
                .expect("Expected to be able to represent value passed to z-score as Decimal"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent z-score as f64")
        })
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(z-score) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        if self.rolling_sum.deq.is_empty() {
            return None;
        }

        self.rolling_sum.replace_last_dec(value);
        self.rolling_std.replace_last_dec(value);
        self.last_x = value;

        self.z_score_dec()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(z-score) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        self.replace_last_dec(Decimal::from_f64(value).expect(
            "Expected to be able to convert value\
                passed to RollingZScore.replace_last to f64",
        ))
        .map(|dec_val| {
            dec_val.to_f64().expect(
                "Expected to be able to represent\
                z-score as f64",
            )
        })
    }

    pub fn z_score_dec(&self) -> Option<Decimal> {
        if self.rolling_sum.rolling_sum_dec().is_none() {
            None
        } else {
            let n = Decimal::from_usize(self.window)
                .expect("Expected to be able to represent window of z_score as decimal");
            let mean = self.rolling_sum.rolling_sum_dec().unwrap() / n;
            let dec_res = (self.last_x - mean)
                .checked_div(self.rolling_std.rolling_std_dec().unwrap())
                .unwrap_or(Decimal::MAX);
            Some(dec_res)
        }
    }

    pub fn z_score(&self) -> Option<f64> {
        if self.rolling_sum.rolling_sum_dec().is_none() {
            None
        } else {
            Some(
                self.z_score_dec()
                    .unwrap()
                    .to_f64()
                    .expect("Expected to be able to represent z-score in f64"),
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingRankPct {
    window: usize,
    buf: VecDeque<OrderedFloat<f64>>,
    counts: BTreeMap<OrderedFloat<f64>, usize>,
    len: usize,
    last_elem: OrderedFloat<f64>,
}

impl RollingRankPct {
    pub fn new(window: usize) -> Self {
        Self {
            window,
            buf: VecDeque::with_capacity(window),
            counts: BTreeMap::new(),
            len: 0,
            last_elem: OrderedFloat(0.0),
        }
    }

    pub fn push(&mut self, x: f64) -> Option<f64> {
        let x = OrderedFloat(x);
        self.last_elem = x;

        // insert
        self.buf.push_back(x);
        *self.counts.entry(x).or_insert(0) += 1;
        self.len += 1;

        // evict
        if self.len > self.window {
            let old = self.buf.pop_front().unwrap();
            let e = self.counts.get_mut(&old).unwrap();
            *e -= 1;
            if *e == 0 {
                self.counts.remove(&old);
            }
            self.len -= 1;
        }

        self.percentile()
    }

    /// Returns Some(percentile) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        if self.len == 0 {
            return None;
        }

        let new_x = OrderedFloat(value);

        // The last pushed value must be the last element in buf.
        // We remove it from buf + counts, then insert the new value.
        let old_x = *self.buf.back().expect("len>0 implies buf not empty");

        // 1) Update buf last element
        *self.buf.back_mut().unwrap() = new_x;

        // 2) Update counts: decrement old, increment new
        {
            let e = self.counts.get_mut(&old_x).unwrap();
            *e -= 1;
            if *e == 0 {
                self.counts.remove(&old_x);
            }
        }

        *self.counts.entry(new_x).or_insert(0) += 1;

        // 3) Update last_elem used by percentile()
        self.last_elem = new_x;

        self.percentile()
    }

    pub fn percentile(&self) -> Option<f64> {
        if self.len < self.window {
            return None;
        }

        // exact percentile
        let mut leq = 0usize;
        for (&v, &c) in self.counts.iter() {
            if v <= self.last_elem {
                leq += c;
            } else {
                break;
            }
        }

        Some(leq as f64 / self.window as f64)
    }
}

#[derive(Debug, Clone)]
pub struct EwmMean {
    idx: usize,
    alpha: Decimal,
    prev: Option<Decimal>,      // ewma_t
    prev_prev: Option<Decimal>, // ewma_{t-1}

    // --- pandas mode (f64 verbatim aggregations.pyx kernel; disjoint state) ---
    pandas: bool,
    old_wt_factor: f64,
    new_wt: f64,
    adjust: bool,
    min_periods: usize,
    weighted: Option<f64>,
    old_wt: f64,
    nobs: usize,
}

impl EwmMean {
    pub fn new(span: usize) -> Self {
        assert!(span > 0);
        let alpha = dec!(2.0)
            / (Decimal::from_usize(span)
                .expect("Expected to be able to represent span as Decimal")
                + dec!(1.0));
        Self {
            idx: 0,
            alpha,
            prev: None,
            prev_prev: None,
            pandas: false,
            old_wt_factor: 0.0,
            new_wt: 0.0,
            adjust: false,
            min_periods: 1,
            weighted: None,
            old_wt: 1.0,
            nobs: 0,
        }
    }

    /// pandas `ewm(alpha=…, adjust, min_periods)` — the SAME struct as the
    /// legacy Decimal mode, flag-switched. Kernel state is disjoint:
    /// `weighted/nobs/old_wt` (f64) never mix with `prev/prev_prev` (Decimal).
    /// Verbatim port of pandas `_libs/window/aggregations.pyx ewm()` — NaN
    /// gap-decay with ignore_na=False, old_wt carry, adjust both modes.
    pub fn pandas_alpha(alpha: f64, adjust: bool, min_periods: usize) -> Self {
        Self {
            idx: 0,
            alpha: dec!(0.0),
            prev: None,
            prev_prev: None,
            pandas: true,
            old_wt_factor: 1.0 - alpha,
            new_wt: if adjust { 1.0 } else { alpha },
            adjust,
            min_periods,
            weighted: None,
            old_wt: 1.0,
            nobs: 0,
        }
    }

    pub fn pandas_span(span: usize, adjust: bool, min_periods: usize) -> Self {
        Self::pandas_alpha(2.0 / (span as f64 + 1.0), adjust, min_periods)
    }

    /// Push a new value.
    /// Returns Some(ewm) once initialized (immediately after first push),
    /// otherwise None.
    /// pandas mode: NaN = missing slot (state accrues `old_wt *= (1−α)`,
    /// nobs unchanged); Some(weighted) once ≥ min_periods observations.
    /// Decimal mode: NaN input panics (legacy semantics preserved).
    pub fn push(&mut self, value_f64: f64) -> Option<f64> {
        if self.pandas {
            return self.push_pandas(value_f64);
        }
        self.push_dec(
            Decimal::from_f64(value_f64)
                .expect("Expected to be able to represent vlue passed to EWMean as Decimal"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent current EwmMean as f64")
        })
    }

    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        if self.pandas {
            let x = value
                .to_f64()
                .expect("Expected to be able to represent value passed to EWMean as f64");
            let _ = self.push_pandas(x);
            return self.weighted.and_then(|w| Decimal::from_f64(w));
        }
        self.idx += 1;

        let current = match self.prev {
            None => value, // first point: match pandas adjust=False recursion behavior
            Some(p) => self.alpha * value + (dec!(1.0) - self.alpha) * p,
        };

        self.prev_prev = self.prev;
        self.prev = Some(current);
        Some(current)
    }

    /// pandas-mode kernel — op order VERBATIM from aggregations.pyx ewm().
    fn push_pandas(&mut self, x: f64) -> Option<f64> {
        let is_obs = !x.is_nan();
        if is_obs {
            self.nobs += 1;
        }
        match self.weighted {
            Some(w) => {
                // ignore_na=False (pandas default): accrue EVERY position
                self.old_wt *= self.old_wt_factor;
                if is_obs {
                    if w != x {
                        self.weighted = Some(
                            (self.old_wt * w + self.new_wt * x) / (self.old_wt + self.new_wt),
                        );
                    }
                    if self.adjust {
                        self.old_wt += self.new_wt;
                    } else {
                        self.old_wt = 1.0;
                    }
                }
            }
            None => {
                if is_obs {
                    self.weighted = Some(x);
                }
            }
        }
        if self.nobs >= self.min_periods {
            self.weighted
        } else {
            None
        }
    }

    /// Replace the last pushed raw value (same index) and recompute ewma_t.
    ///
    /// If no value has been pushed yet, returns None.
    /// (Pandas mode returns None — the kernel has no replace semantic; the
    /// pinned parity tests hold over append-only pushes.)
    pub fn replace_last(&mut self, value_f64: f64) -> Option<f64> {
        if self.pandas {
            return None;
        }
        let value = Decimal::from_f64(value_f64)
            .expect("Expected to be able to represent vlue passed to EWMean as Decimal");
        if self.idx == 0 {
            return None;
        }

        // If there's only one point, ewma_0 = value
        if self.idx == 1 {
            self.prev = Some(value);
            // prev_prev remains None
            return Some(
                value
                    .to_f64()
                    .expect("Expected to be able to represent current EwmMean as f64"),
            );
        }

        // idx >= 2: ewma_t = alpha * x_t + (1-alpha) * ewma_{t-1}
        let pprev = self.prev_prev.expect("idx>=2 implies prev_prev must exist");

        let current = self.alpha * value + (dec!(1.0) - self.alpha) * pprev;
        self.prev = Some(current);
        Some(
            current
                .to_f64()
                .expect("Expected to be able to represent current EwmMean as f64"),
        )
    }

    /// Returns the last computed EWM mean (if any)
    pub fn value(&self) -> Option<f64> {
        if self.pandas {
            if self.nobs >= self.min_periods {
                return self.weighted;
            }
            return None;
        }
        self.prev.map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent current EwmMean as f64")
        })
    }
}

#[derive(Debug, Clone)]
pub struct RollingRoc {
    diff: usize,
    deq: VecDeque<Decimal>, // (value), increasing
}

impl RollingRoc {
    pub fn new(diff: usize) -> Self {
        assert!(diff > 0);
        Self {
            diff,
            deq: VecDeque::new(),
        }
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push_dec(&mut self, value: Decimal) -> Option<Decimal> {
        self.deq.push_back(value);
        if self.deq.len() > self.diff + 1 {
            self.deq.pop_front().unwrap();
        }
        self.rolling_roc_dec()
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push(&mut self, value_f64: f64) -> Option<f64> {
        self.push_dec(
            Decimal::from_f64(value_f64)
                .expect("Expected to be able to represent value in rust decimal for rolling_sum"),
        )
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent rolling_sum as f64")
        })
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(in) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last_dec(&mut self, value: Decimal) -> Option<Decimal> {
        if self.deq.is_empty() {
            return None;
        }

        self.deq.pop_back().unwrap();
        self.deq.push_back(value);

        self.rolling_roc_dec()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(in) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        self.replace_last_dec(Decimal::from_f64(value).expect(
            "Expected to be able to represent value passed to replace_last\
                    in rust decimal for rolling_sum",
        ))
        .map(|dec_val| {
            dec_val
                .to_f64()
                .expect("Expected to be able to represent rolling_sum as f64")
        })
    }

    pub fn rolling_roc(&self) -> Option<f64> {
        self.rolling_roc_dec().map(|v| {
            v.to_f64()
                .expect("Expected to be able to represent ROC as f64")
        })
    }

    pub fn rolling_roc_dec(&self) -> Option<Decimal> {
        if self.deq.len() == self.diff + 1 {
            let first_el = self.deq.front().unwrap();
            Some((self.deq.back().unwrap() - first_el) / first_el)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingDayVwap {
    window: usize,
    date: Option<NaiveDate>,
    cum_price_vol: Decimal,
    cum_vol: Decimal,
}

impl RollingDayVwap {
    // use max no. bars in day - i.e. usually 78
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            date: None,
            cum_price_vol: dec!(0.0),
            cum_vol: dec!(0.0),
        }
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push(&mut self, bar: &HistoricalDataFullKeys) -> Option<Decimal> {
        // on first init, return bar directly
        if self.date.is_none()
            || self
                .date
                .is_some_and(|date| date != bar.get_time().with_timezone(&New_York).date_naive())
        {
            self.date = Some(bar.get_time().with_timezone(&New_York).date_naive());
            self.cum_price_vol = dec!(0);
            self.cum_vol = dec!(0);
        }

        self.cum_price_vol += Decimal::from_f64(bar.get_price())
            .expect("Expected to be able to convert bar close to Decimal")
            * bar.get_volume();
        self.cum_vol += bar.get_volume();

        Some(
            self.cum_price_vol
                .checked_div(self.cum_vol)
                .unwrap_or_else(|| {
                    tracing::error!("Failed to get actual vwap");
                    Decimal::from_f64(bar.get_price())
                        .expect("Expected to be able to convert bar close to Decimal")
                }),
        )
    }

    // /// Replace the last pushed value (same index).
    // ///
    // /// Returns Some(in) once the window is full, otherwise None.
    // ///
    // /// If no value has been pushed yet, returns None.
    // pub fn replace_last(&mut self, bar: HistoricalDataFullKeys) -> Option<f64> {
    //     self.replace_last_dec(Decimal::from_f64(value).expect(
    //         "Expected to be able to represent value passed to replace_last\
    //                 in rust decimal for rolling_sum",
    //     ))
    //     .map(|dec_val| {
    //         dec_val
    //             .to_f64()
    //             .expect("Expected to be able to represent rolling_sum as f64")
    //     })
    // }

    pub fn vwap(&self) -> Option<Decimal> {
        if self.date.is_none() {
            return None;
        }

        Some(
            self.cum_price_vol
                .checked_div(self.cum_vol)
                .unwrap_or_else(|| {
                    tracing::error!("Failed to get actual vwap");
                    self.cum_price_vol
                }),
        )
    }
}

/// `RollingBeta` — rolling ordinary-least-squares β of `y` on `x` over a
/// fixed-size window (f64-based, like `RollingRankPct`). Used by the
/// residual-momentum signal in `relative_v2`: regress an instrument's
/// bar returns (y) on the benchmark/market's bar returns (x), then take
/// the residual `y − β·x` as the idiosyncratic (firm-specific) component
/// (Gutierrez-Pirinsky 2007, adapted to the intraday bar scale).
///
/// Maintains running sums Σx, Σy, Σxy, Σx² in O(1) per push; β is
/// `(n·Σxy − Σx·Σy) / (n·Σx² − (Σx)²)`. Returns `Some(0.0)` for a
/// degenerate window (no market variance) so callers don't divide by zero.
#[derive(Debug, Clone)]
pub struct RollingBeta {
    window: usize,
    deq_x: VecDeque<f64>,
    deq_y: VecDeque<f64>,
    sx: f64,
    sy: f64,
    sxy: f64,
    sxx: f64,
    len: usize,
}

impl RollingBeta {
    pub fn new(window: usize) -> Self {
        assert!(window > 1, "RollingBeta needs window > 1");
        Self {
            window,
            deq_x: VecDeque::with_capacity(window),
            deq_y: VecDeque::with_capacity(window),
            sx: 0.0,
            sy: 0.0,
            sxy: 0.0,
            sxx: 0.0,
            len: 0,
        }
    }

    /// Push a paired sample `(x, y)` — `x` = market return, `y` = instrument
    /// return. Returns `Some(beta)` once the window is full, else `None`.
    pub fn push(&mut self, x: f64, y: f64) -> Option<f64> {
        self.deq_x.push_back(x);
        self.deq_y.push_back(y);
        self.sx += x;
        self.sy += y;
        self.sxy += x * y;
        self.sxx += x * x;
        self.len += 1;
        if self.len > self.window {
            let ox = self.deq_x.pop_front().unwrap();
            let oy = self.deq_y.pop_front().unwrap();
            self.sx -= ox;
            self.sy -= oy;
            self.sxy -= ox * oy;
            self.sxx -= ox * ox;
            self.len -= 1;
        }
        self.beta()
    }

    /// Current β if the window is full. `Some(0.0)` if the market showed no
    /// variance in-window (degenerate) — caller treats instrument return as
    /// entirely idiosyncratic.
    pub fn beta(&self) -> Option<f64> {
        if self.len < self.window {
            return None;
        }
        let n = self.len as f64;
        let denom = n * self.sxx - self.sx * self.sx;
        if denom.abs() < 1e-12 {
            return Some(0.0);
        }
        Some((n * self.sxy - self.sx * self.sy) / denom)
    }
}

/// `RollingFracDiff` — fractional differentiation of a price series of order
/// `d ∈ (0, 1)` (López de Prado, *Advances in Financial Machine Learning*
/// Ch. 5). Standard integer differencing (d=1, i.e. bar returns) is
/// stationary but *memoryless*; the raw price (d=0) has full memory but is
/// non-stationary. A fractional `d` finds the minimum differentiation that
/// still achieves stationarity, preserving the maximum long-memory of the
/// trend — yielding a richer, higher-IC momentum feature than simple
/// returns. Used by `fractional_momentum` as the cross-sectional ranking
/// signal (the FD value is z-scored per instrument for comparability).
///
/// The binomial-expansion weights `w_k = ∏_{i=1..k} (i − 1 − d) / i` (with
/// `w_0 = 1`) decay like `k^(d−1)` for `d < 1`, so the series is truncatable.
/// We precompute the weight vector once (truncated when `|w_k| < threshold`,
/// hard-capped at 1000 terms) and keep a fixed-size `VecDeque` of prices
/// aligned newest→oldest. Each push is an O(window) dot-product — window is
/// modest (a few hundred terms for d≈0.4), so this is cheap.
#[derive(Debug, Clone)]
pub struct RollingFracDiff {
    /// Precomputed weights, stored **newest-aligned**: `weights[i]` pairs
    /// with `deq[i]` where `deq[0]` is the oldest price in the window and
    /// `deq[len-1]` is the newest (which receives `w_0 = 1`).
    weights: Vec<f64>,
    deq: VecDeque<f64>,
    len: usize,
}

impl RollingFracDiff {
    /// Construct with differentiation order `d ∈ (0, 1)` and a
    /// weight-truncation `threshold` (typically `1e-5`). Panics if `d` is
    /// out of range or the resulting weight vector is empty.
    pub fn new(d: f64, threshold: f64) -> Self {
        assert!(
            (0.0..1.0).contains(&d),
            "RollingFracDiff: d must be in (0, 1), got {d}"
        );
        let mut weights = Self::compute_weights(d, threshold);
        assert!(!weights.is_empty(), "RollingFracDiff: empty weight vector");
        // Store newest-aligned: weights[0] pairs with the oldest price in the
        // window, weights[len-1] (= w_0 = 1.0) pairs with the newest price.
        weights.reverse();
        let cap = weights.len();
        Self {
            weights,
            deq: VecDeque::with_capacity(cap),
            len: 0,
        }
    }

    /// Build the (non-reversed) binomial-expansion weight vector
    /// `[w_0, w_1, …, w_{K−1}]` with `w_0 = 1`, truncating when
    /// `|w_k| < threshold` (and a hard cap of 1000 terms for safety).
    fn compute_weights(d: f64, threshold: f64) -> Vec<f64> {
        let mut weights = vec![1.0]; // w_0
        let mut w = 1.0;
        for k in 1..=1000 {
            w *= ((k as f64) - 1.0 - d) / (k as f64);
            if w.abs() < threshold {
                break;
            }
            weights.push(w);
        }
        weights
    }

    /// Push a new price. Returns `Some(fd_value)` once the window is full,
    /// else `None`. The FD value is `Σ_k w_k · price_{t−k}` — a stationary,
    /// memory-preserving momentum feature.
    pub fn push(&mut self, price: f64) -> Option<f64> {
        self.deq.push_back(price);
        self.len += 1;
        if self.len > self.weights.len() {
            self.deq.pop_front();
            self.len -= 1;
        }
        if self.len < self.weights.len() {
            return None;
        }
        let mut acc = 0.0;
        for (i, p) in self.deq.iter().enumerate() {
            acc += self.weights[i] * p;
        }
        Some(acc)
    }

    /// Current FD value if the window is full, else `None`.
    pub fn value(&self) -> Option<f64> {
        if self.len < self.weights.len() {
            return None;
        }
        let mut acc = 0.0;
        for (i, p) in self.deq.iter().enumerate() {
            acc += self.weights[i] * p;
        }
        Some(acc)
    }

    /// The configured window length (number of stored prices == weight count).
    pub fn window(&self) -> usize {
        self.weights.len()
    }
}

// ===========================================================================
// pandas-EWM state machine — verbatim port of the pandas Cython `ewm()`
// kernel (pandas/_libs/window/aggregations.pyx, v2.3.3). The earlier powi
// recursions were removed after a 1-ulp mismatch: pandas accrues decay via a
// RUNNING PRODUCT `old_wt *= (1−α)` on EVERY position (NaN included), blends
// with the exact op order `(old_wt*weighted + new_wt*x)/(old_wt+new_wt)`
// behind a constant-series guard (`weighted != x`), and RESETS old_wt to 1.0
// after every observation in adjust=False mode.
// Fields mirror the Cython locals: old_wt, old_wt_factor = 1−α,
// new_wt = 1 (adjust) | α (no adjust), weighted, nobs.
// ===========================================================================

/// pandas `rolling(window, min_periods).median()` — position-sliding window
/// median in f64 (even count = mean of the two central values, numpy
/// semantics). NaN values occupy WINDOW POSITIONS but are never observations
/// (pandas dense-series semantics). Added to the SAME shared library — no
/// per-strategy duplicates allowed (see stratlab SKILL.md).
#[derive(Debug, Clone)]
pub struct RollingMedian {
    window: usize,
    min_periods: usize,
    buf: std::collections::VecDeque<f64>,
}

impl RollingMedian {
    pub fn new(window: usize, min_periods: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            min_periods,
            buf: std::collections::VecDeque::with_capacity(window),
        }
    }

    /// Push one position. Returns Some(median) once the OBSERVED (non-NaN)
    /// values within the last `window` positions ≥ min_periods.
    pub fn push(&mut self, x: f64) -> Option<f64> {
        self.buf.push_back(x);
        if self.buf.len() > self.window {
            self.buf.pop_front();
        }
        let median = median_of(self.buf.iter().copied().collect());
        median.filter(|_| self.buf.iter().filter(|v| !v.is_nan()).count() >= self.min_periods)
    }

    pub fn value(&self) -> Option<f64> {
        median_of(self.buf.iter().copied().collect())
    }
}

fn median_of(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let n = v.len();
    Some(if n % 2 == 1 {
        v[n / 2]
    } else {
        0.5 * (v[n / 2 - 1] + v[n / 2])
    })
}

#[cfg(test)]
mod rolling_mean_flag_tests {
    use super::RollingMean;

    /// Legacy default: NaN push panics (the original from_f64 expect).
    #[test]
    #[should_panic(expected = "Expected to be able to represent value in rust decimal for rolling_sum")]
    fn legacy_nan_push_panics() {
        let mut rm = RollingMean::new(3);
        rm.push(1.0);
        rm.push(f64::NAN);
    }

    /// pandas-style: NaN occupies a position, is not an observation, and
    /// min_periods gates on OBSERVED count.
    #[test]
    fn pandas_mode_nan_slots_and_min_periods() {
        let mut rm = RollingMean::with_min_periods(3, 2);
        assert_eq!(rm.push(1.0), None); // observed 1 < 2
        assert_eq!(rm.push(f64::NAN), None); // observed 1 < 2
        assert_eq!(rm.push(3.0), Some(2.0)); // observed 2 → mean(1,3)
        assert_eq!(rm.push(5.0), Some(4.0)); // observed 3 → mean(1,3,5)
    }

    /// panic_on_min_periods: a query below min_periods observations panics
    /// instead of silently returning None. push() dispatches the mean, so the
    /// panic fires on the second push (observed 1 < 2).
    #[test]
    #[should_panic(expected = "RollingMean query below min_periods")]
    fn strict_min_periods_panics_on_query() {
        let mut rm = RollingMean::with_flags(3, 2, false, true);
        rm.push(1.0);
        rm.push(3.0);
    }
}

#[cfg(test)]
mod pandas_ewm_tests {
    use super::{EwmMean, RollingMedian};

    /// Pinned against pandas 2.x on 2026-10-25 (see stratlab plan P4 ref):
    /// x = [1, 2, NaN, 4, 5, NaN, NaN, 8, 9, 10], alpha = 1/14.
    #[test]
    fn pandas_gap_semantics_pinned() {
        let xs = [1.0, 2.0, f64::NAN, 4.0, 5.0, f64::NAN, f64::NAN, 8.0, 9.0, 10.0];
        let mut af = EwmMean::pandas_alpha(1.0 / 14.0, false, 1);
        let mut at = EwmMean::pandas_alpha(1.0 / 14.0, true, 1);
        let mut at3 = EwmMean::pandas_alpha(1.0 / 14.0, true, 3);
        let af_pin: Vec<Option<f64>> = [
            "1.0", "1.0714285714285714", "1.0714285714285714", "1.295472287275566",
            "1.5600814096130255", "1.5600814096130255", "1.5600814096130255",
            "2.0875465344420463", "2.5812932105533286", "3.111200838370948",
        ]
        .iter()
        .map(|s| Some(s.parse::<f64>().unwrap()))
        .collect();
        let at_pin: Vec<Option<f64>> = [
            "1.0", "1.5185185185185186", "1.5185185185185186", "2.450390036950869",
            "3.1845780206435945", "3.1845780206435945", "3.1845780206435945",
            "4.458353287624601", "5.46528054048467", "6.339320255579501",
        ]
        .iter()
        .map(|s| Some(s.parse::<f64>().unwrap()))
        .collect();
        for (i, &x) in xs.iter().enumerate() {
            let y_af = af.push_pandas(x);
            let y_at = at.push_pandas(x);
            let y_at3 = at3.push_pandas(x);
            assert_eq!(y_af.map(|v| v.to_string()), af_pin[i].map(|v| v.to_string()), "adjust=false @{i}");
            assert_eq!(y_at.map(|v| v.to_string()), at_pin[i].map(|v| v.to_string()), "adjust=true @{i}");
            let want3 = if i < 3 { None } else { y_at };
            assert_eq!(y_at3, want3, "min_periods=3 @{i}");
        }
    }

    #[test]
    fn rolling_median_pinned() {
        let xs = [5.0, 3.0, 8.0, 1.0, 9.0, 2.0, 7.0];
        let mut m = RollingMedian::new(3, 3);
        let want: Vec<Option<f64>> = [None, None, Some(5.0), Some(3.0), Some(8.0), Some(2.0), Some(7.0)]
            .into_iter()
            .collect();
        for (i, x) in xs.iter().enumerate() {
            assert_eq!(m.push(*x), want[i], "median @{i}");
        }
        let mut e = RollingMedian::new(2, 2);
        assert_eq!(e.push(1.0), None);
        assert_eq!(e.push(2.0), Some(1.5));
        assert_eq!(e.push(3.0), Some(2.5));
        assert_eq!(e.push(4.0), Some(3.5));
    }
}
// (end of the pandas_ewm_tests module; the former TEMPORARY
// probe_coin_tr_ewm differential probe was removed after the
// COIN ATR differential was localized and fixed — the battery-faithful
// TR column remains reproducible via APP/_wmap_decomp.py if needed.)
