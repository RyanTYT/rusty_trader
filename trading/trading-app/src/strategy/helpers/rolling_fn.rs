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
    rolling_sum: Decimal,
    deq: VecDeque<Decimal>, // (value), increasing
}

impl RollingMean {
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
        self.rolling_mean_dec()
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

        self.rolling_mean_dec()
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

    pub fn rolling_mean(&self) -> Option<f64> {
        if self.deq.len() == self.window {
            Some(
                self.rolling_sum
                    .to_f64()
                    .expect("Expected to be able to represent rolling_sum in f64")
                    / self.window as f64,
            )
        } else {
            None
        }
    }

    pub fn rolling_mean_dec(&self) -> Option<Decimal> {
        if self.deq.len() == self.window {
            Some(
                self.rolling_sum
                    / Decimal::from_usize(self.window)
                        .expect("Expected window of RollingMean to be representable as a Decimal"),
            )
        } else {
            None
        }
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
            let dec_res = (self.last_x - mean) / self.rolling_std.rolling_std_dec().unwrap();
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
        }
    }

    /// Push a new value.
    /// Returns Some(ewm) once initialized (immediately after first push), otherwise None.
    pub fn push(&mut self, value_f64: f64) -> Option<f64> {
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
        self.idx += 1;

        let current = match self.prev {
            None => value, // first point: match pandas adjust=False recursion behavior
            Some(p) => self.alpha * value + (dec!(1.0) - self.alpha) * p,
        };

        self.prev_prev = self.prev;
        self.prev = Some(current);
        Some(current)
    }

    /// Replace the last pushed raw value (same index) and recompute ewma_t.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value_f64: f64) -> Option<f64> {
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
