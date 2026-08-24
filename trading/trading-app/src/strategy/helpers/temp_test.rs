use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, VecDeque};
use std::iter::zip;

const LAMBDA: f64 = 2.0;

#[derive(Clone, Debug)]
struct MidPriceBar {
    mid_open: f64,
    mid_high: f64,
    mid_low: f64,
    mid_close: f64,
}

#[derive(Clone, Debug)]
struct RvqParams {
    rv_q_ranker: RollingRankPct,

    rv_30: RollingStd,
    rolling_rv_30_z: RollingZScore,
    rolling_rv_30_log_z: RollingZScore,
}

#[derive(Clone, Debug)]
struct FlowImbalanceParams {
    ask_ret_deque: RollingSum,
    bid_ret_deque: RollingSum,
}

#[derive(Clone, Debug)]
struct RangeParams {
    range_high_deque: RollingMax,
    range_low_deque: RollingMin,
}

#[derive(Debug, Clone)]
pub struct RollingMax {
    window: usize,
    idx: usize,
    deq: VecDeque<(usize, f64)>, // (index, value), decreasing
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

        self.max()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(max) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
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

        self.max()
    }

    /// Get the latest valid deque (monotonic, in-window)
    pub fn deque(&self) -> &VecDeque<(usize, f64)> {
        &self.deq
    }

    pub fn max(&self) -> Option<f64> {
        if self.idx >= self.window {
            Some(self.deq.front().unwrap().1)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingMin {
    window: usize,
    idx: usize,
    deq: VecDeque<(usize, f64)>, // (index, value), increasing
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
        let i = self.idx;
        self.idx += 1;

        // Remove larger values from the back
        while let Some(&(_, v)) = self.deq.back() {
            if v >= value {
                self.deq.pop_back();
            } else {
                break;
            }
        }

        self.deq.push_back((i, value));

        // Remove out-of-window values from the front
        // let window_start = i + 1 - self.window;
        let window_start = (i + 1).saturating_sub(self.window);
        while let Some(&(j, _)) = self.deq.front() {
            if j < window_start {
                self.deq.pop_front();
            } else {
                break;
            }
        }

        self.min()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(in) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
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

        self.min()
    }

    /// Get the latest valid deque (monotonic, in-window)
    pub fn deque(&self) -> &VecDeque<(usize, f64)> {
        &self.deq
    }

    pub fn min(&self) -> Option<f64> {
        if self.idx >= self.window {
            Some(self.deq.front().unwrap().1)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingSum {
    window: usize,
    rolling_sum: f64,
    deq: VecDeque<f64>, // (value), increasing
}

impl RollingSum {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            rolling_sum: 0.0,
            deq: VecDeque::new(),
        }
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        self.deq.push_back(value);
        self.rolling_sum += value;
        if self.deq.len() > self.window {
            self.rolling_sum -= self.deq.pop_front().unwrap();
        }
        self.rolling_sum()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(in) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        if self.deq.is_empty() {
            return None;
        }

        let prev_val = self.deq.pop_back().unwrap();
        self.rolling_sum -= prev_val;
        self.rolling_sum += value;
        self.deq.push_back(value);

        self.rolling_sum()
    }

    /// Get the latest valid deque (monotonic, in-window)
    pub fn deque(&self) -> &VecDeque<f64> {
        &self.deq
    }

    pub fn rolling_sum(&self) -> Option<f64> {
        if self.deq.len() == self.window {
            Some(self.rolling_sum)
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RollingStd {
    window: usize,
    rolling_std: f64,
    rolling_sum: RollingSum,
    rolling_sum_sq: RollingSum,
}

impl RollingStd {
    pub fn new(window: usize) -> Self {
        assert!(window > 0);
        Self {
            window,
            rolling_std: 0.0,
            rolling_sum: RollingSum {
                window,
                rolling_sum: 0.0,
                deq: VecDeque::new(),
            },
            rolling_sum_sq: RollingSum {
                window,
                rolling_sum: 0.0,
                deq: VecDeque::new(),
            },
        }
    }

    /// Push a new value.
    /// Returns avg once the window is full, otherwise None.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        let rolling_sum_val = self.rolling_sum.push(value);
        self.rolling_sum_sq.push(value * value);
        if rolling_sum_val.is_none() {
            return None;
        }
        // println!("{:?}", self.rolling_sum.rolling_sum());

        self.rolling_std()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(std) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        let rolling_sum_val = self.rolling_sum.replace_last(value);
        self.rolling_sum_sq.replace_last(value * value);
        if rolling_sum_val.is_none() {
            return None;
        }

        self.rolling_std()
    }

    pub fn rolling_std(&self) -> Option<f64> {
        if self.rolling_sum.deq.len() < self.window {
            return None;
        }

        let rolling_mean = self
            .rolling_sum
            .rolling_sum()
            .expect("Expected enough data for rolling_sum in rolling_std")
            / (self.window as f64);
        let rolling_sum_sq_val = self
            .rolling_sum_sq
            .rolling_sum()
            .expect("Expected enough data for rolling_sum_sq in rolling_std");

        Some(
            ((rolling_sum_sq_val - (self.window as f64) * rolling_mean * rolling_mean)
                / (self.window as f64 - 1.0)),
        )
    }
}

#[derive(Debug, Clone)]
pub struct RollingZScore {
    window: usize,
    buf: VecDeque<f64>,
    sum: f64,
    sum_sq: f64,
    last_x: f64,
}

impl RollingZScore {
    pub fn new(window: usize) -> Self {
        assert!(window >= 2);
        Self {
            window,
            buf: VecDeque::with_capacity(window),
            sum: 0.0,
            sum_sq: 0.0,
            last_x: 0.0,
        }
    }

    /// Push new value, return z-score of *latest* value
    pub fn push(&mut self, x: f64) -> Option<f64> {
        self.buf.push_back(x);
        self.sum += x;
        self.sum_sq += x * x;

        if self.buf.len() > self.window {
            let old = self.buf.pop_front().unwrap();
            self.sum -= old;
            self.sum_sq -= old * old;
        }

        self.z_score()
    }

    /// Replace the last pushed value (same index).
    ///
    /// Returns Some(z-score) once the window is full, otherwise None.
    ///
    /// If no value has been pushed yet, returns None.
    pub fn replace_last(&mut self, value: f64) -> Option<f64> {
        if self.buf.is_empty() {
            return None;
        }

        let last_val = self.buf.pop_back().unwrap();
        self.sum -= last_val;
        self.sum_sq -= last_val * last_val;
        self.sum += value;
        self.sum_sq += value * value;
        self.buf.push_back(value);

        self.z_score()
    }

    pub fn z_score(&self) -> Option<f64> {
        if self.buf.len() == self.window {
            let n = self.window as f64;
            let mean = self.sum / n;
            let var = (self.sum_sq - n * mean * mean) / (n - 1.0);
            let std = var.max(0.0).sqrt();
            Some((self.last_x - mean) / std)
        } else {
            None
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

use chrono::{DateTime, Duration, TimeZone, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalForexData {
    pub pair: String,
    pub time: DateTime<Utc>,
    pub bid_open: f64,
    pub bid_high: f64,
    pub bid_low: f64,
    pub bid_close: f64,
    pub ask_open: f64,
    pub ask_high: f64,
    pub ask_low: f64,
    pub ask_close: f64,
}

pub fn generate_forex_bars(n: usize) -> Vec<HistoricalForexData> {
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let spread = 0.0002;

    (0..n)
        .map(|i| {
            let base = 1.10 + i as f64 * 0.0001;

            HistoricalForexData {
                pair: "EUR/USD".to_string(),
                time: start + Duration::minutes(5 * i as i64),

                bid_open: base,
                bid_high: base + 0.0003,
                bid_low: base - 0.0003,
                bid_close: base + 0.0001,

                ask_open: base + spread,
                ask_high: base + spread + 0.0003,
                ask_low: base + spread - 0.0003,
                ask_close: base + spread + 0.0001,
            }
        })
        .collect()
}

#[derive(Debug, Clone)]
pub struct EwmMean {
    span: usize,
    idx: usize,
    alpha: f64,
    prev: Option<f64>,
}

impl EwmMean {
    pub fn new(span: usize) -> Self {
        assert!(span > 0);
        let alpha = 2.0 / (span as f64 + 1.0);
        Self {
            span,
            idx: 0,
            alpha,
            prev: None,
        }
    }

    /// Push a new value.
    /// Returns Some(ewm) once initialized (immediately after first push), otherwise None.
    pub fn push(&mut self, value: f64) -> Option<f64> {
        self.idx += 1;

        let current = match self.prev {
            None => value, // first point: match pandas adjust=False recursion behavior
            Some(p) => self.alpha * value + (1.0 - self.alpha) * p,
        };

        self.prev = Some(current);
        Some(current)
    }

    /// Returns the last computed EWM mean (if any)
    pub fn value(&self) -> Option<f64> {
        self.prev
    }

    pub fn alpha(&self) -> f64 {
        self.alpha
    }

    pub fn span(&self) -> usize {
        self.span
    }

    pub fn idx(&self) -> usize {
        self.idx
    }
}

fn main() {
    let bars = generate_forex_bars(10_529);

    let mid_price_bars: Vec<MidPriceBar> = bars
        .iter()
        .map(|bar| MidPriceBar {
            mid_open: (bar.bid_open + bar.ask_open) / 2.0,
            mid_high: (bar.bid_high + bar.ask_high) / 2.0,
            mid_low: (bar.bid_low + bar.ask_low) / 2.0,
            mid_close: (bar.bid_close + bar.ask_close) / 2.0,
        })
        .collect();
    fn log_return<I>(arr: I) -> Vec<f64>
    where
        I: IntoIterator<Item = f64>,
    {
        let ln_arr: Vec<f64> = arr.into_iter().map(|price| price.ln()).collect();
        ln_arr.windows(2).map(|w| w[1] - w[0]).collect()
    }
    let mid_ret = log_return(mid_price_bars.iter().map(|bar| bar.mid_close));
    let bid_ret = log_return(bars.iter().map(|bar| bar.bid_close));
    let ask_ret = log_return(bars.iter().map(|bar| bar.ask_close));

    let mut rolling_std = RollingStd::new(30);
    let rv_30: Vec<f64> = mid_ret
        .iter()
        .filter_map(|elem| rolling_std.push(*elem))
        .collect();

    // ===================================
    // How extreme volatility is currently
    // ===================================
    let mut rolling_rv_30_z = RollingZScore::new(500);
    // let mut rolling_rv_30_sum = RollingSum::new(500);
    // let mut rolling_rv_30_std = RollingStd::new(500);
    let rv_30_abs = rv_30.iter().filter_map(|elem| {
        rolling_rv_30_z.push(*elem)
        // let rolling_sum = rolling_rv_30_sum.push(*elem);
        // rolling_rv_30_std.push(*elem);
        // if rolling_sum.is_none() {
        //     return None;
        // }
        //
        // Some((elem - rolling_sum.unwrap() / 500.0) / rolling_rv_30_std.rolling_std().unwrap())
    });

    let rv_30_log = rv_30.iter().map(|elem| {
        // println!("{:?}", elem);
        elem.ln()
    });
    // ======================================
    // How unusual is the multiplicative move
    // ======================================
    // let mut rolling_rv_30_log_sum = RollingSum::new(500);
    // let mut rolling_rv_30_log_std = RollingStd::new(500);
    let mut rolling_rv_30_log_z = RollingZScore::new(500);
    let rv_30_rel = rv_30_log.filter_map(|elem| {
        rolling_rv_30_log_z.push(elem)
        // let rolling_sum = rolling_rv_30_log_sum.push(elem);
        // rolling_rv_30_log_std.push(elem);
        // rolling_rv_30_log_z.push(elem);
        // if rolling_sum.is_none() {
        //     return None;
        // }
        //
        // Some(
        //     (elem - rolling_sum.unwrap() / 500.0)
        //         / rolling_rv_30_log_std.rolling_std().unwrap(),
        // )
    });

    let rv_z = zip(rv_30_rel, rv_30_abs).map(|(rv_z_rel, rv_z_abs)| rv_z_rel - LAMBDA * rv_z_abs);
    let mut ranker = RollingRankPct::new(10_000);
    // for i in rv_z.clone().collect::<Vec<f64>>().iter().rev().take(30) {
    //     println!("{:?}", i);
    // }
    let rv_q: Vec<f64> = rv_z
        .filter_map(|elem| {
            println!("{elem:?}");
            ranker.push(elem)
        })
        .collect();
    ranker.push(1380.067);
    // let rv_q_params = RvqParams {
    //     rv_q_ranker: ranker,
    //     rv_30: rolling_std,
    //     rolling_rv_30_z,
    //     rolling_rv_30_log_z,
    // };

    // ==============
    // RANGE HIGH/LOW
    // ==============
    let mut range_low = RollingMin::new(30);
    let mut range_high = RollingMax::new(30);
    for mid_price_bar in mid_price_bars.iter().rev().take(30).rev() {
        range_low.push(mid_price_bar.mid_low);
        range_high.push(mid_price_bar.mid_high);
        for _ in 1..20 {
            range_high.replace_last(mid_price_bar.mid_high);
        }
    }
    // for i in 0..20 {
    //     range_high.push(2.1532 - 0.01 * (i+1) as f64);
    // }
    // for i in 0..20 {
    //     range_high.replace_last(2.1532-0.01*20.0+0.0001*(i+1) as f64);
    // }
    // for i in 0..9 {
    //     range_high.push(0.0);
    // }
    // println!("{:?}", range_high.max());
    // range_high.push(0.0);
    // // range_high.push(0.0);
    // // range_high.push(0.0);
    // println!("{:?}", range_high.max());
    // let range_params = RangeParams {
    //     range_high_deque: range_high,
    //     range_low_deque: range_low,
    // };

    // ==============
    // FLOW IMBALANCE
    // ==============
    let mut ask_ret_deque = RollingSum::new(5);
    let mut bid_ret_deque = RollingSum::new(5);
    for ask_ret_val in ask_ret.iter().rev().take(5).rev() {
        ask_ret_deque.push(*ask_ret_val);
    }
    for bid_ret_val in bid_ret.iter().rev().take(5).rev() {
        bid_ret_deque.push(*bid_ret_val);
    }

    let mut ewm = EwmMean::new(20);
    // for close in mid_ret.iter().rev().take(100).rev() {
    for (idx, close) in mid_ret.iter().enumerate().take(100) {
        ewm.push(*close);
        // match ewm.value() {
        //     Some(ans) => {
        //         println!("{:?}: {ans:?}", idx+1);
        //     }
        //     None => {
        //         continue;
        //     }
        // }
    }
    // let flow_imbalance_params = FlowImbalanceParams {
    //     ask_ret_deque,
    //     bid_ret_deque,
    // };
    let flow_imb = ask_ret_deque.rolling_sum().unwrap() - bid_ret_deque.rolling_sum().unwrap();

    println!(
        "{}",
        format!(
            "{}, {}, {}, {}",
            ranker.percentile().unwrap(),
            range_high.max().unwrap(),
            range_low.min().unwrap(),
            flow_imb
        )
    );
    println!(
        "{}, {}",
        ask_ret_deque.rolling_sum().unwrap(),
        bid_ret_deque.rolling_sum().unwrap()
    );
    println!("{:?}", ewm.value());
}
