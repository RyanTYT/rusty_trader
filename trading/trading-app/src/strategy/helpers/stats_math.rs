//! Pure-`f64` statistical helpers for the `stats_logger` strategy.
//!
//! These are leaf functions operating on `&[f64]` slices of (daily or 5-min)
//! returns / prices — no dependency on the rest of the crate. They cover the
//! one-shot, non-streaming computations that the strategy can't get from the
//! rolling helpers in `rolling_fn.rs`: full-sample moments, VaR/CVaR,
//! Sharpe/Sortino/Calmar, max drawdown, variance ratio, Hurst, autocorrelation,
//! Parkinson/Garman-Klass volatility, Amihud/Roll liquidity, OLS regression,
//! cross-sectional rank/z, Jacobi-eigen PCA, and GARCH(1,1) MLE.
//!
//! NaN is the sentinel for "insufficient data"; callers should propagate it
//! into the output tables as a blank/null rather than panicking.

use std::f64::consts::PI;

// =========================================================================
// Basic moments
// =========================================================================

pub fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

pub fn sum(xs: &[f64]) -> f64 {
    xs.iter().sum()
}

/// Population variance (divisor n).
pub fn variance_population(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let m = mean(xs);
    xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / xs.len() as f64
}

/// Sample variance (divisor n-1).
pub fn variance_sample(xs: &[f64]) -> f64 {
    if xs.len() < 2 {
        return f64::NAN;
    }
    let m = mean(xs);
    xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64
}

/// Sample standard deviation (divisor n-1). Matches pandas `.std(ddof=1)`.
pub fn std_sample(xs: &[f64]) -> f64 {
    variance_sample(xs).sqrt()
}

/// Sample skewness — matches pandas `.skew()` (bias-corrected g1).
/// Returns NaN for n < 3 or zero variance.
pub fn skewness(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n < 3 {
        return f64::NAN;
    }
    let m = mean(xs);
    let s = std_sample(xs);
    if s == 0.0 || !s.is_finite() {
        return f64::NAN;
    }
    let sum3 = xs.iter().map(|x| ((x - m) / s).powi(3)).sum::<f64>();
    (n as f64 / ((n - 1) as f64 * (n - 2) as f64)) * sum3
}

/// Excess kurtosis — matches pandas `.kurt()` (Fisher, normal → 0).
/// Returns NaN for n < 4 or zero variance.
pub fn kurtosis_excess(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n < 4 {
        return f64::NAN;
    }
    let m = mean(xs);
    let s = std_sample(xs);
    if s == 0.0 || !s.is_finite() {
        return f64::NAN;
    }
    let sum4 = xs.iter().map(|x| ((x - m) / s).powi(4)).sum::<f64>();
    let factor1 = (n as f64 * (n + 1) as f64) / ((n - 1) as f64 * (n - 2) as f64 * (n - 3) as f64);
    let factor2 = 3.0 * ((n - 1) as f64).powi(2) / ((n - 2) as f64 * (n - 3) as f64);
    factor1 * sum4 - factor2
}

// =========================================================================
// Returns
// =========================================================================

/// ln(p_t / p_{t-1}) for each consecutive pair. Skips non-positive prices.
pub fn log_returns(prices: &[f64]) -> Vec<f64> {
    let mut out = Vec::with_capacity(prices.len().saturating_sub(1));
    for w in prices.windows(2) {
        if w[0] > 0.0 && w[1] > 0.0 {
            out.push((w[1] / w[0]).ln());
        }
    }
    out
}

/// p_t / p_{t-1} - 1 for each consecutive pair. Skips non-positive prices.
pub fn pct_returns(prices: &[f64]) -> Vec<f64> {
    let mut out = Vec::with_capacity(prices.len().saturating_sub(1));
    for w in prices.windows(2) {
        if w[0] > 0.0 && w[1] > 0.0 {
            out.push(w[1] / w[0] - 1.0);
        }
    }
    out
}

/// Total return over the last `h` bars: prices[last]/prices[last-h] - 1.
pub fn last_return_over(prices: &[f64], h: usize) -> Option<f64> {
    if prices.len() <= h || h == 0 {
        return None;
    }
    let prev = *prices.get(prices.len() - 1 - h)?;
    let last = *prices.last()?;
    if prev <= 0.0 || last <= 0.0 {
        return None;
    }
    Some(last / prev - 1.0)
}

// =========================================================================
// OLS regression: y on x (with intercept)
// =========================================================================

#[derive(Debug, Clone)]
pub struct LinReg {
    pub alpha: f64,
    pub beta: f64,
    pub rsq: f64,
    pub corr: f64,
    pub n: usize,
}

/// OLS of `y` on `x` (intercept included). `x` = market/benchmark returns,
/// `y` = instrument returns. None if n < 2 or x has no variance.
pub fn linreg(x: &[f64], y: &[f64]) -> Option<LinReg> {
    let n = x.len().min(y.len());
    if n < 2 {
        return None;
    }
    let mx = mean(&x[..n]);
    let my = mean(&y[..n]);
    let (mut sxx, mut syy, mut sxy) = (0.0, 0.0, 0.0);
    for i in 0..n {
        let dx = x[i] - mx;
        let dy = y[i] - my;
        sxx += dx * dx;
        syy += dy * dy;
        sxy += dx * dy;
    }
    if sxx <= 0.0 {
        return None;
    }
    let beta = sxy / sxx;
    let alpha = my - beta * mx;
    let corr = if syy <= 0.0 {
        0.0
    } else {
        sxy / (sxx.sqrt() * syy.sqrt())
    };
    let rsq = corr * corr;
    Some(LinReg { alpha, beta, rsq, corr, n })
}

/// Residuals y_i - (alpha + beta·x_i).
pub fn residuals(x: &[f64], y: &[f64], alpha: f64, beta: f64) -> Vec<f64> {
    x.iter()
        .zip(y.iter())
        .map(|(xi, yi)| yi - (alpha + beta * xi))
        .collect()
}

/// Mean + t-stat of the residual series (idiosyncratic momentum a la
/// Gutierrez-Pirinsky 2007). Returns (mean_resid, t_stat, std_resid).
pub fn residual_momentum_stats(resid: &[f64]) -> (f64, f64, f64) {
    let n = resid.len();
    if n < 2 {
        return (f64::NAN, f64::NAN, f64::NAN);
    }
    let m = mean(resid);
    let s = std_sample(resid);
    if s <= 0.0 || !s.is_finite() || n == 0 {
        return (m, f64::NAN, s);
    }
    let t = m / (s / (n as f64).sqrt());
    (m, t, s)
}

// =========================================================================
// Risk / drawdown / VaR
// =========================================================================

/// Max drawdown of a price/equity series as a non-positive fraction
/// (0.0 = no drawdown, -0.25 = 25% peak-to-trough).
pub fn max_drawdown(prices: &[f64]) -> f64 {
    if prices.is_empty() {
        return 0.0;
    }
    let mut peak = prices[0];
    let mut max_dd = 0.0;
    for &p in prices {
        if p > peak {
            peak = p;
        }
        if peak > 0.0 {
            let dd = p / peak - 1.0;
            if dd < max_dd {
                max_dd = dd;
            }
        }
    }
    max_dd
}

/// Historical lower-tail quantile. `pct=0.05` → 5th-percentile return (the
/// 95% VaR loss, a negative number). NaN if empty.
pub fn historical_var(xs: &[f64], pct: f64) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let mut s: Vec<f64> = xs.to_vec();
    s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let p = pct.clamp(0.0, 1.0);
    let idx = ((p * (s.len() - 1) as f64).floor() as usize).min(s.len() - 1);
    s[idx]
}

/// Historical CVaR / Expected Shortfall: mean of the tail at-or-below the
/// `pct`-quantile. NaN if empty.
pub fn historical_cvar(xs: &[f64], pct: f64) -> f64 {
    if xs.is_empty() {
        return f64::NAN;
    }
    let var = historical_var(xs, pct);
    if var.is_nan() {
        return f64::NAN;
    }
    let tail: Vec<f64> = xs.iter().cloned().filter(|x| *x <= var).collect();
    if tail.is_empty() {
        var
    } else {
        mean(&tail)
    }
}

/// Annualized Sharpe ratio (rf=0). `ppy` = periods per year (252 daily,
/// 252*78 ≈ 19656 for 5-min). NaN if zero variance.
pub fn sharpe(returns: &[f64], ppy: f64) -> f64 {
    let s = std_sample(returns);
    if s <= 0.0 || !s.is_finite() {
        return f64::NAN;
    }
    (mean(returns) / s) * ppy.sqrt()
}

/// Annualized Sortino ratio (downside-deviation denominator). NaN if no
/// downside or zero downside-dev.
pub fn sortino(returns: &[f64], ppy: f64) -> f64 {
    let m = mean(returns);
    let downside: Vec<f64> = returns.iter().filter(|r| **r < 0.0).map(|r| r.powi(2)).collect();
    if downside.is_empty() {
        return f64::NAN;
    }
    let dvar = downside.iter().sum::<f64>() / downside.len() as f64;
    let dstd = dvar.sqrt();
    if dstd <= 0.0 || !dstd.is_finite() {
        return f64::NAN;
    }
    (m / dstd) * ppy.sqrt()
}

/// Calmar ratio = annualized (geometric) return / |max drawdown| from a
/// price series. `ppy` = periods per year. NaN if no drawdown.
pub fn calmar(prices: &[f64], ppy: f64) -> f64 {
    if prices.len() < 2 {
        return f64::NAN;
    }
    let start = prices[0];
    let end = *prices.last().unwrap();
    if start <= 0.0 {
        return f64::NAN;
    }
    let n = (prices.len() - 1) as f64;
    let ann_ret = (end / start).powf(ppy / n) - 1.0;
    let mdd = max_drawdown(prices).abs();
    if mdd <= 0.0 || !mdd.is_finite() {
        return f64::NAN;
    }
    ann_ret / mdd
}

/// Tail ratio = mean of gains at/above the 95th pct / |mean of losses at/below 5th pct|.
pub fn tail_ratio(returns: &[f64]) -> f64 {
    if returns.len() < 20 {
        return f64::NAN;
    }
    let q_hi = historical_var(returns, 0.95);
    let q_lo = historical_var(returns, 0.05);
    let gains: Vec<f64> = returns.iter().cloned().filter(|r| *r >= q_hi).collect();
    let losses: Vec<f64> = returns.iter().cloned().filter(|r| *r <= q_lo).collect();
    let mg = mean(&gains);
    let ml = mean(&losses);
    if mg.is_nan() || ml.is_nan() || ml.abs() < 1e-18 {
        return f64::NAN;
    }
    mg / ml.abs()
}

// =========================================================================
// Lo-MacKinlay (1988) variance ratio
// =========================================================================

/// Returns (VR(q), z-stat). VR = Var(q-period return) / (q·Var(1-period)).
/// VR>1 → trending (positive autocorrelation), VR<1 → mean-reverting.
/// z is the homoskedastic LM test statistic (~N(0,1) under the random-walk
/// null). None if too few observations.
pub fn variance_ratio(returns: &[f64], q: usize) -> Option<(f64, f64)> {
    let n = returns.len();
    if n < q + 4 || q < 2 {
        return None;
    }
    let rbar = mean(returns);
    let sigma1: f64 = returns.iter().map(|r| (r - rbar).powi(2)).sum::<f64>() / n as f64;
    if sigma1 <= 0.0 {
        return None;
    }
    // q-period overlapping returns R_t(q) = sum_{j=0..q-1} r_{t-j}, for t=q-1..n-1.
    let m = n - q + 1;
    let mut rs: Vec<f64> = Vec::with_capacity(m);
    for t in (q - 1)..n {
        let s: f64 = (0..q).map(|j| returns[t - j]).sum();
        rs.push(s);
    }
    let rmean = mean(&rs);
    let sigmaq: f64 = rs.iter().map(|r| (r - rmean).powi(2)).sum::<f64>() / m as f64;
    let vr = sigmaq / (q as f64 * sigma1);
    let var_factor = 2.0 * (2.0 * q as f64 - 1.0) * (q as f64 - 1.0) / (3.0 * q as f64 * n as f64);
    let z = if var_factor > 0.0 {
        (vr - 1.0) / var_factor.sqrt()
    } else {
        0.0
    };
    Some((vr, z))
}

// =========================================================================
// Autocorrelation
// =========================================================================

/// Lag-`lag` autocorrelation of returns (Pearson, with mean subtraction).
pub fn autocorr(returns: &[f64], lag: usize) -> f64 {
    let n = returns.len();
    if n < lag + 2 || lag == 0 {
        return f64::NAN;
    }
    let m = mean(returns);
    let mut num = 0.0;
    let mut den = 0.0;
    for t in 0..n {
        let d = returns[t] - m;
        den += d * d;
        if t >= lag {
            num += d * (returns[t - lag] - m);
        }
    }
    if den <= 0.0 {
        return f64::NAN;
    }
    num / den
}

// =========================================================================
// Hurst exponent (R/S analysis)
// =========================================================================

/// Hurst exponent via rescaled-range analysis. Partitions the series into
/// blocks of several scales, computes average R/S per scale, and fits
/// log(R/S) = H·log(scale) + c. H<0.5 mean-reverting, ≈0.5 random, >0.5
/// trending. NaN if too few points (< 32) or insufficient scales.
pub fn hurst_rs(returns: &[f64]) -> f64 {
    let n = returns.len();
    if n < 32 {
        return f64::NAN;
    }
    // Scales: largest power-of-2 down to 16, plus intermediates halved.
    let mut scales: Vec<usize> = Vec::new();
    let mut s = n;
    while s >= 16 {
        scales.push(s);
        s /= 2;
    }
    scales.sort();
    scales.dedup();
    if scales.len() < 3 {
        return f64::NAN;
    }
    let mut xs: Vec<f64> = Vec::new();
    let mut ys: Vec<f64> = Vec::new();
    for &k in &scales {
        let nblocks = n / k;
        if nblocks < 1 {
            continue;
        }
        let mut rs_sum = 0.0;
        let mut cnt = 0;
        for b in 0..nblocks {
            let block = &returns[b * k..b * k + k];
            let m = mean(block);
            let mut cum = 0.0;
            let mut zmin = f64::INFINITY;
            let mut zmax = f64::NEG_INFINITY;
            for &r in block {
                cum += r - m;
                if cum < zmin {
                    zmin = cum;
                }
                if cum > zmax {
                    zmax = cum;
                }
            }
            let rrange = zmax - zmin;
            let sd = std_sample(block);
            if sd > 0.0 && rrange.is_finite() {
                rs_sum += rrange / sd;
                cnt += 1;
            }
        }
        if cnt > 0 {
            xs.push((k as f64).ln());
            ys.push((rs_sum / cnt as f64).ln());
        }
    }
    if xs.len() < 3 {
        return f64::NAN;
    }
    linreg(&xs, &ys).map(|r| r.beta).unwrap_or(f64::NAN)
}

// =========================================================================
// Parkinson / Garman-Klass (OHLC volatility estimators)
// =========================================================================

/// Parkinson (1980) variance estimator from intraday highs/lows:
/// mean of 0.5·(ln(H/L))². Annualize by ×ppy outside. NaN if no valid bars.
pub fn parkinson_var(highs: &[f64], lows: &[f64]) -> f64 {
    let n = highs.len().min(lows.len());
    if n == 0 {
        return f64::NAN;
    }
    let (mut acc, mut cnt) = (0.0, 0);
    for i in 0..n {
        if highs[i] > 0.0 && lows[i] > 0.0 && highs[i] >= lows[i] {
            acc += 0.5 * (highs[i] / lows[i]).ln().powi(2);
            cnt += 1;
        }
    }
    if cnt == 0 {
        f64::NAN
    } else {
        acc / cnt as f64
    }
}

/// Garman-Klass (1980) variance estimator from OHLC:
/// 0.5·(ln(H/L))² − (2·ln2 − 1)·(ln(C/O))², averaged over bars. NaN if none.
pub fn garman_klass_var(opens: &[f64], highs: &[f64], lows: &[f64], closes: &[f64]) -> f64 {
    let n = opens.len().min(highs.len()).min(lows.len()).min(closes.len());
    if n == 0 {
        return f64::NAN;
    }
    let (mut acc, mut cnt) = (0.0, 0);
    let coef = 2.0 * (2f64.ln()) - 1.0;
    for i in 0..n {
        if opens[i] > 0.0 && highs[i] > 0.0 && lows[i] > 0.0 && closes[i] > 0.0 && highs[i] >= lows[i] {
            let hl = (highs[i] / lows[i]).ln();
            let co = (closes[i] / opens[i]).ln();
            acc += 0.5 * hl * hl - coef * co * co;
            cnt += 1;
        }
    }
    if cnt == 0 {
        f64::NAN
    } else {
        acc / cnt as f64
    }
}

// =========================================================================
// Liquidity: Amihud illiquidity + Roll effective spread
// =========================================================================

/// Amihud (2002) illiquidity: mean of |r_t| / dollar_volume_t (skipping
/// zero-volume bars). Higher = less liquid. NaN if no valid bars.
pub fn amihud(returns: &[f64], dollar_volumes: &[f64]) -> f64 {
    let n = returns.len().min(dollar_volumes.len());
    if n == 0 {
        return f64::NAN;
    }
    let (mut acc, mut cnt) = (0.0, 0);
    for i in 0..n {
        if dollar_volumes[i] > 0.0 {
            acc += returns[i].abs() / dollar_volumes[i];
            cnt += 1;
        }
    }
    if cnt == 0 {
        f64::NAN
    } else {
        acc / cnt as f64
    }
}

/// Roll (1984) effective bid-ask spread proxy:
/// 2·sqrt(−Cov(r_t, r_{t−1})) if the serial covariance is negative, else 0
/// (a positive serial covariance is inconsistent with Roll's model). NaN if
/// too few returns.
pub fn roll_spread(returns: &[f64]) -> f64 {
    let n = returns.len();
    if n < 3 {
        return f64::NAN;
    }
    let m = mean(returns);
    let mut cov = 0.0;
    let mut cnt = 0;
    for t in 1..n {
        cov += (returns[t] - m) * (returns[t - 1] - m);
        cnt += 1;
    }
    cov /= cnt as f64;
    if cov >= 0.0 {
        return 0.0;
    }
    2.0 * (-cov).sqrt()
}

// =========================================================================
// Cross-sectional rank / z-score
// =========================================================================

/// Cross-sectional rank percentile of `value` among `peers` ∈ [0, 1].
/// Ties get the average rank. 0 = worst, 1 = best.
pub fn cross_sectional_rank_pct(value: f64, peers: &[f64]) -> f64 {
    if peers.is_empty() {
        return f64::NAN;
    }
    let below = peers.iter().filter(|p| **p < value).count();
    let eq = peers.iter().filter(|p| **p == value).count();
    (below as f64 + 0.5 * eq as f64) / peers.len() as f64
}

/// Cross-sectional z-score of `value` against `peers` (sample std). 0 if
/// zero dispersion.
pub fn cross_sectional_zscore(value: f64, peers: &[f64]) -> f64 {
    let m = mean(peers);
    let s = std_sample(peers);
    if s <= 0.0 || !s.is_finite() {
        return 0.0;
    }
    (value - m) / s
}

// =========================================================================
// Jacobi eigenvalue decomposition (symmetric) — for PCA
// =========================================================================

/// Jacobi eigenvalue algorithm for a symmetric `n×n` matrix (row-major).
/// Returns `(eigenvalues, eigenvectors)` sorted DESCENDING by eigenvalue.
/// `eigenvectors[k]` is the k-th eigenvector (length n). O(n³) per sweep,
/// converges in ~10 sweeps for n=40. None if the matrix is non-square.
pub fn jacobi_eigen_symmetric(matrix: &[Vec<f64>]) -> Option<(Vec<f64>, Vec<Vec<f64>>)> {
    let n = matrix.len();
    if n == 0 {
        return None;
    }
    for row in matrix {
        if row.len() != n {
            return None;
        }
    }
    let mut a: Vec<Vec<f64>> = matrix.to_vec();
    let mut v: Vec<Vec<f64>> = (0..n)
        .map(|i| {
            let mut r = vec![0.0; n];
            r[i] = 1.0;
            r
        })
        .collect();
    let max_sweeps = 100;
    let tol = 1e-12;
    for _ in 0..max_sweeps {
        // sum of off-diagonal magnitudes
        let mut off = 0.0;
        for i in 0..n {
            for j in (i + 1)..n {
                off += a[i][j].abs();
            }
        }
        if off < tol {
            break;
        }
        for p in 0..n {
            for q in (p + 1)..n {
                let apq = a[p][q];
                if apq.abs() < 1e-18 {
                    continue;
                }
                let app = a[p][p];
                let aqq = a[q][q];
                let theta = (aqq - app) / (2.0 * apq);
                // t = sign(theta)/(|theta| + sqrt(theta²+1)); if theta==0, t=1.
                let t = if theta >= 0.0 {
                    1.0 / (theta + (1.0 + theta * theta).sqrt())
                } else {
                    -1.0 / (-theta + (1.0 + theta * theta).sqrt())
                };
                let c = 1.0 / (1.0 + t * t).sqrt();
                let s = t * c;
                // Apply rotation to A (symmetric).
                a[p][p] = app - t * apq;
                a[q][q] = aqq + t * apq;
                a[p][q] = 0.0;
                a[q][p] = 0.0;
                for i in 0..n {
                    if i != p && i != q {
                        let aip = a[i][p];
                        let aiq = a[i][q];
                        a[i][p] = c * aip - s * aiq;
                        a[p][i] = a[i][p];
                        a[i][q] = s * aip + c * aiq;
                        a[q][i] = a[i][q];
                    }
                    let vip = v[i][p];
                    let viq = v[i][q];
                    v[i][p] = c * vip - s * viq;
                    v[i][q] = s * vip + c * viq;
                }
            }
        }
    }
    let mut eigs: Vec<(f64, usize)> = (0..n).map(|i| (a[i][i], i)).collect();
    eigs.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let eigvals: Vec<f64> = eigs.iter().map(|(v, _)| *v).collect();
    let eigvecs: Vec<Vec<f64>> = eigs
        .iter()
        .map(|(_, i)| (0..n).map(|r| v[r][*i]).collect())
        .collect();
    Some((eigvals, eigvecs))
}

/// PCA from a return matrix: `asset_returns[i]` = daily returns of asset i
/// (equal length L). Standardizes each asset (sample std), builds the n×n
/// correlation matrix, eigendecomposes. Returns `(eigenvalues desc,
/// eigenvectors)` where `eigenvectors[k][i]` = loading of asset i on PC(k).
/// None if < 2 assets or < 2 observations.
pub fn pca_from_returns(asset_returns: &[Vec<f64>]) -> Option<(Vec<f64>, Vec<Vec<f64>>)> {
    let n = asset_returns.len();
    if n < 2 {
        return None;
    }
    let l = asset_returns[0].len();
    if l < 2 {
        return None;
    }
    for r in asset_returns {
        if r.len() != l {
            return None;
        }
    }
    // Standardize per asset.
    let mut z = vec![vec![0.0; l]; n];
    for i in 0..n {
        let m = mean(&asset_returns[i]);
        let s = std_sample(&asset_returns[i]);
        let sd = if s > 0.0 && s.is_finite() { s } else { 1.0 };
        for t in 0..l {
            z[i][t] = (asset_returns[i][t] - m) / sd;
        }
    }
    // Correlation matrix C[i][j] = (1/L) Σ_t z_i z_j.
    let mut corr = vec![vec![0.0; n]; n];
    for i in 0..n {
        for j in i..n {
            let mut acc = 0.0;
            for t in 0..l {
                acc += z[i][t] * z[j][t];
            }
            let c = acc / l as f64;
            corr[i][j] = c;
            corr[j][i] = c;
        }
    }
    jacobi_eigen_symmetric(&corr)
}

// =========================================================================
// GARCH(1,1) MLE
// =========================================================================

#[derive(Debug, Clone)]
pub struct GarchFit {
    pub omega: f64,
    pub alpha: f64,
    pub beta: f64,
    pub ll: f64,
    pub unc_var: f64,
    pub half_life: f64,
    pub forecast_1d: f64,
}

/// GARCH(1,1) MLE with Gaussian innovations: σ²_t = ω + α·r²_{t-1} + β·σ²_{t-1}.
/// Maximizes the Gaussian log-likelihood over (ω, α, β) with constraints
/// ω>0, α≥0, β≥0, α+β < 0.9999 (stationarity), via a Nelder-Mead simplex.
/// σ²_0 seeded with the sample variance. Returns None if < 30 returns.
pub fn garch11_fit(returns: &[f64]) -> Option<GarchFit> {
    let n = returns.len();
    if n < 30 {
        return None;
    }
    let var0 = variance_sample(returns).max(1e-12);

    // Negative log-likelihood (minimize this).
    let neg_ll = |params: &[f64]| -> f64 {
        let omega = params[0];
        let alpha = params[1];
        let beta = params[2];
        if omega <= 0.0
            || alpha < 0.0
            || beta < 0.0
            || alpha + beta >= 0.9999
            || alpha + beta < 0.0
        {
            return 1e18;
        }
        let mut sig2 = var0;
        let mut ll = 0.0;
        for &r in returns {
            sig2 = omega + alpha * r * r + beta * sig2;
            if !(sig2 > 0.0) || !sig2.is_finite() {
                return 1e18;
            }
            ll += -0.5 * (2.0 * PI.ln() + sig2.ln() + (r * r) / sig2);
        }
        -ll
    };

    let start = vec![0.1 * var0, 0.1, 0.85];
    let result = nelder_mead(&neg_ll, start, 1e-7, 4000);
    let omega = result[0];
    let alpha = result[1];
    let beta = result[2];
    let ll = -neg_ll(&result);
    let persistence = alpha + beta;
    let unc = if persistence < 0.9999 && persistence > 0.0 {
        omega / (1.0 - persistence)
    } else {
        f64::NAN
    };
    // Half-life of variance shocks: variance decays toward unc at rate
    // persistence^h, so h_{1/2} = ln(0.5)/ln(persistence).
    let hl = if persistence > 0.0 && persistence < 1.0 {
        (0.5f64).ln() / persistence.ln()
    } else {
        f64::NAN
    };
    // 1-day-ahead forecast σ²_{T+1} = ω + α·r²_T + β·σ²_T.
    let mut sig2 = var0;
    for &r in returns {
        sig2 = omega + alpha * r * r + beta * sig2;
    }
    let fcast = sig2;
    Some(GarchFit {
        omega,
        alpha,
        beta,
        ll,
        unc_var: unc,
        half_life: hl,
        forecast_1d: fcast,
    })
}

/// h-day-ahead cumulative variance forecast (sum of daily variances σ²_{T+1..T+h}).
/// σ²_{T+1} = fit.forecast_1d; σ²_{T+k} = ω + (α+β)·σ²_{T+k-1} for k ≥ 2
/// (the forecast of r²_{T+k} IS σ²_{T+k}).
pub fn garch_forecast_cumvar(fit: &GarchFit, horizon: usize) -> f64 {
    if horizon == 0 {
        return 0.0;
    }
    let mut sig2 = fit.forecast_1d;
    let mut total = sig2;
    let persistence = fit.alpha + fit.beta;
    for _ in 1..horizon {
        sig2 = fit.omega + persistence * sig2;
        total += sig2;
    }
    total
}

// =========================================================================
// Nelder-Mead simplex (for the GARCH MLE)
// =========================================================================

/// Minimize `f` over `n` dimensions via a Nelder-Mead simplex. `start` is the
/// initial vertex (length n). Returns the best vertex found. Parameters:
/// reflection α=1, expansion γ=2, contraction ρ=0.5, shrink σ=0.5.
fn nelder_mead(f: &dyn Fn(&[f64]) -> f64, start: Vec<f64>, tol: f64, max_iter: usize) -> Vec<f64> {
    let n = start.len();
    if n == 0 {
        return start;
    }
    let alpha = 1.0;
    let gamma = 2.0;
    let rho = 0.5;
    let sigma = 0.5;

    // Build the initial simplex: start + n perturbed vertices.
    let mut simplex: Vec<(Vec<f64>, f64)> = Vec::with_capacity(n + 1);
    simplex.push((start.clone(), f(&start)));
    for i in 0..n {
        let mut p = start.clone();
        p[i] = if p[i].abs() < 1e-12 { 1e-6 } else { p[i] * 1.05 + 1e-6 };
        simplex.push((p.clone(), f(&p)));
    }

    for _ in 0..max_iter {
        simplex.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        let best = simplex[0].1;
        let worst = simplex[n].1;
        if (worst - best).abs() < tol {
            break;
        }
        // Centroid of all but the worst.
        let mut centroid = vec![0.0; n];
        for i in 0..n {
            for j in 0..n {
                centroid[j] += simplex[i].0[j];
            }
        }
        for j in 0..n {
            centroid[j] /= n as f64;
        }
        // Reflection.
        let mut xr = vec![0.0; n];
        for j in 0..n {
            xr[j] = centroid[j] + alpha * (centroid[j] - simplex[n].0[j]);
        }
        let fr = f(&xr);
        if fr >= simplex[0].1 && fr < simplex[n - 1].1 {
            simplex[n] = (xr, fr);
            continue;
        }
        if fr < simplex[0].1 {
            // Expansion.
            let mut xe = vec![0.0; n];
            for j in 0..n {
                xe[j] = centroid[j] + gamma * (xr[j] - centroid[j]);
            }
            let fe = f(&xe);
            simplex[n] = if fe < fr { (xe, fe) } else { (xr, fr) };
            continue;
        }
        // Contraction.
        let mut xc = vec![0.0; n];
        for j in 0..n {
            xc[j] = centroid[j] + rho * (simplex[n].0[j] - centroid[j]);
        }
        let fc = f(&xc);
        if fc < simplex[n].1 {
            simplex[n] = (xc, fc);
            continue;
        }
        // Shrink toward the best.
        let bestp = simplex[0].0.clone();
        for i in 1..=n {
            let mut p = vec![0.0; n];
            for j in 0..n {
                p[j] = bestp[j] + sigma * (simplex[i].0[j] - bestp[j]);
            }
            simplex[i] = (p.clone(), f(&p));
        }
    }
    simplex.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    simplex[0].0.clone()
}
