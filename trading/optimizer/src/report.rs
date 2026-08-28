//! The robustness report — the analysis layer for understanding the
//! optimization results. Produces:
//!
//! - **The Pareto front** — the non-dominated candidates across Sharpe /
//!   return / drawdown. For exploration (seeing the trade-offs). The
//!   optimizer uses `RobustSharpe` (single objective); the Pareto front is
//!   reported for insight, not selection.
//! - **The stability analysis** — the neighborhood metrics per top-K
//!   candidate (mean / std / MAD / min / max of the neighborhood Sharpes).
//!   A stable candidate is a plateau (low std); an overfit candidate is a
//!   spike (high std).
//! - **The equity curves** — the best candidate's IS equity vs the OOS
//!   equity (holdout), or the compounded aggregated OOS equity across all
//!   walk-forward windows. Plus an optional overlay of the Pareto-front
//!   candidates' IS equity curves (colored by total return).
//! - **The walk-forward tracking** — the per-window IS/OOS Sharpe + the
//!   ratio. A real edge has a stable ratio across windows; an overfit edge
//!   degrades.
//!
//! The report is written as a self-contained HTML file (hand-rolled SVG
//! charts + data tables) for easy viewing in a browser.

use std::collections::HashMap;

use chrono::DateTime;
use serde::Serialize;

use trading_app::backtester::BacktestResults;

use crate::functions::optimizer::EvalResult;
use crate::runner::run::{OptResult, WalkForwardResult};

// ─── Report data ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
pub struct ParetoPoint {
    pub params: HashMap<String, f64>,
    pub sharpe: f64,
    pub total_return_pct: f64,
    pub max_drawdown_pct: f64,
    pub sortino: f64,
    pub score: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StabilityPoint {
    pub params: HashMap<String, f64>,
    pub own_sharpe: f64,
    pub neighborhood_sharpes: Vec<f64>,
    pub mean: f64,
    pub std: f64,
    pub mad: f64,
    pub min: f64,
    pub max: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalkForwardPoint {
    pub window: usize,
    pub is_sharpe: f64,
    pub os_sharpe: f64,
    pub ratio: f64,
}

/// One equity-curve series for the report charts. `points` is `(time, equity)`
/// where time is RFC3339. `color_hint` (CSS color) overrides the palette —
/// used by the Pareto overlay to color by total return.
#[derive(Debug, Clone, Serialize)]
pub struct EquityCurveSeries {
    pub label: String,
    pub points: Vec<(String, f64)>,
    pub color_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RobustnessReport {
    pub pareto_front: Vec<ParetoPoint>,
    pub stability: Vec<StabilityPoint>,
    pub walk_forward_tracking: Option<Vec<WalkForwardPoint>>,
    /// The main equity chart: best IS + OOS (holdout), or the compounded
    /// aggregated OOS (walk-forward).
    pub equity_curves: Vec<EquityCurveSeries>,
    /// The Pareto-front candidates' IS equity curves (holdout only — each
    /// candidate shares the same IS period, so overlaying on one time axis is
    /// meaningful). Colored by total return. Empty for walk-forward.
    pub pareto_equity_curves: Vec<EquityCurveSeries>,
}

// ─── Computation ─────────────────────────────────────────────────────────

impl RobustnessReport {
    /// Build the report from a holdout `OptResult` (the `all` candidates +
    /// the best + the out-of-sample validation).
    pub fn from_holdout(result: &OptResult) -> Self {
        let refs: Vec<&EvalResult> = result.all.iter().collect();
        let pareto_refs = pareto_front_refs(&refs);
        let pareto_front = pareto_refs
            .iter()
            .map(|e| ParetoPoint {
                params: e.params.clone(),
                sharpe: e.results.sharpe,
                total_return_pct: e.results.total_return_pct,
                max_drawdown_pct: e.results.max_drawdown_pct,
                sortino: e.results.sortino,
                score: e.score,
            })
            .collect();
        let stability = compute_stability(&refs);

        // Main equity chart: best IS + OOS.
        let mut equity_curves = Vec::with_capacity(2);
        equity_curves.push(EquityCurveSeries {
            label: "IS (best)".to_string(),
            points: result
                .best
                .results
                .equity_curve
                .iter()
                .map(|p| (p.time.clone(), p.equity))
                .collect(),
            color_hint: None,
        });
        if let Some(oos) = &result.out_of_sample {
            equity_curves.push(EquityCurveSeries {
                label: "OOS".to_string(),
                points: oos
                    .equity_curve
                    .iter()
                    .map(|p| (p.time.clone(), p.equity))
                    .collect(),
                color_hint: None,
            });
        }

        // Pareto overlay: each non-dominated candidate's IS equity curve,
        // colored by total return (red = high, blue = low).
        let returns: Vec<f64> = pareto_refs
            .iter()
            .map(|e| e.results.total_return_pct)
            .collect();
        let (rmin, rmax): (f64, f64) = returns.iter().fold(
            (f64::INFINITY, f64::NEG_INFINITY),
            |(mn, mx): (f64, f64), &r| (mn.min(r), mx.max(r)),
        );
        let pareto_equity_curves = pareto_refs
            .iter()
            .enumerate()
            .map(|(i, e)| {
                let r = e.results.total_return_pct;
                let norm = if (rmax - rmin).abs() < 1e-9 {
                    0.5
                } else {
                    (r - rmin) / (rmax - rmin)
                };
                // hue 0 = red (high return), 240 = blue (low return).
                let hue = (1.0 - norm) * 240.0;
                EquityCurveSeries {
                    label: format!("P{i} ret {r:.1}%"),
                    points: e
                        .results
                        .equity_curve
                        .iter()
                        .map(|p| (p.time.clone(), p.equity))
                        .collect(),
                    color_hint: Some(format!("hsl({hue:.0}, 70%, 50%)")),
                }
            })
            .collect();

        Self {
            pareto_front,
            stability,
            walk_forward_tracking: None,
            equity_curves,
            pareto_equity_curves,
        }
    }

    /// Build the report from a `WalkForwardResult` (the per-window bests +
    /// the tracking + the aggregated OOS).
    pub fn from_walk_forward(wf: &WalkForwardResult) -> Self {
        let candidates: Vec<&EvalResult> = wf.per_window.iter().map(|w| &w.best).collect();
        let pareto_front = compute_pareto_front(&candidates);
        let stability = compute_stability(&candidates);
        let walk_forward_tracking = Some(compute_walk_forward_tracking(wf));
        let equity_curves = vec![EquityCurveSeries {
            label: "Aggregated OOS".to_string(),
            points: build_aggregated_oos_curve(wf),
            color_hint: None,
        }];
        Self {
            pareto_front,
            stability,
            walk_forward_tracking,
            equity_curves,
            // Skip the Pareto overlay for walk-forward — the per-window bests
            // have different IS periods, so overlaying their IS equity curves
            // on one time axis would tile discontinuously.
            pareto_equity_curves: Vec::new(),
        }
    }

    /// Write the report as a self-contained HTML file (hand-rolled SVG charts
    /// + data tables). Returns the HTML string.
    pub fn to_html(&self, title: &str) -> String {
        let mut html = String::new();
        html.push_str("<!DOCTYPE html><html><head><meta charset='utf-8'>");
        html.push_str(&format!("<title>{title}</title>"));
        html.push_str("<style>");
        html.push_str("body{font-family:system-ui,sans-serif;margin:2em;max-width:1200px}");
        html.push_str("h1,h2{color:#333} table{border-collapse:collapse;margin:1em 0} th,td{border:1px solid #ccc;padding:4px 8px;text-align:right} th{background:#eee;text-align:left}");
        html.push_str(".chart{margin:2em 0;border:1px solid #eee;padding:1em}");
        html.push_str("</style></head><body>");
        html.push_str(&format!("<h1>{title}</h1>"));

        // Pareto front.
        html.push_str("<h2>Pareto front (Sharpe vs drawdown, colored by return)</h2>");
        html.push_str("<p>Non-dominated candidates across Sharpe / return / max-drawdown. A point on the front is not strictly worse than any other on all 3 metrics.</p>");
        html.push_str(&self.pareto_scatter_svg());
        {
            let rows: Vec<(HashMap<String, f64>, Vec<(String, f64)>)> = self
                .pareto_front
                .iter()
                .map(|p| {
                    (
                        p.params.clone(),
                        vec![
                            ("Sharpe".to_string(), p.sharpe),
                            ("Return %".to_string(), p.total_return_pct),
                            ("MaxDD %".to_string(), p.max_drawdown_pct),
                            ("Score".to_string(), p.score),
                        ],
                    )
                })
                .collect();
            html.push_str(&params_table("pareto", &rows));
        }

        // Stability.
        html.push_str("<h2>Stability (neighborhood std per top-K candidate)</h2>");
        html.push_str("<p>Low std = a plateau (robust edge). High std = a spike (overfit). The MAD (median absolute deviation) is the robust dispersion.</p>");
        html.push_str(&self.stability_bars_svg());
        {
            let rows: Vec<(HashMap<String, f64>, Vec<(String, f64)>)> = self
                .stability
                .iter()
                .map(|s| {
                    (
                        s.params.clone(),
                        vec![
                            ("Own Sharpe".to_string(), s.own_sharpe),
                            ("Mean".to_string(), s.mean),
                            ("Std".to_string(), s.std),
                            ("MAD".to_string(), s.mad),
                            ("Min".to_string(), s.min),
                            ("Max".to_string(), s.max),
                        ],
                    )
                })
                .collect();
            html.push_str(&params_table("stability", &rows));
        }

        // Equity curves (main chart).
        if !self.equity_curves.is_empty() {
            html.push_str("<h2>Equity curves</h2>");
            html.push_str("<p>Best candidate's in-sample equity vs the out-of-sample equity (holdout), or the compounded aggregated OOS equity across all walk-forward windows (scale-invariant per-bar returns chained off the starting capital).</p>");
            html.push_str(&svg_equity(
                &self.equity_curves,
                "Time",
                "Equity",
                900,
                400,
                0.9,
            ));
        }

        // Pareto overlay (IS equity curves).
        if !self.pareto_equity_curves.is_empty() {
            html.push_str("<h2>Pareto front — IS equity curves</h2>");
            html.push_str("<p>Each line is one non-dominated candidate's in-sample equity curve. Color = total return (red = high, blue = low).</p>");
            html.push_str(&svg_equity(
                &self.pareto_equity_curves,
                "Time",
                "Equity",
                900,
                400,
                0.5,
            ));
        }

        // Walk-forward tracking.
        if let Some(tracking) = &self.walk_forward_tracking {
            html.push_str("<h2>Walk-forward tracking (IS vs OOS Sharpe per window)</h2>");
            html.push_str("<p>A real edge has OOS tracking IS across windows (stable ratio). An overfit edge degrades (ratio drops).</p>");
            html.push_str(&self.tracking_lines_svg(tracking));
            html.push_str(&wf_table(tracking));
        }

        html.push_str("</body></html>");
        html
    }

    fn pareto_scatter_svg(&self) -> String {
        let points: Vec<(f64, f64, f64)> = self
            .pareto_front
            .iter()
            .map(|p| (p.max_drawdown_pct, p.sharpe, p.total_return_pct))
            .collect();
        svg_scatter(&points, "MaxDD %", "Sharpe", "Return %", 600, 400)
    }

    fn stability_bars_svg(&self) -> String {
        let bars: Vec<(String, f64)> = self
            .stability
            .iter()
            .enumerate()
            .map(|(i, s)| (format!("C{}", i + 1), s.std))
            .collect();
        svg_bars(&bars, "Neighborhood std", 600, 400)
    }

    fn tracking_lines_svg(&self, tracking: &[WalkForwardPoint]) -> String {
        let is: Vec<(f64, f64)> = tracking
            .iter()
            .map(|t| (t.window as f64, t.is_sharpe))
            .collect();
        let os: Vec<(f64, f64)> = tracking
            .iter()
            .map(|t| (t.window as f64, t.os_sharpe))
            .collect();
        svg_lines(
            &[("IS Sharpe", is), ("OOS Sharpe", os)],
            "Window",
            "Sharpe",
            600,
            400,
        )
    }
}

/// The non-dominated candidates (Sharpe max, return max, drawdown min). A
/// candidate A dominates B if A is ≥ B on all + > B on at least one. Returns
/// references so callers can pull both the scalar metrics (ParetoPoint) AND
/// the equity curve (Pareto overlay) from the same front.
fn pareto_front_refs<'a>(candidates: &[&'a EvalResult]) -> Vec<&'a EvalResult> {
    candidates
        .iter()
        .enumerate()
        .filter(|(i, a)| {
            !candidates.iter().enumerate().any(|(j, b)| {
                *i != j
                    && b.results.sharpe >= a.results.sharpe
                    && b.results.total_return_pct >= a.results.total_return_pct
                    && b.results.max_drawdown_pct <= a.results.max_drawdown_pct
                    && (b.results.sharpe > a.results.sharpe
                        || b.results.total_return_pct > a.results.total_return_pct
                        || b.results.max_drawdown_pct < a.results.max_drawdown_pct)
            })
        })
        .map(|(_, e)| *e)
        .collect()
}

/// Compute the Pareto front: the non-dominated candidates (Sharpe max, return
/// max, drawdown min). A candidate A dominates B if A is ≥ B on all + > B on
/// at least one.
fn compute_pareto_front(candidates: &[&EvalResult]) -> Vec<ParetoPoint> {
    pareto_front_refs(candidates)
        .iter()
        .map(|e| ParetoPoint {
            params: e.params.clone(),
            sharpe: e.results.sharpe,
            total_return_pct: e.results.total_return_pct,
            max_drawdown_pct: e.results.max_drawdown_pct,
            sortino: e.results.sortino,
            score: e.score,
        })
        .collect()
}

/// Compute the stability: the neighborhood metrics per candidate (only those
/// with a non-empty neighborhood — the top-K).
fn compute_stability(candidates: &[&EvalResult]) -> Vec<StabilityPoint> {
    candidates
        .iter()
        .filter(|e| !e.neighborhood.is_empty())
        .map(|e| {
            let mut sharpes: Vec<f64> = e.neighborhood.iter().map(|r| r.sharpe).collect();
            sharpes.push(e.results.sharpe); // include the candidate's own.
            let n = sharpes.len() as f64;
            let mean = sharpes.iter().sum::<f64>() / n;
            let var = sharpes.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / n;
            let std = var.sqrt();
            let mad = median_abs_dev(&sharpes);
            let min = sharpes.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = sharpes.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            StabilityPoint {
                params: e.params.clone(),
                own_sharpe: e.results.sharpe,
                neighborhood_sharpes: sharpes,
                mean,
                std,
                mad,
                min,
                max,
            }
        })
        .collect()
}

/// Compute the walk-forward tracking (per-window IS/OOS Sharpe + ratio).
fn compute_walk_forward_tracking(wf: &WalkForwardResult) -> Vec<WalkForwardPoint> {
    wf.per_window
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let is_sharpe = w.best.results.sharpe;
            let os_sharpe = w.oos.as_ref().map(|o| o.sharpe).unwrap_or(0.0);
            let ratio = if is_sharpe.abs() > 1e-9 {
                os_sharpe / is_sharpe
            } else {
                0.0
            };
            WalkForwardPoint {
                window: i + 1,
                is_sharpe,
                os_sharpe,
                ratio,
            }
        })
        .collect()
}

/// Reconstruct the compounded aggregated OOS equity curve from the per-window
/// OOS equity curves. Mirrors `run::compute_aggregated_oos`: per-bar returns
/// (scale-invariant) are chained off `aggregated_oos.starting_capital`. The
/// curve is seeded with the first window's first OOS timestamp so the plot
/// starts at the beginning of the OOS span.
fn build_aggregated_oos_curve(wf: &WalkForwardResult) -> Vec<(String, f64)> {
    let mut curve = Vec::new();
    let mut equity = wf.aggregated_oos.starting_capital;
    let mut seeded = false;
    for w in &wf.per_window {
        let Some(oos) = &w.oos else {
            continue;
        };
        if oos.equity_curve.is_empty() {
            continue;
        }
        if !seeded {
            curve.push((oos.equity_curve[0].time.clone(), equity));
            seeded = true;
        }
        for pair in oos.equity_curve.windows(2) {
            let prev = pair[0].equity;
            let cur = pair[1].equity;
            if prev.abs() > 1e-9 {
                equity *= 1.0 + (cur - prev) / prev;
            }
            curve.push((pair[1].time.clone(), equity));
        }
    }
    curve
}

/// Median absolute deviation (MAD).
fn median_abs_dev(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let median = {
        let mut s = values.to_vec();
        s.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        if s.len() % 2 == 0 {
            (s[s.len() / 2 - 1] + s[s.len() / 2]) / 2.0
        } else {
            s[s.len() / 2]
        }
    };
    let mut devs: Vec<f64> = values.iter().map(|v| (v - median).abs()).collect();
    devs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    if devs.is_empty() {
        0.0
    } else if devs.len() % 2 == 0 {
        (devs[devs.len() / 2 - 1] + devs[devs.len() / 2]) / 2.0
    } else {
        devs[devs.len() / 2]
    }
}

// ─── Hand-rolled SVG charts ───────────────────────────────────────────────

fn scale(val: f64, min: f64, max: f64, range: f64) -> f64 {
    if (max - min).abs() < 1e-9 {
        range / 2.0
    } else {
        (val - min) / (max - min) * range
    }
}

/// Parse an RFC3339 time string to Unix milliseconds (for axis scaling).
/// Returns `None` on parse failure (the point is then skipped).
fn time_to_millis(s: &str) -> Option<f64> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.timestamp_millis() as f64)
}

fn svg_scatter(
    points: &[(f64, f64, f64)],
    x_label: &str,
    y_label: &str,
    color_label: &str,
    w: u32,
    h: u32,
) -> String {
    let margin = 50;
    let pw = w - 2 * margin;
    let ph = h - 2 * margin;
    let (xmin, xmax) = points
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.0), mx.max(p.0))
        });
    let (ymin, ymax) = points
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.1), mx.max(p.1))
        });
    let (cmin, cmax) = points
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.2), mx.max(p.2))
        });
    let mut svg = format!("<svg width='{w}' height='{h}' xmlns='http://www.w3.org/2000/svg'>");
    // Axes.
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{}' x2='{}' y2='{}' stroke='#333'/>",
        h - margin,
        w - margin,
        h - margin,
    ));
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{margin}' x2='{margin}' y2='{}' stroke='#333'/>",
        h - margin,
    ));
    svg.push_str(&format!(
        "<text x='{}' y='{}' text-anchor='middle'>{x_label}</text>",
        w / 2,
        h - 10,
    ));
    svg.push_str(&format!(
        "<text x='15' y='{}' transform='rotate(-90 15 {})' text-anchor='middle'>{y_label}</text>",
        h / 2,
        h / 2,
    ));
    // Points.
    for (x, y, c) in points {
        let sx = margin as f64 + scale(*x, xmin, xmax, pw as f64);
        let sy = (h - margin) as f64 - scale(*y, ymin, ymax, ph as f64);
        let color_val = scale(*c, cmin, cmax, 1.0);
        let color = format!("hsl({}, 70%, 50%)", (1.0 - color_val) * 240.0);
        svg.push_str(&format!(
            "<circle cx='{:.1}' cy='{:.1}' r='5' fill='{color}' opacity='0.8'/>",
            sx, sy,
        ));
    }
    // Legend (color).
    svg.push_str(&format!(
        "<text x='{}' y='25' text-anchor='end'>{color_label}</text>",
        w - margin,
    ));
    svg.push_str("</svg>");
    svg
}

fn svg_bars(bars: &[(String, f64)], y_label: &str, w: u32, h: u32) -> String {
    let margin = 50;
    let pw = w - 2 * margin;
    let ph = h - 2 * margin;
    let ymax = bars.iter().map(|b| b.1).fold(0.0_f64, f64::max).max(0.001);
    let bar_w = (pw as f64 / bars.len().max(1) as f64) * 0.8;
    let mut svg = format!("<svg width='{w}' height='{h}' xmlns='http://www.w3.org/2000/svg'>");
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{}' x2='{}' y2='{}' stroke='#333'/>",
        h - margin,
        w - margin,
        h - margin,
    ));
    svg.push_str(&format!(
        "<text x='15' y='{}' transform='rotate(-90 15 {})' text-anchor='middle'>{y_label}</text>",
        h / 2,
        h / 2,
    ));
    for (i, (label, val)) in bars.iter().enumerate() {
        let bh = scale(*val, 0.0, ymax, ph as f64);
        let x = margin as f64 + (i as f64 + 0.1) * (pw as f64 / bars.len().max(1) as f64);
        let y = (h - margin) as f64 - bh;
        svg.push_str(&format!(
            "<rect x='{:.1}' y='{:.1}' width='{:.1}' height='{:.1}' fill='steelblue'/>",
            x, y, bar_w, bh,
        ));
        svg.push_str(&format!(
            "<text x='{:.1}' y='{}' text-anchor='middle' font-size='10'>{label}</text>",
            x + bar_w / 2.0,
            h - margin + 15,
        ));
    }
    svg.push_str("</svg>");
    svg
}

fn svg_lines(
    lines: &[(&str, Vec<(f64, f64)>)],
    x_label: &str,
    y_label: &str,
    w: u32,
    h: u32,
) -> String {
    let margin = 50;
    let pw = w - 2 * margin;
    let ph = h - 2 * margin;
    let all: Vec<(f64, f64)> = lines
        .iter()
        .flat_map(|(_, pts)| pts.iter().copied())
        .collect();
    let (xmin, xmax) = all
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.0), mx.max(p.0))
        });
    let (ymin, ymax) = all
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.1), mx.max(p.1))
        });
    let colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"];
    let mut svg = format!("<svg width='{w}' height='{h}' xmlns='http://www.w3.org/2000/svg'>");
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{}' x2='{}' y2='{}' stroke='#333'/>",
        h - margin,
        w - margin,
        h - margin,
    ));
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{margin}' x2='{margin}' y2='{}' stroke='#333'/>",
        h - margin,
    ));
    svg.push_str(&format!(
        "<text x='{}' y='{}' text-anchor='middle'>{x_label}</text>",
        w / 2,
        h - 10,
    ));
    svg.push_str(&format!(
        "<text x='15' y='{}' transform='rotate(-90 15 {})' text-anchor='middle'>{y_label}</text>",
        h / 2,
        h / 2,
    ));
    for (li, (label, pts)) in lines.iter().enumerate() {
        let color = colors[li % colors.len()];
        let points_str: String = pts
            .iter()
            .map(|(x, y)| {
                let sx = margin as f64 + scale(*x, xmin, xmax, pw as f64);
                let sy = (h - margin) as f64 - scale(*y, ymin, ymax, ph as f64);
                format!("{sx:.1},{sy:.1}")
            })
            .collect::<Vec<_>>()
            .join(" ");
        svg.push_str(&format!(
            "<polyline points='{points_str}' fill='none' stroke='{color}' stroke-width='2'/>",
        ));
        // Legend.
        svg.push_str(&format!(
            "<line x1='{}' y1='{}' x2='{}' y2='{}' stroke='{color}' stroke-width='2'/>",
            w - margin,
            25 + li * 15,
            w - margin + 20,
            25 + li * 15,
        ));
        svg.push_str(&format!(
            "<text x='{}' y='{}' font-size='12'>{label}</text>",
            w - margin + 25,
            30 + li * 15,
        ));
    }
    svg.push_str("</svg>");
    svg
}

/// A multi-series equity-curve chart. X = real RFC3339 time (parsed to millis
/// for scaling, formatted as YYYY-MM-DD on the ticks). Y = equity. One
/// polyline per series; `color_hint` (per series) overrides the palette.
/// `opacity` is applied to the polylines (use < 1.0 to faint the Pareto
/// overlay). The legend auto-hides when there are > 6 series (e.g. a large
/// Pareto front) to avoid overflowing the plot.
fn svg_equity(
    series: &[EquityCurveSeries],
    x_label: &str,
    y_label: &str,
    w: u32,
    h: u32,
    opacity: f64,
) -> String {
    if series.is_empty() {
        return "<p>(no equity curve data)</p>".to_string();
    }
    let pts: Vec<(f64, f64)> = series
        .iter()
        .flat_map(|s| {
            s.points
                .iter()
                .filter_map(|(t, e)| time_to_millis(t).map(|ts| (ts, *e)))
        })
        .collect();
    if pts.is_empty() {
        return "<p>(no equity curve data)</p>".to_string();
    }
    let (xmin, xmax) = pts
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.0), mx.max(p.0))
        });
    let (ymin, ymax) = pts
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(mn, mx), p| {
            (mn.min(p.1), mx.max(p.1))
        });
    let margin = 70;
    let pw = w - 2 * margin;
    let ph = h - 2 * margin;
    let colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
    ];
    let show_legend = series.len() <= 6;

    let mut svg = format!("<svg width='{w}' height='{h}' xmlns='http://www.w3.org/2000/svg'>");
    // Axes.
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{}' x2='{}' y2='{}' stroke='#333'/>",
        h - margin,
        w - margin,
        h - margin,
    ));
    svg.push_str(&format!(
        "<line x1='{margin}' y1='{margin}' x2='{margin}' y2='{}' stroke='#333'/>",
        h - margin,
    ));
    // X-axis ticks (6, formatted YYYY-MM-DD).
    for i in 0..=5 {
        let t = xmin + (xmax - xmin) * (i as f64 / 5.0);
        let sx = margin as f64 + scale(t, xmin, xmax, pw as f64);
        svg.push_str(&format!(
            "<line x1='{sx:.1}' y1='{}' x2='{sx:.1}' y2='{}' stroke='#ccc'/>",
            h - margin,
            h - margin + 5,
        ));
        let lbl = DateTime::from_timestamp_millis(t as i64)
            .map(|d| d.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        svg.push_str(&format!(
            "<text x='{sx:.1}' y='{}' text-anchor='middle' font-size='10'>{lbl}</text>",
            h - margin + 18,
        ));
    }
    // Y-axis ticks (5).
    for i in 0..=5 {
        let v = ymin + (ymax - ymin) * (i as f64 / 5.0);
        let sy = (h - margin) as f64 - scale(v, ymin, ymax, ph as f64);
        svg.push_str(&format!(
            "<line x1='{margin}' y1='{sy:.1}' x2='{}' y2='{sy:.1}' stroke='#ccc'/>",
            margin - 5,
        ));
        svg.push_str(&format!(
            "<text x='{}' y='{:.1}' text-anchor='end' font-size='10'>{v:.0}</text>",
            margin - 8,
            sy + 3.0,
        ));
    }
    // Axis labels.
    svg.push_str(&format!(
        "<text x='{}' y='{}' text-anchor='middle'>{x_label}</text>",
        w / 2,
        h - 5,
    ));
    svg.push_str(&format!(
        "<text x='15' y='{}' transform='rotate(-90 15 {})' text-anchor='middle'>{y_label}</text>",
        h / 2,
        h / 2,
    ));
    // Series polylines.
    for (si, s) in series.iter().enumerate() {
        let color = s
            .color_hint
            .clone()
            .unwrap_or_else(|| colors[si % colors.len()].to_string());
        let pts_str: String = s
            .points
            .iter()
            .filter_map(|(t, e)| {
                time_to_millis(t).map(|ts| {
                    let sx = margin as f64 + scale(ts, xmin, xmax, pw as f64);
                    let sy = (h - margin) as f64 - scale(*e, ymin, ymax, ph as f64);
                    format!("{sx:.1},{sy:.1}")
                })
            })
            .collect::<Vec<_>>()
            .join(" ");
        svg.push_str(&format!(
            "<polyline points='{pts_str}' fill='none' stroke='{color}' stroke-width='2' opacity='{opacity}'/>",
        ));
        if show_legend {
            svg.push_str(&format!(
                "<line x1='{}' y1='{}' x2='{}' y2='{}' stroke='{color}' stroke-width='2'/>",
                w - margin,
                25 + si * 15,
                w - margin + 20,
                25 + si * 15,
            ));
            svg.push_str(&format!(
                "<text x='{}' y='{}' font-size='12'>{}</text>",
                w - margin + 25,
                30 + si * 15,
                s.label,
            ));
        }
    }
    svg.push_str("</svg>");
    svg
}

/// A params + metrics table.
fn params_table(id: &str, rows: &[(HashMap<String, f64>, Vec<(String, f64)>)]) -> String {
    if rows.is_empty() {
        return format!("<p>(no data)</p>");
    }
    let mut html = format!("<table id='{id}'>");
    // Header: params + metrics.
    html.push_str("<tr><th>Params</th>");
    for (m, _) in &rows[0].1 {
        html.push_str(&format!("<th>{m}</th>"));
    }
    html.push_str("</tr>");
    for (params, metrics) in rows {
        let params_str = params
            .iter()
            .map(|(k, v)| format!("{k}={v:.4}"))
            .collect::<Vec<_>>()
            .join(", ");
        html.push_str(&format!("<tr><td>{params_str}</td>"));
        for (_, val) in metrics {
            html.push_str(&format!("<td>{val:.4}</td>"));
        }
        html.push_str("</tr>");
    }
    html.push_str("</table>");
    html
}

fn wf_table(tracking: &[WalkForwardPoint]) -> String {
    let mut html = String::from(
        "<table id='wf'><tr><th>Window</th><th>IS Sharpe</th><th>OOS Sharpe</th><th>Ratio</th></tr>",
    );
    for t in tracking {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{:.4}</td><td>{:.4}</td><td>{:.2}</td></tr>",
            t.window, t.is_sharpe, t.os_sharpe, t.ratio,
        ));
    }
    html.push_str("</table>");
    html
}
