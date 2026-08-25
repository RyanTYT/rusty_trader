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
//! - **The walk-forward tracking** — the per-window IS/OOS Sharpe + the
//!   ratio. A real edge has a stable ratio across windows; an overfit edge
//!   degrades.
//!
//! The report is written as a self-contained HTML file (hand-rolled SVG
//! charts + data tables) for easy viewing in a browser.

use std::collections::HashMap;

use serde::Serialize;

use trading_app::backtester::BacktestResults;

use crate::functions::optimizer::EvalResult;
use crate::runner::run::WalkForwardResult;

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

#[derive(Debug, Clone, Serialize)]
pub struct RobustnessReport {
    pub pareto_front: Vec<ParetoPoint>,
    pub stability: Vec<StabilityPoint>,
    pub walk_forward_tracking: Option<Vec<WalkForwardPoint>>,
}

// ─── Computation ─────────────────────────────────────────────────────────

impl RobustnessReport {
    /// Build the report from a holdout `OptResult` (the `all` candidates +
    /// the top-K with neighborhoods).
    pub fn from_holdout(all: &[EvalResult]) -> Self {
        let refs: Vec<&EvalResult> = all.iter().collect();
        let pareto_front = compute_pareto_front(&refs);
        let stability = compute_stability(&refs);
        Self {
            pareto_front,
            stability,
            walk_forward_tracking: None,
        }
    }

    /// Build the report from a `WalkForwardResult` (the per-window bests +
    /// the tracking).
    pub fn from_walk_forward(wf: &WalkForwardResult) -> Self {
        let candidates: Vec<&EvalResult> = wf.per_window.iter().map(|w| &w.best).collect();
        let pareto_front = compute_pareto_front(&candidates);
        let stability = compute_stability(&candidates);
        let walk_forward_tracking = Some(compute_walk_forward_tracking(wf));
        Self {
            pareto_front,
            stability,
            walk_forward_tracking,
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
            let rows: Vec<(HashMap<String, f64>, Vec<(String, f64)>)> = self.pareto_front.iter().map(|p| {
                (p.params.clone(), vec![
                    ("Sharpe".to_string(), p.sharpe),
                    ("Return %".to_string(), p.total_return_pct),
                    ("MaxDD %".to_string(), p.max_drawdown_pct),
                    ("Score".to_string(), p.score),
                ])
            }).collect();
            html.push_str(&params_table("pareto", &rows));
        }

        // Stability.
        html.push_str("<h2>Stability (neighborhood std per top-K candidate)</h2>");
        html.push_str("<p>Low std = a plateau (robust edge). High std = a spike (overfit). The MAD (median absolute deviation) is the robust dispersion.</p>");
        html.push_str(&self.stability_bars_svg());
        {
            let rows: Vec<(HashMap<String, f64>, Vec<(String, f64)>)> = self.stability.iter().map(|s| {
                (s.params.clone(), vec![
                    ("Own Sharpe".to_string(), s.own_sharpe),
                    ("Mean".to_string(), s.mean),
                    ("Std".to_string(), s.std),
                    ("MAD".to_string(), s.mad),
                    ("Min".to_string(), s.min),
                    ("Max".to_string(), s.max),
                ])
            }).collect();
            html.push_str(&params_table("stability", &rows));
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

/// Compute the Pareto front: the non-dominated candidates (Sharpe max, return
/// max, drawdown min). A candidate A dominates B if A is ≥ B on all + > B on
/// at least one.
fn compute_pareto_front(candidates: &[&EvalResult]) -> Vec<ParetoPoint> {
    let pts: Vec<(f64, f64, f64, f64, f64, &HashMap<String, f64>)> = candidates
        .iter()
        .map(|e| {
            (
                e.results.sharpe_per_bar,
                e.results.total_return_pct,
                e.results.max_drawdown_pct,
                e.results.sortino_per_bar,
                e.score,
                &e.params,
            )
        })
        .collect();
    let mut front = Vec::new();
    for (i, a) in pts.iter().enumerate() {
        let dominated = pts.iter().enumerate().any(|(j, b)| {
            i != j
                && b.0 >= a.0 // sharpe
                && b.1 >= a.1 // return
                && b.2 <= a.2 // drawdown (min)
                && (b.0 > a.0 || b.1 > a.1 || b.2 < a.2)
        });
        if !dominated {
            front.push(ParetoPoint {
                params: a.5.clone(),
                sharpe: a.0,
                total_return_pct: a.1,
                max_drawdown_pct: a.2,
                sortino: a.3,
                score: a.4,
            });
        }
    }
    front
}

/// Compute the stability: the neighborhood metrics per candidate (only those
/// with a non-empty neighborhood — the top-K).
fn compute_stability(candidates: &[&EvalResult]) -> Vec<StabilityPoint> {
    candidates
        .iter()
        .filter(|e| !e.neighborhood.is_empty())
        .map(|e| {
            let mut sharpes: Vec<f64> = e.neighborhood.iter().map(|r| r.sharpe_per_bar).collect();
            sharpes.push(e.results.sharpe_per_bar); // include the candidate's own.
            let n = sharpes.len() as f64;
            let mean = sharpes.iter().sum::<f64>() / n;
            let var = sharpes.iter().map(|s| (s - mean).powi(2)).sum::<f64>() / n;
            let std = var.sqrt();
            let mad = median_abs_dev(&sharpes);
            let min = sharpes.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = sharpes.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
            StabilityPoint {
                params: e.params.clone(),
                own_sharpe: e.results.sharpe_per_bar,
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
            let is_sharpe = w.best.results.sharpe_per_bar;
            let os_sharpe = w.oos.as_ref().map(|o| o.sharpe_per_bar).unwrap_or(0.0);
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
    let mut svg = format!(
        "<svg width='{w}' height='{h}' xmlns='http://www.w3.org/2000/svg'>"
    );
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
    let bar_w = (pw as f64 / bars.len() as f64) * 0.8;
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
        let x = margin as f64 + (i as f64 + 0.1) * (pw as f64 / bars.len() as f64);
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
    let all: Vec<(f64, f64)> = lines.iter().flat_map(|(_, pts)| pts.iter().copied()).collect();
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
    let mut html = String::from("<table id='wf'><tr><th>Window</th><th>IS Sharpe</th><th>OOS Sharpe</th><th>Ratio</th></tr>");
    for t in tracking {
        html.push_str(&format!(
            "<tr><td>{}</td><td>{:.4}</td><td>{:.4}</td><td>{:.2}</td></tr>",
            t.window, t.is_sharpe, t.os_sharpe, t.ratio,
        ));
    }
    html.push_str("</table>");
    html
}
