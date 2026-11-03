//! §8 diagnostic seams (env-gated, default OFF, test/probe only).
//!
//! `BLENDED5_FUND_DIAG=1` — per-event log lines at every money-touching
//! hop of the pooled-emission path: pool pushes, pooled classification,
//! sheet composition, per-delta order construction, and EVERY fill
//! (routed and own). Load-bearing for hypothesis-nailing runs only.
//!
//! `BLENDED5_DIAG_EQ_CSV=<path>` — per-slot per-strategy equity CSV
//! (time_ms, sleeve equity × N, rollup) written by the multi replay.
//!
//! `BLENDED5_DIAG_FILLS_CSV=<path>` — every fill appended as
//! `(time_ms, date_et, emitter, order_ref, routed_owner, sym, action,
//! fill_qty, fill_price, fees, fx_rate)`.
//!
//! `BLENDED5_DIAG_WINDOW=YYYY-MM-DD:YYYY-MM-DD` — restricts the FILLS CSV
//! to the ET-date window (equity CSV stays whole-horizon; it is the curve
//! the divergence bisect needs).

use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::{Mutex, OnceLock};

fn env_flag(name: &str) -> bool {
    static FLAGS: OnceLock<Vec<(String, bool)>> = OnceLock::new();
    let f = FLAGS.get_or_init(|| {
        let mut v = Vec::new();
        for n in ["BLENDED5_FUND_DIAG", "BLENDED5_DIAG_WINDOW"] {
            let on = std::env::var(n).ok().as_deref().is_some_and(|s| s.trim() == "1");
            v.push((n.to_string(), on));
        }
        v
    });
    f.iter().any(|(n, on)| n == name && *on)
}

pub fn diag_fills() -> bool {
    env_flag("BLENDED5_FUND_DIAG")
}

/// ET-date window for the fills CSV; `None` = unrestricted.
pub fn diag_window() -> Option<(chrono::NaiveDate, chrono::NaiveDate)> {
    static W: OnceLock<Option<(chrono::NaiveDate, chrono::NaiveDate)>> = OnceLock::new();
    *W.get_or_init(|| {
        std::env::var("BLENDED5_DIAG_WINDOW")
            .ok()
            .and_then(|w| {
                let (a, b) = w.split_once(':')?;
                Some((
                    chrono::NaiveDate::parse_from_str(a.trim(), "%Y-%m-%d").ok()?,
                    chrono::NaiveDate::parse_from_str(b.trim(), "%Y-%m-%d").ok()?,
                ))
            })
    })
}

struct Writer(Mutex<Option<BufWriter<File>>>);

fn writer(key: &'static str) -> &'static Writer {
    static EQ: OnceLock<Writer> = OnceLock::new();
    static FILLS: OnceLock<Writer> = OnceLock::new();
    static HYB: OnceLock<Writer> = OnceLock::new();
    let w = match key {
        "BLENDED5_DIAG_EQ_CSV" => &EQ,
        "BLENDED5_HYB_DIAG" => &HYB,
        _ => &FILLS,
    };
    w.get_or_init(|| Writer(Mutex::new(None)))
}

/// Lazily open one CSV writer for `key` (env var holding the path); `header`
/// written on first use only.
fn csv_writer(
    path_env: &'static str,
    header: &str,
) -> Option<std::sync::MutexGuard<'static, Option<BufWriter<File>>>> {
    let mut g = writer(path_env).0.lock().expect("diag writer poisoned");
    if g.is_none() {
        let path = std::env::var(path_env).ok()?;
        let handle = (File::create(&path), path);
        let file = match handle {
            (Ok(f), _) => f,
            ((Err(e), path)) => {
                eprintln!("diag: cannot create {path}: {e}");
                return None;
            }
        };
        let mut bw = BufWriter::new(file);
        let _ = writeln!(bw, "{header}");
        *g = Some(bw);
    }
    g.is_some().then_some(g)
}

/// Flush + drop the writers (end of run). No-op when never opened.
pub fn diag_close() {
    for key in [
        "BLENDED5_DIAG_EQ_CSV",
        "BLENDED5_DIAG_FILLS_CSV",
        "BLENDED5_HYB_DIAG",
    ] {
        let mut g = writer(key).0.lock().expect("diag writer poisoned");
        if let Some(bw) = g.as_mut() {
            let _ = bw.flush();
        }
        *g = None;
    }
}

/// Per-slot equity row the multi replay appends.
pub fn diag_eq_row(time_ms: i64, cells: &[f64], rollup: f64) {
    let Some(mut g) = csv_writer("BLENDED5_DIAG_EQ_CSV", "time_ms,equity_0,equity_1,equity_2,equity_3,rollup")
    else {
        return;
    };
    if let Some(bw) = g.as_mut() {
        let mut line = time_ms.to_string();
        for v in cells {
            line.push_str(&format!(",{v:.4}"));
        }
        let _ = writeln!(bw, "{line},{rollup:.4}");
    }
}

/// Hybrid OCA-pair lifecycle event (BLENDED5_HYB_DIAG path; no-op when
/// unset). One row per event: emit / dedupe_keep / dedupe_replace / no_bar /
/// flat_cancel (sleeve side); mirror_cancel / eod_carry / rebase / converted /
/// limit_fill (reconcile side). `px` carries the fill price for fills, the
/// new close_mso for rebase, 0.0 otherwise.
pub fn hyb_event(
    date: chrono::NaiveDate,
    mso: i32,
    event: &str,
    owner: &str,
    sym: &str,
    qty: f64,
    px: f64,
) {
    let Some(mut g) = csv_writer("BLENDED5_HYB_DIAG", "date,mso,event,owner,sym,qty,px") else {
        return;
    };
    if let Some(bw) = g.as_mut() {
        let _ = writeln!(bw, "{date},{mso},{event},{owner},{sym},{qty:.4},{px:.4}");
    }
}

/// One fill (any branch). `routed_owner` None = settled on the emitter's own
/// account (or no router entry — surfacing that IS the point).
#[allow(clippy::too_many_arguments)]
pub fn diag_fill(
    time_ms: i64,
    date_et: chrono::NaiveDate,
    emitter: &str,
    order_ref: &str,
    routed_owner: Option<&str>,
    sym: &str,
    action: &str,
    fill_qty: f64,
    fill_price: f64,
    fees: f64,
    fx_rate: f64,
) {
    if !diag_fills() {
        return;
    }
    if let Some((d0, d1)) = diag_window() {
        if date_et < d0 || date_et > d1 {
            return;
        }
    }
    let Some(mut g) = csv_writer(
        "BLENDED5_DIAG_FILLS_CSV",
        "time_ms,date_et,emitter,order_ref,routed_owner,sym,action,fill_qty,fill_price,fees,fx_rate",
    ) else {
        return;
    };
    if let Some(bw) = g.as_mut() {
        let _ = writeln!(
            bw,
            "{time_ms},{date_et},{emitter},{order_ref},{},\"{sym}\",{action},{fill_qty:.6},{fill_price:.6},{fees:.4},{fx_rate:.6}",
            routed_owner.unwrap_or("-"),
        );
    }
}
