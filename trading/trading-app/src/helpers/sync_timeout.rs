//! Run a blocking closure on a dedicated thread and bound how long the caller
//! waits for it.
//!
//! The pattern everywhere here: spawn a `std::thread`, signal completion over a
//! channel, then `recv_timeout` (or `recv`). A timeout does **not** cancel the
//! worker — the thread keeps running detached and its eventual result is
//! discarded. That is acceptable for the IBKR calls these wrap (they block on
//! the socket and cannot be cancelled cleanly), but it means a timed-out call
//! can still mutate IBKR state later.
//!
//! Panics in the worker are detected either by `JoinHandle::join` (when the
//! completion signal arrived) or by the channel disconnecting before a send
//! (when it did not).

use std::{
    fmt::{Debug, Display},
    sync::mpsc,
    thread,
    time::Duration,
};

/// Why a `timeout` / `timeout_panic` call did not return the closure's value.
#[derive(Debug)]
pub enum TimeoutError<E> {
    /// The caller's deadline elapsed; the worker is still running detached.
    Timeout,
    /// The closure returned an error.
    Function(E),
    /// The worker thread panicked before signalling completion.
    WorkerPanic,
}

// SAFETY: the enum is `Send` exactly when `E` is; the manual impl exists
// because the auto-derived bound would require `E: Send` on the impl itself,
// which cannot be expressed for a generic enum without the parameter bound
// appearing on every use.
unsafe impl<E: Send> Send for TimeoutError<E> {}

impl<E: Display> Display for TimeoutError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeoutError::Timeout => write!(f, "operation timed out"),
            TimeoutError::Function(e) => write!(f, "function error: {}", e),
            TimeoutError::WorkerPanic => write!(f, "worker panicked"),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for TimeoutError<E> {}

/// Run `func` on a worker thread and fail with [`TimeoutError::Timeout`] if it
/// does not finish within `duration`.
///
/// The worker is not interrupted on timeout; it detaches and its value is
/// dropped when it eventually returns.
///
/// # Errors
///
/// [`TimeoutError::Timeout`] on deadline, [`TimeoutError::Function`] with the
/// closure's error, or [`TimeoutError::WorkerPanic`] if the thread panicked.
pub(crate) fn timeout<F, O, E>(duration: Duration, func: F) -> Result<O, TimeoutError<E>>
where
    F: FnOnce() -> Result<O, E> + Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    let (tx, rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let result = func();
        let _ = tx.send(()); // signal completion
        result
    });

    match rx.recv_timeout(duration) {
        Ok(_) => match handle.join() {
            Ok(inner) => inner.map_err(|e| TimeoutError::Function(e)),
            Err(_) => Err(TimeoutError::WorkerPanic),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => Err(TimeoutError::Timeout),
        // A panicking worker drops the completion sender without sending, so the
        // channel closes while the caller is still waiting.
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(TimeoutError::WorkerPanic),
    }
}

/// Run `func` on a worker thread from async code, without a deadline.
///
/// Awaits the completion signal, then joins the thread to collect the value or
/// panic state. There is no timeout variant here: the point is to keep a
/// blocking library call off the async scheduler, not to bound it. Dropping the
/// future drops the receiver; the worker still runs to completion detached.
pub(crate) async fn async_call<F, O, E>(func: F) -> Result<O, TimeoutError<E>>
where
    F: FnOnce() -> Result<O, E> + Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    let handle = thread::spawn(move || {
        let result = func();
        let _ = tx.blocking_send(()); // signal completion
        result
    });

    match rx.recv().await {
        Some(_) => match handle.join() {
            Ok(inner) => inner.map_err(|e| TimeoutError::Function(e)),
            Err(_) => Err(TimeoutError::WorkerPanic),
        },
        None => Err(TimeoutError::WorkerPanic),
    }
}

/// Like [`timeout`], but a deadline or a worker panic is a **panic** instead of
/// an error, and the closure's own error is returned unchanged.
///
/// Used where the surrounding code treats "the broker did not answer" as an
/// invariant violation rather than a recoverable condition (e.g. contract
/// validation during startup).
///
/// # Panics
///
/// On timeout, worker panic, or a disconnected completion channel.
pub(crate) fn timeout_panic<F, O, E>(duration: Duration, func: F) -> Result<O, E>
where
    F: FnOnce() -> Result<O, E> + Send + 'static,
    O: Send + 'static,
    E: Send + 'static,
{
    let (tx, rx) = mpsc::channel();

    let handle = thread::spawn(move || {
        let result = func();
        let _ = tx.send(()); // signal completion
        result
    });

    match rx.recv_timeout(duration) {
        Ok(_) => match handle.join() {
            Ok(inner) => inner,
            Err(_) => panic!("Failed to complete function with timeout_panic"),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!("Failed to complete timeout_panic - timed out")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("Failed to complete timeout_panic - disconnected")
        }
    }
}
