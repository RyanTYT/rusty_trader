use std::{
    fmt::{Debug, Display},
    sync::mpsc,
    thread,
    time::Duration,
};

#[derive(Debug)]
pub enum TimeoutError<E> {
    Timeout,
    Function(E),
    WorkerPanic,
}
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
        Err(mpsc::RecvTimeoutError::Disconnected) => Err(TimeoutError::Timeout),
    }
}
