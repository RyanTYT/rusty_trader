//! Shared helpers for the execution test binary.

use std::sync::{Mutex, MutexGuard};

use tempfile::TempDir;
use trading_app::execution::fx_backed_up_order::OrderStore;

static ENV_LOCK: Mutex<()> = Mutex::new(());

/// One temp redb store per test, with `ORDERS_FILE_PATH` set for the process.
/// The guard must outlive the store, so keep the whole tuple alive.
pub fn open_store() -> (TempDir, MutexGuard<'static, ()>, OrderStore) {
    let guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("orders.redb");
    unsafe {
        std::env::set_var("ORDERS_FILE_PATH", &path);
    }
    let store = OrderStore::open().expect("open order store");
    (dir, guard, store)
}
