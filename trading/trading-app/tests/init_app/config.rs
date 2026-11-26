//! `pg_connect_options` (`src/init_app.rs:43-49`) — the only pure config
//! surface in that file that does not open a socket.
//!
//! It reads the deployment profile from the `MEMORY_LIMIT` environment
//! variable (fallback `"small"`, matching the init migration's
//! `DEFAULT_ENV`) and forwards it to Postgres as the `trading_app.env` GUC.
//! There is no value validation or trimming: whatever the env holds is
//! forwarded verbatim. The `DATABASE_URL` argument itself is parsed by sqlx.
//!
//! Environment variables are process-global, so every test below holds one
//! mutex and restores the previous `MEMORY_LIMIT` on drop (including unwind).

use std::ffi::OsString;
use std::sync::{Mutex, MutexGuard};

use trading_app::init_app::pg_connect_options;

const URL: &str = "postgres://alice:s3cret@db.example.com:6543/trading_db";

// ──────────────────────────── env harness ────────────────────────────

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn lock_env() -> MutexGuard<'static, ()> {
    ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

struct EnvGuard {
    key: &'static str,
    prev: Option<OsString>,
}

impl EnvGuard {
    fn set(key: &'static str, value: Option<&str>) -> Self {
        let prev = std::env::var_os(key);
        // SAFETY: every reader of MEMORY_LIMIT in this test binary holds
        // ENV_LOCK, and the guard restores the previous value.
        unsafe {
            match value {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
        Self { key, prev }
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: as in `set`; Drop may run during unwind while the mutex
        // guard is still alive (drop order: `_guard` before `_lock` here).
        unsafe {
            match self.prev.take() {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }
}

fn with_memory_limit<T>(value: Option<&str>, f: impl FnOnce() -> T) -> T {
    let _lock = lock_env();
    let _guard = EnvGuard::set("MEMORY_LIMIT", value);
    f()
}

// ──────────────────────────── tests ────────────────────────────

#[test]
fn memory_limit_profile_forwarding_table() {
    // (MEMORY_LIMIT, expected profile) — presence wins even when empty or
    // invalid; only an UNSET variable falls back to "small".
    let cases: [(Option<&str>, &str); 8] = [
        (None, "small"),
        (Some("local"), "local"),
        (Some("small"), "small"),
        (Some("medium"), "medium"),
        (Some("large"), "large"),
        (Some(""), ""),
        (Some("bogus"), "bogus"),
        (Some(" medium "), " medium "),
    ];

    for (input, profile) in cases {
        let opts = with_memory_limit(input, || pg_connect_options(URL));
        assert_eq!(
            opts.get_options(),
            Some(format!("-c trading_app.env={profile}").as_str()),
            "MEMORY_LIMIT={input:?} must forward {profile:?} as trading_app.env"
        );
    }
}

#[test]
fn database_url_fields_are_parsed() {
    let opts = with_memory_limit(None, || pg_connect_options(URL));
    assert_eq!(opts.get_username(), "alice");
    assert_eq!(opts.get_host(), "db.example.com");
    assert_eq!(opts.get_port(), 6543);
    assert_eq!(opts.get_database(), Some("trading_db"));
}

#[test]
fn preexisting_connect_options_are_preserved_and_appended() {
    // sqlx writes each option as `-c k=v`, space-separated. The migration
    // GUC must be appended, never clobber an existing `options` param.
    let url = "postgres://alice:s3cret@db.example.com:6543/trading_db?options=-c%20foo%3Dbar";
    let opts = with_memory_limit(Some("large"), || pg_connect_options(url));
    assert_eq!(
        opts.get_options(),
        Some("-c foo=bar -c trading_app.env=large")
    );
}

#[test]
#[should_panic(expected = "Expected DATABASE_URL to be a valid Postgres connection string")]
fn unparseable_database_url_panics() {
    let _lock = lock_env();
    let _guard = EnvGuard::set("MEMORY_LIMIT", None);
    let _ = pg_connect_options("not a postgres url");
}
