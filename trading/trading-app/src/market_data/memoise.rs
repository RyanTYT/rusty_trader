//! TTL memoisation of fallible computations.
//!
//! [`Memoized`] wraps a function `I -> Result<V, E>` in a `moka` TTL cache.
//! The cache key is *derived* by `key_fn` instead of requiring `I: Hash + Eq`,
//! so raw inputs such as an [`ibapi::contracts::Contract`] can be cached by a
//! narrower identity than their own equality.
//!
//! # Semantics
//!
//! - `f` always receives the original input, never the derived key.
//! - Concurrent calls for the same key run `f` once: `try_get_with` shares the
//!   in-flight computation instead of stampeding.
//! - Errors are returned but **not cached**, so the next call retries.
//! - Failures are cloned out of the shared `Arc<E>`, hence `E: Clone`.
//! - Entries expire `ttl` after insertion; there is no size bound.

use moka::sync::Cache;
use std::any::Any;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;

// ---------------------------------------------------------------------
// Memoized: I = raw input (may not be Hash/Eq), K = derived cache key
// ---------------------------------------------------------------------
//
// `key_fn` turns the raw input into something hashable/comparable
// (e.g. Contract -> HashContract, f64 -> its bit pattern or a rounded
// string). `f` still receives the *original* input, so the compute
// logic doesn't need to know or care that a derived key exists.

/// A fallible function memoised in a TTL cache.
///
/// - `I` is the raw input type; it needs no `Hash`/`Eq`.
/// - `K` is the derived cache key returned by `key_fn`.
/// - `V` is the cached value; callers receive clones.
/// - `E` is the error type; it must be `Clone + Display` because it is cloned
///   out of the shared result and used for [`AnyMemoized`]'s string errors.
pub struct Memoized<I, K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + Display + 'static,
{
    /// TTL cache keyed by the value produced by `key_fn`.
    cache: Cache<K, V>,
    /// Derives a hashable identity from a raw input without consuming it.
    key_fn: Box<dyn Fn(&I) -> K + Send + Sync>,
    /// The wrapped computation; always receives the original input.
    f: Box<dyn Fn(&I) -> Result<V, E> + Send + Sync>,
}

impl<I, K, V, E> Memoized<I, K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + Display + 'static,
{
    /// Builds a cache whose entries live for `ttl`.
    ///
    /// `key_fn` must be deterministic for a given input: two inputs that should
    /// share a cache entry must derive equal keys. `f` is the computation; it
    /// sees the original `I`, not `K`.
    pub fn new(
        ttl: Duration,
        key_fn: impl Fn(&I) -> K + Send + Sync + 'static,
        f: impl Fn(&I) -> Result<V, E> + Send + Sync + 'static,
    ) -> Self {
        Self {
            cache: Cache::builder().time_to_live(ttl).build(),
            key_fn: Box::new(key_fn),
            f: Box::new(f),
        }
    }

    /// Returns the cached value for `input`, computing it on a miss.
    ///
    /// Concurrent misses for the same key wait on one computation. An `Err` is
    /// propagated as a clone and is *not* stored, so a later call retries.
    ///
    /// # Panics
    ///
    /// Only if the wrapped `f` panics; cache bookkeeping itself is infallible.
    pub fn call(&self, input: I) -> Result<V, E> {
        let key = (self.key_fn)(&input);
        self.cache
            .try_get_with(key, || (self.f)(&input))
            .map_err(|e: Arc<E>| (*e).clone())
    }
}

// ---------------------------------------------------------------------
// Type-erased wrapper, updated for the extra I param
// ---------------------------------------------------------------------
// Only I changes at the erasure boundary — downcast the raw input to I,
// everything else (key derivation, caching) happens inside `call`.

/// Type-erased view of a [`Memoized`], for storing heterogeneous caches in one
/// collection.
///
/// The raw input travels as `Box<dyn Any + Send>` and is downcast back to the
/// concrete `I` inside [`AnyMemoized::call_any`]; the result is boxed again.
pub trait AnyMemoized: Send + Sync {
    /// Calls the underlying [`Memoized::call`] with a type-erased input.
    ///
    /// # Panics
    ///
    /// Panics if the boxed input is not the concrete `I` the entry was built
    /// for; the message names the expected type.
    ///
    /// Errors are stringified with [`Display`].
    fn call_any(&self, input: Box<dyn Any + Send>) -> Result<Box<dyn Any + Send>, String>;
}

impl<I, K, V, E> AnyMemoized for Memoized<I, K, V, E>
where
    I: Send + 'static,
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + Display + 'static,
{
    fn call_any(&self, input: Box<dyn Any + Send>) -> Result<Box<dyn Any + Send>, String> {
        let input = *input.downcast::<I>().unwrap_or_else(|_| {
            panic!(
                "AnyMemoized: input type mismatch for {}",
                std::any::type_name::<I>()
            )
        });
        self.call(input)
            .map(|v| Box::new(v) as Box<dyn Any + Send>)
            .map_err(|e| e.to_string())
    }
}
