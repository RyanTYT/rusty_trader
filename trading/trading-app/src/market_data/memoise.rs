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

pub struct Memoized<I, K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + Display + 'static,
{
    cache: Cache<K, V>,
    key_fn: Box<dyn Fn(&I) -> K + Send + Sync>,
    f: Box<dyn Fn(&I) -> Result<V, E> + Send + Sync>,
}

impl<I, K, V, E> Memoized<I, K, V, E>
where
    K: Hash + Eq + Send + Sync + Clone + 'static,
    V: Clone + Send + Sync + 'static,
    E: Clone + Send + Sync + Display + 'static,
{
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

pub trait AnyMemoized: Send + Sync {
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
