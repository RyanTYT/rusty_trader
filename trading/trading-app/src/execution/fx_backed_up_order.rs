//! Disk-backed parking lot for orders that are waiting on FX funding.
//!
//! When a strategy wants to open a position but the account does not hold enough
//! cash in the required currency, the funding planner parks the order here
//! instead of dropping it: [`OrderStore`](crate::execution::fx_backed_up_order::OrderStore) keeps it on disk until a later
//! cash movement makes it placeable. The syncer and
//! [`place_backed_up_orders`](crate::execution::order_update_stream::event_handlers::execution)
//! read the store back and submit whatever has become affordable.
//!
//! # Storage layout
//!
//! One redb table, `orders_by_FX`, maps a **bare strategy name** to a
//! postcard-encoded `Vec<OrderIBKR>`. The file is named by the
//! `ORDERS_FILE_PATH` environment variable and is opened once per process by
//! [`OrderStore::open`](crate::execution::fx_backed_up_order::OrderStore::open). Keys are always the plain strategy (for example
//! `"blended5"`); the `"{strategy}:{price}"` form only exists inside an order's
//! `order_ref`, where it doubles as the price memo used by
//! [`OrderStore::remove_all_valid_orders`](crate::execution::fx_backed_up_order::OrderStore::remove_all_valid_orders).
//!
//! # Invariants
//!
//! - At most one parked order per *contract* and key:
//!   [`OrderStore::insert_or_update_orders`](crate::execution::fx_backed_up_order::OrderStore::insert_or_update_orders) replaces the parked order for a
//!   contract (matched by [`HashContract`](crate::helpers::contract::HashContract)) while leaving other contracts and
//!   other keys untouched.
//! - Order matching for removal is suffix-insensitive
//!   ([`OrderIBKR`](crate::execution::order_engine::OrderIBKR) equality truncates `order_ref` at the first `:`), so the
//!   same order can be removed whether it was parked under `strategy` or
//!   `strategy:price`.
//! - Funding checks walk the rows in their stored order against a single
//!   shrinking budget; orders that do not fit are kept and stay in order.
//! - Every mutation is a single redb write transaction, so a crash cannot leave
//!   a half-written list. [`OrderStore::sync_full_map`](crate::execution::fx_backed_up_order::OrderStore::sync_full_map) replaces the whole
//!   table atomically.
//!
//! The store is process-local file state, not a database transaction: it is not
//! coordinated with `DbTxn`, and two processes opening the same redb file will
//! contend on the file lock.

use ibapi::contracts::SecurityType;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;

use crate::execution::order_engine::OrderIBKR;
use crate::helpers::contract::HashContract;

/// The single redb table: bare strategy name -> postcard-encoded parked orders.
const ORDERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("orders_by_FX");

/// Persistent, key-valued store of orders parked while waiting for FX cash.
///
/// Keys are bare strategy names; values are `Vec<OrderIBKR>` serialised with
/// `postcard`. The database is opened once per process and every method opens
/// the table inside its own redb transaction, so shared `&self` access is safe
/// and writes are serialised by redb.
pub struct OrderStore {
    /// Handle to the redb file; the `orders_by_FX` table is opened per operation.
    db: Database,
}

impl OrderStore {
    /// Opens (or creates) the redb file named by `ORDERS_FILE_PATH`.
    ///
    /// Parent directories are created when missing, and the `orders_by_FX`
    /// table is opened once inside a write transaction so that later reads and
    /// writes do not have to create it. The file is created empty when it does
    /// not exist, so a fresh deployment starts with an empty parking lot.
    ///
    /// # Panics
    ///
    /// Panics when `ORDERS_FILE_PATH` is not set.
    ///
    /// # Errors
    ///
    /// Returns a context-rich `String` when the directory cannot be created,
    /// the database cannot be opened (for example because another process holds
    /// the file lock), or the table cannot be initialised.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use trading_app::execution::fx_backed_up_order::OrderStore;
    /// # fn main() -> Result<(), String> {
    /// // Requires ORDERS_FILE_PATH to point at a writable file.
    /// let store = OrderStore::open()?;
    /// let parked = store.load_orders("my_strategy")?;
    /// // A fresh file starts empty; a restarted process sees what it parked.
    /// assert!(parked.is_none());
    /// # Ok(())
    /// # }
    /// ```
    pub fn open() -> Result<Self, String> {
        let orders_file_path: String =
            std::env::var("ORDERS_FILE_PATH").expect("Expected ORDERS_FILE_PATH env var to be set");

        if let Some(parent) = std::path::Path::new(&orders_file_path).parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| format!("Failed to create orders directory: {e}"))?;
        }

        let db = Database::create(orders_file_path)
            .map_err(|e| format!("Failed to open redb database: {e}"))?;

        // Initialize the table if it does not already exist
        let write_txn = db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        {
            let _ = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;
        }
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit table creation transaction: {e}"))?;

        Ok(Self { db })
    }

    /// Replaces the parked list for `key` with `orders` (full overwrite).
    ///
    /// Use this when the caller owns the complete list for the key, for example
    /// when rewriting the store from [`OrderStore::load_all_to_map`]. To add or
    /// refresh individual orders without disturbing the rest, use
    /// [`OrderStore::insert_or_update_orders`](crate::execution::fx_backed_up_order::OrderStore::insert_or_update_orders).
    ///
    /// # Errors
    ///
    /// Returns a `String` when serialisation, the write transaction, or the
    /// commit fails.
    pub fn store_orders(&self, key: &str, orders: &[OrderIBKR]) -> Result<(), String> {
        let payload = postcard::to_allocvec(orders)
            .map_err(|e| format!("Failed to serialize orders for key '{key}': {e}"))?;

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        {
            let mut table = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;
            table
                .insert(key, payload.as_slice())
                .map_err(|e| format!("Failed to insert orders for key '{key}': {e}"))?;
        }
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;

        Ok(())
    }

    /// Adds `orders` to the parked list for `key`, replacing any existing entry that has
    /// the same contract, in one write transaction. Entries already parked for the
    /// strategy are kept: an asset-type update only knows about its own target rows.
    ///
    /// Replacement is keyed by [`HashContract`], not by the whole order: if two
    /// orders share a contract, the last one written wins. Orders for other keys
    /// are never touched. An empty slice is a no-op (no write transaction is
    /// opened), which makes it safe to call from paths that may have nothing to
    /// park.
    ///
    /// # Errors
    ///
    /// Returns a `String` when the existing payload cannot be read or decoded,
    /// or when serialisation, the write transaction, or the commit fails.
    pub fn insert_or_update_orders(&self, key: &str, orders: &[OrderIBKR]) -> Result<(), String> {
        if orders.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        {
            let mut table = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;

            let mut existing: Vec<OrderIBKR> = match table
                .get(key)
                .map_err(|e| format!("Failed to read key '{key}' from table: {e}"))?
            {
                Some(access) => postcard::from_bytes(access.value())
                    .map_err(|e| format!("Failed to deserialize orders for key '{key}': {e}"))?,
                None => Vec::new(),
            };

            existing.retain(|parked| {
                !orders.iter().any(|new_order| {
                    HashContract::borrowed(&new_order.contract)
                        == HashContract::borrowed(&parked.contract)
                })
            });
            existing.extend(orders.iter().cloned());

            let payload = postcard::to_allocvec(&existing)
                .map_err(|e| format!("Failed to serialize orders for key '{key}': {e}"))?;
            table
                .insert(key, payload.as_slice())
                .map_err(|e| format!("Failed to insert orders for key '{key}': {e}"))?;
        }
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;

        Ok(())
    }

    /// Removes `orders` from the parked list for `key` in one write transaction.
    /// Matching uses `OrderIBKR` equality, so the `"{strategy}:{price}"` suffix is ignored
    /// and a plain-strategy order matches its parked counterpart.
    ///
    /// A missing key is `Ok(())`. When the key exists, the kept remainder is
    /// written back even if it is empty, so the key survives with an empty
    /// list (use [`OrderStore::delete_orders`] to remove the key itself). An
    /// empty `orders` slice is a no-op.
    ///
    /// # Errors
    ///
    /// Returns a `String` when the payload cannot be read or decoded, or when
    /// serialisation, the write transaction, or the commit fails.
    pub fn remove_orders(&self, key: &str, orders: &[OrderIBKR]) -> Result<(), String> {
        if orders.is_empty() {
            return Ok(());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        {
            let mut table = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;

            let existing: Vec<OrderIBKR> = match table
                .get(key)
                .map_err(|e| format!("Failed to read key '{key}' from table: {e}"))?
            {
                Some(access) => postcard::from_bytes(access.value())
                    .map_err(|e| format!("Failed to deserialize orders for key '{key}': {e}"))?,
                None => return Ok(()),
            };

            let kept: Vec<OrderIBKR> = existing
                .into_iter()
                .filter(|parked| !orders.iter().any(|removed| removed == parked))
                .collect();

            let payload = postcard::to_allocvec(&kept)
                .map_err(|e| format!("Failed to serialize orders for key '{key}': {e}"))?;
            table
                .insert(key, payload.as_slice())
                .map_err(|e| format!("Failed to insert orders for key '{key}': {e}"))?;
        }
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;

        Ok(())
    }

    /// Removes and returns every parked order for `key` that is denominated in
    /// `currency` and fully covered by `available`, in one write transaction.
    /// Orders that cannot be placed stay in the store untouched, in their original order.
    ///
    /// Each order is costed as `|total_quantity| * price * multiplier`, where
    /// the price is parsed from the `"{strategy}:{price}"` suffix of `order_ref`
    /// and the multiplier comes from `contract.multiplier` for options (1.0 for
    /// everything else). Rows are considered in stored order and consume one
    /// shrinking budget, so an expensive early order can keep a cheaper later
    /// one parked even though their total would fit a different allocation.
    ///
    /// Rows in another currency, rows without a parseable price, and option
    /// rows with an invalid multiplier are kept (with an error log) rather than
    /// dropped. `available <= 0.0` and a missing key both return an empty
    /// vector without writing.
    ///
    /// # Errors
    ///
    /// Returns a `String` when the payload cannot be read or decoded, or when
    /// serialisation, the write transaction, or the commit fails.
    pub fn remove_all_valid_orders(
        &self,
        key: &str,
        currency: &str,
        available: f64,
    ) -> Result<Vec<OrderIBKR>, String> {
        if available <= 0.0 {
            return Ok(Vec::new());
        }

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        let ready;
        {
            let mut table = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;

            let existing: Vec<OrderIBKR> = match table
                .get(key)
                .map_err(|e| format!("Failed to read key '{key}' from table: {e}"))?
            {
                Some(access) => postcard::from_bytes(access.value())
                    .map_err(|e| format!("Failed to deserialize orders for key '{key}': {e}"))?,
                None => return Ok(Vec::new()),
            };

            let mut kept: Vec<OrderIBKR> = Vec::with_capacity(existing.len());
            let mut placeable: Vec<OrderIBKR> = Vec::new();
            let mut budget = available;

            for order in existing {
                if order.contract.currency.as_str() != currency {
                    kept.push(order);
                    continue;
                }

                // Parked orders carry their price in the order_ref suffix: "{strategy}:{price}".
                let price: f64 = match order
                    .order
                    .order_ref
                    .rsplit_once(':')
                    .and_then(|(_, price)| price.parse().ok())
                {
                    Some(price) => price,
                    None => {
                        tracing::error!(
                            "Parked order for {} has no parseable price in order_ref",
                            order.contract.symbol
                        );
                        kept.push(order);
                        continue;
                    }
                };

                let multiplier: f64 = if order.contract.security_type == SecurityType::Option {
                    match order.contract.multiplier.parse() {
                        Ok(multiplier) => multiplier,
                        Err(_) => {
                            tracing::error!(
                                "Parked option order for {} has an invalid multiplier",
                                order.contract.symbol
                            );
                            kept.push(order);
                            continue;
                        }
                    }
                } else {
                    1.0
                };

                let required = order.order.total_quantity.abs() * price * multiplier;
                if required <= budget {
                    budget -= required;
                    placeable.push(order);
                } else {
                    kept.push(order);
                }
            }

            let payload = postcard::to_allocvec(&kept)
                .map_err(|e| format!("Failed to serialize orders for key '{key}': {e}"))?;
            table
                .insert(key, payload.as_slice())
                .map_err(|e| format!("Failed to insert orders for key '{key}': {e}"))?;
            ready = placeable;
        }
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit transaction: {e}"))?;

        Ok(ready)
    }

    /// Loads the parked list for `key`.
    ///
    /// Returns `Ok(None)` when the key has never been written, and
    /// `Ok(Some(vec![]))` when it exists but every order has been removed.
    ///
    /// # Errors
    ///
    /// Returns a `String` when the read transaction, table access, or
    /// deserialisation fails (for example after an incompatible schema change).
    pub fn load_orders(&self, key: &str) -> Result<Option<Vec<OrderIBKR>>, String> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| format!("Failed to begin read transaction: {e}"))?;
        let table = read_txn
            .open_table(ORDERS_TABLE)
            .map_err(|e| format!("Failed to open table: {e}"))?;

        if let Some(access) = table
            .get(key)
            .map_err(|e| format!("Failed to read key '{key}' from table: {e}"))?
        {
            let orders: Vec<OrderIBKR> = postcard::from_bytes(access.value())
                .map_err(|e| format!("Failed to deserialize orders for key '{key}': {e}"))?;
            Ok(Some(orders))
        } else {
            Ok(None)
        }
    }

    /// Removes a key and its attached orders from the database.
    ///
    /// Returns `true` when the key existed and was removed, `false` when it was
    /// absent. Unlike [`OrderStore::remove_orders`], this removes the key
    /// itself, so a later [`OrderStore::load_orders`] returns `None`.
    ///
    /// # Errors
    ///
    /// Returns a `String` when the write transaction or the commit fails.
    pub fn delete_orders(&self, key: &str) -> Result<bool, String> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        let removed = {
            let mut table = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;
            table
                .remove(key)
                .map_err(|e| format!("Failed to remove key '{key}': {e}"))?
                .is_some()
        };
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit delete transaction: {e}"))?;
        Ok(removed)
    }

    /// Loads the entire database into a `HashMap<String, Vec<OrderIBKR>>` on engine startup.
    ///
    /// Every key/value pair is decoded inside one read transaction, so the map
    /// is a consistent snapshot of the parking lot at that moment. Keys holding
    /// an empty list are included with an empty vector.
    ///
    /// # Errors
    ///
    /// Returns a `String` when the read transaction, iteration, or
    /// deserialisation of any value fails.
    pub fn load_all_to_map(&self) -> Result<HashMap<String, Vec<OrderIBKR>>, String> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| format!("Failed to begin read transaction: {e}"))?;
        let table = read_txn
            .open_table(ORDERS_TABLE)
            .map_err(|e| format!("Failed to open table: {e}"))?;
        let mut map = HashMap::new();

        let iter = table
            .iter()
            .map_err(|e| format!("Failed to iterate over table: {e}"))?;

        for item in iter {
            let (key, val) =
                item.map_err(|e| format!("Failed to read item in table iteration: {e}"))?;
            let orders: Vec<OrderIBKR> = postcard::from_bytes(val.value())
                .map_err(|e| format!("Failed to deserialize orders during full map load: {e}"))?;
            map.insert(key.value().to_string(), orders);
        }

        Ok(map)
    }

    /// Overwrite the entire database with a full HashMap.
    ///
    /// The whole replacement runs in one write transaction: existing keys are
    /// removed first (redb has no table-wide `clear`), then every map entry is
    /// serialised and inserted. A failure before the commit leaves the previous
    /// contents intact.
    ///
    /// # Errors
    ///
    /// Returns a `String` when iteration, removal, serialisation, or the
    /// commit fails.
    pub fn sync_full_map(&self, map: &HashMap<String, Vec<OrderIBKR>>) -> Result<(), String> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| format!("Failed to begin write transaction: {e}"))?;
        {
            let mut table = write_txn
                .open_table(ORDERS_TABLE)
                .map_err(|e| format!("Failed to open table: {e}"))?;

            // Clear existing entries
            let iter = table
                .iter()
                .map_err(|e| format!("Failed to iterate over table: {e}"))?;

            let mut keys = Vec::new();
            for item in iter {
                let (k, _) =
                    item.map_err(|e| format!("Failed to read key during table sync: {e}"))?;
                keys.push(k.value().to_string());
            }

            for k in keys {
                table
                    .remove(k.as_str())
                    .map_err(|e| format!("Failed to clear key '{k}' during table sync: {e}"))?;
            }

            // Insert new entries from map
            for (key, orders) in map {
                let payload = postcard::to_allocvec(orders).map_err(|e| {
                    format!("Failed to serialize orders for key '{key}' during table sync: {e}")
                })?;
                table
                    .insert(key.as_str(), payload.as_slice())
                    .map_err(|e| format!("Failed to insert key '{key}' during table sync: {e}"))?;
            }
        }
        write_txn
            .commit()
            .map_err(|e| format!("Failed to commit full sync transaction: {e}"))?;
        Ok(())
    }
}
