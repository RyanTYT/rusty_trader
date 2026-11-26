use ibapi::contracts::SecurityType;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;

use crate::execution::order_engine::OrderIBKR;
use crate::helpers::contract::HashContract;

const ORDERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("orders_by_FX");

pub struct OrderStore {
    db: Database,
}

impl OrderStore {
    /// Opens or creates the redb database file on disk
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

    /// Save or replace the list of orders for a given key (e.g., strategy name or symbol)
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

    /// Load orders for a specific key
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

    /// Remove a key and its attached orders from the database
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

    /// Load the entire database into a HashMap<String, Vec<OrderIBKR>> on engine startup
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

    /// Overwrite the entire database with a full HashMap
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
