use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::collections::HashMap;

use crate::execution::order_engine::OrderIBKR;

const ORDERS_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("orders_by_FX");
const ORDERS_FILE_PATH: &str = "orders_state.redb";

pub struct OrderStore {
    db: Database,
}

impl OrderStore {
    /// Opens or creates the redb database file on disk
    pub fn open() -> Result<Self, String> {
        let db = Database::create(ORDERS_FILE_PATH)
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
