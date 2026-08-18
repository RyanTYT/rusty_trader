//! Integration tests for trading-app.
//!
//! These tests require a live Postgres database (with `DATABASE_URL` env var set)
//! and exercise the full CRUD layer, complex SQL queries, and batch operations.
//! Run with:
//!
//!     DATABASE_URL=postgres://user:pass@localhost/db cargo test --test integration_tests
//!
//! The `models/` subdirectory contains per-table CRUD roundtrip tests + advanced
//! ops tests organized by interface CRUD enum (mirrors src/database/models_crud/).

#[path ="common/mod.rs"]
mod common;

#[path = "integration_tests/models/mod.rs"]
mod models;

#[path = "integration_tests/test_db_interface_crud.rs"]
mod test_db_interface_crud;
