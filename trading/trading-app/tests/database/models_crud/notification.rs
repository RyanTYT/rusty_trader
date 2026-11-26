//! `src/database/models_crud/notification.rs`
//!
//! SKIPPED: the generated model primary key is `title`, but the table
//! `trading.notifications` has an identity `id` primary key and no unique
//! constraint on `title`. `read`, `update`, `delete` and `create_or_update`
//! are therefore ambiguous or fail at the SQL level. Re-enable the generated
//! suite once the model or the table is reconciled.
