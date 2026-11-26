//! Mirror of `src/database/models_crud`.
//!
//! Each table module declares a small `fx` fixture module and invokes
//! `crud_suite!`. The macro expands the same nine tests for every table so the
//! generic CRUD contract is covered uniformly.

macro_rules! crud_suite {
    ($accessor:ident, $fx:ident) => {
        crud_suite!($accessor, $fx, uk);
    };
    ($accessor:ident, $fx:ident, $upsert_uk:ident) => {
        crud_suite!(@tests $accessor, $fx, $upsert_uk, #[ignore = "requires a live database"]);
    };
    // Known-bug variant: `create_or_update` cannot satisfy its contract for
    // this table, so the upsert test is kept as an ignored marker instead of
    // being silently dropped.
    ($accessor:ident, $fx:ident, $upsert_uk:ident, ignore_upsert: $reason:literal) => {
        crud_suite!(@tests $accessor, $fx, $upsert_uk, #[ignore = $reason]);
    };
    (@tests $accessor:ident, $fx:ident, $upsert_uk:ident, $($upsert_attr:tt)*) => {
        use crate::common::{SessionKind, TestDb, TestSession, tag};
        use trading_app::database::crud::CRUDTrait;
        use trading_app::database::tables::DbTables;

        const KINDS: [SessionKind; 2] = [SessionKind::Direct, SessionKind::Txn];

        async fn session(pool: &sqlx::PgPool, kind: SessionKind) -> TestSession {
            TestSession::new(pool, kind).await.expect("begin session")
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn create_read_delete() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fk = $fx::fk(&tag(stringify!($accessor)));
                let pk = $fx::pk(&fk);

                s.$accessor().create(&fk).await.expect("create");
                let got = s.$accessor().read(&pk).await.expect("read").expect("row");
                $fx::check(&fk, &got);

                s.$accessor().delete(&pk).await.expect("delete");
                assert!(
                    s.$accessor().read(&pk).await.expect("read after delete").is_none(),
                    "row should be gone after delete"
                );

                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn create_or_ignore_is_idempotent() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fk = $fx::fk(&tag(stringify!($accessor)));
                let pk = $fx::pk(&fk);
                let conflicting = $fx::conflicting(&fk);

                s.$accessor().create(&fk).await.expect("create");
                s.$accessor()
                    .create_or_ignore(&conflicting)
                    .await
                    .expect("create_or_ignore on conflict");

                let got = s.$accessor().read(&pk).await.expect("read").expect("row");
                $fx::check(&fk, &got);

                s.$accessor().delete(&pk).await.expect("delete");
                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        $($upsert_attr)*
        async fn create_or_update_upserts() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fk = $fx::fk(&tag(stringify!($accessor)));
                let pk = $fx::pk(&fk);
                let uk = $fx::$upsert_uk(&tag("upd"));

                s.$accessor().create(&fk).await.expect("create");
                s.$accessor()
                    .create_or_update(&pk, &uk)
                    .await
                    .expect("create_or_update");

                let got = s.$accessor().read(&pk).await.expect("read").expect("row");
                $fx::check_updated(&uk, &got);

                s.$accessor().delete(&pk).await.expect("delete");
                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn update_is_partial() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fk = $fx::fk(&tag(stringify!($accessor)));
                let pk = $fx::pk(&fk);
                let uk = $fx::uk(&tag("upd"));

                s.$accessor().create(&fk).await.expect("create");
                let affected = s.$accessor().update(&pk, &uk).await.expect("update");
                assert_eq!(affected, 1, "one row should be updated");

                let got = s.$accessor().read(&pk).await.expect("read").expect("row");
                $fx::check_updated(&uk, &got);

                s.$accessor().delete(&pk).await.expect("delete");
                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn update_missing_returns_zero() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let pk = $fx::pk_other(&tag(stringify!($accessor)));
                let uk = $fx::uk(&tag("upd"));

                let affected = s.$accessor().update(&pk, &uk).await.expect("update");
                assert_eq!(affected, 0, "missing row should affect nothing");

                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn all_none_update_returns_zero() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fk = $fx::fk(&tag(stringify!($accessor)));
                let pk = $fx::pk(&fk);

                s.$accessor().create(&fk).await.expect("create");
                let affected = s
                    .$accessor()
                    .update(&pk, &$fx::uk_none())
                    .await
                    .expect("all-None update");
                assert_eq!(affected, 0, "all-None update should be a no-op");

                let got = s.$accessor().read(&pk).await.expect("read").expect("row");
                $fx::check(&fk, &got);

                s.$accessor().delete(&pk).await.expect("delete");
                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn read_all_contains_created_rows() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fks: Vec<_> = (0..3)
                    .map(|_| $fx::fk(&tag(stringify!($accessor))))
                    .collect();

                for fk in &fks {
                    s.$accessor().create(fk).await.expect("create");
                }

                let all = s.$accessor().read_all().await.expect("read_all");
                for fk in &fks {
                    assert!(
                        all.iter().any(|row| $fx::matches(row, fk)),
                        "read_all should contain every created row"
                    );
                }

                for fk in &fks {
                    s.$accessor().delete(&$fx::pk(fk)).await.expect("delete");
                }
                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn read_for_update_returns_row() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let fk = $fx::fk(&tag(stringify!($accessor)));
                let pk = $fx::pk(&fk);

                s.$accessor().create(&fk).await.expect("create");
                let got = s
                    .$accessor()
                    .read_for_update(&pk)
                    .await
                    .expect("read_for_update")
                    .expect("row");
                $fx::check(&fk, &got);

                s.$accessor().delete(&pk).await.expect("delete");
                s.finish().await.expect("commit");
            }
        }

        #[tokio::test]
        #[ignore = "requires a live database"]
        async fn missing_reads_and_deletes_are_ok() {
            let db = TestDb::setup().await;
            for kind in KINDS {
                let mut s = session(&db.pool, kind).await;
                $fx::setup(&mut s).await;
                let pk = $fx::pk_other(&tag(stringify!($accessor)));

                assert!(
                    s.$accessor()
                        .read(&pk)
                        .await
                        .expect("read missing")
                        .is_none(),
                    "missing read should return None"
                );
                s.$accessor().delete(&pk).await.expect("delete missing");

                s.finish().await.expect("commit");
            }
        }
    };
}

mod batch_operations;
mod cancelled_orders;
mod concurrency;
mod current_option_positions;
mod current_positions_ops;
mod current_stock_positions;
mod historical_data_ops;
mod historical_forex_data;
mod historical_options_data;
mod historical_stock_data;
mod logs;
mod notification;
mod open_option_orders;
mod open_orders_ops;
mod transactions_ops;
mod open_stock_orders;
mod staged_commissions;
mod strategy;
mod target_stock_positions;
mod target_option_positions;
mod target_positions_ops;
mod stock_transactions;
mod option_transactions;

