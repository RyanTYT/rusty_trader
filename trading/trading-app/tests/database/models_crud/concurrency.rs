//! Transaction and concurrency semantics for `DbDirect` / `DbTxn`.
//!
//! `staged_commissions` is the vehicle for row-level behaviour: its
//! `execution_id` primary key is a real uniqueness/locking target (unlike
//! `logs.logs`, whose identity `id` the model cannot address). The cross-table
//! tests exercise a single transaction over orders, transactions and
//! positions.

use std::time::Duration;

use rust_decimal::Decimal;
use tokio::sync::oneshot;

use crate::common::{TestDb, tag};
use trading_app::database::crud::CRUDTrait;
use trading_app::database::db::{DbDirect, DbTxn};
use trading_app::database::models::{Status, StrategyFullKeys};
use trading_app::database::tables::DbTables;

fn strategy_row(name: &str) -> StrategyFullKeys {
    StrategyFullKeys {
        strategy: name.to_string(),
        status: Status::Active,
    }
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn same_pk_concurrent_inserts_let_exactly_one_win() {
    let db = TestDb::setup().await;
    let fk = super::staged_commissions::fx::fk(&tag("conc_same_pk"));
    let pk = super::staged_commissions::fx::pk(&fk);
    let pool = &db.pool;

    let (first, second) = tokio::join!(
        async {
            let mut db = DbDirect::connect(pool);
            db.staged_commissions().create(&fk).await
        },
        async {
            let mut db = DbDirect::connect(pool);
            db.staged_commissions().create(&fk).await
        },
    );

    let winners = usize::from(first.is_ok()) + usize::from(second.is_ok());
    assert_eq!(winners, 1, "exactly one insert may win: {first:?} / {second:?}");

    let mut db = DbDirect::connect(pool);
    assert!(
        db.staged_commissions().read(&pk).await.unwrap().is_some()
    );
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn uncommitted_writes_are_invisible_and_become_visible_on_commit() {
    let db = TestDb::setup().await;
    let fk = super::staged_commissions::fx::fk(&tag("conc_commit"));
    let pk = super::staged_commissions::fx::pk(&fk);

    let mut tx = DbTxn::begin(&db.pool).await.unwrap();
    tx.staged_commissions().create(&fk).await.unwrap();

    let mut direct = DbDirect::connect(&db.pool);
    assert!(
        direct.staged_commissions().read(&pk).await.unwrap().is_none(),
        "the open transaction is not visible to another session"
    );

    tx.commit().await.unwrap();

    let mut direct = DbDirect::connect(&db.pool);
    assert!(direct.staged_commissions().read(&pk).await.unwrap().is_some());
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn rollback_discards_every_write_in_the_transaction() {
    let db = TestDb::setup().await;
    let fk = super::staged_commissions::fx::fk(&tag("conc_rollback"));
    let pk = super::staged_commissions::fx::pk(&fk);

    let mut tx = DbTxn::begin(&db.pool).await.unwrap();
    tx.staged_commissions().create(&fk).await.unwrap();
    tx.rollback().await.unwrap();

    let mut direct = DbDirect::connect(&db.pool);
    assert!(direct.staged_commissions().read(&pk).await.unwrap().is_none());
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn read_for_update_blocks_until_the_writer_commits() {
    let db = TestDb::setup().await;
    let fk = super::staged_commissions::fx::fk(&tag("conc_lock"));
    let pk = super::staged_commissions::fx::pk(&fk);

    let mut direct = DbDirect::connect(&db.pool);
    direct.staged_commissions().create(&fk).await.unwrap();

    let mut writer = DbTxn::begin(&db.pool).await.unwrap();
    assert!(
        writer
            .staged_commissions()
            .read_for_update(&pk)
            .await
            .unwrap()
            .is_some()
    );

    let (send, mut recv) = oneshot::channel::<Decimal>();
    let pool = db.pool.clone();
    let blocked_pk = pk.clone();
    tokio::spawn(async move {
        let mut reader = DbTxn::begin(&pool).await.unwrap();
        let fees = reader
            .staged_commissions()
            .read_for_update(&blocked_pk)
            .await
            .unwrap()
            .expect("the row exists")
            .fees;
        let _ = send.send(fees);
        reader.commit().await.unwrap();
    });

    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut recv)
            .await
            .is_err(),
        "the reader must block while the writer holds the row lock"
    );

    let uk = super::staged_commissions::fx::uk("upd");
    writer.staged_commissions().update(&pk, &uk).await.unwrap();
    writer.commit().await.unwrap();

    let fees = tokio::time::timeout(Duration::from_secs(2), &mut recv)
        .await
        .expect("the reader unblocks after commit")
        .unwrap();
    assert_eq!(
        fees,
        uk.fees.expect("update sets fees"),
        "the reader re-reads the committed row"
    );
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_direct_read_is_not_blocked_by_a_row_lock() {
    let db = TestDb::setup().await;
    let fk = super::staged_commissions::fx::fk(&tag("conc_direct"));
    let pk = super::staged_commissions::fx::pk(&fk);

    let mut committed = DbDirect::connect(&db.pool);
    committed.staged_commissions().create(&fk).await.unwrap();

    let mut tx = DbTxn::begin(&db.pool).await.unwrap();
    tx.staged_commissions()
        .read_for_update(&pk)
        .await
        .unwrap()
        .expect("the committed row exists");

    let mut direct = DbDirect::connect(&db.pool);
    let row = tokio::time::timeout(
        Duration::from_millis(300),
        direct.staged_commissions().read(&pk),
    )
    .await
    .expect("a plain read does not wait for the lock")
    .unwrap();
    assert!(row.is_some(), "the committed data is still readable");

    tx.rollback().await.unwrap();
}



#[tokio::test]
#[ignore = "requires a live database"]
async fn a_cross_table_transaction_commits_all_or_nothing() {
    let db = TestDb::setup().await;
    let strategy = tag("conc_cross_commit");
    let mut direct = DbDirect::connect(&db.pool);
    direct
        .strategy()
        .create_or_ignore(&strategy_row(&strategy))
        .await
        .unwrap();

    let mut order = super::open_stock_orders::fk(&tag("conc_order"));
    order.strategy = strategy.clone();
    let mut transaction = super::stock_transactions::fk(&tag("conc_txn"));
    transaction.strategy = strategy.clone();
    let mut position = super::current_stock_positions::fk(&tag("conc_position"));
    position.strategy = strategy.clone();

    let order_pk = super::open_stock_orders::pk(&order);
    let transaction_pk = super::stock_transactions::pk(&transaction);
    let position_pk = super::current_stock_positions::pk(&position);

    let mut tx = DbTxn::begin(&db.pool).await.unwrap();
    tx.open_stock_orders().create(&order).await.unwrap();
    tx.stock_transactions().create(&transaction).await.unwrap();
    tx.current_stock_positions().create(&position).await.unwrap();
    tx.commit().await.unwrap();

    let mut direct = DbDirect::connect(&db.pool);
    assert!(
        direct
            .open_stock_orders()
            .read(&order_pk)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        direct
            .stock_transactions()
            .read(&transaction_pk)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        direct
            .current_stock_positions()
            .read(&position_pk)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
#[ignore = "requires a live database"]
async fn a_cross_table_rollback_leaves_no_rows_behind() {
    let db = TestDb::setup().await;
    let strategy = tag("conc_cross_rollback");
    let mut direct = DbDirect::connect(&db.pool);
    direct
        .strategy()
        .create_or_ignore(&strategy_row(&strategy))
        .await
        .unwrap();

    let mut order = super::open_stock_orders::fk(&tag("conc_order_rb"));
    order.strategy = strategy.clone();
    let mut transaction = super::stock_transactions::fk(&tag("conc_txn_rb"));
    transaction.strategy = strategy.clone();
    let mut position = super::current_stock_positions::fk(&tag("conc_position_rb"));
    position.strategy = strategy.clone();

    let order_pk = super::open_stock_orders::pk(&order);
    let transaction_pk = super::stock_transactions::pk(&transaction);
    let position_pk = super::current_stock_positions::pk(&position);

    let mut tx = DbTxn::begin(&db.pool).await.unwrap();
    tx.open_stock_orders().create(&order).await.unwrap();
    tx.stock_transactions().create(&transaction).await.unwrap();
    tx.current_stock_positions().create(&position).await.unwrap();
    tx.rollback().await.unwrap();

    let mut direct = DbDirect::connect(&db.pool);
    assert!(
        direct
            .open_stock_orders()
            .read(&order_pk)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        direct
            .stock_transactions()
            .read(&transaction_pk)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        direct
            .current_stock_positions()
            .read(&position_pk)
            .await
            .unwrap()
            .is_none()
    );
}
