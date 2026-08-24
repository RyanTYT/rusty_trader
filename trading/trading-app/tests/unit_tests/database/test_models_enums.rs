//! Unit tests for the database enum `FromStr`/`Display` impls.
//!
//! See `src/database/models.rs`. Tests cover:
//! - `ExecutionSide::from_str` (BOT→Bought, SLD→Sold, else→panic)
//! - `OptionType::from_str` (P/PUT→Put, C/CALL→Call, X→Err, ""→panic)
//! - `AssetType::from_str` (5 mapped SecurityType variants + unmapped→Unknown)
//! - `Display` impls for AssetType, OptionType, ExecutionSide
//!
//! NOTE: `AssetType::from_str` takes `&SecurityType` (not `&str`) — see the
//! impl in models.rs. It returns `Unknown` (no panic) for unrecognized types.

use ibapi::prelude::SecurityType;
use trading_app::database::models::{AssetType, ExecutionSide, OptionType, Status};

// ============================ ExecutionSide::from_str ============================

#[test]
fn execution_side_bot_to_bought() {
    assert!(matches!(ExecutionSide::from_str("BOT"), ExecutionSide::Bought));
}

#[test]
fn execution_side_sld_to_sold() {
    assert!(matches!(ExecutionSide::from_str("SLD"), ExecutionSide::Sold));
}

#[test]
#[should_panic(expected = "ExecutionSide from_str called with string that is not BOT/SLD")]
fn execution_side_unknown_panics() {
    let _ = ExecutionSide::from_str("UNKNOWN");
}

#[test]
#[should_panic(expected = "ExecutionSide from_str called with string that is not BOT/SLD")]
fn execution_side_empty_panics() {
    let _ = ExecutionSide::from_str("");
}

// ============================ OptionType::from_str ============================

#[test]
fn option_type_p_to_put() {
    assert!(matches!(OptionType::from_str("P"), Ok(OptionType::Put)));
}

#[test]
fn option_type_c_to_call() {
    assert!(matches!(OptionType::from_str("C"), Ok(OptionType::Call)));
}

#[test]
fn option_type_put_full_word_to_put() {
    // from_str only looks at the first char
    assert!(matches!(OptionType::from_str("PUT"), Ok(OptionType::Put)));
}

#[test]
fn option_type_call_full_word_to_call() {
    assert!(matches!(OptionType::from_str("CALL"), Ok(OptionType::Call)));
}

#[test]
fn option_type_unknown_char_returns_err() {
    assert!(OptionType::from_str("X").is_err());
}

#[test]
#[should_panic(expected = "Expected Option Right passed to OptionType to have String of len > 0")]
fn option_type_empty_string_panics() {
    let _ = OptionType::from_str("");
}

// ============================ AssetType::from_str ============================

#[test]
fn asset_type_stock() {
    assert!(matches!(
        AssetType::from_str(&SecurityType::Stock),
        AssetType::Stock
    ));
}

#[test]
fn asset_type_future() {
    assert!(matches!(
        AssetType::from_str(&SecurityType::Future),
        AssetType::Future
    ));
}

#[test]
fn asset_type_option() {
    assert!(matches!(
        AssetType::from_str(&SecurityType::Option),
        AssetType::Option
    ));
}

#[test]
fn asset_type_forexpair() {
    assert!(matches!(
        AssetType::from_str(&SecurityType::ForexPair),
        AssetType::ForexPair
    ));
}

#[test]
fn asset_type_cfd() {
    assert!(matches!(
        AssetType::from_str(&SecurityType::CFD),
        AssetType::CFD
    ));
}

#[test]
fn asset_type_unrecognized_returns_unknown_no_panic() {
    // SecurityType has many variants; any unmapped one should return Unknown, NOT panic.
    // Use a variant that's NOT in the 5 mapped (Stock/Future/Option/ForexPair/CFD).
    // Bond is one such variant.
    let result = AssetType::from_str(&SecurityType::Bond);
    assert!(matches!(result, AssetType::Unknown), "expected Unknown, got {result:?}");
}

// ============================ Display impls ============================

#[test]
fn asset_type_display_strings() {
    assert_eq!(format!("{}", AssetType::Stock), "stock");
    assert_eq!(format!("{}", AssetType::Future), "future");
    assert_eq!(format!("{}", AssetType::Option), "option");
    assert_eq!(format!("{}", AssetType::ForexPair), "forex_pair");
    assert_eq!(format!("{}", AssetType::CFD), "cfd");
    assert_eq!(format!("{}", AssetType::CASH), "cash_asset");
    assert_eq!(format!("{}", AssetType::Unknown), "unknown");
}

#[test]
fn option_type_display_strings() {
    assert_eq!(format!("{}", OptionType::Call), "C");
    assert_eq!(format!("{}", OptionType::Put), "P");
}

// ============================ Status enum ============================

#[test]
fn status_variants_exist() {
    // Just verify the enum variants compile and are constructible
    let _active = Status::Active;
    let _stopping = Status::Stopping;
    let _inactive = Status::Inactive;
}
