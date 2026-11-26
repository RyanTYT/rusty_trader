//! Tests for `order_update_stream/controller.rs`: wire-status parsing.

use trading_app::execution::order_update_stream::controller::StatusOfOrderStatus;

#[test]
fn status_from_str_maps_every_wire_value() {
    use StatusOfOrderStatus::*;
    let cases = [
        ("ApiPending", ApiPending),
        ("PendingSubmit", PendingSubmit),
        ("PendingCancel", PendingCancel),
        ("PreSubmitted", PreSubmitted),
        ("Submitted", Submitted),
        ("ApiCancelled", ApiCancelled),
        ("Cancelled", Cancelled),
        ("Filled", Filled),
        ("Inactive", Inactive),
        ("Unknown", Unknown),
    ];
    for (input, expected) in cases {
        assert_eq!(StatusOfOrderStatus::from_str(input), expected, "{input}");
    }
}

#[test]
fn status_from_str_falls_back_for_wrong_case_and_unknown() {
    assert_eq!(
        StatusOfOrderStatus::from_str("submitted"),
        StatusOfOrderStatus::Unknown
    );
    assert_eq!(
        StatusOfOrderStatus::from_str("SomethingElse"),
        StatusOfOrderStatus::Unknown
    );
}
