//! Tests for `src/exchange_mappings/`.
//!
//! That directory contains exactly one file — `src/exchange_mappings/ISO10383_MIC.csv`,
//! the ISO 10383 market-identifier reference table — and **no Rust module**: there is
//! no `src/exchange_mappings.rs` and `src/lib.rs` does not declare one. The only
//! consumer is `src/strategy/fractional_momentum old.rs:575` (note the space in the
//! filename), which is not declared in `src/strategy/mod.rs` and therefore is not
//! compiled. It does:
//!
//! ```text
//! let data = include_str!("../exchange_mappings/ISO10383_MIC.csv");
//! let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(data.as_bytes());
//! mic_to_ib_pri.insert(record[0].to_string(), record[7].to_string());
//! ```
//!
//! so the data contract this file tests is:
//!   * column 0 is `MIC`, the map key;
//!   * column 7 is `ACRONYM`, the map value the (dead) consumer treats as an
//!     IBKR primary-exchange code;
//!   * the file parses as 17-column CSV with a header row;
//!   * MIC keys are unique (a duplicate would silently overwrite in the `HashMap`);
//!   * lookups are exact-match, case- and whitespace-sensitive;
//!   * an unknown MIC yields no entry (the consumer falls back to `"NASDAQ"`).
//!
//! The IBKR-venue → Yahoo-suffix mapping (`Consolidator::yahoo_ticker_from_contract`,
//! `src/market_data/traits/current_price.rs:288`) is the other table in this area and
//! is already covered exhaustively by `tests/market_data/traits/current_price.rs:91`;
//! it is deliberately not duplicated here.
//!
//! The CSV is parsed with a tiny RFC-4180 reader instead of the `csv` crate:
//! `csv` is a normal dependency, not a dev-dependency, so it is not linkable from
//! an integration test, and the task forbids adding dependencies.

use std::collections::{BTreeMap, HashSet};

/// The same bytes the consumer would `include_str!`.
const RAW: &str = include_str!("../src/exchange_mappings/ISO10383_MIC.csv");

const HEADER: [&str; 17] = [
    "MIC",
    "OPERATING MIC",
    "OPRT/SGMT",
    "MARKET NAME-INSTITUTION DESCRIPTION",
    "LEGAL ENTITY NAME",
    "LEI",
    "MARKET CATEGORY CODE",
    "ACRONYM",
    "ISO COUNTRY CODE (ISO 3166)",
    "CITY",
    "WEBSITE",
    "STATUS",
    "CREATION DATE",
    "LAST UPDATE DATE",
    "LAST VALIDATION DATE",
    "EXPIRY DATE",
    "COMMENTS",
];

const MIC_COL: usize = 0;
const OPERATING_MIC_COL: usize = 1;
const ACRONYM_COL: usize = 7;

// ──────────────────────────── CSV reader ────────────────────────────

/// Minimal RFC-4180 reader: quoted fields, doubled `""` escapes, CRLF or LF
/// record separators, and quoted fields containing commas/newlines.
fn parse_csv(input: &str) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    chars.next();
                    field.push('"');
                } else {
                    in_quotes = false;
                }
            } else {
                field.push(c);
            }
            continue;
        }
        match c {
            '"' => in_quotes = true,
            ',' => row.push(std::mem::take(&mut field)),
            '\r' => {
                if chars.peek() == Some(&'\n') {
                    chars.next();
                }
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            '\n' => {
                row.push(std::mem::take(&mut field));
                rows.push(std::mem::take(&mut row));
            }
            _ => field.push(c),
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    rows
}

fn table() -> Vec<Vec<String>> {
    let text = RAW.strip_prefix('\u{feff}').unwrap_or(RAW);
    parse_csv(text)
}

/// The exact map the dead consumer builds: `record[0] -> record[7]`.
fn mic_to_acronym() -> BTreeMap<String, String> {
    let mut rows = table().into_iter();
    let header = rows.next().expect("CSV has a header row");
    assert_eq!(header.len(), HEADER.len());
    rows.map(|r| (r[MIC_COL].clone(), r[ACRONYM_COL].clone()))
        .collect()
}

// ───────────────────────────── header ─────────────────────────────

/// The header carries a UTF-8 BOM; `csv::Reader::has_headers(true)` skips the
/// header row, so data records are unaffected. Positional reads (`record[0]`,
/// `record[7]`) keep working — a `headers()`/by-name lookup against `"MIC"`
/// would not, because the first header cell is `"\u{feff}MIC"`.
#[test]
fn csv_starts_with_a_utf8_bom_but_data_rows_do_not() {
    assert!(
        RAW.starts_with('\u{feff}'),
        "expected the reference CSV to start with a UTF-8 BOM"
    );

    let rows = table();
    let header = &rows[0];
    assert_eq!(header[MIC_COL], "MIC", "BOM must not leak into the MIC header");
    assert_eq!(header[ACRONYM_COL], "ACRONYM");
    assert_eq!(
        rows[1][MIC_COL], "XZIM",
        "first data record must start on a clean MIC"
    );
    for (i, row) in rows.iter().enumerate().skip(1) {
        assert!(
            !row[MIC_COL].starts_with('\u{feff}'),
            "data row {i} MIC starts with a BOM"
        );
    }
}

/// `record[0]` and `record[7]` are only meaningful if the header keeps this
/// exact layout; the consumer indexes them positionally.
#[test]
fn header_layout_matches_the_positional_indexes_the_consumer_uses() {
    let rows = table();
    assert_eq!(rows[0], HEADER);
    assert_eq!(HEADER[MIC_COL], "MIC");
    assert_eq!(HEADER[OPERATING_MIC_COL], "OPERATING MIC");
    assert_eq!(HEADER[ACRONYM_COL], "ACRONYM");
}

// ────────────────────────── table integrity ──────────────────────────

/// Every record has the same 17 columns as the header. The consumer reads
/// `record[7]` without a length check, so a short record would panic.
#[test]
fn every_record_has_seventeen_columns() {
    let rows = table();
    assert!(rows.len() > 1, "CSV must contain data records");
    for (i, row) in rows.iter().enumerate() {
        assert_eq!(
            row.len(),
            HEADER.len(),
            "row {i} has {} columns, expected {}",
            row.len(),
            HEADER.len()
        );
    }
}

/// MIC keys are unique: a duplicate would silently overwrite the first entry in
/// the consumer's `HashMap` and make the mapping depend on file order.
#[test]
fn mic_keys_are_unique() {
    let rows = table();
    let mut seen: HashSet<&str> = HashSet::new();
    for (i, row) in rows.iter().enumerate().skip(1) {
        assert!(
            seen.insert(row[MIC_COL].as_str()),
            "duplicate MIC '{}' at data row {i}",
            row[MIC_COL]
        );
    }
}

/// ISO 10383 MICs are four uppercase alphanumerics. Anything else here would
/// never match the uppercase MICs Polygon emits.
#[test]
fn mic_keys_are_four_uppercase_alphanumerics() {
    let rows = table();
    for (i, row) in rows.iter().enumerate().skip(1) {
        let mic = &row[MIC_COL];
        assert_eq!(mic.len(), 4, "row {i}: MIC {mic:?} is not 4 chars");
        assert!(
            mic.chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit()),
            "row {i}: MIC {mic:?} is not uppercase alphanumeric"
        );
        assert_eq!(mic, mic.trim(), "row {i}: MIC {mic:?} has whitespace");
    }
}

/// Every `OPERATING MIC` points at an `MIC` present in the table, so the alias
/// column can be resolved without a second lookup source.
#[test]
fn operating_mic_aliases_resolve_to_a_real_mic() {
    let rows = table();
    let keys: HashSet<&str> = rows
        .iter()
        .skip(1)
        .map(|r| r[MIC_COL].as_str())
        .collect();
    for (i, row) in rows.iter().enumerate().skip(1) {
        let op = &row[OPERATING_MIC_COL];
        assert!(
            keys.contains(op.as_str()),
            "row {i}: OPERATING MIC {op:?} is not a table key"
        );
    }
}

/// Acronyms are stored trimmed and uppercase; the consumer copies them into
/// `Contract::primary_exchange` verbatim.
#[test]
fn acronym_values_are_trimmed_and_uppercase() {
    let rows = table();
    for (i, row) in rows.iter().enumerate().skip(1) {
        let acronym = &row[ACRONYM_COL];
        assert_eq!(
            acronym,
            &acronym.trim(),
            "row {i}: acronym {acronym:?} has surrounding whitespace"
        );
        assert_eq!(
            acronym,
            &acronym.to_uppercase(),
            "row {i}: acronym {acronym:?} is not uppercase"
        );
    }
}

// ─────────────────────── exact known rows ───────────────────────

/// A table-driven sample of `MIC -> ACRONYM`, spanning every region the
/// reference file covers. Values are the file's contents; several are *not*
/// IBKR codes (see `mic_to_acronym_arcx_is_iso_data_not_an_ibkr_code`).
#[test]
fn mic_to_acronym_known_rows() {
    let map = mic_to_acronym();

    let cases: &[(&str, &str)] = &[
        // United States
        ("XNAS", "NASDAQ"),
        ("XNYS", "NYSE"),
        ("ARCX", "NYSE"), // ISO acronym for NYSE Arca; IBKR calls this venue ARCA
        ("XASE", "AMEX"),
        ("XCIS", "NSX"),
        ("PSXD", "PSX"),
        ("IEXG", "IEX"),
        ("XCBO", "CBOE"),
        ("BATS", "BATS"),
        // Asia-Pacific
        ("XTKS", "TSE"),
        ("XHKG", "HKEX"),
        ("XSES", "SGX"),
        ("XASX", "ASX"),
        ("XSHG", "SSE"),
        ("XSHE", ""), // ISO row has no acronym
        // Europe
        ("XLON", "LSE"),
        ("XETR", "XETRA"),
        ("XPAR", ""), // ISO row has no acronym
        // Canada
        ("XTSE", "TSX"),
        ("XTSX", "TSX-V"),
    ];

    for (mic, expected) in cases {
        assert_eq!(
            map.get(*mic).map(String::as_str),
            Some(*expected),
            "MIC {mic}"
        );
    }
}

/// The lookup is a plain `BTreeMap`/`HashMap` `get`: exact bytes only. The
/// consumer never trims or uppercases its Polygon input.
#[test]
fn mic_lookup_is_case_and_whitespace_sensitive() {
    let map = mic_to_acronym();

    assert_eq!(map.get("XNAS").map(String::as_str), Some("NASDAQ"));
    assert_eq!(map.get("xnas"), None);
    assert_eq!(map.get("Xnas"), None);
    assert_eq!(map.get(" XNAS"), None);
    assert_eq!(map.get("XNAS "), None);
    assert_eq!(map.get("XNAS\n"), None);
}

/// Unknown MICs (and non-MIC venue tokens the consumer's fallback uses) have no
/// entry; the consumer's `.unwrap_or(&"NASDAQ")` branch is therefore reachable.
#[test]
fn unknown_and_non_mic_tokens_are_absent() {
    let map = mic_to_acronym();

    for token in ["ZZZZ", "QQQQ", "ZZZ9", "0000", "0184", "SMART", "IDEALPRO"] {
        assert_eq!(map.get(token), None, "token {token:?} must not be a MIC key");
    }
    // The fallback token itself is not a MIC in this table.
    assert_eq!(map.get("NASDAQ"), None);
    assert_eq!(map.get("NYSE"), None);
    assert_eq!(map.get("ARCA"), None);
}

/// `XSHE` (Shenzhen) exists as a MIC but its acronym is empty, so the consumer
/// would insert `""` as `primary_exchange`. The entry exists; the value is
/// simply blank — documented here so a downstream `Some("")` is not mistaken
/// for a missing key.
#[test]
fn shenzhen_mic_maps_to_an_empty_acronym() {
    let map = mic_to_acronym();
    assert_eq!(map.get("XSHE").map(String::as_str), Some(""));
}

/// KNOWN BUG (in the consumer's assumption, not in the ISO data): the dead
/// `fractional_momentum old.rs` code names this map `mic_to_ib_pri` and feeds
/// `record[7]` straight into `Contract::primary_exchange`. `ACRONYM` is the
/// exchange's own acronym, not IBKR's primary-exchange code:
///
///   * `ARCX` is NYSE Arca → observed `"NYSE"`, IBKR primary exchange `"ARCA"`
///     (cf. `src/strategy/blended5_fund/fund.rs:246` `ibkr_primary_exchange`
///     returning `"ARCA"` for SPY, and `US_EXCHANGES` in
///     `src/market_data/traits/current_price.rs:307`).
///
/// The CSV is authoritative ISO 10383 reference data and must not be edited, so
/// this test marks the intended consumer-visible value.
#[test]
#[ignore = "known bug: ISO ACRONYM for ARCX is 'NYSE', but the consumer treats it as an IBKR primary exchange, where NYSE Arca is 'ARCA'"]
fn mic_to_acronym_arcx_is_iso_data_not_an_ibkr_code() {
    let map = mic_to_acronym();
    assert_eq!(map.get("ARCX").map(String::as_str), Some("ARCA"));
}

// ───────────────────── dead consumer semantics ─────────────────────

/// Reproduces the dead consumer's map construction and fallback exactly:
/// `mic_to_ib_pri.get(mic).unwrap_or(&"NASDAQ")`. This pins the observable
/// behaviour of the data + fallback pair without needing the uncompiled file.
#[test]
fn consumer_fallback_returns_nasdaq_for_unknown_mics() {
    let map = mic_to_acronym();
    let resolve = |mic: &str| -> String {
        map.get(mic).cloned().unwrap_or_else(|| "NASDAQ".to_string())
    };

    assert_eq!(resolve("XNAS"), "NASDAQ");
    assert_eq!(resolve("XNYS"), "NYSE");
    assert_eq!(resolve("ZZZZ"), "NASDAQ");
    assert_eq!(resolve("xnas"), "NASDAQ");
    assert_eq!(resolve("XSHE"), "", "known MIC with a blank acronym wins over fallback");
}
