//! Dual CSV + Parquet writer for the `stats_logger` strategy.
//!
//! Each [`DualWriter`] owns one output table (e.g. `daily`, `per_bar`,
//! `cross_section`, …). Rows are appended as [`Cell`] vectors; the CSV is
//! written row-by-row (streaming, so the multi-million-row `per_bar` table
//! never builds up in memory) while the Parquet side is buffered into
//! batches of `batch_size` rows and flushed as Arrow `RecordBatch`es via
//! `parquet::arrow::ArrowWriter`. NaN/Inf floats become NULLs in Parquet
//! and empty fields in CSV. On [`DualWriter::finish`] (or `Drop`) the
//! Parquet footer + CSV buffer are flushed.
//!
//! No schema migration: each table is created fresh on construction
//! (`create + truncate`), so a re-run overwrites the previous output.

use std::fs::File;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

/// Column types supported in the output tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColType {
    F64,
    Str,
    I64,
}

/// One cell of a row. `Null` is a null for any column type; `F64` carrying
/// NaN/Inf is also written as null by the Parquet side (and as an empty
/// CSV field).
#[derive(Debug, Clone)]
pub enum Cell {
    F64(f64),
    Str(String),
    I64(i64),
    Null,
}

/// Static schema spec: a table name (file stem under `stats/`) + an ordered
/// list of `(column_name, type)`. Names are `&'static str` so the spec can
/// be a `const`.
#[derive(Debug, Clone)]
pub struct SchemaSpec {
    pub name: &'static str,
    pub cols: &'static [(&'static str, ColType)],
}

/// A dual CSV + Parquet writer for one table.
pub struct DualWriter {
    schema: SchemaSpec,
    pq_schema: SchemaRef,
    csv: Option<csv::Writer<File>>,
    pq: Option<Mutex<ArrowWriter<File>>>,
    batch: Vec<Vec<Cell>>,
    batch_size: usize,
    finished: bool,
}

impl DualWriter {
    /// Create the `stats/` directory (if missing) and open fresh
    /// `stats/{name}.csv` + `stats/{name}.parquet` files (truncating any
    /// prior content). Writes the CSV header immediately. Parquet is
    /// always available (hard dep).
    pub fn new(schema: SchemaSpec, batch_size: usize) -> Result<Self, String> {
        std::fs::create_dir_all("stats").map_err(|e| format!("create stats/ dir: {e}"))?;

        // ---- CSV ----
        let csv_path = format!("stats/{}.csv", schema.name);
        let csv_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&csv_path)
            .map_err(|e| format!("open {csv_path}: {e}"))?;
        let mut csv = csv::Writer::from_writer(csv_file);
        let header: Vec<&str> = schema.cols.iter().map(|(n, _)| *n).collect();
        csv.write_record(&header)
            .map_err(|e| format!("write CSV header for {csv_path}: {e}"))?;

        // ---- Parquet ----
        let pq_path = format!("stats/{}.parquet", schema.name);
        let fields: Vec<Field> = schema
            .cols
            .iter()
            .map(|(n, t)| {
                let dt = match t {
                    ColType::F64 => DataType::Float64,
                    ColType::Str => DataType::Utf8,
                    ColType::I64 => DataType::Int64,
                };
                Field::new(*n, dt, true) // nullable
            })
            .collect();
        let pq_schema: SchemaRef = Arc::new(Schema::new(fields));
        let pq_file = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&pq_path)
            .map_err(|e| format!("open {pq_path}: {e}"))?;
        let pq = Mutex::new(
            ArrowWriter::try_new(pq_file, pq_schema.clone(), None)
                .map_err(|e| format!("ArrowWriter::try_new {pq_path}: {e}"))?,
        );

        Ok(Self {
            schema,
            pq_schema,
            csv: Some(csv),
            pq: Some(pq),
            batch: Vec::with_capacity(batch_size.max(1)),
            batch_size: batch_size.max(1),
            finished: false,
        })
    }

    /// Append one row. Writes the CSV field immediately (streaming) and
    /// buffers the row for the next Parquet batch flush.
    pub fn write_row(&mut self, row: Vec<Cell>) -> Result<(), String> {
        if self.finished {
            return Err("write_row on finished DualWriter".to_string());
        }
        if let Some(csv) = self.csv.as_mut() {
            let fields: Vec<String> = row.iter().map(cell_to_csv).collect();
            let refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
            csv.write_record(&refs)
                .map_err(|e| format!("csv write_record: {e}"))?;
        }
        self.batch.push(row);
        if self.batch.len() >= self.batch_size {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// Build + write the buffered Parquet batch (one `RecordBatch`).
    fn flush_batch(&mut self) -> Result<(), String> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let Some(pq) = self.pq.as_mut() else {
            self.batch.clear();
            return Ok(());
        };
        let mut pq = pq.lock().unwrap();
        let ncols = self.schema.cols.len();
        let nrows = self.batch.len();
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(ncols);
        for (col_idx, (_, ctype)) in self.schema.cols.iter().enumerate() {
            let arr: ArrayRef = match ctype {
                ColType::F64 => {
                    let mut vals = Vec::with_capacity(nrows);
                    for row in &self.batch {
                        let v = match row.get(col_idx) {
                            Some(Cell::F64(x)) if x.is_finite() => Some(*x),
                            _ => None,
                        };
                        vals.push(v);
                    }
                    Arc::new(Float64Array::from(vals)) as ArrayRef
                }
                ColType::Str => {
                    let mut vals: Vec<Option<String>> = Vec::with_capacity(nrows);
                    for row in &self.batch {
                        let v = match row.get(col_idx) {
                            Some(Cell::Str(s)) => Some(s.clone()),
                            _ => None,
                        };
                        vals.push(v);
                    }
                    Arc::new(StringArray::from(vals)) as ArrayRef
                }
                ColType::I64 => {
                    let mut vals = Vec::with_capacity(nrows);
                    for row in &self.batch {
                        let v = match row.get(col_idx) {
                            Some(Cell::I64(i)) => Some(*i),
                            _ => None,
                        };
                        vals.push(v);
                    }
                    Arc::new(Int64Array::from(vals)) as ArrayRef
                }
            };
            arrays.push(arr);
        }
        let rb = RecordBatch::try_new(self.pq_schema.clone(), arrays)
            .map_err(|e| format!("RecordBatch::try_new: {e}"))?;
        pq.write(&rb).map_err(|e| format!("parquet write: {e}"))?;
        self.batch.clear();
        Ok(())
    }

    /// Flush any buffered Parquet rows + close the Parquet writer (writes
    /// the footer) + flush the CSV. Idempotent.
    pub fn finish(&mut self) -> Result<(), String> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.flush_batch()?;
        if let Some(pq) = self.pq.take() {
            let aw = pq.into_inner().unwrap_or_else(|e| e.into_inner());
            let _ = aw.close().map_err(|e| format!("parquet close: {e}"))?;
        }
        if let Some(csv) = self.csv.as_mut() {
            csv.flush().map_err(|e| format!("csv flush: {e}"))?;
        }
        Ok(())
    }
}

impl Drop for DualWriter {
    fn drop(&mut self) {
        // Best-effort flush if `finish` wasn't called explicitly.
        let _ = self.finish();
    }
}

/// CSV string for a cell: empty for Null / NaN / Inf, the raw value
/// otherwise (Rust's default `Display` for f64 is shortest round-trippable).
fn cell_to_csv(c: &Cell) -> String {
    match c {
        Cell::F64(x) => {
            if x.is_finite() {
                format!("{}", x)
            } else {
                String::new()
            }
        }
        Cell::Str(s) => s.clone(),
        Cell::I64(i) => format!("{}", i),
        Cell::Null => String::new(),
    }
}
