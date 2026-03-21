//! Integration tests for file-based validation (Parquet, CSV, JSON).
//!
//! Each test writes a small file to a temp directory, registers it via
//! `register_file_as_table`, and validates a contract against the data.

use arrow_array::RecordBatch;
use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use contracts_core::{
    ContractBuilder, DataFormat, FieldBuilder, FieldConstraints, ValidationContext,
};
use contracts_validator::{DataValidator, register_file_as_table};
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::prelude::*;
use std::sync::Arc;

/// Helper: create a two-column RecordBatch (id: Int64, name: Utf8).
fn sample_batch() -> RecordBatch {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, true),
    ]));

    let mut id_builder = Int64Builder::new();
    id_builder.append_value(1);
    id_builder.append_value(2);
    id_builder.append_value(3);

    let mut name_builder = StringBuilder::new();
    name_builder.append_value("alice");
    name_builder.append_value("bob");
    name_builder.append_null();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
        ],
    )
    .unwrap()
}

/// Helper: write a RecordBatch to a Parquet file and return the path.
async fn write_parquet(dir: &std::path::Path, batch: RecordBatch) -> String {
    let ctx = SessionContext::new();
    ctx.register_batch("tmp", batch).unwrap();
    let df = ctx.table("tmp").await.unwrap();
    let path = dir.join("data.parquet");
    df.write_parquet(
        path.to_str().unwrap(),
        DataFrameWriteOptions::default(),
        None,
    )
    .await
    .unwrap();
    path.to_str().unwrap().to_string()
}

/// Helper: write a RecordBatch to a CSV file and return the path.
async fn write_csv(dir: &std::path::Path, batch: RecordBatch) -> String {
    let ctx = SessionContext::new();
    ctx.register_batch("tmp", batch).unwrap();
    let df = ctx.table("tmp").await.unwrap();
    let path = dir.join("data.csv");
    df.write_csv(
        path.to_str().unwrap(),
        DataFrameWriteOptions::default(),
        None,
    )
    .await
    .unwrap();
    path.to_str().unwrap().to_string()
}

/// Helper: write a RecordBatch to a JSON (NDJSON) file and return the path.
async fn write_json(dir: &std::path::Path, batch: RecordBatch) -> String {
    let ctx = SessionContext::new();
    ctx.register_batch("tmp", batch).unwrap();
    let df = ctx.table("tmp").await.unwrap();
    let path = dir.join("data.json");
    df.write_json(
        path.to_str().unwrap(),
        DataFrameWriteOptions::default(),
        None,
    )
    .await
    .unwrap();
    path.to_str().unwrap().to_string()
}

fn sample_contract(format: DataFormat, location: &str) -> contracts_core::Contract {
    ContractBuilder::new("file_test", "test-owner")
        .location(location)
        .format(format)
        .field(FieldBuilder::new("id", "int64").nullable(false).build())
        .field(FieldBuilder::new("name", "string").nullable(true).build())
        .build()
}

// -----------------------------------------------------------------------
// Parquet tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn parquet_validation_passes_with_valid_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path(), sample_batch()).await;

    let ctx = register_file_as_table(&DataFormat::Parquet, &path, None)
        .await
        .unwrap();

    let contract = sample_contract(DataFormat::Parquet, &path);
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &ValidationContext::new())
        .await;

    assert!(report.passed, "errors: {:?}", report.errors);
    assert!(report.stats.records_validated > 0);
}

#[tokio::test]
async fn parquet_validation_detects_null_violation() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path(), sample_batch()).await;

    let ctx = register_file_as_table(&DataFormat::Parquet, &path, None)
        .await
        .unwrap();

    // Declare name as non-nullable — the data has a null in row 3
    let contract = ContractBuilder::new("file_test", "test-owner")
        .location(&path)
        .format(DataFormat::Parquet)
        .field(FieldBuilder::new("id", "int64").nullable(false).build())
        .field(FieldBuilder::new("name", "string").nullable(false).build())
        .build();

    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &ValidationContext::new())
        .await;

    assert!(!report.passed);
    assert!(report.errors.iter().any(|e| e.contains("name")));
}

#[tokio::test]
async fn parquet_validation_with_sample_size() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path(), sample_batch()).await;

    let ctx = register_file_as_table(&DataFormat::Parquet, &path, Some(2))
        .await
        .unwrap();

    let contract = sample_contract(DataFormat::Parquet, &path);
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &ValidationContext::new())
        .await;

    assert!(report.stats.records_validated <= 2);
}

// -----------------------------------------------------------------------
// CSV tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn csv_validation_passes_with_valid_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_csv(dir.path(), sample_batch()).await;

    let ctx = register_file_as_table(&DataFormat::Csv, &path, None)
        .await
        .unwrap();

    let contract = sample_contract(DataFormat::Csv, &path);
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &ValidationContext::new())
        .await;

    assert!(report.passed, "errors: {:?}", report.errors);
    assert!(report.stats.records_validated > 0);
}

// -----------------------------------------------------------------------
// JSON tests
// -----------------------------------------------------------------------

#[tokio::test]
async fn json_validation_passes_with_valid_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_json(dir.path(), sample_batch()).await;

    let ctx = register_file_as_table(&DataFormat::Json, &path, None)
        .await
        .unwrap();

    let contract = sample_contract(DataFormat::Json, &path);
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &ValidationContext::new())
        .await;

    assert!(report.passed, "errors: {:?}", report.errors);
    assert!(report.stats.records_validated > 0);
}

// -----------------------------------------------------------------------
// Error cases
// -----------------------------------------------------------------------

#[tokio::test]
async fn unsupported_format_returns_error() {
    let result = register_file_as_table(&DataFormat::Delta, "/nonexistent", None).await;
    let err = result
        .err()
        .expect("expected an error for unsupported format");
    assert!(err.contains("not supported"), "unexpected error: {err}");
}

#[tokio::test]
async fn register_file_does_not_panic_for_missing_path() {
    // DataFusion registers file sources lazily, so a missing path may not
    // error until query time.  This test just verifies no panic occurs.
    let _ = register_file_as_table(
        &DataFormat::Parquet,
        "/tmp/nonexistent_dce_test.parquet",
        None,
    )
    .await;
}

// -----------------------------------------------------------------------
// Constraint validation through file reader
// -----------------------------------------------------------------------

#[tokio::test]
async fn parquet_validation_detects_constraint_violation() {
    let dir = tempfile::tempdir().unwrap();
    let path = write_parquet(dir.path(), sample_batch()).await;

    let ctx = register_file_as_table(&DataFormat::Parquet, &path, None)
        .await
        .unwrap();

    let contract = ContractBuilder::new("file_test", "test-owner")
        .location(&path)
        .format(DataFormat::Parquet)
        .field(FieldBuilder::new("id", "int64").nullable(false).build())
        .field(
            FieldBuilder::new("name", "string")
                .nullable(true)
                .constraint(FieldConstraints::AllowedValues {
                    values: vec!["alice".to_string()], // "bob" is not allowed
                })
                .build(),
        )
        .build();

    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &ValidationContext::new())
        .await;

    assert!(!report.passed);
    assert!(report.errors.iter().any(|e| e.contains("name")));
}
