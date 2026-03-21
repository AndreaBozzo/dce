//! Tests for the native DataFusion context validation path.
//!
//! These tests verify `validate_with_context()` works correctly when a
//! `SessionContext` with a pre-registered table is passed directly,
//! bypassing the `DataSet` → Arrow conversion.

use arrow_array::RecordBatch;
use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use contracts_core::{
    CompletenessCheck, ContractBuilder, CustomCheck, DataFormat, FieldBuilder, FieldConstraints,
    QualityChecks, TargetLeakageCheck, ValidationContext,
};
use contracts_validator::DataValidator;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Helper: register a RecordBatch as "data" in a new SessionContext.
fn make_context(batch: RecordBatch) -> SessionContext {
    let ctx = SessionContext::new();
    ctx.register_batch("data", batch).unwrap();
    ctx
}

#[tokio::test]
async fn test_context_nullability_check() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "id",
        ArrowDataType::Utf8,
        true,
    )]));

    let mut builder = StringBuilder::new();
    builder.append_value("a");
    builder.append_null();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(FieldBuilder::new("id", "string").nullable(false).build())
        .build();

    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(!report.passed);
    assert!(report.errors.iter().any(|e| e.contains("null")));
    assert_eq!(report.stats.records_validated, 2);
}

#[tokio::test]
async fn test_context_constraint_allowed_values() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "status",
        ArrowDataType::Utf8,
        false,
    )]));

    let mut builder = StringBuilder::new();
    builder.append_value("active");
    builder.append_value("inactive");
    builder.append_value("unknown"); // not in allowed values
    let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("status", "string")
                .nullable(false)
                .constraint(FieldConstraints::AllowedValues {
                    values: vec!["active".to_string(), "inactive".to_string()],
                })
                .build(),
        )
        .build();

    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(!report.passed);
    assert!(report.errors.iter().any(|e| e.contains("allowed")));
}

#[tokio::test]
async fn test_context_range_constraint() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "age",
        ArrowDataType::Int64,
        false,
    )]));

    let mut builder = Int64Builder::new();
    builder.append_value(25);
    builder.append_value(150); // out of range
    let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("age", "int64")
                .nullable(false)
                .constraint(FieldConstraints::Range {
                    min: 0.0,
                    max: 130.0,
                })
                .build(),
        )
        .build();

    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(!report.passed);
    assert!(report.errors.iter().any(|e| e.contains("range")));
}

#[tokio::test]
async fn test_context_quality_completeness() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "email",
        ArrowDataType::Utf8,
        true,
    )]));

    let mut builder = StringBuilder::new();
    builder.append_value("a@b.com");
    builder.append_null();
    builder.append_null();
    builder.append_value("c@d.com");
    let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(FieldBuilder::new("email", "string").nullable(true).build())
        .quality_checks(QualityChecks {
            completeness: Some(CompletenessCheck {
                threshold: 0.9, // 50% completeness will fail 90% threshold
                fields: vec!["email".to_string()],
            }),
            uniqueness: None,
            freshness: None,
            custom_checks: None,
            ml_checks: None,
        })
        .build();

    // Non-strict: quality issues become warnings
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(report.passed); // warnings don't fail
    assert!(!report.warnings.is_empty());
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.to_lowercase().contains("completeness"))
    );
}

#[tokio::test]
async fn test_context_custom_sql_check() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "amount",
        ArrowDataType::Float64,
        false,
    )]));

    let mut builder = Float64Builder::new();
    builder.append_value(10.0);
    builder.append_value(-5.0); // negative amount
    builder.append_value(20.0);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("amount", "float64")
                .nullable(false)
                .build(),
        )
        .quality_checks(QualityChecks {
            completeness: None,
            uniqueness: None,
            freshness: None,
            custom_checks: Some(vec![CustomCheck {
                name: "no_negative_amounts".to_string(),
                definition: "SELECT COUNT(*) FROM data WHERE amount < 0".to_string(),
                severity: Some("error".to_string()),
            }]),
            ml_checks: None,
        })
        .build();

    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(!report.passed);
    assert!(
        report
            .errors
            .iter()
            .any(|e| e.contains("no_negative_amounts"))
    );
}

#[tokio::test]
async fn test_context_passing_validation() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("id", ArrowDataType::Int64, false),
        ArrowField::new("name", ArrowDataType::Utf8, false),
    ]));

    let mut id_builder = Int64Builder::new();
    id_builder.append_value(1);
    id_builder.append_value(2);

    let mut name_builder = StringBuilder::new();
    name_builder.append_value("Alice");
    name_builder.append_value("Bob");

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(id_builder.finish()),
            Arc::new(name_builder.finish()),
        ],
    )
    .unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(FieldBuilder::new("id", "int64").nullable(false).build())
        .field(FieldBuilder::new("name", "string").nullable(false).build())
        .build();

    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(report.passed);
    assert!(report.errors.is_empty());
    assert_eq!(report.stats.records_validated, 2);
    assert_eq!(report.stats.fields_checked, 2);
}

/// SQL-based ML checks now execute in the native context path.
/// TargetLeakage with a perfect correlation (feature == target) should produce a warning.
#[tokio::test]
async fn test_context_ml_checks_execute_via_sql() {
    let schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("feature", ArrowDataType::Float64, false),
        ArrowField::new("target", ArrowDataType::Float64, false),
    ]));

    let feature = arrow_array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let target = arrow_array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(feature), Arc::new(target)]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("feature", "float64")
                .nullable(false)
                .build(),
        )
        .field(
            FieldBuilder::new("target", "float64")
                .nullable(false)
                .build(),
        )
        .quality_checks(QualityChecks {
            completeness: None,
            uniqueness: None,
            freshness: None,
            custom_checks: None,
            ml_checks: Some(contracts_core::MlChecks {
                no_overlap: None,
                temporal_split: None,
                class_balance: None,
                feature_drift: None,
                target_leakage: Some(TargetLeakageCheck {
                    target_field: "target".to_string(),
                    feature_fields: vec!["feature".to_string()],
                    max_correlation: Some(0.9),
                }),
                null_rate_by_group: None,
            }),
        })
        .build();

    // Non-strict: ML check results go to warnings
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(report.passed);
    assert!(
        report.warnings.iter().any(|w| w.contains("TargetLeakage")),
        "Expected TargetLeakage warning, got: {:?}",
        report.warnings,
    );
}

/// NoOverlap and TemporalSplit still produce a skip warning in the context path.
#[tokio::test]
async fn test_context_row_only_ml_checks_skipped_with_warning() {
    let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
        "value",
        ArrowDataType::Float64,
        false,
    )]));

    let mut builder = Float64Builder::new();
    builder.append_value(1.0);
    let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();

    let ctx = make_context(batch);

    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("value", "float64")
                .nullable(false)
                .build(),
        )
        .quality_checks(QualityChecks {
            completeness: None,
            uniqueness: None,
            freshness: None,
            custom_checks: None,
            ml_checks: Some(contracts_core::MlChecks {
                no_overlap: Some(contracts_core::NoOverlapCheck {
                    split_field: "split".to_string(),
                    key_fields: vec!["value".to_string()],
                }),
                temporal_split: None,
                class_balance: None,
                feature_drift: None,
                target_leakage: None,
                null_rate_by_group: None,
            }),
        })
        .build();

    let context = ValidationContext::new();
    let mut validator = DataValidator::new();
    let report = validator
        .validate_with_context(&contract, &ctx, &context)
        .await;

    assert!(report.passed);
    assert!(
        report
            .warnings
            .iter()
            .any(|w| w.contains("NoOverlap and TemporalSplit")),
        "Expected row-only ML skip warning, got: {:?}",
        report.warnings,
    );
}
