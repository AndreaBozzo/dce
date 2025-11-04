//! Tests to verify correct handling of null values across all validators.
//!
//! This test suite ensures that null values are handled consistently:
//! - Schema validator checks nullability constraints
//! - Constraint validator skips null values (nullability is schema's responsibility)
//! - Quality checks count nulls for completeness but don't error on them independently
//!
//! This prevents logical bugs where null values might incorrectly pass or fail validation.

use contracts_core::{
    CompletenessCheck, ContractBuilder, DataFormat, FieldBuilder, FieldConstraints, QualityChecks,
    ValidationContext,
};
use contracts_validator::{DataSet, DataValidator, DataValue};
use std::collections::HashMap;

#[test]
fn test_null_in_non_nullable_field_fails_schema_validation() {
    // Non-nullable field with null should fail at schema validation level
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("id", "string")
                .nullable(false) // Explicitly non-nullable
                .build(),
        )
        .build();

    let mut row = HashMap::new();
    row.insert("id".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(!report.passed, "Null in non-nullable field should fail");
    assert_eq!(report.errors.len(), 1);
    assert!(
        report.errors[0].contains("null"),
        "Error should mention null: {}",
        report.errors[0]
    );
}

#[test]
fn test_null_in_nullable_field_passes_schema_validation() {
    // Nullable field with null should pass schema validation
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("id", "string")
                .nullable(true) // Explicitly nullable
                .build(),
        )
        .build();

    let mut row = HashMap::new();
    row.insert("id".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(
        report.passed,
        "Null in nullable field should pass, errors: {:?}",
        report.errors
    );
    assert_eq!(report.errors.len(), 0);
}

#[test]
fn test_null_skips_constraint_validation() {
    // Null values should skip constraint checks even if constraints are defined
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("status", "string")
                .nullable(true)
                .constraint(FieldConstraints::AllowedValues {
                    values: vec!["active".to_string(), "inactive".to_string()],
                })
                .build(),
        )
        .build();

    let mut row = HashMap::new();
    row.insert("status".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(
        report.passed,
        "Null should skip constraint validation, errors: {:?}",
        report.errors
    );
    assert_eq!(report.errors.len(), 0);
}

#[test]
fn test_null_with_range_constraint_skipped() {
    // Null values should skip range constraints
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("age", "int64")
                .nullable(true)
                .constraint(FieldConstraints::Range {
                    min: 0.0,
                    max: 120.0,
                })
                .build(),
        )
        .build();

    let mut row = HashMap::new();
    row.insert("age".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed, "Null should skip range constraint");
}

#[test]
fn test_null_with_pattern_constraint_skipped() {
    // Null values should skip pattern constraints
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("url", "string")
                .nullable(true)
                .constraint(FieldConstraints::Pattern {
                    regex: r"^https?://.*".to_string(),
                })
                .build(),
        )
        .build();

    let mut row = HashMap::new();
    row.insert("url".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed, "Null should skip pattern constraint");
}

#[test]
fn test_null_counted_in_completeness_check() {
    // Null values should be counted for completeness checks
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("id", "string")
                .nullable(true) // Nullable to avoid schema violation
                .build(),
        )
        .quality_checks(QualityChecks {
            completeness: Some(CompletenessCheck {
                threshold: 0.8, // 80% threshold
                fields: vec!["id".to_string()],
            }),
            uniqueness: None,
            freshness: None,
            custom_checks: None,
        })
        .build();

    // Create dataset with 70% completeness (30% null)
    let mut rows = Vec::new();
    for i in 0..10 {
        let mut row = HashMap::new();
        if i < 7 {
            row.insert("id".to_string(), DataValue::String(format!("id_{}", i)));
        } else {
            row.insert("id".to_string(), DataValue::Null);
        }
        rows.push(row);
    }

    let dataset = DataSet::from_rows(rows);
    let context = ValidationContext::new(); // Non-strict mode
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    // Should pass overall but have completeness warning
    assert!(report.passed, "Should pass in non-strict mode");
    assert!(
        !report.warnings.is_empty(),
        "Should have completeness warning"
    );
    assert!(
        report.warnings[0].contains("Completeness"),
        "Warning should be about completeness: {}",
        report.warnings[0]
    );
}

#[test]
fn test_missing_field_vs_null_field() {
    // Missing field and null field should be treated the same for non-nullable fields
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("required_id", "string")
                .nullable(false)
                .build(),
        )
        .field(
            FieldBuilder::new("optional_id", "string")
                .nullable(true)
                .build(),
        )
        .build();

    // Row 1: required field is null
    let mut row1 = HashMap::new();
    row1.insert("required_id".to_string(), DataValue::Null);
    row1.insert("optional_id".to_string(), DataValue::Null);

    // Row 2: required field is missing
    let mut row2 = HashMap::new();
    // required_id is completely missing
    row2.insert("optional_id".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row1, row2]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(!report.passed);
    assert_eq!(
        report.errors.len(),
        2,
        "Both null and missing should fail for non-nullable field"
    );
}

#[test]
fn test_null_in_non_nullable_with_constraint() {
    // Null in non-nullable field should fail at schema level, not reach constraint validation
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("status", "string")
                .nullable(false) // Non-nullable
                .constraint(FieldConstraints::AllowedValues {
                    values: vec!["active".to_string()],
                })
                .build(),
        )
        .build();

    let mut row = HashMap::new();
    row.insert("status".to_string(), DataValue::Null);

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(!report.passed);
    // Should only have 1 error from schema validation, not 2 (schema + constraint)
    assert_eq!(
        report.errors.len(),
        1,
        "Should fail once at schema level, not also at constraint level"
    );
    assert!(report.errors[0].contains("null"));
}

#[test]
fn test_completeness_with_missing_vs_null() {
    // Completeness should treat missing fields and null values the same way
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(FieldBuilder::new("id", "string").nullable(true).build())
        .quality_checks(QualityChecks {
            completeness: Some(CompletenessCheck {
                threshold: 0.8,
                fields: vec!["id".to_string()],
            }),
            uniqueness: None,
            freshness: None,
            custom_checks: None,
        })
        .build();

    let mut rows = Vec::new();

    // 3 rows with values
    for i in 0..3 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String(format!("id_{}", i)));
        rows.push(row);
    }

    // 3 rows with null
    for _ in 0..3 {
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::Null);
        rows.push(row);
    }

    // 4 rows with field completely missing
    for _ in 0..4 {
        let row = HashMap::new(); // id field not present at all
        rows.push(row);
    }

    // Total: 10 rows, 3 with values, 7 without = 30% completeness (below 80% threshold)

    let dataset = DataSet::from_rows(rows);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed); // Non-strict mode
    assert!(!report.warnings.is_empty());
    assert!(report.warnings[0].contains("30.00%")); // Should show 30% completeness
}

#[test]
fn test_strict_mode_with_null_violations() {
    // In strict mode, quality check failures on nulls should be errors
    let contract = ContractBuilder::new("test", "owner")
        .location("s3://test")
        .format(DataFormat::Iceberg)
        .field(FieldBuilder::new("id", "string").nullable(true).build())
        .quality_checks(QualityChecks {
            completeness: Some(CompletenessCheck {
                threshold: 0.99,
                fields: vec!["id".to_string()],
            }),
            uniqueness: None,
            freshness: None,
            custom_checks: None,
        })
        .build();

    let mut rows = Vec::new();
    for i in 0..10 {
        let mut row = HashMap::new();
        if i < 9 {
            row.insert("id".to_string(), DataValue::String(format!("id_{}", i)));
        } else {
            row.insert("id".to_string(), DataValue::Null);
        }
        rows.push(row);
    }

    let dataset = DataSet::from_rows(rows);
    let context = ValidationContext::new().with_strict(true);
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(!report.passed, "Should fail in strict mode");
    assert!(!report.errors.is_empty());
    assert!(report.errors[0].contains("Completeness"));
}
