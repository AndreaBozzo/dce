//! Integration tests for the validation engine.
//!
//! These tests demonstrate end-to-end validation scenarios using complete contracts
//! and realistic datasets.

use contracts_core::{
    CompletenessCheck, Contract, ContractBuilder, CustomCheck, DataFormat, FieldBuilder,
    FieldConstraints, FreshnessCheck, QualityChecks, UniquenessCheck, ValidationContext,
};
use contracts_validator::{DataSet, DataValidator, DataValue};
use std::collections::HashMap;

/// Creates a realistic user events contract similar to the example in examples/
fn create_user_events_contract() -> Contract {
    ContractBuilder::new("user_events", "analytics-team")
        .version("1.0.0")
        .description("User interaction events dataset for analytics and ML")
        .location("s3://data-lake/analytics/user_events")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("event_id", "string")
                .nullable(false)
                .description("Unique identifier for each event")
                .tags(vec!["primary_key".to_string(), "pii".to_string()])
                .build(),
        )
        .field(
            FieldBuilder::new("user_id", "string")
                .nullable(false)
                .description("Unique user identifier")
                .tags(vec!["pii".to_string(), "partition_key".to_string()])
                .build(),
        )
        .field(
            FieldBuilder::new("event_type", "string")
                .nullable(false)
                .description("Type of user interaction")
                .constraint(FieldConstraints::AllowedValues {
                    values: vec![
                        "page_view".to_string(),
                        "button_click".to_string(),
                        "form_submit".to_string(),
                        "purchase".to_string(),
                        "sign_up".to_string(),
                        "sign_out".to_string(),
                    ],
                })
                .build(),
        )
        .field(
            FieldBuilder::new("event_timestamp", "timestamp")
                .nullable(false)
                .description("When the event occurred (UTC)")
                .tags(vec!["sort_key".to_string()])
                .build(),
        )
        .field(
            FieldBuilder::new("session_id", "string")
                .nullable(true)
                .description("Session identifier for grouping events")
                .build(),
        )
        .field(
            FieldBuilder::new("page_url", "string")
                .nullable(true)
                .description("URL where the event occurred")
                .constraint(FieldConstraints::Pattern {
                    regex: r"^https?://.*".to_string(),
                })
                .build(),
        )
        .quality_checks(QualityChecks {
            completeness: Some(CompletenessCheck {
                threshold: 0.99,
                fields: vec![
                    "event_id".to_string(),
                    "user_id".to_string(),
                    "event_type".to_string(),
                    "event_timestamp".to_string(),
                ],
            }),
            uniqueness: Some(UniquenessCheck {
                fields: vec!["event_id".to_string()],
                scope: Some("global".to_string()),
            }),
            freshness: Some(FreshnessCheck {
                max_delay: "1h".to_string(),
                metric: "event_timestamp".to_string(),
            }),
            custom_checks: Some(vec![
                CustomCheck {
                    name: "valid_event_types".to_string(),
                    definition: "SELECT COUNT(*) = 0 FROM user_events WHERE event_type NOT IN ('page_view', 'button_click', 'form_submit', 'purchase', 'sign_up', 'sign_out')".to_string(),
                    severity: Some("error".to_string()),
                },
                CustomCheck {
                    name: "future_timestamps".to_string(),
                    definition: "SELECT COUNT(*) = 0 FROM user_events WHERE event_timestamp > CURRENT_TIMESTAMP()".to_string(),
                    severity: Some("error".to_string()),
                },
            ]),
        })
        .build()
}

/// Creates a valid dataset that passes all contract checks
fn create_valid_dataset() -> DataSet {
    let mut rows = Vec::new();

    for i in 0..100 {
        let mut row = HashMap::new();
        row.insert(
            "event_id".to_string(),
            DataValue::String(format!("evt_{}", i)),
        );
        row.insert(
            "user_id".to_string(),
            DataValue::String(format!("user_{}", i % 20)),
        );
        row.insert(
            "event_type".to_string(),
            DataValue::String(["page_view", "button_click", "form_submit"][i % 3].to_string()),
        );

        // Recent timestamp (within last hour)
        let now = chrono::Utc::now();
        let recent = now - chrono::Duration::minutes((i % 50) as i64);
        row.insert(
            "event_timestamp".to_string(),
            DataValue::Timestamp(recent.to_rfc3339()),
        );

        row.insert(
            "session_id".to_string(),
            DataValue::String(format!("sess_{}", i % 10)),
        );

        row.insert(
            "page_url".to_string(),
            DataValue::String(format!("https://example.com/page{}", i)),
        );

        rows.push(row);
    }

    DataSet::from_rows(rows)
}

#[test]
fn test_valid_user_events_dataset() {
    let contract = create_user_events_contract();
    let dataset = create_valid_dataset();
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(
        report.passed,
        "Expected validation to pass, but got errors: {:?}",
        report.errors
    );
    assert_eq!(report.errors.len(), 0);
    assert_eq!(report.stats.records_validated, 100);
}

#[test]
fn test_schema_violations() {
    let contract = create_user_events_contract();

    // Create a dataset with schema violations
    let mut row = HashMap::new();
    row.insert("event_id".to_string(), DataValue::Null); // Non-nullable field is null
    row.insert(
        "user_id".to_string(),
        DataValue::String("user_1".to_string()),
    );
    row.insert(
        "event_type".to_string(),
        DataValue::String("page_view".to_string()),
    );
    row.insert(
        "event_timestamp".to_string(),
        DataValue::Timestamp(chrono::Utc::now().to_rfc3339()),
    );

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(!report.passed);
    assert!(!report.errors.is_empty());
    assert!(report.errors[0].contains("null"));
}

#[test]
fn test_constraint_violations() {
    let contract = create_user_events_contract();

    // Create a dataset with constraint violations
    let mut row = HashMap::new();
    row.insert(
        "event_id".to_string(),
        DataValue::String("evt_1".to_string()),
    );
    row.insert(
        "user_id".to_string(),
        DataValue::String("user_1".to_string()),
    );
    row.insert(
        "event_type".to_string(),
        DataValue::String("invalid_type".to_string()),
    ); // Not in allowed values
    row.insert(
        "event_timestamp".to_string(),
        DataValue::Timestamp(chrono::Utc::now().to_rfc3339()),
    );
    row.insert(
        "page_url".to_string(),
        DataValue::String("not-a-url".to_string()),
    ); // Doesn't match pattern

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(!report.passed);
    assert!(report.errors.len() >= 2); // Both constraint violations
}

#[test]
fn test_completeness_check() {
    // Create a simpler contract with nullable event_id to test completeness without schema violations
    let contract = ContractBuilder::new("test_events", "test-team")
        .location("s3://test/data")
        .format(DataFormat::Iceberg)
        .field(
            FieldBuilder::new("event_id", "string")
                .nullable(true) // Allow null to avoid schema violations
                .build(),
        )
        .field(
            FieldBuilder::new("user_id", "string")
                .nullable(false)
                .build(),
        )
        .quality_checks(QualityChecks {
            completeness: Some(CompletenessCheck {
                threshold: 0.95, // 95% threshold
                fields: vec!["event_id".to_string()],
            }),
            uniqueness: None,
            freshness: None,
            custom_checks: None,
        })
        .build();

    // Create a dataset with low completeness (90% < 95% threshold)
    let mut rows = Vec::new();
    for i in 0..100 {
        let mut row = HashMap::new();
        if i < 90 {
            row.insert(
                "event_id".to_string(),
                DataValue::String(format!("evt_{}", i)),
            );
        } else {
            row.insert("event_id".to_string(), DataValue::Null); // 10% null
        }
        row.insert(
            "user_id".to_string(),
            DataValue::String(format!("user_{}", i)),
        );
        rows.push(row);
    }

    let dataset = DataSet::from_rows(rows);
    let context = ValidationContext::new(); // Non-strict mode - quality checks are warnings
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    // In non-strict mode, quality check failures are warnings
    assert!(
        report.passed,
        "Expected passed=true, got errors: {:?}",
        report.errors
    );
    assert!(!report.warnings.is_empty());
    assert!(report.warnings[0].contains("Completeness"));
}

#[test]
fn test_uniqueness_check() {
    let contract = create_user_events_contract();

    // Create a dataset with duplicate event_ids
    let mut rows = Vec::new();
    for i in 0..5 {
        let mut row = HashMap::new();
        row.insert(
            "event_id".to_string(),
            DataValue::String("duplicate_id".to_string()),
        ); // Same ID
        row.insert(
            "user_id".to_string(),
            DataValue::String(format!("user_{}", i)),
        );
        row.insert(
            "event_type".to_string(),
            DataValue::String("page_view".to_string()),
        );
        row.insert(
            "event_timestamp".to_string(),
            DataValue::Timestamp(chrono::Utc::now().to_rfc3339()),
        );
        rows.push(row);
    }

    let dataset = DataSet::from_rows(rows);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed); // Non-strict mode
    assert!(!report.warnings.is_empty());
    assert!(report.warnings.iter().any(|w| w.contains("Uniqueness")));
}

#[test]
fn test_freshness_check() {
    let contract = create_user_events_contract();

    // Create a dataset with stale data (2 hours old, threshold is 1 hour)
    let mut row = HashMap::new();
    row.insert(
        "event_id".to_string(),
        DataValue::String("evt_1".to_string()),
    );
    row.insert(
        "user_id".to_string(),
        DataValue::String("user_1".to_string()),
    );
    row.insert(
        "event_type".to_string(),
        DataValue::String("page_view".to_string()),
    );

    let old = chrono::Utc::now() - chrono::Duration::hours(2);
    row.insert(
        "event_timestamp".to_string(),
        DataValue::Timestamp(old.to_rfc3339()),
    );

    let dataset = DataSet::from_rows(vec![row]);
    let context = ValidationContext::new();
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed); // Non-strict mode
    assert!(!report.warnings.is_empty());
    assert!(report.warnings.iter().any(|w| w.contains("stale")));
}

#[test]
fn test_strict_mode_quality_checks() {
    let contract = create_user_events_contract();

    // Create a dataset with low completeness
    let mut rows = Vec::new();
    for i in 0..100 {
        let mut row = HashMap::new();
        if i < 90 {
            row.insert(
                "event_id".to_string(),
                DataValue::String(format!("evt_{}", i)),
            );
        } else {
            row.insert("event_id".to_string(), DataValue::Null);
        }
        row.insert(
            "user_id".to_string(),
            DataValue::String(format!("user_{}", i)),
        );
        row.insert(
            "event_type".to_string(),
            DataValue::String("page_view".to_string()),
        );
        row.insert(
            "event_timestamp".to_string(),
            DataValue::Timestamp(chrono::Utc::now().to_rfc3339()),
        );
        rows.push(row);
    }

    let dataset = DataSet::from_rows(rows);
    let context = ValidationContext::new().with_strict(true); // Strict mode
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    // In strict mode, quality check failures are errors
    assert!(!report.passed);
    assert!(!report.errors.is_empty());
}

#[test]
fn test_sample_size_validation() {
    let contract = create_user_events_contract();
    let dataset = create_valid_dataset(); // 100 rows

    let context = ValidationContext::new().with_sample_size(10); // Only validate 10 rows
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed);
    assert_eq!(report.stats.records_validated, 10);
}

#[test]
fn test_schema_only_validation() {
    let contract = create_user_events_contract();
    let dataset = create_valid_dataset();

    let context = ValidationContext::new().with_schema_only(true);
    let mut validator = DataValidator::new();

    let report = validator.validate_with_data(&contract, &dataset, &context);

    assert!(report.passed);
    // No quality check warnings in schema-only mode
    assert_eq!(report.warnings.len(), 0);
}
