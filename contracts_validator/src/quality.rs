//! Quality check validation logic.
//!
//! This module handles validation of data quality checks including:
//! - Completeness: Percentage of non-null values
//! - Uniqueness: Detection of duplicate values
//! - Freshness: Data staleness checks (implemented separately)

use crate::{DataSet, DataValue, ValidationError};
use contracts_core::{CompletenessCheck, Contract, UniquenessCheck};
use std::collections::HashSet;

/// Validates quality checks on a dataset.
pub struct QualityValidator;

impl QualityValidator {
    /// Creates a new quality validator.
    pub fn new() -> Self {
        Self
    }

    /// Validates all quality checks in a contract against a dataset.
    ///
    /// Returns a list of validation errors. An empty list indicates success.
    pub fn validate(&self, contract: &Contract, dataset: &DataSet) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        let quality_checks = match &contract.quality_checks {
            Some(qc) => qc,
            None => return errors, // No quality checks defined
        };

        // Skip quality checks for empty datasets
        if dataset.is_empty() {
            return errors;
        }

        // Completeness check
        if let Some(completeness) = &quality_checks.completeness {
            errors.extend(self.validate_completeness(completeness, dataset));
        }

        // Uniqueness check
        if let Some(uniqueness) = &quality_checks.uniqueness {
            errors.extend(self.validate_uniqueness(uniqueness, dataset));
        }

        errors
    }

    /// Validates completeness requirements.
    fn validate_completeness(
        &self,
        check: &CompletenessCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        for field_name in &check.fields {
            let result = self.check_field_completeness(field_name, dataset, check.threshold);
            if let Err(err) = result {
                errors.push(err);
            }
        }

        errors
    }

    /// Checks completeness for a single field.
    fn check_field_completeness(
        &self,
        field_name: &str,
        dataset: &DataSet,
        threshold: f64,
    ) -> Result<(), ValidationError> {
        let total_rows = dataset.len();
        if total_rows == 0 {
            return Ok(());
        }

        let mut non_null_count = 0;

        for row in dataset.rows() {
            if let Some(value) = row.get(field_name) {
                if !value.is_null() {
                    non_null_count += 1;
                }
            }
            // Missing field counts as null
        }

        let completeness_ratio = non_null_count as f64 / total_rows as f64;

        if completeness_ratio < threshold {
            return Err(ValidationError::quality_check(format!(
                "Completeness check failed for field '{}': {:.2}% < {:.2}% (threshold)",
                field_name,
                completeness_ratio * 100.0,
                threshold * 100.0
            )));
        }

        Ok(())
    }

    /// Validates uniqueness requirements.
    fn validate_uniqueness(
        &self,
        check: &UniquenessCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        let duplicates = self.find_duplicates(&check.fields, dataset);

        if !duplicates.is_empty() {
            errors.push(ValidationError::quality_check(format!(
                "Uniqueness check failed for fields [{}]: found {} duplicate(s)",
                check.fields.join(", "),
                duplicates.len()
            )));
        }

        errors
    }

    /// Finds duplicate values in the specified fields.
    fn find_duplicates(&self, fields: &[String], dataset: &DataSet) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut duplicates = Vec::new();

        for row in dataset.rows() {
            // Build a composite key from all uniqueness fields
            let mut key_parts = Vec::new();
            let mut has_all_fields = true;

            for field in fields {
                match row.get(field) {
                    Some(value) => {
                        key_parts.push(self.value_to_string(value));
                    }
                    None => {
                        has_all_fields = false;
                        break;
                    }
                }
            }

            if !has_all_fields {
                continue; // Skip rows with missing fields
            }

            let key = key_parts.join("|");

            if !seen.insert(key.clone()) {
                // This is a duplicate
                duplicates.push(key);
            }
        }

        duplicates
    }

    /// Converts a DataValue to a string representation for comparison.
    fn value_to_string(&self, value: &DataValue) -> String {
        match value {
            DataValue::Null => "NULL".to_string(),
            DataValue::String(s) => s.clone(),
            DataValue::Int(i) => i.to_string(),
            DataValue::Float(f) => f.to_string(),
            DataValue::Bool(b) => b.to_string(),
            DataValue::Timestamp(ts) => ts.clone(),
            DataValue::Map(_) => "[map]".to_string(),
            DataValue::List(_) => "[list]".to_string(),
        }
    }
}

impl Default for QualityValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_core::{ContractBuilder, DataFormat, FieldBuilder, QualityChecks};
    use std::collections::HashMap;

    #[test]
    fn test_completeness_pass() {
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
        for i in 0..10 {
            let mut row = HashMap::new();
            if i < 9 {
                // 90% completeness
                row.insert("id".to_string(), DataValue::String(i.to_string()));
            } else {
                row.insert("id".to_string(), DataValue::Null);
            }
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0, "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_completeness_fail() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(true).build())
            .quality_checks(QualityChecks {
                completeness: Some(CompletenessCheck {
                    threshold: 0.95,
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
                // 90% completeness
                row.insert("id".to_string(), DataValue::String(i.to_string()));
            } else {
                row.insert("id".to_string(), DataValue::Null);
            }
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], ValidationError::QualityCheckFailed(_)));
    }

    #[test]
    fn test_uniqueness_pass() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: Some(UniquenessCheck {
                    fields: vec!["id".to_string()],
                    scope: None,
                }),
                freshness: None,
                custom_checks: None,
            })
            .build();

        let mut rows = Vec::new();
        for i in 0..5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), DataValue::String(i.to_string()));
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_uniqueness_fail() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: Some(UniquenessCheck {
                    fields: vec!["id".to_string()],
                    scope: None,
                }),
                freshness: None,
                custom_checks: None,
            })
            .build();

        let mut rows = Vec::new();
        for i in 0..5 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), DataValue::String((i % 3).to_string()));
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], ValidationError::QualityCheckFailed(_)));
    }

    #[test]
    fn test_composite_uniqueness() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("user_id", "string")
                    .nullable(false)
                    .build(),
            )
            .field(
                FieldBuilder::new("event_id", "string")
                    .nullable(false)
                    .build(),
            )
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: Some(UniquenessCheck {
                    fields: vec!["user_id".to_string(), "event_id".to_string()],
                    scope: None,
                }),
                freshness: None,
                custom_checks: None,
            })
            .build();

        let mut rows = Vec::new();

        // Add unique combinations
        let mut row1 = HashMap::new();
        row1.insert("user_id".to_string(), DataValue::String("u1".to_string()));
        row1.insert("event_id".to_string(), DataValue::String("e1".to_string()));
        rows.push(row1);

        let mut row2 = HashMap::new();
        row2.insert("user_id".to_string(), DataValue::String("u1".to_string()));
        row2.insert("event_id".to_string(), DataValue::String("e2".to_string()));
        rows.push(row2);

        let dataset = DataSet::from_rows(rows);
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_empty_dataset_no_quality_checks() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
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

        let dataset = DataSet::empty();
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0); // Empty dataset skips quality checks
    }

    #[test]
    fn test_multiple_fields_completeness() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(true).build())
            .field(FieldBuilder::new("name", "string").nullable(true).build())
            .quality_checks(QualityChecks {
                completeness: Some(CompletenessCheck {
                    threshold: 0.9,
                    fields: vec!["id".to_string(), "name".to_string()],
                }),
                uniqueness: None,
                freshness: None,
                custom_checks: None,
            })
            .build();

        let mut rows = Vec::new();
        for i in 0..10 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), DataValue::String(i.to_string()));
            if i < 8 {
                // 80% completeness for name - should fail
                row.insert("name".to_string(), DataValue::String(format!("name{}", i)));
            } else {
                row.insert("name".to_string(), DataValue::Null);
            }
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let validator = QualityValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1); // Only name field should fail
    }
}
