//! Main validation engine.
//!
//! This module provides the main `DataValidator` that orchestrates all validation
//! checks including schema, constraints, quality checks, and custom validations.

use crate::{ConstraintValidator, CustomValidator, DataSet, QualityValidator, SchemaValidator};
use contracts_core::{
    Contract, ContractValidator, ValidationContext, ValidationReport, ValidationStats,
};
use std::time::Instant;

/// Main validation engine for data contracts.
///
/// Orchestrates all validation checks and produces comprehensive validation reports.
///
/// # Example
///
/// ```rust
/// use contracts_validator::{DataValidator, DataSet};
/// use contracts_core::{Contract, ValidationContext, ContractBuilder, DataFormat};
///
/// # let contract = ContractBuilder::new("test", "owner")
/// #     .location("s3://test")
/// #     .format(DataFormat::Iceberg)
/// #     .build();
/// let mut validator = DataValidator::new();
/// let dataset = DataSet::empty();
/// let context = ValidationContext::new();
///
/// let report = validator.validate_with_data(&contract, &dataset, &context);
///
/// if report.passed {
///     println!("Validation passed!");
/// } else {
///     for error in &report.errors {
///         println!("Error: {}", error);
///     }
/// }
/// ```
pub struct DataValidator {
    schema_validator: SchemaValidator,
    constraint_validator: ConstraintValidator,
    quality_validator: QualityValidator,
    custom_validator: CustomValidator,
}

impl DataValidator {
    /// Creates a new data validator.
    pub fn new() -> Self {
        Self {
            schema_validator: SchemaValidator::new(),
            constraint_validator: ConstraintValidator::new(),
            quality_validator: QualityValidator::new(),
            custom_validator: CustomValidator::new(),
        }
    }

    /// Validates a contract against a dataset.
    ///
    /// This is the main validation entry point. It runs all validation checks
    /// and returns a comprehensive report.
    ///
    /// # Arguments
    ///
    /// * `contract` - The contract to validate against
    /// * `dataset` - The data to validate
    /// * `context` - Validation context with options
    ///
    /// # Returns
    ///
    /// A `ValidationReport` containing all errors, warnings, and statistics.
    pub fn validate_with_data(
        &mut self,
        contract: &Contract,
        dataset: &DataSet,
        context: &ValidationContext,
    ) -> ValidationReport {
        let start = Instant::now();
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Sample dataset if needed
        let dataset_to_validate = if let Some(sample_size) = context.sample_size {
            dataset.sample(sample_size)
        } else {
            dataset.clone()
        };

        // 1. Schema validation (always runs)
        let schema_errors = self
            .schema_validator
            .validate(contract, &dataset_to_validate);
        errors.extend(schema_errors.iter().map(|e| e.to_string()));

        // If schema validation fails and strict mode, stop here
        if context.strict && !errors.is_empty() {
            return self.build_report(errors, warnings, &dataset_to_validate, start);
        }

        // 2. Constraint validation
        let constraint_errors = self
            .constraint_validator
            .validate(contract, &dataset_to_validate);
        errors.extend(constraint_errors.iter().map(|e| e.to_string()));

        // Stop if in schema-only mode
        if context.schema_only {
            return self.build_report(errors, warnings, &dataset_to_validate, start);
        }

        // 3. Quality checks
        let quality_errors = self
            .quality_validator
            .validate(contract, &dataset_to_validate);

        // Quality check errors can be warnings in non-strict mode
        if context.strict {
            errors.extend(quality_errors.iter().map(|e| e.to_string()));
        } else {
            warnings.extend(quality_errors.iter().map(|e| e.to_string()));
        }

        // 4. Custom checks and freshness
        let custom_errors = self
            .custom_validator
            .validate(contract, &dataset_to_validate);

        // Custom check errors follow the severity defined in the check
        // For now, treat as warnings in non-strict mode
        if context.strict {
            errors.extend(custom_errors.iter().map(|e| e.to_string()));
        } else {
            warnings.extend(custom_errors.iter().map(|e| e.to_string()));
        }

        self.build_report(errors, warnings, &dataset_to_validate, start)
    }

    /// Builds a validation report from collected errors and warnings.
    fn build_report(
        &self,
        errors: Vec<String>,
        warnings: Vec<String>,
        dataset: &DataSet,
        start: Instant,
    ) -> ValidationReport {
        let duration_ms = start.elapsed().as_millis() as u64;

        ValidationReport {
            passed: errors.is_empty(),
            errors,
            warnings,
            stats: ValidationStats {
                records_validated: dataset.len(),
                fields_checked: 0, // Could be enhanced to count actual fields
                constraints_evaluated: 0, // Could be enhanced to count constraints
                duration_ms,
            },
        }
    }

    /// Validates only the contract definition itself (no data).
    ///
    /// Useful for checking if a contract is well-formed before attempting
    /// to validate data against it.
    pub fn validate_definition(&self, contract: &Contract) -> ValidationReport {
        let start = Instant::now();
        let errors: Vec<String> = self
            .schema_validator
            .validate_schema_definition(contract)
            .iter()
            .map(|e| e.to_string())
            .collect();

        ValidationReport {
            passed: errors.is_empty(),
            errors,
            warnings: Vec::new(),
            stats: ValidationStats {
                records_validated: 0,
                fields_checked: contract.schema.fields.len(),
                constraints_evaluated: 0,
                duration_ms: start.elapsed().as_millis() as u64,
            },
        }
    }
}

impl Default for DataValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl ContractValidator for DataValidator {
    fn validate(
        &self,
        _contract: &Contract,
        _context: &ValidationContext,
    ) -> contracts_core::ValidationResult {
        // This implementation requires actual data, so we return Ok by default
        // Real validation happens via validate_with_data
        Ok(())
    }

    fn validate_schema(
        &self,
        contract: &Contract,
        _context: &ValidationContext,
    ) -> contracts_core::ValidationResult {
        let errors = self.schema_validator.validate_schema_definition(contract);
        if errors.is_empty() {
            Ok(())
        } else {
            Err(contracts_core::ContractError::SchemaValidation(
                errors
                    .into_iter()
                    .map(|e| e.to_string())
                    .collect::<Vec<_>>()
                    .join("; "),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataValue;
    use contracts_core::{
        CompletenessCheck, ContractBuilder, DataFormat, FieldBuilder, FieldConstraints,
        QualityChecks,
    };
    use std::collections::HashMap;

    #[test]
    fn test_empty_dataset() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .build();

        let dataset = DataSet::empty();
        let context = ValidationContext::new();
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(report.passed);
    }

    #[test]
    fn test_valid_data() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .field(
                FieldBuilder::new("age", "int64")
                    .nullable(false)
                    .constraint(FieldConstraints::Range {
                        min: 0.0,
                        max: 150.0,
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("123".to_string()));
        row.insert("age".to_string(), DataValue::Int(25));

        let dataset = DataSet::from_rows(vec![row]);
        let context = ValidationContext::new();
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(
            report.passed,
            "Expected pass, got errors: {:?}",
            report.errors
        );
        assert_eq!(report.stats.records_validated, 1);
    }

    #[test]
    fn test_schema_error() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .build();

        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::Null); // Null in non-nullable field

        let dataset = DataSet::from_rows(vec![row]);
        let context = ValidationContext::new();
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(!report.passed);
        assert_eq!(report.errors.len(), 1);
    }

    #[test]
    fn test_constraint_error() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("age", "int64")
                    .nullable(false)
                    .constraint(FieldConstraints::Range {
                        min: 0.0,
                        max: 120.0,
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert("age".to_string(), DataValue::Int(200)); // Out of range

        let dataset = DataSet::from_rows(vec![row]);
        let context = ValidationContext::new();
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(!report.passed);
        assert_eq!(report.errors.len(), 1);
    }

    #[test]
    fn test_quality_check_warning() {
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
                row.insert("id".to_string(), DataValue::String(i.to_string()));
            } else {
                row.insert("id".to_string(), DataValue::Null);
            }
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let context = ValidationContext::new(); // Non-strict mode
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(report.passed); // Passes because quality checks are warnings in non-strict mode
        assert_eq!(report.warnings.len(), 1);
    }

    #[test]
    fn test_strict_mode() {
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
                row.insert("id".to_string(), DataValue::String(i.to_string()));
            } else {
                row.insert("id".to_string(), DataValue::Null);
            }
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let context = ValidationContext::new().with_strict(true);
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(!report.passed); // Fails in strict mode
        assert_eq!(report.errors.len(), 1);
    }

    #[test]
    fn test_schema_only_mode() {
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

        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("123".to_string()));

        let dataset = DataSet::from_rows(vec![row]);
        let context = ValidationContext::new().with_schema_only(true);
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(report.passed);
        assert_eq!(report.warnings.len(), 0); // No quality checks run
    }

    #[test]
    fn test_sample_size() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .build();

        let mut rows = Vec::new();
        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), DataValue::String(i.to_string()));
            rows.push(row);
        }

        let dataset = DataSet::from_rows(rows);
        let context = ValidationContext::new().with_sample_size(10);
        let mut validator = DataValidator::new();

        let report = validator.validate_with_data(&contract, &dataset, &context);
        assert!(report.passed);
        assert_eq!(report.stats.records_validated, 10); // Only 10 sampled
    }

    #[test]
    fn test_validate_definition() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .build();

        let validator = DataValidator::new();
        let report = validator.validate_definition(&contract);
        assert!(report.passed);
    }
}
