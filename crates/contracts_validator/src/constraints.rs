//! Constraint validation logic.
//!
//! This module handles validation of field constraints including:
//! - AllowedValues: Field must be one of a predefined set
//! - Range: Numeric field must be within min/max bounds
//! - Pattern: String field must match a regex pattern
//! - Custom: User-defined constraint expressions

use crate::{DataRow, DataSet, DataValue, ValidationError};
use contracts_core::{Contract, Field, FieldConstraints};
use regex::Regex;
use std::collections::HashMap;

/// Validates field constraints in a dataset.
pub struct ConstraintValidator {
    /// Cache of compiled regex patterns
    regex_cache: HashMap<String, Regex>,
}

impl ConstraintValidator {
    /// Creates a new constraint validator.
    pub fn new() -> Self {
        Self {
            regex_cache: HashMap::new(),
        }
    }

    /// Validates all constraints in a dataset against a contract.
    ///
    /// Returns a list of validation errors. An empty list indicates success.
    pub fn validate(&mut self, contract: &Contract, dataset: &DataSet) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if dataset.is_empty() {
            return errors;
        }

        // Validate each row
        for (row_idx, row) in dataset.rows().enumerate() {
            errors.extend(self.validate_row(contract, row, row_idx));
        }

        errors
    }

    /// Validates constraints in a single row.
    fn validate_row(
        &mut self,
        contract: &Contract,
        row: &DataRow,
        row_idx: usize,
    ) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        for field in &contract.schema.fields {
            if let Some(constraints) = &field.constraints {
                for constraint in constraints {
                    if let Some(err) = self.validate_constraint(field, constraint, row, row_idx) {
                        errors.push(err);
                    }
                }
            }
        }

        errors
    }

    /// Validates a single constraint on a field.
    fn validate_constraint(
        &mut self,
        field: &Field,
        constraint: &FieldConstraints,
        row: &DataRow,
        row_idx: usize,
    ) -> Option<ValidationError> {
        let value = row.get(&field.name)?;

        // Skip validation for null values (nullability is handled by schema validator)
        if value.is_null() {
            return None;
        }

        match constraint {
            FieldConstraints::AllowedValues { values } => {
                self.validate_allowed_values(field, value, values, row_idx)
            }
            FieldConstraints::Range { min, max } => {
                self.validate_range(field, value, *min, *max, row_idx)
            }
            FieldConstraints::Pattern { regex } => {
                self.validate_pattern(field, value, regex, row_idx)
            }
            FieldConstraints::Custom { definition } => {
                self.validate_custom(field, value, definition, row_idx)
            }
        }
    }

    /// Validates that a value is in the allowed set.
    fn validate_allowed_values(
        &self,
        field: &Field,
        value: &DataValue,
        allowed: &[String],
        _row_idx: usize,
    ) -> Option<ValidationError> {
        let str_value = match value {
            DataValue::String(s) => s.as_str(),
            DataValue::Int(i) => return self.check_int_in_allowed(*i, allowed, field),
            DataValue::Float(f) => return self.check_float_in_allowed(*f, allowed, field),
            DataValue::Bool(b) => {
                let b_str = b.to_string();
                return self.check_string_in_allowed(&b_str, allowed, field);
            }
            _ => {
                return Some(ValidationError::constraint(
                    &field.name,
                    format!(
                        "AllowedValues constraint not applicable to type {}",
                        value.type_name()
                    ),
                ));
            }
        };

        self.check_string_in_allowed(str_value, allowed, field)
    }

    fn check_string_in_allowed(
        &self,
        value: &str,
        allowed: &[String],
        field: &Field,
    ) -> Option<ValidationError> {
        if !allowed.iter().any(|a| a == value) {
            return Some(ValidationError::constraint(
                &field.name,
                format!(
                    "Value '{}' not in allowed values: [{}]",
                    value,
                    allowed.join(", ")
                ),
            ));
        }
        None
    }

    fn check_int_in_allowed(
        &self,
        value: i64,
        allowed: &[String],
        field: &Field,
    ) -> Option<ValidationError> {
        let value_str = value.to_string();
        if !allowed.contains(&value_str) {
            return Some(ValidationError::constraint(
                &field.name,
                format!(
                    "Value {} not in allowed values: [{}]",
                    value,
                    allowed.join(", ")
                ),
            ));
        }
        None
    }

    fn check_float_in_allowed(
        &self,
        value: f64,
        allowed: &[String],
        field: &Field,
    ) -> Option<ValidationError> {
        let value_str = value.to_string();
        if !allowed.contains(&value_str) {
            return Some(ValidationError::constraint(
                &field.name,
                format!(
                    "Value {} not in allowed values: [{}]",
                    value,
                    allowed.join(", ")
                ),
            ));
        }
        None
    }

    /// Validates that a numeric value is within a range.
    fn validate_range(
        &self,
        field: &Field,
        value: &DataValue,
        min: f64,
        max: f64,
        _row_idx: usize,
    ) -> Option<ValidationError> {
        let num_value = match value.as_float() {
            Some(n) => n,
            None => {
                return Some(ValidationError::constraint(
                    &field.name,
                    format!(
                        "Range constraint requires numeric type, found {}",
                        value.type_name()
                    ),
                ));
            }
        };

        if num_value < min || num_value > max {
            return Some(ValidationError::constraint(
                &field.name,
                format!("Value {} out of range [{}, {}]", num_value, min, max),
            ));
        }

        None
    }

    /// Validates that a string value matches a regex pattern.
    fn validate_pattern(
        &mut self,
        field: &Field,
        value: &DataValue,
        pattern: &str,
        _row_idx: usize,
    ) -> Option<ValidationError> {
        let str_value = match value.as_string() {
            Some(s) => s,
            None => {
                return Some(ValidationError::constraint(
                    &field.name,
                    format!(
                        "Pattern constraint requires string type, found {}",
                        value.type_name()
                    ),
                ));
            }
        };

        // Get or compile regex
        let regex = match self.get_or_compile_regex(pattern) {
            Ok(r) => r,
            Err(e) => {
                return Some(ValidationError::InvalidRegex {
                    field: field.name.clone(),
                    error: e,
                });
            }
        };

        if !regex.is_match(str_value) {
            return Some(ValidationError::constraint(
                &field.name,
                format!("Value '{}' does not match pattern '{}'", str_value, pattern),
            ));
        }

        None
    }

    /// Validates a custom constraint (currently just syntax validation).
    fn validate_custom(
        &self,
        _field: &Field,
        _value: &DataValue,
        _definition: &str,
        _row_idx: usize,
    ) -> Option<ValidationError> {
        // Custom constraints are not executed at this level
        // They are validated at the quality check level
        None
    }

    /// Gets a compiled regex from cache or compiles and caches it.
    fn get_or_compile_regex(&mut self, pattern: &str) -> Result<&Regex, String> {
        if !self.regex_cache.contains_key(pattern) {
            let regex = Regex::new(pattern).map_err(|e| e.to_string())?;
            self.regex_cache.insert(pattern.to_string(), regex);
        }
        Ok(self.regex_cache.get(pattern).unwrap())
    }
}

impl Default for ConstraintValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_core::{ContractBuilder, DataFormat, FieldBuilder};

    #[test]
    fn test_allowed_values_valid() {
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

        let mut row = HashMap::new();
        row.insert(
            "status".to_string(),
            DataValue::String("active".to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_allowed_values_invalid() {
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

        let mut row = HashMap::new();
        row.insert(
            "status".to_string(),
            DataValue::String("pending".to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ValidationError::ConstraintViolation { .. }
        ));
    }

    #[test]
    fn test_range_valid() {
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
        row.insert("age".to_string(), DataValue::Int(25));

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_range_invalid() {
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
        row.insert("age".to_string(), DataValue::Int(150));

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ValidationError::ConstraintViolation { .. }
        ));
    }

    #[test]
    fn test_pattern_valid() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("url", "string")
                    .nullable(false)
                    .constraint(FieldConstraints::Pattern {
                        regex: r"^https?://.*".to_string(),
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert(
            "url".to_string(),
            DataValue::String("https://example.com".to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_pattern_invalid() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("url", "string")
                    .nullable(false)
                    .constraint(FieldConstraints::Pattern {
                        regex: r"^https?://.*".to_string(),
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert(
            "url".to_string(),
            DataValue::String("not-a-url".to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ValidationError::ConstraintViolation { .. }
        ));
    }

    #[test]
    fn test_invalid_regex() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("test", "string")
                    .nullable(false)
                    .constraint(FieldConstraints::Pattern {
                        regex: "[invalid(regex".to_string(),
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert("test".to_string(), DataValue::String("test".to_string()));

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], ValidationError::InvalidRegex { .. }));
    }

    #[test]
    fn test_multiple_constraints() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("status", "string")
                    .nullable(false)
                    .constraint(FieldConstraints::AllowedValues {
                        values: vec!["active".to_string(), "inactive".to_string()],
                    })
                    .constraint(FieldConstraints::Pattern {
                        regex: r"^[a-z]+$".to_string(),
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert(
            "status".to_string(),
            DataValue::String("active".to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_null_values_skipped() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("status", "string")
                    .nullable(true)
                    .constraint(FieldConstraints::AllowedValues {
                        values: vec!["active".to_string()],
                    })
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert("status".to_string(), DataValue::Null);

        let dataset = DataSet::from_rows(vec![row]);
        let mut validator = ConstraintValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0); // Null values skip constraint checks
    }
}
