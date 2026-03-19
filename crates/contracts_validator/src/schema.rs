//! Schema validation logic.
//!
//! This module handles validation of data schemas against contract definitions,
//! including field presence, type checking, and nullability constraints.

use crate::{DataRow, DataSet, DataValue, ValidationError};
use contracts_core::{Contract, DataType, Field, PrimitiveType};
use std::collections::HashSet;

/// Validates the schema of a dataset against a contract.
///
/// Checks that all required fields are present, types match, and nullability
/// constraints are satisfied.
pub struct SchemaValidator;

impl SchemaValidator {
    /// Creates a new schema validator.
    pub fn new() -> Self {
        Self
    }

    /// Validates a dataset against the contract schema.
    ///
    /// Returns a list of validation errors. An empty list indicates success.
    pub fn validate(&self, contract: &Contract, dataset: &DataSet) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        // If dataset is empty, only validate schema definition itself
        if dataset.is_empty() {
            return errors;
        }

        // Validate each row
        for (row_idx, row) in dataset.rows().enumerate() {
            errors.extend(self.validate_row(contract, row, row_idx));
        }

        errors
    }

    /// Validates a single row against the schema.
    fn validate_row(
        &self,
        contract: &Contract,
        row: &DataRow,
        row_idx: usize,
    ) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        // Check required fields
        for field in &contract.schema.fields {
            if let Some(err) = self.validate_field(field, row, row_idx) {
                errors.push(err);
            }
        }

        // Check for extra fields in strict mode (optional feature for future)
        // For now, we allow extra fields

        errors
    }

    /// Validates a single field in a row.
    fn validate_field(
        &self,
        field: &Field,
        row: &DataRow,
        row_idx: usize,
    ) -> Option<ValidationError> {
        let value = row.get(&field.name);

        // Check field presence
        let value = match value {
            Some(v) => v,
            None => {
                // Field is missing
                if !field.nullable {
                    return Some(ValidationError::missing_field(&field.name));
                }
                return None; // Missing nullable field is OK
            }
        };

        // Check nullability
        if value.is_null() && !field.nullable {
            return Some(ValidationError::null_violation(&field.name, Some(row_idx)));
        }

        // Check type (skip for null values)
        if !value.is_null()
            && let Some(err) = self.validate_type(field, value, row_idx)
        {
            return Some(err);
        }

        None
    }

    /// Validates the type of a field value, including recursive element validation.
    fn validate_type(
        &self,
        field: &Field,
        value: &DataValue,
        _row_idx: usize,
    ) -> Option<ValidationError> {
        if !Self::type_matches(&field.field_type, value) {
            return Some(ValidationError::type_mismatch(
                &field.name,
                field.field_type.to_string(),
                value.type_name(),
            ));
        }
        None
    }

    /// Recursively checks whether a value matches an expected DataType.
    fn type_matches(expected: &DataType, value: &DataValue) -> bool {
        match expected {
            DataType::Primitive(p) => match p {
                PrimitiveType::String => matches!(value, DataValue::String(_)),
                PrimitiveType::Int32 | PrimitiveType::Int64 => matches!(value, DataValue::Int(_)),
                PrimitiveType::Float32 | PrimitiveType::Float64 => {
                    matches!(value, DataValue::Float(_) | DataValue::Int(_))
                }
                PrimitiveType::Boolean => matches!(value, DataValue::Bool(_)),
                PrimitiveType::Timestamp => matches!(value, DataValue::Timestamp(_)),
                // Lenient for date, time, decimal, uuid, binary — accept any value
                _ => true,
            },
            DataType::List {
                element_type,
                contains_null,
            } => {
                if let DataValue::List(items) = value {
                    items.iter().all(|item| {
                        if item.is_null() {
                            *contains_null
                        } else {
                            Self::type_matches(element_type, item)
                        }
                    })
                } else {
                    false
                }
            }
            DataType::Map {
                value_type,
                value_contains_null,
                ..
            } => {
                if let DataValue::Map(entries) = value {
                    entries.values().all(|v| {
                        if v.is_null() {
                            *value_contains_null
                        } else {
                            Self::type_matches(value_type, v)
                        }
                    })
                } else {
                    false
                }
            }
            DataType::Struct { fields } => {
                if let DataValue::Map(entries) = value {
                    fields.iter().all(|sf| {
                        match entries.get(&sf.name) {
                            Some(v) if v.is_null() => sf.nullable,
                            Some(v) => Self::type_matches(&sf.data_type, v),
                            // Missing fields are OK if nullable
                            None => sf.nullable,
                        }
                    })
                } else {
                    false
                }
            }
        }
    }

    /// Validates that all required fields are present in the schema.
    pub fn validate_schema_definition(&self, contract: &Contract) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if contract.schema.fields.is_empty() {
            errors.push(ValidationError::schema("Schema has no fields defined"));
        }

        // Check for duplicate field names
        let mut seen = HashSet::new();
        for field in &contract.schema.fields {
            if !seen.insert(&field.name) {
                errors.push(ValidationError::schema(format!(
                    "Duplicate field name: {}",
                    field.name
                )));
            }
        }

        errors
    }
}

impl Default for SchemaValidator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_core::{ContractBuilder, FieldBuilder};
    use std::collections::HashMap;

    fn create_test_contract() -> Contract {
        ContractBuilder::new("test_contract", "test-owner")
            .location("s3://test/data")
            .format(contracts_core::DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .field(FieldBuilder::new("age", "int64").nullable(false).build())
            .field(FieldBuilder::new("email", "string").nullable(true).build())
            .build()
    }

    #[test]
    fn test_empty_dataset() {
        let contract = create_test_contract();
        let dataset = DataSet::empty();
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_valid_row() {
        let contract = create_test_contract();
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("123".to_string()));
        row.insert("age".to_string(), DataValue::Int(25));
        row.insert(
            "email".to_string(),
            DataValue::String("test@example.com".to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0, "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_missing_required_field() {
        let contract = create_test_contract();
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("123".to_string()));
        // Missing 'age' field which is required

        let dataset = DataSet::from_rows(vec![row]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], ValidationError::MissingField(_)));
    }

    #[test]
    fn test_null_in_non_nullable_field() {
        let contract = create_test_contract();
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::Null);
        row.insert("age".to_string(), DataValue::Int(25));

        let dataset = DataSet::from_rows(vec![row]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ValidationError::NullConstraintViolation { .. }
        ));
    }

    #[test]
    fn test_null_in_nullable_field() {
        let contract = create_test_contract();
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("123".to_string()));
        row.insert("age".to_string(), DataValue::Int(25));
        row.insert("email".to_string(), DataValue::Null); // nullable field

        let dataset = DataSet::from_rows(vec![row]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_type_mismatch() {
        let contract = create_test_contract();
        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("123".to_string()));
        row.insert(
            "age".to_string(),
            DataValue::String("not a number".to_string()),
        ); // Wrong type

        let dataset = DataSet::from_rows(vec![row]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], ValidationError::TypeMismatch { .. }));
    }

    #[test]
    fn test_multiple_rows() {
        let contract = create_test_contract();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), DataValue::String("1".to_string()));
        row1.insert("age".to_string(), DataValue::Int(25));

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), DataValue::String("2".to_string()));
        // Missing age field

        let dataset = DataSet::from_rows(vec![row1, row2]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1); // Only row2 has error
    }

    #[test]
    fn test_validate_schema_definition() {
        let contract = create_test_contract();
        let validator = SchemaValidator::new();

        let errors = validator.validate_schema_definition(&contract);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_empty_schema_definition() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(contracts_core::DataFormat::Iceberg)
            .build();
        let validator = SchemaValidator::new();

        let errors = validator.validate_schema_definition(&contract);
        assert_eq!(errors.len(), 1);
    }

    #[test]
    fn test_int_to_float_coercion() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(contracts_core::DataFormat::Iceberg)
            .field(
                FieldBuilder::new("value", "float64")
                    .nullable(false)
                    .build(),
            )
            .build();

        let mut row = HashMap::new();
        row.insert("value".to_string(), DataValue::Int(42)); // Int can coerce to float

        let dataset = DataSet::from_rows(vec![row]);
        let validator = SchemaValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0); // Should accept int for float field
    }
}
