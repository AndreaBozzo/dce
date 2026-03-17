//! Error types for validation operations.

use thiserror::Error;

/// Errors that can occur during validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    /// Schema validation error
    #[error("Schema validation failed: {0}")]
    SchemaError(String),

    /// Field type mismatch
    #[error("Type mismatch for field '{field}': expected {expected}, found {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },

    /// Required field is missing
    #[error("Required field '{0}' is missing")]
    MissingField(String),

    /// Field should not be null
    #[error("Field '{field}' is null but nullability is not allowed (row {row:?})")]
    NullConstraintViolation { field: String, row: Option<usize> },

    /// Constraint violation
    #[error("Constraint violation for field '{field}': {message}")]
    ConstraintViolation { field: String, message: String },

    /// Quality check failed
    #[error("Quality check failed: {0}")]
    QualityCheckFailed(String),

    /// Custom check failed
    #[error("Custom check '{name}' failed: {message}")]
    CustomCheckFailed { name: String, message: String },

    /// Invalid regex pattern
    #[error("Invalid regex pattern for field '{field}': {error}")]
    InvalidRegex { field: String, error: String },

    /// Freshness check failed
    #[error("Freshness check failed: data is stale by {delay}")]
    StaleData { delay: String },

    /// Invalid time duration format
    #[error("Invalid time duration format: {0}")]
    InvalidDuration(String),

    /// Generic validation error
    #[error("Validation error: {0}")]
    General(String),
}

impl ValidationError {
    /// Creates a new schema error.
    pub fn schema(message: impl Into<String>) -> Self {
        Self::SchemaError(message.into())
    }

    /// Creates a new type mismatch error.
    pub fn type_mismatch(
        field: impl Into<String>,
        expected: impl Into<String>,
        actual: impl Into<String>,
    ) -> Self {
        Self::TypeMismatch {
            field: field.into(),
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Creates a new missing field error.
    pub fn missing_field(field: impl Into<String>) -> Self {
        Self::MissingField(field.into())
    }

    /// Creates a new null constraint violation error.
    pub fn null_violation(field: impl Into<String>, row: Option<usize>) -> Self {
        Self::NullConstraintViolation {
            field: field.into(),
            row,
        }
    }

    /// Creates a new constraint violation error.
    pub fn constraint(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ConstraintViolation {
            field: field.into(),
            message: message.into(),
        }
    }

    /// Creates a new quality check error.
    pub fn quality_check(message: impl Into<String>) -> Self {
        Self::QualityCheckFailed(message.into())
    }

    /// Creates a new custom check error.
    pub fn custom_check(name: impl Into<String>, message: impl Into<String>) -> Self {
        Self::CustomCheckFailed {
            name: name.into(),
            message: message.into(),
        }
    }
}
