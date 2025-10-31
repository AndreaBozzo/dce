//! Error types for data contracts.
//!
//! This module defines all error types that can occur when working with
//! data contracts, including validation errors, schema mismatches, and
//! constraint violations.

use thiserror::Error;

/// Result type for data contract operations.
pub type Result<T> = std::result::Result<T, ContractError>;

/// Main error type for data contract operations.
#[derive(Error, Debug)]
pub enum ContractError {
    /// Schema validation failed
    #[error("Schema validation error: {0}")]
    SchemaValidation(String),

    /// Field constraint violation
    #[error("Constraint violation in field '{field}': {message}")]
    ConstraintViolation {
        /// Field name where constraint was violated
        field: String,
        /// Description of the violation
        message: String,
    },

    /// Quality check failed
    #[error("Quality check '{check}' failed: {message}")]
    QualityCheckFailed {
        /// Name of the quality check
        check: String,
        /// Failure details
        message: String,
    },

    /// SLA violation
    #[error("SLA violation: {0}")]
    SlaViolation(String),

    /// Contract version incompatibility
    #[error("Incompatible contract version: expected {expected}, got {actual}")]
    VersionMismatch {
        /// Expected version
        expected: String,
        /// Actual version
        actual: String,
    },

    /// Missing required field
    #[error("Missing required field: {0}")]
    MissingField(String),

    /// Invalid field type
    #[error("Invalid type for field '{field}': expected {expected}, got {actual}")]
    InvalidFieldType {
        /// Field name
        field: String,
        /// Expected type
        expected: String,
        /// Actual type
        actual: String,
    },

    /// Format not supported
    #[error("Unsupported data format: {0}")]
    UnsupportedFormat(String),

    /// Serialization/deserialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Generic error
    #[error("{0}")]
    Other(String),
}

/// Error type for validation operations.
#[derive(Error, Debug)]
pub enum ValidationError {
    /// Field value is null but field is not nullable
    #[error("Field '{0}' cannot be null")]
    NullConstraint(String),

    /// Value not in allowed values list
    #[error("Value '{value}' not in allowed values for field '{field}'")]
    NotInAllowedValues {
        /// Field name
        field: String,
        /// Invalid value
        value: String,
    },

    /// Value outside allowed range
    #[error("Value {value} outside range [{min}, {max}] for field '{field}'")]
    OutOfRange {
        /// Field name
        field: String,
        /// Invalid value
        value: f64,
        /// Minimum allowed
        min: f64,
        /// Maximum allowed
        max: f64,
    },

    /// Value doesn't match pattern
    #[error("Value for field '{field}' doesn't match pattern '{pattern}'")]
    PatternMismatch {
        /// Field name
        field: String,
        /// Expected pattern
        pattern: String,
    },

    /// Custom validation failed
    #[error("Custom validation '{name}' failed: {message}")]
    CustomValidation {
        /// Validation name
        name: String,
        /// Failure message
        message: String,
    },
}
