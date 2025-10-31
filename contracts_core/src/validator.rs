//! Validation traits and types for data contracts.
//!
//! This module defines the core traits for implementing contract validators.
//! Different implementations can validate contracts against various data formats
//! (Iceberg, Delta Lake, etc.).

use crate::{Contract, ContractError};

/// Result type for validation operations.
pub type ValidationResult<T = ()> = std::result::Result<T, ContractError>;

/// Core trait for validating data contracts.
///
/// Implementations of this trait can validate contracts against different
/// data sources and formats. Each validator is responsible for checking
/// schema compatibility, quality rules, and SLA compliance.
///
/// # Example
///
/// ```rust
/// use contracts_core::{Contract, ContractValidator, ValidationResult, ValidationContext};
///
/// struct MyValidator;
///
/// impl ContractValidator for MyValidator {
///     fn validate(&self, contract: &Contract, context: &ValidationContext) -> ValidationResult {
///         // Validation logic here
///         Ok(())
///     }
/// }
/// ```
pub trait ContractValidator: Send + Sync {
    /// Validates a contract against the data source.
    ///
    /// # Arguments
    ///
    /// * `contract` - The contract to validate
    /// * `context` - Additional context for validation
    ///
    /// # Returns
    ///
    /// `Ok(())` if validation succeeds, or a `ContractError` if validation fails.
    fn validate(&self, contract: &Contract, context: &ValidationContext) -> ValidationResult;

    /// Validates only the schema portion of the contract.
    ///
    /// Default implementation delegates to `validate()`.
    fn validate_schema(
        &self,
        contract: &Contract,
        context: &ValidationContext,
    ) -> ValidationResult {
        self.validate(contract, context)
    }

    /// Validates quality checks defined in the contract.
    ///
    /// Default implementation returns Ok if no quality checks are defined.
    fn validate_quality(
        &self,
        contract: &Contract,
        context: &ValidationContext,
    ) -> ValidationResult {
        if contract.quality_checks.is_some() {
            self.validate(contract, context)
        } else {
            Ok(())
        }
    }

    /// Validates SLA compliance.
    ///
    /// Default implementation returns Ok if no SLA is defined.
    fn validate_sla(&self, contract: &Contract, context: &ValidationContext) -> ValidationResult {
        if contract.sla.is_some() {
            self.validate(contract, context)
        } else {
            Ok(())
        }
    }
}

/// Context for validation operations.
///
/// Provides additional information needed during validation,
/// such as environment settings, credentials, and validation options.
#[derive(Debug, Default, Clone)]
pub struct ValidationContext {
    /// Whether to perform strict validation
    pub strict: bool,

    /// Whether to validate schema only (skip data validation)
    pub schema_only: bool,

    /// Maximum number of records to sample for quality checks
    pub sample_size: Option<usize>,

    /// Additional metadata for the validation
    pub metadata: std::collections::HashMap<String, String>,
}

impl ValidationContext {
    /// Creates a new validation context with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets strict validation mode.
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Sets schema-only validation mode.
    pub fn with_schema_only(mut self, schema_only: bool) -> Self {
        self.schema_only = schema_only;
        self
    }

    /// Sets the sample size for quality checks.
    pub fn with_sample_size(mut self, size: usize) -> Self {
        self.sample_size = Some(size);
        self
    }

    /// Adds metadata to the context.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Report of validation results.
///
/// Contains detailed information about validation outcomes,
/// including errors, warnings, and statistics.
#[derive(Debug, Clone)]
pub struct ValidationReport {
    /// Whether validation passed overall
    pub passed: bool,

    /// List of errors encountered
    pub errors: Vec<String>,

    /// List of warnings
    pub warnings: Vec<String>,

    /// Validation statistics
    pub stats: ValidationStats,
}

/// Statistics about validation execution.
#[derive(Debug, Clone, Default)]
pub struct ValidationStats {
    /// Number of records validated
    pub records_validated: usize,

    /// Number of fields checked
    pub fields_checked: usize,

    /// Number of constraints evaluated
    pub constraints_evaluated: usize,

    /// Validation duration in milliseconds
    pub duration_ms: u64,
}

impl ValidationReport {
    /// Creates a new successful validation report.
    pub fn success() -> Self {
        Self {
            passed: true,
            errors: Vec::new(),
            warnings: Vec::new(),
            stats: ValidationStats::default(),
        }
    }

    /// Creates a new failed validation report with an error.
    pub fn failure(error: impl Into<String>) -> Self {
        Self {
            passed: false,
            errors: vec![error.into()],
            warnings: Vec::new(),
            stats: ValidationStats::default(),
        }
    }

    /// Adds an error to the report.
    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
        self.passed = false;
    }

    /// Adds a warning to the report.
    pub fn add_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }
}
