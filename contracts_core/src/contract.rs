//! Data contract types and structures.
//!
//! This module contains the core types for defining data contracts, including
//! schemas, quality checks, and service level agreements.

use serde::{Deserialize, Serialize};

/// A data contract defining the structure, quality, and SLA for a dataset.
///
/// A `Contract` is the main entry point for defining a data contract. It contains
/// all the metadata, schema definition, quality checks, and service level agreements
/// for a dataset.
///
/// # Example
///
/// ```rust
/// use contracts_core::{Contract, Schema, DataFormat};
///
/// let contract = Contract {
///     version: "1.0.0".to_string(),
///     name: "user_events".to_string(),
///     owner: "analytics-team".to_string(),
///     description: Some("User interaction events dataset".to_string()),
///     schema: Schema {
///         fields: vec![],
///         format: DataFormat::Iceberg,
///         location: "s3://data/user_events".to_string(),
///     },
///     quality_checks: None,
///     sla: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contract {
    /// Semantic version of the contract (e.g., "1.0.0")
    pub version: String,

    /// Unique name identifying this contract
    pub name: String,

    /// Team or individual responsible for this contract
    pub owner: String,

    /// Human-readable description of the dataset
    pub description: Option<String>,

    /// Schema definition including fields and format
    pub schema: Schema,

    /// Optional quality validation rules
    pub quality_checks: Option<QualityChecks>,

    /// Optional service level agreement
    pub sla: Option<SLA>,
}

/// Supported data format types for the dataset.
///
/// Defines the physical storage format and table format for the data.
/// The engine can validate contracts against different formats.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    /// Apache Iceberg table format
    Iceberg,
    /// Apache Parquet columnar format
    Parquet,
    /// JSON format
    Json,
    /// CSV format
    Csv,
    /// Apache Avro format
    Avro,
    /// Apache ORC format
    Orc,
    /// Delta Lake table format
    Delta,
    /// Apache Hudi table format
    Hudi,
    /// Custom format with identifier
    Custom(String),
}

/// Schema definition for a dataset.
///
/// Describes the structure of the data including field definitions,
/// storage format, and physical location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    /// List of field definitions in the schema
    pub fields: Vec<Field>,

    /// Data format type
    pub format: DataFormat,

    /// Physical location of the data (e.g., S3 path, database URI)
    pub location: String,
}

/// A single field definition in a schema.
///
/// Represents a column or field in the dataset with its type,
/// nullability, and optional constraints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    /// Field name
    pub name: String,

    /// Field data type (e.g., "string", "int64", "timestamp")
    #[serde(rename = "type")]
    pub field_type: String,

    /// Whether the field can contain null values
    pub nullable: bool,

    /// Optional human-readable description
    pub description: Option<String>,

    /// Optional tags for categorization or metadata
    pub tags: Option<Vec<String>>,

    /// Optional validation constraints
    pub constraints: Option<Vec<FieldConstraints>>,
}

/// Validation constraints that can be applied to a field.
///
/// Defines rules that field values must satisfy for the data to be valid.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FieldConstraints {
    /// Field value must be one of the allowed values
    AllowedValues {
        /// List of valid values
        values: Vec<String>,
    },

    /// Numeric field must be within the specified range
    Range {
        /// Minimum value (inclusive)
        min: f64,
        /// Maximum value (inclusive)
        max: f64,
    },

    /// Field value must match the regex pattern
    Pattern {
        /// Regular expression pattern
        regex: String,
    },

    /// Custom constraint with arbitrary definition
    Custom {
        /// Custom constraint definition
        definition: String,
    },
}

/// Quality check definitions for data validation.
///
/// Specifies rules for data quality including completeness, uniqueness,
/// freshness, and custom validation checks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityChecks {
    /// Check for null/missing values
    pub completeness: Option<CompletenessCheck>,

    /// Check for duplicate values
    pub uniqueness: Option<UniquenessCheck>,

    /// Check for data staleness
    pub freshness: Option<FreshnessCheck>,

    /// User-defined validation checks
    pub custom_checks: Option<Vec<CustomCheck>>,
}

/// Freshness check to ensure data is up-to-date.
///
/// Validates that data is not stale by checking the time
/// since the last update against a maximum allowed delay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessCheck {
    /// Maximum allowed delay (e.g., "1h", "30m", "1d")
    pub max_delay: String,

    /// Metric to measure freshness (e.g., "created_at", "updated_at")
    pub metric: String,
}

/// Completeness check for null/missing values.
///
/// Ensures that specified fields have values in at least
/// a certain percentage of records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletenessCheck {
    /// Minimum percentage of non-null values (0.0 to 1.0)
    pub threshold: f64,

    /// List of fields to check
    pub fields: Vec<String>,
}

/// Uniqueness check for duplicate detection.
///
/// Validates that combinations of specified fields are unique
/// within a defined scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniquenessCheck {
    /// Fields that should be unique together
    pub fields: Vec<String>,

    /// Optional scope for uniqueness (e.g., "per_day", "global")
    pub scope: Option<String>,
}

/// Custom validation check with user-defined logic.
///
/// Allows arbitrary validation rules to be specified
/// using a custom definition language or SQL expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomCheck {
    /// Name of the custom check
    pub name: String,

    /// Check definition (e.g., SQL expression, validation rule)
    pub definition: String,

    /// Severity level (e.g., "error", "warning", "info")
    pub severity: Option<String>,
}

/// Service Level Agreement for data availability and performance.
///
/// Defines guarantees about data availability, query response times,
/// and consequences for SLA violations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SLA {
    /// Guaranteed availability percentage (0.0 to 1.0)
    pub availability: Option<f64>,

    /// Maximum response time for queries (e.g., "100ms", "1s")
    pub response_time: Option<String>,

    /// Description of penalties for SLA violations
    pub penalties: Option<String>,
}
