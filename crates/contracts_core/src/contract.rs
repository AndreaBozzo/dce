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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

    /// ML-specific quality checks
    pub ml_checks: Option<MlChecks>,
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

/// ML-specific quality checks for machine learning datasets.
///
/// These checks ensure that datasets used for ML training and evaluation
/// follow best practices around data splitting, class balance, and
/// feature-target separation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlChecks {
    /// Ensures train/test/validation splits have no overlapping rows
    pub no_overlap: Option<NoOverlapCheck>,

    /// Validates temporal ordering in train/test splits
    pub temporal_split: Option<TemporalSplitCheck>,

    /// Validates class label distribution is not overly skewed
    pub class_balance: Option<ClassBalanceCheck>,

    /// Detects feature distribution drift between splits using PSI
    pub feature_drift: Option<FeatureDriftCheck>,

    /// Detects features with suspiciously high correlation to the target
    pub target_leakage: Option<TargetLeakageCheck>,

    /// Detects disparate null rates across groups/splits
    pub null_rate_by_group: Option<NullRateByGroupCheck>,
}

/// Ensures that the specified split field produces non-overlapping groups.
///
/// For ML pipelines, it is critical that the train, validation, and test sets
/// share no rows. This check validates uniqueness of a key field across splits.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoOverlapCheck {
    /// The field that denotes the split (e.g., "split" with values "train"/"test"/"val")
    pub split_field: String,

    /// The key field(s) that must not overlap across splits (e.g., "user_id")
    pub key_fields: Vec<String>,
}

/// Validates temporal ordering between splits.
///
/// For time-series ML, training data must precede test data chronologically.
/// This check ensures max(timestamp) in "train" <= min(timestamp) in "test".
///
/// When `split_order` is provided, validates all adjacent pairs in order
/// (e.g., `["train", "val", "test"]` checks train <= val and val <= test).
/// Otherwise falls back to the two-field `train_split`/`test_split` behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalSplitCheck {
    /// The field that denotes the split (e.g., "split")
    pub split_field: String,

    /// The timestamp field to check ordering on
    pub timestamp_field: String,

    /// The split value representing training data (default: "train")
    pub train_split: String,

    /// The split value representing test data (default: "test")
    pub test_split: String,

    /// Ordered list of split names for N-way temporal validation.
    /// When present, overrides `train_split`/`test_split`.
    pub split_order: Option<Vec<String>>,
}

/// Validates that class labels are reasonably balanced.
///
/// Extremely imbalanced datasets can silently degrade model quality.
/// This check ensures no single class exceeds a maximum proportion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassBalanceCheck {
    /// The label/target field to check
    pub label_field: String,

    /// Maximum allowed proportion for any single class (0.0 to 1.0)
    /// e.g., 0.95 means no class can be >95% of the data
    pub max_proportion: f64,

    /// Minimum allowed proportion for any single class (0.0 to 1.0)
    /// e.g., 0.01 means every class must be >=1% of the data
    pub min_proportion: Option<f64>,
}

/// Detects feature distribution drift between a reference and current split
/// using Population Stability Index (PSI).
///
/// PSI > 0.1 suggests moderate drift; > 0.2 suggests significant drift.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureDriftCheck {
    /// The field that denotes the split (e.g., "split")
    pub split_field: String,

    /// The split value used as the reference distribution (e.g., "train")
    pub reference_split: String,

    /// The split value used as the current distribution (e.g., "test")
    pub current_split: String,

    /// Numeric feature fields to check for drift
    pub feature_fields: Vec<String>,

    /// Number of bins for PSI calculation (default: 10)
    pub num_bins: Option<usize>,

    /// PSI threshold above which drift is flagged (default: 0.2)
    pub threshold: Option<f64>,
}

/// Detects features with suspiciously high correlation to the target,
/// which may indicate target leakage.
///
/// Computes Pearson correlation between each feature and the target.
/// Features exceeding `max_correlation` are flagged.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TargetLeakageCheck {
    /// The target/label field
    pub target_field: String,

    /// Feature fields to check for leakage
    pub feature_fields: Vec<String>,

    /// Maximum allowed absolute correlation (default: 0.95)
    pub max_correlation: Option<f64>,
}

/// Detects disparate null rates across groups or splits.
///
/// Flags fields where the difference in null rates between groups
/// exceeds a threshold, indicating potential data quality issues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NullRateByGroupCheck {
    /// The field used to group rows (e.g., "split", "region")
    pub group_field: String,

    /// Fields to check for null rate disparity
    pub check_fields: Vec<String>,

    /// Maximum allowed difference in null rates across groups (default: 0.1)
    pub max_null_rate_diff: Option<f64>,
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
