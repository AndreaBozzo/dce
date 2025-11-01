//! Parser for Data Contracts DSL (YAML/TOML formats).
//!
//! This module provides functionality to parse data contracts from YAML and TOML files
//! into the strongly-typed `Contract` structure.
//!
//! # Example
//!
//! ```rust
//! use contracts_parser::parse_yaml;
//!
//! let yaml = r#"
//! version: "1.0.0"
//! name: user_events
//! owner: analytics-team
//! description: User events dataset
//! schema:
//!   format: iceberg
//!   location: s3://data/user_events
//!   fields:
//!     - name: user_id
//!       type: string
//!       nullable: false
//! "#;
//!
//! let contract = parse_yaml(yaml).expect("Failed to parse contract");
//! assert_eq!(contract.name, "user_events");
//! ```

use contracts_core::Contract;
use std::path::Path;
use thiserror::Error;

/// Errors that can occur during contract parsing.
#[derive(Debug, Error)]
pub enum ParserError {
    /// YAML parsing or deserialization failed
    #[error("Failed to parse YAML: {0}")]
    YamlError(#[from] serde_yaml::Error),

    /// TOML parsing or deserialization failed
    #[error("Failed to parse TOML: {0}")]
    TomlError(String),

    /// File I/O error
    #[error("File I/O error: {0}")]
    IoError(#[from] std::io::Error),

    /// Unsupported file format
    #[error("Unsupported file format: {0}")]
    UnsupportedFormat(String),

    /// Invalid file extension
    #[error("Invalid or missing file extension")]
    InvalidExtension,
}

/// Result type alias for parser operations.
pub type Result<T> = std::result::Result<T, ParserError>;

/// Supported contract file formats.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContractFormat {
    /// YAML format (.yml, .yaml)
    Yaml,
    /// TOML format (.toml)
    Toml,
}

/// Parse a contract from a YAML string.
///
/// # Arguments
///
/// * `content` - The YAML string to parse
///
/// # Example
///
/// ```rust
/// use contracts_parser::parse_yaml;
///
/// let yaml = r#"
/// version: "1.0.0"
/// name: my_dataset
/// owner: data-team
/// schema:
///   format: parquet
///   location: s3://bucket/data
///   fields: []
/// "#;
///
/// let contract = parse_yaml(yaml).unwrap();
/// assert_eq!(contract.name, "my_dataset");
/// ```
pub fn parse_yaml(content: &str) -> Result<Contract> {
    let contract: Contract = serde_yaml::from_str(content)?;
    Ok(contract)
}

/// Parse a contract from a TOML string.
///
/// # Arguments
///
/// * `content` - The TOML string to parse
///
/// # Example
///
/// ```rust
/// use contracts_parser::parse_toml;
///
/// let toml = r#"
/// version = "1.0.0"
/// name = "my_dataset"
/// owner = "data-team"
///
/// [schema]
/// format = "parquet"
/// location = "s3://bucket/data"
/// fields = []
/// "#;
///
/// let contract = parse_toml(toml).unwrap();
/// assert_eq!(contract.name, "my_dataset");
/// ```
pub fn parse_toml(content: &str) -> Result<Contract> {
    let contract: Contract =
        toml::from_str(content).map_err(|e| ParserError::TomlError(e.to_string()))?;
    Ok(contract)
}

/// Detect the contract format from a file path based on its extension.
///
/// # Arguments
///
/// * `path` - Path to the contract file
///
/// # Supported Extensions
///
/// * `.yaml`, `.yml` → `ContractFormat::Yaml`
/// * `.toml` → `ContractFormat::Toml`
///
/// # Errors
///
/// Returns `ParserError::InvalidExtension` if the file has no extension.
/// Returns `ParserError::UnsupportedFormat` if the extension is not recognized.
pub fn detect_format(path: &Path) -> Result<ContractFormat> {
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .ok_or(ParserError::InvalidExtension)?;

    match extension.to_lowercase().as_str() {
        "yaml" | "yml" => Ok(ContractFormat::Yaml),
        "toml" => Ok(ContractFormat::Toml),
        other => Err(ParserError::UnsupportedFormat(other.to_string())),
    }
}

/// Parse a contract from a file with automatic format detection.
///
/// The format is determined by the file extension:
/// - `.yaml`, `.yml` → parsed as YAML
/// - `.toml` → parsed as TOML
///
/// # Arguments
///
/// * `path` - Path to the contract file
///
/// # Example
///
/// ```no_run
/// use contracts_parser::parse_file;
/// use std::path::Path;
///
/// let contract = parse_file(Path::new("contracts/user_events.yml")).unwrap();
/// println!("Loaded contract: {}", contract.name);
/// ```
pub fn parse_file(path: &Path) -> Result<Contract> {
    let content = std::fs::read_to_string(path)?;
    let format = detect_format(path)?;

    match format {
        ContractFormat::Yaml => parse_yaml(&content),
        ContractFormat::Toml => parse_toml(&content),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_core::{DataFormat, Field, Schema};
    use pretty_assertions::assert_eq;

    #[test]
    fn test_parse_valid_yaml_minimal() {
        let yaml = r#"
version: "1.0.0"
name: test_contract
owner: test-team
schema:
  format: parquet
  location: s3://test/data
  fields: []
"#;

        let contract = parse_yaml(yaml).expect("Failed to parse valid YAML");

        assert_eq!(contract.version, "1.0.0");
        assert_eq!(contract.name, "test_contract");
        assert_eq!(contract.owner, "test-team");
        assert_eq!(contract.description, None);
        assert_eq!(contract.schema.location, "s3://test/data");
        assert!(contract.schema.fields.is_empty());
        assert!(contract.quality_checks.is_none());
        assert!(contract.sla.is_none());
    }

    #[test]
    fn test_parse_valid_yaml_with_fields() {
        let yaml = r#"
version: "1.0.0"
name: user_data
owner: analytics
description: User dataset
schema:
  format: iceberg
  location: s3://data/users
  fields:
    - name: user_id
      type: string
      nullable: false
      description: Unique user ID
      tags:
        - primary_key
        - pii
    - name: email
      type: string
      nullable: true
      constraints:
        - type: pattern
          regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$
"#;

        let contract = parse_yaml(yaml).expect("Failed to parse YAML with fields");

        assert_eq!(contract.name, "user_data");
        assert_eq!(contract.schema.fields.len(), 2);

        let user_id = &contract.schema.fields[0];
        assert_eq!(user_id.name, "user_id");
        assert_eq!(user_id.field_type, "string");
        assert!(!user_id.nullable);
        assert_eq!(user_id.description, Some("Unique user ID".to_string()));
        assert_eq!(
            user_id.tags,
            Some(vec!["primary_key".to_string(), "pii".to_string()])
        );

        let email = &contract.schema.fields[1];
        assert_eq!(email.name, "email");
        assert!(email.nullable);
        assert!(email.constraints.is_some());
    }

    #[test]
    fn test_parse_yaml_with_quality_checks() {
        let yaml = r#"
version: "1.0.0"
name: events
owner: analytics
schema:
  format: iceberg
  location: s3://data/events
  fields: []
quality_checks:
  completeness:
    threshold: 0.99
    fields:
      - event_id
      - user_id
  uniqueness:
    fields:
      - event_id
    scope: global
  freshness:
    max_delay: 1h
    metric: event_timestamp
  custom_checks:
    - name: valid_types
      definition: "SELECT COUNT(*) = 0 FROM events WHERE type NOT IN ('a', 'b')"
      severity: error
"#;

        let contract = parse_yaml(yaml).expect("Failed to parse YAML with quality checks");

        let qc = contract
            .quality_checks
            .expect("Quality checks should be present");

        // Completeness
        let completeness = qc.completeness.expect("Completeness should be present");
        assert_eq!(completeness.threshold, 0.99);
        assert_eq!(completeness.fields, vec!["event_id", "user_id"]);

        // Uniqueness
        let uniqueness = qc.uniqueness.expect("Uniqueness should be present");
        assert_eq!(uniqueness.fields, vec!["event_id"]);
        assert_eq!(uniqueness.scope, Some("global".to_string()));

        // Freshness
        let freshness = qc.freshness.expect("Freshness should be present");
        assert_eq!(freshness.max_delay, "1h");
        assert_eq!(freshness.metric, "event_timestamp");

        // Custom checks
        let custom = qc.custom_checks.expect("Custom checks should be present");
        assert_eq!(custom.len(), 1);
        assert_eq!(custom[0].name, "valid_types");
        assert_eq!(custom[0].severity, Some("error".to_string()));
    }

    #[test]
    fn test_parse_yaml_with_sla() {
        let yaml = r#"
version: "1.0.0"
name: api_data
owner: backend-team
schema:
  format: parquet
  location: s3://data/api
  fields: []
sla:
  availability: 0.999
  response_time: 100ms
  penalties: Credit 10% for violations
"#;

        let contract = parse_yaml(yaml).expect("Failed to parse YAML with SLA");

        let sla = contract.sla.expect("SLA should be present");
        assert_eq!(sla.availability, Some(0.999));
        assert_eq!(sla.response_time, Some("100ms".to_string()));
        assert_eq!(sla.penalties, Some("Credit 10% for violations".to_string()));
    }

    #[test]
    fn test_parse_invalid_yaml() {
        let invalid_yaml = r#"
version: "1.0.0"
name: test
owner: team
schema:
  invalid_field: this should fail
  missing required fields
"#;

        let result = parse_yaml(invalid_yaml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParserError::YamlError(_)));
    }

    #[test]
    fn test_parse_yaml_missing_required_fields() {
        let yaml = r#"
version: "1.0.0"
name: test
"#;

        let result = parse_yaml(yaml);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_valid_toml_minimal() {
        let toml = r#"
version = "1.0.0"
name = "test_contract"
owner = "test-team"

[schema]
format = "parquet"
location = "s3://test/data"
fields = []
"#;

        let contract = parse_toml(toml).expect("Failed to parse valid TOML");

        assert_eq!(contract.version, "1.0.0");
        assert_eq!(contract.name, "test_contract");
        assert_eq!(contract.owner, "test-team");
        assert_eq!(contract.schema.location, "s3://test/data");
    }

    #[test]
    fn test_parse_toml_with_fields() {
        let toml = r#"
version = "1.0.0"
name = "user_data"
owner = "analytics"
description = "User dataset"

[schema]
format = "iceberg"
location = "s3://data/users"

[[schema.fields]]
name = "user_id"
type = "string"
nullable = false
description = "Unique user ID"
tags = ["primary_key", "pii"]

[[schema.fields]]
name = "email"
type = "string"
nullable = true
"#;

        let contract = parse_toml(toml).expect("Failed to parse TOML with fields");

        assert_eq!(contract.name, "user_data");
        assert_eq!(contract.schema.fields.len(), 2);

        let user_id = &contract.schema.fields[0];
        assert_eq!(user_id.name, "user_id");
        assert_eq!(user_id.field_type, "string");
        assert!(!user_id.nullable);
    }

    #[test]
    fn test_parse_invalid_toml() {
        let invalid_toml = r#"
version = "1.0.0"
name = "test"
[[[invalid syntax
"#;

        let result = parse_toml(invalid_toml);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParserError::TomlError(_)));
    }

    #[test]
    fn test_detect_format_yaml() {
        let path = Path::new("contract.yaml");
        assert_eq!(detect_format(path).unwrap(), ContractFormat::Yaml);

        let path = Path::new("contract.yml");
        assert_eq!(detect_format(path).unwrap(), ContractFormat::Yaml);
    }

    #[test]
    fn test_detect_format_toml() {
        let path = Path::new("contract.toml");
        assert_eq!(detect_format(path).unwrap(), ContractFormat::Toml);
    }

    #[test]
    fn test_detect_format_unsupported() {
        let path = Path::new("contract.json");
        let result = detect_format(path);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ParserError::UnsupportedFormat(_)
        ));
    }

    #[test]
    fn test_detect_format_no_extension() {
        let path = Path::new("contract");
        let result = detect_format(path);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), ParserError::InvalidExtension));
    }

    #[test]
    fn test_parse_file_yaml() {
        // This test uses the actual example file
        let path = Path::new("../examples/contracts/user_events.yml");

        // Only run if the file exists
        if path.exists() {
            let contract = parse_file(path).expect("Failed to parse example YAML file");

            assert_eq!(contract.version, "1.0.0");
            assert_eq!(contract.name, "user_events");
            assert_eq!(contract.owner, "analytics-team");
            assert_eq!(
                contract.description,
                Some("User interaction events dataset for analytics and ML".to_string())
            );
            assert_eq!(contract.schema.fields.len(), 9);
            assert!(contract.quality_checks.is_some());
            assert!(contract.sla.is_some());
        }
    }

    #[test]
    fn test_round_trip_yaml() {
        // Create a contract, serialize to YAML, parse it back
        let original = Contract {
            version: "1.0.0".to_string(),
            name: "test".to_string(),
            owner: "team".to_string(),
            description: Some("Test description".to_string()),
            schema: Schema {
                fields: vec![Field {
                    name: "id".to_string(),
                    field_type: "string".to_string(),
                    nullable: false,
                    description: Some("ID field".to_string()),
                    tags: Some(vec!["key".to_string()]),
                    constraints: None,
                }],
                format: DataFormat::Parquet,
                location: "s3://test".to_string(),
            },
            quality_checks: None,
            sla: None,
        };

        // Serialize to YAML
        let yaml = serde_yaml::to_string(&original).expect("Failed to serialize");

        // Parse it back
        let parsed = parse_yaml(&yaml).expect("Failed to parse");

        // Compare
        assert_eq!(parsed.version, original.version);
        assert_eq!(parsed.name, original.name);
        assert_eq!(parsed.owner, original.owner);
        assert_eq!(parsed.description, original.description);
        assert_eq!(parsed.schema.fields.len(), original.schema.fields.len());
        assert_eq!(parsed.schema.fields[0].name, original.schema.fields[0].name);
        assert_eq!(parsed.schema.location, original.schema.location);
    }
}
