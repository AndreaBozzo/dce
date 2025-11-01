//! Builder pattern for creating data contracts.
//!
//! This module provides ergonomic builders for constructing contracts
//! and their components with a fluent API.

use crate::{
    CompletenessCheck, Contract, CustomCheck, DataFormat, Field, FieldConstraints, FreshnessCheck,
    QualityChecks, Schema, UniquenessCheck, SLA,
};

/// Builder for creating a `Contract`.
///
/// # Example
///
/// ```rust
/// use contracts_core::{ContractBuilder, DataFormat};
///
/// let contract = ContractBuilder::new("user_events", "analytics-team")
///     .version("1.0.0")
///     .description("User interaction events")
///     .location("s3://data/user_events")
///     .format(DataFormat::Iceberg)
///     .build();
/// ```
#[derive(Debug, Default)]
pub struct ContractBuilder {
    name: Option<String>,
    owner: Option<String>,
    version: Option<String>,
    description: Option<String>,
    location: Option<String>,
    format: Option<DataFormat>,
    fields: Vec<Field>,
    quality_checks: Option<QualityChecks>,
    sla: Option<SLA>,
}

impl ContractBuilder {
    /// Creates a new contract builder with required fields.
    ///
    /// # Arguments
    ///
    /// * `name` - Unique contract name
    /// * `owner` - Contract owner identifier
    pub fn new(name: impl Into<String>, owner: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            owner: Some(owner.into()),
            version: Some("1.0.0".to_string()),
            ..Default::default()
        }
    }

    /// Sets the contract version.
    pub fn version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Sets the contract description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the data location.
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Sets the data format.
    pub fn format(mut self, format: DataFormat) -> Self {
        self.format = Some(format);
        self
    }

    /// Adds a field to the schema.
    pub fn field(mut self, field: Field) -> Self {
        self.fields.push(field);
        self
    }

    /// Adds multiple fields to the schema.
    pub fn fields(mut self, fields: Vec<Field>) -> Self {
        self.fields.extend(fields);
        self
    }

    /// Sets quality checks.
    pub fn quality_checks(mut self, checks: QualityChecks) -> Self {
        self.quality_checks = Some(checks);
        self
    }

    /// Sets the SLA.
    pub fn sla(mut self, sla: SLA) -> Self {
        self.sla = Some(sla);
        self
    }

    /// Builds the contract.
    ///
    /// # Panics
    ///
    /// Panics if required fields (name, owner, location, format) are not set.
    pub fn build(self) -> Contract {
        Contract {
            version: self.version.expect("version is required"),
            name: self.name.expect("name is required"),
            owner: self.owner.expect("owner is required"),
            description: self.description,
            schema: Schema {
                fields: self.fields,
                format: self.format.expect("format is required"),
                location: self.location.expect("location is required"),
            },
            quality_checks: self.quality_checks,
            sla: self.sla,
        }
    }
}

/// Builder for creating a `Field`.
///
/// # Example
///
/// ```rust
/// use contracts_core::FieldBuilder;
///
/// let field = FieldBuilder::new("user_id", "string")
///     .description("Unique user identifier")
///     .nullable(false)
///     .build();
/// ```
#[derive(Debug, Default)]
pub struct FieldBuilder {
    name: Option<String>,
    field_type: Option<String>,
    nullable: bool,
    description: Option<String>,
    tags: Option<Vec<String>>,
    constraints: Option<Vec<FieldConstraints>>,
}

impl FieldBuilder {
    /// Creates a new field builder.
    ///
    /// # Arguments
    ///
    /// * `name` - Field name
    /// * `field_type` - Field type (e.g., "string", "int64")
    pub fn new(name: impl Into<String>, field_type: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            field_type: Some(field_type.into()),
            nullable: true,
            ..Default::default()
        }
    }

    /// Sets whether the field is nullable.
    pub fn nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Sets the field description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the field tags.
    pub fn tags(mut self, tags: Vec<String>) -> Self {
        self.tags = Some(tags);
        self
    }

    /// Adds a constraint to the field.
    pub fn constraint(mut self, constraint: FieldConstraints) -> Self {
        self.constraints
            .get_or_insert_with(Vec::new)
            .push(constraint);
        self
    }

    /// Builds the field.
    ///
    /// # Panics
    ///
    /// Panics if required fields (name, field_type) are not set.
    pub fn build(self) -> Field {
        Field {
            name: self.name.expect("name is required"),
            field_type: self.field_type.expect("field_type is required"),
            nullable: self.nullable,
            description: self.description,
            tags: self.tags,
            constraints: self.constraints,
        }
    }
}

/// Builder for creating `QualityChecks`.
#[derive(Debug, Default)]
pub struct QualityChecksBuilder {
    completeness: Option<CompletenessCheck>,
    uniqueness: Option<UniquenessCheck>,
    freshness: Option<FreshnessCheck>,
    custom_checks: Option<Vec<CustomCheck>>,
}

impl QualityChecksBuilder {
    /// Creates a new quality checks builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the completeness check.
    pub fn completeness(mut self, check: CompletenessCheck) -> Self {
        self.completeness = Some(check);
        self
    }

    /// Sets the uniqueness check.
    pub fn uniqueness(mut self, check: UniquenessCheck) -> Self {
        self.uniqueness = Some(check);
        self
    }

    /// Sets the freshness check.
    pub fn freshness(mut self, check: FreshnessCheck) -> Self {
        self.freshness = Some(check);
        self
    }

    /// Adds a custom check.
    pub fn custom_check(mut self, check: CustomCheck) -> Self {
        self.custom_checks.get_or_insert_with(Vec::new).push(check);
        self
    }

    /// Builds the quality checks.
    pub fn build(self) -> QualityChecks {
        QualityChecks {
            completeness: self.completeness,
            uniqueness: self.uniqueness,
            freshness: self.freshness,
            custom_checks: self.custom_checks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_contract_builder_minimal() {
        let contract = ContractBuilder::new("test", "team")
            .location("s3://data")
            .format(DataFormat::Parquet)
            .build();

        assert_eq!(contract.name, "test");
        assert_eq!(contract.owner, "team");
        assert_eq!(contract.version, "1.0.0"); // Default version
        assert_eq!(contract.schema.location, "s3://data");
        assert!(contract.description.is_none());
        assert!(contract.quality_checks.is_none());
        assert!(contract.sla.is_none());
    }

    #[test]
    fn test_contract_builder_full() {
        let field = FieldBuilder::new("id", "string").nullable(false).build();
        let qc = QualityChecksBuilder::new()
            .completeness(CompletenessCheck {
                threshold: 0.95,
                fields: vec!["id".to_string()],
            })
            .build();
        let sla = SLA {
            availability: Some(0.99),
            response_time: Some("100ms".to_string()),
            penalties: None,
        };

        let contract = ContractBuilder::new("users", "analytics")
            .version("2.0.0")
            .description("User data")
            .location("s3://users")
            .format(DataFormat::Iceberg)
            .field(field)
            .quality_checks(qc)
            .sla(sla)
            .build();

        assert_eq!(contract.name, "users");
        assert_eq!(contract.version, "2.0.0");
        assert_eq!(contract.description, Some("User data".to_string()));
        assert_eq!(contract.schema.fields.len(), 1);
        assert!(contract.quality_checks.is_some());
        assert!(contract.sla.is_some());
    }

    #[test]
    #[should_panic(expected = "version is required")]
    fn test_contract_builder_panic_missing_version() {
        // Create builder without using new() to skip default version
        let builder = ContractBuilder {
            name: Some("test".to_string()),
            owner: Some("team".to_string()),
            version: None, // Missing version
            location: Some("s3://data".to_string()),
            format: Some(DataFormat::Parquet),
            ..Default::default()
        };
        builder.build();
    }

    #[test]
    #[should_panic(expected = "location is required")]
    fn test_contract_builder_panic_missing_location() {
        ContractBuilder::new("test", "team")
            .format(DataFormat::Parquet)
            .build();
    }

    #[test]
    #[should_panic(expected = "format is required")]
    fn test_contract_builder_panic_missing_format() {
        ContractBuilder::new("test", "team")
            .location("s3://data")
            .build();
    }

    #[test]
    fn test_contract_builder_multiple_fields() {
        let fields = vec![
            FieldBuilder::new("id", "string").build(),
            FieldBuilder::new("name", "string").build(),
        ];

        let contract = ContractBuilder::new("test", "team")
            .location("s3://data")
            .format(DataFormat::Parquet)
            .fields(fields)
            .build();

        assert_eq!(contract.schema.fields.len(), 2);
        assert_eq!(contract.schema.fields[0].name, "id");
        assert_eq!(contract.schema.fields[1].name, "name");
    }

    #[test]
    fn test_field_builder_minimal() {
        let field = FieldBuilder::new("user_id", "string").build();

        assert_eq!(field.name, "user_id");
        assert_eq!(field.field_type, "string");
        assert!(field.nullable); // Default is true
        assert!(field.description.is_none());
        assert!(field.tags.is_none());
        assert!(field.constraints.is_none());
    }

    #[test]
    fn test_field_builder_full() {
        let field = FieldBuilder::new("email", "string")
            .nullable(false)
            .description("User email address")
            .tags(vec!["pii".to_string(), "required".to_string()])
            .constraint(FieldConstraints::Pattern {
                regex: r"^[a-z]+@[a-z]+\.[a-z]+$".to_string(),
            })
            .build();

        assert_eq!(field.name, "email");
        assert!(!field.nullable);
        assert_eq!(field.description, Some("User email address".to_string()));
        assert_eq!(
            field.tags,
            Some(vec!["pii".to_string(), "required".to_string()])
        );
        assert_eq!(field.constraints.as_ref().unwrap().len(), 1);
    }

    #[test]
    fn test_field_builder_multiple_constraints() {
        let field = FieldBuilder::new("age", "int32")
            .constraint(FieldConstraints::Range {
                min: 0.0,
                max: 150.0,
            })
            .constraint(FieldConstraints::Custom {
                definition: "age > 18".to_string(),
            })
            .build();

        let constraints = field.constraints.as_ref().unwrap();
        assert_eq!(constraints.len(), 2);
    }

    #[test]
    #[should_panic(expected = "name is required")]
    fn test_field_builder_panic_missing_name() {
        FieldBuilder::default().build();
    }

    #[test]
    #[should_panic(expected = "field_type is required")]
    fn test_field_builder_panic_missing_type() {
        // Create builder without type
        let builder = FieldBuilder {
            name: Some("id".to_string()),
            field_type: None, // Missing type
            ..Default::default()
        };
        builder.build();
    }

    #[test]
    fn test_quality_checks_builder_empty() {
        let qc = QualityChecksBuilder::new().build();

        assert!(qc.completeness.is_none());
        assert!(qc.uniqueness.is_none());
        assert!(qc.freshness.is_none());
        assert!(qc.custom_checks.is_none());
    }

    #[test]
    fn test_quality_checks_builder_full() {
        let qc = QualityChecksBuilder::new()
            .completeness(CompletenessCheck {
                threshold: 0.99,
                fields: vec!["id".to_string()],
            })
            .uniqueness(UniquenessCheck {
                fields: vec!["id".to_string()],
                scope: Some("global".to_string()),
            })
            .freshness(FreshnessCheck {
                max_delay: "1h".to_string(),
                metric: "updated_at".to_string(),
            })
            .custom_check(CustomCheck {
                name: "check1".to_string(),
                definition: "COUNT(*) > 0".to_string(),
                severity: Some("error".to_string()),
            })
            .custom_check(CustomCheck {
                name: "check2".to_string(),
                definition: "AVG(value) < 100".to_string(),
                severity: Some("warning".to_string()),
            })
            .build();

        assert!(qc.completeness.is_some());
        assert!(qc.uniqueness.is_some());
        assert!(qc.freshness.is_some());
        assert_eq!(qc.custom_checks.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_data_format_custom() {
        let contract = ContractBuilder::new("test", "team")
            .location("s3://data")
            .format(DataFormat::Custom("MyCustomFormat".to_string()))
            .build();

        match contract.schema.format {
            DataFormat::Custom(ref name) => assert_eq!(name, "MyCustomFormat"),
            _ => panic!("Expected Custom format"),
        }
    }

    #[test]
    fn test_field_constraints_custom() {
        let field = FieldBuilder::new("score", "float")
            .constraint(FieldConstraints::Custom {
                definition: "score BETWEEN 0 AND 100".to_string(),
            })
            .build();

        let constraints = field.constraints.as_ref().unwrap();
        assert_eq!(constraints.len(), 1);

        match &constraints[0] {
            FieldConstraints::Custom { definition } => {
                assert_eq!(definition, "score BETWEEN 0 AND 100");
            }
            _ => panic!("Expected Custom constraint"),
        }
    }

    #[test]
    fn test_field_constraints_allowed_values() {
        let field = FieldBuilder::new("status", "string")
            .constraint(FieldConstraints::AllowedValues {
                values: vec!["active".to_string(), "inactive".to_string()],
            })
            .build();

        let constraints = field.constraints.as_ref().unwrap();
        match &constraints[0] {
            FieldConstraints::AllowedValues { values } => {
                assert_eq!(values.len(), 2);
                assert_eq!(values[0], "active");
            }
            _ => panic!("Expected AllowedValues constraint"),
        }
    }

    #[test]
    fn test_field_constraints_range() {
        let field = FieldBuilder::new("temperature", "double")
            .constraint(FieldConstraints::Range {
                min: -273.15,
                max: 1000.0,
            })
            .build();

        let constraints = field.constraints.as_ref().unwrap();
        match &constraints[0] {
            FieldConstraints::Range { min, max } => {
                assert_eq!(*min, -273.15);
                assert_eq!(*max, 1000.0);
            }
            _ => panic!("Expected Range constraint"),
        }
    }

    #[test]
    fn test_field_constraints_pattern() {
        let field = FieldBuilder::new("uuid", "string")
            .constraint(FieldConstraints::Pattern {
                regex: r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
                    .to_string(),
            })
            .build();

        let constraints = field.constraints.as_ref().unwrap();
        match &constraints[0] {
            FieldConstraints::Pattern { regex } => {
                assert!(regex.contains("^[0-9a-f]{8}"));
            }
            _ => panic!("Expected Pattern constraint"),
        }
    }
}
