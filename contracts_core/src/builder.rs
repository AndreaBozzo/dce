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
