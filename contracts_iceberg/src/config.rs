//! Configuration for Iceberg connections.

use crate::IcebergError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Configuration for connecting to an Apache Iceberg table.
///
/// Supports various catalog types (REST, Hive, AWS Glue, etc.) and storage backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Location of the Iceberg table (e.g., "s3://bucket/path/to/table")
    pub table_location: String,

    /// Optional catalog type (rest, hive, glue, etc.)
    pub catalog_type: Option<String>,

    /// Optional catalog URI
    pub catalog_uri: Option<String>,

    /// Optional warehouse location
    pub warehouse: Option<String>,

    /// Additional properties for catalog configuration
    pub properties: HashMap<String, String>,

    /// Optional table namespace (for catalogs that require it)
    pub namespace: Option<Vec<String>>,

    /// Optional table name (if different from location)
    pub table_name: Option<String>,
}

impl IcebergConfig {
    /// Creates a new builder for `IcebergConfig`.
    pub fn builder() -> IcebergConfigBuilder {
        IcebergConfigBuilder::default()
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), IcebergError> {
        if self.table_location.is_empty() {
            return Err(IcebergError::ConfigurationError(
                "table_location cannot be empty".to_string(),
            ));
        }

        Ok(())
    }
}

/// Builder for `IcebergConfig`.
#[derive(Debug, Clone, Default)]
pub struct IcebergConfigBuilder {
    table_location: Option<String>,
    catalog_type: Option<String>,
    catalog_uri: Option<String>,
    warehouse: Option<String>,
    properties: HashMap<String, String>,
    namespace: Option<Vec<String>>,
    table_name: Option<String>,
}

impl IcebergConfigBuilder {
    /// Sets the table location.
    pub fn table_location<S: Into<String>>(mut self, location: S) -> Self {
        self.table_location = Some(location.into());
        self
    }

    /// Sets the catalog type.
    pub fn catalog_type<S: Into<String>>(mut self, catalog_type: S) -> Self {
        self.catalog_type = Some(catalog_type.into());
        self
    }

    /// Sets the catalog URI.
    pub fn catalog_uri<S: Into<String>>(mut self, uri: S) -> Self {
        self.catalog_uri = Some(uri.into());
        self
    }

    /// Sets the warehouse location.
    pub fn warehouse<S: Into<String>>(mut self, warehouse: S) -> Self {
        self.warehouse = Some(warehouse.into());
        self
    }

    /// Adds a property to the configuration.
    pub fn property<K: Into<String>, V: Into<String>>(mut self, key: K, value: V) -> Self {
        self.properties.insert(key.into(), value.into());
        self
    }

    /// Sets multiple properties at once.
    pub fn properties(mut self, properties: HashMap<String, String>) -> Self {
        self.properties = properties;
        self
    }

    /// Sets the table namespace.
    pub fn namespace(mut self, namespace: Vec<String>) -> Self {
        self.namespace = Some(namespace);
        self
    }

    /// Sets the table name.
    pub fn table_name<S: Into<String>>(mut self, name: S) -> Self {
        self.table_name = Some(name.into());
        self
    }

    /// Builds the `IcebergConfig`.
    ///
    /// Returns an error if required fields are missing.
    pub fn build(self) -> Result<IcebergConfig, IcebergError> {
        let config = IcebergConfig {
            table_location: self.table_location.ok_or_else(|| {
                IcebergError::ConfigurationError("table_location is required".to_string())
            })?,
            catalog_type: self.catalog_type,
            catalog_uri: self.catalog_uri,
            warehouse: self.warehouse,
            properties: self.properties,
            namespace: self.namespace,
            table_name: self.table_name,
        };

        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder() {
        let config = IcebergConfig::builder()
            .table_location("s3://bucket/table")
            .catalog_type("rest")
            .catalog_uri("http://localhost:8181")
            .warehouse("s3://bucket/warehouse")
            .property("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .namespace(vec!["db".to_string(), "schema".to_string()])
            .table_name("my_table")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.table_location, "s3://bucket/table");
        assert_eq!(config.catalog_type, Some("rest".to_string()));
        assert_eq!(
            config.properties.get("io-impl").unwrap(),
            "org.apache.iceberg.aws.s3.S3FileIO"
        );
    }

    #[test]
    fn test_config_missing_location() {
        let result = IcebergConfig::builder().build();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            IcebergError::ConfigurationError(_)
        ));
    }

    #[test]
    fn test_config_empty_location() {
        let result = IcebergConfig::builder().table_location("").build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_minimal() {
        let config = IcebergConfig::builder()
            .table_location("s3://bucket/table")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.table_location, "s3://bucket/table");
        assert!(config.catalog_type.is_none());
        assert!(config.properties.is_empty());
    }
}
