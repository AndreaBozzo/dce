//! Configuration for Iceberg connections.

use crate::IcebergError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Type of Iceberg catalog to use.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CatalogType {
    /// Direct file-based access (no catalog)
    FileIO,

    /// REST catalog
    Rest {
        /// REST catalog URI (e.g., "http://localhost:8181")
        uri: String,
        /// Warehouse location
        warehouse: String,
    },

    /// AWS Glue catalog
    Glue {
        /// Warehouse location (typically S3)
        warehouse: String,
        /// Optional Glue catalog ID
        catalog_id: Option<String>,
        /// Optional AWS region
        region: Option<String>,
    },

    /// Hive Metastore catalog
    Hms {
        /// Hive Metastore URI (e.g., "127.0.0.1:9083")
        uri: String,
        /// Warehouse location
        warehouse: String,
    },
}

/// Configuration for connecting to an Apache Iceberg table.
///
/// Supports various catalog types (REST, Hive, AWS Glue, etc.) and storage backends.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IcebergConfig {
    /// Catalog configuration
    pub catalog: CatalogType,

    /// Table namespace (e.g., ["database", "schema"])
    pub namespace: Vec<String>,

    /// Table name
    pub table_name: String,

    /// Additional properties for catalog configuration
    pub properties: HashMap<String, String>,
}

impl IcebergConfig {
    /// Creates a new builder for `IcebergConfig`.
    pub fn builder() -> IcebergConfigBuilder {
        IcebergConfigBuilder::default()
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), IcebergError> {
        if self.table_name.is_empty() {
            return Err(IcebergError::ConfigurationError(
                "table_name cannot be empty".to_string(),
            ));
        }

        if self.namespace.is_empty() {
            return Err(IcebergError::ConfigurationError(
                "namespace cannot be empty".to_string(),
            ));
        }

        Ok(())
    }

    /// Returns the warehouse location from the catalog configuration.
    pub fn warehouse(&self) -> Option<&str> {
        match &self.catalog {
            CatalogType::FileIO => None,
            CatalogType::Rest { warehouse, .. } => Some(warehouse),
            CatalogType::Glue { warehouse, .. } => Some(warehouse),
            CatalogType::Hms { warehouse, .. } => Some(warehouse),
        }
    }
}

/// Builder for `IcebergConfig`.
#[derive(Debug, Clone, Default)]
pub struct IcebergConfigBuilder {
    catalog: Option<CatalogType>,
    namespace: Option<Vec<String>>,
    table_name: Option<String>,
    properties: HashMap<String, String>,
}

impl IcebergConfigBuilder {
    /// Sets the catalog type to FileIO (direct file access).
    pub fn file_io(mut self) -> Self {
        self.catalog = Some(CatalogType::FileIO);
        self
    }

    /// Sets the catalog type to REST.
    pub fn rest_catalog<S: Into<String>>(mut self, uri: S, warehouse: S) -> Self {
        self.catalog = Some(CatalogType::Rest {
            uri: uri.into(),
            warehouse: warehouse.into(),
        });
        self
    }

    /// Sets the catalog type to AWS Glue.
    pub fn glue_catalog<S: Into<String>>(mut self, warehouse: S) -> Self {
        self.catalog = Some(CatalogType::Glue {
            warehouse: warehouse.into(),
            catalog_id: None,
            region: None,
        });
        self
    }

    /// Sets the catalog type to AWS Glue with additional options.
    pub fn glue_catalog_with_options<S: Into<String>>(
        mut self,
        warehouse: S,
        catalog_id: Option<String>,
        region: Option<String>,
    ) -> Self {
        self.catalog = Some(CatalogType::Glue {
            warehouse: warehouse.into(),
            catalog_id,
            region,
        });
        self
    }

    /// Sets the catalog type to Hive Metastore.
    pub fn hms_catalog<S: Into<String>>(mut self, uri: S, warehouse: S) -> Self {
        self.catalog = Some(CatalogType::Hms {
            uri: uri.into(),
            warehouse: warehouse.into(),
        });
        self
    }

    /// Sets the catalog directly.
    pub fn catalog(mut self, catalog: CatalogType) -> Self {
        self.catalog = Some(catalog);
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

    /// Builds the `IcebergConfig`.
    ///
    /// Returns an error if required fields are missing.
    pub fn build(self) -> Result<IcebergConfig, IcebergError> {
        let config = IcebergConfig {
            catalog: self.catalog.ok_or_else(|| {
                IcebergError::ConfigurationError("catalog type is required".to_string())
            })?,
            namespace: self.namespace.ok_or_else(|| {
                IcebergError::ConfigurationError("namespace is required".to_string())
            })?,
            table_name: self.table_name.ok_or_else(|| {
                IcebergError::ConfigurationError("table_name is required".to_string())
            })?,
            properties: self.properties,
        };

        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_builder_rest() {
        let config = IcebergConfig::builder()
            .rest_catalog("http://localhost:8181", "s3://bucket/warehouse")
            .namespace(vec!["db".to_string(), "schema".to_string()])
            .table_name("my_table")
            .property("io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.table_name, "my_table");
        assert_eq!(
            config.namespace,
            vec!["db".to_string(), "schema".to_string()]
        );
        assert!(matches!(config.catalog, CatalogType::Rest { .. }));
        assert_eq!(
            config.properties.get("io-impl").unwrap(),
            "org.apache.iceberg.aws.s3.S3FileIO"
        );
    }

    #[test]
    fn test_config_builder_glue() {
        let config = IcebergConfig::builder()
            .glue_catalog("s3://bucket/warehouse")
            .namespace(vec!["database".to_string()])
            .table_name("events")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(matches!(config.catalog, CatalogType::Glue { .. }));
        assert_eq!(config.warehouse(), Some("s3://bucket/warehouse"));
    }

    #[test]
    fn test_config_builder_hms() {
        let config = IcebergConfig::builder()
            .hms_catalog("127.0.0.1:9083", "s3://bucket/warehouse")
            .namespace(vec!["db".to_string()])
            .table_name("users")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(matches!(config.catalog, CatalogType::Hms { .. }));
    }

    #[test]
    fn test_config_builder_file_io() {
        let config = IcebergConfig::builder()
            .file_io()
            .namespace(vec!["local".to_string()])
            .table_name("test_table")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();
        assert!(matches!(config.catalog, CatalogType::FileIO));
        assert_eq!(config.warehouse(), None);
    }

    #[test]
    fn test_config_missing_catalog() {
        let result = IcebergConfig::builder()
            .namespace(vec!["db".to_string()])
            .table_name("table")
            .build();
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            IcebergError::ConfigurationError(_)
        ));
    }

    #[test]
    fn test_config_missing_namespace() {
        let result = IcebergConfig::builder()
            .file_io()
            .table_name("table")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_missing_table_name() {
        let result = IcebergConfig::builder()
            .file_io()
            .namespace(vec!["db".to_string()])
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_empty_table_name() {
        let result = IcebergConfig::builder()
            .file_io()
            .namespace(vec!["db".to_string()])
            .table_name("")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_catalog_type_serde() {
        let catalog = CatalogType::Rest {
            uri: "http://localhost:8181".to_string(),
            warehouse: "s3://warehouse".to_string(),
        };

        let json = serde_json::to_string(&catalog).unwrap();
        let deserialized: CatalogType = serde_json::from_str(&json).unwrap();
        assert_eq!(catalog, deserialized);
    }
}
