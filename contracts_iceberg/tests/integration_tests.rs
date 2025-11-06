//! Integration tests for contracts_iceberg
//!
//! These tests verify the integration between different components
//! and provide examples of usage patterns.

use contracts_iceberg::IcebergConfig;

#[test]
fn test_config_builder_rest_catalog() {
    // Test building a valid REST catalog configuration
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["my_namespace".to_string()])
        .table_name("my_table")
        .build();

    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.table_name, "my_table");
}

#[test]
fn test_config_builder_glue_catalog() {
    // Test building a valid AWS Glue catalog configuration
    let config = IcebergConfig::builder()
        .glue_catalog("s3://my-bucket/warehouse")
        .namespace(vec!["my_db".to_string()])
        .table_name("my_table")
        .build();

    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.table_name, "my_table");
}

#[test]
fn test_config_builder_glue_catalog_with_options() {
    // Test building a Glue catalog configuration with options
    let config = IcebergConfig::builder()
        .glue_catalog_with_options(
            "s3://my-bucket/warehouse",
            Some("my-catalog-id".to_string()),
            Some("us-west-2".to_string()),
        )
        .namespace(vec!["my_db".to_string()])
        .table_name("my_table")
        .build();

    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.table_name, "my_table");
}

#[test]
fn test_config_builder_hms_catalog() {
    // Test building a valid Hive Metastore catalog configuration
    let config = IcebergConfig::builder()
        .hms_catalog("thrift://localhost:9083", "/warehouse")
        .namespace(vec!["default".to_string()])
        .table_name("events")
        .build();

    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.table_name, "events");
}

#[test]
fn test_config_builder_missing_catalog_type() {
    // Test that building without specifying a catalog type fails
    let config = IcebergConfig::builder()
        .namespace(vec!["db".to_string()])
        .table_name("table")
        .build();

    assert!(config.is_err());
    assert!(config.unwrap_err().to_string().contains("catalog type"));
}

#[test]
fn test_config_builder_missing_table_name() {
    // Test that building without a table name fails
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["db".to_string()])
        .build();

    assert!(config.is_err());
    assert!(config.unwrap_err().to_string().contains("table_name"));
}

#[test]
fn test_config_builder_missing_namespace() {
    // Test that building without a namespace fails
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .table_name("table")
        .build();

    assert!(config.is_err());
    assert!(config.unwrap_err().to_string().contains("namespace"));
}

#[test]
fn test_config_warehouse_accessor() {
    // Test the warehouse() accessor method
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["test".to_string()])
        .table_name("events")
        .build()
        .unwrap();

    assert_eq!(config.warehouse(), Some("/warehouse"));
}

#[test]
fn test_config_serialization() {
    // Test that configuration can be serialized and deserialized
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["test".to_string()])
        .table_name("events")
        .build()
        .unwrap();

    // Serialize to JSON
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("test"));
    assert!(json.contains("events"));

    // Deserialize back
    let deserialized: IcebergConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.table_name, config.table_name);
}

#[test]
fn test_multiple_namespace_levels() {
    // Test configuration with multi-level namespace (e.g., catalog.db.schema)
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec![
            "catalog".to_string(),
            "database".to_string(),
            "schema".to_string(),
        ])
        .table_name("table")
        .build();

    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.namespace.len(), 3);
}

#[test]
fn test_config_with_custom_properties() {
    // Test adding custom properties to the configuration
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["test".to_string()])
        .table_name("events")
        .property("custom_prop", "custom_value")
        .property("another_prop", "another_value")
        .build();

    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(
        config.properties.get("custom_prop"),
        Some(&"custom_value".to_string())
    );
    assert_eq!(
        config.properties.get("another_prop"),
        Some(&"another_value".to_string())
    );
}

#[test]
fn test_config_validation_empty_table_name() {
    // Test that validation catches empty table name
    let config = IcebergConfig {
        catalog: contracts_iceberg::CatalogType::Rest {
            uri: "http://localhost:8181".to_string(),
            warehouse: "/warehouse".to_string(),
        },
        namespace: vec!["db".to_string()],
        table_name: "".to_string(),
        properties: Default::default(),
    };

    assert!(config.validate().is_err());
}

#[test]
fn test_config_validation_empty_namespace() {
    // Test that validation catches empty namespace
    let config = IcebergConfig {
        catalog: contracts_iceberg::CatalogType::Rest {
            uri: "http://localhost:8181".to_string(),
            warehouse: "/warehouse".to_string(),
        },
        namespace: vec![],
        table_name: "table".to_string(),
        properties: Default::default(),
    };

    assert!(config.validate().is_err());
}
