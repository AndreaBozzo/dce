//! Example: Configuring Hive Metastore Catalog
//!
//! This example demonstrates how to configure a Hive Metastore (HMS) catalog
//! for use with the Data Contracts Engine.
//!
//! Prerequisites:
//! - Running Hive Metastore instance
//! - Network access to the HMS thrift endpoint
//!
//! To run this example:
//! ```bash
//! cargo run --example hms_catalog
//! ```

use contracts_iceberg::IcebergConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Iceberg Hive Metastore Catalog Configuration Example ===\n");

    // Create an Iceberg configuration for Hive Metastore catalog
    println!("Creating Hive Metastore configuration...");
    let config = IcebergConfig::builder()
        .hms_catalog(
            "thrift://localhost:9083", // HMS thrift endpoint
            "/warehouse/hive",         // Warehouse location (HDFS or S3)
        )
        .namespace(vec!["default".to_string()]) // Hive database
        .table_name("product_catalog") // Table name
        .build()?;

    println!("✓ Configuration created successfully!\n");
    println!("Configuration details:");
    println!("  Catalog: Hive Metastore");
    println!("  URI: thrift://localhost:9083");
    println!("  Database: {:?}", config.namespace);
    println!("  Table: {}", config.table_name);
    println!("  Warehouse: {:?}", config.warehouse());

    // Multi-level namespace (e.g., catalog.database.schema)
    println!("\nCreating configuration with multi-level namespace...");
    let config_multi_ns = IcebergConfig::builder()
        .hms_catalog("thrift://localhost:9083", "/warehouse/hive")
        .namespace(vec![
            "catalog".to_string(),
            "database".to_string(),
            "schema".to_string(),
        ])
        .table_name("events")
        .build()?;

    println!("✓ Multi-level namespace configuration created!");
    println!("  Namespace levels: {:?}\n", config_multi_ns.namespace);

    // With custom HMS properties
    let config_with_props = IcebergConfig::builder()
        .hms_catalog("thrift://localhost:9083", "/warehouse/hive")
        .namespace(vec!["default".to_string()])
        .table_name("product_catalog")
        .property("hive.metastore.uris", "thrift://localhost:9083")
        .property("hive.metastore.timeout", "30")
        .build()?;

    println!("✓ Configuration with HMS properties created!");
    println!("  Properties: {:?}\n", config_with_props.properties);

    println!("=== Example completed successfully ===");
    println!("\nNote: Ensure the Hive Metastore is running and accessible.");
    println!("Default HMS thrift port is 9083.");

    Ok(())
}
