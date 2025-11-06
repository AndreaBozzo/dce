//! Example: Configuring Iceberg REST Catalog
//!
//! This example demonstrates how to configure an Iceberg REST catalog
//! for use with the Data Contracts Engine.
//!
//! To run this example:
//! ```bash
//! cargo run --example rest_catalog
//! ```

use contracts_iceberg::IcebergConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Iceberg REST Catalog Configuration Example ===\n");

    // Create an Iceberg configuration for REST catalog
    println!("Creating Iceberg configuration for REST catalog...");
    let config = IcebergConfig::builder()
        .rest_catalog(
            "http://localhost:8181", // REST catalog URI
            "/warehouse",            // Warehouse location
        )
        .namespace(vec!["my_namespace".to_string()]) // Database/namespace
        .table_name("events") // Table name
        .build()?;

    println!("✓ Configuration created successfully!\n");
    println!("Configuration details:");
    println!("  Catalog: REST");
    println!("  URI: http://localhost:8181");
    println!("  Namespace: {:?}", config.namespace);
    println!("  Table: {}", config.table_name);
    println!("  Warehouse: {:?}", config.warehouse());

    // You can add custom properties
    let config_with_props = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["my_namespace".to_string()])
        .table_name("events")
        .property("custom_key", "custom_value")
        .property("token", "my-auth-token")
        .build()?;

    println!("\n✓ Configuration with custom properties created!");
    println!("  Properties: {:?}", config_with_props.properties);

    println!("\n=== Example completed successfully ===");
    println!("\nNext steps:");
    println!("  1. Use IcebergValidator::new(config).await to create a validator");
    println!("  2. Call validator.validate_table() or validate_schema_only()");
    println!("  3. See the integration tests for complete validation examples");

    Ok(())
}
