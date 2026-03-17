//! Example: Configuring AWS Glue Catalog
//!
//! This example demonstrates how to configure an AWS Glue catalog
//! for use with the Data Contracts Engine.
//!
//! Prerequisites:
//! - AWS credentials configured (via env vars, ~/.aws/credentials, or IAM role)
//! - Appropriate IAM permissions for Glue and S3
//!
//! To run this example:
//! ```bash
//! cargo run --example glue_catalog
//! ```

use contracts_iceberg::IcebergConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Iceberg AWS Glue Catalog Configuration Example ===\n");

    // Simple Glue catalog configuration
    println!("Creating basic Glue catalog configuration...");
    let config = IcebergConfig::builder()
        .glue_catalog("s3://my-bucket/warehouse") // S3 warehouse location
        .namespace(vec!["my_database".to_string()]) // Glue database name
        .table_name("user_events") // Glue table name
        .build()?;

    println!("✓ Basic configuration created!");
    println!("  Catalog: AWS Glue");
    println!("  Database: {:?}", config.namespace);
    println!("  Table: {}", config.table_name);
    println!("  Warehouse: {:?}\n", config.warehouse());

    // Glue catalog with additional options (catalog ID and region)
    println!("Creating Glue catalog with additional options...");
    let _config_with_options = IcebergConfig::builder()
        .glue_catalog_with_options(
            "s3://my-bucket/warehouse",
            Some("my-glue-catalog-id".to_string()),
            Some("us-west-2".to_string()),
        )
        .namespace(vec!["my_database".to_string()])
        .table_name("user_events")
        .build()?;

    println!("✓ Configuration with options created!");
    println!("  Includes catalog ID and region settings\n");

    // With custom properties
    let config_with_props = IcebergConfig::builder()
        .glue_catalog("s3://my-bucket/warehouse")
        .namespace(vec!["my_database".to_string()])
        .table_name("user_events")
        .property("glue.skip-archive", "true")
        .property("glue.lf-tags-database", "environment=prod")
        .build()?;

    println!("✓ Configuration with Glue properties created!");
    println!("  Properties: {:?}\n", config_with_props.properties);

    println!("=== Example completed successfully ===");
    println!("\nNote: Make sure AWS credentials are properly configured.");
    println!("The validator will use these credentials to access Glue and S3.");

    Ok(())
}
