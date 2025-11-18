use anyhow::{anyhow, Context, Result};
use contracts_core::{DataFormat, ValidationContext};
use contracts_iceberg::{IcebergConfig, IcebergValidator};
use contracts_parser::parse_file;
use contracts_validator::{DataSet, DataValidator};
use std::path::Path;
use tracing::info;

use crate::output;

pub async fn execute(
    contract_path: &str,
    strict: bool,
    schema_only: bool,
    sample_size: Option<usize>,
    format: &str,
) -> Result<()> {
    info!("Validating contract: {}", contract_path);
    info!("Strict mode: {}", strict);
    info!("Schema only: {}", schema_only);
    if let Some(size) = sample_size {
        info!("Sample size: {}", size);
    }

    // Parse the contract file
    let path = Path::new(contract_path);
    let contract = parse_file(path)
        .with_context(|| format!("Failed to parse contract file: {}", contract_path))?;

    output::print_info(&format!(
        "Contract loaded: {} v{} (owner: {})",
        contract.name, contract.version, contract.owner
    ));

    // Create validation context with user-provided options
    let context = ValidationContext {
        strict,
        schema_only,
        sample_size,
        metadata: Default::default(),
    };

    // Dispatch to appropriate validator based on contract format
    let report = match contract.schema.format {
        DataFormat::Iceberg => {
            // In schema-only mode, skip catalog connection
            if schema_only {
                output::print_info("Schema-only mode: validating contract structure without catalog");
                let dataset = DataSet::empty();
                let mut validator = DataValidator::new();
                validator.validate_with_data(&contract, &dataset, &context)
            } else {
                output::print_info("Detected Iceberg format, connecting to catalog...");
                validate_iceberg_table(&contract, &context).await?
            }
        }
        _ => {
            // For other formats, fall back to schema-only validation for now
            output::print_info(&format!(
                "Format {:?} not yet fully supported, performing schema-only validation",
                contract.schema.format
            ));
            let dataset = DataSet::empty();
            let mut validator = DataValidator::new();
            validator.validate_with_data(&contract, &dataset, &context)
        }
    };

    // Print the validation report
    output::print_validation_report(&report, format);

    if !report.passed {
        std::process::exit(1);
    }

    Ok(())
}

/// Validates an Iceberg table against a contract.
///
/// Extracts catalog configuration from environment variables and contract location.
async fn validate_iceberg_table(
    contract: &contracts_core::Contract,
    context: &ValidationContext,
) -> Result<contracts_core::ValidationReport> {
    // Parse location to extract namespace and table name
    // Expected formats:
    // - s3://warehouse/namespace/table
    // - /path/to/warehouse/namespace/table
    let location = &contract.schema.location;

    // Extract namespace and table name from location
    // This is a simplified parser - in production you'd want more robust parsing
    let (namespace, table_name) = parse_iceberg_location(location)?;

    output::print_info(&format!(
        "Parsed location: namespace={}, table={}",
        namespace.join("."),
        table_name
    ));

    // Get catalog configuration from environment variables
    // REST_CATALOG_URI: e.g., "http://localhost:8181"
    // WAREHOUSE: e.g., "s3://warehouse" or derived from location
    let catalog_uri = std::env::var("REST_CATALOG_URI")
        .ok()
        .or_else(|| std::env::var("ICEBERG_REST_URI").ok());

    let warehouse = std::env::var("WAREHOUSE")
        .ok()
        .or_else(|| std::env::var("ICEBERG_WAREHOUSE").ok())
        .or_else(|| extract_warehouse_from_location(location));

    // Build Iceberg configuration
    let config = if let (Some(uri), Some(warehouse)) = (catalog_uri, warehouse) {
        output::print_info(&format!("Using REST catalog: {}", uri));
        IcebergConfig::builder()
            .rest_catalog(uri, warehouse)
            .namespace(namespace)
            .table_name(table_name)
            .build()
            .context("Failed to build Iceberg configuration")?
    } else {
        return Err(anyhow!(
            "Missing Iceberg catalog configuration. Please set environment variables:\n\
             - REST_CATALOG_URI or ICEBERG_REST_URI (e.g., http://localhost:8181)\n\
             - WAREHOUSE or ICEBERG_WAREHOUSE (e.g., s3://my-warehouse)\n\
             \n\
             Example:\n\
             export REST_CATALOG_URI=http://localhost:8181\n\
             export WAREHOUSE=s3://my-data-lake"
        ));
    };

    // Create validator and validate
    output::print_info("Connecting to Iceberg catalog...");
    let validator = IcebergValidator::new(config).await.context(
        "Failed to connect to Iceberg catalog. Check that:\n\
                  1. The catalog is running and accessible\n\
                  2. Network connectivity is available\n\
                  3. Credentials are configured correctly (for cloud storage)",
    )?;

    output::print_info("Reading data from Iceberg table...");

    // Use the unified API with ValidationContext
    let report = validator
        .validate_table(contract, context)
        .await
        .context("Validation failed")?;

    Ok(report)
}

/// Parses an Iceberg location to extract namespace and table name.
///
/// Examples:
/// - "s3://warehouse/db/table" -> (["db"], "table")
/// - "/warehouse/db.schema/table" -> (["db", "schema"], "table")
fn parse_iceberg_location(location: &str) -> Result<(Vec<String>, String)> {
    // Remove scheme if present (s3://, file://, etc.)
    let path = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("file://"))
        .or_else(|| location.strip_prefix("hdfs://"))
        .unwrap_or(location);

    // Split by '/' and take the last components
    let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if parts.len() < 2 {
        return Err(anyhow!(
            "Invalid Iceberg location format: {}. Expected format: <warehouse>/<namespace>/<table>",
            location
        ));
    }

    // Last part is table name, second-to-last is namespace (may contain dots)
    let table_name = parts[parts.len() - 1].to_string();
    let namespace_part = parts[parts.len() - 2];

    // Namespace may be dot-separated (e.g., "db.schema")
    let namespace: Vec<String> = namespace_part.split('.').map(String::from).collect();

    Ok((namespace, table_name))
}

/// Extracts warehouse path from a full location.
///
/// Example: "s3://bucket/warehouse/db/table" -> "s3://bucket/warehouse"
fn extract_warehouse_from_location(location: &str) -> Option<String> {
    // For S3 paths, extract bucket and potential prefix
    if let Some(s3_path) = location.strip_prefix("s3://") {
        let parts: Vec<&str> = s3_path.split('/').filter(|s| !s.is_empty()).collect();
        if parts.len() >= 3 {
            // s3://bucket/warehouse_path
            return Some(format!("s3://{}/{}", parts[0], parts[1]));
        } else if !parts.is_empty() {
            // Just the bucket
            return Some(format!("s3://{}", parts[0]));
        }
    }

    None
}
