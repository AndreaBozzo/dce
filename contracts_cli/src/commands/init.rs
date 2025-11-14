use anyhow::{anyhow, Context, Result};
use contracts_core::{ContractBuilder, DataFormat};
use contracts_iceberg::{IcebergConfig, IcebergValidator};
use std::fs::File;
use std::io::Write;
use tracing::info;

use crate::output;

pub async fn execute(
    source: &str,
    output_path: Option<&str>,
    catalog_type: &str,
    namespace: Option<String>,
    table: Option<String>,
    owner: Option<String>,
    description: Option<String>,
) -> Result<()> {
    info!("Initializing contract from Iceberg source: {}", source);

    // Parse catalog type and build config
    let config = build_iceberg_config(source, catalog_type, namespace.clone(), table.clone())?;

    output::print_info(&format!(
        "Connecting to Iceberg catalog: {:?}",
        config.catalog
    ));

    // Create validator and extract schema
    let validator = IcebergValidator::new(config.clone())
        .await
        .context("Failed to connect to Iceberg catalog")?;

    let schema = validator
        .extract_schema()
        .await
        .context("Failed to extract schema from Iceberg table")?;

    output::print_success(&format!(
        "Extracted schema with {} fields",
        schema.fields.len()
    ));

    // Build contract from extracted schema
    // Use the actual table name from config, not from first field
    let table_name = &config.table_name;

    // Use provided owner or default to "data-team"
    let owner_name = owner.as_deref().unwrap_or("data-team");

    // Use provided description or generate a default one
    let contract_description = description.unwrap_or_else(|| {
        format!(
            "Auto-generated contract from Iceberg table {}.{}",
            namespace.as_ref().unwrap_or(&"default".to_string()),
            table_name
        )
    });

    let mut builder = ContractBuilder::new(table_name, owner_name)
        .version("1.0.0")
        .description(&contract_description)
        .location(&schema.location) // Use location from extracted schema
        .format(DataFormat::Iceberg);

    // Add all fields from schema
    for field in &schema.fields {
        builder = builder.field(field.clone());
    }

    let contract = builder.build();

    // Serialize to YAML
    let yaml =
        serde_yaml_ng::to_string(&contract).context("Failed to serialize contract to YAML")?;

    // Output to file or stdout
    if let Some(path) = output_path {
        let mut file = File::create(path)
            .with_context(|| format!("Failed to create output file: {}", path))?;
        file.write_all(yaml.as_bytes())
            .with_context(|| format!("Failed to write to file: {}", path))?;
        output::print_success(&format!("Contract written to: {}", path));
    } else {
        println!("{}", yaml);
    }

    Ok(())
}

fn build_iceberg_config(
    source: &str,
    catalog_type: &str,
    namespace: Option<String>,
    table: Option<String>,
) -> Result<IcebergConfig> {
    let namespace_vec = namespace
        .map(|ns| ns.split('.').map(String::from).collect())
        .ok_or_else(|| anyhow!("Namespace is required for Iceberg init"))?;

    let table_name = table.ok_or_else(|| anyhow!("Table name is required for Iceberg init"))?;

    let config = match catalog_type {
        "rest" => {
            // For REST: source is the catalog URI, need warehouse from env or default
            let warehouse = std::env::var("WAREHOUSE")
                .or_else(|_| std::env::var("ICEBERG_WAREHOUSE"))
                .unwrap_or_else(|_| "/warehouse".to_string());

            IcebergConfig::builder()
                .rest_catalog(source, &warehouse)
                .namespace(namespace_vec)
                .table_name(&table_name)
                .build()?
        }

        #[cfg(feature = "glue-catalog")]
        "glue" => {
            // For Glue: source should be the warehouse (S3 path)
            IcebergConfig::builder()
                .glue_catalog(source) // source is warehouse for Glue
                .namespace(namespace_vec)
                .table_name(&table_name)
                .build()?
        }

        #[cfg(feature = "hms-catalog")]
        "hms" => {
            // For HMS: source is the HMS URI, need warehouse from env or default
            let warehouse = std::env::var("WAREHOUSE")
                .or_else(|_| std::env::var("ICEBERG_WAREHOUSE"))
                .unwrap_or_else(|_| "/warehouse".to_string());

            IcebergConfig::builder()
                .hms_catalog(source, &warehouse)
                .namespace(namespace_vec)
                .table_name(&table_name)
                .build()?
        }

        _ => {
            return Err(anyhow!(
                "Unsupported catalog type: {}. Supported types: rest{}{}",
                catalog_type,
                if cfg!(feature = "glue-catalog") {
                    ", glue"
                } else {
                    ""
                },
                if cfg!(feature = "hms-catalog") {
                    ", hms"
                } else {
                    ""
                }
            ))
        }
    };

    Ok(config)
}
