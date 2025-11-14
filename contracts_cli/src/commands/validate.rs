use anyhow::{Context, Result};
use contracts_core::ValidationContext;
use contracts_parser::parse_file;
use contracts_validator::{DataSet, DataValidator};
use std::path::Path;
use tracing::info;

use crate::output;

pub async fn execute(contract_path: &str, strict: bool, format: &str) -> Result<()> {
    info!("Validating contract: {}", contract_path);
    info!("Strict mode: {}", strict);

    // Parse the contract file
    let path = Path::new(contract_path);
    let contract = parse_file(path)
        .with_context(|| format!("Failed to parse contract file: {}", contract_path))?;

    output::print_info(&format!(
        "Contract loaded: {} v{} (owner: {})",
        contract.name, contract.version, contract.owner
    ));

    // Create validation context
    let context = ValidationContext {
        strict,
        schema_only: false,
        sample_size: None,
        metadata: Default::default(),
    };

    // For now, validate with empty dataset (schema-only validation)
    // In a real implementation, this would connect to the actual data source
    // based on contract.schema.location and contract.schema.format
    let dataset = DataSet::empty();

    output::print_info("Performing schema-only validation (data validation not yet implemented)");

    let mut validator = DataValidator::new();
    let report = validator.validate_with_data(&contract, &dataset, &context);

    // Print the validation report
    output::print_validation_report(&report, format);

    if !report.passed {
        std::process::exit(1);
    }

    Ok(())
}
