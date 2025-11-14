use anyhow::{Context, Result};
use contracts_parser::parse_file;
use std::path::Path;
use tracing::info;

use crate::output;

pub async fn execute(contract_path: &str, _format: &str) -> Result<()> {
    info!("Checking contract schema: {}", contract_path);

    // Parse the contract file
    let path = Path::new(contract_path);
    let contract = parse_file(path)
        .with_context(|| format!("Failed to parse contract file: {}", contract_path))?;

    output::print_info(&format!(
        "Contract loaded: {} v{} (owner: {})",
        contract.name, contract.version, contract.owner
    ));

    // Contract parsed successfully means schema is valid
    output::print_success("Contract schema is valid");

    // Print contract summary
    println!("\nContract Summary:");
    println!("  Name:        {}", contract.name);
    println!("  Version:     {}", contract.version);
    println!("  Owner:       {}", contract.owner);
    println!(
        "  Description: {}",
        contract.description.as_deref().unwrap_or("N/A")
    );
    println!("  Format:      {:?}", contract.schema.format);
    println!("  Location:    {}", contract.schema.location);
    println!("  Fields:      {}", contract.schema.fields.len());

    if let Some(qc) = &contract.quality_checks {
        let mut checks = Vec::new();
        if qc.completeness.is_some() {
            checks.push("completeness".to_string());
        }
        if qc.uniqueness.is_some() {
            checks.push("uniqueness".to_string());
        }
        if qc.freshness.is_some() {
            checks.push("freshness".to_string());
        }
        if let Some(custom) = &qc.custom_checks {
            if !custom.is_empty() {
                checks.push(format!("{} custom", custom.len()));
            }
        }
        println!("  Quality Checks: {}", checks.join(", "));
    }

    if let Some(sla) = &contract.sla {
        println!("\nSLA:");
        if let Some(avail) = sla.availability {
            println!("  Availability:   {}", avail);
        }
        if let Some(rt) = &sla.response_time {
            println!("  Response Time:  {}", rt);
        }
    }

    Ok(())
}
