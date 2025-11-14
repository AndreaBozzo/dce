use colored::*;
use contracts_core::ValidationReport;
use serde_json::json;

pub fn print_validation_report(report: &ValidationReport, format: &str) {
    match format {
        "json" => print_json_report(report),
        _ => print_text_report(report),
    }
}

fn print_text_report(report: &ValidationReport) {
    println!("\n{}", "═".repeat(60));
    println!("{}", "  VALIDATION REPORT".bold());
    println!("{}", "═".repeat(60));

    if report.passed {
        println!(
            "\n{} {}",
            "✓".green().bold(),
            "Validation PASSED".green().bold()
        );
    } else {
        println!(
            "\n{} {}",
            "✗".red().bold(),
            "Validation FAILED".red().bold()
        );
    }

    if !report.errors.is_empty() {
        println!("\n{}", "Errors:".red().bold());
        for (i, error) in report.errors.iter().enumerate() {
            println!("  {}. {}", i + 1, error.to_string().red());
        }
    }

    if !report.warnings.is_empty() {
        println!("\n{}", "Warnings:".yellow().bold());
        for (i, warning) in report.warnings.iter().enumerate() {
            println!("  {}. {}", i + 1, warning.to_string().yellow());
        }
    }

    println!("\n{}", "Summary:".bold());
    println!("  Total errors:   {}", report.errors.len());
    println!("  Total warnings: {}", report.warnings.len());
    println!("{}", "═".repeat(60));
}

fn print_json_report(report: &ValidationReport) {
    let output = json!({
        "passed": report.passed,
        "errors": report.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
        "warnings": report.warnings.iter().map(|w| w.to_string()).collect::<Vec<_>>(),
        "summary": {
            "error_count": report.errors.len(),
            "warning_count": report.warnings.len(),
        }
    });

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

pub fn print_success(message: &str) {
    println!("{} {}", "✓".green().bold(), message.green());
}

#[allow(dead_code)]
pub fn print_error(message: &str) {
    eprintln!("{} {}", "✗".red().bold(), message.red());
}

pub fn print_info(message: &str) {
    println!("{} {}", "ℹ".blue().bold(), message);
}
