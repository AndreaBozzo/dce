use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

/// Helper to get the path to test fixtures
fn fixture_path(name: &str) -> String {
    format!("tests/fixtures/{}", name)
}

/// Helper to create a Command for the dce binary
// TODO: Migrate to cargo::cargo_bin_cmd! macro when available
// See: https://github.com/assert-rs/assert_cmd/issues/139
#[allow(deprecated)]
fn dce() -> Command {
    Command::cargo_bin("dce").expect("Failed to find dce binary")
}

// ============================================================================
// check command tests
// ============================================================================

#[test]
fn test_check_valid_contract() {
    dce()
        .arg("check")
        .arg(fixture_path("simple_contract.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("simple_test"))
        .stdout(predicate::str::contains("test-team"))
        .stdout(predicate::str::contains("Iceberg"));
}

#[test]
fn test_check_contract_with_quality() {
    dce()
        .arg("check")
        .arg(fixture_path("contract_with_quality.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("quality_test"))
        .stdout(predicate::str::contains("Quality Checks"))
        .stdout(predicate::str::contains("completeness"))
        .stdout(predicate::str::contains("uniqueness"))
        .stdout(predicate::str::contains("freshness"))
        .stdout(predicate::str::contains("SLA"));
}

#[test]
fn test_check_toml_contract() {
    dce()
        .arg("check")
        .arg(fixture_path("contract.toml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("toml_test"))
        .stdout(predicate::str::contains("Iceberg"));
}

#[test]
fn test_check_invalid_contract() {
    dce()
        .arg("check")
        .arg(fixture_path("invalid_contract.yml"))
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_check_missing_file() {
    dce()
        .arg("check")
        .arg("nonexistent.yml")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_check_contract_schema_details() {
    dce()
        .arg("check")
        .arg(fixture_path("simple_contract.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("Fields"))
        .stdout(predicate::str::contains("2")); // 2 fields
}

// ============================================================================
// validate command tests (schema-only mode)
// ============================================================================

#[test]
fn test_validate_schema_only_mode() {
    // Use Parquet format to avoid Iceberg catalog requirement
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("validation").or(predicate::str::contains("Validation")))
        .stdout(predicate::str::contains("passed").or(predicate::str::contains("PASSED")));
}

#[test]
fn test_validate_schema_only_with_quality_checks() {
    // Note: Iceberg format requires catalog even for schema-only (known limitation)
    // Use Parquet instead
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success();
}

#[test]
fn test_validate_invalid_contract() {
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg(fixture_path("invalid_contract.yml"))
        .assert()
        .failure();
}

#[test]
fn test_validate_missing_file() {
    dce()
        .arg("validate")
        .arg("nonexistent.yml")
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_validate_json_output() {
    let output = dce()
        .arg("validate")
        .arg("--schema-only")
        .arg("--format")
        .arg("json")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let output_str = String::from_utf8_lossy(&output);

    // Output may have logs before JSON, extract the JSON part
    let json_start = output_str.find('{').expect("Should contain JSON object");
    let json_part = &output_str[json_start..];

    // Should be valid JSON
    assert!(
        serde_json::from_str::<serde_json::Value>(json_part).is_ok(),
        "Output should be valid JSON: {}",
        json_part
    );
}

#[test]
fn test_validate_text_output_default() {
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("validation").or(predicate::str::contains("Validation")));
}

#[test]
fn test_validate_with_sample_size() {
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg("--sample-size")
        .arg("5000")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success();
}

#[test]
fn test_validate_strict_mode() {
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg("--strict")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success();
}

// ============================================================================
// init command tests
// ============================================================================

#[test]
fn test_init_missing_catalog_uri() {
    dce()
        .arg("init")
        .arg("--catalog")
        .arg("rest")
        .arg("--namespace")
        .arg("test")
        .arg("--table")
        .arg("events")
        .assert()
        .failure()
        .stderr(predicate::str::contains("catalog").or(predicate::str::contains("required")));
}

#[test]
fn test_init_help() {
    dce()
        .arg("init")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("catalog"))
        .stdout(predicate::str::contains("namespace"))
        .stdout(predicate::str::contains("table"));
}

#[test]
fn test_init_with_output_file() {
    let temp_dir = TempDir::new().unwrap();
    let output_path = temp_dir.path().join("generated_contract.yml");

    // This test will fail without a real catalog, but we can verify the CLI parses args correctly
    // We expect it to fail trying to connect, not on argument parsing
    let result = dce()
        .arg("init")
        .arg("http://fake-catalog:8181")
        .arg("--catalog")
        .arg("rest")
        .arg("--namespace")
        .arg("test_ns")
        .arg("--table")
        .arg("test_table")
        .arg("--owner")
        .arg("test-team")
        .arg("--output")
        .arg(output_path.to_str().unwrap())
        .assert()
        .failure(); // Will fail due to connection, but that's expected

    // Verify it failed on connection, not argument parsing
    let stderr = String::from_utf8_lossy(&result.get_output().stderr);
    assert!(
        !stderr.contains("required") && !stderr.contains("invalid argument"),
        "Should fail on connection, not argument parsing"
    );
}

#[test]
fn test_init_with_description() {
    let result = dce()
        .arg("init")
        .arg("http://fake-catalog:8181")
        .arg("--catalog")
        .arg("rest")
        .arg("--namespace")
        .arg("analytics")
        .arg("--table")
        .arg("events")
        .arg("--description")
        .arg("Test description")
        .assert()
        .failure(); // Will fail on connection

    let stderr = String::from_utf8_lossy(&result.get_output().stderr);
    assert!(
        !stderr.contains("required") && !stderr.contains("invalid argument"),
        "Should fail on connection, not argument parsing"
    );
}

#[test]
fn test_init_glue_catalog() {
    let result = dce()
        .arg("init")
        .arg("arn:aws:glue:us-east-1:123456789:database/test")
        .arg("--catalog")
        .arg("glue")
        .arg("--namespace")
        .arg("test_db")
        .arg("--table")
        .arg("test_table")
        .assert()
        .failure(); // Will fail on AWS connection

    let stderr = String::from_utf8_lossy(&result.get_output().stderr);
    assert!(
        !stderr.contains("required") && !stderr.contains("invalid argument"),
        "Should fail on AWS connection, not argument parsing"
    );
}

// ============================================================================
// General CLI tests
// ============================================================================

#[test]
fn test_cli_help() {
    dce()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("validate"))
        .stdout(predicate::str::contains("check"))
        .stdout(predicate::str::contains("init"));
}

#[test]
fn test_cli_version() {
    dce()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_VERSION")));
}

#[test]
fn test_validate_help() {
    dce()
        .arg("validate")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("schema-only"))
        .stdout(predicate::str::contains("strict"))
        .stdout(predicate::str::contains("sample-size"))
        .stdout(predicate::str::contains("format"));
}

#[test]
fn test_check_help() {
    dce()
        .arg("check")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("contract"));
}

// ============================================================================
// Edge cases and error handling
// ============================================================================

#[test]
fn test_validate_with_invalid_sample_size() {
    dce()
        .arg("validate")
        .arg("--sample-size")
        .arg("invalid")
        .arg(fixture_path("simple_contract.yml"))
        .assert()
        .failure()
        .stderr(predicate::str::contains("invalid").or(predicate::str::contains("error")));
}

#[test]
fn test_validate_with_invalid_format() {
    dce()
        .arg("validate")
        .arg("--format")
        .arg("invalid_format")
        .arg(fixture_path("simple_contract.yml"))
        .assert()
        .failure();
}

#[test]
fn test_validate_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let empty_file = temp_dir.path().join("empty.yml");
    fs::write(&empty_file, "").unwrap();

    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg(empty_file.to_str().unwrap())
        .assert()
        .failure()
        .stderr(predicate::str::contains("Error"));
}

#[test]
fn test_check_contract_field_constraints() {
    dce()
        .arg("check")
        .arg(fixture_path("contract_with_quality.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("3")); // 3 fields
}

#[test]
fn test_validate_all_output_modes() {
    // Test text format
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg("--format")
        .arg("text")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success();

    // Test json format
    dce()
        .arg("validate")
        .arg("--schema-only")
        .arg("--format")
        .arg("json")
        .arg(fixture_path("parquet_contract.yml"))
        .assert()
        .success();
}

#[test]
fn test_check_displays_location() {
    dce()
        .arg("check")
        .arg(fixture_path("simple_contract.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("s3://test/simple"));
}

#[test]
fn test_multiple_field_types() {
    dce()
        .arg("check")
        .arg(fixture_path("contract_with_quality.yml"))
        .assert()
        .success()
        .stdout(predicate::str::contains("quality_test"))
        .stdout(predicate::str::contains("Fields"))
        .stdout(predicate::str::contains("3")); // 3 fields
}
