# Data Contracts Engine (DCE)

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--release-orange.svg)](https://github.com/AndreaBozzo/dce)
[![Phase](https://img.shields.io/badge/phase-1%20foundation-blue.svg)](#roadmap)

> **Note**: This project is in active development. v0.0.1 will be released upon Phase 1 completion.
> **Currently Available**: Core types, YAML/TOML parsing, validation engine, programmatic contract building
> **Coming in v0.0.1**: CLI commands, Iceberg integration

A high-performance, Rust-native data contracts engine for modern data platforms. Define, validate, and enforce data quality contracts across multiple formats and cloud providers.

## Origin Story

DCE evolves from [dataprof](https://github.com/AndreaBozzo/dataprof), a fast data quality assessment tool built in Rust. While dataprof excels at profiling and analyzing existing datasets (completeness, consistency, uniqueness, accuracy, timeliness), DCE shifts the paradigm toward **proactive data contracts**—defining, validating, and enforcing quality expectations *before* data enters your platform. Think of dataprof as the diagnostic tool and DCE as the preventive framework that ensures data quality from the outset.

### From Assessment to Contracts: The Evolution

| Aspect | dataprof | DCE |
|--------|----------|-----|
| **Purpose** | Analyze & profile existing data | Define & enforce data contracts |
| **Approach** | Reactive (diagnose issues) | Proactive (prevent issues) |
| **Use Case** | "What's wrong with my data?" | "Does my data meet the contract?" |
| **Output** | Quality reports & visualizations | Validation pass/fail with detailed errors |
| **Integration** | CLI, Python bindings, GitHub Actions | Rust SDK, CLI (planned), multiple formats |
| **Focus** | Data assessment & profiling | Contract validation & enforcement |

Both tools share Rust's performance benefits (memory efficiency, speed, safety), but serve complementary roles in a data quality strategy: dataprof helps you understand your data, DCE helps you control it.

## Overview

Data Contracts Engine provides a universal framework for defining and validating data contracts, ensuring data quality and compliance across your entire data platform. Unlike vendor-specific solutions, DCE is cloud-agnostic and supports multiple table formats.

### What's Available Now (v0.0.1-dev)

DCE currently provides a complete validation framework with the following capabilities:

**Core Features:**
- **Type-Safe Contracts**: Rust-native data structures with full serde support
- **YAML/TOML Parsing**: Load contracts from configuration files
- **Builder Pattern API**: Ergonomic programmatic contract creation
- **Validation Engine**: Schema, constraint, and quality checks validation
- **Multiple Validation Modes**: Strict, non-strict, and schema-only validation
- **Comprehensive Types**: Schema definitions, quality checks, SLA specifications, field constraints
- **Serialization**: Export contracts to YAML/JSON for storage and versioning

**Iceberg Integration (Complete!):**
- **Full Catalog Support**: REST, AWS Glue, Hive Metastore catalogs
- **Schema & Data Validation**: Extract schemas and validate data from Iceberg tables
- **Type System**: Complete support for all Iceberg primitive types (dates, decimals, timestamps)
- See [contracts_iceberg/README.md](contracts_iceberg/README.md) for detailed documentation

**Quality Assurance:**
- **Well-Tested**: 138+ tests covering core functionality, parsing, validation, and Iceberg
- **Fully Documented**: Complete rustdoc with examples for all catalog types
- **Production Ready**: Clean compilation, comprehensive error handling

**You can currently:**
1. Define contracts programmatically using the builder pattern API
2. Parse YAML/TOML contract files into type-safe Rust structures
3. Validate data against contracts (schema, constraints, quality checks)
4. **Connect to Iceberg catalogs and validate tables** ([see Iceberg docs](contracts_iceberg/README.md))
5. Serialize contracts back to YAML/JSON for storage
6. Inspect contract metadata (schema, fields, quality checks, SLA)
7. Run validation in multiple modes (strict, non-strict, schema-only)

See [examples/contracts/user_events.yml](examples/contracts/user_events.yml) for a complete working example.

### Coming in v0.0.1 Release

- **CLI Tool**: `dce validate`, `dce init`, `dce check` commands
- **Integration Tests**: End-to-end workflow testing
- **Documentation Polish**: Final review and examples

### Future Roadmap (Post v0.0.1)

- **Multi-Format Support**: Delta Lake, Hudi, Parquet
- **Python SDK**: PyO3-based bindings
- **Git Integration**: Pre-commit hooks, GitHub Actions
- **Advanced Features**: ML-powered drift detection, auto-generation

## Architecture

```
dce/
├── contracts_core      # ✅ Core data structures and types (COMPLETE)
├── contracts_parser    # ✅ YAML/TOML contract parsing (COMPLETE)
├── contracts_validator # ✅ Validation engine (COMPLETE)
├── contracts_iceberg   # ✅ Apache Iceberg integration (COMPLETE - 100%)
├── contracts_cli       # ⏳ Command-line interface (PLANNED)
└── contracts_sdk       # ⏳ Public Rust SDK (PLANNED)
```

## Quick Start

### Current Usage (Development)

```bash
# Clone and build
git clone https://github.com/AndreaBozzo/dce
cd dce
cargo build --workspace

# Run tests to see it in action
cargo test --workspace

# Generate documentation
cargo doc --open --no-deps
```

### Define a Contract

Create a contract file `user_events.yml`:

```yaml
version: "1.0.0"
name: user_events
owner: analytics-team
description: User interaction events dataset

schema:
  format: iceberg
  location: s3://data/user_events
  fields:
    - name: user_id
      type: string
      nullable: false
      description: Unique user identifier

    - name: event_type
      type: string
      nullable: false
      constraints:
        - type: allowedvalues
          values: [click, view, purchase]

    - name: timestamp
      type: timestamp
      nullable: false

quality_checks:
  completeness:
    threshold: 0.99
    fields: [user_id, event_type, timestamp]

  freshness:
    max_delay: 1h
    metric: timestamp

sla:
  availability: 0.999
  response_time: 100ms
```

### Using Contracts (Available Now)

```rust
// Add to Cargo.toml (development path dependencies):
// contracts_parser = { path = "path/to/dce/contracts_parser" }
// contracts_validator = { path = "path/to/dce/contracts_validator" }
// contracts_core = { path = "path/to/dce/contracts_core" }
//
// Note: Will be available from crates.io after v0.0.1 release

use contracts_parser::parse_file;
use contracts_validator::{DataValidator, DataSet, DataValue};
use contracts_core::ValidationContext;
use std::path::Path;
use std::collections::HashMap;

// Load and inspect a contract
let contract = parse_file(Path::new("user_events.yml"))?;
println!("Contract: {} v{}", contract.name, contract.version);
println!("Owner: {}", contract.owner);
println!("Fields: {}", contract.schema.fields.len());

// Validate data against the contract
let mut validator = DataValidator::new();
let mut row = HashMap::new();
row.insert("user_id".to_string(), DataValue::String("user123".to_string()));
row.insert("event_type".to_string(), DataValue::String("click".to_string()));

let dataset = DataSet::from_rows(vec![row]);
let context = ValidationContext::new();
let report = validator.validate_with_data(&contract, &dataset, &context);

if report.passed {
    println!("✓ Validation passed!");
} else {
    for error in &report.errors {
        eprintln!("✗ {}", error);
    }
}
```

### CLI Commands (Coming in v0.0.1)

The following commands are planned for the initial release:

```bash
# Validate contract against actual data
dce validate user_events.yml

# Schema-only validation
dce validate --schema-only user_events.yml

# Generate contract from existing Iceberg table
dce init --from-iceberg s3://data/user_events

# Compare two contract versions
dce diff old.yml new.yml

# Check contract compatibility
dce check user_events.yml
```

### Rust SDK (Available Now)

```rust
use contracts_core::{ContractBuilder, DataFormat, FieldBuilder, FieldConstraints};
use contracts_parser::parse_file;
use std::path::Path;

// 1. Parse a contract from YAML/TOML
let contract = parse_file(Path::new("examples/contracts/user_events.yml"))
    .expect("Failed to parse contract");

println!("Loaded: {} v{} (owner: {})",
    contract.name, contract.version, contract.owner);
println!("Schema: {} fields at {}",
    contract.schema.fields.len(), contract.schema.location);

// Access quality checks
if let Some(qc) = &contract.quality_checks {
    if let Some(c) = &qc.completeness {
        println!("Completeness threshold: {}", c.threshold);
    }
}

// 2. Build a contract programmatically
let contract = ContractBuilder::new("user_events", "analytics-team")
    .version("1.0.0")
    .description("User interaction events")
    .location("s3://data/user_events")
    .format(DataFormat::Iceberg)
    .field(
        FieldBuilder::new("user_id", "string")
            .nullable(false)
            .description("Unique user identifier")
            .tags(vec!["pii".to_string(), "primary_key".to_string()])
            .build()
    )
    .field(
        FieldBuilder::new("event_type", "string")
            .nullable(false)
            .constraint(FieldConstraints::AllowedValues {
                values: vec!["click".to_string(), "view".to_string()],
            })
            .build()
    )
    .build();

// 3. Serialize to YAML for storage/versioning
let yaml = serde_yaml_ng::to_string(&contract).unwrap();
println!("{}", yaml);

// 4. Serialize to JSON for APIs
let json = serde_json::to_string_pretty(&contract).unwrap();
println!("{}", json);
```

**Note**: Data validation is now available! The validator supports schema validation, constraint checking (allowed values, ranges, patterns), and quality checks (completeness, uniqueness, freshness).

### Using Iceberg Integration

```rust
use contracts_iceberg::{IcebergValidator, IcebergConfig};
use contracts_core::ValidationContext;

// Configure connection to Iceberg catalog
let config = IcebergConfig::builder()
    .rest_catalog("http://localhost:8181", "/warehouse")
    .namespace(vec!["database".to_string()])
    .table_name("events")
    .build()?;

// Create validator and validate table
let validator = IcebergValidator::new(config).await?;
let contract = parse_file(Path::new("my_contract.yml"))?;
let context = ValidationContext::default();

let report = validator.validate_table(&contract, &context).await?;
if report.valid {
    println!("✓ Validation passed!");
}
```

**For complete Iceberg documentation**, including:
- All catalog types (REST, AWS Glue, Hive Metastore)
- Configuration examples
- Supported data types
- Known limitations

See **[contracts_iceberg/README.md](contracts_iceberg/README.md)**

## Roadmap

### Phase 1: Foundation (75% Complete)
- [x] Core data structures and types
- [x] Workspace setup and architecture
- [x] Builder patterns and validators
- [x] YAML/TOML parser implementation
- [x] Comprehensive test suite (138+ tests)
- [x] Generic validation engine (schema, constraints, quality checks)
- [x] Iceberg type conversion and schema extraction
- [x] Iceberg configuration and error handling
- [x] Iceberg catalog integration (REST, Glue, HMS, FileIO)
- [x] Iceberg data reading and validation
- [ ] CLI basic commands (`validate`, `init`, `check`)
- [ ] Integration tests with local/mock Iceberg tables
- [ ] Documentation & Polish
- [ ] Release 0.0.1

### Phase 2: Multi-Format
- [ ] Delta Lake support
- [ ] Apache Hudi support
- [ ] Python SDK
- [ ] GitHub Actions integration
- [ ] Pre-commit hooks

### Phase 3: Ecosystem
- [ ] Great Expectations adapter
- [ ] dbt integration
- [ ] Apache Airflow operator
- [ ] Spark connector

### Phase 4: Intelligence
- [ ] Auto-contract generation from data
- [ ] ML-powered drift detection
- [ ] Cost optimization recommendations
- [ ] Contract registry service

## Contributing

We welcome contributions! Check our [issue tracker](https://github.com/AndreaBozzo/dce/issues) for open tasks and enhancement opportunities.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/AndreaBozzo/dce
cd dce

# Build the workspace
cargo build --workspace

# Run tests
cargo test --workspace

# Run clippy
cargo clippy --workspace

# Generate documentation
cargo doc --workspace --no-deps --open
```

## Why DCE?

**Note**: DCE is in early development. For production use today, consider mature alternatives like [Great Expectations](https://greatexpectations.io/), [Soda Core](https://www.soda.io/), or [dbt tests](https://docs.getdbt.com/docs/build/tests). DCE aims to differentiate through:

### vs. Python-Based Tools (Great Expectations, Soda)
- **Performance**: Rust-native for high-throughput validation with minimal overhead
- **Embeddable**: No Python runtime needed, can embed in Rust data pipelines
- **Memory Efficient**: Suitable for resource-constrained environments (building on dataprof's 20x memory efficiency)
- **Single Binary**: Zero dependencies deployment

### vs. SQL-Based Tools (dbt tests)
- **Pre-Ingestion**: Validate before data enters your warehouse
- **Cloud-Agnostic**: Not tied to warehouse execution
- **Format-Aware**: Native integration with Iceberg, Delta, Hudi table formats
- **Schema Evolution**: Track contract changes alongside data changes

### vs. Vendor Solutions
- **Open Source**: Full transparency, community-driven development
- **No Lock-in**: Works with any cloud provider or on-premises
- **Git-Native**: Version contracts alongside code
- **Extensible**: Plugin architecture for custom validators

**Current Limitation**: DCE is not yet feature-complete. v0.0.1 will offer schema and constraint validation with Iceberg support, with advanced features (multi-format support, ML-powered detection) following in subsequent releases.

## License

This project is dual-licensed under:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

You may choose either license for your use.

## Community

- [GitHub Discussions](https://github.com/AndreaBozzo/dce/discussions)
- [Issue Tracker](https://github.com/AndreaBozzo/dce/issues)
- [Contributing Guide](CONTRIBUTING.md) (coming soon)

## Acknowledgments

Built with:
- [Apache Iceberg Rust](https://github.com/apache/iceberg-rust)
- [Serde](https://serde.rs/)
- [Clap](https://clap.rs/)

---

## Development Status

**Current Phase**: Phase 1 - Foundation (75% complete)

**Component Status**:
- ✅ **Complete**: contracts_core, contracts_parser, contracts_validator
- ✅ **Complete**: contracts_iceberg (100% - full catalog, data reading, validation)
- ⏳ **Next Up**: contracts_cli (command-line interface)
- ⏸️ **Planned**: contracts_sdk (public API wrapper)

**Latest Updates**:
- **November 6, 2025**: 🎉 Iceberg integration complete! Full catalog support, type conversion (dates/decimals/timestamps), 43 tests. See [contracts_iceberg/README.md](contracts_iceberg/README.md)
- **November 4, 2025**: Validation engine complete with comprehensive test suite (109 tests)
- **November 1, 2025**: Parser implementation complete (YAML/TOML support)
- **October 31, 2025**: Core data structures and workspace architecture established

**What's Next for v0.0.1**:
1. ~~Complete Iceberg integration~~ ✅ **DONE**
2. CLI implementation with `validate`, `init`, and `check` commands
3. Integration tests with local/mock Iceberg tables
4. Usage examples for different catalog types
5. First public release

**Contributing**: We welcome contributors! Check our [issue tracker](https://github.com/AndreaBozzo/dce/issues) for opportunities to contribute.

For questions or feedback, please [open an issue](https://github.com/AndreaBozzo/dce/issues/new).
