# Data Contracts Engine (DCE)

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--release-orange.svg)](https://github.com/yourusername/dce)
[![Phase](https://img.shields.io/badge/phase-1%20foundation-blue.svg)](#roadmap)

> **Note**: This project is in active development. v0.0.1 will be released upon Phase 1 completion.
> **Currently Available**: Core types, YAML/TOML parsing, programmatic contract building
> **Coming in v0.0.1**: Validation engine, CLI commands, Iceberg integration

A high-performance, Rust-native data contracts engine for modern data platforms. Define, validate, and enforce data quality contracts across multiple formats and cloud providers.

## Overview

Data Contracts Engine provides a universal framework for defining and validating data contracts, ensuring data quality and compliance across your entire data platform. Unlike vendor-specific solutions, DCE is cloud-agnostic and supports multiple table formats.

### Available Now (v0.0.1-dev)

- **Type-Safe Contracts**: Rust-native data structures with full serde support
- **YAML/TOML Parsing**: Load contracts from configuration files
- **Builder Pattern API**: Ergonomic programmatic contract creation
- **Comprehensive Types**: Schema, quality checks, SLA, field constraints
- **Well-Tested**: 42 tests covering core functionality and parsing
- **Fully Documented**: Complete rustdoc with examples

### Planned for v0.0.1 Release

- **Schema Validation**: Verify data against contract definitions
- **CLI Tool**: `dce validate`, `dce init`, `dce check` commands
- **Iceberg Integration**: Validate against Apache Iceberg tables
- **Quality Checks**: Completeness, uniqueness, freshness validation
- **Integration Tests**: End-to-end workflow testing

### Future Roadmap (Post v0.0.1)

- **Multi-Format Support**: Delta Lake, Hudi, Parquet
- **Python SDK**: PyO3-based bindings
- **Git Integration**: Pre-commit hooks, GitHub Actions
- **Advanced Features**: ML-powered drift detection, auto-generation

## What Works Right Now

You can currently use DCE to:

1. **Define contracts programmatically** using the builder pattern API
2. **Parse YAML/TOML** contract files into type-safe Rust structures
3. **Serialize contracts** back to YAML/JSON for storage
4. **Inspect contract metadata** (schema, fields, quality checks, SLA)
5. **Validate contract syntax** (via parsing)

See [examples/contracts/user_events.yml](examples/contracts/user_events.yml) for a complete working example.

## Architecture

```
dce/
├── contracts_core      # ✅ Core data structures and types (COMPLETE)
├── contracts_parser    # ✅ YAML/TOML contract parsing (COMPLETE)
├── contracts_validator # ⏳ Validation engine (IN PROGRESS)
├── contracts_iceberg   # ⏳ Apache Iceberg integration (PLANNED)
├── contracts_cli       # ⏳ Command-line interface (PLANNED)
└── contracts_sdk       # ⏳ Public Rust SDK (PLANNED)
```

## Quick Start

### Current Usage (Development)

```bash
# Clone and build
git clone https://github.com/yourusername/dce
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
// Add to Cargo.toml:
// contracts_parser = { path = "path/to/dce/contracts_parser" }
// contracts_core = { path = "path/to/dce/contracts_core" }

use contracts_parser::parse_file;
use std::path::Path;

// Load and inspect a contract
let contract = parse_file(Path::new("user_events.yml"))?;
println!("Contract: {} v{}", contract.name, contract.version);
println!("Owner: {}", contract.owner);
println!("Fields: {}", contract.schema.fields.len());
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

**Note**: Actual data validation will be available in v0.0.1. Currently, you can only parse, build, and serialize contracts.

## Roadmap

### Phase 1: Foundation
- [x] Core data structures and types
- [x] Workspace setup and architecture
- [x] Builder patterns and validators
- [x] YAML/TOML parser implementation
- [x] Comprehensive test suite (core + parser)
- [ ] Apache Iceberg validator
- [ ] CLI basic commands

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

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/dce
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
- **Performance**: Rust-native for high-throughput validation (target: 10-100x faster)
- **Embeddable**: No Python runtime needed, can embed in Rust data pipelines
- **Memory Efficient**: Suitable for resource-constrained environments
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

**Current Limitation**: DCE is not yet feature-complete. v0.0.1 will offer basic schema validation, with advanced features following in subsequent releases.

## License

This project is dual-licensed under:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
- Apache License 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)

You may choose either license for your use.

## Community

- [GitHub Discussions](https://github.com/yourusername/dce/discussions)
- [Issue Tracker](https://github.com/yourusername/dce/issues)
- [Contributing Guide](CONTRIBUTING.md)

## Acknowledgments

Built with:
- [Apache Iceberg Rust](https://github.com/apache/iceberg-rust)
- [Serde](https://serde.rs/)
- [Clap](https://clap.rs/)

---

## Development Status

**Current Phase**: Phase 1 - Foundation

**Progress Overview**:
- ✅ **Complete** (2/5): contracts_core, contracts_parser
- ⏳ **In Progress** (0/5): contracts_validator (next up)
- ⏸️ **Planned** (3/5): contracts_iceberg, contracts_cli, contracts_sdk

**Phase 1 Completion**: ~40% (2/5 core components)

**Target for v0.0.1**:
- Validation engine with schema and constraint checking
- Basic CLI with `validate` command
- Iceberg table format support
- End-to-end integration tests

**Latest Updates**:
- 2025-01: Parser implementation complete (YAML/TOML support)
- 2025-01: Comprehensive test suite (42 tests, 100% passing)
- 2025-01: Core data structures fully tested and documented

**Contributing**: We welcome contributors! The validator implementation is the next critical milestone. See open issues for details.

For questions or feedback, please [open an issue](https://github.com/yourusername/dce/issues/new).
