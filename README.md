# Data Contracts Engine (DCE)

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)

A high-performance, Rust-native data contracts engine for modern data platforms. Define, validate, and enforce data quality contracts across multiple formats and cloud providers.

## Overview

Data Contracts Engine provides a universal framework for defining and validating data contracts, ensuring data quality and compliance across your entire data platform. Unlike vendor-specific solutions, DCE is cloud-agnostic and supports multiple table formats.

### Key Features

- **🚀 Performance**: Rust-native implementation for maximum speed and efficiency
- **🔄 Multi-Format Support**: Apache Iceberg, Delta Lake, Hudi, Parquet, and more
- **☁️ Cloud-Agnostic**: Works with any cloud provider or on-premises deployment
- **✅ Comprehensive Validation**: Schema, quality checks, freshness, and SLA enforcement
- **🔧 Developer-Friendly**: Git-native workflow with CLI and SDK
- **📦 Zero Dependencies**: Single binary deployment, no JVM required
- **🔌 Extensible**: Plugin architecture for custom validators

## Architecture

```
dce/
├── contracts_core      # Core data structures and types
├── contracts_parser    # YAML/TOML contract parsing
├── contracts_validator # Validation engine and traits
├── contracts_iceberg   # Apache Iceberg integration
├── contracts_cli       # Command-line interface (dce)
└── contracts_sdk       # Public Rust SDK
```

## Quick Start

### Installation

```bash
# From crates.io
cargo install contracts_cli

# Or build from source
git clone https://github.com/yourusername/dce
cd dce
cargo build --release
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

### Validate a Contract

```bash
# Validate contract against actual data
dce validate user_events.yml

# Schema-only validation
dce validate --schema-only user_events.yml

# Generate contract from existing data
dce init --from-iceberg s3://data/user_events
```

## Usage

### CLI Commands

```bash
# Initialize a new contract
dce init <name>

# Validate a contract
dce validate <contract.yml>

# Compare two contract versions
dce diff <old.yml> <new.yml>

# Check contract compatibility
dce check <contract.yml>
```

### Rust SDK

```rust
use contracts_core::{ContractBuilder, DataFormat, FieldBuilder};
use contracts_parser::parse_file;
use std::path::Path;

// Parse a contract from YAML/TOML
let contract = parse_file(Path::new("user_events.yml"))
    .expect("Failed to parse contract");

println!("Loaded contract: {} (v{})", contract.name, contract.version);
println!("Schema has {} fields", contract.schema.fields.len());

// Or build a contract programmatically
let contract = ContractBuilder::new("user_events", "analytics-team")
    .version("1.0.0")
    .location("s3://data/user_events")
    .format(DataFormat::Iceberg)
    .field(
        FieldBuilder::new("user_id", "string")
            .nullable(false)
            .description("Unique user identifier")
            .build()
    )
    .build();

// Serialize to YAML
let yaml = serde_yaml::to_string(&contract).unwrap();
println!("{}", yaml);
```

## Roadmap

### Phase 1: Foundation (Months 1-3)
- [x] Core data structures and types
- [x] Workspace setup and architecture
- [x] Builder patterns and validators
- [x] YAML/TOML parser implementation
- [x] Comprehensive test suite (core + parser)
- [ ] Apache Iceberg validator
- [ ] CLI basic commands

### Phase 2: Multi-Format (Months 4-6)
- [ ] Delta Lake support
- [ ] Apache Hudi support
- [ ] Python SDK
- [ ] GitHub Actions integration
- [ ] Pre-commit hooks

### Phase 3: Ecosystem (Months 7-9)
- [ ] Great Expectations adapter
- [ ] dbt integration
- [ ] Apache Airflow operator
- [ ] Spark connector

### Phase 4: Intelligence (Months 10-12)
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

### vs. Vendor-Specific Solutions
- **No Lock-in**: Works with any cloud provider
- **Open Source**: Full transparency and community-driven
- **Lightweight**: No heavy infrastructure requirements

### vs. Traditional Data Quality Tools
- **Proactive**: Enforce contracts before problems occur
- **Git-Native**: Version control for data contracts
- **Developer-First**: CLI and SDK for automation

### vs. Manual Processes
- **Automated**: Continuous validation in CI/CD
- **Consistent**: Single source of truth for data contracts
- **Scalable**: Handles millions of records efficiently

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

**Status**: Active Development - Phase 1 (Foundation - 60% Complete)

**Latest Updates**:
- Parser implementation complete (YAML/TOML support)
- Comprehensive test suite (42 tests across core and parser)
- Core data structures fully tested and validated

For questions or feedback, please [open an issue](https://github.com/yourusername/dce/issues/new).
