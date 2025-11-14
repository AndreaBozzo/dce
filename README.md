# Data Contracts Engine (DCE)

[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/status-v0.0.1--pre-orange.svg)](https://github.com/AndreaBozzo/dce)

A high-performance, Rust-native data contracts engine for modern data platforms. Define, validate, and enforce data quality contracts across multiple table formats.

**Origin**: DCE evolves from [dataprof](https://github.com/AndreaBozzo/dataprof), shifting from reactive profiling to proactive contract enforcement.

## Quick Start

```bash
# Install
git clone https://github.com/AndreaBozzo/dce
cd dce
cargo build --release
export PATH=$PATH:$(pwd)/target/release

# Validate contract
export REST_CATALOG_URI=http://localhost:8181
export WAREHOUSE=s3://my-warehouse
dce validate contract.yml

# Generate contract from table
dce init http://localhost:8181 --catalog rest --namespace db --table events -o contract.yml

# Check syntax
dce check contract.yml
```

## Contract Example

```yaml
version: "1.0.0"
name: user_events
owner: analytics-team

schema:
  format: iceberg
  location: s3://data/user_events
  fields:
    - name: user_id
      type: string
      nullable: false
    - name: event_type
      type: string
      constraints:
        - type: allowedvalues
          values: [click, view, purchase]

quality_checks:
  completeness:
    threshold: 0.99
    fields: [user_id, event_type]
  freshness:
    max_delay: 1h
    metric: timestamp
```

Full example: [examples/contracts/user_events.yml](examples/contracts/user_events.yml)

## CLI Reference

### validate
```bash
dce validate contract.yml                    # Full validation
dce validate --schema-only contract.yml      # Fast, no data
dce validate --sample-size 10000 contract.yml
dce validate --strict contract.yml           # Warnings = errors
dce validate --format json contract.yml      # CI/CD
```

### init
```bash
dce init <catalog-uri> \
  --catalog rest \
  --namespace analytics \
  --table events \
  --owner data-team \
  --output contract.yml
```

### check
```bash
dce check contract.yml  # Syntax validation
```

**Environment Variables**:
- `REST_CATALOG_URI` / `ICEBERG_REST_URI`: Catalog endpoint
- `WAREHOUSE` / `ICEBERG_WAREHOUSE`: Warehouse location

## Programmatic Usage

```rust
use contracts_parser::parse_file;
use contracts_validator::{DataValidator, DataSet, ValidationContext};

let contract = parse_file("contract.yml")?;
let validator = DataValidator::new();
let dataset = DataSet::from_rows(vec![/* data */]);
let context = ValidationContext::new();

let report = validator.validate_with_data(&contract, &dataset, &context);
if !report.passed {
    for error in report.errors {
        eprintln!("✗ {}", error);
    }
}
```

Iceberg: see [contracts_iceberg/README.md](contracts_iceberg/README.md)

## Features

- **Formats**: Apache Iceberg (REST, Glue, HMS catalogs)
- **Validation**: Schema, constraints, quality checks, custom SQL
- **Types**: All primitives + complex (struct, list, map)
- **Modes**: Full data, schema-only, strict, sampled
- **Output**: Text, JSON

## Roadmap

### Phase 1: Foundation (85% Complete)
- [x] Core data structures and types
- [x] YAML/TOML parser
- [x] Validation engine
- [x] Iceberg integration (REST, Glue, HMS)
- [x] CLI commands (validate, init, check)
- [ ] Integration tests
- [ ] v0.0.1 release

### Phase 2: Multi-Format
- [ ] Delta Lake support
- [ ] Apache Hudi support
- [ ] Parquet/CSV validation
- [ ] Python SDK
- [ ] GitHub Actions integration

### Phase 3: Ecosystem
- [ ] Great Expectations adapter
- [ ] dbt integration
- [ ] Apache Airflow operator

### Phase 4: Intelligence
- [ ] Auto-contract generation
- [ ] ML-powered drift detection
- [ ] Contract registry service

## Development

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace
cargo doc --workspace --no-deps --open
```

## Why DCE?

**Performance**: Rust-native for high throughput, ~20x memory efficiency vs Python
**Embeddable**: Single binary, no runtime dependencies
**Cloud-Agnostic**: Works anywhere, not tied to specific platforms
**Format-Aware**: Native Iceberg/Delta/Hudi support

**Note**: Early stage. For production today, see [Great Expectations](https://greatexpectations.io/) or [Soda](https://www.soda.io/).

## License

Dual-licensed: MIT or Apache 2.0

## Links

- [GitHub](https://github.com/AndreaBozzo/dce)
- [Issues](https://github.com/AndreaBozzo/dce/issues)
- [Iceberg Docs](contracts_iceberg/README.md)
