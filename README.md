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

- **Formats**: Apache Iceberg (REST, Glue, HMS catalogs) - *full data validation*
  - Parquet, CSV, Delta, Hudi - *schema-only validation (full support in v0.1.0)*
- **Validation**: Schema, constraints, quality checks, custom SQL
- **Types**: All primitives + complex (struct, list, map)
- **Modes**: Full data, schema-only, strict, sampled
- **Output**: Text, JSON

## Known Limitations (v0.0.1)

### Data Validation
- **Iceberg Only**: Full data validation currently works only for Apache Iceberg format
- Other formats (Parquet, CSV, Delta, Hudi) fall back to schema-only validation
- Multi-format support planned for v0.1.0

### Type Support
- **Complex Types**: Nested structures (Struct, List, Map) are recognized but lose detailed type information during validation
- All primitive types (string, int, long, float, double, decimal, boolean, date, time, timestamp, uuid, binary) are fully supported
- See [Iceberg README](contracts_iceberg/README.md) for detailed limitations

### Quality Checks
- **Custom SQL**: Framework exists with syntax validation; SQL execution with DataFusion planned for v0.1.0
- **Freshness**: Supports ISO 8601, Unix epoch, and common date formats (YYYY-MM-DD, YYYY-MM-DD HH:MM:SS)
- **Completeness & Uniqueness**: Fully implemented and production-ready

### Testing
- **CLI integration tests**: 30 tests (100% pass rate)
- **Total test coverage**: 194 tests across all packages

## Roadmap

### Phase 1: Foundation (85% Complete)
- [x] Core data structures and types (95% - complex type details pending)
- [x] YAML/TOML parser (100% - fully complete)
- [x] Validation engine (95% - schema, constraints, quality checks working)
- [x] Iceberg integration (85% - REST, Glue, HMS catalogs operational)
- [x] CLI commands: `check` and `init` (100% - fully implemented)
- [x] CLI `validate` command (85% - Iceberg format fully supported)
- [x] Enhanced freshness validation (Unix epoch, multiple formats)
- [x] CLI integration tests (30 tests, 100% pass rate)
- [x] Comprehensive test coverage (194 tests total)
- [ ] v0.0.1 release

**Legend**: [x] Complete | [ ] Not Started

### Phase 2: Enhanced Validation (v0.1.0)
- [ ] **SQL execution with Apache DataFusion** - Execute custom SQL checks against datasets in-memory
- [ ] Multi-format data validation (Parquet, CSV, Delta, Hudi) - Full data validation support
- [ ] Complex type support (Struct, List, Map with full type information)
- [ ] Schema evolution tracking
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
