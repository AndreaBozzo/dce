# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Improvements and refinements to the `dce` CLI UX (error messages, help text, argument validation).
- Additional documentation on architecture and the conceptual model (Contract, Schema, Quality Checks, SLA).
- More complete contract examples in `examples/contracts/` (for example `user_events.yml`), aligned with validation tests.
- Initial CI setup (tests, fmt, clippy) to be finalized before the first public release.

### Changed
- Alignment of workspace metadata (version, README badge, descriptions) in preparation for the first release.
- Clarified the scope of the initial release (marking SDK and Python bindings as experimental or out of scope).

### Deprecated
- Nothing deprecated yet, but some APIs may evolve before a future `0.1.0` stable release.

### Fixed
- Improved test coverage for edge-case validations (null handling, multiple constraints, data quality on realistic datasets).

---

## [0.0.1] – *not yet published*

> **Note:** This is a draft of the first public release.  
> The Git tag and binaries will be created once the project is considered stable enough.

### Added
- **Data Contracts Core** (`contracts_core`):
  - Strongly-typed structures for `Contract`, `Schema`, `Field`, `Constraint`, `QualityChecks`, and `Sla`.
  - Ergonomic builders for defining contracts programmatically.
- **DSL Parser** (`contracts_parser`):
  - Parsing of contracts from **YAML** and **TOML** into the core `Contract` type.
  - Automatic format detection based on file extension.
  - Typed errors for YAML, TOML, and I/O failures.
- **Validation Engine** (`contracts_validator`):
  - Schema validation (types, nullability, missing/extra fields).
  - Constraint validation (allowed values, ranges, patterns).
  - Data quality checks (completeness, uniqueness, freshness), with support for strict mode and schema-only mode.
  - Infrastructure for custom checks and dataset abstractions.
- **Apache Iceberg Integration** (`contracts_iceberg`):
  - `IcebergConfig` and `IcebergValidator` for validating Iceberg tables against data contracts.
  - Support for REST, Glue, HMS, and file-based catalogs.
  - Schema extraction and type conversion between Iceberg/Arrow and DCE types.
- **CLI** (`contracts_cli`):
  - `dce validate` to validate a contract against an Iceberg table (with `--strict`, `--schema-only`, `--sample-size`, and `--format text|json`).
  - `dce check` to validate contract syntax and schema without reading data.
  - `dce init` to generate a contract from an existing Iceberg table (REST/Glue/HMS catalogs).
- **Examples and tests**:
  - Example contracts under `examples/contracts/`.
  - Extensive unit and integration test coverage across core, parser, validator, Iceberg integration, and CLI.
