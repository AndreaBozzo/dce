# Data Contracts Engine - Architecture

## Overview

DCE is designed as a modular, extensible system for defining and validating data contracts across multiple table formats and cloud platforms.

## Core Principles

1. **Performance First**: Rust-native for maximum speed
2. **Zero Lock-in**: Cloud and format agnostic
3. **Developer Experience**: Git-native workflow, simple CLI
4. **Extensibility**: Plugin architecture for custom validators
5. **Type Safety**: Leverage Rust's type system for correctness

## System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                        contracts_sdk                         │
│                   (Public Rust API)                          │
└──────────────┬──────────────────────────────────────────────┘
               │
       ┌───────┴────────┬──────────────┬─────────────┐
       │                │              │             │
┌──────▼──────┐  ┌─────▼──────┐ ┌────▼──────┐ ┌───▼────────┐
│contracts_cli│  │contracts_  │ │contracts_ │ │contracts_  │
│   (dce)     │  │parser      │ │validator  │ │iceberg     │
└─────────────┘  └────────────┘ └───────────┘ └────────────┘
                        │              │             │
                        └──────┬───────┴─────────────┘
                               │
                        ┌──────▼──────┐
                        │contracts_   │
                        │core         │
                        └─────────────┘
```

## Crate Responsibilities

### contracts_core
**Purpose**: Foundational types and traits

**Contains**:
- `Contract`, `Schema`, `Field` data structures
- `DataFormat` enum (Iceberg, Delta, Hudi, etc.)
- Error types (`ContractError`, `ValidationError`)
- Validation traits (`ContractValidator`)
- Builder patterns

**Dependencies**: None (minimal external deps: serde, thiserror)

### contracts_parser
**Purpose**: Parse contract definitions from YAML/TOML

**Contains**:
- YAML/TOML deserialization
- Schema validation
- Contract versioning logic

**Dependencies**: contracts_core, serde_yaml_ng

### contracts_validator
**Purpose**: Generic validation engine

**Contains**:
- Rule evaluation framework
- Constraint checking
- Quality checks implementation
- Validation context and reporting

**Dependencies**: contracts_core, regex, chrono

### contracts_iceberg
**Purpose**: Apache Iceberg specific implementation

**Contains**:
- Iceberg schema reader
- Contract-to-Iceberg mapping
- Iceberg-specific validation
- Table metadata compatibility

**Dependencies**: contracts_core, contracts_validator, iceberg-rust

### contracts_cli
**Purpose**: Command-line interface

**Contains**:
- CLI argument parsing (clap)
- Command implementations (validate, init, diff)
- Output formatting
- Git integration

**Dependencies**: All crates, clap, colored

**Binary**: `dce`

### contracts_sdk
**Purpose**: Unified public API

**Contains**:
- Re-exports from all crates
- High-level convenience functions
- Documentation examples

**Dependencies**: All crates

## Data Flow

### Contract Validation Flow

```
1. User Input (YAML/TOML)
   │
   ▼
2. contracts_parser
   │ Parse & Deserialize
   ▼
3. Contract struct (contracts_core)
   │
   ▼
4. contracts_validator
   │ Schema & constraint validation
   ▼
5. Format-specific validator (e.g., contracts_iceberg)
   │ Read actual data schema
   │ Compare with contract
   ▼
6. ValidationReport
   │
   ▼
7. CLI Output / SDK Result
```

## Extension Points

### Adding New Table Format

To add support for a new format (e.g., Delta Lake):

1. Create new crate: `contracts_delta`
2. Implement `ContractValidator` trait
3. Add format-specific logic
4. Update `contracts_sdk` to export new validator

### Adding New Validation Rules

1. Add rule type to `contracts_core::FieldConstraints`
2. Implement evaluation in `contracts_validator`
3. Add tests

### Custom Validators

Users can implement the `ContractValidator` trait:

```rust
pub trait ContractValidator {
    fn validate(
        &self,
        contract: &Contract,
        context: &ValidationContext
    ) -> ValidationResult;
}
```

## Technology Stack

- **Language**: Rust 2021 Edition
- **Serialization**: Serde
- **CLI**: Clap v4
- **Async**: Tokio (for future networking features)
- **Iceberg**: iceberg-rust v0.7
- **Error Handling**: thiserror, anyhow

## Design Patterns

### Builder Pattern
Used for ergonomic contract construction:
```rust
let contract = ContractBuilder::new("users", "team")
    .format(DataFormat::Iceberg)
    .field(FieldBuilder::new("id", "string").build())
    .build();
```

### Trait-Based Validation
Abstract validation logic through traits:
```rust
impl ContractValidator for IcebergValidator {
    fn validate(&self, contract: &Contract, ctx: &ValidationContext) -> ValidationResult {
        // Implementation
    }
}
```

### Error Propagation
Consistent error handling with custom error types:
```rust
pub type Result<T> = std::result::Result<T, ContractError>;
```

## Performance Considerations

1. **Zero-Copy Parsing**: Use borrowed types where possible
2. **Parallel Validation**: Validate fields concurrently (future)
3. **Lazy Loading**: Only load data when needed
4. **Caching**: Memoize schema reads (future)
5. **Compilation**: Release builds with LTO and optimizations

## Security

1. **No Unsafe Code**: Avoid unsafe blocks unless absolutely necessary
2. **Input Validation**: Sanitize all user inputs
3. **Dependency Auditing**: Regular `cargo audit`
4. **Minimal Dependencies**: Only include necessary deps

## Testing Strategy

1. **Unit Tests**: Per module in each crate
2. **Integration Tests**: Cross-crate in `tests/`
3. **Example Tests**: Doctests in documentation
4. **Property Testing**: Future with proptest
5. **Benchmarks**: Future with criterion

## Future Architecture

### Phase 2: Multi-Format
- Add `contracts_delta`, `contracts_hudi`
- Unified validation interface

### Phase 3: Ecosystem
- Python bindings via PyO3 in `contracts_python`
- Great Expectations integration
- dbt adapter

### Phase 4: Intelligence
- ML-powered contract generation
- Drift detection service
- SaaS contract registry

## References

- [ADR-001: Workspace Structure](./architecture/ADR-001-workspace-structure.md)
- [Apache Iceberg Spec](https://iceberg.apache.org/spec/)
- [Data Contract Specification](https://datacontract.com/)
