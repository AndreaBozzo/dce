# ADR-001: Workspace Structure

**Status**: Accepted
**Date**: 2024-10-31
**Authors**: DCE Team

## Context

The Data Contracts Engine needs a clear crate structure that supports modularity, reusability, and independent development of different components.

## Decision

We adopt a Cargo workspace with the following crates:

1. **contracts_core** - Core data structures and types
   - Contract, Schema, Field definitions
   - Error types
   - Validation traits
   - Builder patterns
   - Zero external dependencies on other DCE crates

2. **contracts_parser** - DSL parsing
   - YAML/TOML parsing
   - Contract deserialization
   - Depends on: contracts_core

3. **contracts_validator** - Validation engine
   - Rule evaluation
   - Quality checks implementation
   - Depends on: contracts_core

4. **contracts_iceberg** - Apache Iceberg integration
   - Iceberg-specific validation
   - Schema compatibility checks
   - Depends on: contracts_core, contracts_validator

5. **contracts_cli** - Command-line tool
   - Binary: `dce`
   - Commands: validate, init, diff, check
   - Depends on: all above crates

6. **contracts_sdk** - Public Rust SDK
   - Unified API for library consumers
   - Re-exports from all crates
   - Depends on: all crates

## Rationale

### Advantages
- **Modularity**: Each crate has a single responsibility
- **Reusability**: Core types can be used without pulling in validation logic
- **Independent Versioning**: Crates can evolve at different paces
- **Compilation Speed**: Only changed crates need recompilation
- **Testing**: Each crate can be tested independently

### Trade-offs
- More complex dependency management
- Potential for circular dependencies (mitigated by design)
- Need to maintain consistency across crates

## Alternatives Considered

### Single Monolithic Crate
- **Rejected**: Would make it hard to use just the core types
- Forces users to pull in all dependencies (parser, validators, etc.)

### Feature Flags
- **Rejected**: Less clear separation of concerns
- Makes dependency graph complex
- Harder to maintain

## Implementation

```toml
[workspace]
members = [
    "contracts_core",
    "contracts_parser",
    "contracts_validator",
    "contracts_iceberg",
    "contracts_cli",
    "contracts_sdk",
]
```

Shared dependencies managed at workspace level:
- serde, thiserror, tokio, clap, etc.

## Consequences

### Positive
- Clear module boundaries
- Easy to add new format support (Delta, Hudi) as separate crates
- SDK consumers can choose what to include

### Negative
- Need to maintain multiple Cargo.toml files
- Documentation spans multiple crates

## Future Considerations

- Add `contracts_delta` for Delta Lake support
- Add `contracts_hudi` for Apache Hudi support
- Consider `contracts_python` for Python bindings (PyO3)
