# Contributing to Data Contracts Engine

Thank you for your interest in contributing to DCE! This document provides guidelines and instructions for contributing.

## Code of Conduct

Be respectful, inclusive, and professional in all interactions.

## Getting Started

### Prerequisites

- Rust 1.70 or later
- Git
- Familiarity with data engineering concepts

### Development Setup

```bash
# Clone the repository
git clone https://github.com/AndreaBozzo/dce
cd dce

# Build the workspace
cargo build --workspace

# Run tests
cargo test --workspace

# Run clippy for linting
cargo clippy --workspace -- -D warnings

# Format code
cargo fmt --all

# Generate documentation
cargo doc --workspace --no-deps --open
```

## Project Structure

```
dce/
├── contracts_core/      # Core types and traits
├── contracts_parser/    # YAML/TOML parsing
├── contracts_validator/ # Validation engine
├── contracts_iceberg/   # Iceberg integration
├── contracts_cli/       # CLI tool
├── contracts_sdk/       # Public API
├── docs/               # Documentation
├── examples/           # Example contracts
└── tests/              # Integration tests
```

## How to Contribute

### Reporting Bugs

1. Check if the bug is already reported in [Issues](https://github.com/yourusername/dce/issues)
2. If not, create a new issue with:
   - Clear title and description
   - Steps to reproduce
   - Expected vs actual behavior
   - Rust version and OS
   - Relevant logs or error messages

### Suggesting Features

1. Check [Discussions](https://github.com/yourusername/dce/discussions) for similar ideas
2. Create a new discussion or issue describing:
   - Use case and motivation
   - Proposed solution
   - Alternatives considered

### Pull Requests

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/my-feature`
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass: `cargo test --workspace`
6. Ensure code is formatted: `cargo fmt --all`
7. Ensure no clippy warnings: `cargo clippy --workspace -- -D warnings`
8. Commit with clear messages
9. Push to your fork
10. Create a Pull Request

#### PR Guidelines

- **Title**: Clear, descriptive summary
- **Description**: Explain what, why, and how
- **Link Issues**: Reference related issues
- **Tests**: Include tests for new features
- **Documentation**: Update docs if needed
- **Changelog**: Add entry to CHANGELOG.md

## Development Guidelines

### Code Style

- Follow Rust standard conventions
- Use `rustfmt` for formatting
- Use `clippy` for linting
- Maximum line length: 100 characters
- Prefer explicit over implicit

### Documentation

- All public items must have doc comments
- Include examples in doc comments
- Document error conditions
- Update README if adding user-facing features

Example:
```rust
/// Validates a contract against Iceberg schema.
///
/// # Arguments
///
/// * `contract` - The contract to validate
/// * `context` - Validation context with options
///
/// # Returns
///
/// `Ok(())` if validation passes, `Err` otherwise.
///
/// # Example
///
/// ```
/// let validator = IcebergValidator::new();
/// let result = validator.validate(&contract, &context);
/// ```
pub fn validate(&self, contract: &Contract, context: &ValidationContext) -> ValidationResult {
    // implementation
}
```

### Testing

- Write unit tests for all public functions
- Write integration tests for cross-crate functionality
- Use `pretty_assertions` for better test output
- Test error cases, not just happy paths

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_contract_builder() {
        let contract = ContractBuilder::new("test", "owner")
            .format(DataFormat::Iceberg)
            .build();

        assert_eq!(contract.name, "test");
        assert_eq!(contract.owner, "owner");
    }
}
```

### Error Handling

- Use `thiserror` for error types
- Provide context in error messages
- Use `Result<T, ContractError>` for fallible operations

```rust
#[derive(Error, Debug)]
pub enum ContractError {
    #[error("Schema validation failed: {0}")]
    SchemaValidation(String),

    #[error("Field '{field}' not found")]
    MissingField { field: String },
}
```

### Performance

- Avoid unnecessary allocations
- Use references where possible
- Profile before optimizing
- Document performance characteristics

## Commit Messages

Format: `<type>(<scope>): <subject>`

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Formatting, no code change
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance tasks

Examples:
```
feat(parser): add TOML support for contracts
fix(validator): handle null values in constraints
docs(readme): update installation instructions
test(core): add tests for builder pattern
```

## Release Process

1. Update version in all `Cargo.toml` files
2. Update `CHANGELOG.md`
3. Create PR with version bump
4. After merge, tag release: `git tag v0.1.0`
5. Push tag: `git push origin v0.1.0`
6. GitHub Actions will build and publish

## Architecture Decisions

For significant architectural changes:

1. Create an Architecture Decision Record (ADR)
2. Place in `docs/architecture/`
3. Follow template in existing ADRs
4. Discuss in PR before merging

## Questions?

- Open a [Discussion](https://github.com/yourusername/dce/discussions)
- Ask in Pull Request comments
- Check existing documentation

## License

By contributing, you agree that your contributions will be licensed under both MIT and Apache-2.0 licenses.

Thank you for contributing to DCE!
