# Data Contracts Engine CLI

Command-line interface for the Data Contracts Engine.

## Installation

```bash
# Build from source
cargo build --release --package contracts_cli

# The binary will be available at:
# target/release/dce
```

## Usage

### Check Contract Schema

Validate a contract file's structure without checking data:

```bash
dce check examples/contracts/user_events.yml
```

**Output:**
```
✓ Contract schema is valid

Contract Summary:
  Name:        user_events
  Version:     1.0.0
  Owner:       analytics-team
  Description: User interaction events dataset for analytics and ML
  Format:      Iceberg
  Location:    s3://data-lake/analytics/user_events
  Fields:      9
  Quality Checks: completeness, uniqueness, freshness, 3 custom
```

### Validate Contract (Schema-only for now)

```bash
dce validate examples/contracts/user_events.yml
```

**Note**: Currently performs schema-only validation. Full data validation against actual data sources will be implemented in a future release.

### Initialize Contract from Iceberg Table

Generate a contract from an existing Iceberg table:

```bash
dce init \
  --catalog rest \
  --namespace database.schema \
  --table my_table \
  --output my_contract.yml \
  http://localhost:8181
```

## Commands

### `dce check <contract>`

Validates the contract file structure and displays a summary.

**Options:**
- `-f, --format <FORMAT>` - Output format: text, json (default: text)
- `-v, --verbose` - Enable verbose logging

### `dce validate <contract>`

Validates a contract against data (currently schema-only).

**Options:**
- `-s, --strict` - Enable strict validation mode (fail on warnings)
- `-f, --format <FORMAT>` - Output format: text, json (default: text)
- `-v, --verbose` - Enable verbose logging

### `dce init <source>`

Initialize a new contract from an existing Iceberg table.

**Options:**
- `-c, --catalog <TYPE>` - Catalog type: rest, glue, hms (default: rest)
- `-n, --namespace <NS>` - Table namespace (e.g., "database.schema")
- `-t, --table <NAME>` - Table name
- `-o, --output <FILE>` - Output file path (defaults to stdout)
- `-v, --verbose` - Enable verbose logging

## Current Limitations

### Data Validation

The `validate` command currently performs **schema-only validation**. It does not yet connect to actual data sources or validate data against quality checks.

**Planned for future releases:**
- Connect to Iceberg tables and read actual data
- Validate data against quality checks (completeness, uniqueness, freshness)
- Support for custom SQL-based checks
- Data profiling and statistics

### Catalog Support

The `init` command supports Iceberg catalogs with feature flags:

- **REST Catalog** (default): Enabled by default
- **Glue Catalog**: Requires `--features glue-catalog`
- **HMS Catalog**: Requires `--features hms-catalog`

To build with all catalogs:

```bash
cargo build --release --package contracts_cli --features all-catalogs
```

## Development

Run with verbose logging:

```bash
cargo run --package contracts_cli -- --verbose check examples/contracts/user_events.yml
```

Run tests:

```bash
cargo test --package contracts_cli
```

## Roadmap

**v0.0.1 (Current)**:
- [x] CLI structure with clap
- [x] `check` command (schema validation)
- [x] `validate` command (schema-only)
- [x] `init` command (Iceberg schema extraction)
- [ ] Integration tests
- [ ] Error handling improvements

**v0.1.0 (Next)**:
- [ ] Full data validation (connect to Iceberg tables)
- [ ] Quality checks validation
- [ ] Custom checks execution
- [ ] Progress bars for long-running operations
- [ ] Better error messages

**v0.2.0 (Future)**:
- [ ] `diff` command (compare contracts)
- [ ] `generate` command (auto-generate from data profiling)
- [ ] CI/CD integration examples
- [ ] Pre-commit hook support
