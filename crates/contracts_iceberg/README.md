# contracts_iceberg

Apache Iceberg integration for the Data Contracts Engine.

## Overview

This crate provides integration between Apache Iceberg tables and the Data Contracts Engine (DCE), enabling validation of Iceberg tables against predefined data contracts.

## Features

- **Multiple Catalog Support**: REST, AWS Glue, Hive Metastore (HMS), and FileIO catalogs
- **Schema Validation**: Verify that table schema matches contract expectations
- **Data Validation**: Sample and validate actual data against contract rules
- **Type Conversion**: Comprehensive mapping between Iceberg and DCE type systems
- **Async/Await**: Full async support for efficient I/O operations

## Supported Catalog Types

| Catalog Type | Status | Notes |
|-------------|---------|-------|
| REST        | ✅ Complete | Full support for REST catalog API |
| AWS Glue    | ✅ Complete | Requires AWS credentials and IAM permissions |
| Hive Metastore | ✅ Complete | Requires network access to HMS thrift endpoint |
| FileIO      | ⚠️ Limited | Requires direct metadata file path via properties |

## Supported Data Types

### Primitive Types (100% Complete)

| Iceberg Type | DCE Type | Notes |
|-------------|----------|-------|
| Boolean     | boolean  | ✅ Fully supported |
| Int         | int32    | ✅ Fully supported |
| Long        | int64    | ✅ Fully supported |
| Float       | float32  | ✅ Fully supported |
| Double      | float64  | ✅ Fully supported |
| Decimal     | decimal  | ✅ Fully supported (128 and 256 bit) |
| Date        | date     | ✅ Fully supported (Date32 and Date64) |
| Time        | time     | ✅ Fully supported |
| Timestamp   | timestamp | ✅ All precisions (second, milli, micro, nano) |
| TimestampTz | timestamp | ✅ All precisions with timezone |
| String      | string   | ✅ Fully supported |
| UUID        | uuid     | ✅ Fully supported |
| Fixed       | binary   | ✅ Fully supported |
| Binary      | binary   | ✅ Fully supported |

### Complex Types (Partial Support)

| Iceberg Type | DCE Type | Status | Notes |
|-------------|----------|--------|-------|
| Struct      | map      | ⚠️ Partial | Mapped to generic "map", nested structure not preserved |
| List        | list     | ⚠️ Partial | Mapped to generic "list", element type not preserved |
| Map         | map      | ⚠️ Partial | Mapped to generic "map", key/value types not preserved |

## Usage

### Basic Example

```rust
use contracts_core::{Contract, DataFormat, ValidationContext};
use contracts_iceberg::{IcebergConfig, IcebergValidator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure connection to Iceberg catalog
    let config = IcebergConfig::builder()
        .rest_catalog("http://localhost:8181", "/warehouse")
        .namespace(vec!["my_db".to_string()])
        .table_name("events")
        .sample_size(100)
        .build()?;

    // Create validator
    let validator = IcebergValidator::new(config)?;

    // Load your contract
    let contract = Contract::builder()
        .version("1.0.0")
        .id("events-contract")
        .data_format(DataFormat::Iceberg)
        // ... define schema ...
        .build()?;

    // Validate
    let context = ValidationContext::default();
    let report = validator.validate_table(&contract, &context).await?;

    println!("Validation result: {}", report.valid);
    Ok(())
}
```

### More Examples

See the [examples](examples/) directory for complete examples of:
- [REST Catalog](examples/rest_catalog.rs) - Using REST catalog backend
- [AWS Glue Catalog](examples/glue_catalog.rs) - Using AWS Glue Data Catalog
- [Hive Metastore](examples/hms_catalog.rs) - Using Hive Metastore

Run examples with:
```bash
cargo run --example rest_catalog
cargo run --example glue_catalog
cargo run --example hms_catalog
```

## Configuration

### REST Catalog

```rust
let config = IcebergConfig::builder()
    .rest_catalog(
        "http://localhost:8181",  // REST API URI
        "/warehouse"               // Warehouse path
    )
    .namespace(vec!["database".to_string()])
    .table_name("table_name")
    .build()?;
```

### AWS Glue Catalog

```rust
let config = IcebergConfig::builder()
    .glue_catalog(
        Some("catalog-id".to_string()),  // Optional catalog ID
        Some("us-west-2".to_string())    // AWS region
    )
    .namespace(vec!["database".to_string()])
    .table_name("table_name")
    .build()?;
```

Requires AWS credentials configured via:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- IAM role (when running on EC2/ECS/Lambda)

### Hive Metastore Catalog

```rust
let config = IcebergConfig::builder()
    .hms_catalog("thrift://localhost:9083")  // HMS thrift endpoint
    .namespace(vec!["database".to_string()])
    .table_name("table_name")
    .build()?;
```

### FileIO Catalog (Limited)

```rust
let config = IcebergConfig::builder()
    .file_io_catalog("s3://bucket/warehouse")
    .namespace(vec!["db".to_string()])
    .table_name("table")
    .add_property(
        "metadata_location",
        "s3://bucket/warehouse/db/table/metadata/v1.metadata.json"
    )
    .build()?;
```

**Note**: FileIO catalog requires direct metadata file path and has limited functionality compared to other catalog types.

## Validation Modes

### Schema-Only Validation (Fast)

Validates only the table schema without reading data:

```rust
let report = validator.validate_schema_only(&contract, &context).await?;
```

### Full Validation (With Data Sampling)

Validates both schema and actual data:

```rust
let report = validator.validate_table(&contract, &context).await?;
```

Configure sample size:
```rust
let config = IcebergConfig::builder()
    // ... catalog config ...
    .sample_size(500)  // Number of rows to sample
    .build()?;
```

## Known Limitations

### 1. Complex Type Handling

Complex types (Struct, List, Map) are currently mapped to generic strings without preserving nested type information:

```rust
// Current behavior
Struct { field1: Int, field2: String } → "map"
List<Int> → "list"
Map<String, Int> → "map"
```

**Impact**: Cannot validate nested structures or enforce specific element/key/value types.

**Workaround**: Use flattened schemas or validate complex types separately.

### 2. FileIO Catalog

FileIO catalog support is limited and requires direct metadata file paths:

```rust
// Requires explicit metadata_location property
.add_property("metadata_location", "s3://path/to/metadata.json")
```

**Impact**: Less convenient than catalog-based loading; manual metadata path management required.

**Workaround**: Use REST, Glue, or HMS catalogs when possible.

### 3. Schema Evolution

Schema evolution tracking is not currently implemented.

**Impact**: Cannot detect or validate schema changes over time.

**Workaround**: Maintain contract versions manually.

### 4. Partition Information

Partition information is not exposed in the schema.

**Impact**: Cannot validate partitioning schemes against contracts.

**Workaround**: Validate partitioning separately if needed.

### 5. Metadata Tables

Iceberg metadata tables (snapshots, manifests, etc.) are not accessible.

**Impact**: Cannot validate metadata or access table history.

**Workaround**: Access metadata tables directly via Iceberg APIs if needed.

## Error Handling

The crate defines custom error types via `IcebergError`:

```rust
pub enum IcebergError {
    ConnectionError(String),
    TableNotFound(String),
    SchemaExtractionError(String),
    ConfigurationError(String),
    DataReadError(String),
    TypeConversionError(String),
    ValidationError(String),
    UnsupportedOperation(String),
}
```

All errors implement `std::error::Error` and can be converted to/from `anyhow::Error`.

## Dependencies

- `iceberg` 0.7.0 - Core Iceberg functionality
- `iceberg-catalog-rest` 0.7 - REST catalog support
- `iceberg-catalog-glue` 0.7 - AWS Glue support
- `iceberg-catalog-hms` 0.7 - Hive Metastore support
- `arrow-array` 55.2.0 - Arrow data structures
- `arrow-schema` 55.2.0 - Arrow schema types
- `chrono` - Date/time handling
- `tokio` - Async runtime
- `tracing` - Logging

## Testing

Run unit tests:
```bash
cargo test --package contracts_iceberg
```

Run integration tests:
```bash
cargo test --package contracts_iceberg --test integration_tests
```

Run all tests with output:
```bash
cargo test --package contracts_iceberg -- --nocapture
```

## Contributing

When contributing, please ensure:
1. All tests pass: `cargo test --package contracts_iceberg`
2. No clippy warnings: `cargo clippy --package contracts_iceberg`
3. Code is formatted: `cargo fmt --package contracts_iceberg`
4. New features include tests and documentation

## License

See the main repository LICENSE file for license information.
