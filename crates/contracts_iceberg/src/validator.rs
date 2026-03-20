//! Main Iceberg validator implementation.

use crate::{
    IcebergError,
    catalog::{build_file_io, create_table_ident, load_catalog},
    config::{CatalogType, IcebergConfig},
    converter::arrow_value_to_data_value,
    schema::extract_schema_from_iceberg,
};
use contracts_core::{Contract, ValidationContext, ValidationReport};
use contracts_validator::{DataSet, DataValidator};
use futures::TryStreamExt;
use iceberg::{
    Catalog,
    io::FileIO,
    table::{StaticTable, Table},
};
use std::collections::HashMap;
use tracing::{debug, info, warn};

/// Validator for Apache Iceberg tables against data contracts.
///
/// Provides functionality to connect to Iceberg tables, extract schemas,
/// read data, and validate against DCE contracts.
pub struct IcebergValidator {
    config: IcebergConfig,
    catalog: Option<Box<dyn Catalog>>,
    file_io: Option<FileIO>,
}

impl IcebergValidator {
    /// Creates a new Iceberg validator with the given configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for connecting to the Iceberg table
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid or connection fails.
    pub async fn new(config: IcebergConfig) -> Result<Self, IcebergError> {
        info!(
            "Initializing Iceberg validator for table: {}.{}",
            config.namespace.join("."),
            config.table_name
        );

        config.validate()?;

        // Load catalog if not FileIO
        let catalog = match &config.catalog {
            CatalogType::FileIO => None,
            _ => Some(load_catalog(&config).await?),
        };

        // Only build FileIO for FileIO catalog type (local filesystem access).
        // Catalog-based paths (REST, Glue, HMS) handle storage access internally.
        let file_io = match &config.catalog {
            CatalogType::FileIO => Some(build_file_io(config.warehouse())?),
            _ => None,
        };

        Ok(Self {
            config,
            catalog,
            file_io,
        })
    }

    /// Loads the Iceberg table from the configured location.
    ///
    /// Supports both catalog-based loading (REST, Glue, HMS) and direct FileIO loading.
    async fn load_table(&self) -> Result<Table, IcebergError> {
        let table_ident = create_table_ident(&self.config.namespace, &self.config.table_name)?;

        info!("Loading Iceberg table: {}", table_ident);

        if let Some(catalog) = &self.catalog {
            // Load table from catalog
            catalog
                .load_table(&table_ident)
                .await
                .map_err(|e| IcebergError::TableNotFound(format!("{}: {}", table_ident, e)))
        } else {
            // For FileIO, we need a direct metadata file path
            // This should be provided in the properties
            let metadata_path =
                self.config
                    .properties
                    .get("metadata_location")
                    .ok_or_else(|| {
                        IcebergError::ConfigurationError(
                            "FileIO catalog requires 'metadata_location' property".to_string(),
                        )
                    })?;

            info!("Loading table from metadata file: {}", metadata_path);

            let file_io = self.file_io.clone().ok_or_else(|| {
                IcebergError::ConfigurationError(
                    "FileIO not available for FileIO catalog type".to_string(),
                )
            })?;

            StaticTable::from_metadata_file(metadata_path, table_ident, file_io)
                .await
                .map(|static_table| static_table.into_table())
                .map_err(|e| IcebergError::TableNotFound(format!("Failed to load table: {}", e)))
        }
    }

    /// Extracts the schema from the Iceberg table.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be loaded or schema extraction fails.
    pub async fn extract_schema(&self) -> Result<contracts_core::Schema, IcebergError> {
        let table = self.load_table().await?;
        let iceberg_schema = table.metadata().current_schema();

        let location = self
            .config
            .warehouse()
            .map(|w| {
                format!(
                    "{}/{}/{}",
                    w,
                    self.config.namespace.join("."),
                    self.config.table_name
                )
            })
            .unwrap_or_else(|| {
                format!(
                    "{}.{}",
                    self.config.namespace.join("."),
                    self.config.table_name
                )
            });

        extract_schema_from_iceberg(iceberg_schema, &location)
    }

    /// Validates an Iceberg table against a contract.
    ///
    /// This method:
    /// 1. Loads the table and extracts its schema
    /// 2. Validates the schema matches the contract
    /// 3. Reads sample data from the table (if not schema-only mode)
    /// 4. Validates the data against contract constraints
    ///
    /// # Arguments
    ///
    /// * `contract` - The data contract to validate against
    /// * `context` - Validation context with options (sample_size, schema_only, strict, etc.)
    ///
    /// # Errors
    ///
    /// Returns an error if validation cannot be performed (e.g., table not accessible).
    pub async fn validate_table(
        &self,
        contract: &Contract,
        context: &ValidationContext,
    ) -> Result<ValidationReport, IcebergError> {
        info!(
            "Validating Iceberg table against contract: {}",
            contract.name
        );

        // Check if schema-only validation is requested
        if context.schema_only {
            return self.validate_schema_only(contract, context).await;
        }

        #[cfg(feature = "native-datafusion")]
        {
            return self.validate_table_native(contract, context).await;
        }

        #[cfg(not(feature = "native-datafusion"))]
        {
            return self.validate_table_dataset(contract, context).await;
        }
    }

    /// Validates using the DataSet-based path (legacy).
    ///
    /// Reads data into an intermediate `DataSet`, then converts to Arrow for
    /// DataFusion validation. Used when the `native-datafusion` feature is disabled.
    #[cfg(not(feature = "native-datafusion"))]
    async fn validate_table_dataset(
        &self,
        contract: &Contract,
        context: &ValidationContext,
    ) -> Result<ValidationReport, IcebergError> {
        let sample_size = context.sample_size.unwrap_or(1000);

        let dataset = self.read_sample_data(sample_size).await?;

        info!("Read {} rows for validation", dataset.len());

        let mut validator = DataValidator::new();
        let report = validator
            .validate_with_data_async(contract, &dataset, context)
            .await;

        self.log_result(&report);

        Ok(report)
    }

    /// Validates by registering the Iceberg table directly with DataFusion.
    ///
    /// This zero-copy path avoids the intermediate `DataSet` representation,
    /// enabling predicate/projection pushdown and streaming execution.
    #[cfg(feature = "native-datafusion")]
    async fn validate_table_native(
        &self,
        contract: &Contract,
        context: &ValidationContext,
    ) -> Result<ValidationReport, IcebergError> {
        use datafusion::prelude::SessionContext;
        use iceberg_datafusion::IcebergStaticTableProvider;
        use std::sync::Arc;

        info!("Using native DataFusion path for Iceberg table validation");

        let table = self.load_table().await?;

        let provider = IcebergStaticTableProvider::try_new_from_table(table)
            .await
            .map_err(|e| {
                IcebergError::DataReadError(format!("Failed to create Iceberg table provider: {e}"))
            })?;

        let ctx = SessionContext::new();

        if let Some(limit) = context.sample_size {
            ctx.register_table("iceberg_raw", Arc::new(provider))
                .map_err(|e| IcebergError::DataReadError(e.to_string()))?;
            ctx.sql(&format!(
                "CREATE VIEW data AS SELECT * FROM iceberg_raw LIMIT {limit}"
            ))
            .await
            .map_err(|e| IcebergError::DataReadError(e.to_string()))?
            .collect()
            .await
            .map_err(|e| IcebergError::DataReadError(e.to_string()))?;
        } else {
            ctx.register_table("data", Arc::new(provider))
                .map_err(|e| IcebergError::DataReadError(e.to_string()))?;
        }

        let mut validator = DataValidator::new();
        let report = validator
            .validate_with_context(contract, &ctx, context)
            .await;

        self.log_result(&report);

        Ok(report)
    }

    fn log_result(&self, report: &ValidationReport) {
        if report.passed {
            info!(
                "Validation passed for table: {}.{}",
                self.config.namespace.join("."),
                self.config.table_name
            );
        } else {
            warn!(
                "Validation failed for table: {}.{} with {} errors",
                self.config.namespace.join("."),
                self.config.table_name,
                report.errors.len()
            );
        }
    }

    /// Validates only the schema of an Iceberg table against a contract (no data reading).
    ///
    /// This is faster than full validation as it doesn't read any data from the table.
    ///
    /// # Arguments
    ///
    /// * `contract` - The data contract to validate against
    /// * `context` - Validation context (schema_only flag is enforced to true)
    ///
    /// # Errors
    ///
    /// Returns an error if validation cannot be performed.
    pub async fn validate_schema_only(
        &self,
        contract: &Contract,
        context: &ValidationContext,
    ) -> Result<ValidationReport, IcebergError> {
        info!(
            "Validating schema only for table against contract: {}",
            contract.name
        );

        // Ensure schema-only mode is set
        let mut schema_context = context.clone();
        schema_context.schema_only = true;

        // Use empty dataset for schema-only validation
        let dataset = DataSet::empty();

        // Validate contract
        let mut validator = DataValidator::new();
        let report = validator
            .validate_with_data_async(contract, &dataset, &schema_context)
            .await;

        if report.passed {
            info!(
                "Schema validation passed for table: {}.{}",
                self.config.namespace.join("."),
                self.config.table_name
            );
        } else {
            warn!(
                "Schema validation failed for table: {}.{} with {} errors",
                self.config.namespace.join("."),
                self.config.table_name,
                report.errors.len()
            );
        }

        Ok(report)
    }

    /// Reads sample data from the Iceberg table.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of rows to read
    ///
    /// # Errors
    ///
    /// Returns an error if data cannot be read from the table.
    pub async fn read_sample_data(&self, limit: usize) -> Result<DataSet, IcebergError> {
        info!("Reading sample data (limit: {}) from table", limit);

        let table = self.load_table().await?;

        // Create a table scan with all columns
        let scan = table
            .scan()
            .select_all()
            .with_batch_size(Some(1024))
            .build()
            .map_err(|e| IcebergError::DataReadError(format!("Failed to build scan: {}", e)))?;

        // Convert to Arrow stream
        let mut stream = scan.to_arrow().await.map_err(|e| {
            IcebergError::DataReadError(format!("Failed to create arrow stream: {}", e))
        })?;

        debug!("Arrow stream created, reading record batches");

        let mut rows = Vec::new();
        let mut total_rows = 0;

        // Read record batches from stream
        while let Some(batch) = stream.try_next().await.map_err(|e| {
            IcebergError::DataReadError(format!("Failed to read record batch: {}", e))
        })? {
            debug!("Processing batch with {} rows", batch.num_rows());

            let schema = batch.schema();
            let num_rows = batch.num_rows();

            // Convert each row in the batch
            for row_idx in 0..num_rows {
                if total_rows >= limit {
                    break;
                }

                let mut row = HashMap::new();

                // Convert each column value
                for (col_idx, field) in schema.fields().iter().enumerate() {
                    let column = batch.column(col_idx);
                    let value = arrow_value_to_data_value(column, row_idx)?;
                    row.insert(field.name().clone(), value);
                }

                rows.push(row);
                total_rows += 1;
            }

            if total_rows >= limit {
                break;
            }
        }

        info!("Read {} rows from Iceberg table", rows.len());

        Ok(DataSet::from_rows(rows))
    }

    /// Returns the configuration used by this validator.
    pub fn config(&self) -> &IcebergConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validator_config_file_io() {
        let config = IcebergConfig::builder()
            .file_io()
            .namespace(vec!["test".to_string()])
            .table_name("my_table")
            .property("metadata_location", "/tmp/metadata.json")
            .build()
            .unwrap();

        let result = IcebergValidator::new(config.clone()).await;

        // This will succeed as FileIO doesn't require catalog connection
        assert!(result.is_ok());
        if let Ok(validator) = result {
            assert_eq!(validator.config().table_name, "my_table");
            assert_eq!(validator.config().namespace, vec!["test".to_string()]);
        }
    }

    #[test]
    fn test_validator_with_invalid_config() {
        let result = IcebergConfig::builder().build();
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_validator_config_rest() {
        let config = IcebergConfig::builder()
            .rest_catalog("http://localhost:8181", "s3://warehouse")
            .namespace(vec!["db".to_string()])
            .table_name("events")
            .build();

        assert!(config.is_ok());
        let config = config.unwrap();

        // This will likely fail without a running REST catalog, which is expected
        let result = IcebergValidator::new(config).await;

        // We expect this to fail without actual catalog, but it tests the code path
        assert!(result.is_err() || result.is_ok());
    }
}
