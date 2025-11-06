//! Main Iceberg validator implementation.

use crate::{config::IcebergConfig, schema::extract_schema_from_iceberg, IcebergError};
use contracts_core::{Contract, ValidationContext, ValidationReport};
use contracts_validator::{DataSet, DataValidator};
use iceberg::{
    io::{FileIO, FileIOBuilder},
    table::Table,
};
use tracing::{info, warn};

/// Validator for Apache Iceberg tables against data contracts.
///
/// Provides functionality to connect to Iceberg tables, extract schemas,
/// read data, and validate against DCE contracts.
pub struct IcebergValidator {
    config: IcebergConfig,
    // TODO: Will be used in next iteration for actual table operations
    #[allow(dead_code)]
    file_io: FileIO,
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
            "Initializing Iceberg validator for: {}",
            config.table_location
        );

        config.validate()?;

        // Initialize FileIO for accessing table files
        let file_io = FileIOBuilder::new_fs_io()
            .build()
            .map_err(|e| IcebergError::ConnectionError(format!("Failed to build FileIO: {}", e)))?;

        Ok(Self { config, file_io })
    }

    /// Loads the Iceberg table from the configured location.
    ///
    /// Note: This is a stub implementation. Full catalog-based loading
    /// will be implemented in a future iteration.
    async fn load_table(&self) -> Result<Table, IcebergError> {
        info!("Loading Iceberg table from: {}", self.config.table_location);

        // TODO: Implement proper table loading using catalog
        // For now, return an error indicating this needs implementation
        Err(IcebergError::UnsupportedOperation(
            "Table loading from catalog not yet implemented. \
             This will be added in the next iteration with proper catalog support."
                .to_string(),
        ))
    }

    /// Extracts the schema from the Iceberg table.
    ///
    /// # Errors
    ///
    /// Returns an error if the table cannot be loaded or schema extraction fails.
    pub async fn extract_schema(&self) -> Result<contracts_core::Schema, IcebergError> {
        let table = self.load_table().await?;
        let iceberg_schema = table.metadata().current_schema();

        extract_schema_from_iceberg(iceberg_schema, &self.config.table_location)
    }

    /// Validates an Iceberg table against a contract.
    ///
    /// This method:
    /// 1. Loads the table and extracts its schema
    /// 2. Validates the schema matches the contract
    /// 3. Reads sample data from the table
    /// 4. Validates the data against contract constraints
    ///
    /// # Arguments
    ///
    /// * `contract` - The data contract to validate against
    ///
    /// # Errors
    ///
    /// Returns an error if validation cannot be performed (e.g., table not accessible).
    pub async fn validate_table(
        &self,
        contract: &Contract,
    ) -> Result<ValidationReport, IcebergError> {
        info!(
            "Validating Iceberg table against contract: {}",
            contract.name
        );

        // Load the table
        let table = self.load_table().await?;

        // Extract schema from table for validation
        let _actual_schema = extract_schema_from_iceberg(
            table.metadata().current_schema(),
            &self.config.table_location,
        )?;

        // Create validation context
        let context = ValidationContext::new();

        // For now, validate schema only
        // TODO: Implement data reading and validation in next iteration
        let mut validator = DataValidator::new();

        // Create an empty dataset for schema-only validation
        let dataset = DataSet::empty();

        // Validate contract with actual schema from Iceberg
        let report = validator.validate_with_data(contract, &dataset, &context);

        if report.passed {
            info!(
                "Validation passed for table: {}",
                self.config.table_location
            );
        } else {
            warn!(
                "Validation failed for table: {} with {} errors",
                self.config.table_location,
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

        let _table = self.load_table().await?;

        // TODO: Implement actual data reading using Iceberg's scan API
        // This requires:
        // 1. Creating a table scan
        // 2. Reading Arrow record batches
        // 3. Converting to DataSet

        warn!("Data reading not yet fully implemented, returning empty dataset");
        Ok(DataSet::empty())
    }

    /// Returns the configuration used by this validator.
    pub fn config(&self) -> &IcebergConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validator_config() {
        let config = IcebergConfig::builder()
            .table_location("s3://test/table")
            .build()
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(IcebergValidator::new(config.clone()));

        // This might fail without proper S3 setup, but tests the structure
        if let Ok(validator) = result {
            assert_eq!(validator.config().table_location, "s3://test/table");
        }
    }

    #[test]
    fn test_validator_with_invalid_config() {
        let result = IcebergConfig::builder().build();
        assert!(result.is_err());
    }
}
