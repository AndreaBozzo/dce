//! Apache Iceberg integration for Data Contracts Engine.
//!
//! This module provides functionality to validate data contracts against Apache Iceberg tables.
//! It handles schema extraction, data validation, and type conversions between Iceberg and DCE types.
//!
//! # Example
//!
//! ```no_run
//! use contracts_iceberg::{IcebergValidator, IcebergConfig};
//! use contracts_core::Contract;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure Iceberg connection
//! let config = IcebergConfig::builder()
//!     .table_location("s3://bucket/path/to/table")
//!     .build()?;
//!
//! // Create validator
//! let validator = IcebergValidator::new(config).await?;
//!
//! // Load contract
//! // let contract = ...;
//!
//! // Validate table against contract
//! // let report = validator.validate_table(&contract).await?;
//! # Ok(())
//! # }
//! ```

use thiserror::Error;

mod config;
mod converter;
mod schema;
mod validator;

pub use config::IcebergConfig;
pub use validator::IcebergValidator;

/// Error types specific to Iceberg operations.
#[derive(Error, Debug)]
pub enum IcebergError {
    /// Failed to connect to Iceberg catalog
    #[error("Failed to connect to Iceberg catalog: {0}")]
    ConnectionError(String),

    /// Table not found
    #[error("Iceberg table not found: {0}")]
    TableNotFound(String),

    /// Schema extraction failed
    #[error("Failed to extract schema from Iceberg table: {0}")]
    SchemaExtractionError(String),

    /// Type conversion error
    #[error("Failed to convert Iceberg type to DCE type: {0}")]
    TypeConversionError(String),

    /// Data reading error
    #[error("Failed to read data from Iceberg table: {0}")]
    DataReadError(String),

    /// Configuration error
    #[error("Invalid Iceberg configuration: {0}")]
    ConfigurationError(String),

    /// Unsupported operation
    #[error("Unsupported Iceberg operation: {0}")]
    UnsupportedOperation(String),

    /// Generic Iceberg error
    #[error("Iceberg error: {0}")]
    Other(String),
}

impl From<iceberg::Error> for IcebergError {
    fn from(err: iceberg::Error) -> Self {
        IcebergError::Other(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = IcebergError::TableNotFound("test_table".to_string());
        assert_eq!(err.to_string(), "Iceberg table not found: test_table");
    }

    #[test]
    fn test_error_from_iceberg() {
        let iceberg_err = iceberg::Error::new(iceberg::ErrorKind::Unexpected, "test error");
        let err = IcebergError::from(iceberg_err);
        assert!(matches!(err, IcebergError::Other(_)));
    }
}
