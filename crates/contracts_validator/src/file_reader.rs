//! File-based data source registration for DataFusion validation.
//!
//! Registers local Parquet, CSV, and JSON (NDJSON) files as DataFusion tables
//! so they can be validated through the same SQL-based engine used for Iceberg.

use contracts_core::DataFormat;
use datafusion::prelude::*;
use tracing::info;

/// Registers a local file as a DataFusion table named `"data"`.
///
/// Uses DataFusion's built-in readers for Parquet, CSV, and NDJSON formats.
/// When `sample_size` is provided, the table is wrapped in a `LIMIT` view
/// (same pattern as the Iceberg native-datafusion path).
///
/// # Errors
///
/// Returns an error if the format is not supported for file-based validation
/// or if the file cannot be read.
pub async fn register_file_as_table(
    format: &DataFormat,
    path: &str,
    sample_size: Option<usize>,
) -> Result<SessionContext, String> {
    let ctx = SessionContext::new();

    let table_name = if sample_size.is_some() {
        "raw_data"
    } else {
        "data"
    };

    match format {
        DataFormat::Parquet => {
            info!("Registering Parquet file: {}", path);
            ctx.register_parquet(table_name, path, ParquetReadOptions::default())
                .await
                .map_err(|e| format!("Failed to register Parquet file '{path}': {e}"))?;
        }
        DataFormat::Csv => {
            info!("Registering CSV file: {}", path);
            ctx.register_csv(table_name, path, CsvReadOptions::default())
                .await
                .map_err(|e| format!("Failed to register CSV file '{path}': {e}"))?;
        }
        DataFormat::Json => {
            info!("Registering JSON (NDJSON) file: {}", path);
            ctx.register_json(table_name, path, NdJsonReadOptions::default())
                .await
                .map_err(|e| format!("Failed to register JSON file '{path}': {e}"))?;
        }
        other => {
            return Err(format!(
                "Format {other:?} is not supported for file-based validation. \
                 Supported formats: Parquet, CSV, JSON"
            ));
        }
    }

    if let Some(limit) = sample_size {
        info!("Applying sample size limit: {}", limit);
        ctx.sql(&format!(
            "CREATE VIEW data AS SELECT * FROM raw_data LIMIT {limit}"
        ))
        .await
        .map_err(|e| format!("Failed to create sampled view: {e}"))?
        .collect()
        .await
        .map_err(|e| format!("Failed to materialise sampled view: {e}"))?;
    }

    Ok(ctx)
}
