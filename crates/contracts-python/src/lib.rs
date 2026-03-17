//! Python bindings for the Data Contracts Engine (DCE).
//!
//! Exposes the DCE validation engine to Python via PyO3, accepting
//! PyArrow RecordBatches for zero-copy data validation.

use arrow::array::RecordBatch;
use arrow::pyarrow::FromPyArrow;
use contracts_core::{Contract, ValidationContext};
use contracts_parser::{parse_toml, parse_yaml};
use contracts_validator::{DataSet, DataValidator, DataValue};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use std::collections::HashMap;

/// Convert an Arrow RecordBatch into a DCE DataSet.
fn record_batch_to_dataset(batch: &RecordBatch) -> PyResult<DataSet> {
    let schema = batch.schema();
    let num_rows = batch.num_rows();
    let mut rows = Vec::with_capacity(num_rows);

    for row_idx in 0..num_rows {
        let mut row = HashMap::new();
        for (col_idx, field) in schema.fields().iter().enumerate() {
            let col = batch.column(col_idx);
            let value = arrow_col_to_data_value(col.as_ref(), row_idx);
            row.insert(field.name().clone(), value);
        }
        rows.push(row);
    }

    Ok(DataSet::from_rows(rows))
}

/// Convert a single cell from an Arrow array into a DataValue.
fn arrow_col_to_data_value(array: &dyn arrow::array::Array, idx: usize) -> DataValue {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    if array.is_null(idx) {
        return DataValue::Null;
    }

    match array.data_type() {
        DataType::Boolean => {
            let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            DataValue::Bool(a.value(idx))
        }
        DataType::Int8 => {
            let a = array.as_any().downcast_ref::<Int8Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::Int16 => {
            let a = array.as_any().downcast_ref::<Int16Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::Int32 => {
            let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::Int64 => {
            let a = array.as_any().downcast_ref::<Int64Array>().unwrap();
            DataValue::Int(a.value(idx))
        }
        DataType::UInt8 => {
            let a = array.as_any().downcast_ref::<UInt8Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::UInt16 => {
            let a = array.as_any().downcast_ref::<UInt16Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::UInt32 => {
            let a = array.as_any().downcast_ref::<UInt32Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::UInt64 => {
            let a = array.as_any().downcast_ref::<UInt64Array>().unwrap();
            DataValue::Int(a.value(idx) as i64)
        }
        DataType::Float32 => {
            let a = array.as_any().downcast_ref::<Float32Array>().unwrap();
            DataValue::Float(a.value(idx) as f64)
        }
        DataType::Float64 => {
            let a = array.as_any().downcast_ref::<Float64Array>().unwrap();
            DataValue::Float(a.value(idx))
        }
        DataType::Utf8 => {
            let a = array.as_any().downcast_ref::<StringArray>().unwrap();
            DataValue::String(a.value(idx).to_owned())
        }
        DataType::LargeUtf8 => {
            let a = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
            DataValue::String(a.value(idx).to_owned())
        }
        DataType::Timestamp(_, _) => {
            let a = array.as_any();
            if let Some(arr) = a.downcast_ref::<TimestampMicrosecondArray>() {
                DataValue::Timestamp(
                    arr.value_as_datetime(idx)
                        .map(|d| d.to_string())
                        .unwrap_or_default(),
                )
            } else if let Some(arr) = a.downcast_ref::<TimestampMillisecondArray>() {
                DataValue::Timestamp(
                    arr.value_as_datetime(idx)
                        .map(|d| d.to_string())
                        .unwrap_or_default(),
                )
            } else if let Some(arr) = a.downcast_ref::<TimestampSecondArray>() {
                DataValue::Timestamp(
                    arr.value_as_datetime(idx)
                        .map(|d| d.to_string())
                        .unwrap_or_default(),
                )
            } else if let Some(arr) = a.downcast_ref::<TimestampNanosecondArray>() {
                DataValue::Timestamp(
                    arr.value_as_datetime(idx)
                        .map(|d| d.to_string())
                        .unwrap_or_default(),
                )
            } else {
                DataValue::Null
            }
        }
        DataType::Date32 => {
            let a = array.as_any().downcast_ref::<Date32Array>().unwrap();
            DataValue::Timestamp(
                a.value_as_date(idx)
                    .map(|d| d.to_string())
                    .unwrap_or_default(),
            )
        }
        DataType::Date64 => {
            let a = array.as_any().downcast_ref::<Date64Array>().unwrap();
            DataValue::Timestamp(
                a.value_as_datetime(idx)
                    .map(|d| d.to_string())
                    .unwrap_or_default(),
            )
        }
        _ => {
            // Fallback: represent unknown types as Null
            DataValue::Null
        }
    }
}

/// Build a ValidationContext from Python keyword arguments.
fn build_context(strict: bool, schema_only: bool, sample_size: Option<usize>) -> ValidationContext {
    let mut ctx = ValidationContext::new()
        .with_strict(strict)
        .with_schema_only(schema_only);
    if let Some(s) = sample_size {
        ctx = ctx.with_sample_size(s);
    }
    ctx
}

/// Serialise a ValidationReport into a Python dict.
fn report_to_pydict<'py>(
    py: Python<'py>,
    report: &contracts_core::ValidationReport,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("passed", report.passed)?;
    dict.set_item("errors", &report.errors)?;
    dict.set_item("warnings", &report.warnings)?;

    let stats = PyDict::new(py);
    stats.set_item("records_validated", report.stats.records_validated)?;
    stats.set_item("fields_checked", report.stats.fields_checked)?;
    stats.set_item("constraints_evaluated", report.stats.constraints_evaluated)?;
    stats.set_item("duration_ms", report.stats.duration_ms)?;
    dict.set_item("stats", stats)?;

    Ok(dict)
}

// ---------------------------------------------------------------------------
// Public Python API
// ---------------------------------------------------------------------------

/// Parse a YAML contract string and return the Contract as a JSON string.
#[pyfunction]
fn parse_contract_yaml(yaml: &str) -> PyResult<String> {
    let contract = parse_yaml(yaml).map_err(|e| PyValueError::new_err(e.to_string()))?;
    serde_json::to_string(&contract).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Parse a TOML contract string and return the Contract as a JSON string.
#[pyfunction]
fn parse_contract_toml(toml_str: &str) -> PyResult<String> {
    let contract = parse_toml(toml_str).map_err(|e| PyValueError::new_err(e.to_string()))?;
    serde_json::to_string(&contract).map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Validate a contract definition (no data required).
#[pyfunction]
fn validate_contract<'py>(py: Python<'py>, contract_yaml: &str) -> PyResult<Bound<'py, PyDict>> {
    let contract: Contract =
        parse_yaml(contract_yaml).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let validator = DataValidator::new();
    let report = validator.validate_definition(&contract);
    report_to_pydict(py, &report)
}

/// Validate a PyArrow RecordBatch against a YAML contract.
#[pyfunction]
#[pyo3(signature = (contract_yaml, batch, strict=false, schema_only=false, sample_size=None))]
fn validate_batch<'py>(
    py: Python<'py>,
    contract_yaml: &str,
    batch: Bound<'_, PyAny>,
    strict: bool,
    schema_only: bool,
    sample_size: Option<usize>,
) -> PyResult<Bound<'py, PyDict>> {
    let contract: Contract =
        parse_yaml(contract_yaml).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let rb = RecordBatch::from_pyarrow_bound(&batch)
        .map_err(|e| PyValueError::new_err(format!("Failed to convert PyArrow batch: {e}")))?;
    let dataset = record_batch_to_dataset(&rb)?;
    let ctx = build_context(strict, schema_only, sample_size);
    let mut validator = DataValidator::new();
    let report = validator.validate_with_data(&contract, &dataset, &ctx);
    report_to_pydict(py, &report)
}

/// Validate multiple PyArrow RecordBatches against a YAML contract.
#[pyfunction]
#[pyo3(signature = (contract_yaml, batches, strict=false, schema_only=false, sample_size=None))]
fn validate_batches<'py>(
    py: Python<'py>,
    contract_yaml: &str,
    batches: Vec<Bound<'_, PyAny>>,
    strict: bool,
    schema_only: bool,
    sample_size: Option<usize>,
) -> PyResult<Bound<'py, PyDict>> {
    let contract: Contract =
        parse_yaml(contract_yaml).map_err(|e| PyValueError::new_err(e.to_string()))?;

    let mut all_rows = Vec::new();
    for batch_obj in &batches {
        let rb = RecordBatch::from_pyarrow_bound(batch_obj)
            .map_err(|e| PyValueError::new_err(format!("Failed to convert PyArrow batch: {e}")))?;
        let ds = record_batch_to_dataset(&rb)?;
        for row in ds.rows() {
            all_rows.push(row.clone());
        }
    }
    let dataset = DataSet::from_rows(all_rows);
    let ctx = build_context(strict, schema_only, sample_size);
    let mut validator = DataValidator::new();
    let report = validator.validate_with_data(&contract, &dataset, &ctx);
    report_to_pydict(py, &report)
}

/// Data Contracts Engine — Python bindings.
///
/// Example
/// -------
/// >>> import pyarrow as pa
/// >>> import dce
/// >>>
/// >>> yaml = open("contract.yml").read()
/// >>> batch = pa.table({"user_id": ["a","b"]}).to_batches()[0]
/// >>> report = dce.validate_batch(yaml, batch)
/// >>> assert report["passed"]
#[pymodule]
fn dce(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(parse_contract_yaml, m)?)?;
    m.add_function(wrap_pyfunction!(parse_contract_toml, m)?)?;
    m.add_function(wrap_pyfunction!(validate_contract, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batch, m)?)?;
    m.add_function(wrap_pyfunction!(validate_batches, m)?)?;
    Ok(())
}
