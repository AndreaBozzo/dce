//! Python bindings for the Data Contracts Engine (DCE).
//!
//! Exposes the DCE validation engine to Python via PyO3, accepting
//! PyArrow RecordBatches for zero-copy data validation.

use arrow::array::RecordBatch;
use arrow::pyarrow::FromPyArrow;
use contracts_core::{Contract, ValidationContext, ValidationReport};
use contracts_parser::{parse_toml, parse_yaml};
use contracts_validator::{DataSet, DataValidator, DataValue};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use std::collections::HashMap;
use std::sync::OnceLock;

/// Global Tokio runtime shared across all Python function calls.
/// Avoids the overhead of creating a new runtime per invocation and
/// prevents conflicts with existing runtimes in the process.
fn tokio_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().expect("failed to create Tokio runtime"))
}

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

/// Serialise a ValidationReport into a Python dict with per-category breakdown.
fn report_to_pydict<'py>(
    py: Python<'py>,
    report: &ValidationReport,
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

    // Per-category breakdown of errors/warnings
    let checks = PyDict::new(py);

    let schema_errors: Vec<&String> = report
        .errors
        .iter()
        .filter(|e| {
            e.contains("is null but nullability")
                || e.contains("Missing required field")
                || e.contains("Type mismatch")
                || e.contains("not found in data")
        })
        .collect();
    let schema_dict = PyDict::new(py);
    schema_dict.set_item(
        "errors",
        schema_errors.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )?;
    checks.set_item("schema", schema_dict)?;

    let constraint_errors: Vec<&String> = report
        .errors
        .iter()
        .filter(|e| e.contains("Constraint violation"))
        .collect();
    let constraint_dict = PyDict::new(py);
    constraint_dict.set_item(
        "errors",
        constraint_errors
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>(),
    )?;
    checks.set_item("constraints", constraint_dict)?;

    let quality_items: Vec<&String> = report
        .errors
        .iter()
        .chain(report.warnings.iter())
        .filter(|e| e.contains("Quality check failed"))
        .collect();
    let quality_dict = PyDict::new(py);
    quality_dict.set_item(
        "issues",
        quality_items.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )?;
    checks.set_item("quality", quality_dict)?;

    let ml_items: Vec<&String> = report
        .errors
        .iter()
        .chain(report.warnings.iter())
        .filter(|e| {
            e.contains("NoOverlap")
                || e.contains("TemporalSplit")
                || e.contains("ClassBalance")
                || e.contains("FeatureDrift")
                || e.contains("TargetLeakage")
                || e.contains("NullRateByGroup")
        })
        .collect();
    let ml_dict = PyDict::new(py);
    ml_dict.set_item(
        "issues",
        ml_items.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )?;
    checks.set_item("ml", ml_dict)?;

    let custom_items: Vec<&String> = report
        .errors
        .iter()
        .chain(report.warnings.iter())
        .filter(|e| e.contains("Custom check") || e.contains("custom_check"))
        .collect();
    let custom_dict = PyDict::new(py);
    custom_dict.set_item(
        "issues",
        custom_items.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )?;
    checks.set_item("custom", custom_dict)?;

    dict.set_item("checks", checks)?;

    Ok(dict)
}

/// Parse a contract from YAML or TOML, auto-detecting format.
fn parse_contract(source: &str) -> PyResult<Contract> {
    // Try YAML first, fall back to TOML
    parse_yaml(source)
        .or_else(|_| parse_toml(source))
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

/// Convert a PyArrow batch argument to a DCE DataSet.
fn batch_to_dataset(batch: &Bound<'_, PyAny>) -> PyResult<DataSet> {
    let rb = RecordBatch::from_pyarrow_bound(batch)
        .map_err(|e| PyValueError::new_err(format!("Failed to convert PyArrow batch: {e}")))?;
    record_batch_to_dataset(&rb)
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
    let contract = parse_contract(contract_yaml)?;
    let validator = DataValidator::new();
    let report = validator.validate_definition(&contract);
    report_to_pydict(py, &report)
}

/// Validate a PyArrow RecordBatch against a YAML/TOML contract.
///
/// Uses the async DataFusion path for full custom SQL execution.
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
    let contract = parse_contract(contract_yaml)?;
    let dataset = batch_to_dataset(&batch)?;
    let ctx = build_context(strict, schema_only, sample_size);
    let mut validator = DataValidator::new();

    let report =
        tokio_runtime().block_on(validator.validate_with_data_async(&contract, &dataset, &ctx));

    report_to_pydict(py, &report)
}

/// Validate multiple PyArrow RecordBatches against a YAML/TOML contract.
///
/// Uses the async DataFusion path for full custom SQL execution.
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
    let contract = parse_contract(contract_yaml)?;

    let mut all_rows = Vec::new();
    for batch_obj in &batches {
        let ds = batch_to_dataset(batch_obj)?;
        for row in ds.rows() {
            all_rows.push(row.clone());
        }
    }
    let dataset = DataSet::from_rows(all_rows);
    let ctx = build_context(strict, schema_only, sample_size);
    let mut validator = DataValidator::new();

    let report =
        tokio_runtime().block_on(validator.validate_with_data_async(&contract, &dataset, &ctx));

    report_to_pydict(py, &report)
}

/// Validate only schema (field presence, types, nullability) against a batch.
#[pyfunction]
fn validate_schema_only<'py>(
    py: Python<'py>,
    contract_yaml: &str,
    batch: Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let contract = parse_contract(contract_yaml)?;
    let dataset = batch_to_dataset(&batch)?;
    let ctx = ValidationContext::new().with_schema_only(true);
    let mut validator = DataValidator::new();
    let report = validator.validate_with_data(&contract, &dataset, &ctx);
    report_to_pydict(py, &report)
}

/// Validate only quality checks (completeness, uniqueness, freshness, ML) against a batch.
#[pyfunction]
fn validate_quality_only<'py>(
    py: Python<'py>,
    contract_yaml: &str,
    batch: Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let contract = parse_contract(contract_yaml)?;
    let dataset = batch_to_dataset(&batch)?;
    let validator = DataValidator::new();
    let report = validator.validate_quality_only(&contract, &dataset);
    report_to_pydict(py, &report)
}

/// Validate only ML checks (overlap, temporal split, class balance, drift, leakage, null rate) against a batch.
#[pyfunction]
fn validate_ml_only<'py>(
    py: Python<'py>,
    contract_yaml: &str,
    batch: Bound<'_, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let contract = parse_contract(contract_yaml)?;
    let dataset = batch_to_dataset(&batch)?;
    let validator = DataValidator::new();
    let report = validator.validate_ml_only(&contract, &dataset);
    report_to_pydict(py, &report)
}

/// Lightweight profiling of a PyArrow RecordBatch.
///
/// Returns a dict with row_count and per-column stats:
/// null_count, unique_count, and for numeric columns: min, max, mean.
#[pyfunction]
fn profile_batch<'py>(py: Python<'py>, batch: Bound<'_, PyAny>) -> PyResult<Bound<'py, PyDict>> {
    let rb = RecordBatch::from_pyarrow_bound(&batch)
        .map_err(|e| PyValueError::new_err(format!("Failed to convert PyArrow batch: {e}")))?;

    let result = PyDict::new(py);
    let num_rows = rb.num_rows();
    result.set_item("row_count", num_rows)?;

    let columns = PyList::empty(py);

    for (col_idx, field) in rb.schema().fields().iter().enumerate() {
        let col = rb.column(col_idx);
        let col_dict = PyDict::new(py);
        col_dict.set_item("name", field.name().as_str())?;
        col_dict.set_item("type", format!("{}", field.data_type()))?;
        col_dict.set_item("null_count", col.null_count())?;

        // Compute unique count and numeric stats by iterating as DataValues
        let mut unique_values = std::collections::HashSet::new();
        let mut numeric_values: Vec<f64> = Vec::new();

        for row_idx in 0..num_rows {
            let val = arrow_col_to_data_value(col.as_ref(), row_idx);
            // Track unique non-null values via string repr
            match &val {
                DataValue::Null => {}
                DataValue::Int(i) => {
                    unique_values.insert(format!("i:{i}"));
                    numeric_values.push(*i as f64);
                }
                DataValue::Float(f) => {
                    unique_values.insert(format!("f:{f}"));
                    numeric_values.push(*f);
                }
                DataValue::Bool(b) => {
                    unique_values.insert(format!("b:{b}"));
                }
                DataValue::String(s) => {
                    unique_values.insert(format!("s:{s}"));
                }
                DataValue::Timestamp(t) => {
                    unique_values.insert(format!("t:{t}"));
                }
                DataValue::Map(_) | DataValue::List(_) => {
                    unique_values.insert(format!("c:{:?}", val));
                }
            }
        }

        col_dict.set_item("unique_count", unique_values.len())?;

        if !numeric_values.is_empty() {
            let min = numeric_values.iter().cloned().fold(f64::INFINITY, f64::min);
            let max = numeric_values
                .iter()
                .cloned()
                .fold(f64::NEG_INFINITY, f64::max);
            let mean = numeric_values.iter().sum::<f64>() / numeric_values.len() as f64;
            col_dict.set_item("min", min)?;
            col_dict.set_item("max", max)?;
            col_dict.set_item("mean", mean)?;
        } else {
            col_dict.set_item("min", py.None())?;
            col_dict.set_item("max", py.None())?;
            col_dict.set_item("mean", py.None())?;
        }

        columns.append(col_dict)?;
    }

    result.set_item("columns", columns)?;
    Ok(result)
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
    m.add_function(wrap_pyfunction!(validate_schema_only, m)?)?;
    m.add_function(wrap_pyfunction!(validate_quality_only, m)?)?;
    m.add_function(wrap_pyfunction!(validate_ml_only, m)?)?;
    m.add_function(wrap_pyfunction!(profile_batch, m)?)?;
    Ok(())
}
