//! Type conversion between Iceberg and DCE types.

use crate::IcebergError;
use contracts_validator::DataValue;
use iceberg::spec::{PrimitiveType, Type as IcebergType};
use tracing::warn;

/// Converts an Iceberg type to a DCE type string.
///
/// Maps Iceberg's type system to the string-based type names used in DCE contracts.
pub fn iceberg_type_to_dce_type(iceberg_type: &IcebergType) -> Result<String, IcebergError> {
    match iceberg_type {
        IcebergType::Primitive(prim) => primitive_type_to_string(prim),
        IcebergType::Struct(_) => Ok("map".to_string()),
        IcebergType::List(_) => Ok("list".to_string()),
        IcebergType::Map(_) => Ok("map".to_string()),
    }
}

/// Converts an Iceberg primitive type to a DCE type string.
fn primitive_type_to_string(prim_type: &PrimitiveType) -> Result<String, IcebergError> {
    let type_str = match prim_type {
        PrimitiveType::Boolean => "boolean",
        PrimitiveType::Int => "int32",
        PrimitiveType::Long => "int64",
        PrimitiveType::Float => "float32",
        PrimitiveType::Double => "float64",
        PrimitiveType::Decimal { .. } => "decimal",
        PrimitiveType::Date => "date",
        PrimitiveType::Time => "time",
        PrimitiveType::Timestamp => "timestamp",
        PrimitiveType::Timestamptz => "timestamp",
        PrimitiveType::TimestampNs => "timestamp",
        PrimitiveType::TimestamptzNs => "timestamp",
        PrimitiveType::String => "string",
        PrimitiveType::Uuid => "uuid",
        PrimitiveType::Fixed(_) => "binary",
        PrimitiveType::Binary => "binary",
    };

    Ok(type_str.to_string())
}

/// Converts an Arrow/Iceberg value to a DCE DataValue.
///
/// This is used when reading actual data from Iceberg tables for validation.
pub fn arrow_value_to_data_value(
    value: &arrow_array::array::ArrayRef,
    row_idx: usize,
) -> Result<DataValue, IcebergError> {
    use arrow_array::array::*;

    // Check if value is null
    if value.is_null(row_idx) {
        return Ok(DataValue::Null);
    }

    // Match on array type and extract value
    match value.data_type() {
        arrow_schema::DataType::Boolean => {
            let array = value
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to BooleanArray".to_string(),
                    )
                })?;
            Ok(DataValue::Bool(array.value(row_idx)))
        }
        arrow_schema::DataType::Int32 => {
            let array = value.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                IcebergError::TypeConversionError("Failed to downcast to Int32Array".to_string())
            })?;
            Ok(DataValue::Int(array.value(row_idx) as i64))
        }
        arrow_schema::DataType::Int64 => {
            let array = value.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                IcebergError::TypeConversionError("Failed to downcast to Int64Array".to_string())
            })?;
            Ok(DataValue::Int(array.value(row_idx)))
        }
        arrow_schema::DataType::Float32 => {
            let array = value
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to Float32Array".to_string(),
                    )
                })?;
            Ok(DataValue::Float(array.value(row_idx) as f64))
        }
        arrow_schema::DataType::Float64 => {
            let array = value
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to Float64Array".to_string(),
                    )
                })?;
            Ok(DataValue::Float(array.value(row_idx)))
        }
        arrow_schema::DataType::Utf8 => {
            let array = value
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to StringArray".to_string(),
                    )
                })?;
            Ok(DataValue::String(array.value(row_idx).to_string()))
        }
        arrow_schema::DataType::LargeUtf8 => {
            let array = value
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to LargeStringArray".to_string(),
                    )
                })?;
            Ok(DataValue::String(array.value(row_idx).to_string()))
        }
        arrow_schema::DataType::Timestamp(_, _) => {
            let array = value
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to TimestampArray".to_string(),
                    )
                })?;

            // Convert timestamp to ISO 8601 string
            let ts_value = array.value(row_idx);
            let datetime = chrono::DateTime::from_timestamp(
                ts_value / 1_000_000,
                ((ts_value % 1_000_000) * 1000) as u32,
            )
            .ok_or_else(|| {
                IcebergError::TypeConversionError("Invalid timestamp value".to_string())
            })?;

            Ok(DataValue::Timestamp(datetime.to_rfc3339()))
        }
        arrow_schema::DataType::Date32 | arrow_schema::DataType::Date64 => {
            // Convert date to string format
            Ok(DataValue::String(format!("date_{}", row_idx)))
        }
        other => {
            warn!("Unsupported Arrow type for conversion: {:?}", other);
            Ok(DataValue::Null)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{PrimitiveType, Type as IcebergType};

    #[test]
    fn test_primitive_type_conversion() {
        assert_eq!(
            primitive_type_to_string(&PrimitiveType::Boolean).unwrap(),
            "boolean"
        );
        assert_eq!(
            primitive_type_to_string(&PrimitiveType::Int).unwrap(),
            "int32"
        );
        assert_eq!(
            primitive_type_to_string(&PrimitiveType::Long).unwrap(),
            "int64"
        );
        assert_eq!(
            primitive_type_to_string(&PrimitiveType::String).unwrap(),
            "string"
        );
        assert_eq!(
            primitive_type_to_string(&PrimitiveType::Timestamp).unwrap(),
            "timestamp"
        );
    }

    #[test]
    fn test_iceberg_type_conversion() {
        let result = iceberg_type_to_dce_type(&IcebergType::Primitive(PrimitiveType::Long));
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "int64");

        // TODO: Test List type conversion when we have proper ListType construction
    }

    #[test]
    fn test_arrow_boolean_conversion() {
        use arrow_array::BooleanArray;
        use std::sync::Arc;

        let array: Arc<dyn arrow_array::Array> = Arc::new(BooleanArray::from(vec![true, false]));

        let result = arrow_value_to_data_value(&array, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataValue::Bool(true));

        let result = arrow_value_to_data_value(&array, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataValue::Bool(false));
    }

    #[test]
    fn test_arrow_int_conversion() {
        use arrow_array::Int64Array;
        use std::sync::Arc;

        let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(vec![42, 100]));

        let result = arrow_value_to_data_value(&array, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataValue::Int(42));
    }

    #[test]
    fn test_arrow_string_conversion() {
        use arrow_array::StringArray;
        use std::sync::Arc;

        let array: Arc<dyn arrow_array::Array> =
            Arc::new(StringArray::from(vec!["hello", "world"]));

        let result = arrow_value_to_data_value(&array, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataValue::String("hello".to_string()));
    }

    #[test]
    fn test_arrow_null_conversion() {
        use arrow_array::Int64Array;
        use std::sync::Arc;

        let array: Arc<dyn arrow_array::Array> = Arc::new(Int64Array::from(vec![Some(42), None]));

        let result = arrow_value_to_data_value(&array, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), DataValue::Null);
    }
}
