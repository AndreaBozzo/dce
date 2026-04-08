//! Type conversion between Iceberg and DCE types.

use crate::IcebergError;
use contracts_core::{DataType, PrimitiveType as DcePrimitiveType, StructField as DceStructField};
use contracts_validator::DataValue;
use iceberg::spec::{
    ListType, MapType, NestedField, PrimitiveType, StructType, Type as IcebergType,
};
use std::sync::Arc;
use tracing::warn;

/// Converts an Iceberg type to a DCE `DataType`.
///
/// Maps Iceberg's type system to the structured DCE type representation,
/// including recursive support for complex types (List, Map, Struct).
pub fn iceberg_type_to_dce_type(iceberg_type: &IcebergType) -> Result<DataType, IcebergError> {
    match iceberg_type {
        IcebergType::Primitive(prim) => Ok(DataType::Primitive(primitive_to_dce(prim))),

        IcebergType::Struct(struct_type) => {
            let fields = struct_type
                .fields()
                .iter()
                .map(|field| {
                    let data_type = iceberg_type_to_dce_type(&field.field_type)?;
                    Ok(DceStructField {
                        name: field.name.clone(),
                        data_type,
                        nullable: !field.required,
                    })
                })
                .collect::<Result<Vec<_>, IcebergError>>()?;

            Ok(DataType::Struct { fields })
        }

        IcebergType::List(list_type) => {
            let element_type = iceberg_type_to_dce_type(&list_type.element_field.field_type)?;
            Ok(DataType::List {
                element_type: Box::new(element_type),
                contains_null: !list_type.element_field.required,
            })
        }

        IcebergType::Map(map_type) => {
            let key_type = iceberg_type_to_dce_type(&map_type.key_field.field_type)?;
            let value_type = iceberg_type_to_dce_type(&map_type.value_field.field_type)?;
            Ok(DataType::Map {
                key_type: Box::new(key_type),
                value_type: Box::new(value_type),
                value_contains_null: !map_type.value_field.required,
            })
        }
    }
}

/// Converts an Iceberg primitive type to a DCE PrimitiveType.
fn primitive_to_dce(prim_type: &PrimitiveType) -> DcePrimitiveType {
    match prim_type {
        PrimitiveType::Boolean => DcePrimitiveType::Boolean,
        PrimitiveType::Int => DcePrimitiveType::Int32,
        PrimitiveType::Long => DcePrimitiveType::Int64,
        PrimitiveType::Float => DcePrimitiveType::Float32,
        PrimitiveType::Double => DcePrimitiveType::Float64,
        PrimitiveType::Decimal { .. } => DcePrimitiveType::Decimal,
        PrimitiveType::Date => DcePrimitiveType::Date,
        PrimitiveType::Time => DcePrimitiveType::Time,
        PrimitiveType::Timestamp => DcePrimitiveType::Timestamp,
        PrimitiveType::Timestamptz => DcePrimitiveType::Timestamp,
        PrimitiveType::TimestampNs => DcePrimitiveType::Timestamp,
        PrimitiveType::TimestamptzNs => DcePrimitiveType::Timestamp,
        PrimitiveType::String => DcePrimitiveType::String,
        PrimitiveType::Uuid => DcePrimitiveType::Uuid,
        PrimitiveType::Fixed(_) => DcePrimitiveType::Binary,
        PrimitiveType::Binary => DcePrimitiveType::Binary,
    }
}

// ---------------------------------------------------------------------------
// DCE -> Iceberg conversion
// ---------------------------------------------------------------------------

/// Converts a DCE `DataType` to an Iceberg `Type`.
///
/// This is the reverse of `iceberg_type_to_dce_type`. Field IDs in complex
/// types are generated starting from `field_id_start` and incrementing.
pub fn dce_type_to_iceberg_type(
    dce_type: &DataType,
    field_id_start: &mut i32,
) -> Result<IcebergType, IcebergError> {
    match dce_type {
        DataType::Primitive(prim) => Ok(IcebergType::Primitive(dce_to_primitive(prim))),

        DataType::Struct { fields } => {
            let iceberg_fields: Vec<Arc<NestedField>> = fields
                .iter()
                .map(|f| {
                    let id = *field_id_start;
                    *field_id_start += 1;
                    let field_type = dce_type_to_iceberg_type(&f.data_type, field_id_start)?;
                    let nf = if f.nullable {
                        NestedField::optional(id, &f.name, field_type)
                    } else {
                        NestedField::required(id, &f.name, field_type)
                    };
                    Ok(Arc::new(nf))
                })
                .collect::<Result<Vec<_>, IcebergError>>()?;

            Ok(IcebergType::Struct(StructType::new(iceberg_fields)))
        }

        DataType::List {
            element_type,
            contains_null,
        } => {
            let elem_id = *field_id_start;
            *field_id_start += 1;
            let elem_type = dce_type_to_iceberg_type(element_type, field_id_start)?;
            let elem_field = if *contains_null {
                NestedField::optional(elem_id, "element", elem_type)
            } else {
                NestedField::required(elem_id, "element", elem_type)
            };
            Ok(IcebergType::List(ListType::new(Arc::new(elem_field))))
        }

        DataType::Map {
            key_type,
            value_type,
            value_contains_null,
        } => {
            let key_id = *field_id_start;
            *field_id_start += 1;
            let iceberg_key = dce_type_to_iceberg_type(key_type, field_id_start)?;

            let val_id = *field_id_start;
            *field_id_start += 1;
            let iceberg_val = dce_type_to_iceberg_type(value_type, field_id_start)?;

            let key_field = Arc::new(NestedField::required(key_id, "key", iceberg_key));
            let val_field = if *value_contains_null {
                Arc::new(NestedField::optional(val_id, "value", iceberg_val))
            } else {
                Arc::new(NestedField::required(val_id, "value", iceberg_val))
            };
            Ok(IcebergType::Map(MapType::new(key_field, val_field)))
        }
    }
}

/// Converts a DCE primitive type to an Iceberg PrimitiveType.
fn dce_to_primitive(dce_prim: &DcePrimitiveType) -> PrimitiveType {
    match dce_prim {
        DcePrimitiveType::Boolean => PrimitiveType::Boolean,
        DcePrimitiveType::Int32 => PrimitiveType::Int,
        DcePrimitiveType::Int64 => PrimitiveType::Long,
        DcePrimitiveType::Float32 => PrimitiveType::Float,
        DcePrimitiveType::Float64 => PrimitiveType::Double,
        DcePrimitiveType::Decimal => PrimitiveType::Decimal {
            precision: 38,
            scale: 18,
        },
        DcePrimitiveType::Date => PrimitiveType::Date,
        DcePrimitiveType::Time => PrimitiveType::Time,
        DcePrimitiveType::Timestamp => PrimitiveType::Timestamptz,
        DcePrimitiveType::String => PrimitiveType::String,
        DcePrimitiveType::Uuid => PrimitiveType::Uuid,
        DcePrimitiveType::Binary => PrimitiveType::Binary,
    }
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
        arrow_schema::DataType::Timestamp(unit, _) => {
            use arrow_schema::TimeUnit;

            let datetime = match unit {
                TimeUnit::Second => {
                    let array = value
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| {
                            IcebergError::TypeConversionError(
                                "Failed to downcast to TimestampSecondArray".to_string(),
                            )
                        })?;
                    let ts_value = array.value(row_idx);
                    chrono::DateTime::from_timestamp(ts_value, 0)
                }
                TimeUnit::Millisecond => {
                    let array = value
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| {
                            IcebergError::TypeConversionError(
                                "Failed to downcast to TimestampMillisecondArray".to_string(),
                            )
                        })?;
                    let ts_value = array.value(row_idx);
                    chrono::DateTime::from_timestamp(
                        ts_value / 1_000,
                        ((ts_value % 1_000) * 1_000_000) as u32,
                    )
                }
                TimeUnit::Microsecond => {
                    let array = value
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| {
                            IcebergError::TypeConversionError(
                                "Failed to downcast to TimestampMicrosecondArray".to_string(),
                            )
                        })?;
                    let ts_value = array.value(row_idx);
                    chrono::DateTime::from_timestamp(
                        ts_value / 1_000_000,
                        ((ts_value % 1_000_000) * 1000) as u32,
                    )
                }
                TimeUnit::Nanosecond => {
                    let array = value
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| {
                            IcebergError::TypeConversionError(
                                "Failed to downcast to TimestampNanosecondArray".to_string(),
                            )
                        })?;
                    let ts_value = array.value(row_idx);
                    chrono::DateTime::from_timestamp(
                        ts_value / 1_000_000_000,
                        (ts_value % 1_000_000_000) as u32,
                    )
                }
            }
            .ok_or_else(|| {
                IcebergError::TypeConversionError("Invalid timestamp value".to_string())
            })?;

            Ok(DataValue::Timestamp(datetime.to_rfc3339()))
        }
        arrow_schema::DataType::Date32 => {
            // Date32 is days since Unix epoch
            let array = value
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to Date32Array".to_string(),
                    )
                })?;
            let days = array.value(row_idx);
            let datetime =
                chrono::DateTime::from_timestamp(days as i64 * 86400, 0).ok_or_else(|| {
                    IcebergError::TypeConversionError("Invalid date value".to_string())
                })?;
            Ok(DataValue::String(datetime.format("%Y-%m-%d").to_string()))
        }
        arrow_schema::DataType::Date64 => {
            // Date64 is milliseconds since Unix epoch
            let array = value
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to Date64Array".to_string(),
                    )
                })?;
            let millis = array.value(row_idx);
            let datetime =
                chrono::DateTime::from_timestamp(millis / 1000, (millis % 1000) as u32 * 1_000_000)
                    .ok_or_else(|| {
                        IcebergError::TypeConversionError("Invalid date value".to_string())
                    })?;
            Ok(DataValue::String(datetime.format("%Y-%m-%d").to_string()))
        }
        arrow_schema::DataType::Decimal128(_precision, scale) => {
            let array = value
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to Decimal128Array".to_string(),
                    )
                })?;
            let decimal_value = array.value(row_idx);
            // Convert to float for validation purposes
            let divisor = 10_i128.pow(*scale as u32);
            let float_value = decimal_value as f64 / divisor as f64;
            Ok(DataValue::Float(float_value))
        }
        arrow_schema::DataType::Decimal256(_precision, _scale) => {
            let array = value
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| {
                    IcebergError::TypeConversionError(
                        "Failed to downcast to Decimal256Array".to_string(),
                    )
                })?;
            // Decimal256 values are represented as i256, convert to string for precision
            let decimal_str = array.value_as_string(row_idx);
            // Try to parse as float for validation
            let float_value = decimal_str.parse::<f64>().map_err(|_| {
                IcebergError::TypeConversionError("Failed to parse Decimal256 value".to_string())
            })?;
            Ok(DataValue::Float(float_value))
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
            primitive_to_dce(&PrimitiveType::Boolean),
            DcePrimitiveType::Boolean,
        );
        assert_eq!(
            primitive_to_dce(&PrimitiveType::Int),
            DcePrimitiveType::Int32,
        );
        assert_eq!(
            primitive_to_dce(&PrimitiveType::Long),
            DcePrimitiveType::Int64,
        );
        assert_eq!(
            primitive_to_dce(&PrimitiveType::String),
            DcePrimitiveType::String,
        );
        assert_eq!(
            primitive_to_dce(&PrimitiveType::Timestamp),
            DcePrimitiveType::Timestamp,
        );
    }

    #[test]
    fn test_iceberg_type_conversion() {
        let result = iceberg_type_to_dce_type(&IcebergType::Primitive(PrimitiveType::Long));
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            DataType::Primitive(DcePrimitiveType::Int64)
        );
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

    // ---- DCE -> Iceberg (reverse conversion) tests ----

    #[test]
    fn test_dce_primitive_to_iceberg() {
        assert_eq!(
            dce_to_primitive(&DcePrimitiveType::Boolean),
            PrimitiveType::Boolean
        );
        assert_eq!(
            dce_to_primitive(&DcePrimitiveType::Int32),
            PrimitiveType::Int
        );
        assert_eq!(
            dce_to_primitive(&DcePrimitiveType::Int64),
            PrimitiveType::Long
        );
        assert_eq!(
            dce_to_primitive(&DcePrimitiveType::Float64),
            PrimitiveType::Double
        );
        assert_eq!(
            dce_to_primitive(&DcePrimitiveType::String),
            PrimitiveType::String
        );
        assert_eq!(
            dce_to_primitive(&DcePrimitiveType::Timestamp),
            PrimitiveType::Timestamptz
        );
    }

    #[test]
    fn test_dce_type_to_iceberg_primitive() {
        let mut id = 1;
        let result =
            dce_type_to_iceberg_type(&DataType::Primitive(DcePrimitiveType::Int64), &mut id);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), IcebergType::Primitive(PrimitiveType::Long));
        // Primitive doesn't consume field IDs
        assert_eq!(id, 1);
    }

    #[test]
    fn test_dce_roundtrip_primitive() {
        let original = IcebergType::Primitive(PrimitiveType::String);
        let dce = iceberg_type_to_dce_type(&original).unwrap();
        let mut id = 1;
        let back = dce_type_to_iceberg_type(&dce, &mut id).unwrap();
        assert_eq!(original, back);
    }
}
