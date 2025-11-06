//! Schema extraction from Iceberg tables.

use crate::{converter::iceberg_type_to_dce_type, IcebergError};
use contracts_core::{DataFormat, Field as ContractField, Schema as ContractSchema};
use iceberg::spec::{NestedField, Schema as IcebergSchema};
use tracing::{debug, info};

/// Extracts a DCE schema from an Iceberg table schema.
///
/// Converts Iceberg's schema representation to the DCE contract schema format.
pub fn extract_schema_from_iceberg(
    iceberg_schema: &IcebergSchema,
    location: &str,
) -> Result<ContractSchema, IcebergError> {
    info!("Extracting schema from Iceberg table at {}", location);

    let mut fields = Vec::new();

    // Access fields via as_struct() which returns the struct type containing fields
    let struct_type = iceberg_schema.as_struct();
    for field in struct_type.fields() {
        let contract_field = convert_iceberg_field(field)?;
        fields.push(contract_field);
    }

    debug!("Extracted {} fields from Iceberg schema", fields.len());

    Ok(ContractSchema {
        fields,
        format: DataFormat::Iceberg,
        location: location.to_string(),
    })
}

/// Converts an Iceberg field to a DCE contract field.
fn convert_iceberg_field(field: &NestedField) -> Result<ContractField, IcebergError> {
    let field_type = iceberg_type_to_dce_type(&field.field_type)?;

    debug!(
        "Converting field: {} (Iceberg type: {:?} -> DCE type: {})",
        field.name, field.field_type, field_type
    );

    Ok(ContractField {
        name: field.name.clone(),
        field_type,
        nullable: !field.required,
        description: field.doc.clone(),
        tags: None,
        constraints: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type as IcebergType};

    #[test]
    fn test_convert_simple_field() {
        let field = NestedField::required(1, "id", IcebergType::Primitive(PrimitiveType::Long));

        let result = convert_iceberg_field(&field);
        assert!(result.is_ok());

        let contract_field = result.unwrap();
        assert_eq!(contract_field.name, "id");
        assert_eq!(contract_field.field_type, "int64");
        assert!(!contract_field.nullable);
    }

    #[test]
    fn test_convert_nullable_field() {
        let field = NestedField::optional(2, "name", IcebergType::Primitive(PrimitiveType::String));

        let result = convert_iceberg_field(&field);
        assert!(result.is_ok());

        let contract_field = result.unwrap();
        assert_eq!(contract_field.name, "name");
        assert_eq!(contract_field.field_type, "string");
        assert!(contract_field.nullable);
    }

    #[test]
    fn test_convert_field_with_doc() {
        let field = NestedField::required(
            3,
            "created_at",
            IcebergType::Primitive(PrimitiveType::Timestamp),
        )
        .with_doc("Creation timestamp");

        let result = convert_iceberg_field(&field);
        assert!(result.is_ok());

        let contract_field = result.unwrap();
        assert_eq!(contract_field.name, "created_at");
        assert_eq!(contract_field.field_type, "timestamp");
        assert_eq!(
            contract_field.description,
            Some("Creation timestamp".to_string())
        );
    }

    #[test]
    fn test_extract_schema() {
        use std::sync::Arc;

        let iceberg_schema = IcebergSchema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    IcebergType::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    IcebergType::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "active",
                    IcebergType::Primitive(PrimitiveType::Boolean),
                )),
            ])
            .build()
            .unwrap();

        let result = extract_schema_from_iceberg(&iceberg_schema, "s3://test/table");
        assert!(result.is_ok());

        let schema = result.unwrap();
        assert_eq!(schema.fields.len(), 3);
        assert_eq!(schema.format, DataFormat::Iceberg);
        assert_eq!(schema.location, "s3://test/table");

        assert_eq!(schema.fields[0].name, "id");
        assert_eq!(schema.fields[1].name, "name");
        assert_eq!(schema.fields[2].name, "active");
    }
}
