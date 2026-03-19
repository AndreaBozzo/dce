//! Recursive data type representation for schema fields.
//!
//! Provides a type-safe alternative to string-based type definitions,
//! with support for complex nested types (List, Map, Struct).

use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use std::fmt;

/// Canonical primitive data types supported by DCE.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum PrimitiveType {
    String,
    Int32,
    Int64,
    Float32,
    Float64,
    Boolean,
    Timestamp,
    Date,
    Time,
    Decimal,
    Uuid,
    Binary,
}

/// A recursive data type for schema fields.
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    /// A primitive (scalar) type.
    Primitive(PrimitiveType),
    /// An ordered list of elements.
    List {
        element_type: Box<DataType>,
        contains_null: bool,
    },
    /// A key-value map.
    Map {
        key_type: Box<DataType>,
        value_type: Box<DataType>,
        value_contains_null: bool,
    },
    /// A nested struct with named fields.
    Struct { fields: Vec<StructField> },
}

/// A named field inside a Struct DataType.
#[derive(Debug, Clone, PartialEq)]
pub struct StructField {
    pub name: std::string::String,
    pub data_type: DataType,
    pub nullable: bool,
}

// ---------------------------------------------------------------------------
// Display — canonical string representation
// ---------------------------------------------------------------------------

impl fmt::Display for PrimitiveType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            PrimitiveType::String => "string",
            PrimitiveType::Int32 => "int32",
            PrimitiveType::Int64 => "int64",
            PrimitiveType::Float32 => "float32",
            PrimitiveType::Float64 => "float64",
            PrimitiveType::Boolean => "boolean",
            PrimitiveType::Timestamp => "timestamp",
            PrimitiveType::Date => "date",
            PrimitiveType::Time => "time",
            PrimitiveType::Decimal => "decimal",
            PrimitiveType::Uuid => "uuid",
            PrimitiveType::Binary => "binary",
        };
        write!(f, "{}", s)
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Primitive(p) => write!(f, "{}", p),
            DataType::List { element_type, .. } => write!(f, "list<{}>", element_type),
            DataType::Map {
                key_type,
                value_type,
                ..
            } => write!(f, "map<{},{}>", key_type, value_type),
            DataType::Struct { fields } => {
                write!(f, "struct<")?;
                for (i, field) in fields.iter().enumerate() {
                    if i > 0 {
                        write!(f, ",")?;
                    }
                    write!(f, "{}:{}", field.name, field.data_type)?;
                }
                write!(f, ">")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Parsing — recursive descent from string representation
// ---------------------------------------------------------------------------

/// Parse a type string into a `DataType`.
pub fn parse_data_type(input: &str) -> Result<DataType, std::string::String> {
    let input = input.trim().to_lowercase();
    if input.is_empty() {
        return Err("empty type string".into());
    }
    parse_type_inner(&input)
}

fn parse_type_inner(input: &str) -> Result<DataType, String> {
    if let Some(inner) =
        strip_wrapper(input, "list<", ">").or_else(|| strip_wrapper(input, "array<", ">"))
    {
        let element = parse_type_inner(inner.trim())?;
        return Ok(DataType::List {
            element_type: Box::new(element),
            contains_null: true,
        });
    }

    if let Some(inner) = strip_wrapper(input, "map<", ">") {
        let parts = split_at_depth_zero(inner, ',')?;
        if parts.len() != 2 {
            return Err(format!(
                "map type expects exactly 2 type parameters, got {}: '{}'",
                parts.len(),
                input
            ));
        }
        let key = parse_type_inner(parts[0].trim())?;
        let value = parse_type_inner(parts[1].trim())?;
        return Ok(DataType::Map {
            key_type: Box::new(key),
            value_type: Box::new(value),
            value_contains_null: true,
        });
    }

    if let Some(inner) = strip_wrapper(input, "struct<", ">") {
        let parts = split_at_depth_zero(inner, ',')?;
        let mut fields = Vec::with_capacity(parts.len());
        for part in parts {
            let part = part.trim();
            let colon_pos = part
                .find(':')
                .ok_or_else(|| format!("struct field '{}' missing ':' separator", part))?;
            let name = part[..colon_pos].trim().to_string();
            let type_str = part[colon_pos + 1..].trim();
            let data_type = parse_type_inner(type_str)?;
            fields.push(StructField {
                name,
                data_type,
                nullable: true,
            });
        }
        return Ok(DataType::Struct { fields });
    }

    // Primitive type with alias resolution
    match input {
        "string" | "varchar" | "text" => Ok(DataType::Primitive(PrimitiveType::String)),
        "int" | "int32" | "integer" => Ok(DataType::Primitive(PrimitiveType::Int32)),
        "int64" | "long" | "bigint" => Ok(DataType::Primitive(PrimitiveType::Int64)),
        "float" | "float32" => Ok(DataType::Primitive(PrimitiveType::Float32)),
        "float64" | "double" => Ok(DataType::Primitive(PrimitiveType::Float64)),
        "boolean" | "bool" => Ok(DataType::Primitive(PrimitiveType::Boolean)),
        "timestamp" | "datetime" => Ok(DataType::Primitive(PrimitiveType::Timestamp)),
        "date" => Ok(DataType::Primitive(PrimitiveType::Date)),
        "time" => Ok(DataType::Primitive(PrimitiveType::Time)),
        "decimal" => Ok(DataType::Primitive(PrimitiveType::Decimal)),
        "uuid" => Ok(DataType::Primitive(PrimitiveType::Uuid)),
        "binary" => Ok(DataType::Primitive(PrimitiveType::Binary)),
        // Lenient: accept unknown types wrapped as String primitive to maintain backward compat
        _ => Ok(DataType::Primitive(PrimitiveType::String)),
    }
}

/// Strip a known prefix and suffix (the angle-bracket wrapper), returning the inner content.
/// Only succeeds if the brackets are balanced.
fn strip_wrapper<'a>(input: &'a str, prefix: &str, suffix: &str) -> Option<&'a str> {
    let s = input.strip_prefix(prefix)?;
    let s = s.strip_suffix(suffix)?;
    // Verify brackets are balanced within the inner content
    let mut depth = 0i32;
    for ch in s.chars() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth < 0 {
                    return None;
                }
            }
            _ => {}
        }
    }
    if depth == 0 { Some(s) } else { None }
}

/// Split a string by `delimiter` only at angle-bracket depth zero.
fn split_at_depth_zero(input: &str, delimiter: char) -> Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut depth = 0i32;
    let mut start = 0;

    for (i, ch) in input.char_indices() {
        match ch {
            '<' => depth += 1,
            '>' => {
                depth -= 1;
                if depth < 0 {
                    return Err(format!("unbalanced '>' in type: '{}'", input));
                }
            }
            c if c == delimiter && depth == 0 => {
                parts.push(&input[start..i]);
                start = i + ch.len_utf8();
            }
            _ => {}
        }
    }
    if depth != 0 {
        return Err(format!("unbalanced '<' in type: '{}'", input));
    }
    parts.push(&input[start..]);
    Ok(parts)
}

// ---------------------------------------------------------------------------
// From<&str> / From<String> — panicking conversion for builder ergonomics
// ---------------------------------------------------------------------------

impl From<&str> for DataType {
    fn from(s: &str) -> Self {
        parse_data_type(s).unwrap_or_else(|e| panic!("invalid data type '{}': {}", s, e))
    }
}

impl From<String> for DataType {
    fn from(s: String) -> Self {
        DataType::from(s.as_str())
    }
}

// ---------------------------------------------------------------------------
// Serde — serialize as string, deserialize by parsing
// ---------------------------------------------------------------------------

impl Serialize for DataType {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for DataType {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        parse_data_type(&s).map_err(de::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_primitive_types() {
        assert_eq!(
            parse_data_type("string").unwrap(),
            DataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            parse_data_type("int").unwrap(),
            DataType::Primitive(PrimitiveType::Int32)
        );
        assert_eq!(
            parse_data_type("int32").unwrap(),
            DataType::Primitive(PrimitiveType::Int32)
        );
        assert_eq!(
            parse_data_type("integer").unwrap(),
            DataType::Primitive(PrimitiveType::Int32)
        );
        assert_eq!(
            parse_data_type("int64").unwrap(),
            DataType::Primitive(PrimitiveType::Int64)
        );
        assert_eq!(
            parse_data_type("long").unwrap(),
            DataType::Primitive(PrimitiveType::Int64)
        );
        assert_eq!(
            parse_data_type("bigint").unwrap(),
            DataType::Primitive(PrimitiveType::Int64)
        );
        assert_eq!(
            parse_data_type("float").unwrap(),
            DataType::Primitive(PrimitiveType::Float32)
        );
        assert_eq!(
            parse_data_type("float32").unwrap(),
            DataType::Primitive(PrimitiveType::Float32)
        );
        assert_eq!(
            parse_data_type("float64").unwrap(),
            DataType::Primitive(PrimitiveType::Float64)
        );
        assert_eq!(
            parse_data_type("double").unwrap(),
            DataType::Primitive(PrimitiveType::Float64)
        );
        assert_eq!(
            parse_data_type("boolean").unwrap(),
            DataType::Primitive(PrimitiveType::Boolean)
        );
        assert_eq!(
            parse_data_type("bool").unwrap(),
            DataType::Primitive(PrimitiveType::Boolean)
        );
        assert_eq!(
            parse_data_type("timestamp").unwrap(),
            DataType::Primitive(PrimitiveType::Timestamp)
        );
        assert_eq!(
            parse_data_type("datetime").unwrap(),
            DataType::Primitive(PrimitiveType::Timestamp)
        );
        assert_eq!(
            parse_data_type("date").unwrap(),
            DataType::Primitive(PrimitiveType::Date)
        );
        assert_eq!(
            parse_data_type("uuid").unwrap(),
            DataType::Primitive(PrimitiveType::Uuid)
        );
        assert_eq!(
            parse_data_type("binary").unwrap(),
            DataType::Primitive(PrimitiveType::Binary)
        );
    }

    #[test]
    fn test_parse_case_insensitive() {
        assert_eq!(
            parse_data_type("STRING").unwrap(),
            DataType::Primitive(PrimitiveType::String)
        );
        assert_eq!(
            parse_data_type("Int64").unwrap(),
            DataType::Primitive(PrimitiveType::Int64)
        );
        assert_eq!(
            parse_data_type("  Boolean  ").unwrap(),
            DataType::Primitive(PrimitiveType::Boolean)
        );
    }

    #[test]
    fn test_parse_list() {
        assert_eq!(
            parse_data_type("list<string>").unwrap(),
            DataType::List {
                element_type: Box::new(DataType::Primitive(PrimitiveType::String)),
                contains_null: true,
            }
        );
        assert_eq!(
            parse_data_type("array<int64>").unwrap(),
            DataType::List {
                element_type: Box::new(DataType::Primitive(PrimitiveType::Int64)),
                contains_null: true,
            }
        );
    }

    #[test]
    fn test_parse_map() {
        assert_eq!(
            parse_data_type("map<string,int64>").unwrap(),
            DataType::Map {
                key_type: Box::new(DataType::Primitive(PrimitiveType::String)),
                value_type: Box::new(DataType::Primitive(PrimitiveType::Int64)),
                value_contains_null: true,
            }
        );
    }

    #[test]
    fn test_parse_struct() {
        assert_eq!(
            parse_data_type("struct<name:string,age:int32>").unwrap(),
            DataType::Struct {
                fields: vec![
                    StructField {
                        name: "name".into(),
                        data_type: DataType::Primitive(PrimitiveType::String),
                        nullable: true,
                    },
                    StructField {
                        name: "age".into(),
                        data_type: DataType::Primitive(PrimitiveType::Int32),
                        nullable: true,
                    },
                ]
            }
        );
    }

    #[test]
    fn test_parse_nested() {
        // list<map<string,int>>
        let dt = parse_data_type("list<map<string,int32>>").unwrap();
        assert_eq!(
            dt,
            DataType::List {
                element_type: Box::new(DataType::Map {
                    key_type: Box::new(DataType::Primitive(PrimitiveType::String)),
                    value_type: Box::new(DataType::Primitive(PrimitiveType::Int32)),
                    value_contains_null: true,
                }),
                contains_null: true,
            }
        );

        // map<string,list<int>>
        let dt = parse_data_type("map<string,list<int64>>").unwrap();
        assert_eq!(
            dt,
            DataType::Map {
                key_type: Box::new(DataType::Primitive(PrimitiveType::String)),
                value_type: Box::new(DataType::List {
                    element_type: Box::new(DataType::Primitive(PrimitiveType::Int64)),
                    contains_null: true,
                }),
                value_contains_null: true,
            }
        );
    }

    #[test]
    fn test_display_round_trip() {
        let types = vec![
            "string",
            "int64",
            "list<string>",
            "map<string,int32>",
            "struct<name:string,age:int32>",
            "list<map<string,int64>>",
            "map<string,list<float64>>",
        ];
        for t in types {
            let dt = parse_data_type(t).unwrap();
            let s = dt.to_string();
            let dt2 = parse_data_type(&s).unwrap();
            assert_eq!(dt, dt2, "round-trip failed for '{}'", t);
        }
    }

    #[test]
    fn test_from_str() {
        let dt: DataType = "list<string>".into();
        assert_eq!(
            dt,
            DataType::List {
                element_type: Box::new(DataType::Primitive(PrimitiveType::String)),
                contains_null: true,
            }
        );
    }

    #[test]
    fn test_serde_round_trip() {
        let dt = DataType::Map {
            key_type: Box::new(DataType::Primitive(PrimitiveType::String)),
            value_type: Box::new(DataType::Primitive(PrimitiveType::Int64)),
            value_contains_null: true,
        };
        let json = serde_json::to_string(&dt).unwrap();
        assert_eq!(json, "\"map<string,int64>\"");
        let dt2: DataType = serde_json::from_str(&json).unwrap();
        assert_eq!(dt, dt2);
    }
}
