//! Dataset representation for validation.
//!
//! This module provides types for representing data to be validated against contracts.

use std::collections::HashMap;

/// A value in a dataset.
///
/// Represents different types of values that can appear in data records.
#[derive(Debug, Clone, PartialEq)]
pub enum DataValue {
    /// Null/missing value
    Null,
    /// String value
    String(String),
    /// Integer value
    Int(i64),
    /// Floating point value
    Float(f64),
    /// Boolean value
    Bool(bool),
    /// Timestamp value (ISO 8601 string)
    Timestamp(String),
    /// Map/struct value
    Map(HashMap<String, DataValue>),
    /// List/array value
    List(Vec<DataValue>),
}

impl DataValue {
    /// Returns true if this value is null.
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }

    /// Returns the type name of this value.
    pub fn type_name(&self) -> &'static str {
        match self {
            DataValue::Null => "null",
            DataValue::String(_) => "string",
            DataValue::Int(_) => "int64",
            DataValue::Float(_) => "float64",
            DataValue::Bool(_) => "boolean",
            DataValue::Timestamp(_) => "timestamp",
            DataValue::Map(_) => "map",
            DataValue::List(_) => "list",
        }
    }

    /// Attempts to get this value as a string.
    pub fn as_string(&self) -> Option<&str> {
        match self {
            DataValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Attempts to get this value as an integer.
    pub fn as_int(&self) -> Option<i64> {
        match self {
            DataValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    /// Attempts to get this value as a float.
    pub fn as_float(&self) -> Option<f64> {
        match self {
            DataValue::Float(f) => Some(*f),
            DataValue::Int(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Attempts to get this value as a boolean.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DataValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Attempts to get this value as a timestamp string.
    pub fn as_timestamp(&self) -> Option<&str> {
        match self {
            DataValue::Timestamp(s) => Some(s),
            _ => None,
        }
    }
}

impl From<String> for DataValue {
    fn from(s: String) -> Self {
        DataValue::String(s)
    }
}

impl From<&str> for DataValue {
    fn from(s: &str) -> Self {
        DataValue::String(s.to_string())
    }
}

impl From<i64> for DataValue {
    fn from(i: i64) -> Self {
        DataValue::Int(i)
    }
}

impl From<f64> for DataValue {
    fn from(f: f64) -> Self {
        DataValue::Float(f)
    }
}

impl From<bool> for DataValue {
    fn from(b: bool) -> Self {
        DataValue::Bool(b)
    }
}

/// A single row of data.
pub type DataRow = HashMap<String, DataValue>;

/// A dataset containing multiple rows.
///
/// Represents a collection of data records to be validated against a contract.
#[derive(Debug, Clone)]
pub struct DataSet {
    /// The data rows
    rows: Vec<DataRow>,
}

impl DataSet {
    /// Creates a new empty dataset.
    pub fn empty() -> Self {
        Self { rows: Vec::new() }
    }

    /// Creates a new dataset from rows.
    pub fn from_rows(rows: Vec<DataRow>) -> Self {
        Self { rows }
    }

    /// Returns the number of rows in the dataset.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if the dataset is empty.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns an iterator over the rows.
    pub fn rows(&self) -> impl Iterator<Item = &DataRow> {
        self.rows.iter()
    }

    /// Returns a mutable iterator over the rows.
    pub fn rows_mut(&mut self) -> impl Iterator<Item = &mut DataRow> {
        self.rows.iter_mut()
    }

    /// Gets a specific row by index.
    pub fn get_row(&self, index: usize) -> Option<&DataRow> {
        self.rows.get(index)
    }

    /// Adds a row to the dataset.
    pub fn add_row(&mut self, row: DataRow) {
        self.rows.push(row);
    }

    /// Takes a sample of rows from the dataset.
    ///
    /// If `size` is greater than the number of rows, returns all rows.
    pub fn sample(&self, size: usize) -> DataSet {
        let sample_size = size.min(self.rows.len());
        DataSet {
            rows: self.rows.iter().take(sample_size).cloned().collect(),
        }
    }
}

impl Default for DataSet {
    fn default() -> Self {
        Self::empty()
    }
}

impl FromIterator<DataRow> for DataSet {
    fn from_iter<T: IntoIterator<Item = DataRow>>(iter: T) -> Self {
        Self {
            rows: iter.into_iter().collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_value_types() {
        assert_eq!(DataValue::Null.type_name(), "null");
        assert_eq!(DataValue::String("test".into()).type_name(), "string");
        assert_eq!(DataValue::Int(42).type_name(), "int64");
        assert_eq!(DataValue::Float(3.5).type_name(), "float64");
        assert_eq!(DataValue::Bool(true).type_name(), "boolean");
    }

    #[test]
    fn test_data_value_conversions() {
        let val = DataValue::String("hello".into());
        assert_eq!(val.as_string(), Some("hello"));
        assert_eq!(val.as_int(), None);

        let val = DataValue::Int(42);
        assert_eq!(val.as_int(), Some(42));
        assert_eq!(val.as_float(), Some(42.0));
        assert_eq!(val.as_string(), None);
    }

    #[test]
    fn test_dataset_operations() {
        let mut dataset = DataSet::empty();
        assert_eq!(dataset.len(), 0);
        assert!(dataset.is_empty());

        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::Int(1));
        dataset.add_row(row);

        assert_eq!(dataset.len(), 1);
        assert!(!dataset.is_empty());

        let row = dataset.get_row(0).unwrap();
        assert_eq!(row.get("id"), Some(&DataValue::Int(1)));
    }

    #[test]
    fn test_dataset_sample() {
        let mut dataset = DataSet::empty();
        for i in 0..10 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), DataValue::Int(i));
            dataset.add_row(row);
        }

        let sample = dataset.sample(5);
        assert_eq!(sample.len(), 5);

        let large_sample = dataset.sample(100);
        assert_eq!(large_sample.len(), 10); // Only has 10 rows
    }
}
