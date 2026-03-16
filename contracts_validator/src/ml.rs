//! ML-specific validation logic.
//!
//! This module handles validation of machine learning quality checks:
//! - NoOverlap: Ensures train/test/val splits share no rows on key fields
//! - TemporalSplit: Validates chronological ordering between splits
//! - ClassBalance: Checks that label distributions are not overly skewed

use crate::{DataSet, DataValue, ValidationError};
use contracts_core::{ClassBalanceCheck, MlChecks, NoOverlapCheck, TemporalSplitCheck};
use std::collections::{HashMap, HashSet};

/// Validates ML-specific quality checks on a dataset.
pub struct MlValidator;

impl MlValidator {
    pub fn new() -> Self {
        Self
    }

    /// Validates all ML checks in the given `MlChecks` against a dataset.
    pub fn validate(&self, ml_checks: &MlChecks, dataset: &DataSet) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        if dataset.is_empty() {
            return errors;
        }

        if let Some(ref check) = ml_checks.no_overlap {
            errors.extend(self.validate_no_overlap(check, dataset));
        }

        if let Some(ref check) = ml_checks.temporal_split {
            errors.extend(self.validate_temporal_split(check, dataset));
        }

        if let Some(ref check) = ml_checks.class_balance {
            errors.extend(self.validate_class_balance(check, dataset));
        }

        errors
    }

    /// Validates that key fields do not overlap across splits.
    fn validate_no_overlap(
        &self,
        check: &NoOverlapCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        // Group composite keys by split value
        let mut keys_per_split: HashMap<String, HashSet<String>> = HashMap::new();

        for row in dataset.rows() {
            let split_val = match row.get(&check.split_field) {
                Some(v) => value_to_key(v),
                None => continue,
            };

            let composite_key: String = check
                .key_fields
                .iter()
                .map(|f| row.get(f).map(value_to_key).unwrap_or_default())
                .collect::<Vec<_>>()
                .join("|");

            keys_per_split
                .entry(split_val)
                .or_default()
                .insert(composite_key);
        }

        let splits: Vec<&String> = keys_per_split.keys().collect();
        let mut errors = Vec::new();

        for i in 0..splits.len() {
            for j in (i + 1)..splits.len() {
                let overlap: Vec<_> = keys_per_split[splits[i]]
                    .intersection(&keys_per_split[splits[j]])
                    .collect();

                if !overlap.is_empty() {
                    errors.push(ValidationError::quality_check(format!(
                        "NoOverlap check failed: splits '{}' and '{}' share {} overlapping key(s) on [{}]",
                        splits[i],
                        splits[j],
                        overlap.len(),
                        check.key_fields.join(", "),
                    )));
                }
            }
        }

        errors
    }

    /// Validates that max(timestamp) in train <= min(timestamp) in test.
    fn validate_temporal_split(
        &self,
        check: &TemporalSplitCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let mut train_max: Option<String> = None;
        let mut test_min: Option<String> = None;

        for row in dataset.rows() {
            let split_val = match row.get(&check.split_field) {
                Some(v) => value_to_key(v),
                None => continue,
            };

            let ts = match row.get(&check.timestamp_field) {
                Some(DataValue::Timestamp(t)) => t.clone(),
                Some(DataValue::String(s)) => s.clone(),
                _ => continue,
            };

            if split_val == check.train_split {
                if train_max.as_ref().is_none_or(|cur| ts > *cur) {
                    train_max = Some(ts);
                }
            } else if split_val == check.test_split {
                if test_min.as_ref().is_none_or(|cur| ts < *cur) {
                    test_min = Some(ts);
                }
            }
        }

        let mut errors = Vec::new();

        match (train_max, test_min) {
            (Some(train), Some(test)) if train > test => {
                errors.push(ValidationError::quality_check(format!(
                    "TemporalSplit check failed: max '{}' timestamp ({}) > min '{}' timestamp ({})",
                    check.train_split, train, check.test_split, test,
                )));
            }
            _ => {}
        }

        errors
    }

    /// Validates that no single class exceeds `max_proportion` (and optionally
    /// that every class meets `min_proportion`).
    fn validate_class_balance(
        &self,
        check: &ClassBalanceCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut total: usize = 0;

        for row in dataset.rows() {
            if let Some(val) = row.get(&check.label_field) {
                if !val.is_null() {
                    *counts.entry(value_to_key(val)).or_default() += 1;
                    total += 1;
                }
            }
        }

        if total == 0 {
            return vec![];
        }

        let mut errors = Vec::new();

        for (label, count) in &counts {
            let proportion = *count as f64 / total as f64;

            if proportion > check.max_proportion {
                errors.push(ValidationError::quality_check(format!(
                    "ClassBalance check failed: class '{}' has proportion {:.2}% > max {:.2}%",
                    label,
                    proportion * 100.0,
                    check.max_proportion * 100.0,
                )));
            }

            if let Some(min) = check.min_proportion {
                if proportion < min {
                    errors.push(ValidationError::quality_check(format!(
                        "ClassBalance check failed: class '{}' has proportion {:.2}% < min {:.2}%",
                        label,
                        proportion * 100.0,
                        min * 100.0,
                    )));
                }
            }
        }

        errors
    }
}

impl Default for MlValidator {
    fn default() -> Self {
        Self::new()
    }
}

fn value_to_key(v: &DataValue) -> String {
    match v {
        DataValue::Null => "NULL".to_string(),
        DataValue::String(s) => s.clone(),
        DataValue::Int(i) => i.to_string(),
        DataValue::Float(f) => f.to_string(),
        DataValue::Bool(b) => b.to_string(),
        DataValue::Timestamp(ts) => ts.clone(),
        DataValue::Map(_) => "[map]".to_string(),
        DataValue::List(_) => "[list]".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_core::{ClassBalanceCheck, MlChecks, NoOverlapCheck, TemporalSplitCheck};
    use std::collections::HashMap;

    fn make_row(pairs: Vec<(&str, DataValue)>) -> HashMap<String, DataValue> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    // ---- NoOverlap ----

    #[test]
    fn test_no_overlap_pass() {
        let checks = MlChecks {
            no_overlap: Some(NoOverlapCheck {
                split_field: "split".into(),
                key_fields: vec!["user_id".into()],
            }),
            temporal_split: None,
            class_balance: None,
        };

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("user_id", DataValue::String("a".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("user_id", DataValue::String("b".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_no_overlap_fail() {
        let checks = MlChecks {
            no_overlap: Some(NoOverlapCheck {
                split_field: "split".into(),
                key_fields: vec!["user_id".into()],
            }),
            temporal_split: None,
            class_balance: None,
        };

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("user_id", DataValue::String("a".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("user_id", DataValue::String("a".into())), // overlaps
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("NoOverlap"));
    }

    // ---- TemporalSplit ----

    #[test]
    fn test_temporal_split_pass() {
        let checks = MlChecks {
            no_overlap: None,
            temporal_split: Some(TemporalSplitCheck {
                split_field: "split".into(),
                timestamp_field: "ts".into(),
                train_split: "train".into(),
                test_split: "test".into(),
            }),
            class_balance: None,
        };

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("ts", DataValue::Timestamp("2024-01-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("ts", DataValue::Timestamp("2024-06-01".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_temporal_split_fail() {
        let checks = MlChecks {
            no_overlap: None,
            temporal_split: Some(TemporalSplitCheck {
                split_field: "split".into(),
                timestamp_field: "ts".into(),
                train_split: "train".into(),
                test_split: "test".into(),
            }),
            class_balance: None,
        };

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("ts", DataValue::Timestamp("2024-06-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("ts", DataValue::Timestamp("2024-01-01".into())), // before train
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("TemporalSplit"));
    }

    // ---- ClassBalance ----

    #[test]
    fn test_class_balance_pass() {
        let checks = MlChecks {
            no_overlap: None,
            temporal_split: None,
            class_balance: Some(ClassBalanceCheck {
                label_field: "label".into(),
                max_proportion: 0.8,
                min_proportion: Some(0.1),
            }),
        };

        let rows = vec![
            make_row(vec![("label", DataValue::String("cat".into()))]),
            make_row(vec![("label", DataValue::String("cat".into()))]),
            make_row(vec![("label", DataValue::String("dog".into()))]),
            make_row(vec![("label", DataValue::String("dog".into()))]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_class_balance_fail_max() {
        let checks = MlChecks {
            no_overlap: None,
            temporal_split: None,
            class_balance: Some(ClassBalanceCheck {
                label_field: "label".into(),
                max_proportion: 0.7,
                min_proportion: None,
            }),
        };

        // 9 cat, 1 dog = 90% cat
        let mut rows: Vec<_> = (0..9)
            .map(|_| make_row(vec![("label", DataValue::String("cat".into()))]))
            .collect();
        rows.push(make_row(vec![(
            "label",
            DataValue::String("dog".into()),
        )]));

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("ClassBalance"));
        assert!(errors[0].to_string().contains("cat"));
    }

    #[test]
    fn test_class_balance_fail_min() {
        let checks = MlChecks {
            no_overlap: None,
            temporal_split: None,
            class_balance: Some(ClassBalanceCheck {
                label_field: "label".into(),
                max_proportion: 0.95,
                min_proportion: Some(0.15),
            }),
        };

        // 9 cat, 1 dog = 10% dog < 15%
        let mut rows: Vec<_> = (0..9)
            .map(|_| make_row(vec![("label", DataValue::String("cat".into()))]))
            .collect();
        rows.push(make_row(vec![(
            "label",
            DataValue::String("dog".into()),
        )]));

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("dog"));
    }

    #[test]
    fn test_empty_dataset() {
        let checks = MlChecks {
            no_overlap: Some(NoOverlapCheck {
                split_field: "split".into(),
                key_fields: vec!["id".into()],
            }),
            temporal_split: Some(TemporalSplitCheck {
                split_field: "split".into(),
                timestamp_field: "ts".into(),
                train_split: "train".into(),
                test_split: "test".into(),
            }),
            class_balance: Some(ClassBalanceCheck {
                label_field: "label".into(),
                max_proportion: 0.8,
                min_proportion: None,
            }),
        };

        let ds = DataSet::empty();
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }
}
