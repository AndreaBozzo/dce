//! ML-specific validation logic.
//!
//! This module handles validation of machine learning quality checks:
//! - NoOverlap: Ensures train/test/val splits share no rows on key fields
//! - TemporalSplit: Validates chronological ordering between splits
//! - ClassBalance: Checks that label distributions are not overly skewed
//! - FeatureDrift: Detects distribution shift via Population Stability Index
//! - TargetLeakage: Detects features with high correlation to the target
//! - NullRateByGroup: Detects disparate null rates across groups

use crate::{DataSet, DataValue, ValidationError, parse_timestamp};
use chrono::{DateTime, Utc};
use contracts_core::{
    ClassBalanceCheck, FeatureDriftCheck, MlChecks, NoOverlapCheck, NullRateByGroupCheck,
    TargetLeakageCheck, TemporalSplitCheck,
};
use std::collections::{HashMap, HashSet};

type SplitTimestampStats = HashMap<String, (Option<DateTime<Utc>>, Option<DateTime<Utc>>)>;

/// Validates ML-specific quality checks on a dataset.
pub struct MlValidator;

impl MlValidator {
    pub fn new() -> Self {
        Self
    }

    /// Validates only the ML checks that require row-level DataSet iteration
    /// (NoOverlap, TemporalSplit). The remaining checks (ClassBalance, FeatureDrift,
    /// TargetLeakage, NullRateByGroup) are handled via SQL aggregates in
    /// `DataFusionEngine::check_ml()`.
    pub fn validate_row_only(
        &self,
        ml_checks: &MlChecks,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
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

        errors
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

        if let Some(ref check) = ml_checks.feature_drift {
            errors.extend(self.validate_feature_drift(check, dataset));
        }

        if let Some(ref check) = ml_checks.target_leakage {
            errors.extend(self.validate_target_leakage(check, dataset));
        }

        if let Some(ref check) = ml_checks.null_rate_by_group {
            errors.extend(self.validate_null_rate_by_group(check, dataset));
        }

        errors
    }

    /// Validates that key fields do not overlap across splits.
    /// Reports up to 5 sample overlapping keys in each error message.
    fn validate_no_overlap(
        &self,
        check: &NoOverlapCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let mut keys_per_split: HashMap<String, HashSet<String>> = HashMap::new();
        let mut skipped_rows = 0usize;

        for row in dataset.rows() {
            let split_val = match row.get(&check.split_field) {
                Some(v) if !v.is_null() => value_to_key(v),
                None => continue,
                _ => {
                    skipped_rows += 1;
                    continue;
                }
            };

            let mut key_parts = Vec::with_capacity(check.key_fields.len());
            let mut missing_key = false;

            for field in &check.key_fields {
                match row.get(field) {
                    Some(value) if !value.is_null() => key_parts.push(value_to_key(value)),
                    _ => {
                        missing_key = true;
                        break;
                    }
                }
            }

            if missing_key {
                skipped_rows += 1;
                continue;
            }

            let composite_key = key_parts.join("|");

            keys_per_split
                .entry(split_val)
                .or_default()
                .insert(composite_key);
        }

        let mut splits: Vec<&String> = keys_per_split.keys().collect();
        splits.sort();
        let mut errors = Vec::new();

        if skipped_rows > 0 {
            errors.push(ValidationError::quality_check(format!(
                "NoOverlap check skipped {} row(s) with missing '{}' or key field(s) [{}]",
                skipped_rows,
                check.split_field,
                check.key_fields.join(", "),
            )));
        }

        for i in 0..splits.len() {
            for j in (i + 1)..splits.len() {
                let overlap: Vec<_> = keys_per_split[splits[i]]
                    .intersection(&keys_per_split[splits[j]])
                    .collect();

                if !overlap.is_empty() {
                    let mut sorted_overlap = overlap.clone();
                    sorted_overlap.sort();
                    let samples: Vec<_> =
                        sorted_overlap.iter().take(5).map(|s| s.as_str()).collect();
                    let sample_str = samples.join(", ");
                    let suffix = if overlap.len() > 5 { ", ..." } else { "" };

                    errors.push(ValidationError::quality_check(format!(
                        "NoOverlap check failed: splits '{}' and '{}' share {} overlapping key(s) on [{}]. \
                         Samples: [{}{}]",
                        splits[i],
                        splits[j],
                        overlap.len(),
                        check.key_fields.join(", "),
                        sample_str,
                        suffix,
                    )));
                }
            }
        }

        errors
    }

    /// Validates temporal ordering between splits.
    ///
    /// When `split_order` is provided, validates all adjacent pairs.
    /// Otherwise falls back to the two-field train_split/test_split behavior.
    fn validate_temporal_split(
        &self,
        check: &TemporalSplitCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        // Collect min/max timestamps per split
        let mut split_stats: SplitTimestampStats = HashMap::new();
        let mut invalid_timestamps = 0usize;

        for row in dataset.rows() {
            let split_val = match row.get(&check.split_field) {
                Some(v) if !v.is_null() => value_to_key(v),
                _ => continue,
            };

            let ts = match row.get(&check.timestamp_field) {
                Some(DataValue::Timestamp(t)) => t.clone(),
                Some(DataValue::String(s)) => s.clone(),
                _ => continue,
            };

            let parsed_ts = match parse_timestamp(&ts) {
                Ok(parsed) => parsed,
                Err(_) => {
                    invalid_timestamps += 1;
                    continue;
                }
            };

            let entry = split_stats.entry(split_val).or_insert((None, None));
            // entry.0 = min, entry.1 = max
            if entry.0.is_none_or(|cur| parsed_ts < cur) {
                entry.0 = Some(parsed_ts);
            }
            if entry.1.is_none_or(|cur| parsed_ts > cur) {
                entry.1 = Some(parsed_ts);
            }
        }

        let mut errors = Vec::new();

        if invalid_timestamps > 0 {
            errors.push(ValidationError::quality_check(format!(
                "TemporalSplit check skipped {} row(s) with invalid '{}' values",
                invalid_timestamps, check.timestamp_field,
            )));
        }

        // Determine the ordered pairs to check
        let pairs: Vec<(String, String)> = if let Some(ref order) = check.split_order {
            if order.len() < 2 {
                return vec![ValidationError::quality_check(
                    "TemporalSplit check failed: split_order must contain at least 2 entries"
                        .to_string(),
                )];
            }
            order
                .windows(2)
                .map(|w| (w[0].clone(), w[1].clone()))
                .collect()
        } else {
            vec![(check.train_split.clone(), check.test_split.clone())]
        };

        for (earlier, later) in &pairs {
            let earlier_max = split_stats.get(earlier).and_then(|s| s.1);
            let later_min = split_stats.get(later).and_then(|s| s.0);

            match (earlier_max, later_min) {
                (Some(e_max), Some(l_min)) if e_max > l_min => {
                    errors.push(ValidationError::quality_check(format!(
                        "TemporalSplit check failed: max '{}' timestamp ({}) > min '{}' timestamp ({})",
                        earlier,
                        e_max.to_rfc3339(),
                        later,
                        l_min.to_rfc3339(),
                    )));
                }
                (None, _) => {
                    errors.push(ValidationError::quality_check(format!(
                        "TemporalSplit check could not evaluate '{}' because no valid '{}' values were found",
                        earlier, check.timestamp_field,
                    )));
                }
                (_, None) => {
                    errors.push(ValidationError::quality_check(format!(
                        "TemporalSplit check could not evaluate '{}' because no valid '{}' values were found",
                        later, check.timestamp_field,
                    )));
                }
                _ => {}
            }
        }

        errors
    }

    /// Validates that no single class exceeds `max_proportion` (and optionally
    /// that every class meets `min_proportion`).
    /// Includes all class proportions in error messages for debugging.
    fn validate_class_balance(
        &self,
        check: &ClassBalanceCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let mut counts: HashMap<String, usize> = HashMap::new();
        let mut total: usize = 0;

        for row in dataset.rows() {
            if let Some(val) = row.get(&check.label_field)
                && !val.is_null()
            {
                *counts.entry(value_to_key(val)).or_default() += 1;
                total += 1;
            }
        }

        if total == 0 {
            return vec![];
        }

        // Build summary of all proportions for error messages
        let proportions: HashMap<&str, f64> = counts
            .iter()
            .map(|(label, count)| (label.as_str(), *count as f64 / total as f64))
            .collect();

        let summary: String = {
            let mut items: Vec<_> = proportions
                .iter()
                .map(|(label, p)| format!("{}={:.2}%", label, p * 100.0))
                .collect();
            items.sort();
            items.join(", ")
        };

        let mut errors = Vec::new();

        for (label, proportion) in &proportions {
            if *proportion > check.max_proportion {
                errors.push(ValidationError::quality_check(format!(
                    "ClassBalance check failed: class '{}' has proportion {:.2}% > max {:.2}%. \
                     All proportions: {}",
                    label,
                    proportion * 100.0,
                    check.max_proportion * 100.0,
                    summary,
                )));
            }

            if let Some(min) = check.min_proportion
                && *proportion < min
            {
                errors.push(ValidationError::quality_check(format!(
                    "ClassBalance check failed: class '{}' has proportion {:.2}% < min {:.2}%. \
                     All proportions: {}",
                    label,
                    proportion * 100.0,
                    min * 100.0,
                    summary,
                )));
            }
        }

        errors
    }

    /// Detects feature distribution drift using Population Stability Index (PSI).
    ///
    /// PSI = Σ (current% - ref%) × ln(current% / ref%) over quantile bins.
    /// PSI > 0.1 = moderate drift, > 0.2 = significant drift.
    fn validate_feature_drift(
        &self,
        check: &FeatureDriftCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let num_bins = check.num_bins.unwrap_or(10);
        let threshold = check.threshold.unwrap_or(0.2);
        let epsilon = 1e-6;

        // Group numeric values by split for each feature
        let mut ref_values: HashMap<&str, Vec<f64>> = HashMap::new();
        let mut cur_values: HashMap<&str, Vec<f64>> = HashMap::new();

        for row in dataset.rows() {
            let split_val = match row.get(&check.split_field) {
                Some(v) if !v.is_null() => value_to_key(v),
                _ => continue,
            };

            let target_map = if split_val == check.reference_split {
                &mut ref_values
            } else if split_val == check.current_split {
                &mut cur_values
            } else {
                continue;
            };

            for field in &check.feature_fields {
                if let Some(val) = row.get(field)
                    && let Some(f) = value_to_float(val)
                {
                    target_map.entry(field.as_str()).or_default().push(f);
                }
            }
        }

        let mut errors = Vec::new();

        for field in &check.feature_fields {
            let ref_vals = match ref_values.get(field.as_str()) {
                Some(v) if v.len() >= num_bins => v,
                _ => {
                    errors.push(ValidationError::quality_check(format!(
                        "FeatureDrift check: insufficient reference data for field '{}' in split '{}'",
                        field, check.reference_split,
                    )));
                    continue;
                }
            };
            let cur_vals = match cur_values.get(field.as_str()) {
                Some(v) if !v.is_empty() => v,
                _ => {
                    errors.push(ValidationError::quality_check(format!(
                        "FeatureDrift check: no current data for field '{}' in split '{}'",
                        field, check.current_split,
                    )));
                    continue;
                }
            };

            // Compute quantile boundaries from reference
            let mut sorted_ref = ref_vals.clone();
            sorted_ref.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

            let boundaries: Vec<f64> = (1..num_bins)
                .map(|i| {
                    let idx = (i as f64 / num_bins as f64 * sorted_ref.len() as f64) as usize;
                    sorted_ref[idx.min(sorted_ref.len() - 1)]
                })
                .collect();

            let bin_counts = |vals: &[f64]| -> Vec<f64> {
                let mut bins = vec![0.0; num_bins];
                for &v in vals {
                    let bin = boundaries
                        .iter()
                        .position(|&b| v < b)
                        .unwrap_or(num_bins - 1);
                    bins[bin] += 1.0;
                }
                let total = vals.len() as f64;
                bins.iter().map(|c| c / total + epsilon).collect()
            };

            let ref_pcts = bin_counts(ref_vals);
            let cur_pcts = bin_counts(cur_vals);

            let psi: f64 = ref_pcts
                .iter()
                .zip(cur_pcts.iter())
                .map(|(r, c)| (c - r) * (c / r).ln())
                .sum();

            if psi > threshold {
                errors.push(ValidationError::quality_check(format!(
                    "FeatureDrift check failed: field '{}' has PSI {:.4} > threshold {:.2} \
                     (reference='{}', current='{}')",
                    field, psi, threshold, check.reference_split, check.current_split,
                )));
            }
        }

        errors
    }

    /// Detects features with suspiciously high Pearson correlation to the target.
    fn validate_target_leakage(
        &self,
        check: &TargetLeakageCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let max_corr = check.max_correlation.unwrap_or(0.95);
        let mut errors = Vec::new();

        for field in &check.feature_fields {
            let mut xs = Vec::new();
            let mut ys = Vec::new();

            for row in dataset.rows() {
                let target_val = match row.get(&check.target_field).and_then(value_to_float) {
                    Some(v) => v,
                    None => continue,
                };
                let feature_val = match row.get(field).and_then(value_to_float) {
                    Some(v) => v,
                    None => continue,
                };
                xs.push(feature_val);
                ys.push(target_val);
            }

            if xs.len() < 3 {
                continue;
            }

            let corr = pearson_correlation(&xs, &ys);

            if corr.abs() > max_corr {
                errors.push(ValidationError::quality_check(format!(
                    "TargetLeakage check failed: feature '{}' has correlation {:.4} with target '{}' \
                     (|r| > {:.2})",
                    field, corr, check.target_field, max_corr,
                )));
            }
        }

        errors
    }

    /// Detects disparate null rates across groups/splits.
    fn validate_null_rate_by_group(
        &self,
        check: &NullRateByGroupCheck,
        dataset: &DataSet,
    ) -> Vec<ValidationError> {
        let max_diff = check.max_null_rate_diff.unwrap_or(0.1);

        // group -> field -> (null_count, total_count)
        let mut stats: HashMap<String, HashMap<&str, (usize, usize)>> = HashMap::new();

        for row in dataset.rows() {
            let group = match row.get(&check.group_field) {
                Some(v) if !v.is_null() => value_to_key(v),
                _ => continue,
            };

            let group_stats = stats.entry(group).or_default();
            for field in &check.check_fields {
                let entry = group_stats.entry(field.as_str()).or_insert((0, 0));
                entry.1 += 1; // total
                if row.get(field).is_none_or(|v| v.is_null()) {
                    entry.0 += 1; // null
                }
            }
        }

        let mut errors = Vec::new();

        for field in &check.check_fields {
            let rates: Vec<(&str, f64)> = stats
                .iter()
                .filter_map(|(group, fields)| {
                    fields.get(field.as_str()).map(|(nulls, total)| {
                        (
                            group.as_str(),
                            if *total > 0 {
                                *nulls as f64 / *total as f64
                            } else {
                                0.0
                            },
                        )
                    })
                })
                .collect();

            if rates.len() < 2 {
                continue;
            }

            let min_rate = rates.iter().map(|(_, r)| *r).fold(f64::INFINITY, f64::min);
            let max_rate = rates
                .iter()
                .map(|(_, r)| *r)
                .fold(f64::NEG_INFINITY, f64::max);
            let diff = max_rate - min_rate;

            if diff > max_diff {
                let detail: Vec<_> = rates
                    .iter()
                    .map(|(g, r)| format!("{}={:.2}%", g, r * 100.0))
                    .collect();

                errors.push(ValidationError::quality_check(format!(
                    "NullRateByGroup check failed: field '{}' has null rate diff {:.2}% > max {:.2}%. \
                     Rates: [{}]",
                    field,
                    diff * 100.0,
                    max_diff * 100.0,
                    detail.join(", "),
                )));
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

/// Attempts to extract a float from a DataValue (for numeric checks).
fn value_to_float(v: &DataValue) -> Option<f64> {
    match v {
        DataValue::Float(f) => Some(*f),
        DataValue::Int(i) => Some(*i as f64),
        DataValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

/// Computes Pearson correlation coefficient between two slices.
fn pearson_correlation(xs: &[f64], ys: &[f64]) -> f64 {
    let n = xs.len() as f64;
    let mean_x = xs.iter().sum::<f64>() / n;
    let mean_y = ys.iter().sum::<f64>() / n;

    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;

    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = x - mean_x;
        let dy = y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }

    let denom = (var_x * var_y).sqrt();
    if denom < 1e-15 { 0.0 } else { cov / denom }
}

#[cfg(test)]
mod tests {
    use super::*;
    use contracts_core::{
        ClassBalanceCheck, FeatureDriftCheck, MlChecks, NoOverlapCheck, NullRateByGroupCheck,
        TargetLeakageCheck, TemporalSplitCheck,
    };
    use std::collections::HashMap;

    fn make_row(pairs: Vec<(&str, DataValue)>) -> HashMap<String, DataValue> {
        pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
    }

    fn ml_checks_only_no_overlap(check: NoOverlapCheck) -> MlChecks {
        MlChecks {
            no_overlap: Some(check),
            temporal_split: None,
            class_balance: None,
            feature_drift: None,
            target_leakage: None,
            null_rate_by_group: None,
        }
    }

    fn ml_checks_only_temporal(check: TemporalSplitCheck) -> MlChecks {
        MlChecks {
            no_overlap: None,
            temporal_split: Some(check),
            class_balance: None,
            feature_drift: None,
            target_leakage: None,
            null_rate_by_group: None,
        }
    }

    fn ml_checks_only_balance(check: ClassBalanceCheck) -> MlChecks {
        MlChecks {
            no_overlap: None,
            temporal_split: None,
            class_balance: Some(check),
            feature_drift: None,
            target_leakage: None,
            null_rate_by_group: None,
        }
    }

    // ---- NoOverlap ----

    #[test]
    fn test_no_overlap_pass() {
        let checks = ml_checks_only_no_overlap(NoOverlapCheck {
            split_field: "split".into(),
            key_fields: vec!["user_id".into()],
        });

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
    fn test_no_overlap_fail_with_samples() {
        let checks = ml_checks_only_no_overlap(NoOverlapCheck {
            split_field: "split".into(),
            key_fields: vec!["user_id".into()],
        });

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("user_id", DataValue::String("a".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("user_id", DataValue::String("a".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        let msg = errors[0].to_string();
        assert!(msg.contains("NoOverlap"));
        assert!(msg.contains("Samples:"));
        assert!(msg.contains("a"));
    }

    #[test]
    fn test_no_overlap_skips_rows_with_missing_keys() {
        let checks = ml_checks_only_no_overlap(NoOverlapCheck {
            split_field: "split".into(),
            key_fields: vec!["user_id".into()],
        });

        let rows = vec![
            make_row(vec![("split", DataValue::String("train".into()))]),
            make_row(vec![("split", DataValue::String("test".into()))]),
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
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("skipped 2 row(s)"));
    }

    // ---- TemporalSplit ----

    #[test]
    fn test_temporal_split_pass() {
        let checks = ml_checks_only_temporal(TemporalSplitCheck {
            split_field: "split".into(),
            timestamp_field: "ts".into(),
            train_split: "train".into(),
            test_split: "test".into(),
            split_order: None,
        });

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
        let checks = ml_checks_only_temporal(TemporalSplitCheck {
            split_field: "split".into(),
            timestamp_field: "ts".into(),
            train_split: "train".into(),
            test_split: "test".into(),
            split_order: None,
        });

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("ts", DataValue::Timestamp("2024-06-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("ts", DataValue::Timestamp("2024-01-01".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("TemporalSplit"));
    }

    #[test]
    fn test_temporal_split_parses_unix_epoch_values() {
        let checks = ml_checks_only_temporal(TemporalSplitCheck {
            split_field: "split".into(),
            timestamp_field: "ts".into(),
            train_split: "train".into(),
            test_split: "test".into(),
            split_order: None,
        });

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("ts", DataValue::String("10".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("ts", DataValue::String("9".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("TemporalSplit"));
    }

    #[test]
    fn test_temporal_split_three_way_pass() {
        let checks = ml_checks_only_temporal(TemporalSplitCheck {
            split_field: "split".into(),
            timestamp_field: "ts".into(),
            train_split: "train".into(),
            test_split: "test".into(),
            split_order: Some(vec!["train".into(), "val".into(), "test".into()]),
        });

        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("ts", DataValue::Timestamp("2024-01-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("val".into())),
                ("ts", DataValue::Timestamp("2024-04-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("ts", DataValue::Timestamp("2024-07-01".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_temporal_split_three_way_fail() {
        let checks = ml_checks_only_temporal(TemporalSplitCheck {
            split_field: "split".into(),
            timestamp_field: "ts".into(),
            train_split: "train".into(),
            test_split: "test".into(),
            split_order: Some(vec!["train".into(), "val".into(), "test".into()]),
        });

        // val timestamps are before train
        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("ts", DataValue::Timestamp("2024-06-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("val".into())),
                ("ts", DataValue::Timestamp("2024-01-01".into())),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("ts", DataValue::Timestamp("2024-12-01".into())),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        let msg = errors[0].to_string();
        assert!(msg.contains("train"));
        assert!(msg.contains("val"));
    }

    // ---- ClassBalance ----

    #[test]
    fn test_class_balance_pass() {
        let checks = ml_checks_only_balance(ClassBalanceCheck {
            label_field: "label".into(),
            max_proportion: 0.8,
            min_proportion: Some(0.1),
        });

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
    fn test_class_balance_fail_max_with_proportions() {
        let checks = ml_checks_only_balance(ClassBalanceCheck {
            label_field: "label".into(),
            max_proportion: 0.7,
            min_proportion: None,
        });

        let mut rows: Vec<_> = (0..9)
            .map(|_| make_row(vec![("label", DataValue::String("cat".into()))]))
            .collect();
        rows.push(make_row(vec![("label", DataValue::String("dog".into()))]));

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        let msg = errors[0].to_string();
        assert!(msg.contains("ClassBalance"));
        assert!(msg.contains("cat"));
        assert!(msg.contains("All proportions:"));
    }

    #[test]
    fn test_class_balance_fail_min() {
        let checks = ml_checks_only_balance(ClassBalanceCheck {
            label_field: "label".into(),
            max_proportion: 0.95,
            min_proportion: Some(0.15),
        });

        let mut rows: Vec<_> = (0..9)
            .map(|_| make_row(vec![("label", DataValue::String("cat".into()))]))
            .collect();
        rows.push(make_row(vec![("label", DataValue::String("dog".into()))]));

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("dog"));
    }

    // ---- FeatureDrift ----

    #[test]
    fn test_feature_drift_pass_similar_distributions() {
        let checks = MlChecks {
            feature_drift: Some(FeatureDriftCheck {
                split_field: "split".into(),
                reference_split: "train".into(),
                current_split: "test".into(),
                feature_fields: vec!["score".into()],
                num_bins: Some(5),
                threshold: Some(0.2),
            }),
            ..default_ml_checks()
        };

        // Similar distributions in train and test
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(make_row(vec![
                ("split", DataValue::String("train".into())),
                ("score", DataValue::Float(i as f64)),
            ]));
            rows.push(make_row(vec![
                ("split", DataValue::String("test".into())),
                ("score", DataValue::Float(i as f64 + 1.0)),
            ]));
        }

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_feature_drift_fail_different_distributions() {
        let checks = MlChecks {
            feature_drift: Some(FeatureDriftCheck {
                split_field: "split".into(),
                reference_split: "train".into(),
                current_split: "test".into(),
                feature_fields: vec!["score".into()],
                num_bins: Some(5),
                threshold: Some(0.2),
            }),
            ..default_ml_checks()
        };

        // Very different distributions
        let mut rows = Vec::new();
        for i in 0..100 {
            rows.push(make_row(vec![
                ("split", DataValue::String("train".into())),
                ("score", DataValue::Float(i as f64)),
            ]));
            rows.push(make_row(vec![
                ("split", DataValue::String("test".into())),
                ("score", DataValue::Float((i as f64 * 10.0) + 500.0)),
            ]));
        }

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert!(!errors.is_empty());
        assert!(errors[0].to_string().contains("FeatureDrift"));
        assert!(errors[0].to_string().contains("PSI"));
    }

    // ---- TargetLeakage ----

    #[test]
    fn test_target_leakage_pass() {
        let checks = MlChecks {
            target_leakage: Some(TargetLeakageCheck {
                target_field: "target".into(),
                feature_fields: vec!["feature".into()],
                max_correlation: Some(0.95),
            }),
            ..default_ml_checks()
        };

        // Low correlation: random-ish values
        let rows: Vec<_> = (0..50)
            .map(|i| {
                make_row(vec![
                    ("target", DataValue::Float(i as f64)),
                    (
                        "feature",
                        DataValue::Float(if i % 2 == 0 { 100.0 } else { 0.0 }),
                    ),
                ])
            })
            .collect();

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_target_leakage_fail_perfect_correlation() {
        let checks = MlChecks {
            target_leakage: Some(TargetLeakageCheck {
                target_field: "target".into(),
                feature_fields: vec!["feature".into()],
                max_correlation: Some(0.95),
            }),
            ..default_ml_checks()
        };

        // Perfect correlation: feature = target * 2
        let rows: Vec<_> = (0..50)
            .map(|i| {
                make_row(vec![
                    ("target", DataValue::Float(i as f64)),
                    ("feature", DataValue::Float(i as f64 * 2.0)),
                ])
            })
            .collect();

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("TargetLeakage"));
    }

    // ---- NullRateByGroup ----

    #[test]
    fn test_null_rate_by_group_pass() {
        let checks = MlChecks {
            null_rate_by_group: Some(NullRateByGroupCheck {
                group_field: "split".into(),
                check_fields: vec!["score".into()],
                max_null_rate_diff: Some(0.1),
            }),
            ..default_ml_checks()
        };

        // Same null rate in both groups
        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("score", DataValue::Float(1.0)),
            ]),
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("score", DataValue::Null),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("score", DataValue::Float(2.0)),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("score", DataValue::Null),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    #[test]
    fn test_null_rate_by_group_fail() {
        let checks = MlChecks {
            null_rate_by_group: Some(NullRateByGroupCheck {
                group_field: "split".into(),
                check_fields: vec!["score".into()],
                max_null_rate_diff: Some(0.1),
            }),
            ..default_ml_checks()
        };

        // train: 0% nulls, test: 100% nulls
        let rows = vec![
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("score", DataValue::Float(1.0)),
            ]),
            make_row(vec![
                ("split", DataValue::String("train".into())),
                ("score", DataValue::Float(2.0)),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("score", DataValue::Null),
            ]),
            make_row(vec![
                ("split", DataValue::String("test".into())),
                ("score", DataValue::Null),
            ]),
        ];

        let ds = DataSet::from_rows(rows);
        let v = MlValidator::new();
        let errors = v.validate(&checks, &ds);
        assert_eq!(errors.len(), 1);
        let msg = errors[0].to_string();
        assert!(msg.contains("NullRateByGroup"));
        assert!(msg.contains("score"));
    }

    // ---- Empty dataset ----

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
                split_order: None,
            }),
            class_balance: Some(ClassBalanceCheck {
                label_field: "label".into(),
                max_proportion: 0.8,
                min_proportion: None,
            }),
            feature_drift: None,
            target_leakage: None,
            null_rate_by_group: None,
        };

        let ds = DataSet::empty();
        let v = MlValidator::new();
        assert!(v.validate(&checks, &ds).is_empty());
    }

    fn default_ml_checks() -> MlChecks {
        MlChecks {
            no_overlap: None,
            temporal_split: None,
            class_balance: None,
            feature_drift: None,
            target_leakage: None,
            null_rate_by_group: None,
        }
    }
}
