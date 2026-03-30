//! DataFusion-backed validation engine.
//!
//! Translates contract constraints into SQL queries and executes them via
//! DataFusion against an in-memory Arrow table.  This provides vectorized,
//! batch-level validation instead of row-by-row iteration.

use crate::{DataSet, DataValue};
use arrow_array::Array;
use arrow_array::RecordBatch;
use arrow_array::builder::*;
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use contracts_core::{
    ClassBalanceCheck, CompletenessCheck, Contract, DataType, FeatureDriftCheck, Field,
    FieldConstraints, MlChecks, NullRateByGroupCheck, PrimitiveType, QualityChecks,
    TargetLeakageCheck, UniquenessCheck, ValidationContext, ValidationReport, ValidationStats,
};
use datafusion::prelude::*;
use std::sync::Arc;
use std::time::Instant;

/// A validation engine backed by Apache DataFusion.
///
/// Registers the incoming dataset as a temporary table and runs SQL queries
/// derived from the contract to detect violations.
pub struct DataFusionEngine;

impl DataFusionEngine {
    pub fn new() -> Self {
        Self
    }

    /// Validate `dataset` against `contract` using DataFusion SQL.
    ///
    /// This is an async method because DataFusion execution is async.
    pub async fn validate(
        &self,
        contract: &Contract,
        dataset: &DataSet,
        context: &ValidationContext,
    ) -> ValidationReport {
        let start = Instant::now();
        let mut errors: Vec<String> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        if dataset.is_empty() {
            return self.build_report(errors, warnings, contract, dataset, start);
        }

        // Build Arrow RecordBatch from dataset
        let batch = match dataset_to_record_batch(&contract.schema.fields, dataset) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("Failed to create Arrow batch: {e}"));
                return self.build_report(errors, warnings, contract, dataset, start);
            }
        };

        // Create DataFusion context and register the table
        let ctx = SessionContext::new();
        if let Err(e) = ctx.register_batch("data", batch) {
            errors.push(format!("Failed to register table: {e}"));
            return self.build_report(errors, warnings, contract, dataset, start);
        }

        // --- 0. Schema presence checks ---
        let presence_errs = self.check_schema_presence(contract, &ctx).await;
        errors.extend(presence_errs);

        // --- 1. Schema / nullability checks ---
        let null_errs = self.check_nullability(contract, &ctx).await;
        errors.extend(null_errs);

        if context.strict && !errors.is_empty() {
            return self.build_report(errors, warnings, contract, dataset, start);
        }

        // --- 2. Field constraints ---
        let constraint_errs = self.check_constraints(contract, &ctx).await;
        errors.extend(constraint_errs);

        if context.schema_only {
            return self.build_report(errors, warnings, contract, dataset, start);
        }

        // --- 3. Quality checks ---
        if let Some(ref qc) = contract.quality_checks {
            let qc_errs = self.check_quality(qc, &ctx).await;
            if context.strict {
                errors.extend(qc_errs);
            } else {
                warnings.extend(qc_errs);
            }
        }

        // --- 4. ML checks (SQL-based) ---
        if let Some(ref qc) = contract.quality_checks
            && let Some(ref ml) = qc.ml_checks
        {
            let ml_errs = self.check_ml(ml, &ctx).await;
            if context.strict {
                errors.extend(ml_errs);
            } else {
                warnings.extend(ml_errs);
            }
        }

        self.build_report(errors, warnings, contract, dataset, start)
    }

    /// Validate against a `SessionContext` that already has a `"data"` table registered.
    ///
    /// This is the zero-copy native path: no `DataSet` materialisation is needed.
    /// The caller is responsible for registering the table before calling this method.
    pub async fn validate_with_context(
        &self,
        contract: &Contract,
        ctx: &SessionContext,
        context: &ValidationContext,
    ) -> ValidationReport {
        let start = Instant::now();
        let mut errors: Vec<String> = Vec::new();
        let mut warnings: Vec<String> = Vec::new();

        // --- 0. Schema presence checks ---
        let presence_errs = self.check_schema_presence(contract, ctx).await;
        errors.extend(presence_errs);

        // --- 1. Schema / nullability checks ---
        let null_errs = self.check_nullability(contract, ctx).await;
        errors.extend(null_errs);

        if context.strict && !errors.is_empty() {
            return self
                .build_report_from_context(errors, warnings, contract, ctx, start)
                .await;
        }

        // --- 2. Field constraints ---
        let constraint_errs = self.check_constraints(contract, ctx).await;
        errors.extend(constraint_errs);

        if context.schema_only {
            return self
                .build_report_from_context(errors, warnings, contract, ctx, start)
                .await;
        }

        // --- 3. Quality checks ---
        if let Some(ref qc) = contract.quality_checks {
            let qc_errs = self.check_quality(qc, ctx).await;
            if context.strict {
                errors.extend(qc_errs);
            } else {
                warnings.extend(qc_errs);
            }
        }

        // --- 4. ML checks (SQL-based) ---
        if let Some(ref qc) = contract.quality_checks
            && let Some(ref ml) = qc.ml_checks
        {
            let ml_errs = self.check_ml(ml, ctx).await;
            if context.strict {
                errors.extend(ml_errs);
            } else {
                warnings.extend(ml_errs);
            }
        }

        self.build_report_from_context(errors, warnings, contract, ctx, start)
            .await
    }

    /// Build a validation report when using the native context path.
    ///
    /// Obtains the row count via `SELECT COUNT(*) FROM data` instead of `dataset.len()`.
    async fn build_report_from_context(
        &self,
        errors: Vec<String>,
        warnings: Vec<String>,
        contract: &Contract,
        ctx: &SessionContext,
        start: Instant,
    ) -> ValidationReport {
        let mut errors = errors;

        let records_validated = match count_query(ctx, "SELECT COUNT(*) AS cnt FROM data").await {
            Ok(count) => count as usize,
            Err(e) => {
                errors.push(format!("Failed to count validated records: {e}"));
                0
            }
        };

        let constraints_evaluated: usize = contract
            .schema
            .fields
            .iter()
            .map(|f| f.constraints.as_ref().map(|c| c.len()).unwrap_or(0))
            .sum();

        let quality_checks_count = contract
            .quality_checks
            .as_ref()
            .map(|qc| {
                let mut n = 0usize;
                if qc.completeness.is_some() {
                    n += 1;
                }
                if qc.uniqueness.is_some() {
                    n += 1;
                }
                if qc.freshness.is_some() {
                    n += 1;
                }
                if let Some(ref c) = qc.custom_checks {
                    n += c.len();
                }
                // Only count SQL-executed ML checks. Row-only checks
                // (no_overlap, temporal_split) are skipped in the native
                // context path and should not inflate constraints_evaluated.
                if let Some(ref ml) = qc.ml_checks {
                    if ml.class_balance.is_some() {
                        n += 1;
                    }
                    if ml.feature_drift.is_some() {
                        n += 1;
                    }
                    if ml.target_leakage.is_some() {
                        n += 1;
                    }
                    if ml.null_rate_by_group.is_some() {
                        n += 1;
                    }
                }
                n
            })
            .unwrap_or(0);

        ValidationReport {
            passed: errors.is_empty(),
            errors,
            warnings,
            stats: ValidationStats {
                records_validated,
                fields_checked: contract.schema.fields.len(),
                constraints_evaluated: constraints_evaluated + quality_checks_count,
                duration_ms: start.elapsed().as_millis() as u64,
            },
        }
    }

    // -----------------------------------------------------------------------
    // Nullability
    // -----------------------------------------------------------------------

    /// Check that every field declared in the contract exists in the data table.
    async fn check_schema_presence(
        &self,
        contract: &Contract,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let mut errs = Vec::new();

        // Retrieve the table's column names from DataFusion.
        let table_columns: std::collections::HashSet<String> =
            match ctx.sql("SELECT * FROM data LIMIT 0").await {
                Ok(df) => df
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect(),
                Err(_) => return errs, // table not accessible, will be caught later
            };

        for field in &contract.schema.fields {
            if !table_columns.contains(&field.name) {
                errs.push(format!(
                    "Field '{}' is declared in the contract but missing from the data",
                    field.name
                ));
            }
        }
        errs
    }

    async fn check_nullability(&self, contract: &Contract, ctx: &SessionContext) -> Vec<String> {
        let mut errs = Vec::new();
        for field in &contract.schema.fields {
            if field.nullable {
                continue;
            }
            let sql = format!(
                "SELECT COUNT(*) AS cnt FROM data WHERE \"{}\" IS NULL",
                field.name
            );
            match count_query(ctx, &sql).await {
                Ok(cnt) if cnt > 0 => {
                    errs.push(format!(
                        "Field '{}' is null but nullability is not allowed ({cnt} row(s))",
                        field.name
                    ));
                }
                Ok(_) => {}
                Err(_) => {} // column may not exist; already reported by check_schema_presence
            }
        }
        errs
    }

    // -----------------------------------------------------------------------
    // Constraints
    // -----------------------------------------------------------------------

    async fn check_constraints(&self, contract: &Contract, ctx: &SessionContext) -> Vec<String> {
        let mut errs = Vec::new();
        for field in &contract.schema.fields {
            let constraints = match &field.constraints {
                Some(c) => c,
                None => continue,
            };
            for c in constraints {
                let field_errs = self.check_one_constraint(field, c, ctx).await;
                errs.extend(field_errs);
            }
        }
        errs
    }

    async fn check_one_constraint(
        &self,
        field: &Field,
        constraint: &FieldConstraints,
        ctx: &SessionContext,
    ) -> Vec<String> {
        match constraint {
            FieldConstraints::AllowedValues { values } => {
                self.check_allowed_values(field, values, ctx).await
            }
            FieldConstraints::Range { min, max } => self.check_range(field, *min, *max, ctx).await,
            FieldConstraints::Pattern { regex } => self.check_pattern(field, regex, ctx).await,
            FieldConstraints::Custom { .. } => Vec::new(),
        }
    }

    async fn check_allowed_values(
        &self,
        field: &Field,
        values: &[String],
        ctx: &SessionContext,
    ) -> Vec<String> {
        let in_list: String = values
            .iter()
            .map(|v| format!("'{}'", v.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM data \
             WHERE \"{}\" IS NOT NULL AND CAST(\"{}\" AS VARCHAR) NOT IN ({in_list})",
            field.name, field.name
        );
        match count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Constraint violation for field '{}': {cnt} row(s) not in allowed values [{}]",
                field.name,
                values.join(", ")
            )],
            _ => Vec::new(),
        }
    }

    async fn check_range(
        &self,
        field: &Field,
        min: f64,
        max: f64,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM data \
             WHERE \"{}\" IS NOT NULL AND (CAST(\"{}\" AS DOUBLE) < {min} OR CAST(\"{}\" AS DOUBLE) > {max})",
            field.name, field.name, field.name
        );
        match count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Constraint violation for field '{}': {cnt} row(s) out of range [{min}, {max}]",
                field.name
            )],
            _ => Vec::new(),
        }
    }

    async fn check_pattern(&self, field: &Field, regex: &str, ctx: &SessionContext) -> Vec<String> {
        let escaped = regex.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM data \
             WHERE \"{}\" IS NOT NULL AND CAST(\"{}\" AS VARCHAR) NOT SIMILAR TO '{escaped}'",
            field.name, field.name
        );
        match count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Constraint violation for field '{}': {cnt} row(s) do not match pattern '{regex}'",
                field.name
            )],
            Err(_) => {
                let sql2 = format!(
                    "SELECT COUNT(*) AS cnt FROM data \
                     WHERE \"{}\" IS NOT NULL AND regexp_match(CAST(\"{}\" AS VARCHAR), '{escaped}') IS NULL",
                    field.name, field.name
                );
                match count_query(ctx, &sql2).await {
                    Ok(cnt) if cnt > 0 => vec![format!(
                        "Constraint violation for field '{}': {cnt} row(s) do not match pattern '{regex}'",
                        field.name
                    )],
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Quality checks
    // -----------------------------------------------------------------------

    async fn check_quality(&self, qc: &QualityChecks, ctx: &SessionContext) -> Vec<String> {
        let mut errs = Vec::new();
        if let Some(ref comp) = qc.completeness {
            errs.extend(self.check_completeness(comp, ctx).await);
        }
        if let Some(ref uniq) = qc.uniqueness {
            errs.extend(self.check_uniqueness(uniq, ctx).await);
        }
        errs
    }

    async fn check_completeness(
        &self,
        check: &CompletenessCheck,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let mut errs = Vec::new();
        for field_name in &check.fields {
            let sql = format!(
                "SELECT \
                     CAST(COUNT(\"{field_name}\") AS DOUBLE) / CAST(COUNT(*) AS DOUBLE) AS ratio \
                 FROM data"
            );
            if let Ok(batches) = ctx.sql(&sql).await
                && let Ok(batches) = batches.collect().await
                && let Some(batch) = batches.first()
                && batch.num_rows() > 0
            {
                let col = batch.column(0);
                if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Float64Array>() {
                    let ratio = arr.value(0);
                    if ratio < check.threshold {
                        errs.push(format!(
                            "Quality check failed: Completeness check failed for field '{}': {:.2}% < {:.2}% (threshold)",
                            field_name,
                            ratio * 100.0,
                            check.threshold * 100.0
                        ));
                    }
                }
            }
        }
        errs
    }

    async fn check_uniqueness(&self, check: &UniquenessCheck, ctx: &SessionContext) -> Vec<String> {
        let cols = check
            .fields
            .iter()
            .map(|f| format!("\"{}\"", f))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("SELECT COUNT(*) - COUNT(DISTINCT ({cols})) AS dupes FROM data");
        match count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Quality check failed: Uniqueness check failed for fields [{}]: found {} duplicate(s)",
                check.fields.join(", "),
                cnt
            )],
            _ => Vec::new(),
        }
    }

    // -----------------------------------------------------------------------
    // ML checks (SQL-based)
    // -----------------------------------------------------------------------

    /// Run SQL-based ML checks: ClassBalance, TargetLeakage, FeatureDrift, NullRateByGroup.
    ///
    /// NoOverlap and TemporalSplit remain row-by-row in `MlValidator` because they
    /// do not have OOM risk and require custom DataSet iteration logic.
    pub(crate) async fn check_ml(&self, ml_checks: &MlChecks, ctx: &SessionContext) -> Vec<String> {
        let mut errs = Vec::new();
        if let Some(ref check) = ml_checks.class_balance {
            errs.extend(self.check_ml_class_balance(check, ctx).await);
        }
        if let Some(ref check) = ml_checks.target_leakage {
            errs.extend(self.check_ml_target_leakage(check, ctx).await);
        }
        if let Some(ref check) = ml_checks.feature_drift {
            errs.extend(self.check_ml_feature_drift(check, ctx).await);
        }
        if let Some(ref check) = ml_checks.null_rate_by_group {
            errs.extend(self.check_ml_null_rate_by_group(check, ctx).await);
        }
        errs
    }

    /// Detects features with suspiciously high Pearson correlation to the target
    /// using DataFusion's built-in `corr()` aggregate (streaming, bounded memory).
    async fn check_ml_target_leakage(
        &self,
        check: &TargetLeakageCheck,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let max_corr = check.max_correlation.unwrap_or(0.95);
        let mut errs = Vec::new();

        for field in &check.feature_fields {
            let sql = format!(
                "SELECT COUNT(*) AS n, \
                        corr(CAST(\"{field}\" AS DOUBLE), CAST(\"{}\" AS DOUBLE)) AS r \
                 FROM data \
                 WHERE \"{field}\" IS NOT NULL AND \"{}\" IS NOT NULL",
                check.target_field, check.target_field
            );

            if let Ok(batches) = ctx.sql(&sql).await
                && let Ok(batches) = batches.collect().await
                && let Some(batch) = batches.first()
                && batch.num_rows() > 0
            {
                let n_col = batch.column(0);
                let r_col = batch.column(1);

                let n = if let Some(a) = n_col.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    a.value(0)
                } else if let Some(a) = n_col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
                    a.value(0) as i64
                } else {
                    continue;
                };

                if n < 3 {
                    continue;
                }

                if let Some(arr) = r_col.as_any().downcast_ref::<arrow_array::Float64Array>()
                    && !arr.is_null(0)
                {
                    let corr = arr.value(0);
                    if corr.abs() > max_corr {
                        errs.push(format!(
                            "Quality check failed: TargetLeakage check failed: feature '{}' has \
                             correlation {:.4} with target '{}' (|r| > {:.2})",
                            field, corr, check.target_field, max_corr,
                        ));
                    }
                }
            }
        }

        errs
    }

    /// Validates class label distribution using SQL GROUP BY with a window function.
    /// Returns one row per class — only proportions are held in memory.
    async fn check_ml_class_balance(
        &self,
        check: &ClassBalanceCheck,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let label = &check.label_field;
        // Two-step: first get counts per class, then compute proportions
        // using a CROSS JOIN with the total count.
        let sql = format!(
            "WITH class_counts AS ( \
                 SELECT CAST(\"{label}\" AS VARCHAR) AS label, COUNT(*) AS cnt \
                 FROM data \
                 WHERE \"{label}\" IS NOT NULL \
                 GROUP BY CAST(\"{label}\" AS VARCHAR) \
             ), \
             total AS ( \
                 SELECT CAST(SUM(cnt) AS DOUBLE) AS n FROM class_counts \
             ) \
             SELECT c.label, CAST(c.cnt AS DOUBLE) / t.n AS proportion \
             FROM class_counts c CROSS JOIN total t"
        );

        let batches = match ctx.sql(&sql).await {
            Ok(df) => match df.collect().await {
                Ok(b) => b,
                Err(_) => return Vec::new(),
            },
            Err(_) => return Vec::new(),
        };

        let mut proportions: Vec<(String, f64)> = Vec::new();

        for batch in &batches {
            let label_col = batch.column(0);
            let prop_col = batch.column(1);

            // DataFusion may return Utf8 or Utf8View depending on version
            let label_strings: Vec<Option<String>> = if let Some(a) = label_col
                .as_any()
                .downcast_ref::<arrow_array::StringArray>(
            ) {
                (0..batch.num_rows())
                    .map(|i| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i).to_string())
                        }
                    })
                    .collect()
            } else if let Some(a) = label_col
                .as_any()
                .downcast_ref::<arrow_array::StringViewArray>()
            {
                (0..batch.num_rows())
                    .map(|i| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i).to_string())
                        }
                    })
                    .collect()
            } else {
                continue;
            };

            let props = match prop_col
                .as_any()
                .downcast_ref::<arrow_array::Float64Array>()
            {
                Some(a) => a,
                None => continue,
            };

            for (i, label_opt) in label_strings.into_iter().enumerate() {
                if let Some(l) = label_opt
                    && !props.is_null(i)
                {
                    proportions.push((l, props.value(i)));
                }
            }
        }

        if proportions.is_empty() {
            return Vec::new();
        }

        let summary = {
            let mut items: Vec<String> = proportions
                .iter()
                .map(|(l, p)| format!("{}={:.2}%", l, p * 100.0))
                .collect();
            items.sort();
            items.join(", ")
        };

        let mut errs = Vec::new();

        for (label_val, proportion) in &proportions {
            if *proportion > check.max_proportion {
                errs.push(format!(
                    "Quality check failed: ClassBalance check failed: class '{}' has proportion \
                     {:.2}% > max {:.2}%. All proportions: {}",
                    label_val,
                    proportion * 100.0,
                    check.max_proportion * 100.0,
                    summary,
                ));
            }

            if let Some(min) = check.min_proportion
                && *proportion < min
            {
                errs.push(format!(
                    "Quality check failed: ClassBalance check failed: class '{}' has proportion \
                     {:.2}% < min {:.2}%. All proportions: {}",
                    label_val,
                    proportion * 100.0,
                    min * 100.0,
                    summary,
                ));
            }
        }

        errs
    }

    /// Detects feature distribution drift using Population Stability Index (PSI).
    ///
    /// Uses `NTILE(num_bins)` for exact equal-frequency binning on the reference split,
    /// then bins the current split using the derived boundaries. Only bin counts are
    /// held in memory — no raw value vectors.
    ///
    /// PSI = Σ (current% - ref%) × ln(current% / ref%) over quantile bins.
    async fn check_ml_feature_drift(
        &self,
        check: &FeatureDriftCheck,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let num_bins = check.num_bins.unwrap_or(10);
        let threshold = check.threshold.unwrap_or(0.2);
        let epsilon = 1e-6;
        let mut errs = Vec::new();

        // NTILE requires at least 2 bins for meaningful PSI comparison
        if num_bins < 2 {
            return errs;
        }

        // Escape single quotes in split values to prevent SQL injection
        let ref_split_escaped = check.reference_split.replace('\'', "''");
        let cur_split_escaped = check.current_split.replace('\'', "''");

        for field in &check.feature_fields {
            // Step 1: Get exact quantile bin boundaries and counts from the reference split
            let ref_sql = format!(
                "SELECT MIN(val) AS lo, MAX(val) AS hi, COUNT(*) AS cnt, bin \
                 FROM ( \
                     SELECT CAST(\"{field}\" AS DOUBLE) AS val, \
                            NTILE({num_bins}) OVER (ORDER BY CAST(\"{field}\" AS DOUBLE)) AS bin \
                     FROM data \
                     WHERE \"{}\" = '{ref_split_escaped}' AND \"{field}\" IS NOT NULL \
                 ) GROUP BY bin ORDER BY bin",
                check.split_field,
            );

            let ref_batches = match ctx.sql(&ref_sql).await {
                Ok(df) => match df.collect().await {
                    Ok(b) => b,
                    Err(_) => {
                        errs.push(format!(
                            "Quality check failed: FeatureDrift check: insufficient reference \
                             data for field '{}' in split '{}'",
                            field, check.reference_split,
                        ));
                        continue;
                    }
                },
                Err(_) => {
                    errs.push(format!(
                        "Quality check failed: FeatureDrift check: insufficient reference \
                         data for field '{}' in split '{}'",
                        field, check.reference_split,
                    ));
                    continue;
                }
            };

            // Extract boundaries and reference bin counts, indexed by the bin column
            // to avoid depending on batch ordering.
            let mut boundaries = vec![f64::NAN; num_bins];
            let mut ref_counts = vec![0.0_f64; num_bins];
            let mut bins_found = 0usize;

            for batch in &ref_batches {
                let hi_col = batch.column(1); // MAX(val) = upper boundary
                let cnt_col = batch.column(2);
                let bin_col = batch.column(3); // bin number (1-indexed from NTILE)

                let hi_arr = match hi_col.as_any().downcast_ref::<arrow_array::Float64Array>() {
                    Some(a) => a,
                    None => continue,
                };

                for i in 0..batch.num_rows() {
                    let bin_idx = if let Some(a) =
                        bin_col.as_any().downcast_ref::<arrow_array::Int64Array>()
                    {
                        a.value(i) as usize
                    } else if let Some(a) =
                        bin_col.as_any().downcast_ref::<arrow_array::UInt64Array>()
                    {
                        a.value(i) as usize
                    } else {
                        continue;
                    };

                    // NTILE bins are 1-indexed
                    if bin_idx < 1 || bin_idx > num_bins {
                        continue;
                    }
                    let idx = bin_idx - 1;

                    if !hi_arr.is_null(i) {
                        boundaries[idx] = hi_arr.value(i);
                    }

                    let cnt = if let Some(a) =
                        cnt_col.as_any().downcast_ref::<arrow_array::Int64Array>()
                    {
                        a.value(i) as f64
                    } else if let Some(a) =
                        cnt_col.as_any().downcast_ref::<arrow_array::UInt64Array>()
                    {
                        a.value(i) as f64
                    } else {
                        0.0
                    };
                    ref_counts[idx] = cnt;
                    bins_found += 1;
                }
            }

            if bins_found < num_bins {
                errs.push(format!(
                    "Quality check failed: FeatureDrift check: insufficient reference data \
                     for field '{}' in split '{}'",
                    field, check.reference_split,
                ));
                continue;
            }

            // Build CASE WHEN for current split binning using boundary upper limits.
            // boundaries[0..n-2] are the upper limits of bins 1..n-1; the last bin catches the rest.
            let case_clauses: Vec<String> = boundaries[..boundaries.len() - 1]
                .iter()
                .enumerate()
                .map(|(i, b)| format!("WHEN CAST(\"{field}\" AS DOUBLE) <= {b} THEN {}", i + 1))
                .collect();

            let case_expr = format!(
                "CASE {} ELSE {} END",
                case_clauses.join(" "),
                boundaries.len()
            );

            // Step 2: Count current split values per bin
            let cur_sql = format!(
                "SELECT {case_expr} AS bin, COUNT(*) AS cnt \
                 FROM data \
                 WHERE \"{}\" = '{cur_split_escaped}' AND \"{field}\" IS NOT NULL \
                 GROUP BY {case_expr}",
                check.split_field,
            );

            let cur_batches = match ctx.sql(&cur_sql).await {
                Ok(df) => match df.collect().await {
                    Ok(b) => b,
                    Err(_) => {
                        errs.push(format!(
                            "Quality check failed: FeatureDrift check: no current data \
                             for field '{}' in split '{}'",
                            field, check.current_split,
                        ));
                        continue;
                    }
                },
                Err(_) => {
                    errs.push(format!(
                        "Quality check failed: FeatureDrift check: no current data \
                         for field '{}' in split '{}'",
                        field, check.current_split,
                    ));
                    continue;
                }
            };

            // Parse current bin counts
            let mut cur_counts = vec![0.0_f64; num_bins];
            let mut has_current_data = false;

            for batch in &cur_batches {
                let bin_col = batch.column(0);
                let cnt_col = batch.column(1);

                for i in 0..batch.num_rows() {
                    let bin_idx = if let Some(a) =
                        bin_col.as_any().downcast_ref::<arrow_array::Int64Array>()
                    {
                        a.value(i) as usize
                    } else if let Some(a) =
                        bin_col.as_any().downcast_ref::<arrow_array::UInt64Array>()
                    {
                        a.value(i) as usize
                    } else {
                        continue;
                    };

                    let cnt = if let Some(a) =
                        cnt_col.as_any().downcast_ref::<arrow_array::Int64Array>()
                    {
                        a.value(i) as f64
                    } else if let Some(a) =
                        cnt_col.as_any().downcast_ref::<arrow_array::UInt64Array>()
                    {
                        a.value(i) as f64
                    } else {
                        continue;
                    };

                    // NTILE bins are 1-indexed
                    if bin_idx >= 1 && bin_idx <= num_bins {
                        cur_counts[bin_idx - 1] += cnt;
                        has_current_data = true;
                    }
                }
            }

            if !has_current_data {
                errs.push(format!(
                    "Quality check failed: FeatureDrift check: no current data \
                     for field '{}' in split '{}'",
                    field, check.current_split,
                ));
                continue;
            }

            // Step 3: Compute PSI from bin counts
            let ref_total: f64 = ref_counts.iter().sum();
            let cur_total: f64 = cur_counts.iter().sum();

            let ref_pcts: Vec<f64> = ref_counts.iter().map(|c| c / ref_total + epsilon).collect();
            let cur_pcts: Vec<f64> = cur_counts.iter().map(|c| c / cur_total + epsilon).collect();

            let psi: f64 = ref_pcts
                .iter()
                .zip(cur_pcts.iter())
                .map(|(r, c)| (c - r) * (c / r).ln())
                .sum();

            if psi > threshold {
                errs.push(format!(
                    "Quality check failed: FeatureDrift check failed: field '{}' has PSI {:.4} \
                     > threshold {:.2} (reference='{}', current='{}')",
                    field, psi, threshold, check.reference_split, check.current_split,
                ));
            }
        }

        errs
    }

    /// Detects disparate null rates across groups via SQL GROUP BY.
    /// Only group-level aggregates are held in memory.
    async fn check_ml_null_rate_by_group(
        &self,
        check: &NullRateByGroupCheck,
        ctx: &SessionContext,
    ) -> Vec<String> {
        let max_diff = check.max_null_rate_diff.unwrap_or(0.1);
        let mut errs = Vec::new();

        for field in &check.check_fields {
            let sql = format!(
                "SELECT CAST(\"{}\" AS VARCHAR) AS grp, \
                        CAST(SUM(CASE WHEN \"{field}\" IS NULL THEN 1 ELSE 0 END) AS DOUBLE) \
                            / CAST(COUNT(*) AS DOUBLE) AS null_rate \
                 FROM data \
                 WHERE \"{}\" IS NOT NULL \
                 GROUP BY CAST(\"{}\" AS VARCHAR)",
                check.group_field, check.group_field, check.group_field,
            );

            let batches = match ctx.sql(&sql).await {
                Ok(df) => match df.collect().await {
                    Ok(b) => b,
                    Err(_) => continue,
                },
                Err(_) => continue,
            };

            let mut rates: Vec<(String, f64)> = Vec::new();

            for batch in &batches {
                let grp_col = batch.column(0);
                let rate_col = batch.column(1);

                // DataFusion may return Utf8 or Utf8View depending on version
                let group_strings: Vec<Option<String>> =
                    if let Some(a) = grp_col.as_any().downcast_ref::<arrow_array::StringArray>() {
                        (0..batch.num_rows())
                            .map(|i| {
                                if a.is_null(i) {
                                    None
                                } else {
                                    Some(a.value(i).to_string())
                                }
                            })
                            .collect()
                    } else if let Some(a) = grp_col
                        .as_any()
                        .downcast_ref::<arrow_array::StringViewArray>()
                    {
                        (0..batch.num_rows())
                            .map(|i| {
                                if a.is_null(i) {
                                    None
                                } else {
                                    Some(a.value(i).to_string())
                                }
                            })
                            .collect()
                    } else {
                        continue;
                    };

                let rate_arr = match rate_col
                    .as_any()
                    .downcast_ref::<arrow_array::Float64Array>()
                {
                    Some(a) => a,
                    None => continue,
                };

                for (i, grp) in group_strings.into_iter().enumerate() {
                    if let Some(g) = grp
                        && !rate_arr.is_null(i)
                    {
                        rates.push((g, rate_arr.value(i)));
                    }
                }
            }

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
                let detail: Vec<String> = rates
                    .iter()
                    .map(|(g, r)| format!("{}={:.2}%", g, r * 100.0))
                    .collect();

                errs.push(format!(
                    "Quality check failed: NullRateByGroup check failed: field '{}' has null rate \
                     diff {:.2}% > max {:.2}%. Rates: [{}]",
                    field,
                    diff * 100.0,
                    max_diff * 100.0,
                    detail.join(", "),
                ));
            }
        }

        errs
    }

    // -----------------------------------------------------------------------
    // Report
    // -----------------------------------------------------------------------

    fn build_report(
        &self,
        errors: Vec<String>,
        warnings: Vec<String>,
        contract: &Contract,
        dataset: &DataSet,
        start: Instant,
    ) -> ValidationReport {
        let constraints_evaluated: usize = contract
            .schema
            .fields
            .iter()
            .map(|f| f.constraints.as_ref().map(|c| c.len()).unwrap_or(0))
            .sum();

        let quality_checks_count = contract
            .quality_checks
            .as_ref()
            .map(|qc| {
                let mut n = 0usize;
                if qc.completeness.is_some() {
                    n += 1;
                }
                if qc.uniqueness.is_some() {
                    n += 1;
                }
                if qc.freshness.is_some() {
                    n += 1;
                }
                if let Some(ref c) = qc.custom_checks {
                    n += c.len();
                }
                if let Some(ref ml) = qc.ml_checks {
                    if ml.no_overlap.is_some() {
                        n += 1;
                    }
                    if ml.temporal_split.is_some() {
                        n += 1;
                    }
                    if ml.class_balance.is_some() {
                        n += 1;
                    }
                    if ml.feature_drift.is_some() {
                        n += 1;
                    }
                    if ml.target_leakage.is_some() {
                        n += 1;
                    }
                    if ml.null_rate_by_group.is_some() {
                        n += 1;
                    }
                }
                n
            })
            .unwrap_or(0);

        ValidationReport {
            passed: errors.is_empty(),
            errors,
            warnings,
            stats: ValidationStats {
                records_validated: dataset.len(),
                fields_checked: contract.schema.fields.len(),
                constraints_evaluated: constraints_evaluated + quality_checks_count,
                duration_ms: start.elapsed().as_millis() as u64,
            },
        }
    }
}

impl Default for DataFusionEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Shared utilities
// ---------------------------------------------------------------------------

/// Convert a DCE DataSet + contract fields into an Arrow RecordBatch.
///
/// Shared utility used by both `DataFusionEngine` and custom SQL execution.
pub(crate) fn dataset_to_record_batch(
    fields: &[Field],
    dataset: &DataSet,
) -> Result<RecordBatch, String> {
    let num_rows = dataset.len();

    // Collect rows once so each column builder borrows from the same slice,
    // avoiding O(rows * cols) cloning.
    let rows: Vec<_> = dataset.rows().cloned().collect();

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(fields.len());
    for field in fields {
        let col = build_arrow_column(field, &rows, num_rows)?;
        columns.push(col);
    }

    // Derive the schema from the actual arrays so that complex inner types
    // (which may have been collapsed to Utf8 by the builders) are consistent.
    let arrow_fields: Vec<ArrowField> = fields
        .iter()
        .zip(columns.iter())
        .map(|(f, col)| ArrowField::new(&f.name, col.data_type().clone(), f.nullable))
        .collect();
    let schema = Arc::new(ArrowSchema::new(arrow_fields));

    RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
}

/// Run a SQL query that returns a single count column and extract the i64 result.
pub(crate) async fn count_query(ctx: &SessionContext, sql: &str) -> Result<i64, String> {
    let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
    let batches = df.collect().await.map_err(|e| e.to_string())?;
    let batch = batches.first().ok_or("no batches")?;
    if batch.num_rows() == 0 {
        return Ok(0);
    }
    let col = batch.column(0);
    if let Some(a) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        Ok(a.value(0))
    } else if let Some(a) = col.as_any().downcast_ref::<arrow_array::UInt64Array>() {
        Ok(a.value(0) as i64)
    } else {
        Err(format!(
            "unexpected count column type: {:?}",
            col.data_type()
        ))
    }
}

fn build_arrow_column(
    field: &Field,
    rows: &[crate::DataRow],
    num_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, String> {
    let arrow_dt = dce_type_to_arrow(&field.field_type);
    build_arrow_array(&arrow_dt, &field.name, rows, num_rows)
}

/// Builds an Arrow array from dataset rows for a given column name and Arrow type.
///
/// Handles primitives directly and delegates to specialised helpers for
/// List, Struct, and Map types.
fn build_arrow_array(
    arrow_dt: &ArrowDataType,
    col_name: &str,
    rows: &[crate::DataRow],
    num_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, String> {
    match arrow_dt {
        ArrowDataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for row in rows {
                match row.get(col_name) {
                    Some(DataValue::String(s)) => builder.append_value(s),
                    Some(DataValue::Timestamp(s)) => builder.append_value(s),
                    Some(DataValue::Int(i)) => builder.append_value(i.to_string()),
                    Some(DataValue::Float(f)) => builder.append_value(f.to_string()),
                    Some(DataValue::Bool(b)) => builder.append_value(b.to_string()),
                    Some(DataValue::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for row in rows {
                match row.get(col_name) {
                    Some(DataValue::Int(i)) => builder.append_value(*i),
                    Some(DataValue::Float(f)) => builder.append_value(*f as i64),
                    Some(DataValue::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for row in rows {
                match row.get(col_name) {
                    Some(DataValue::Float(f)) => builder.append_value(*f),
                    Some(DataValue::Int(i)) => builder.append_value(*i as f64),
                    Some(DataValue::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for row in rows {
                match row.get(col_name) {
                    Some(DataValue::Bool(b)) => builder.append_value(*b),
                    Some(DataValue::Null) | None => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        ArrowDataType::List(item_field) => build_list_array(item_field, col_name, rows, num_rows),
        ArrowDataType::Struct(struct_fields) => {
            build_struct_array(struct_fields, col_name, rows, num_rows)
        }
        ArrowDataType::Map(entries_field, _keys_sorted) => {
            build_map_array(entries_field, col_name, rows, num_rows)
        }
        _ => {
            // Fallback: store as Utf8
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for row in rows {
                match row.get(col_name) {
                    Some(DataValue::Null) | None => builder.append_null(),
                    Some(v) => builder.append_value(format!("{v:?}")),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

// ---------------------------------------------------------------------------
// Complex-type Arrow builders
// ---------------------------------------------------------------------------

/// Append a single `DataValue` into a primitive builder, treating unsupported
/// or null values as nulls in the target array.
fn append_primitive_value(
    builder: &mut Box<dyn ArrayBuilder>,
    val: Option<&DataValue>,
    inner_dt: &ArrowDataType,
) {
    match inner_dt {
        ArrowDataType::Utf8 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            match val {
                Some(DataValue::String(s)) => b.append_value(s),
                Some(DataValue::Int(i)) => b.append_value(i.to_string()),
                Some(DataValue::Float(f)) => b.append_value(f.to_string()),
                Some(DataValue::Bool(v)) => b.append_value(v.to_string()),
                Some(DataValue::Timestamp(s)) => b.append_value(s),
                _ => b.append_null(),
            }
        }
        ArrowDataType::Int64 => {
            let b = builder.as_any_mut().downcast_mut::<Int64Builder>().unwrap();
            match val {
                Some(DataValue::Int(i)) => b.append_value(*i),
                Some(DataValue::Float(f)) => b.append_value(*f as i64),
                _ => b.append_null(),
            }
        }
        ArrowDataType::Float64 => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<Float64Builder>()
                .unwrap();
            match val {
                Some(DataValue::Float(f)) => b.append_value(*f),
                Some(DataValue::Int(i)) => b.append_value(*i as f64),
                _ => b.append_null(),
            }
        }
        ArrowDataType::Boolean => {
            let b = builder
                .as_any_mut()
                .downcast_mut::<BooleanBuilder>()
                .unwrap();
            match val {
                Some(DataValue::Bool(v)) => b.append_value(*v),
                _ => b.append_null(),
            }
        }
        _ => {
            // Nested complex inner types — serialise as JSON string
            let b = builder
                .as_any_mut()
                .downcast_mut::<StringBuilder>()
                .unwrap();
            match val {
                Some(DataValue::Null) | None => b.append_null(),
                Some(v) => b.append_value(format!("{v:?}")),
            }
        }
    }
}

/// Create a boxed primitive `ArrayBuilder` for the given Arrow type.
///
/// Non-primitive types (List, Struct, Map, etc.) are not supported by this
/// builder and fall back to `StringBuilder` (JSON serialisation).  Use
/// [`effective_builder_type`] to determine the actual Arrow type that will
/// be produced.
fn make_primitive_builder(dt: &ArrowDataType, capacity: usize) -> Box<dyn ArrayBuilder> {
    match dt {
        ArrowDataType::Int64 => Box::new(Int64Builder::with_capacity(capacity)),
        ArrowDataType::Float64 => Box::new(Float64Builder::with_capacity(capacity)),
        ArrowDataType::Boolean => Box::new(BooleanBuilder::with_capacity(capacity)),
        _ => Box::new(StringBuilder::with_capacity(capacity, capacity * 32)),
    }
}

/// Returns the Arrow type that [`make_primitive_builder`] actually produces
/// for `dt`.  Complex types are collapsed to `Utf8` because the builder
/// serialises them as JSON strings.
fn effective_builder_type(dt: &ArrowDataType) -> ArrowDataType {
    match dt {
        ArrowDataType::Int64 | ArrowDataType::Float64 | ArrowDataType::Boolean => dt.clone(),
        _ => ArrowDataType::Utf8,
    }
}

/// Build an Arrow `ListArray` from `DataValue::List` entries.
fn build_list_array(
    item_field: &Arc<ArrowField>,
    col_name: &str,
    rows: &[crate::DataRow],
    num_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, String> {
    let inner_dt = item_field.data_type();
    // Use the effective type so the builder's physical type matches the
    // schema field (complex inner types are serialised as Utf8).
    let eff = effective_builder_type(inner_dt);
    let actual_field = ArrowField::new(item_field.name(), eff.clone(), item_field.is_nullable());
    let values_builder = make_primitive_builder(inner_dt, num_rows * 4);
    let mut list_builder = ListBuilder::new(values_builder).with_field(actual_field);

    for row in rows {
        match row.get(col_name) {
            Some(DataValue::List(items)) => {
                let vb = list_builder.values();
                for item in items {
                    append_primitive_value(vb, Some(item), inner_dt);
                }
                list_builder.append(true);
            }
            Some(DataValue::Null) | None => {
                list_builder.append(false);
            }
            _ => {
                list_builder.append(false);
            }
        }
    }
    Ok(Arc::new(list_builder.finish()))
}

/// Build an Arrow `StructArray` from `DataValue::Map` entries
/// (DCE uses `Map` for both map and struct data values).
fn build_struct_array(
    struct_fields: &arrow_schema::Fields,
    col_name: &str,
    rows: &[crate::DataRow],
    num_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, String> {
    let field_defs: Vec<_> = struct_fields.iter().collect();
    let mut child_builders: Vec<Box<dyn ArrayBuilder>> = field_defs
        .iter()
        .map(|f| make_primitive_builder(f.data_type(), num_rows))
        .collect();

    let mut null_buffer = BooleanBuilder::with_capacity(num_rows);

    for row in rows {
        match row.get(col_name) {
            Some(DataValue::Map(map)) => {
                for (i, f) in field_defs.iter().enumerate() {
                    let val = map.get(f.name().as_str());
                    append_primitive_value(&mut child_builders[i], val, f.data_type());
                }
                null_buffer.append_value(true);
            }
            _ => {
                for (i, f) in field_defs.iter().enumerate() {
                    append_primitive_value(&mut child_builders[i], None, f.data_type());
                }
                null_buffer.append_value(false);
            }
        }
    }

    let child_arrays: Vec<Arc<dyn arrow_array::Array>> =
        child_builders.iter_mut().map(|b| b.finish()).collect();

    // Adjust field types to match what make_primitive_builder actually produced
    // (complex inner types are serialised as Utf8).
    let effective_fields: Vec<ArrowField> = field_defs
        .iter()
        .map(|f| {
            ArrowField::new(
                f.name(),
                effective_builder_type(f.data_type()),
                f.is_nullable(),
            )
        })
        .collect();
    let effective_schema: arrow_schema::Fields = effective_fields.into();

    let struct_array = arrow_array::StructArray::try_new(
        effective_schema,
        child_arrays,
        Some(null_buffer.finish().values().clone().into()),
    )
    .map_err(|e| format!("Failed to build StructArray for '{col_name}': {e}"))?;

    Ok(Arc::new(struct_array))
}

/// Build an Arrow `MapArray` from `DataValue::Map` entries.
fn build_map_array(
    entries_field: &Arc<ArrowField>,
    col_name: &str,
    rows: &[crate::DataRow],
    num_rows: usize,
) -> Result<Arc<dyn arrow_array::Array>, String> {
    // Extract key and value types from the entries struct
    let entries_dt = entries_field.data_type();
    let (key_dt, val_dt) = match entries_dt {
        ArrowDataType::Struct(fields) if fields.len() == 2 => {
            (fields[0].data_type().clone(), fields[1].data_type().clone())
        }
        _ => {
            return Err(format!(
                "Unexpected Map entries type for '{col_name}': {entries_dt:?}"
            ));
        }
    };

    let key_builder = make_primitive_builder(&key_dt, num_rows * 4);
    let val_builder = make_primitive_builder(&val_dt, num_rows * 4);
    let mut map_builder = MapBuilder::new(None, key_builder, val_builder);

    for row in rows {
        match row.get(col_name) {
            Some(DataValue::Map(map)) => {
                for (k, v) in map {
                    append_primitive_value(
                        map_builder.keys(),
                        Some(&DataValue::String(k.clone())),
                        &key_dt,
                    );
                    append_primitive_value(map_builder.values(), Some(v), &val_dt);
                }
                map_builder.append(true).map_err(|e| e.to_string())?;
            }
            Some(DataValue::Null) | None => {
                map_builder.append(false).map_err(|e| e.to_string())?;
            }
            _ => {
                map_builder.append(false).map_err(|e| e.to_string())?;
            }
        }
    }

    Ok(Arc::new(map_builder.finish()))
}

/// Map a DCE DataType to an Arrow DataType.
///
/// Primitive types map to their natural Arrow counterparts.
/// Complex types (List, Map, Struct) are mapped recursively to native Arrow
/// types so that DataFusion can query nested fields directly.
fn dce_type_to_arrow(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Primitive(p) => match p {
            PrimitiveType::String | PrimitiveType::Uuid => ArrowDataType::Utf8,
            PrimitiveType::Int32 => ArrowDataType::Int64,
            PrimitiveType::Int64 => ArrowDataType::Int64,
            PrimitiveType::Float32 => ArrowDataType::Float64,
            PrimitiveType::Float64 => ArrowDataType::Float64,
            PrimitiveType::Boolean => ArrowDataType::Boolean,
            PrimitiveType::Timestamp | PrimitiveType::Date | PrimitiveType::Time => {
                ArrowDataType::Utf8
            }
            PrimitiveType::Decimal | PrimitiveType::Binary => ArrowDataType::Utf8,
        },
        DataType::List {
            element_type,
            contains_null,
        } => {
            let inner = dce_type_to_arrow(element_type);
            ArrowDataType::List(Arc::new(ArrowField::new("item", inner, *contains_null)))
        }
        DataType::Map {
            key_type: _,
            value_type,
            value_contains_null,
        } => {
            // DataValue::Map is backed by HashMap<String, DataValue>, so keys
            // are always strings at runtime.  Force key type to Utf8 to keep
            // the Arrow schema consistent with the actual builder output.
            let val = dce_type_to_arrow(value_type);
            ArrowDataType::Map(
                Arc::new(ArrowField::new(
                    "entries",
                    ArrowDataType::Struct(
                        vec![
                            ArrowField::new("key", ArrowDataType::Utf8, false),
                            ArrowField::new("value", val, *value_contains_null),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            )
        }
        DataType::Struct { fields } => {
            let arrow_fields: Vec<ArrowField> = fields
                .iter()
                .map(|f| ArrowField::new(&f.name, dce_type_to_arrow(&f.data_type), f.nullable))
                .collect();
            ArrowDataType::Struct(arrow_fields.into())
        }
    }
}

#[cfg(test)]
mod ml_tests {
    use super::*;
    use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};

    /// Helper: register a RecordBatch as "data" in a new SessionContext.
    async fn ctx_with_batch(batch: RecordBatch) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_batch("data", batch).unwrap();
        ctx
    }

    // -----------------------------------------------------------------------
    // TargetLeakage
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_ml_target_leakage_sql_detects_perfect_correlation() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("feature", ArrowDataType::Float64, false),
            ArrowField::new("target", ArrowDataType::Float64, false),
        ]));

        let feature = arrow_array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let target = arrow_array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(feature), Arc::new(target)]).unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = TargetLeakageCheck {
            target_field: "target".to_string(),
            feature_fields: vec!["feature".to_string()],
            max_correlation: Some(0.95),
        };

        let errs = engine.check_ml_target_leakage(&check, &ctx).await;
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("TargetLeakage"));
        assert!(errs[0].contains("feature"));
    }

    #[tokio::test]
    async fn test_ml_target_leakage_sql_passes_low_correlation() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("feature", ArrowDataType::Float64, false),
            ArrowField::new("target", ArrowDataType::Float64, false),
        ]));

        let feature = arrow_array::Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let target = arrow_array::Float64Array::from(vec![5.0, 1.0, 4.0, 2.0, 3.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(feature), Arc::new(target)]).unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = TargetLeakageCheck {
            target_field: "target".to_string(),
            feature_fields: vec!["feature".to_string()],
            max_correlation: Some(0.95),
        };

        let errs = engine.check_ml_target_leakage(&check, &ctx).await;
        assert!(errs.is_empty());
    }

    #[tokio::test]
    async fn test_ml_target_leakage_sql_skips_insufficient_data() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("feature", ArrowDataType::Float64, false),
            ArrowField::new("target", ArrowDataType::Float64, false),
        ]));

        let feature = arrow_array::Float64Array::from(vec![1.0, 2.0]);
        let target = arrow_array::Float64Array::from(vec![1.0, 2.0]);
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(feature), Arc::new(target)]).unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = TargetLeakageCheck {
            target_field: "target".to_string(),
            feature_fields: vec!["feature".to_string()],
            max_correlation: Some(0.95),
        };

        let errs = engine.check_ml_target_leakage(&check, &ctx).await;
        assert!(errs.is_empty(), "Should skip with < 3 data points");
    }

    // -----------------------------------------------------------------------
    // ClassBalance
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_ml_class_balance_sql_detects_imbalance() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "label",
            ArrowDataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        for _ in 0..95 {
            builder.append_value("A");
        }
        for _ in 0..5 {
            builder.append_value("B");
        }
        let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = ClassBalanceCheck {
            label_field: "label".to_string(),
            max_proportion: 0.9,
            min_proportion: None,
        };

        let errs = engine.check_ml_class_balance(&check, &ctx).await;
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("ClassBalance"));
        assert!(errs[0].contains("A"));
    }

    #[tokio::test]
    async fn test_ml_class_balance_sql_passes_balanced() {
        let schema = Arc::new(ArrowSchema::new(vec![ArrowField::new(
            "label",
            ArrowDataType::Utf8,
            false,
        )]));

        let mut builder = StringBuilder::new();
        for _ in 0..50 {
            builder.append_value("A");
        }
        for _ in 0..50 {
            builder.append_value("B");
        }
        let batch = RecordBatch::try_new(schema, vec![Arc::new(builder.finish())]).unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = ClassBalanceCheck {
            label_field: "label".to_string(),
            max_proportion: 0.9,
            min_proportion: None,
        };

        let errs = engine.check_ml_class_balance(&check, &ctx).await;
        assert!(errs.is_empty());
    }

    // -----------------------------------------------------------------------
    // NullRateByGroup
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_ml_null_rate_by_group_sql_detects_disparity() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("group", ArrowDataType::Utf8, false),
            ArrowField::new("value", ArrowDataType::Float64, true),
        ]));

        let mut group_builder = StringBuilder::new();
        let mut value_builder = Float64Builder::new();

        // Group A: 10 rows, 0 nulls
        for i in 0..10 {
            group_builder.append_value("A");
            value_builder.append_value(i as f64);
        }
        // Group B: 10 rows, 5 nulls (50% null rate)
        for i in 0..10 {
            group_builder.append_value("B");
            if i < 5 {
                value_builder.append_null();
            } else {
                value_builder.append_value(i as f64);
            }
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(group_builder.finish()),
                Arc::new(value_builder.finish()),
            ],
        )
        .unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = NullRateByGroupCheck {
            group_field: "group".to_string(),
            check_fields: vec!["value".to_string()],
            max_null_rate_diff: Some(0.1),
        };

        let errs = engine.check_ml_null_rate_by_group(&check, &ctx).await;
        assert_eq!(errs.len(), 1);
        assert!(errs[0].contains("NullRateByGroup"));
        assert!(errs[0].contains("value"));
    }

    // -----------------------------------------------------------------------
    // FeatureDrift (PSI)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_ml_feature_drift_sql_detects_shift() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("split", ArrowDataType::Utf8, false),
            ArrowField::new("feature", ArrowDataType::Float64, false),
        ]));

        let mut split_builder = StringBuilder::new();
        let mut feature_builder = Float64Builder::new();

        // Reference split: values 0..100
        for i in 0..100 {
            split_builder.append_value("train");
            feature_builder.append_value(i as f64);
        }
        // Current split: values 50..150 (shifted distribution)
        for i in 50..150 {
            split_builder.append_value("test");
            feature_builder.append_value(i as f64);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(split_builder.finish()),
                Arc::new(feature_builder.finish()),
            ],
        )
        .unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = FeatureDriftCheck {
            split_field: "split".to_string(),
            reference_split: "train".to_string(),
            current_split: "test".to_string(),
            feature_fields: vec!["feature".to_string()],
            num_bins: Some(10),
            threshold: Some(0.2),
        };

        let errs = engine.check_ml_feature_drift(&check, &ctx).await;
        assert_eq!(errs.len(), 1, "Expected PSI drift error, got: {:?}", errs);
        assert!(errs[0].contains("FeatureDrift"));
        assert!(errs[0].contains("PSI"));
    }

    #[tokio::test]
    async fn test_ml_feature_drift_sql_passes_same_distribution() {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new("split", ArrowDataType::Utf8, false),
            ArrowField::new("feature", ArrowDataType::Float64, false),
        ]));

        let mut split_builder = StringBuilder::new();
        let mut feature_builder = Float64Builder::new();

        // Both splits have the same distribution: 0..100
        for i in 0..100 {
            split_builder.append_value("train");
            feature_builder.append_value(i as f64);
        }
        for i in 0..100 {
            split_builder.append_value("test");
            feature_builder.append_value(i as f64);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(split_builder.finish()),
                Arc::new(feature_builder.finish()),
            ],
        )
        .unwrap();
        let ctx = ctx_with_batch(batch).await;

        let engine = DataFusionEngine::new();
        let check = FeatureDriftCheck {
            split_field: "split".to_string(),
            reference_split: "train".to_string(),
            current_split: "test".to_string(),
            feature_fields: vec!["feature".to_string()],
            num_bins: Some(10),
            threshold: Some(0.2),
        };

        let errs = engine.check_ml_feature_drift(&check, &ctx).await;
        assert!(
            errs.is_empty(),
            "Same distribution should pass, got: {:?}",
            errs
        );
    }
}

#[cfg(test)]
mod complex_type_tests {
    use super::*;
    use contracts_core::{PrimitiveType, StructField as DceStructField};

    #[test]
    fn dce_type_to_arrow_list_string() {
        let dt = DataType::List {
            element_type: Box::new(DataType::Primitive(PrimitiveType::String)),
            contains_null: true,
        };
        let arrow = dce_type_to_arrow(&dt);
        assert!(
            matches!(arrow, ArrowDataType::List(_)),
            "Expected List, got {arrow:?}"
        );
    }

    #[test]
    fn dce_type_to_arrow_struct() {
        let dt = DataType::Struct {
            fields: vec![
                DceStructField {
                    name: "x".to_string(),
                    data_type: DataType::Primitive(PrimitiveType::Int64),
                    nullable: false,
                },
                DceStructField {
                    name: "y".to_string(),
                    data_type: DataType::Primitive(PrimitiveType::String),
                    nullable: true,
                },
            ],
        };
        let arrow = dce_type_to_arrow(&dt);
        match &arrow {
            ArrowDataType::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].name(), "x");
                assert_eq!(fields[1].name(), "y");
            }
            other => panic!("Expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn dce_type_to_arrow_map_string_int() {
        let dt = DataType::Map {
            key_type: Box::new(DataType::Primitive(PrimitiveType::String)),
            value_type: Box::new(DataType::Primitive(PrimitiveType::Int64)),
            value_contains_null: true,
        };
        let arrow = dce_type_to_arrow(&dt);
        assert!(
            matches!(arrow, ArrowDataType::Map(_, _)),
            "Expected Map, got {arrow:?}"
        );
    }

    #[test]
    fn build_list_column_from_dataset() {
        let dt = DataType::List {
            element_type: Box::new(DataType::Primitive(PrimitiveType::String)),
            contains_null: false,
        };
        let field = Field {
            name: "tags".to_string(),
            field_type: dt,
            nullable: false,
            description: None,
            constraints: None,
            tags: None,
        };

        let mut row1 = std::collections::HashMap::new();
        row1.insert(
            "tags".to_string(),
            DataValue::List(vec![
                DataValue::String("a".to_string()),
                DataValue::String("b".to_string()),
            ]),
        );

        let mut row2 = std::collections::HashMap::new();
        row2.insert(
            "tags".to_string(),
            DataValue::List(vec![DataValue::String("c".to_string())]),
        );

        let dataset = DataSet::from_rows(vec![row1, row2]);
        let rows: Vec<_> = dataset.rows().cloned().collect();
        let result = build_arrow_column(&field, &rows, 2);
        assert!(result.is_ok(), "build_arrow_column failed: {:?}", result);
        let array = result.unwrap();
        assert_eq!(array.len(), 2);
    }

    #[test]
    fn build_struct_column_from_dataset() {
        let dt = DataType::Struct {
            fields: vec![
                DceStructField {
                    name: "x".to_string(),
                    data_type: DataType::Primitive(PrimitiveType::Int64),
                    nullable: false,
                },
                DceStructField {
                    name: "y".to_string(),
                    data_type: DataType::Primitive(PrimitiveType::String),
                    nullable: true,
                },
            ],
        };
        let field = Field {
            name: "point".to_string(),
            field_type: dt,
            nullable: false,
            description: None,
            constraints: None,
            tags: None,
        };

        let mut inner = std::collections::HashMap::new();
        inner.insert("x".to_string(), DataValue::Int(42));
        inner.insert("y".to_string(), DataValue::String("hello".to_string()));

        let mut row = std::collections::HashMap::new();
        row.insert("point".to_string(), DataValue::Map(inner));

        let dataset = DataSet::from_rows(vec![row]);
        let rows: Vec<_> = dataset.rows().cloned().collect();
        let result = build_arrow_column(&field, &rows, 1);
        assert!(result.is_ok(), "build_arrow_column failed: {:?}", result);
        let array = result.unwrap();
        assert_eq!(array.len(), 1);
    }

    #[test]
    fn build_map_column_from_dataset() {
        let dt = DataType::Map {
            key_type: Box::new(DataType::Primitive(PrimitiveType::String)),
            value_type: Box::new(DataType::Primitive(PrimitiveType::String)),
            value_contains_null: true,
        };
        let field = Field {
            name: "props".to_string(),
            field_type: dt,
            nullable: true,
            description: None,
            constraints: None,
            tags: None,
        };

        let mut inner = std::collections::HashMap::new();
        inner.insert("k1".to_string(), DataValue::String("v1".to_string()));
        inner.insert("k2".to_string(), DataValue::String("v2".to_string()));

        let mut row = std::collections::HashMap::new();
        row.insert("props".to_string(), DataValue::Map(inner));

        let dataset = DataSet::from_rows(vec![row]);
        let rows: Vec<_> = dataset.rows().cloned().collect();
        let result = build_arrow_column(&field, &rows, 1);
        assert!(result.is_ok(), "build_arrow_column failed: {:?}", result);
        let array = result.unwrap();
        assert_eq!(array.len(), 1);
    }
}
