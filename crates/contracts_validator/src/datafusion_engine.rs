//! DataFusion-backed validation engine.
//!
//! Translates contract constraints into SQL queries and executes them via
//! DataFusion against an in-memory Arrow table.  This provides vectorized,
//! batch-level validation instead of row-by-row iteration.

use crate::{DataSet, DataValue};
use arrow_array::RecordBatch;
use arrow_array::builder::*;
use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
use contracts_core::{
    CompletenessCheck, Contract, Field, FieldConstraints, QualityChecks, UniquenessCheck,
    ValidationContext, ValidationReport, ValidationStats,
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

        // Sample if requested
        let dataset = if let Some(size) = context.sample_size {
            dataset.sample(size)
        } else {
            dataset.clone()
        };

        // Build Arrow RecordBatch from dataset
        let batch = match self.dataset_to_record_batch(contract, &dataset) {
            Ok(b) => b,
            Err(e) => {
                errors.push(format!("Failed to create Arrow batch: {e}"));
                return self.build_report(errors, warnings, contract, &dataset, start);
            }
        };

        // Create DataFusion context and register the table
        let ctx = SessionContext::new();
        if let Err(e) = ctx.register_batch("data", batch) {
            errors.push(format!("Failed to register table: {e}"));
            return self.build_report(errors, warnings, contract, &dataset, start);
        }

        // --- 1. Schema / nullability checks ---
        let null_errs = self.check_nullability(contract, &ctx).await;
        errors.extend(null_errs);

        if context.strict && !errors.is_empty() {
            return self.build_report(errors, warnings, contract, &dataset, start);
        }

        // --- 2. Field constraints ---
        let constraint_errs = self.check_constraints(contract, &ctx).await;
        errors.extend(constraint_errs);

        if context.schema_only {
            return self.build_report(errors, warnings, contract, &dataset, start);
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

        self.build_report(errors, warnings, contract, &dataset, start)
    }

    // -----------------------------------------------------------------------
    // Nullability
    // -----------------------------------------------------------------------

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
            if let Ok(cnt) = self.count_query(ctx, &sql).await
                && cnt > 0
            {
                errs.push(format!(
                    "Field '{}' is null but nullability is not allowed ({cnt} row(s))",
                    field.name
                ));
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
        match self.count_query(ctx, &sql).await {
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
        match self.count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Constraint violation for field '{}': {cnt} row(s) out of range [{min}, {max}]",
                field.name
            )],
            _ => Vec::new(),
        }
    }

    async fn check_pattern(&self, field: &Field, regex: &str, ctx: &SessionContext) -> Vec<String> {
        // DataFusion supports `~` (regexp match) operator
        let escaped = regex.replace('\'', "''");
        let sql = format!(
            "SELECT COUNT(*) AS cnt FROM data \
             WHERE \"{}\" IS NOT NULL AND CAST(\"{}\" AS VARCHAR) NOT SIMILAR TO '{escaped}'",
            field.name, field.name
        );
        // Fallback: if SIMILAR TO is not compatible with the regex, try regexp_match
        match self.count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Constraint violation for field '{}': {cnt} row(s) do not match pattern '{regex}'",
                field.name
            )],
            Err(_) => {
                // Try with regexp_match as fallback
                let sql2 = format!(
                    "SELECT COUNT(*) AS cnt FROM data \
                     WHERE \"{}\" IS NOT NULL AND regexp_match(CAST(\"{}\" AS VARCHAR), '{escaped}') IS NULL",
                    field.name, field.name
                );
                match self.count_query(ctx, &sql2).await {
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
        // Freshness and custom SQL checks remain as-is (not easily DataFusion-izable
        // because they depend on wall-clock time comparisons).
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
        match self.count_query(ctx, &sql).await {
            Ok(cnt) if cnt > 0 => vec![format!(
                "Quality check failed: Uniqueness check failed for fields [{}]: found {} duplicate(s)",
                check.fields.join(", "),
                cnt
            )],
            _ => Vec::new(),
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /// Run a SQL query that returns a single `COUNT(*)` / `cnt` column and
    /// extract the i64 result.
    async fn count_query(&self, ctx: &SessionContext, sql: &str) -> Result<i64, String> {
        let df = ctx.sql(sql).await.map_err(|e| e.to_string())?;
        let batches = df.collect().await.map_err(|e| e.to_string())?;
        let batch = batches.first().ok_or("no batches")?;
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        let col = batch.column(0);
        // DataFusion may return Int64, UInt64 or others depending on the query
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

    /// Convert a DCE DataSet + Contract schema into an Arrow RecordBatch.
    fn dataset_to_record_batch(
        &self,
        contract: &Contract,
        dataset: &DataSet,
    ) -> Result<RecordBatch, String> {
        let fields = &contract.schema.fields;
        let num_rows = dataset.len();

        // Build Arrow schema
        let arrow_fields: Vec<ArrowField> = fields
            .iter()
            .map(|f| {
                let dt = dce_type_to_arrow(&f.field_type);
                ArrowField::new(&f.name, dt, f.nullable)
            })
            .collect();
        let schema = Arc::new(ArrowSchema::new(arrow_fields));

        // Build columns
        let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(fields.len());
        for field in fields {
            let col = self.build_arrow_column(field, dataset, num_rows)?;
            columns.push(col);
        }

        RecordBatch::try_new(schema, columns).map_err(|e| e.to_string())
    }

    fn build_arrow_column(
        &self,
        field: &Field,
        dataset: &DataSet,
        num_rows: usize,
    ) -> Result<Arc<dyn arrow_array::Array>, String> {
        let dt = dce_type_to_arrow(&field.field_type);
        match dt {
            DataType::Utf8 => {
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in dataset.rows() {
                    match row.get(&field.name) {
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
            DataType::Int64 => {
                let mut builder = Int64Builder::with_capacity(num_rows);
                for row in dataset.rows() {
                    match row.get(&field.name) {
                        Some(DataValue::Int(i)) => builder.append_value(*i),
                        Some(DataValue::Float(f)) => builder.append_value(*f as i64),
                        Some(DataValue::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::with_capacity(num_rows);
                for row in dataset.rows() {
                    match row.get(&field.name) {
                        Some(DataValue::Float(f)) => builder.append_value(*f),
                        Some(DataValue::Int(i)) => builder.append_value(*i as f64),
                        Some(DataValue::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::with_capacity(num_rows);
                for row in dataset.rows() {
                    match row.get(&field.name) {
                        Some(DataValue::Bool(b)) => builder.append_value(*b),
                        Some(DataValue::Null) | None => builder.append_null(),
                        _ => builder.append_null(),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
            _ => {
                // Fallback: store as Utf8
                let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
                for row in dataset.rows() {
                    match row.get(&field.name) {
                        Some(DataValue::Null) | None => builder.append_null(),
                        Some(v) => builder.append_value(format!("{:?}", v)),
                    }
                }
                Ok(Arc::new(builder.finish()))
            }
        }
    }

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

/// Map a DCE type string to an Arrow DataType.
fn dce_type_to_arrow(type_str: &str) -> DataType {
    match type_str.to_lowercase().as_str() {
        "string" | "varchar" | "text" => DataType::Utf8,
        "int" | "int32" | "integer" => DataType::Int64,
        "int64" | "long" | "bigint" => DataType::Int64,
        "float" | "float32" => DataType::Float64,
        "float64" | "double" => DataType::Float64,
        "boolean" | "bool" => DataType::Boolean,
        "timestamp" | "datetime" => DataType::Utf8, // store as string; DataFusion casts as needed
        t if t.starts_with("map") || t.starts_with("list") || t.starts_with("array") => {
            DataType::Utf8
        }
        _ => DataType::Utf8,
    }
}
