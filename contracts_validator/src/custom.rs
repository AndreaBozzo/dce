//! Custom check and freshness validation logic.
//!
//! This module handles:
//! - Freshness checks: Validates data staleness based on timestamps
//! - Custom SQL checks: Syntax validation (execution deferred to Phase 2)

use crate::{DataSet, ValidationError};
use chrono::{DateTime, Duration, Utc};
use contracts_core::{Contract, CustomCheck, FreshnessCheck};

/// Validates custom checks and freshness requirements.
pub struct CustomValidator;

impl CustomValidator {
    /// Creates a new custom validator.
    pub fn new() -> Self {
        Self
    }

    /// Validates freshness and custom checks in a contract.
    pub fn validate(&self, contract: &Contract, dataset: &DataSet) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        let quality_checks = match &contract.quality_checks {
            Some(qc) => qc,
            None => return errors,
        };

        // Skip checks for empty datasets
        if dataset.is_empty() {
            return errors;
        }

        // Freshness check
        if let Some(freshness) = &quality_checks.freshness {
            if let Err(err) = self.validate_freshness(freshness, dataset) {
                errors.push(err);
            }
        }

        // Custom checks - for now just validate syntax
        if let Some(custom_checks) = &quality_checks.custom_checks {
            errors.extend(self.validate_custom_checks(custom_checks));
        }

        errors
    }

    /// Validates freshness requirements.
    fn validate_freshness(
        &self,
        check: &FreshnessCheck,
        dataset: &DataSet,
    ) -> Result<(), ValidationError> {
        let max_delay = parse_duration(&check.max_delay)?;
        let now = Utc::now();

        // Find the most recent timestamp in the metric field
        let mut most_recent: Option<DateTime<Utc>> = None;

        for row in dataset.rows() {
            if let Some(value) = row.get(&check.metric) {
                if let Some(ts_str) = value.as_timestamp() {
                    match parse_timestamp(ts_str) {
                        Ok(ts) => {
                            if most_recent.is_none() || ts > most_recent.unwrap() {
                                most_recent = Some(ts);
                            }
                        }
                        Err(_) => continue, // Skip invalid timestamps
                    }
                }
            }
        }

        let most_recent = most_recent.ok_or_else(|| {
            ValidationError::quality_check(format!(
                "Freshness check failed: no valid timestamps found in field '{}'",
                check.metric
            ))
        })?;

        let age = now.signed_duration_since(most_recent);

        if age > max_delay {
            return Err(ValidationError::StaleData {
                delay: format_duration(age),
            });
        }

        Ok(())
    }

    /// Validates custom SQL checks (syntax only, no execution).
    fn validate_custom_checks(&self, checks: &[CustomCheck]) -> Vec<ValidationError> {
        let mut errors = Vec::new();

        for check in checks {
            // Basic syntax validation
            if check.definition.trim().is_empty() {
                errors.push(ValidationError::custom_check(
                    &check.name,
                    "Custom check definition is empty",
                ));
                continue;
            }

            // Check if it looks like SQL (very basic validation)
            let def_upper = check.definition.to_uppercase();
            if !def_upper.contains("SELECT") && !def_upper.contains("COUNT") {
                errors.push(ValidationError::custom_check(
                    &check.name,
                    "Custom check definition does not appear to be a SQL query",
                ));
            }

            // Note: Full SQL parsing and execution is deferred to Phase 2
        }

        errors
    }
}

impl Default for CustomValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses a duration string like "1h", "30m", "1d".
fn parse_duration(duration_str: &str) -> Result<Duration, ValidationError> {
    let duration_str = duration_str.trim();

    if duration_str.is_empty() {
        return Err(ValidationError::InvalidDuration(
            "Duration string is empty".to_string(),
        ));
    }

    // Extract number and unit
    let (num_str, unit) = duration_str.split_at(
        duration_str
            .chars()
            .position(|c| !c.is_numeric())
            .unwrap_or(duration_str.len()),
    );

    let num: i64 = num_str.parse().map_err(|_| {
        ValidationError::InvalidDuration(format!("Invalid number in duration: {}", num_str))
    })?;

    let duration = match unit.trim().to_lowercase().as_str() {
        "s" | "sec" | "second" | "seconds" => Duration::seconds(num),
        "m" | "min" | "minute" | "minutes" => Duration::minutes(num),
        "h" | "hr" | "hour" | "hours" => Duration::hours(num),
        "d" | "day" | "days" => Duration::days(num),
        "w" | "week" | "weeks" => Duration::weeks(num),
        "" if num_str == duration_str => {
            // No unit specified, assume seconds
            Duration::seconds(num)
        }
        _ => {
            return Err(ValidationError::InvalidDuration(format!(
                "Unknown duration unit: {}",
                unit
            )))
        }
    };

    Ok(duration)
}

/// Parses a timestamp string in ISO 8601 format.
fn parse_timestamp(ts_str: &str) -> Result<DateTime<Utc>, ValidationError> {
    DateTime::parse_from_rfc3339(ts_str)
        .map(|dt| dt.with_timezone(&Utc))
        .or_else(|_| {
            // Try parsing as a simple date
            ts_str.parse::<DateTime<Utc>>().map_err(|_| {
                ValidationError::InvalidDuration(format!("Invalid timestamp: {}", ts_str))
            })
        })
}

/// Formats a duration for display.
fn format_duration(duration: Duration) -> String {
    if duration.num_days() > 0 {
        format!("{}d", duration.num_days())
    } else if duration.num_hours() > 0 {
        format!("{}h", duration.num_hours())
    } else if duration.num_minutes() > 0 {
        format!("{}m", duration.num_minutes())
    } else {
        format!("{}s", duration.num_seconds())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DataValue;
    use contracts_core::{ContractBuilder, DataFormat, FieldBuilder, QualityChecks};
    use std::collections::HashMap;

    #[test]
    fn test_parse_duration() {
        assert_eq!(parse_duration("1h").unwrap(), Duration::hours(1));
        assert_eq!(parse_duration("30m").unwrap(), Duration::minutes(30));
        assert_eq!(parse_duration("1d").unwrap(), Duration::days(1));
        assert_eq!(parse_duration("2w").unwrap(), Duration::weeks(2));
        assert_eq!(parse_duration("90s").unwrap(), Duration::seconds(90));
    }

    #[test]
    fn test_parse_duration_variants() {
        assert_eq!(parse_duration("1hour").unwrap(), Duration::hours(1));
        assert_eq!(parse_duration("5minutes").unwrap(), Duration::minutes(5));
        assert_eq!(parse_duration("2days").unwrap(), Duration::days(2));
    }

    #[test]
    fn test_parse_duration_invalid() {
        assert!(parse_duration("").is_err());
        assert!(parse_duration("abc").is_err());
        assert!(parse_duration("1x").is_err());
    }

    #[test]
    fn test_freshness_check_pass() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("timestamp", "timestamp")
                    .nullable(false)
                    .build(),
            )
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: None,
                freshness: Some(FreshnessCheck {
                    max_delay: "1h".to_string(),
                    metric: "timestamp".to_string(),
                }),
                custom_checks: None,
            })
            .build();

        // Create a recent timestamp
        let now = Utc::now();
        let recent = now - Duration::minutes(10); // 10 minutes ago

        let mut row = HashMap::new();
        row.insert(
            "timestamp".to_string(),
            DataValue::Timestamp(recent.to_rfc3339()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let validator = CustomValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_freshness_check_fail() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("timestamp", "timestamp")
                    .nullable(false)
                    .build(),
            )
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: None,
                freshness: Some(FreshnessCheck {
                    max_delay: "1h".to_string(),
                    metric: "timestamp".to_string(),
                }),
                custom_checks: None,
            })
            .build();

        // Create an old timestamp
        let now = Utc::now();
        let old = now - Duration::hours(2); // 2 hours ago

        let mut row = HashMap::new();
        row.insert(
            "timestamp".to_string(),
            DataValue::Timestamp(old.to_rfc3339()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let validator = CustomValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(errors[0], ValidationError::StaleData { .. }));
    }

    #[test]
    fn test_custom_check_validation() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: None,
                freshness: None,
                custom_checks: Some(vec![CustomCheck {
                    name: "test_check".to_string(),
                    definition: "SELECT COUNT(*) FROM table".to_string(),
                    severity: Some("error".to_string()),
                }]),
            })
            .build();

        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("1".to_string()));

        let dataset = DataSet::from_rows(vec![row]);
        let validator = CustomValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0); // Valid SQL syntax
    }

    #[test]
    fn test_custom_check_empty_definition() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(FieldBuilder::new("id", "string").nullable(false).build())
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: None,
                freshness: None,
                custom_checks: Some(vec![CustomCheck {
                    name: "empty_check".to_string(),
                    definition: "".to_string(),
                    severity: Some("error".to_string()),
                }]),
            })
            .build();

        let mut row = HashMap::new();
        row.insert("id".to_string(), DataValue::String("1".to_string()));

        let dataset = DataSet::from_rows(vec![row]);
        let validator = CustomValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            errors[0],
            ValidationError::CustomCheckFailed { .. }
        ));
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(Duration::days(2)), "2d");
        assert_eq!(format_duration(Duration::hours(5)), "5h");
        assert_eq!(format_duration(Duration::minutes(45)), "45m");
        assert_eq!(format_duration(Duration::seconds(30)), "30s");
    }
}
