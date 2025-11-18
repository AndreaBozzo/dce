//! Custom check and freshness validation logic.
//!
//! This module handles:
//! - Freshness checks: Validates data staleness based on timestamps
//! - Custom SQL checks: Syntax validation (execution deferred to Phase 2)

use crate::{DataSet, ValidationError};
use chrono::{DateTime, Duration, Utc};
use contracts_core::{Contract, CustomCheck, FreshnessCheck};

#[cfg(test)]
use chrono::Timelike;

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

/// Parses a timestamp string in multiple formats.
///
/// Supports:
/// - ISO 8601 / RFC 3339 (e.g., "2024-01-15T10:30:00Z", "2024-01-15T10:30:00+00:00")
/// - Unix epoch seconds (e.g., "1705318200")
/// - Unix epoch milliseconds (e.g., "1705318200000")
/// - Date only format (e.g., "2024-01-15")
/// - Common datetime formats (e.g., "2024-01-15 10:30:00")
fn parse_timestamp(ts_str: &str) -> Result<DateTime<Utc>, ValidationError> {
    let ts_str = ts_str.trim();

    // Try ISO 8601 / RFC 3339 format first (most common)
    if let Ok(dt) = DateTime::parse_from_rfc3339(ts_str) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing as unix epoch (seconds or milliseconds)
    if let Ok(epoch) = ts_str.parse::<i64>() {
        // If the number is very large, assume milliseconds
        if epoch > 10_000_000_000 {
            // Milliseconds
            if let Some(dt) = chrono::DateTime::from_timestamp_millis(epoch) {
                return Ok(dt);
            }
        } else {
            // Seconds
            if let Some(dt) = chrono::DateTime::from_timestamp(epoch, 0) {
                return Ok(dt);
            }
        }
    }

    // Try common date-time formats without timezone
    // Format: YYYY-MM-DD HH:MM:SS
    if ts_str.contains(' ') && ts_str.len() >= 19 {
        if let Ok(naive) = chrono::NaiveDateTime::parse_from_str(ts_str, "%Y-%m-%d %H:%M:%S") {
            return Ok(DateTime::from_naive_utc_and_offset(naive, Utc));
        }
    }

    // Try date-only format (assume start of day UTC)
    // Format: YYYY-MM-DD
    if ts_str.len() == 10 && ts_str.chars().filter(|c| *c == '-').count() == 2 {
        if let Ok(date) = chrono::NaiveDate::parse_from_str(ts_str, "%Y-%m-%d") {
            let datetime = date.and_hms_opt(0, 0, 0).unwrap();
            return Ok(DateTime::from_naive_utc_and_offset(datetime, Utc));
        }
    }

    // Try standard DateTime<Utc> parsing as fallback
    if let Ok(dt) = ts_str.parse::<DateTime<Utc>>() {
        return Ok(dt);
    }

    Err(ValidationError::InvalidDuration(format!(
        "Invalid timestamp: {}. Supported formats: ISO 8601, Unix epoch (seconds/milliseconds), YYYY-MM-DD, YYYY-MM-DD HH:MM:SS",
        ts_str
    )))
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

    #[test]
    fn test_parse_timestamp_iso8601() {
        // ISO 8601 / RFC 3339 format
        assert!(parse_timestamp("2024-01-15T10:30:00Z").is_ok());
        assert!(parse_timestamp("2024-01-15T10:30:00+00:00").is_ok());
        assert!(parse_timestamp("2024-01-15T10:30:00-05:00").is_ok());
    }

    #[test]
    fn test_parse_timestamp_unix_epoch() {
        // Unix epoch seconds (2024-01-15 10:30:00 UTC)
        let ts = parse_timestamp("1705318200").unwrap();
        assert_eq!(ts.timestamp(), 1705318200);

        // Unix epoch milliseconds
        let ts = parse_timestamp("1705318200000").unwrap();
        assert_eq!(ts.timestamp(), 1705318200);
    }

    #[test]
    fn test_parse_timestamp_date_only() {
        // Date-only format (assumes start of day UTC)
        let ts = parse_timestamp("2024-01-15").unwrap();
        assert_eq!(ts.format("%Y-%m-%d").to_string(), "2024-01-15");
        assert_eq!(ts.hour(), 0);
        assert_eq!(ts.minute(), 0);
        assert_eq!(ts.second(), 0);
    }

    #[test]
    fn test_parse_timestamp_datetime_space() {
        // Common datetime format with space
        let ts = parse_timestamp("2024-01-15 10:30:00").unwrap();
        assert_eq!(
            ts.format("%Y-%m-%d %H:%M:%S").to_string(),
            "2024-01-15 10:30:00"
        );
    }

    #[test]
    fn test_parse_timestamp_with_whitespace() {
        // Should handle leading/trailing whitespace
        assert!(parse_timestamp("  2024-01-15T10:30:00Z  ").is_ok());
        assert!(parse_timestamp(" 1705318200 ").is_ok());
    }

    #[test]
    fn test_parse_timestamp_invalid() {
        // Invalid formats should return error
        assert!(parse_timestamp("invalid").is_err());
        assert!(parse_timestamp("").is_err());
        assert!(parse_timestamp("2024-13-45").is_err()); // Invalid date
        assert!(parse_timestamp("not a timestamp").is_err());
    }

    #[test]
    fn test_parse_timestamp_edge_cases() {
        // Epoch 0
        let ts = parse_timestamp("0").unwrap();
        assert_eq!(ts.timestamp(), 0);

        // Recent timestamp
        let now = Utc::now();
        let ts_str = now.to_rfc3339();
        assert!(parse_timestamp(&ts_str).is_ok());
    }

    #[test]
    fn test_freshness_with_unix_epoch() {
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

        // Create recent timestamp using unix epoch
        let now = Utc::now();
        let recent = now - Duration::minutes(10);
        let epoch = recent.timestamp();

        let mut row = HashMap::new();
        row.insert(
            "timestamp".to_string(),
            DataValue::Timestamp(epoch.to_string()),
        );

        let dataset = DataSet::from_rows(vec![row]);
        let validator = CustomValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }

    #[test]
    fn test_freshness_with_date_only() {
        let contract = ContractBuilder::new("test", "owner")
            .location("s3://test")
            .format(DataFormat::Iceberg)
            .field(
                FieldBuilder::new("date", "timestamp")
                    .nullable(false)
                    .build(),
            )
            .quality_checks(QualityChecks {
                completeness: None,
                uniqueness: None,
                freshness: Some(FreshnessCheck {
                    max_delay: "7d".to_string(), // 7 days
                    metric: "date".to_string(),
                }),
                custom_checks: None,
            })
            .build();

        // Use today's date
        let today = Utc::now().format("%Y-%m-%d").to_string();

        let mut row = HashMap::new();
        row.insert("date".to_string(), DataValue::Timestamp(today));

        let dataset = DataSet::from_rows(vec![row]);
        let validator = CustomValidator::new();

        let errors = validator.validate(&contract, &dataset);
        assert_eq!(errors.len(), 0);
    }
}
