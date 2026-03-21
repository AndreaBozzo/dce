//! # Data Contracts Validator
//!
//! Validation engine for data contracts. This crate provides the core validation
//! logic for checking data against contract definitions, including:
//!
//! - Schema validation (field presence, types, nullability)
//! - Constraint validation (allowed values, ranges, patterns)
//! - Quality checks (completeness, uniqueness, freshness)
//! - Custom SQL-based validation rules
//!
//! ## Example
//!
//! ```rust
//! use contracts_validator::{DataValidator, DataSet};
//! use contracts_core::{Contract, ValidationContext};
//! # use contracts_core::{ContractBuilder, DataFormat};
//!
//! # let contract = ContractBuilder::new("test", "owner")
//! #     .location("s3://test")
//! #     .format(DataFormat::Iceberg)
//! #     .build();
//! let mut validator = DataValidator::new();
//! let context = ValidationContext::new();
//!
//! // Validate with empty dataset for now
//! let dataset = DataSet::empty();
//! let report = validator.validate_with_data(&contract, &dataset, &context);
//!
//! if report.passed {
//!     println!("Validation passed!");
//! } else {
//!     println!("Validation failed: {:?}", report.errors);
//! }
//! ```

mod constraints;
mod custom;
mod datafusion_engine;
mod dataset;
mod engine;
mod error;
mod file_reader;
mod ml;
mod quality;
mod schema;

pub use constraints::*;
pub use custom::*;
pub use datafusion_engine::*;
pub use dataset::*;
pub use engine::*;
pub use error::*;
pub use file_reader::*;
pub use ml::*;
pub use quality::*;
pub use schema::*;
