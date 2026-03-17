//! # Data Contracts Core
//!
//! Core data structures and types for the Data Contracts Engine.
//!
//! This crate provides the fundamental building blocks for defining, parsing, and working with
//! data contracts. A data contract is a formal agreement about the structure, quality, and
//! semantics of data shared between systems.
//!
//! ## Key Concepts
//!
//! - **Contract**: The main data structure representing a complete data contract
//! - **Schema**: Defines the structure and format of the data
//! - **Quality Checks**: Validation rules for data quality (completeness, uniqueness, freshness)
//! - **SLA**: Service Level Agreement for data availability and performance
//!
//! ## Example
//!
//! ```rust
//! use contracts_core::{Contract, Schema, Field, DataFormat};
//!
//! let contract = Contract {
//!     version: "1.0.0".to_string(),
//!     name: "user_events".to_string(),
//!     owner: "analytics-team".to_string(),
//!     description: Some("User interaction events".to_string()),
//!     schema: Schema {
//!         fields: vec![
//!             Field {
//!                 name: "user_id".to_string(),
//!                 field_type: "string".to_string(),
//!                 nullable: false,
//!                 description: Some("Unique user identifier".to_string()),
//!                 tags: None,
//!                 constraints: None,
//!             },
//!         ],
//!         format: DataFormat::Iceberg,
//!         location: "s3://data/user_events".to_string(),
//!     },
//!     quality_checks: None,
//!     sla: None,
//! };
//! ```

pub mod builder;
pub mod contract;
pub mod error;
pub mod validator;

pub use builder::*;
pub use contract::*;
pub use error::*;
pub use validator::*;
