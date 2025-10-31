use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Contract {
    pub version: String,
    pub name:String,
    pub owner: String,
    pub description: Option<String>,
    pub schema: Schema,
    pub quality_checks: Option<QualityChecks>,
    pub sla: Option<SLA>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFormat {
    Iceberg,
    Parquet,
    Json,
    Csv,
    Avro,
    Orc,
    Delta,
    Hudi,
    Custom(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub fields: Vec<Field>,
    pub format: DataFormat,
    pub location: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Field {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    pub nullable: bool,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub constraints: Option<Vec<FieldConstraints>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FieldConstraints {
    AllowedValues { values: Vec<String> },
    Range { min: f64, max: f64 },
    Pattern { regex: String },
    Custom { definition: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QualityChecks {
    pub completeness: Option<CompletenessCheck>,
    pub uniqueness: Option<UniquenessCheck>,
    pub freshness: Option<FreshnessCheck>,
    pub custom_checks: Option<Vec<CustomCheck>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshnessCheck {
    pub max_delay: String,
    pub metric: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletenessCheck {
    pub threshold: f64,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniquenessCheck {
    pub fields: Vec<String>,
    pub scope: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomCheck {
    pub name: String,
    pub definition: String,
    pub severity: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SLA {
    pub availability: Option<f64>,
    pub response_time: Option<String>,
    pub penalties: Option<String>,
}