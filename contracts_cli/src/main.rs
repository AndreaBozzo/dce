mod commands;
mod output;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "dce")]
#[command(version, about = "Data Contracts Engine CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,

    /// Enable verbose logging
    #[arg(short, long, global = true)]
    verbose: bool,
}

#[derive(Subcommand)]
enum Commands {
    /// Validate a contract against actual data
    Validate {
        /// Path to the contract file (YAML or TOML)
        contract: String,

        /// Enable strict validation mode (fail on warnings)
        #[arg(short, long)]
        strict: bool,

        /// Output format: text, json
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Check contract schema without validating data
    Check {
        /// Path to the contract file (YAML or TOML)
        contract: String,

        /// Output format: text, json
        #[arg(short, long, default_value = "text")]
        format: String,
    },

    /// Initialize a new contract from an existing Iceberg table
    Init {
        /// Iceberg table location or catalog URI
        source: String,

        /// Output file path (defaults to stdout)
        #[arg(short, long)]
        output: Option<String>,

        /// Catalog type: rest, glue, hms
        #[arg(short, long, default_value = "rest")]
        catalog: String,

        /// Table namespace (e.g., "database.schema")
        #[arg(short, long)]
        namespace: Option<String>,

        /// Table name
        #[arg(short, long)]
        table: Option<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize tracing
    let log_level = if cli.verbose {
        tracing::Level::DEBUG
    } else {
        tracing::Level::INFO
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_level(true)
                .compact(),
        )
        .with(tracing_subscriber::filter::LevelFilter::from_level(
            log_level,
        ))
        .init();

    // Execute command
    match cli.command {
        Commands::Validate {
            contract,
            strict,
            format,
        } => commands::validate::execute(&contract, strict, &format).await,

        Commands::Check { contract, format } => commands::check::execute(&contract, &format).await,

        Commands::Init {
            source,
            output,
            catalog,
            namespace,
            table,
        } => commands::init::execute(&source, output.as_deref(), &catalog, namespace, table).await,
    }
}
