pub mod analyze;
pub mod bayesian;
pub mod config;
pub mod dashboard_server;
pub mod edge;
pub mod executor;
pub mod kelly;
pub mod lmsr;
pub mod market_scanner;
pub mod price_feeds;
pub mod risk;
pub mod timezone;
pub mod trading_loop;
pub mod venue;

/// Top-level error type aggregating all module errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),
    #[error("price feed error: {0}")]
    PriceFeed(#[from] price_feeds::PriceFeedError),
    #[error("market scanner error: {0}")]
    MarketScanner(#[from] market_scanner::ScannerError),
    #[error("LMSR computation error: {0}")]
    Lmsr(#[from] lmsr::LmsrError),
    #[error("bayesian update error: {0}")]
    Bayesian(#[from] bayesian::BayesianError),
    #[error("edge calculation error: {0}")]
    Edge(#[from] edge::EdgeError),
    #[error("execution error: {0}")]
    Executor(#[from] executor::ExecutorError),
    #[error("risk limit breached: {0}")]
    Risk(#[from] risk::RiskError),
    #[error("venue error: {0}")]
    Venue(#[from] venue::VenueError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}
