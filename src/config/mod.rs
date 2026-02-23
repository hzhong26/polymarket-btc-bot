pub mod types;
pub use types::*;

use secrecy::SecretString;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("missing environment variable: {0}")]
    MissingEnv(String),
    #[error("invalid venue configuration: {0}")]
    InvalidVenue(String),
}

/// Load config from a TOML file at the given path.
pub fn load_config(path: &str) -> Result<AppConfig, ConfigError> {
    let content = std::fs::read_to_string(path)?;
    let config: AppConfig = toml::from_str(&content)?;
    Ok(config)
}

/// Load the private key from the `PRIVATE_KEY` environment variable.
/// Returns a SecretString that redacts on Debug/Display.
pub fn load_private_key() -> Result<SecretString, ConfigError> {
    std::env::var("PRIVATE_KEY")
        .map(SecretString::from)
        .map_err(|_| ConfigError::MissingEnv("PRIVATE_KEY".into()))
}

/// Load the US venue API key from env.
pub fn load_us_api_key() -> Result<SecretString, ConfigError> {
    std::env::var("POLYMARKET_US_API_KEY")
        .map(SecretString::from)
        .map_err(|_| ConfigError::MissingEnv("POLYMARKET_US_API_KEY".into()))
}

/// Load the US venue API secret from env.
pub fn load_us_api_secret() -> Result<SecretString, ConfigError> {
    std::env::var("POLYMARKET_US_API_SECRET")
        .map(SecretString::from)
        .map_err(|_| ConfigError::MissingEnv("POLYMARKET_US_API_SECRET".into()))
}
