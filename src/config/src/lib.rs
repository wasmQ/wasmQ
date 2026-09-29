pub mod executor;
pub mod hub;
pub mod scheduler;
pub mod storage;
pub mod transport;

use std::fs::read_to_string;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::executor::ExecutorConfig;
use crate::hub::HubConfig;
use crate::scheduler::SchedulerConfig;
use crate::storage::StorageConfig;
use crate::transport::TransportConfig;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Config {
    pub transport: TransportConfig,
    pub hub: HubConfig,
    pub storage: StorageConfig,
    pub scheduler: SchedulerConfig,
    pub executors: ExecutorConfig,
}

impl Config {
    /// Serializes a [`Config`] instance into a TOML string
    pub fn to_toml(&self) -> Result<String> {
        toml::to_string_pretty(self)
            .with_context(|| String::from("Failed to serialize configuration to TOML format."))
    }

    /// Creates a [`Config`] instance from a `.toml` file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let contents = read_to_string(path)?;
        Self::from_toml(&contents)
    }

    fn from_toml(c: &str) -> Result<Self> {
        toml::from_str(c)
            .with_context(|| String::from("Failed to parse configuration file in TOML format."))
    }
}

#[cfg(test)]
mod test {
    use crate::{Config, transport::TransportConfig};

    #[test]
    fn parse_local_config() {
        let config_toml = r#"
            [transport]
            type = "UnixSocket"
            base_path = "/tmp/wasmq_sys"

            [hub]
            api_addr = "127.0.0.1:6283"

            [storage]
            backend = "Memory"

            [scheduler]
            check_interval_secs = 1

            [executors]
            count = 2
            max_concurrent_jobs = 5
            "#;

        let config = Config::from_toml(config_toml);

        assert!(config.is_ok());

        let config = config.expect("Expected a valid configuration file");

        assert_eq!(
            config.transport,
            TransportConfig::UnixSocket {
                base_path: "/tmp/wasmq_sys".into()
            }
        );
    }
}
