use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type")]
pub enum TransportConfig {
    UnixSocket { base_path: PathBuf },
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self::UnixSocket {
            base_path: PathBuf::from("/tmp/wasmq_sys"),
        }
    }
}
