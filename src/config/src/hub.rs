use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use serde::{Deserialize, Serialize};

pub const WASMQ_SERVER_DEFAULT_PORT: u16 = 6283;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HubConfig {
    pub api_addr: SocketAddr,
}

impl Default for HubConfig {
    fn default() -> Self {
        Self {
            api_addr: SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::LOCALHOST,
                WASMQ_SERVER_DEFAULT_PORT,
            )),
        }
    }
}
