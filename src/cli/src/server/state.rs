use std::sync::Arc;

use wasmq_config::Config;
use wasmq_repository::TaskRepository;

use crate::process::hub::Hub;

pub type SharedServices = Arc<Services>;

#[derive(Clone)]
pub struct Services {
    pub config: Arc<Config>,
    pub hub: Arc<Hub>,
    pub repo: Arc<TaskRepository>,
}

impl Services {
    pub fn new(config: Arc<Config>, hub: Arc<Hub>, repo: Arc<TaskRepository>) -> Self {
        Self { hub, repo, config }
    }
}
