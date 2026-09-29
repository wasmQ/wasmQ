mod api;
mod state;

use std::sync::Arc;

use anyhow::Result;
use axum::Router;
use wasmq_repository::TaskRepository;
use tokio::net::TcpListener;

use wasmq_config::Config;

use crate::process::hub::Hub;
use crate::server::api::routes;
use crate::server::state::Services;
use crate::utils::shutdown_signal;

pub async fn run_server(
    config: Arc<Config>,
    hub: Arc<Hub>,
    repo: Arc<TaskRepository>,
) -> Result<()> {
    let services = Arc::new(Services::new(Arc::clone(&config), hub, repo));
    let app = Router::new().merge(routes(services));
    let listener = TcpListener::bind("0.0.0.0:6283").await?;

    println!("Server listening. {}", config.hub.api_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}
