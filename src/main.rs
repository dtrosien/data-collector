extern crate tracing;

use data_collector::configuration::get_configuration;
use data_collector::utils::telemetry::{get_open_telemetry_subscriber, init_subscriber};

use data_collector::startup::Application;
use opentelemetry::global::shutdown_tracer_provider;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subscriber =
        get_open_telemetry_subscriber("data_collector".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let configuration = get_configuration().expect("Failed to read configuration.");

    Application::build(configuration).await.run().await?;

    shutdown_tracer_provider();
    Ok(())
}
