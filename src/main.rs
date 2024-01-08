extern crate tracing;

use data_collector::configuration::get_configuration;
use data_collector::utils::telemetry::{get_subscriber, init_subscriber};

use data_collector::startup::Application;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subscriber = get_subscriber("data_collector".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let configuration = get_configuration().expect("Failed to read configuration.");

    Application::build(configuration).await.run().await?;

    Ok(())
}
