extern crate tracing;

use data_collector::configuration::get_configuration;
use data_collector::db;
use data_collector::runner::run;
use data_collector::source_apis::nyse;
use data_collector::telemetry::{get_subscriber, init_subscriber};

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subscriber = get_subscriber("data_collector".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let configuration = get_configuration().expect("Failed to read configuration.");
    let connection_pool = db::create_connection_pool(&configuration);
    connection_pool.set_connect_options(configuration.database.with_db());

    nyse::load_and_store_missing_data(&connection_pool)
        .await
        .unwrap();

    run(connection_pool, configuration.application.tasks).await?;

    println!("done");
    Ok(())
}
