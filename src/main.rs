use data_collector::configuration::get_configuration;
use data_collector::db;
use data_collector::runner::run;
use data_collector::telemetry::{get_subscriber, init_subscriber};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subscriber = get_subscriber("data_collector".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let mut configuration = get_configuration().expect("Failed to read configuration.");
    configuration.database.database_name = "newsletter".to_string();
    let connection_pool = db::create_connection_pool(&configuration);

    run(connection_pool, configuration.application.tasks).await?;

    let url = "https://listingmanager.nyse.com/api/corpax/";

    nyse::load_and_store_missing_data(url, &connection_pool).await?;

    println!("done");
    Ok(())
}


