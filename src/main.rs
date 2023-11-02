use data_collector::configuration::get_configuration;
use data_collector::runner::run;
use data_collector::source_apis::nyse;
use data_collector::telemetry::{get_subscriber, init_subscriber};

use sqlx::PgPool;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subscriber = get_subscriber("data_collector".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let mut configuration = get_configuration().expect("Failed to read configuration.");
    configuration.database.database_name = "newsletter".to_string();
    // let connection_pool = db::create_connection_pool(&configuration);
    let connection_pool = PgPool::connect_with(configuration.database.with_db())
        .await
        .expect("Failed to connect to Postgres.");

    let url = "https://listingmanager.nyse.com/api/corpax/";

    nyse::load_and_store_missing_data(url, &connection_pool)
        .await
        .unwrap();

    run(connection_pool, configuration.application.tasks).await?;

    println!("done");
    Ok(())
}
