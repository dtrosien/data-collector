#![allow(dead_code)]

use data_collector::configuration;
use data_collector::nyse::nyse::{self};

use sqlx::PgPool;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // let tasks = collect_conf::load_file("configs/config.yaml").unwrap();
    let mut configuration = configuration::get_configuration().unwrap();
    configuration.database.database_name = "newsletter".to_string();

    let connection_pool = PgPool::connect_with(configuration.database.with_db())
        .await
        .expect("Failed to connect to Postgres.");

    let url = "https://listingmanager.nyse.com/api/corpax/";

    nyse::load_and_store_missing_data(url, &connection_pool).await?;

    println!("done");
    Ok(())
}

fn say_hello() -> String {
    "Hello, world!".to_string()
}

#[cfg(test)]
mod tests {
    use crate::say_hello;

    #[test]
    fn say_hello_test() {
        assert_eq!(say_hello(), format!("Hello, world!"))
    }
}
