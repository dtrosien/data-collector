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
// my request: https://api.polygon.io/v1/open-close/AAMC/2022-12-16?adjusted=true&apiKey=iMX3MwHNhuj_RAHRG1zkor2o4WyZqp2U
// thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value', src/collectors/source_apis/polygon_open_close.rs:159:50
// stack backtrace:
//    0: rust_begin_unwind
//              at /rustc/5680fa18feaa87f3ff04063800aec256c3d4b4be/library/std/src/panicking.rs:593:5

// my request: https://api.polygon.io/v1/open-close/AAME/2023-09-27?adjusted=true&apiKey=iMX3MwHNhuj_RAHRG1zkor2o4WyZqp2U
// thread 'tokio-runtime-worker' panicked at 'called `Option::unwrap()` on a `None` value', src/collectors/source_apis/polygon_open_close.rs:159:50
