use data_collector::configuration::{get_configuration, TaskSetting};
use data_collector::db;
use data_collector::task::{execute_task, Task};
use data_collector::telemetry::{get_subscriber, init_subscriber};
use sqlx::PgPool;
use std::error::Error;
use tokio::task::JoinHandle;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let subscriber = get_subscriber("data_collector".into(), "info".into(), std::io::stdout);
    init_subscriber(subscriber);

    let configuration = get_configuration().expect("Failed to read configuration.");
    let connection_pool = db::create_connection_pool(&configuration);

    run(connection_pool, configuration.application.tasks).await?;

    Ok(())
}

pub async fn run(
    db_pool: PgPool,
    task_settings: Vec<TaskSetting>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let handles: Vec<JoinHandle<Result<(), Box<dyn Error + Send + Sync>>>> = task_settings
        .into_iter()
        .map(|ts| {
            let task = Task::new(&ts, &db_pool);
            tokio::spawn(async move { task.run().await })
        })
        .collect();

    // todo maybe later with joinset, join_all!() has performance pitfalls
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await?);
    }

    Ok(())
}
