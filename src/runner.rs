use crate::configuration::TaskSetting;
use crate::tasks::task::Task;
use sqlx::PgPool;
use std::error::Error;
use tokio::task::JoinHandle;

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
