use crate::configuration::TaskSetting;
use crate::tasks::{build_task_prio_queue, execute_task};
use crate::utils::errors::Result;
use crate::utils::futures::join_handle_results;
use sqlx::PgPool;

/// runs all tasks concurrently and waits till all tasks finished,
/// if one tasks failed, all other tasks will still processed, but overall the function will return an error
#[tracing::instrument(name = "Start running tasks", skip(task_settings, pool))]
pub async fn run(pool: PgPool, task_settings: &[TaskSetting]) -> Result<()> {
    let handles = build_task_prio_queue(task_settings, &pool)
        .await
        .into_iter()
        .map(execute_task)
        .collect();

    join_handle_results(handles).await
}
