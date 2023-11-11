use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::future_utils::join_handle_results;
use crate::task::{execute_task, Task};
use futures_util::future::BoxFuture;
use sqlx::PgPool;
use std::collections::BinaryHeap;
use tokio::task::JoinHandle;

/// runs all tasks concurrently and waits till all tasks finished,
/// if one task failed, all other task will still processed, but overall the function will return an error
#[tracing::instrument(name = "Start running tasks", skip(task_settings, pool))]
pub async fn run(pool: PgPool, task_settings: &[TaskSetting]) -> Result<()> {
    let handles = build_task_prio_queue(task_settings, &pool)
        .await
        .into_iter()
        .map(execute_task)
        .collect();

    join_handle_results(handles).await
}

/// build a priority queue for Tasks based on a binary heap
/// only tasks with prio > 0 will be scheduled
#[tracing::instrument(name = "Building task priority queue", skip(task_settings, pool))]
pub async fn build_task_prio_queue(
    task_settings: &[TaskSetting],
    pool: &PgPool,
) -> BinaryHeap<Task> {
    let mut prio_queue = BinaryHeap::with_capacity(task_settings.len());

    for ts in task_settings.iter().filter(|s| s.priority >= 0) {
        let task = Task::new(ts, pool);
        prio_queue.push(task)
    }

    prio_queue
}

/// used when spawning tokio tasks, needs adjustment when async traits are allowed
pub trait Runnable: Send + Sync {
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>>;
}

// currently not useful since rust does not (yet) support trait upcasting
pub fn execute_runnable(runnable: Box<dyn Runnable>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { runnable.run().await })
}

//todo check if this is a possible way to generalize more
pub fn execute<T: Runnable + 'static>(e: Box<T>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { e.run().await })
}
