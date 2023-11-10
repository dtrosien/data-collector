use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::task::{execute_task, Task};
use futures_util::future::BoxFuture;
use sqlx::PgPool;
use std::collections::BinaryHeap;
use tokio::task::JoinHandle;

pub trait Runnable: Send + Sync {
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>>;
}

pub async fn run(db_pool: PgPool, task_settings: &[TaskSetting]) -> Result<()> {
    let mut handles: Vec<JoinHandle<Result<()>>> = vec![];
    let mut prio_queue = build_prio_queue(task_settings, &db_pool).await;
    while let Some(task) = prio_queue.pop() {
        handles.push(execute_task(task));
    }

    // todo maybe later with joinset, join_all!() has performance pitfalls
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await?);
    }

    Ok(())
}

pub async fn build_prio_queue(task_settings: &[TaskSetting], db_pool: &PgPool) -> BinaryHeap<Task> {
    let mut prio_queue = BinaryHeap::with_capacity(task_settings.len());

    for ts in task_settings.iter() {
        let task = Task::new(ts, db_pool);
        prio_queue.push(task)
    }

    prio_queue
}

pub fn execute_runnable(runnable: Box<dyn Runnable>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { runnable.run().await })
}
pub fn execute<T: Runnable + 'static>(e: Box<T>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { e.run().await })
}
