use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::task::{execute_runnable, Runnable, Task};
use sqlx::PgPool;
use std::collections::{BTreeMap, BinaryHeap};
use std::error::Error;
use tokio::task::JoinHandle;

pub async fn run(db_pool: PgPool, task_settings: &[TaskSetting]) -> Result<()> {
    let mut handles: Vec<JoinHandle<Result<()>>> = vec![];
    let mut schedule = build_schedule(task_settings, &db_pool).await;
    while let Some((prio, task)) = schedule.pop_first() {
        handles.push(execute_runnable(task));
    }

    // todo maybe later with joinset, join_all!() has performance pitfalls
    let mut results = Vec::with_capacity(handles.len());
    for handle in handles {
        results.push(handle.await?);
    }

    Ok(())
}

pub async fn build_schedule(
    task_settings: &[TaskSetting],
    db_pool: &PgPool,
) -> BTreeMap<i32, Box<dyn Runnable>> {
    let mut scheduled_runnables = BTreeMap::new();
    for ts in task_settings.iter() {
        let task: Box<dyn Runnable> = Box::new(Task::new(ts, db_pool));
        scheduled_runnables.insert(ts.priority, task);
    }
    scheduled_runnables
}
