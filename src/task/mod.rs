use crate::actions::{create_action, BoxedAction};
use crate::configuration::TaskSetting;
use crate::error::Result;
use crate::runner::Runnable;
use futures_util::future::{join_all, BoxFuture};
use sqlx::PgPool;
use std::cmp::Ordering;
use std::error::Error;
use tokio::task::JoinHandle;
use tracing::warn;
use uuid::Uuid;

pub struct Task {
    id: Uuid,
    priority: i32,
    actions: Vec<BoxedAction>,
    action_dependencies: ActionDependencies,
}

impl Eq for Task {}

impl PartialEq<Self> for Task {
    fn eq(&self, other: &Self) -> bool {
        self.priority.eq(&other.priority)
    }
}

impl PartialOrd<Self> for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        self.priority.cmp(&other.priority)
    }
}

impl Task {
    pub fn new(setting: &TaskSetting, db: &PgPool) -> Self {
        let actions = setting
            .actions
            .iter()
            .filter_map(|at| create_action(at).ok())
            .collect();

        Task {
            id: Uuid::new_v4(),
            priority: setting.priority,
            actions,
            action_dependencies: ActionDependencies {
                pool: db.clone(),
                setting: setting.clone(),
            },
        }
    }
}

#[derive(Clone)]
pub struct ActionDependencies {
    pub pool: PgPool,
    pub setting: TaskSetting,
}

impl Runnable for Task {
    #[tracing::instrument(
    name = "Running task",
    skip(self),
    fields(
    task_id = %self.id,
    // collectors = %self.collectors
    )
    )]
    fn run<'a>(&self) -> BoxFuture<'a, Result<()>> {
        let action_futures = self
            .actions
            .iter()
            .map(|x| x.perform(self.action_dependencies.clone()))
            .collect::<Vec<BoxFuture<'a, Result<()>>>>();

        Box::pin(join_future_results(action_futures))
    }
}

/// fails if a single future fails and cancels all the other futures
async fn join_future_results_strict(futures: Vec<BoxFuture<'_, Result<()>>>) -> Result<()> {
    let results = join_all(futures).await;
    results.into_iter().collect()
}

/// fails if a single future fails but finishes all futures
async fn join_future_results(futures: Vec<BoxFuture<'_, Result<()>>>) -> Result<()> {
    let mut errors = Vec::new();
    for future in futures {
        if let Err(err) = future.await {
            errors.push(err);
        }
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err("One or more actions failed".into())
    }
}

pub fn execute_task(boxed_task: Task) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { boxed_task.run().await })
}
