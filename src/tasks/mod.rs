use crate::configuration::TaskSetting;
use crate::tasks::actions::{create_action, BoxedAction};
use crate::tasks::runnable::Runnable;
use crate::utils::error::Result;
use crate::utils::future_utils::join_future_results;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use sqlx::PgPool;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub mod actions;
pub mod runnable;

pub struct Task {
    id: Uuid,
    priority: i32,
    actions: Vec<BoxedAction>,
    action_dependencies: ActionDependencies, // maybe use Arc<Mutex> to reduce mem overhead -> however more blocking of threads
}

#[derive(Clone)]
pub struct ActionDependencies {
    pub pool: PgPool,
    pub setting: TaskSetting,
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

    pub fn get_priority(&self) -> i32 {
        self.priority
    }
}

#[async_trait]
impl Runnable for Task {
    #[tracing::instrument(name = "Running tasks", skip(self), fields(task_id = %self.id,))]
    async fn run(&self) -> Result<()> {
        let action_futures = self
            .actions
            .iter()
            .map(|action| action.execute(self.action_dependencies.clone()))
            .collect::<Vec<BoxFuture<Result<()>>>>();

        join_future_results(action_futures).await
    }
}

pub fn execute_task(task: Task) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { task.run().await })
}

/// build a priority queue for Tasks based on a binary heap
/// only tasks with prio > 0 will be scheduled
#[tracing::instrument(name = "Building tasks priority queue", skip(task_settings, pool))]
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
