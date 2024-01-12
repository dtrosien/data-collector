use crate::configuration::TaskSetting;
use crate::tasks::actions::action::{create_action, BoxedAction};
use crate::tasks::runnable::Runnable;
use crate::utils::errors::Result;
use crate::utils::futures::join_future_results;
use async_trait::async_trait;
use futures_util::future::BoxFuture;
use reqwest::Client;
use sqlx::PgPool;
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use tokio::task::JoinHandle;
use uuid::Uuid;

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
    pub client: Client,
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
    pub fn new(setting: &TaskSetting, db: &PgPool, client: &Client) -> Self {
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
                client: client.clone(),
            },
        }
    }

    pub fn get_priority(&self) -> i32 {
        self.priority
    }
}

#[async_trait]
impl Runnable for Task {
    #[tracing::instrument(name = "Running tasks", skip(self), fields(task_id = % self.id,))]
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
#[tracing::instrument(
    name = "Building tasks priority queue",
    skip(task_settings, pool, client)
)]
pub async fn build_task_prio_queue(
    task_settings: &[TaskSetting],
    pool: &PgPool,
    client: &Client,
) -> BinaryHeap<Task> {
    let mut prio_queue = BinaryHeap::with_capacity(task_settings.len());

    for ts in task_settings.iter().filter(|s| s.priority >= 0) {
        let task = Task::new(ts, pool, client);
        prio_queue.push(task)
    }

    prio_queue
}

#[cfg(test)]
mod test {
    use crate::collectors::collector_sources::CollectorSource::All;
    use crate::collectors::sp500_fields::Fields;
    use crate::configuration::TaskSetting;
    use crate::tasks::actions::action::ActionType::Collect;
    use crate::tasks::task::build_task_prio_queue;
    use crate::utils::test_helpers::get_test_client;
    use sqlx::{Pool, Postgres};

    #[sqlx::test]
    async fn prio_queue_order_and_filter(pool: Pool<Postgres>) {
        // Arrange
        let priorities_in = vec![1, 500, 300, 1000, -2, 90, -20];
        let priorities_out = vec![1000, 500, 300, 90, 1];
        let base_task = TaskSetting {
            comment: None,
            actions: vec![Collect],
            sp500_fields: vec![Fields::Nyse],
            priority: 0,
            include_sources: vec![All],
            exclude_sources: vec![],
        };
        let mut tasks = vec![];
        for n in priorities_in {
            let mut task = base_task.clone();
            task.priority = n;
            tasks.push(task);
        }
        let client = get_test_client();

        // Act
        let mut queue = build_task_prio_queue(&tasks, &pool, &client).await;

        // Assert
        for n in priorities_out {
            assert_eq!(queue.pop().unwrap().get_priority(), n)
        }
    }
}
