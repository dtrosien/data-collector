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
use std::sync::Arc;
use tokio::task::JoinHandle;
use uuid::Uuid;

pub struct Task {
    pub id: Uuid,
    pub execution_sequence_position: i32,
    pub actions: Vec<BoxedAction>,
    pub action_dependencies: ActionDependencies, // maybe use Arc<Mutex> to reduce mem overhead -> however more blocking of threads
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
        self.execution_sequence_position
            .eq(&other.execution_sequence_position)
    }
}

impl PartialOrd<Self> for Task {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> Ordering {
        self.execution_sequence_position
            .cmp(&other.execution_sequence_position)
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
            execution_sequence_position: setting.execution_sequence_position,
            actions,
            action_dependencies: ActionDependencies {
                pool: db.clone(),
                setting: setting.clone(),
                client: client.clone(),
            },
        }
    }

    pub fn get_execution_sequence_position(&self) -> i32 {
        self.execution_sequence_position
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

pub fn execute_task(task: Arc<Task>) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { task.run().await })
}
