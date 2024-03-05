// use crate::configuration::TaskSetting;
// use crate::dag_scheduler::task::{Runnable, StatsMap, TaskError};
// use crate::tasks::actions::action::{create_action, BoxedAction};
// use crate::utils::futures::join_future_results;
// use async_trait::async_trait;
// use futures_util::future::BoxFuture;
// use reqwest::Client;
// use sqlx::PgPool;
// use std::cmp::Ordering;
// use std::sync::Arc;
// use tokio::task::JoinHandle;
// use uuid::Uuid;
//
// pub struct Task {
//     pub id: Uuid,
//     pub action: BoxedAction,
//     pub action_dependencies: ActionDependencies, // maybe use Arc<Mutex> to reduce mem overhead -> however more blocking of threads
// }
//
// #[derive(Clone)]
// pub struct ActionDependencies {
//     pub pool: PgPool,
//     pub setting: TaskSetting,
//     pub client: Client,
// }

// impl Eq for Task {}

// impl PartialEq<Self> for Task {
//     fn eq(&self, other: &Self) -> bool {
//         self.execution_sequence_position
//             .eq(&other.execution_sequence_position)
//     }
// }

// impl PartialOrd<Self> for Task {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }
//
// impl Ord for Task {
//     fn cmp(&self, other: &Self) -> Ordering {
//         self.execution_sequence_position
//             .cmp(&other.execution_sequence_position)
//     }
// }

// impl Task {
//     pub fn new(setting: &TaskSetting, db: &PgPool, client: &Client) -> Self {
//         let action = create_action(&setting.task_type);
//
//         Task {
//             id: Uuid::new_v4(),
//             action,
//             action_dependencies: ActionDependencies {
//                 pool: db.clone(),
//                 setting: setting.clone(),
//                 client: client.clone(),
//             },
//         }
//     }
//
//     //  pub fn get_execution_sequence_position(&self) -> i32 {
//     //      self.execution_sequence_position
//     //  }
// }

// #[async_trait]
// impl Runnable for Task {
//     #[tracing::instrument(name = "Running tasks", skip(self), fields(task_id = % self.id,))]
//     async fn run(&self) -> Result<Option<StatsMap>, TaskError> {
//         let action_futures = self
//             .actions
//             .iter()
//             .map(|action| action.execute(self.action_dependencies.clone()))
//             .collect::<Vec<BoxFuture<Result<Option<StatsMap>, TaskError>>>>();
//
//         join_future_results(action_futures).await
//     }
// }

// pub fn execute_task(task: Arc<Task>) -> JoinHandle<anyhow::Result<(), TaskError>> {
//     tokio::spawn(async move { task.run().await })
// }

// #[derive(thiserror::Error, Debug)]
// pub enum TaskError {
//     #[error("Database interaction failed")]
//     DatabaseError(#[source] sqlx::Error),
//     #[error("The action of the task failed")]
//     ClientRequestError(#[source] reqwest::Error),
//     #[error("Something went wrong")]
//     UnexpectedError(#[from] anyhow::Error),
// }
