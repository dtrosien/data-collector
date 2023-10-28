use crate::configuration::TaskSetting;
use crate::{client, db};
use sqlx::PgPool;
use std::error::Error;
use tokio::task::JoinHandle;
use uuid::Uuid;

#[derive(Debug)]
pub struct Task {
    id: Uuid,
    pool: PgPool,
    url: String,
}

impl Task {
    pub fn new(setting: &TaskSetting, db: &PgPool) -> Self {
        Task {
            id: Uuid::new_v4(),
            pool: db.clone(),
            url: setting.exclude_sources.clone().unwrap().pop().unwrap(), // todo dummy
        }
    }

    #[tracing::instrument(
    name = "Running task",
    fields(
    task_id = %self.id,
    url = %self.url
    )
    )]
    pub async fn run(&self) -> Result<(), Box<dyn Error + Send + Sync>> {
        let _response = client::query_api(&self.url).await;

        db::insert_into(&self.pool)
            .await
            .expect("TODO: panic message");
        Ok(())
    }
}

pub async fn execute_task(task: Task) -> JoinHandle<Result<(), Box<dyn Error + Send + Sync>>> {
    tokio::spawn(async move { task.run().await })
}
